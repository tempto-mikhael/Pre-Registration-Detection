#!/usr/bin/env python3
"""
LLM verification of pre-registration detection using pluggable providers.

Sends each ambiguous paper's text to an LLM provider (Gemini/OpenRouter)
and asks whether the paper reports its OWN pre-registration.
Resumable, rate-limited, outputs CSV.

Two pipeline groups:
  A – keyword-only hits (scanner found prereg wording but no registry link)
  C – link-backed candidates (scanner or enrichment found a registry link)

Usage:
    # Set your API key first:
    $env:GEMINI_API_KEY = "your-key-here"

    # Run the pipeline review set:
    python llm_verify.py --group all

    # Custom settings:
    python llm_verify.py --group all --max-chars 15000 --tpm 250000 --model gemini-2.0-flash

Requirements:
    pip install google-generativeai
"""

import argparse
import csv
import json
import os
import re
import sys
import time
from functools import lru_cache
from pathlib import Path

import fitz  # PyMuPDF
import requests
from path_utils import resolve_existing_path, resolve_output_path

try:
    from google import genai
    from google.genai import types
    HAS_GEMINI = True
except ImportError:
    genai = None
    types = None
    HAS_GEMINI = False

ROOT = Path(__file__).parent.parent
OUTPUT_DIR = ROOT / "output"
DEFAULT_SCAN_CSV = OUTPUT_DIR / "pdf_scan_results.csv"
FALLBACK_SCAN_CSV = OUTPUT_DIR / "pdf_scan_results_v2.csv"
DEFAULT_ENRICHED_CSV = OUTPUT_DIR / "pdf_scan_prereg_links_dedup.csv"
FALLBACK_ENRICHED_CSV = OUTPUT_DIR / "pdf_scan_prereg_links.csv"
DEFAULT_RESULTS_CSV = OUTPUT_DIR / "llm_verdicts.csv"

PREREG_TERMS = (
    "pre-registration",
    "preregistration",
    "pre-registered",
    "preregistered",
    "pre analysis plan",
    "pre-analysis plan",
    "analysis plan",
    "registered report",
)

MATERIALS_TERMS = (
    "replication material",
    "replication materials",
    "replication data",
    "data and code",
    "data and scripts",
    "data and materials",
    "data, code, and materials",
    "data and code scripts",
    "supplementary data",
    "supplementary files",
    "code scripts",
    "reproduce analyses",
    "reproduction code",
)

VERIFIED_QUALITIES = {"VERIFIED", "DOI_CONFIRMED", "AUTHOR_CONFIRMED"}

FOCUSED_TEXT_PATTERNS = [
    re.compile(r"pre-?registered", re.IGNORECASE),
    re.compile(r"pre-?registration", re.IGNORECASE),
    re.compile(r"preregistration", re.IGNORECASE),
    re.compile(r"pre-?analysis plan", re.IGNORECASE),
    re.compile(r"analysis plan", re.IGNORECASE),
    re.compile(r"registered an analysis plan", re.IGNORECASE),
    re.compile(r"registered report", re.IGNORECASE),
    re.compile(r"aspredicted", re.IGNORECASE),
    re.compile(r"osf\s*\.\s*io", re.IGNORECASE),
    re.compile(r"open science framework", re.IGNORECASE),
    re.compile(r"socialscienceregistry\s*\.\s*org", re.IGNORECASE),
    re.compile(r"egap\s*\.\s*org", re.IGNORECASE),
    re.compile(r"aearctr-\s*\d+", re.IGNORECASE),
    re.compile(r"\b3ie\b", re.IGNORECASE),
]

DIRECT_PREREG_PATTERNS = [
    re.compile(r"\b(?:we|our|this (?:paper|study|experiment)|the study|the paper)\b.{0,140}\bpre-?registered\b", re.IGNORECASE | re.DOTALL),
    re.compile(r"\bpreregistered at\b.{0,160}\b(?:osf|open science framework|aspredicted|egap|socialscienceregistry|aearctr|3ie)\b", re.IGNORECASE | re.DOTALL),
    re.compile(r"\bregistered an analysis plan\b.{0,160}\b(?:osf|open science framework|aspredicted|egap|socialscienceregistry|3ie)\b", re.IGNORECASE | re.DOTALL),
    re.compile(r"\bpre-?analysis plan\b.{0,220}\b(?:before|prior to|a priori)\b.{0,180}\b(?:data|follow-up|impact analyses|analysis|completion)\b", re.IGNORECASE | re.DOTALL),
    re.compile(r"\bas we preregistered\b", re.IGNORECASE),
    re.compile(r"\bpreregistration\b.{0,220}\b(?:osf|open science framework|aspredicted|egap|socialscienceregistry|aearctr)\b", re.IGNORECASE | re.DOTALL),
]

REGISTRY_URL_PATTERNS = [
    re.compile(r"https?://(?:www\.)?aspredicted\.org/blind(?:\.php)?\?x=[a-z0-9]+", re.IGNORECASE),
    re.compile(r"https?://(?:www\.)?socialscienceregistry\.org/trials/\d+", re.IGNORECASE),
    re.compile(r"https?://(?:www\.)?egap\.org/registration/\d+", re.IGNORECASE),
    re.compile(r"https?://(?:www\.)?osf\.io/[a-z0-9]+(?:/[^\s)\]}]*)?", re.IGNORECASE),
]

DEFAULT_MAX_CHARS = 0  # 0 = no limit (full PDF text); set to e.g. 40_000 to cap
DEFAULT_MODEL     = "gemini-3-flash-preview"
DEFAULT_TPM       = 250_000
DEFAULT_PROVIDER  = "gemini"

OPENROUTER_CHAT_URL = "https://openrouter.ai/api/v1/chat/completions"
OPENROUTER_MODELS_URL = "https://openrouter.ai/api/v1/models"
DEFAULT_OPENROUTER_FREE_MODELS = [
    "deepseek/deepseek-r1:free",
    "deepseek/deepseek-chat-v3-0324:free",
    "meta-llama/llama-3.3-70b-instruct:free",
    "meta-llama/llama-3.1-8b-instruct:free",
    "qwen/qwen3-next-80b-a3b-instruct:free",
    "qwen/qwen3-32b:free",
    "qwen/qwen-2.5-72b-instruct:free",
    "qwen/qwen3-coder:free",
    "google/gemma-3-27b-it:free",
    "mistralai/mistral-small-3.2-24b-instruct:free",
]

OPENROUTER_TRANSIENT_WAIT_BASE = 45
OPENROUTER_TRANSIENT_WAIT_MAX = 300
OPENROUTER_ROTATION_WAIT_SECONDS = 25
LEGACY_CONFIDENCE_MAP = {"high": 0.9, "medium": 0.6, "low": 0.25}


def load_env_file(env_path: Path):
    """Load KEY=VALUE pairs from a local .env file into os.environ.

    Existing environment variables are not overridden.
    """
    if not env_path.exists():
        return

    for raw_line in env_path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue

        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip()
        if not key:
            continue

        if ((value.startswith('"') and value.endswith('"'))
                or (value.startswith("'") and value.endswith("'"))):
            value = value[1:-1]

        if key not in os.environ:
            os.environ[key] = value

# ── Prompts ───────────────────────────────────────────────────────────────────

SYSTEM_INSTRUCTION = """\
You are an expert research assistant specializing in academic economics papers.
Your task is to determine whether a given paper reports that the study itself
was pre-registered (i.e., the authors registered a pre-analysis plan or
pre-registration BEFORE conducting the study described in the paper).

COMMON REGISTRIES IN ECONOMICS (count as pre-registration if the paper's OWN
study is registered there):
- AEA RCT Registry (socialscienceregistry.org)
- AsPredicted (aspredicted.org)
- OSF / Open Science Framework (osf.io)
- EGAP (egap.org)
- ClinicalTrials.gov (for RCTs)
- ISRCTN, AEARCTR, or similar trial registries
- Any pre-analysis plan (PAP) filed before data collection

IMPORTANT DISTINCTIONS:
- A paper that CITES another pre-registered study is NOT itself pre-registered.
- A paper that DISCUSSES pre-registration as a methodology is NOT itself pre-registered.
- A paper that mentions "registered report" for its OWN submission IS pre-registered.
- A paper that says "we pre-registered our hypotheses at [URL]" IS pre-registered.
- A paper that says "our pre-analysis plan is available at..." IS pre-registered.
- If a provided external link is a real registry page for the same paper and the
  registry title clearly matches the paper, that is positive evidence even when
  the PDF uses brief wording or does not repeat the exact URL in the body text.
- If the registry evidence shows a different title, different authors, or weak
  match quality, treat that as meaningful negative evidence that the link may
  belong to another study.
- SPECIAL RULE FOR BLIND ASPREDICTED LINKS: If the registry URL contains "aspredicted.org/blind.php?x=", 
  treat this as definitive evidence of pre-registration REGARDLESS of author overlap or title match, 
  since blind registrations are anonymous by design and zero author overlap is expected.
- Look carefully in FOOTNOTES, DATA sections, APPENDICES, and ACKNOWLEDGEMENTS,
  not just the abstract — pre-registration disclosures are often in footnotes.
- The paper text provided may be sampled from BOTH the beginning AND the end of the
  PDF to maximise coverage of footnotes and appendices.

Always respond with ONLY a JSON object (no markdown fences, no extra text):
{
  "prereg": true or false,
  "confidence": number between 0.00 and 1.00,
  "evidence": "brief quote or description of the evidence (max 150 chars)",
  "registry_url": "URL if found, else null",
  "reasoning": "1-2 sentence explanation of your decision"
}"""

PROMPT_A = """\
Paper filename: {filename}
Journal: {journal}
Keywords that triggered detection: {keywords}

Our automated scanner flagged this paper because it found the keyword(s) above
somewhere in the full PDF. The text below is sampled from the BEGINNING and END
of the paper (to cover abstract, footnotes, data sections, and appendices).
Search carefully — the pre-registration statement is often in a footnote on the
first data/methods page, or at the end of the paper in an appendix or
acknowledgements section.

Determine whether THIS paper reports its OWN pre-registration.

--- PAPER TEXT (beginning + end sample) ---
{text}"""

PROMPT_C = """\
Paper filename: {filename}
Journal: {journal}

Our automated pipeline found these pre-registration links associated with
this paper:
{links_section}

Registry evidence gathered for the best candidate link:
{registry_evidence}

The text below is sampled from the paper with extra focus on preregistration-related passages.
Please read the paper text and determine:
1. Does this paper report its OWN pre-registration?
2. Do the links above belong to THIS paper's pre-registration, or are they
   from cited/referenced studies?

Important:
- The links above were surfaced by our registry-search pipeline, so treat them
  as serious candidate matches rather than random URLs.
- A verified paper-specific registry entry can count as preregistration evidence
  even if the paper text itself does not explicitly mention preregistration.
- Absence of an explicit preregistration statement in the paper text is only
  weak negative evidence and should not override a strong direct registry match.
 - If the paper itself explicitly says the study/experiment was pre-registered,
   registered an analysis plan, or filed a pre-analysis plan before data collection,
   count that as preregistration even if the registry metadata is incomplete.
 - Do not count plain data/materials/replication/supplementary repositories as
   preregistration unless the paper text or registry page explicitly indicates
   preregistration, registration, or a pre-analysis plan.
- If the evidence only shows replication materials, data, code, or
  supplementary files, return prereg=false.
- For OSF links, an OSF project/node page is not enough by itself. Treat OSF
  nodes/projects as materials repositories unless the evidence explicitly shows
  preregistration/registration terms.
- SPECIAL RULE FOR BLIND ASPREDICTED LINKS: If any registry URL contains "aspredicted.org/blind.php?x=", 
  treat this as definitive evidence of pre-registration REGARDLESS of author overlap, title match, or other quality metrics, 
  since blind registrations are anonymous by design and zero author overlap is expected.
- But if the registry evidence itself points to a different title, different
  authors, or weak match quality, use that as evidence that the link may belong
  to another study (except for blind AsPredicted links as noted above).

--- PAPER TEXT (beginning + end sample) ---
{text}"""


# ── Batch prompt helpers ───────────────────────────────────────────────────────

def build_paper_section(paper: dict, index: int, text: str) -> str:
    """Build the per-paper section for a batch prompt."""
    section = f"=== PAPER {index} ===\n"
    section += f"Filename: {paper['filename']}\n"
    section += f"Journal: {paper['journal']}\n"

    if paper["group"] == "A":
        section += f"Detection context: Keywords detected: {paper['keywords']}. "
        section += "No external registry links found.\n"
    elif paper["group"] == "C":
        section += (f"Detection context: Our pipeline found these links:\n"
                    f"{paper.get('links_section', '')}")
        section += (f"Registry evidence:\n"
                    f"{paper.get('registry_evidence', '(none)')}\n")
        section += ("Determine if links belong to THIS paper. Treat the registry "
                    "links as serious candidate matches, but also use title/author "
                    "mismatch evidence when the registry appears to belong to a "
                    "different study.\n")

    section += f"--- PAPER TEXT ---\n{text}\n"
    return section


BATCH_PROMPT_TEMPLATE = """\
You will analyze {n} academic papers below. For EACH paper, determine whether \
it reports its OWN pre-registration.

IMPORTANT: Respond with ONLY a JSON ARRAY of exactly {n} objects (no markdown \
fences, no extra text). Each object must be in order matching the paper index:

[
  {{
    "paper_index": 1,
    "prereg": true or false,
    "confidence": number between 0.00 and 1.00,
    "evidence": "brief quote or description (max 150 chars)",
    "registry_url": "URL if found, else null",
    "reasoning": "1-2 sentence explanation"
  }},
  ...
]

{papers_text}"""


# ── Data loading ──────────────────────────────────────────────────────────────

def load_scan(scan_csv: Path):
    result = {}
    with open(scan_csv, newline="", encoding="utf-8") as f:
        for r in csv.DictReader(f):
            fname = Path(r["pdf_path"]).name
            result[fname] = r
    return result


def load_enriched(enriched_csv: Path):
    result = {}
    if not enriched_csv.exists():
        return result
    with open(enriched_csv, newline="", encoding="utf-8") as f:
        for r in csv.DictReader(f):
            fname = Path(r["pdf_path"]).name
            result[fname] = r
    return result


def split_links(raw: str | None) -> list[str]:
    if not raw:
        return []
    out = []
    seen = set()
    for part in str(raw).split(";"):
        text = part.strip().rstrip(".,;:")
        if text and text not in seen:
            seen.add(text)
            out.append(text)
    return out


def unique_preserve(items: list[str]) -> list[str]:
    seen = set()
    out = []
    for item in items:
        if not item or item in seen:
            continue
        seen.add(item)
        out.append(item)
    return out


def normalise_registry_text(text: str) -> str:
    if not text:
        return ""
    cleaned = text.replace("\u200b", "").replace("\u200c", "").replace("\u200d", "").replace("\ufeff", "")
    replacements = [
        (r"https?\s*:\s*/\s*/\s*", lambda m: "https://" if "https" in m.group(0).lower() else "http://"),
        (r"aspredicted\s*\.\s*org", "aspredicted.org"),
        (r"osf\s*\.\s*io", "osf.io"),
        (r"egap\s*\.\s*org", "egap.org"),
        (r"socialscienceregistry\s*\.\s*org", "socialscienceregistry.org"),
        (r"blind\s*\.\s*php", "blind.php"),
        (r"\?\s*x\s*=\s*", "?x="),
        (r"/\s+", "/"),
        (r"\s+/", "/"),
    ]
    for pattern, replacement in replacements:
        cleaned = re.sub(pattern, replacement, cleaned, flags=re.IGNORECASE)
    cleaned = re.sub(r"(?<=\d)\s+(?=\d)", "", cleaned)
    return cleaned


def extract_registry_urls_from_text(text: str) -> list[str]:
    normalised = normalise_registry_text(text)
    found = []
    for pattern in REGISTRY_URL_PATTERNS:
        for match in pattern.findall(normalised):
            found.append(match.rstrip(".,;:)"))
    return unique_preserve(found)


def _snippet_around(text: str, start: int, end: int, radius: int = 180) -> str:
    lo = max(0, start - radius)
    hi = min(len(text), end + radius)
    return " ".join(text[lo:hi].split())


def detect_direct_prereg_signal(text: str, extra_links: list[str] | None = None) -> dict:
    raw = text or ""
    normalised = normalise_registry_text(raw)
    links = unique_preserve(extract_registry_urls_from_text(normalised) + split_links("; ".join(extra_links or [])))

    if not any(re.search(r"osf\.io/[a-z0-9]+", link, flags=re.IGNORECASE) for link in links):
        name_match = re.search(r"\(\s*name\s*:\s*([^)]+?)\s*\)", normalised, flags=re.IGNORECASE)
        if name_match and ("open science framework" in normalised.lower() or "osf.io/" in normalised.lower()):
            resolved = search_osf_registration_by_title(name_match.group(1).strip())
            if resolved:
                links = unique_preserve([resolved] + links)

    blind_links = [link for link in links if "aspredicted.org/blind.php?x=" in link.lower()]
    if blind_links:
        pattern = re.search(r"aspredicted\.org/blind(?:\.php)?\?x=[a-z0-9]+", normalised, flags=re.IGNORECASE)
        snippet = _snippet_around(normalised, pattern.start(), pattern.end()) if pattern else "Blind AsPredicted preregistration link cited in paper text."
        return {
            "matched": True,
            "rule": "blind_aspredicted_text_link",
            "evidence": snippet[:220],
            "registry_url": blind_links[0],
        }

    for pattern in DIRECT_PREREG_PATTERNS:
        match = pattern.search(normalised)
        if not match:
            continue
        return {
            "matched": True,
            "rule": "explicit_text_prereg_statement",
            "evidence": _snippet_around(normalised, match.start(), match.end())[:220],
            "registry_url": links[0] if links else "",
        }

    return {"matched": False, "rule": "", "evidence": "", "registry_url": links[0] if links else ""}


@lru_cache(maxsize=2048)
def _osf_api_record(url: str) -> dict:
    m = re.search(r"osf\.io/(?:preprints/osf/)?([a-z0-9]+)", url, re.IGNORECASE)
    if not m:
        return {}
    node_id = m.group(1).lower()
    for endpoint in ("registrations", "nodes", "preprints"):
        try:
            r = requests.get(
                f"https://api.osf.io/v2/{endpoint}/{node_id}/",
                headers={"User-Agent": "ercautomation/1.0"},
                timeout=15,
            )
            if r.status_code != 200:
                continue
            data = r.json().get("data", {})
            attrs = data.get("attributes", {})
            return {
                "osf_endpoint": endpoint,
                "osf_type": data.get("type", "") or "",
                "osf_category": attrs.get("category", "") or "",
                "osf_title": attrs.get("title", "") or "",
                "osf_description": attrs.get("description", "") or "",
            }
        except Exception:
            continue
    return {}


@lru_cache(maxsize=512)
def search_osf_registration_by_title(title: str) -> str:
    query = (title or "").strip()
    if not query:
        return ""
    try:
        r = requests.get(
            "https://api.osf.io/v2/registrations/",
            params={"filter[title]": query[:150], "page[size]": 5},
            headers={"User-Agent": "ercautomation/1.0"},
            timeout=20,
        )
        if r.status_code != 200:
            return ""
        items = r.json().get("data", []) or []
        query_lower = query.lower()
        for item in items:
            attrs = item.get("attributes", {})
            candidate = (attrs.get("title") or "").strip()
            if candidate.lower() == query_lower and item.get("id"):
                return f"https://osf.io/{item['id']}/"
        for item in items:
            attrs = item.get("attributes", {})
            candidate = (attrs.get("title") or "").strip().lower()
            if query_lower in candidate and item.get("id"):
                return f"https://osf.io/{item['id']}/"
    except Exception:
        return ""
    return ""


def _contains_any(text: str, phrases: tuple[str, ...]) -> bool:
    lower = (text or "").lower()
    return any(p in lower for p in phrases)


@lru_cache(maxsize=2048)
def _fetch_registry_evidence(url: str, paper_title: str, paper_doi: str) -> dict:
    try:
        from find_prereg_links import (
            author_overlap,
            crossref_authors_by_doi,
            crossref_authors_by_title,
            validate_link_quality,
        )
    except Exception:
        return {}

    evidence = {}
    lq = validate_link_quality(url, paper_title or "", paper_doi or "")
    if lq.get("quality"):
        evidence["best_link_quality"] = lq.get("quality")
    if lq.get("registry_page_title"):
        evidence["best_link_title"] = lq.get("registry_page_title")
    if lq.get("sim") not in (None, "", "N/A"):
        evidence["best_link_sim"] = lq.get("sim")

    page_text = lq.get("page_text") or ""
    evidence["page_has_prereg_terms"] = "1" if _contains_any(page_text, PREREG_TERMS) else "0"
    evidence["page_has_materials_terms"] = "1" if _contains_any(page_text, MATERIALS_TERMS) else "0"
    if "osf.io" in (url or "").lower():
        osf = _osf_api_record(url)
        endpoint = osf.get("osf_endpoint", "")
        category = osf.get("osf_category", "")
        if endpoint or category:
            evidence["osf_object_type"] = f"{endpoint}:{category}".strip(":")
        desc = osf.get("osf_description", "") or ""
        if desc:
            if _contains_any(desc, PREREG_TERMS):
                evidence["page_has_prereg_terms"] = "1"
            if _contains_any(desc, MATERIALS_TERMS):
                evidence["page_has_materials_terms"] = "1"
    if page_text and (paper_title or paper_doi):
        paper_authors = crossref_authors_by_doi(paper_doi) if paper_doi else []
        if not paper_authors and paper_title:
            paper_authors, _ = crossref_authors_by_title(paper_title)
        if paper_authors:
            _, detail = author_overlap(paper_authors, page_text, url)
            if detail:
                evidence["author_match"] = detail
    return evidence


def _author_match_ratio(detail: str) -> float:
    text = (detail or "").strip()
    m = re.match(r"(\d+)\s*/\s*(\d+)", text)
    if not m:
        return -1.0
    denom = int(m.group(2))
    if denom == 0:
        return -1.0
    return int(m.group(1)) / denom


def _link_quality_score(quality: str) -> int:
    scores = {
        "DOI_CONFIRMED": 6,
        "VERIFIED": 5,
        "AUTHOR_CONFIRMED": 4,
        "UNCERTAIN": 3,
        "NO_TITLE": 2,
        "TITLE_MISMATCH": 1,
        "UNREACHABLE": 0,
    }
    quality = (quality or "").strip()
    if quality.startswith("AI_LINK_"):
        return -1
    return scores.get(quality, -1)


def _merge_link_evidence(link: str, title_guess: str, doi_guess: str, enriched_row: dict, include_stale: bool) -> dict:
    evidence = {
        "best_link_quality": "",
        "best_link_title": "",
        "best_link_sim": "",
        "author_match": "",
        "ai_link_check": "",
        "ai_link_reasoning": "",
        "osf_object_type": "",
        "page_has_prereg_terms": "",
        "page_has_materials_terms": "",
    }
    if include_stale:
        evidence.update({
            "best_link_quality": (enriched_row.get("best_link_quality") or "").strip(),
            "best_link_title": (enriched_row.get("best_link_title") or "").strip(),
            "best_link_sim": (enriched_row.get("best_link_sim") or "").strip(),
            "author_match": (enriched_row.get("author_match") or "").strip(),
            "ai_link_check": (enriched_row.get("ai_link_check") or "").strip(),
            "ai_link_reasoning": (enriched_row.get("ai_link_reasoning") or "").strip(),
            "osf_object_type": (enriched_row.get("osf_object_type") or "").strip(),
            "page_has_prereg_terms": (enriched_row.get("page_has_prereg_terms") or "").strip(),
            "page_has_materials_terms": (enriched_row.get("page_has_materials_terms") or "").strip(),
        })

    fresh = _fetch_registry_evidence(link, title_guess, doi_guess)
    for key, value in fresh.items():
        if value not in (None, ""):
            evidence[key] = str(value).strip()

    author_ratio = _author_match_ratio(evidence.get("author_match", ""))
    if (
        author_ratio >= 0.5
        and evidence.get("best_link_quality") in {"UNCERTAIN", "TITLE_MISMATCH", "NO_TITLE", ""}
    ):
        evidence["best_link_quality"] = "AUTHOR_CONFIRMED"
    return evidence


def best_registry_evidence(scan_row: dict, enriched_row: dict) -> tuple[str, dict]:
    links = split_links(scan_row.get("auto_link_prereg")) + split_links(enriched_row.get("all_found_links"))
    links = unique_preserve(links)
    if not links:
        return "", {}

    title_guess = (enriched_row.get("title_guess") or "").strip()
    doi_guess = (enriched_row.get("doi_from_pdf") or "").strip()

    best_link = ""
    best_evidence = {}
    best_score = None
    for idx, link in enumerate(links):
        evidence = _merge_link_evidence(
            link=link,
            title_guess=title_guess,
            doi_guess=doi_guess,
            enriched_row=enriched_row,
            include_stale=(idx == 0),
        )
        score = (
            1 if "aspredicted.org/blind.php?x=" in link.lower() else 0,
            _link_quality_score(evidence.get("best_link_quality", "")),
            _author_match_ratio(evidence.get("author_match", "")),
            1 if evidence.get("page_has_prereg_terms") == "1" else 0,
            1 if evidence.get("osf_object_type", "").startswith("registrations") else 0,
            len(link),
            -idx,
        )
        if best_score is None or score > best_score:
            best_link = link
            best_evidence = evidence
            best_score = score
    return best_link, best_evidence


def build_registry_evidence_section(scan_row: dict, enriched_row: dict) -> str:
    links = unique_preserve(split_links(scan_row.get("auto_link_prereg")) + split_links(enriched_row.get("all_found_links")))
    if not links:
        return "(none)"

    title_guess = (enriched_row.get("title_guess") or "").strip()
    doi_guess = (enriched_row.get("doi_from_pdf") or "").strip()
    best_link, evidence = best_registry_evidence(scan_row, enriched_row)
    if not best_link:
        return "(none)"

    if evidence.get("best_link_quality") and not evidence["best_link_quality"].startswith("AI_LINK_"):
        if evidence.get("ai_link_check", "").startswith("rejected"):
            evidence["ai_link_check"] = ""
            evidence["ai_link_reasoning"] = ""
    if _author_match_ratio(evidence.get("author_match", "")) > 0:
        if evidence.get("ai_link_check", "").startswith("rejected"):
            evidence["ai_link_check"] = ""
            evidence["ai_link_reasoning"] = ""

    lines = [f"  Best candidate link: {best_link}"]
    if evidence["best_link_quality"]:
        lines.append(f"  Link quality: {evidence['best_link_quality']}")
    if evidence["best_link_title"]:
        lines.append(f"  Registry page title: {evidence['best_link_title']}")
    if evidence["best_link_sim"]:
        lines.append(f"  Title similarity: {evidence['best_link_sim']}")
    if evidence["author_match"]:
        lines.append(f"  Author overlap: {evidence['author_match']}")
    if evidence["osf_object_type"]:
        lines.append(f"  OSF object type: {evidence['osf_object_type']}")
    if evidence["page_has_prereg_terms"]:
        lines.append(f"  Page has prereg terms: {evidence['page_has_prereg_terms']}")
    if evidence["page_has_materials_terms"]:
        lines.append(f"  Page has materials terms: {evidence['page_has_materials_terms']}")
    if evidence["ai_link_check"]:
        lines.append(f"  Link-specific AI check: {evidence['ai_link_check']}")
    if evidence["ai_link_reasoning"]:
        lines.append(f"  Link-specific AI reasoning: {evidence['ai_link_reasoning']}")
    if title_guess:
        lines.append(f"  Paper title used for comparison: {title_guess}")
    if doi_guess:
        lines.append(f"  Paper DOI used for comparison: {doi_guess}")
    return "\n".join(lines)


def load_done(results_csv: Path) -> set:
    """Load already-processed filenames from results CSV."""
    done = set()
    if results_csv.exists():
        with open(results_csv, newline="", encoding="utf-8") as f:
            for r in csv.DictReader(f):
                done.add(r["filename"])
    return done


def extract_text(pdf_path: str, max_chars: int) -> str:
    """Extract full PDF text, or a beginning+end sample if max_chars > 0.

    When max_chars=0 (default) the entire document is returned so the LLM
    sees every page. When max_chars>0 the first 65% of the budget comes from
    the start of the document and the last 35% from the end, preserving both
    the abstract/intro/footnotes and the appendix/acknowledgements where
    pre-registration disclosures commonly appear.
    """
    try:
        doc = fitz.open(pdf_path)
        all_pages = [page.get_text("text") for page in doc]
        doc.close()
        full_text = "\n".join(all_pages)

        if max_chars == 0 or len(full_text) <= max_chars:
            return full_text

        head_budget = int(max_chars * 0.65)
        tail_budget = max_chars - head_budget

        head = full_text[:head_budget]
        tail = full_text[-tail_budget:]

        return (
            head
            + "\n\n[... middle section omitted for length ...\n"
            + " Note: text continues from end of paper below ...]\n\n"
            + tail
        )
    except Exception as e:
        return f"[ERROR extracting text: {e}]"


def extract_text_for_prereg_review(pdf_path: str, max_chars: int) -> str:
    full_text = extract_text(pdf_path, 0)
    if full_text.startswith("[ERROR"):
        return full_text
    if max_chars == 0 or len(full_text) <= max_chars:
        return full_text

    head_budget = int(max_chars * 0.30)
    tail_budget = int(max_chars * 0.20)
    focus_budget = max_chars - head_budget - tail_budget

    head = full_text[:head_budget]
    tail = full_text[-tail_budget:] if tail_budget > 0 else ""

    snippets = []
    seen_spans = []
    for pattern in FOCUSED_TEXT_PATTERNS:
        for match in pattern.finditer(full_text):
            start = max(0, match.start() - 220)
            end = min(len(full_text), match.end() + 420)
            if any(not (end < s or start > e) for s, e in seen_spans):
                continue
            seen_spans.append((start, end))
            snippets.append(" ".join(full_text[start:end].split()))
            if len(snippets) >= 8:
                break
        if len(snippets) >= 8:
            break

    focus_text = "\n\n".join(snippets)
    if len(focus_text) > focus_budget:
        focus_text = focus_text[:focus_budget]

    parts = [head]
    if focus_text:
        parts.append("[... preregistration-focused excerpts from the full PDF ...]\n" + focus_text)
    if tail:
        parts.append("[... ending excerpt ...]\n" + tail)
    return "\n\n".join(part for part in parts if part)


# ── Pipeline candidate assembly ───────────────────────────────────────────────

def build_groups(scan, enriched, requested_groups, done_filenames=None):
    papers = []
    already_in = set()
    done_already = done_filenames or set()

    if "A" in requested_groups:
        for fname, s in scan.items():
            if fname in done_already:
                continue
            if not s.get("pdf_path"):
                continue
            e = enriched.get(fname, {})
            has_pdf_link = bool((s.get("auto_link_prereg") or "").strip())
            has_enriched_links = bool((e.get("all_found_links") or "").strip())
            if has_pdf_link or has_enriched_links:
                continue
            papers.append({
                "filename": fname,
                "pdf_path": s["pdf_path"],
                "journal": s.get("journal", ""),
                "keywords": s.get("triggered_keywords", ""),
                "group": "A",
                "scan_row": s,
                "enriched_row": e,
            })
            already_in.add(fname)

    if "C" in requested_groups:
        for fname in sorted(set(list(scan.keys()) + list(enriched.keys()))):
            if fname in done_already or fname in already_in:
                continue
            s = scan.get(fname, {})
            e = enriched.get(fname, {})
            pdf_path = s.get("pdf_path") or e.get("pdf_path", "")
            if not pdf_path:
                continue
            pdf_link = (s.get("auto_link_prereg") or "").strip()
            enrich_links = (e.get("all_found_links") or "").strip()
            if not pdf_link and not enrich_links:
                continue
            quality = (e.get("best_link_quality") or "").strip()
            links_section = ""
            if pdf_link:
                links_section += f"  From PDF text: {pdf_link}\n"
            if enrich_links:
                links_section += f"  From enrichment ({quality}): {enrich_links}\n"
            papers.append({
                "filename": fname,
                "pdf_path": pdf_path,
                "journal": s.get("journal", ""),
                "keywords": s.get("triggered_keywords", ""),
                "links_section": links_section,
                "registry_evidence": build_registry_evidence_section(s, e),
                "group": "C",
                "scan_row": s,
                "enriched_row": e,
            })

    papers.sort(key=lambda p: (p["group"], p["filename"]))
    return papers


def deterministic_pipeline_verdict(paper: dict, max_chars: int) -> dict | None:
    scan_row = paper.get("scan_row", {})
    enriched_row = paper.get("enriched_row", {})
    text = extract_text_for_prereg_review(paper["pdf_path"], max_chars)
    if text.startswith("[ERROR"):
        return {
            "group": paper["group"],
            "filename": paper["filename"],
            "journal": paper["journal"],
            "llm_prereg": None,
            "llm_confidence": "error",
            "llm_evidence": "",
            "llm_registry_url": "",
            "llm_reasoning": text[:200],
            "input_tokens": 0,
            "output_tokens": 0,
            "llm_model": "deterministic_rule",
        }

    links = split_links(scan_row.get("auto_link_prereg")) + split_links(enriched_row.get("all_found_links"))
    links = unique_preserve(links)
    direct = detect_direct_prereg_signal(text, links)
    best_link, evidence = best_registry_evidence(scan_row, enriched_row)

    if direct.get("matched"):
        return {
            "group": paper["group"],
            "filename": paper["filename"],
            "journal": paper["journal"],
            "llm_prereg": True,
            "llm_confidence": "0.98",
            "llm_evidence": (direct.get("evidence") or "Paper text explicitly reports preregistration.")[:150],
            "llm_registry_url": best_link or direct.get("registry_url") or "",
            "llm_reasoning": (
                "The paper text contains a direct own-study preregistration disclosure, so this "
                "counts as preregistered even if registry metadata is incomplete or anonymous."
            ),
            "input_tokens": 0,
            "output_tokens": 0,
            "llm_model": "deterministic_rule",
        }

    quality = (evidence.get("best_link_quality") or "").strip()
    strong_registry = quality in {"VERIFIED", "DOI_CONFIRMED"} or (
        quality == "AUTHOR_CONFIRMED"
        and (
            evidence.get("page_has_prereg_terms") == "1"
            or (evidence.get("osf_object_type") or "").startswith("registrations")
        )
    )
    if strong_registry and best_link:
        return {
            "group": paper["group"],
            "filename": paper["filename"],
            "journal": paper["journal"],
            "llm_prereg": True,
            "llm_confidence": "0.93" if quality in {"VERIFIED", "DOI_CONFIRMED"} else "0.80",
            "llm_evidence": (
                f"Registry evidence is {quality}"
                + (
                    f" with author overlap {evidence.get('author_match')}"
                    if evidence.get("author_match") else ""
                )
            )[:150],
            "llm_registry_url": best_link,
            "llm_reasoning": (
                "The strongest available registry record matches this paper closely enough to count "
                "as preregistration evidence, and weak text absence should not override it."
            ),
            "input_tokens": 0,
            "output_tokens": 0,
            "llm_model": "deterministic_rule",
        }

    return None


# ── Direct provider call helpers ─────────────────────────────────────────────

MAX_RETRIES = 5


class RateLimitError(RuntimeError):
    pass


def is_rate_limited_message(message: str) -> bool:
    msg = (message or "").lower()
    return (
        "429" in msg
        or "resourceexhausted" in msg
        or "rate limit" in msg
        or "quota" in msg
        or "too many requests" in msg
    )


def normalize_confidence_value(value) -> str:
    """Normalize model confidence to a 0.00-1.00 string, keeping legacy support."""
    if value is None:
        return ""
    if isinstance(value, bool):
        return ""
    if isinstance(value, (int, float)):
        return f"{max(0.0, min(1.0, float(value))):.2f}"

    text = str(value).strip()
    if not text:
        return ""
    if text.lower() == "error":
        return "error"

    legacy = LEGACY_CONFIDENCE_MAP.get(text.lower())
    if legacy is not None:
        return f"{legacy:.2f}"

    try:
        return f"{max(0.0, min(1.0, float(text))):.2f}"
    except ValueError:
        return ""


def is_no_endpoint_message(message: str) -> bool:
    msg = (message or "").lower()
    return "no endpoints found" in msg or ("http 404" in msg and "endpoint" in msg)


def build_single_prompt(paper: dict, max_chars: int, _preextracted: str = None) -> str:
    text = _preextracted if _preextracted is not None else extract_text_for_prereg_review(paper["pdf_path"], max_chars)
    if paper["group"] == "A":
        return PROMPT_A.format(
            filename=paper["filename"], journal=paper["journal"],
            keywords=paper["keywords"], text=text)
    return PROMPT_C.format(
        filename=paper["filename"], journal=paper["journal"],
        links_section=paper.get("links_section", ""),
        registry_evidence=paper.get("registry_evidence", "(none)"),
        text=text)


def discover_openrouter_free_models(api_key: str, limit: int = 16) -> list[str]:
    headers = {
        "Authorization": f"Bearer {api_key}",
        "HTTP-Referer": "https://local.erc-automation",
        "X-Title": "ERC PreReg Verification",
    }
    try:
        response = requests.get(OPENROUTER_MODELS_URL, headers=headers, timeout=30)
        response.raise_for_status()
        data = response.json().get("data", [])
    except Exception:
        return DEFAULT_OPENROUTER_FREE_MODELS[:]

    free_models = []
    for m in data:
        model_id = (m.get("id") or "").strip()
        if not model_id:
            continue
        pricing = m.get("pricing") or {}
        prompt_price = str(pricing.get("prompt", "")).strip()
        completion_price = str(pricing.get("completion", "")).strip()
        is_free = model_id.endswith(":free") or (
            prompt_price in {"0", "0.0", "0.00"}
            and completion_price in {"0", "0.0", "0.00"}
        )
        if not is_free:
            continue
        context_len = int(m.get("context_length") or 0)
        free_models.append((context_len, model_id))

    free_models.sort(key=lambda x: x[0], reverse=True)
    discovered = [mid for _, mid in free_models[:limit]]
    if not discovered:
        return DEFAULT_OPENROUTER_FREE_MODELS[:]

    merged = []
    seen = set()
    for model in DEFAULT_OPENROUTER_FREE_MODELS + discovered:
        if model and model not in seen:
            merged.append(model)
            seen.add(model)
    return merged


def extract_openrouter_text(response_json: dict) -> str:
    choices = response_json.get("choices") or []
    if not choices:
        return ""
    message = (choices[0].get("message") or {})
    content = message.get("content")
    if isinstance(content, str):
        return content
    if isinstance(content, list):
        chunks = []
        for item in content:
            if isinstance(item, dict) and item.get("type") == "text":
                chunks.append(item.get("text", ""))
        return "\n".join(chunks)
    return ""


def clean_llm_content(content: str) -> str:
    """Strip thinking blocks (<think>…</think>) and markdown fences from LLM output."""
    import re
    # Remove <think>…</think> blocks (qwen3 / deepseek-r1 thinking models)
    content = re.sub(r"<think>.*?</think>", "", content, flags=re.DOTALL)
    content = content.strip()
    # Remove markdown code fences
    if content.startswith("```"):
        lines = content.split("\n")
        lines = [l for l in lines if not l.startswith("```")]
        content = "\n".join(lines).strip()
    return content


def openrouter_chat_completion(
    api_key: str,
    model: str,
    prompt: str,
    max_output_tokens: int,
    response_format_type: str | None = None,
):
    payload = {
        "model": model,
        "messages": [
            {"role": "system", "content": SYSTEM_INSTRUCTION},
            {"role": "user", "content": prompt},
        ],
        "temperature": 0.1,
        "max_tokens": max_output_tokens,
    }
    if response_format_type:
        payload["response_format"] = {"type": response_format_type}
    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json",
        "HTTP-Referer": "https://local.erc-automation",
        "X-Title": "ERC PreReg Verification",
    }

    resp = requests.post(OPENROUTER_CHAT_URL, headers=headers, json=payload, timeout=180)
    if resp.status_code == 429:
        raise RateLimitError("OpenRouter rate-limited (HTTP 429)")
    if resp.status_code >= 500:
        raise RuntimeError(f"OpenRouter server error HTTP {resp.status_code}")
    if resp.status_code >= 400:
        body = resp.text[:240]
        if is_rate_limited_message(body):
            raise RateLimitError(body)
        raise RuntimeError(f"OpenRouter HTTP {resp.status_code}: {body}")

    data = resp.json()
    text = extract_openrouter_text(data).strip()
    usage = data.get("usage") or {}
    input_tokens = int(usage.get("prompt_tokens") or 0)
    output_tokens = int(usage.get("completion_tokens") or 0)
    return text, input_tokens, output_tokens


def call_native_provider_single(client, paper: dict, max_chars: int) -> dict:
    """Send one paper to the direct provider client and return a parsed verdict."""
    prompt = build_single_prompt(paper, max_chars)

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            response = client.models.generate_content(
                model=client._model_name,
                contents=prompt,
                config=types.GenerateContentConfig(
                    system_instruction=SYSTEM_INSTRUCTION,
                    temperature=0.1,
                    max_output_tokens=8192,
                    response_mime_type="application/json",
                    thinking=types.ThinkingConfig(
                        thinkingBudget=1024,
                    ),
                ),
            )
            content = clean_llm_content(response.text.strip())

            verdict = json.loads(content)
            usage = response.usage_metadata
            input_tokens = usage.prompt_token_count if usage else 0
            output_tokens = usage.candidates_token_count if usage else 0
            break  # success
        except json.JSONDecodeError:
            verdict = {
                "prereg": None, "confidence": "error",
                "evidence": content[:150] if content else "",
                "reasoning": "JSON parse failed",
            }
            usage = getattr(response, "usage_metadata", None)
            input_tokens = usage.prompt_token_count if usage else 0
            output_tokens = 0
            break  # don't retry JSON errors
        except Exception as e:
            err_str = str(e)
            is_rate_limit = is_rate_limited_message(err_str)
            if is_rate_limit and attempt < MAX_RETRIES:
                wait = min(30 * attempt, 120)  # 30s, 60s, 90s, 120s
                print(f"\n    Rate limited (attempt {attempt}/{MAX_RETRIES}), waiting {wait}s...", flush=True)
                time.sleep(wait)
                continue
            verdict = {
                "prereg": None, "confidence": "error",
                "evidence": "", "reasoning": err_str[:200],
            }
            input_tokens = 0
            output_tokens = 0
            break

    return {
        "group": paper["group"],
        "filename": paper["filename"],
        "journal": paper["journal"],
        "llm_prereg": verdict.get("prereg"),
        "llm_confidence": normalize_confidence_value(verdict.get("confidence", "")),
        "llm_evidence": verdict.get("evidence", ""),
        "llm_registry_url": verdict.get("registry_url") or "",
        "llm_reasoning": verdict.get("reasoning", ""),
        "input_tokens": input_tokens,
        "output_tokens": output_tokens,
    }


def call_native_provider_batch(client, papers: list, max_chars: int) -> list:
    """Send a batch of papers through the direct provider client and parse results."""
    n = len(papers)
    # Build combined prompt
    paper_sections = []
    for i, paper in enumerate(papers, 1):
        text = extract_text_for_prereg_review(paper["pdf_path"], max_chars)
        section = build_paper_section(paper, i, text)
        paper_sections.append(section)

    papers_text = "\n\n".join(paper_sections)
    prompt = BATCH_PROMPT_TEMPLATE.format(n=n, papers_text=papers_text)

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            response = client.models.generate_content(
                model=client._model_name,
                contents=prompt,
                config=types.GenerateContentConfig(
                    system_instruction=SYSTEM_INSTRUCTION,
                    temperature=0.1,
                    max_output_tokens=max(8192, 2000 * n),
                    response_mime_type="application/json",
                    thinking=types.ThinkingConfig(
                        thinkingBudget=min(2048, 512 * n),
                    ),
                ),
            )

            # Check for truncation
            fr = (response.candidates[0].finish_reason
                  if response.candidates else None)
            if fr and str(fr) != "FinishReason.STOP" and str(fr) != "STOP":
                print(f"\n    WARNING: finish_reason={fr} (may be truncated)")

            content = clean_llm_content(response.text.strip())

            verdicts = json.loads(content)
            if not isinstance(verdicts, list):
                verdicts = [verdicts]

            usage = response.usage_metadata
            input_tokens = usage.prompt_token_count if usage else 0
            output_tokens = usage.candidates_token_count if usage else 0

            results = []
            for j, paper in enumerate(papers):
                v = verdicts[j] if j < len(verdicts) else {
                    "prereg": None, "confidence": "error",
                    "evidence": "", "reasoning": "Missing from batch response"
                }
                tok_in = input_tokens // n
                tok_out = output_tokens // n
                results.append({
                    "group": paper["group"],
                    "filename": paper["filename"],
                    "journal": paper["journal"],
                    "llm_prereg": v.get("prereg"),
                    "llm_confidence": normalize_confidence_value(v.get("confidence", "")),
                    "llm_evidence": v.get("evidence", ""),
                    "llm_registry_url": v.get("registry_url") or "",
                    "llm_reasoning": v.get("reasoning", ""),
                    "input_tokens": tok_in,
                    "output_tokens": tok_out,
                })
            return results

        except json.JSONDecodeError as jde:
            print(f"\n    JSON PARSE ERROR: {jde}")
            print(f"    Response length: {len(content)} chars")
            print(f"    First 300 chars: {content[:300]}")
            print(f"    Last  200 chars: {content[-200:] if len(content) > 200 else content}")
            results = []
            for paper in papers:
                results.append({
                    "group": paper["group"],
                    "filename": paper["filename"],
                    "journal": paper["journal"],
                    "llm_prereg": None,
                    "llm_confidence": "error",
                    "llm_evidence": (content[:150] if content else ""),
                    "llm_registry_url": "",
                    "llm_reasoning": f"JSON parse failed: {str(jde)[:100]}",
                    "input_tokens": 0,
                    "output_tokens": 0,
                })
            return results

        except Exception as e:
            err_str = str(e)
            is_rate_limit = is_rate_limited_message(err_str)
            if is_rate_limit and attempt < MAX_RETRIES:
                wait = min(60 * attempt, 300)
                print(f"\n    Rate limited (attempt {attempt}/{MAX_RETRIES}), "
                      f"waiting {wait}s...", flush=True)
                time.sleep(wait)
                continue
            results = []
            for paper in papers:
                results.append({
                    "group": paper["group"],
                    "filename": paper["filename"],
                    "journal": paper["journal"],
                    "llm_prereg": None,
                    "llm_confidence": "error",
                    "llm_evidence": "",
                    "llm_registry_url": "",
                    "llm_reasoning": err_str[:200],
                    "input_tokens": 0,
                    "output_tokens": 0,
                })
            return results


def _make_error_result(paper: dict, reason: str) -> dict:
    """Return a non-retryable error result for a paper (e.g. PDF extraction failure)."""
    return {
        "group": paper["group"],
        "filename": paper["filename"],
        "journal": paper["journal"],
        "llm_prereg": None,
        "llm_confidence": "error",
        "llm_evidence": "",
        "llm_registry_url": "",
        "llm_reasoning": reason[:200],
        "input_tokens": 0,
        "output_tokens": 0,
    }


def call_openrouter_batch_once(api_key: str, model: str, papers: list, max_chars: int) -> list:
    # Pre-screen: extract text for every paper and flag extraction failures upfront
    # so they never reach the LLM and cannot produce a spurious False verdict.
    texts = [extract_text_for_prereg_review(p["pdf_path"], max_chars) for p in papers]
    bad_idx = {i for i, t in enumerate(texts) if t.startswith("[ERROR")}
    good_indices = [i for i in range(len(papers)) if i not in bad_idx]

    if not good_indices:
        # Every paper failed extraction – return errors without an LLM call
        return [_make_error_result(papers[i], texts[i][:200]) for i in range(len(papers))]

    good_papers = [papers[i] for i in good_indices]
    n = len(good_papers)
    paper_sections = []
    for section_i, (orig_i, paper) in enumerate(zip(good_indices, good_papers), 1):
        section = build_paper_section(paper, section_i, texts[orig_i])
        paper_sections.append(section)

    papers_text = "\n\n".join(paper_sections)
    prompt = BATCH_PROMPT_TEMPLATE.format(n=n, papers_text=papers_text)

    content = ""
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            content, input_tokens, output_tokens = openrouter_chat_completion(
                api_key=api_key,
                model=model,
                prompt=prompt,
                max_output_tokens=1_500,
                response_format_type=None,
            )

            content = clean_llm_content(content)

            parsed = json.loads(content)
            if isinstance(parsed, list):
                verdicts = parsed
            elif isinstance(parsed, dict):
                if isinstance(parsed.get("verdicts"), list):
                    verdicts = parsed["verdicts"]
                elif isinstance(parsed.get("results"), list):
                    verdicts = parsed["results"]
                elif isinstance(parsed.get("papers"), list):
                    verdicts = parsed["papers"]
                else:
                    verdicts = [parsed]
            else:
                verdicts = []

            good_results = []
            for j, paper in enumerate(good_papers):
                v = verdicts[j] if j < len(verdicts) else {
                    "prereg": None, "confidence": "error",
                    "evidence": "", "reasoning": "Missing from batch response"
                }
                tok_in = input_tokens // n
                tok_out = output_tokens // n
                good_results.append({
                    "group": paper["group"],
                    "filename": paper["filename"],
                    "journal": paper["journal"],
                    "llm_prereg": v.get("prereg"),
                    "llm_confidence": normalize_confidence_value(v.get("confidence", "")),
                    "llm_evidence": v.get("evidence", ""),
                    "llm_registry_url": v.get("registry_url") or "",
                    "llm_reasoning": v.get("reasoning", ""),
                    "input_tokens": tok_in,
                    "output_tokens": tok_out,
                })

            # Merge LLM results back with pre-error results, preserving original order
            if bad_idx:
                good_iter = iter(good_results)
                final = []
                for i in range(len(papers)):
                    if i in bad_idx:
                        final.append(_make_error_result(papers[i], texts[i][:200]))
                    else:
                        final.append(next(good_iter))
                return final
            return good_results

        except json.JSONDecodeError as jde:
            results = []
            for paper in papers:
                results.append({
                    "group": paper["group"],
                    "filename": paper["filename"],
                    "journal": paper["journal"],
                    "llm_prereg": None,
                    "llm_confidence": "error",
                    "llm_evidence": (content[:150] if content else ""),
                    "llm_registry_url": "",
                    "llm_reasoning": f"JSON parse failed: {str(jde)[:100]}",
                    "input_tokens": 0,
                    "output_tokens": 0,
                })
            return results

        except RateLimitError:
            raise


def call_openrouter_single_once(api_key: str, model: str, paper: dict, max_chars: int) -> dict:
    # Guard: pre-extract text so a PDF failure cannot reach the LLM and produce a
    # spurious False verdict (LLM would see an error string and find no evidence).
    _text = extract_text_for_prereg_review(paper["pdf_path"], max_chars)
    if _text.startswith("[ERROR"):
        return _make_error_result(paper, _text[:200])
    prompt = build_single_prompt(paper, max_chars, _preextracted=_text)
    content = ""
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            content, input_tokens, output_tokens = openrouter_chat_completion(
                api_key=api_key,
                model=model,
                prompt=prompt,
                max_output_tokens=1_500,
                response_format_type="json_object",
            )

            content = clean_llm_content(content)

            verdict = json.loads(content)
            return {
                "group": paper["group"],
                "filename": paper["filename"],
                "journal": paper["journal"],
                "llm_prereg": verdict.get("prereg"),
                "llm_confidence": normalize_confidence_value(verdict.get("confidence", "")),
                "llm_evidence": verdict.get("evidence", ""),
                "llm_registry_url": verdict.get("registry_url") or "",
                "llm_reasoning": verdict.get("reasoning", ""),
                "input_tokens": input_tokens,
                "output_tokens": output_tokens,
            }
        except json.JSONDecodeError as jde:
            return {
                "group": paper["group"],
                "filename": paper["filename"],
                "journal": paper["journal"],
                "llm_prereg": None,
                "llm_confidence": "error",
                "llm_evidence": (content[:150] if content else ""),
                "llm_registry_url": "",
                "llm_reasoning": f"JSON parse failed: {str(jde)[:100]}",
                "input_tokens": 0,
                "output_tokens": 0,
            }
        except RateLimitError:
            raise
        except Exception:
            if attempt < MAX_RETRIES:
                wait = min(OPENROUTER_TRANSIENT_WAIT_BASE * attempt, OPENROUTER_TRANSIENT_WAIT_MAX)
                print(f"\n    OpenRouter transient error (single, attempt {attempt}/{MAX_RETRIES}), waiting {wait}s...", flush=True)
                time.sleep(wait)
                continue
            raise


def call_openrouter_single_with_fallback(api_key: str, model_pool: list[str], paper: dict, max_chars: int) -> dict:
    attempts_left = len(model_pool)
    idx = 0
    last_error = None
    while attempts_left > 0 and model_pool:
        idx = idx % len(model_pool)
        model = model_pool[idx]
        try:
            result = call_openrouter_single_once(api_key, model, paper, max_chars)
            result["llm_reasoning"] = (result.get("llm_reasoning") or "").strip()
            return result, model, idx
        except RateLimitError as e:
            last_error = str(e)
            print(f"\n    Single fallback rate-limited: {model} — rotating.")
            if attempts_left > 1:
                print(f"    Cooling down {OPENROUTER_ROTATION_WAIT_SECONDS}s before next model...")
                time.sleep(OPENROUTER_ROTATION_WAIT_SECONDS)
            idx = (idx + 1) % len(model_pool)
            attempts_left -= 1
            continue
        except Exception as e:
            last_error = str(e)
            if is_no_endpoint_message(last_error):
                print(f"\n    Single fallback removing unavailable model: {model}")
                model_pool.pop(idx)
                attempts_left = min(attempts_left - 1, len(model_pool))
                if not model_pool:
                    break
                idx = idx % len(model_pool)
                continue
            print(f"\n    Single fallback failed: {model} ({last_error[:120]}) — rotating.")
            if attempts_left > 1:
                print(f"    Cooling down {OPENROUTER_ROTATION_WAIT_SECONDS}s before next model...")
                time.sleep(OPENROUTER_ROTATION_WAIT_SECONDS)
            idx = (idx + 1) % len(model_pool)
            attempts_left -= 1
            continue

    err = (last_error or "All OpenRouter models failed")[:200]
    return {
        "group": paper["group"],
        "filename": paper["filename"],
        "journal": paper["journal"],
        "llm_prereg": None,
        "llm_confidence": "error",
        "llm_evidence": "",
        "llm_registry_url": "",
        "llm_reasoning": err,
        "input_tokens": 0,
        "output_tokens": 0,
        "_retryable": True,
    }, None, 0


def call_openrouter_batch_with_fallback(api_key: str, model_pool: list[str], papers: list, max_chars: int):
    if not model_pool:
        raise RuntimeError("No OpenRouter models configured")

    attempts_left = len(model_pool)
    idx = 0
    last_error = None
    while attempts_left > 0 and model_pool:
        idx = idx % len(model_pool)
        model = model_pool[idx]
        try:
            results = call_openrouter_batch_once(api_key, model, papers, max_chars)
            missing_idxs = [
                i for i, r in enumerate(results)
                if (r.get("llm_reasoning") or "") == "Missing from batch response"
            ]
            if missing_idxs:
                print(f"\n    Incomplete batch response ({len(missing_idxs)}/{len(results)} missing). Recovering with single-paper calls...")
                for mi in missing_idxs:
                    single_result, single_model, single_idx = call_openrouter_single_with_fallback(
                        api_key=api_key,
                        model_pool=model_pool,
                        paper=papers[mi],
                        max_chars=max_chars,
                    )
                    results[mi] = single_result
                    if single_model:
                        model_pool[:] = model_pool[single_idx:] + model_pool[:single_idx]
            return results, model, idx
        except RateLimitError as e:
            last_error = str(e)
            print(f"\n    Model rate-limited: {model} — rotating to next model.")
            if attempts_left > 1:
                print(f"    Cooling down {OPENROUTER_ROTATION_WAIT_SECONDS}s before next model...")
                time.sleep(OPENROUTER_ROTATION_WAIT_SECONDS)
            idx = (idx + 1) % len(model_pool)
            attempts_left -= 1
            continue
        except Exception as e:
            last_error = str(e)
            if is_no_endpoint_message(last_error):
                print(f"\n    Removing unavailable model: {model}")
                model_pool.pop(idx)
                attempts_left = min(attempts_left - 1, len(model_pool))
                if not model_pool:
                    break
                idx = idx % len(model_pool)
                continue
            print(f"\n    Model failed: {model} ({last_error[:120]}) — rotating.")
            if attempts_left > 1:
                print(f"    Cooling down {OPENROUTER_ROTATION_WAIT_SECONDS}s before next model...")
                time.sleep(OPENROUTER_ROTATION_WAIT_SECONDS)
            idx = (idx + 1) % len(model_pool)
            attempts_left -= 1
            continue

    err = (last_error or "All OpenRouter models failed")[:200]
    results = []
    for paper in papers:
        results.append({
            "group": paper["group"],
            "filename": paper["filename"],
            "journal": paper["journal"],
            "llm_prereg": None,
            "llm_confidence": "error",
            "llm_evidence": "",
            "llm_registry_url": "",
            "llm_reasoning": err,
            "input_tokens": 0,
            "output_tokens": 0,
            "_retryable": True,
        })
    return results, None, 0


# ── Rate limiter ──────────────────────────────────────────────────────────────

class TokenBucket:
    """Simple token-per-minute rate limiter."""

    def __init__(self, tpm: int):
        self.tpm = tpm
        self.tokens_used = 0
        self.window_start = time.time()

    def wait_if_needed(self, estimated_tokens: int):
        now = time.time()
        elapsed = now - self.window_start

        # Reset window every 60 seconds
        if elapsed >= 60:
            self.tokens_used = 0
            self.window_start = now
            return

        # If adding this request would exceed limit, wait
        if self.tokens_used + estimated_tokens > self.tpm:
            wait_time = 60 - elapsed + 1  # wait until window resets + 1s buffer
            print(f"    Rate limit: waiting {wait_time:.0f}s for token window reset...")
            time.sleep(wait_time)
            self.tokens_used = 0
            self.window_start = time.time()

    def record(self, tokens: int):
        self.tokens_used += tokens


# ── CSV output ────────────────────────────────────────────────────────────────

FIELDS = [
    "group", "filename", "journal",
    "llm_prereg", "llm_confidence", "llm_evidence",
    "llm_registry_url", "llm_reasoning",
    "input_tokens", "output_tokens",
    "llm_model",
]


def append_result(result: dict, results_csv: Path):
    """Append one result row to the CSV (creating header if new file)."""
    write_header = not results_csv.exists() or results_csv.stat().st_size == 0
    with open(results_csv, "a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=FIELDS)
        if write_header:
            writer.writeheader()
        writer.writerow(result)


# ── Summary ───────────────────────────────────────────────────────────────────

def print_summary(results_csv: Path):
    if not results_csv.exists():
        return
    results = []
    with open(results_csv, newline="", encoding="utf-8") as f:
        for r in csv.DictReader(f):
            results.append(r)

    total = len(results)
    by_group = {}
    total_tokens = 0
    for r in results:
        g = r["group"]
        if g not in by_group:
            by_group[g] = {"yes": 0, "no": 0, "error": 0}
        prereg = r["llm_prereg"]
        if prereg == "True":
            by_group[g]["yes"] += 1
        elif prereg == "False":
            by_group[g]["no"] += 1
        else:
            by_group[g]["error"] += 1
        total_tokens += int(r.get("input_tokens") or 0) + int(r.get("output_tokens") or 0)

    print(f"\n{'='*60}")
    print(f"RESULTS SUMMARY  ({total} papers processed)")
    print(f"{'='*60}")
    print(f"Total tokens used: {total_tokens:,}")

    for g in sorted(by_group):
        s = by_group[g]
        gt = s["yes"] + s["no"] + s["error"]
        print(f"\n  Group {g} ({gt} papers):")
        print(f"    LLM says pre-registered  : {s['yes']}")
        print(f"    LLM says NOT pre-registered: {s['no']}")
        print(f"    Errors                   : {s['error']}")
        if gt and g == "A":
            print(f"    -> {s['yes']}/{gt} text-only papers confirmed ({s['yes']/gt:.1%})")
        elif gt and g == "C":
            print(f"    -> {s['yes']}/{gt} link-backed candidates confirmed ({s['yes']/gt:.1%})")


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Review pipeline preregistration candidates via LLM API")
    parser.add_argument("--group", action="append", default=[],
                        help="Groups to process: A (text-only/no candidate link), C (link-backed), or all")
    parser.add_argument("--max-chars", type=int, default=DEFAULT_MAX_CHARS,
                        help="Max chars of PDF text to send (0 = full PDF, default 0)")
    parser.add_argument("--provider", choices=["gemini", "openrouter"], default=DEFAULT_PROVIDER,
                        help="LLM provider (default: gemini)")
    parser.add_argument("--model", default=None,
                        help="Primary model override (provider-specific)")
    parser.add_argument("--openrouter-models", default="auto",
                        help="Comma-separated OpenRouter fallback models, or 'auto' for discovered free models")
    parser.add_argument("--tpm", type=int, default=DEFAULT_TPM,
                        help="Tokens per minute limit (default 250000)")
    parser.add_argument("--batch-size", type=int, default=10,
                        help="Papers per API request (default 10)")
    parser.add_argument("--max-requests", type=int, default=None,
                        help="Max API calls per run (default: unlimited)")
    parser.add_argument("--reset", action="store_true",
                        help="Delete existing results and start fresh")
    parser.add_argument("--scan", type=str, default=None,
                        help=f"Path to scan CSV (default: {DEFAULT_SCAN_CSV})")
    parser.add_argument("--enriched", type=str, default=None,
                        help=f"Path to enriched CSV (default: {DEFAULT_ENRICHED_CSV})")
    parser.add_argument("--results-csv", type=str, default=None,
                        help=f"Path to verdict output CSV (default: {DEFAULT_RESULTS_CSV})")
    args = parser.parse_args()

    load_env_file(ROOT / ".env")

    scan_csv = resolve_existing_path(args.scan, DEFAULT_SCAN_CSV, "scan CSV", fallbacks=[FALLBACK_SCAN_CSV])
    enriched_csv = resolve_existing_path(
        args.enriched,
        DEFAULT_ENRICHED_CSV,
        "enriched CSV",
        fallbacks=[FALLBACK_ENRICHED_CSV],
        required=False,
    )
    results_csv = resolve_output_path(args.results_csv, DEFAULT_RESULTS_CSV)

    provider = args.provider.lower()
    selected_model = args.model or (DEFAULT_MODEL if provider == "gemini" else None)

    client = None
    api_key = None
    model_pool = []

    if provider == "gemini":
        if not HAS_GEMINI:
            print("ERROR: google-genai not installed. Run:")
            print("  pip install google-genai")
            sys.exit(1)
        api_key = os.environ.get("GEMINI_API_KEY") or os.environ.get("GOOGLE_API_KEY")
        if not api_key:
            print("ERROR: Set GEMINI_API_KEY or GOOGLE_API_KEY environment variable.")
            print('  $env:GEMINI_API_KEY = "your-key-here"')
            sys.exit(1)
        client = genai.Client(api_key=api_key)
        client._model_name = selected_model
    else:
        api_key = os.environ.get("OPENROUTER_API_KEY")
        if not api_key:
            print("ERROR: Set OPENROUTER_API_KEY environment variable.")
            print('  $env:OPENROUTER_API_KEY = "your-key-here"')
            sys.exit(1)

        if args.openrouter_models.strip().lower() == "auto":
            model_pool = discover_openrouter_free_models(api_key)
        else:
            model_pool = [m.strip() for m in args.openrouter_models.split(",") if m.strip()]

        if selected_model:
            model_pool = [selected_model] + [m for m in model_pool if m != selected_model]

        if not model_pool:
            model_pool = DEFAULT_OPENROUTER_FREE_MODELS[:]

    groups = [g.upper() for g in args.group] if args.group else ["ALL"]
    if "ALL" in groups:
        groups = ["A", "C"]

    if args.reset and results_csv.exists():
        results_csv.unlink()
        print("Cleared previous results.")

    print("Loading data...")
    scan = load_scan(scan_csv)
    enriched = load_enriched(enriched_csv)
    done = load_done(results_csv)
    papers = build_groups(scan, enriched, groups, done_filenames=done)

    remaining = [p for p in papers if p["filename"] not in done]
    print(f"  Total in selected groups: {len(papers)}")
    print(f"  Already processed: {len(done)}")
    print(f"  Remaining: {len(remaining)}")

    if not remaining:
        print("\nAll papers already processed!")
        print_summary(results_csv)
        return

    deterministic_results = []
    llm_remaining = []
    for paper in remaining:
        deterministic = deterministic_pipeline_verdict(paper, args.max_chars)
        if deterministic is None:
            llm_remaining.append(paper)
        else:
            deterministic_results.append(deterministic)

    for result in deterministic_results:
        append_result(result, results_csv)

    if deterministic_results:
        print(f"  Deterministic decisions written: {len(deterministic_results)}")

    remaining = llm_remaining
    if not remaining:
        print("\nAll remaining papers were resolved deterministically.")
        print_summary(results_csv)
        return

    # Batching setup
    batch_size = args.batch_size
    max_requests = args.max_requests
    num_batches = (len(remaining) + batch_size - 1) // batch_size
    batches_this_run = num_batches if max_requests is None else min(num_batches, max_requests)
    papers_this_run = min(len(remaining), batches_this_run * batch_size)

    if provider == "gemini":
        print(f"\nProvider: Gemini")
        print(f"Using model: {selected_model}")
    else:
        print(f"\nProvider: OpenRouter")
        print(f"Model rotation pool ({len(model_pool)}):")
        for m in model_pool:
            print(f"  - {m}")
    print(f"Batch size: {batch_size} papers/request")
    print(f"Batches needed: {num_batches} total, {batches_this_run} this run "
          f"({'unlimited' if max_requests is None else f'max {max_requests}'} RPD)")
    print(f"Papers this run: {papers_this_run}/{len(remaining)}")

    bucket = TokenBucket(args.tpm)
    est_tokens_per_batch = (args.max_chars // 4 + 200) * batch_size

    start_time = time.time()
    papers_done = len(deterministic_results)
    api_calls = 0

    for batch_idx in range(batches_this_run):
        batch_start = batch_idx * batch_size
        batch_end = min(batch_start + batch_size, len(remaining))
        batch = remaining[batch_start:batch_end]

        bucket.wait_if_needed(est_tokens_per_batch)

        print(f"\n  Batch {batch_idx+1}/{batches_this_run} "
              f"({len(batch)} papers, API call {api_calls+1}):", flush=True)
        for p in batch:
            print(f"    - {p['group']}: {p['filename'][:60]}")

        if provider == "gemini":
            results = call_native_provider_batch(client, batch, args.max_chars)
            used_model = selected_model
        else:
            results, used_model, used_idx = call_openrouter_batch_with_fallback(
                api_key=api_key,
                model_pool=model_pool,
                papers=batch,
                max_chars=args.max_chars,
            )
            if used_model:
                model_pool = model_pool[used_idx:] + model_pool[:used_idx]
                print(f"    -> OpenRouter model used: {used_model}")
        api_calls += 1

        # Stamp every result with the model that produced it
        for r in results:
            r.setdefault("llm_model", used_model or "")

        total_tok = sum(r["input_tokens"] + r["output_tokens"] for r in results)
        bucket.record(total_tok)

        batch_skipped = 0
        for result in results:
            if result.pop("_retryable", False):
                print(f"    -> {result['filename'][:55]}: SKIPPED (infrastructure error, will retry)")
                batch_skipped += 1
                continue
            prereg_str = ("YES" if result["llm_prereg"] is True
                          else "NO" if result["llm_prereg"] is False
                          else "ERR")
            print(f"    -> {result['filename'][:55]}: "
                  f"{prereg_str} ({result['llm_confidence']})")
            append_result(result, results_csv)

        written = len(batch) - batch_skipped
        papers_done += written
        if batch_skipped:
            print(f"    [{written} written, {batch_skipped} skipped (retryable), {total_tok:,} tokens]", flush=True)
        else:
            print(f"    [{papers_done} done, {total_tok:,} tokens]")

        # RPM guard: max 5 requests/min on free tier
        if api_calls % 5 == 0 and batch_idx < batches_this_run - 1:
            print("    (Waiting 62s for RPM window reset...)")
            time.sleep(62)

    elapsed = time.time() - start_time
    print(f"\nDone! Processed {papers_done} papers in {api_calls} API calls, "
          f"{elapsed/60:.1f} minutes.")
    print(f"Results: {results_csv}")

    if papers_done < len(remaining):
        leftover = len(remaining) - papers_done
        runs_left = (leftover + batch_size * max_requests - 1) // (batch_size * max_requests)
        print(f"\n  ⚠ {leftover} papers remaining — re-run tomorrow "
              f"(~{runs_left} more day(s) needed).")

    print_summary(results_csv)


if __name__ == "__main__":
    main()
