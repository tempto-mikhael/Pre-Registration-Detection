"""
find_prereg_links.py
--------------------
Standalone registry-link enrichment utility for PDF scan outputs.

This script is not the main pipeline entrypoint; `enrich_pdf_scan_links.py`
reuses its source-check functions directly. When run standalone, it reads a
PDF-scan CSV and searches alternative sources for registry URLs:

  0. Cached PDF text     — re-scan downloaded OA PDFs for registry URLs.
  1. CrossRef metadata   — checks the `relation` field + references[].
  2. Semantic Scholar    — free API; sometimes contains registry links.
  3. Landing page HTML   — works for non-JS-rendered journals (Wiley, Springer,
                           Elsevier); AEA pages limited due to JS rendering.
  4. OpenAlex            — scans all metadata fields for registry patterns.
  5. OpenAlex refs       — batch-fetch cited works' DOIs for registry patterns.
  6. EGAP registry       — title search on egap.org/research-designs.
  7. AEA RCT Registry    — title search on socialscienceregistry.org.
  8. DataCite            — title query filtered by resource-type=preregistration.
  9. OSF Registrations   — title search via OSF API /v2/registrations/.

After link discovery, each output row is enriched with:
  • best_link_quality  — title-similarity validation of the top found link
                         (VERIFIED | DOI_CONFIRMED | AUTHOR_CONFIRMED |
                          UNCERTAIN | TITLE_MISMATCH | NO_TITLE | UNREACHABLE)
  • best_link_sim      — SequenceMatcher ratio between paper title and
                         registry page title
  • author_check       — CrossRef author overlap detail for uncertain links

Input:  output/pdf_scan_results.csv
Output: output/preregfind.csv

Usage:
  python scripts/find_prereg_links.py [--delay 1.0]

  --delay  : seconds between requests per source (default: 1.0)
"""

import argparse
import csv
import json
import re
import sys
import time
import unicodedata
from difflib import SequenceMatcher
from pathlib import Path
from urllib.parse import urlparse

import fitz  # pymupdf
import requests
from bs4 import BeautifulSoup
from path_utils import resolve_existing_path, resolve_output_path

# ── Paths ──────────────────────────────────────────────────────────────────────
PROJECT_ROOT = Path(__file__).parent.parent
DEFAULT_SCAN_CSV = PROJECT_ROOT / "output" / "pdf_scan_results.csv"
DEFAULT_OUTPUT_CSV = PROJECT_ROOT / "output" / "preregfind.csv"
DEFAULT_OA_PDFS_DIR = PROJECT_ROOT / "output" / "oa_pdfs"
OA_PDFS_DIR = DEFAULT_OA_PDFS_DIR

CONTACT_EMAIL = "makgyumyush22@ku.edu.tr"

SESSION = requests.Session()
SESSION.headers.update({
    "User-Agent": f"ercautomation/1.0 (mailto:{CONTACT_EMAIL})",
    "Accept": "application/json",
})

BROWSER_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                  "AppleWebKit/537.36 Chrome/120.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,*/*",
}

# ── Registry patterns ──────────────────────────────────────────────────────────
REGISTRY_PATTERNS = [
    re.compile(r"https?://(?:www\.)?socialscienceregistry\.org/trials/[\d ]+\d", re.I),
    re.compile(r"\bAEARCTR-[\d ]+\d\b",                                      re.I),
    re.compile(r"https?://(?:www\.)?osf\.io/[A-Za-z0-9]{3,}",               re.I),
    re.compile(r"https?://(?:www\.)?aspredicted\.org/[A-Za-z0-9/_-]+",      re.I),
    re.compile(r"https?://(?:www\.)?clinicaltrials\.gov/\S+",                re.I),
    re.compile(r"https?://(?:www\.)?egap\.org/\S+",                         re.I),
    re.compile(r"\bAsPredicted\s*#\s*[\d ]+\d",                              re.I),
    re.compile(r"https?://(?:www\.)?ridie\.\S+",                             re.I),
]

# Links that are just registry homepages / generic pages (not paper-specific)
GENERIC_LINK_PATTERNS = [
    re.compile(r'^https?://(www\.)?aspredicted\.org/blind\s*$', re.I),
    re.compile(r'^https?://(www\.)?aspredicted\.org/?$', re.I),
    re.compile(r'^https?://(www\.)?egap\.org/?$', re.I),
    re.compile(r'^https?://(www\.)?osf\.io/?$', re.I),
    re.compile(r'^https?://(www\.)?osf\.io/(download|preprints|registries|search|meetings|institutions)\s*$', re.I),
    re.compile(r'^https?://(www\.)?socialscienceregistry\.org/?$', re.I),
    re.compile(r'^https?://(www\.)?socialscienceregistry\.org/trials/?$', re.I),
    re.compile(r'^https?://(www\.)?socialscienceregistry\.org/trials/0\s*$', re.I),
]

def is_generic_link(url: str) -> bool:
    """Return True if the URL is a generic registry homepage, not a paper-specific link."""
    return any(p.match(url.strip()) for p in GENERIC_LINK_PATTERNS)

REGISTRY_DOMAINS = [
    "socialscienceregistry.org", "osf.io", "aspredicted.org",
    "clinicaltrials.gov", "egap.org", "ridie.",
]

VOTER_PHRASES = [
    "preregistration law", "pre-registration law", "voter preregistration",
    "voting preregistration", "youth preregistration", "election preregistration",
    "preregistration requirement", "preregistration program", "preregistration policy",
    "preregistration statute", "preregistration promotes",
]

STRONG_RESEARCH_PHRASES = [
    "analysis plan", "pre-analysis plan", "preanalysis plan",
    "aearctr-", "socialscienceregistry.org", "osf.io",
    "aspredicted.org", "egap.org", "clinicaltrials.gov",
    "open science framework",
]


# ─────────────────────────────────────────────────────────────────────────────
# Utilities
# ─────────────────────────────────────────────────────────────────────────────

def unique(lst):
    seen = set()
    return [x for x in lst if x and not (x in seen or seen.add(x))]


def _strip_spaces(s: str) -> str:
    """Remove spaces inserted by PDF renderers inside digit strings."""
    return re.sub(r'(?<=\d) (?=\d)', '', s)


def _repair_registry_url_spacing(text: str) -> str:
    cleaned = text or ""
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
    return cleaned


def normalise(url: str) -> str:
    """Convert bare AEARCTR-NNNNN to a full URL; strip PDF digit-spaces."""
    clean = _strip_spaces(_repair_registry_url_spacing(url))
    if re.match(r"^AEARCTR-\d+$", clean, re.I):
        n = int(re.search(r"\d+", clean).group())
        return f"https://www.socialscienceregistry.org/trials/{n}"
    if re.match(r"^AsPredicted\s*#\s*\d+$", clean, re.I):
        n = re.search(r"\d+", clean).group()
        return f"https://aspredicted.org/blind.php?x={n}"
    return clean


def extract_links(text: str) -> list:
    text = _repair_registry_url_spacing(text)
    found = []
    for pat in REGISTRY_PATTERNS:
        found.extend(normalise(m) for m in pat.findall(text))
    return unique([u for u in found if not is_generic_link(u)])


def detect_voter_fp(text: str) -> bool:
    t = text.lower()
    return any(p in t for p in VOTER_PHRASES)


def has_strong_signal(text: str) -> bool:
    t = text.lower()
    return any(p in t for p in STRONG_RESEARCH_PHRASES)


def triggered_keywords(text: str) -> str:
    hits = []
    t = text.lower()
    checks = [
        ("pre-analysis plan",     "analysis plan" in t or "pre-analysis" in t),
        ("pre-registration",      "pre-registration" in t or "preregistration" in t),
        ("pre-registered",        "pre-registered" in t or "preregistered" in t),
        ("aearctr-",              "aearctr-" in t),
        ("osf.io",                "osf.io" in t),
        ("aspredicted",           "aspredicted" in t),
        ("\\bpap\\b",             bool(re.search(r"\bpap\b", text, re.I))),
        ("\\bosf\\b",             bool(re.search(r"\bosf\b", text, re.I))),
        ("open science framework","open science framework" in t),
        ("egap.org",              "egap.org" in t),
        ("clinicaltrials.gov",    "clinicaltrials.gov" in t),
        ("analysis plan",         "analysis plan" in t),
    ]
    return "; ".join(k for k, v in checks if v) or "(unknown)"


# ─────────────────────────────────────────────────────────────────────────────
# Link validation & author cross-check
# ─────────────────────────────────────────────────────────────────────────────

SIM_VERIFIED           = 0.45
SIM_UNCERTAIN          = 0.25
AUTHOR_MATCH_THRESHOLD = 0.50
MIN_SURNAME_LENGTH     = 3
CROSSREF_HEADERS       = {"User-Agent": f"ercautomation/1.0 (mailto:{CONTACT_EMAIL})"}


def _osf_api_title(url: str) -> str:
    """Fetch OSF registration/node/preprint title via JSON API (avoids JS-render)."""
    m = re.search(r"osf\.io/(?:preprints/osf/)?([a-z0-9]+)", url, re.IGNORECASE)
    if not m:
        return ""
    node_id = m.group(1).lower()
    for endpoint in ("registrations", "nodes", "preprints"):
        try:
            r = requests.get(
                f"https://api.osf.io/v2/{endpoint}/{node_id}/",
                headers={"User-Agent": f"ercautomation/1.0 (mailto:{CONTACT_EMAIL})"},
                timeout=15,
            )
            if r.status_code == 200:
                attrs = r.json().get("data", {}).get("attributes", {})
                title = attrs.get("title", "")
                if title:
                    return title.strip()
        except Exception:
            pass
    return ""


def _aspredicted_title(soup) -> str:
    """Extract study title from an AsPredicted page."""
    h3 = soup.find("h3")
    if h3:
        italic = h3.find("i")
        if italic:
            t = italic.get_text(strip=True).strip("'\"")
            if t and len(t) > 5:
                return t
        h3_text = h3.get_text(" ", strip=True)
        h3_text = re.sub(r"\(AsPredicted\s*#[\d,]+\).*", "", h3_text, flags=re.IGNORECASE).strip("'\" ")
        if h3_text and len(h3_text) > 5 and not h3_text.startswith("#"):
            return h3_text
    for tr in soup.find_all("tr"):
        cells = tr.find_all(["td", "th"])
        if len(cells) >= 2 and "title" in cells[0].get_text(strip=True).lower():
            candidate = cells[1].get_text(strip=True)
            if len(candidate) > 10:
                return candidate
    return ""


def _extract_registry_title(soup, url: str) -> str:
    """Extract pre-registration title from a registry page's HTML."""
    domain = urlparse(url).netloc.lower()
    if "socialscienceregistry" in domain:
        for sel in [".trial-title", "h1.trial-title", "h1.title", "h1",
                    "[class*='trial'][class*='title']"]:
            el = soup.select_one(sel)
            if el:
                t = el.get_text(strip=True)
                if t and t != "AEA RCT Registry":
                    return t
        og = soup.find("meta", property="og:title")
        if og and og.get("content"):
            c = og["content"].strip()
            if c and c != "AEA RCT Registry":
                return c
    elif "aspredicted" in domain:
        t = _aspredicted_title(soup)
        if t and t not in ("Pre-registrations", "AsPredicted"):
            return t
    elif "egap" in domain:
        for sel in [".plan-title", "h1.plan-title", "h1", "h2"]:
            el = soup.select_one(sel)
            if el and el.get_text(strip=True):
                return el.get_text(strip=True)
    title_tag = soup.find("title")
    if title_tag:
        t = title_tag.get_text(strip=True)
        for suffix in [" | AEA RCT Registry", " | OSF", " | AsPredicted", " - EGAP"]:
            if t.lower().endswith(suffix.lower()):
                t = t[: -len(suffix)].strip()
        if t and t not in ("AEA RCT Registry", "OSF", "AsPredicted"):
            return t
    return ""


def validate_link_quality(url: str, paper_title: str, paper_doi: str = "") -> dict:
    """
    GET a registry URL and assess link quality by title similarity and DOI presence.
    Returns dict with keys: quality, sim, doi_in_page, page_text.
    quality values: VERIFIED | DOI_CONFIRMED | UNCERTAIN | TITLE_MISMATCH |
                    NO_TITLE | UNREACHABLE
    """
    result = {
        "quality": "UNREACHABLE",
        "sim": "N/A",
        "doi_in_page": False,
        "page_text": "",
        "registry_page_title": "",
    }
    try:
        resp = requests.get(url, headers=BROWSER_HEADERS, timeout=20, allow_redirects=True)
        if resp.status_code >= 400:
            result["quality"] = "UNREACHABLE"
            return result
        html = resp.text
        result["page_text"] = html
        final_url = resp.url or url
        # DOI check
        if paper_doi:
            doi_clean = paper_doi.strip().lower().lstrip("https://doi.org/").lstrip("doi:")
            result["doi_in_page"] = doi_clean in html.lower()
        # Title extraction
        is_osf = "osf.io" in final_url.lower()
        if is_osf:
            page_title = _osf_api_title(final_url)
            if not page_title:
                soup = BeautifulSoup(html, "html.parser")
                page_title = _extract_registry_title(soup, final_url)
        else:
            soup = BeautifulSoup(html, "html.parser")
            page_title = _extract_registry_title(soup, final_url)
        result["registry_page_title"] = page_title
        # Similarity
        if not page_title or not paper_title:
            sim, sim_str = 0.0, "N/A"
        else:
            sim = SequenceMatcher(None,
                                  paper_title.lower().strip(),
                                  page_title.lower().strip()).ratio()
            sim_str = f"{sim:.3f}"
        result["sim"] = sim_str
        # Quality verdict
        if result["doi_in_page"] and sim < SIM_VERIFIED:
            result["quality"] = "DOI_CONFIRMED"
        elif page_title and sim >= SIM_VERIFIED:
            result["quality"] = "VERIFIED"
        elif sim_str == "N/A":
            result["quality"] = "NO_TITLE"
        elif sim >= SIM_UNCERTAIN:
            result["quality"] = "UNCERTAIN"
        else:
            result["quality"] = "TITLE_MISMATCH"
    except Exception:
        result["quality"] = "UNREACHABLE"
    return result


def _normalize_name(name: str) -> str:
    """Strip accents and lowercase for fuzzy name matching."""
    nfkd = unicodedata.normalize("NFKD", name)
    return "".join(c for c in nfkd if not unicodedata.combining(c)).lower()


def crossref_authors_by_doi(doi: str) -> list:
    """Return list of author family names from CrossRef for a given DOI."""
    if not doi or doi.startswith(("TITLE_SLUG:", "PII:")):
        return []
    try:
        r = requests.get(
            f"https://api.crossref.org/works/{doi}",
            headers=CROSSREF_HEADERS, timeout=15,
        )
        if r.status_code != 200:
            return []
        return [a["family"] for a in r.json().get("message", {}).get("author", [])
                if a.get("family")]
    except Exception:
        return []


def crossref_authors_by_title(title: str) -> tuple:
    """Search CrossRef by title; returns (family_names, doi). Falls back to empty."""
    if not title:
        return [], ""
    try:
        r = requests.get(
            "https://api.crossref.org/works",
            params={"query.title": title[:120], "rows": 1},
            headers=CROSSREF_HEADERS, timeout=15,
        )
        if r.status_code != 200:
            return [], ""
        items = r.json().get("message", {}).get("items", [])
        if not items:
            return [], ""
        item = items[0]
        cr_title = " ".join(item.get("title", [])).lower()
        if SequenceMatcher(None, title.lower()[:100], cr_title[:100]).ratio() < 0.55:
            return [], ""
        families = [a["family"] for a in item.get("author", []) if a.get("family")]
        return families, item.get("DOI", "")
    except Exception:
        return [], ""


def _osf_api_contributors(url: str) -> list:
    """Get contributor family names from OSF API for a registration/node/preprint."""
    m = re.search(r"osf\.io/(?:preprints/osf/)?([a-z0-9]+)", url, re.IGNORECASE)
    if not m:
        return []
    node_id = m.group(1).lower()
    for endpoint in ("registrations", "nodes", "preprints"):
        try:
            api_url = f"https://api.osf.io/v2/{endpoint}/{node_id}/contributors/?embed=users"
            r = requests.get(api_url, timeout=15)
            if r.status_code != 200:
                continue
            names = []
            for item in r.json().get("data", []):
                user_data = item.get("embeds", {}).get("users", {}).get("data", {})
                if isinstance(user_data, dict):
                    attrs = user_data.get("attributes", {})
                    family = attrs.get("family_name", "")
                    if family:
                        names.append(family)
                    elif attrs.get("full_name"):
                        names.append(attrs["full_name"].split()[-1])
            if names:
                return names
        except Exception:
            pass
    return []


def author_overlap(paper_authors: list, registry_page_text: str,
                   registry_url: str) -> tuple:
    """
    Fraction of paper authors whose surnames appear on the registry page.
    Uses OSF API for OSF pages; page text for others.
    Returns (overlap_ratio, detail_string).
    """
    if not paper_authors:
        return 0.0, "no_authors"
    osf_contribs = []
    if "osf.io" in registry_url.lower():
        osf_contribs = _osf_api_contributors(registry_url)
    matchable = [a for a in paper_authors if len(a) >= MIN_SURNAME_LENGTH]
    if not matchable:
        return 0.0, "short_names_only"
    page_lower = _normalize_name(registry_page_text)
    osf_lower  = " ".join(_normalize_name(n) for n in osf_contribs)
    matched = [s for s in matchable
               if _normalize_name(s) in page_lower
               or (osf_lower and _normalize_name(s) in osf_lower)]
    ratio  = len(matched) / len(matchable)
    detail = f"{len(matched)}/{len(matchable)} ({', '.join(matched) if matched else 'none'})"
    return ratio, detail


# ─────────────────────────────────────────────────────────────────────────────
# Source 0: Cached PDF text (GAP 1 fix)
# ─────────────────────────────────────────────────────────────────────────────

def check_cached_pdf(pdf_filename: str) -> tuple:
    """Return (links, triggered_kws, full_text) from cached OA PDF if available."""
    if not pdf_filename:
        return ([], "", "")
    # The pdf_filename column may contain the base name with or without extension
    candidates = []
    base = pdf_filename.strip()
    for ext in ("", ".pdf"):
        candidates.append(OA_PDFS_DIR / (base + ext))
    # Also try glob (filename truncated in xlsx)
    pdf_path = None
    for c in candidates:
        if c.exists():
            pdf_path = c
            break
    if pdf_path is None:
        # Try prefix match (filename may be truncated)
        name_prefix = base[:40].lower()
        for p in OA_PDFS_DIR.glob("*.pdf"):
            if p.stem.lower().startswith(name_prefix[:30]):
                pdf_path = p
                break
    if pdf_path is None:
        return ([], "", "")
    try:
        doc = fitz.open(str(pdf_path))
        text = ""
        for page in doc:
            text += page.get_text()
        doc.close()
        links = extract_links(text)
        kws   = triggered_keywords(text)
        return (links, kws, text)
    except Exception:
        return ([], "", "")


# ─────────────────────────────────────────────────────────────────────────────
# Source 1: CrossRef relation field + references[]
# ─────────────────────────────────────────────────────────────────────────────

def check_crossref(doi: str) -> list:
    if not doi or doi.startswith(("TITLE_SLUG:", "PII:")):
        return []
    try:
        r = SESSION.get(f"https://api.crossref.org/works/{doi}", timeout=15)
        if r.status_code != 200:
            return []
        msg = r.json().get("message", {})
        links = []
        # relation contains pre-reg pointers in some journals
        relation = msg.get("relation", {})
        for rel_type, rel_list in relation.items():
            for item in (rel_list or []):
                url = item.get("id", "")
                if url and any(d in url for d in REGISTRY_DOMAINS):
                    links.append(url)
        # scan full relation block as raw text
        links.extend(extract_links(json.dumps(relation)))
        # link field
        for lnk in msg.get("link", []):
            url = lnk.get("URL", "")
            if any(d in url for d in REGISTRY_DOMAINS):
                links.append(url)
        # GAP 3: references[] array — scan raw citation strings (free tier may return these)
        for ref in msg.get("reference", []):
            unstructured = ref.get("unstructured", "") or ""
            doi_r = ref.get("DOI", "") or ""
            links.extend(extract_links(unstructured))
            links.extend(extract_links(doi_r))
        return unique(links)
    except Exception:
        return []


# ─────────────────────────────────────────────────────────────────────────────
# Source 2: Semantic Scholar (free, no auth needed)
# ─────────────────────────────────────────────────────────────────────────────

def check_semantic_scholar(doi: str, title: str) -> list:
    _hdrs = {"User-Agent": f"ercautomation/1.0 (mailto:{CONTACT_EMAIL})", "Accept": "application/json"}
    links = []

    if doi and not doi.startswith(("TITLE_SLUG:", "PII:")):
        try:
            r = requests.get(
                f"https://api.semanticscholar.org/graph/v1/paper/DOI:{doi}",
                params={"fields": "externalIds,title,abstract,openAccessPdf"},
                headers=_hdrs, timeout=15,
            )
            if r.status_code == 200:
                data = r.json()
                abstract = data.get("abstract", "") or ""
                links.extend(extract_links(abstract))
                # scan all externalIds values
                links.extend(extract_links(json.dumps(data.get("externalIds", {}))))
        except Exception:
            pass

    # Fallback: title search (only if no DOI match)
    if not links and title:
        try:
            r = requests.get(
                "https://api.semanticscholar.org/graph/v1/paper/search",
                params={"query": title[:100], "fields": "abstract,externalIds", "limit": 3},
                headers=_hdrs, timeout=15,
            )
            if r.status_code == 200:
                for paper in r.json().get("data", []):
                    links.extend(extract_links(paper.get("abstract", "") or ""))
        except Exception:
            pass

    return unique(links)


# ─────────────────────────────────────────────────────────────────────────────
# Source 3: Landing page HTML
# Works well for Wiley / Springer / Elsevier.
# AEA and Econometrica are JS-rendered so the HTML body is minimal,
# but we still scan raw HTML and JSON-LD blocks.
# ─────────────────────────────────────────────────────────────────────────────

def check_landing_page(doi: str) -> tuple:
    if not doi or doi.startswith(("TITLE_SLUG:", "PII:")):
        return ("", [])
    url = (f"https://www.aeaweb.org/articles?id={doi}"
           if doi.startswith("10.1257/") else f"https://doi.org/{doi}")
    try:
        resp = requests.get(url, timeout=20, headers=BROWSER_HEADERS, allow_redirects=True)
        final_url = resp.url
        if resp.status_code != 200:
            return (final_url, [])
        html = resp.text
        soup = BeautifulSoup(html, "lxml")
        links = []
        # <a href> to registry domains
        for tag in soup.find_all("a", href=True):
            href = tag["href"].strip()
            if any(d in href for d in REGISTRY_DOMAINS):
                links.append(href)
        # raw text scan
        links.extend(extract_links(html))
        # JSON-LD structured data (many journals embed this)
        for script in soup.find_all("script", type="application/ld+json"):
            try:
                links.extend(extract_links(json.dumps(json.loads(script.string or ""))))
            except Exception:
                pass
        # <meta> tags
        for meta in soup.find_all("meta"):
            content = meta.get("content", "") or ""
            if any(d in content for d in REGISTRY_DOMAINS):
                links.extend(extract_links(content))
        return (final_url, unique(links))
    except Exception:
        return ("", [])


# ─────────────────────────────────────────────────────────────────────────────
# Source 4: OpenAlex (scan full metadata blob for registry patterns)
# ─────────────────────────────────────────────────────────────────────────────

def check_openalex(doi: str) -> list:
    if not doi or doi.startswith(("TITLE_SLUG:", "PII:")):
        return []
    try:
        r = SESSION.get(
            f"https://api.openalex.org/works/doi:{doi}",
            params={"mailto": CONTACT_EMAIL}, timeout=15,
        )
        if r.status_code != 200:
            return []
        return extract_links(json.dumps(r.json()))
    except Exception:
        return []


# ─────────────────────────────────────────────────────────────────────────────
# Source 5: OpenAlex referenced_works — scan reference list for registry DOIs
# (GAP 2)
# ─────────────────────────────────────────────────────────────────────────────

def check_openalex_refs(doi: str) -> list:
    """Fetch the referenced_works list and look for OSF / registry DOIs in it.

    Uses batch API calls (50 IDs at a time) to stay fast.
    Only fetches works whose OpenAlex ID suggests an OSF-type DOI pattern
    (we pre-filter by scanning the referenced_works URL list for nothing specific,
    then batch-fetch their DOIs in chunks of 50).
    """
    if not doi or doi.startswith(("TITLE_SLUG:", "PII:")):
        return []
    try:
        r = SESSION.get(
            f"https://api.openalex.org/works/doi:{doi}",
            params={"select": "referenced_works", "mailto": CONTACT_EMAIL},
            timeout=15,
        )
        if r.status_code != 200:
            return []
        ref_works = r.json().get("referenced_works", []) or []
        if not ref_works:
            return []

        # Extract bare OpenAlex IDs (W1234567) from full URLs
        oa_ids = []
        for w in ref_works:
            oa_id = w.split("/")[-1] if "/" in w else w
            oa_ids.append(oa_id)

        links = []
        BATCH = 50  # OpenAlex allows up to 50 pipe-separated IDs
        for i in range(0, len(oa_ids), BATCH):
            chunk = oa_ids[i:i + BATCH]
            filter_str = "|".join(chunk)
            try:
                rb = SESSION.get(
                    "https://api.openalex.org/works",
                    params={"filter": f"openalex_id:{filter_str}",
                            "select": "doi",
                            "per-page": BATCH,
                            "mailto": CONTACT_EMAIL},
                    timeout=20,
                )
                if rb.status_code == 200:
                    for item in rb.json().get("results", []):
                        ref_doi = item.get("doi", "") or ""
                        links.extend(extract_links(ref_doi))
            except Exception:
                pass
            time.sleep(0.15)
        return unique(links)
    except Exception:
        return []


# ─────────────────────────────────────────────────────────────────────────────
# Source 6: EGAP registry title search (GAP 4)
# ─────────────────────────────────────────────────────────────────────────────

def check_egap(title: str) -> list:
    """Search EGAP research-designs for the paper title and extract registry URLs."""
    if not title:
        return []
    try:
        query = title[:80]
        r = requests.get(
            "https://egap.org/research-designs/",
            params={"s": query},
            headers=BROWSER_HEADERS, timeout=20,
        )
        if r.status_code != 200:
            return []
        soup = BeautifulSoup(r.text, "lxml")
        links = []
        for a in soup.find_all("a", href=True):
            href = a["href"]
            if "egap.org" in href and "/registration/" in href:
                links.append(href)
        return unique(links)
    except Exception:
        return []


# ─────────────────────────────────────────────────────────────────────────────
# Source 7: AEA RCT Registry HTML search (GAP 5)
# ─────────────────────────────────────────────────────────────────────────────

def check_aearctr_html(title: str) -> list:
    """Search the public AEA RCT Registry HTML search page for the paper title."""
    if not title:
        return []
    try:
        query = title[:80]
        r = requests.get(
            "https://www.socialscienceregistry.org/trials",
            params={"search": query},
            headers=BROWSER_HEADERS, timeout=20,
        )
        if r.status_code != 200:
            return []
        soup = BeautifulSoup(r.text, "lxml")
        links = []
        for a in soup.find_all("a", href=True):
            href = a["href"]
            if "socialscienceregistry.org/trials/" in href and href.split("/trials/")[-1].isdigit():
                links.append(href if href.startswith("http") else "https://www.socialscienceregistry.org" + href)
        return unique(links)
    except Exception:
        return []


# ─────────────────────────────────────────────────────────────────────────────
# Source 8: DataCite search for pre-registrations (GAP 6)
# ─────────────────────────────────────────────────────────────────────────────

def check_datacite(title: str) -> list:
    """Query DataCite for pre-registration objects matching the paper title."""
    if not title:
        return []
    try:
        r = SESSION.get(
            "https://api.datacite.org/dois",
            params={"query": title[:80], "resource-type-id": "preregistration", "page[size]": 5},
            timeout=15,
        )
        if r.status_code != 200:
            return []
        items = r.json().get("data", []) or []
        links = []
        for item in items:
            attr = item.get("attributes", {})
            url = attr.get("url", "") or ""
            doi_dc = attr.get("doi", "") or ""
            if url:
                links.append(url)
            if doi_dc:
                links.extend(extract_links(f"https://doi.org/{doi_dc}"))
        return unique(links)
    except Exception:
        return []


# ─────────────────────────────────────────────────────────────────────────────
# Source 9: OSF Registrations title search
# ─────────────────────────────────────────────────────────────────────────────

def check_osf_search(title: str) -> list:
    """Search OSF Registries by title; return list of matching registration URLs."""
    if not title:
        return []
    try:
        r = requests.get(
            "https://api.osf.io/v2/registrations/",
            params={"filter[title]": title[:120], "page[size]": 5},
            headers={"User-Agent": f"ercautomation/1.0 (mailto:{CONTACT_EMAIL})"},
            timeout=20,
        )
        if r.status_code != 200:
            return []
        items = r.json().get("data", []) or []
        return unique(["https://osf.io/" + item["id"] for item in items if item.get("id")])
    except Exception:
        return []


# ─────────────────────────────────────────────────────────────────────────────
# Verdict
# ─────────────────────────────────────────────────────────────────────────────

def get_verdict(all_links: list, voter_fp: bool, text: str) -> str:
    if all_links:
        return "CONFIRMED_link_found"
    if voter_fp and not has_strong_signal(text):
        return "LIKELY_FP_voter_context"
    if has_strong_signal(text):
        return "PROBABLE_no_url_strong_kw"
    return "POSSIBLE_no_url_weak_kw"


# ─────────────────────────────────────────────────────────────────────────────
# Output schema
# ─────────────────────────────────────────────────────────────────────────────

FIELDS = [
    "filename", "pdf_path", "journal", "title", "doi",
    "text_source",
    "triggered_by",
    "voter_fp_signal",
    "pipeline_links",
    "cached_pdf_links",
    "crossref_links",
    "s2_links",
    "landing_page_url",
    "landing_page_links",
    "openalex_links",
    "openalex_refs_links",
    "egap_links",
    "aearctr_html_links",
    "datacite_links",
    "osf_title_links",
    "all_found_links",
    "best_link_quality",
    "best_link_title",
    "best_link_sim",
    "author_check",
    "verdict",
]


# ─────────────────────────────────────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────────────────────────────────────

def main():
    ap = argparse.ArgumentParser(description="Find pre-registration links for auto_prereg=1 rows")
    ap.add_argument("--delay", type=float, default=1.0)
    ap.add_argument("--scan", type=str, default=None,
                    help=f"Path to scan CSV (default: {DEFAULT_SCAN_CSV})")
    ap.add_argument("--output", type=str, default=None,
                    help=f"Path to output CSV (default: {DEFAULT_OUTPUT_CSV})")
    ap.add_argument("--oa-pdfs-dir", type=str, default=None,
                    help=f"Directory of cached PDFs (default: {DEFAULT_OA_PDFS_DIR})")
    args = ap.parse_args()

    scan_csv = resolve_existing_path(args.scan, DEFAULT_SCAN_CSV, "scan CSV")
    output_csv = resolve_output_path(args.output, DEFAULT_OUTPUT_CSV)

    global OA_PDFS_DIR
    OA_PDFS_DIR = Path(args.oa_pdfs_dir) if args.oa_pdfs_dir else DEFAULT_OA_PDFS_DIR

    with open(scan_csv, newline="", encoding="utf-8") as f:
        all_rows = list(csv.DictReader(f))

    detections = [r for r in all_rows if str(r.get("auto_prereg", "")).strip() == "1"]

    print(f"Scan file    : {scan_csv}  ({len(all_rows)} total rows)")
    print(f"Detections   : {len(detections)} (auto_prereg=1)")
    print()

    out_rows = []
    for i, r in enumerate(detections, 1):
        doi   = r.get("doi", "") or ""
        title = (r.get("title") or "").strip()

        print(f"[{i}/{len(detections)}] {r.get('filename', '')} — {title[:65]}")

        text_source = r.get("text_source", "") or ""
        pipe_lnk    = r.get("auto_link_prereg", "") or ""
        pdf_fname   = r.get("filename", "") or ""

        # GAP 1: always scan cached PDF first (most impactful for full_pdf detections)
        print(f"  [0/9] Cached PDF...")
        pdf_links, pdf_kws, pdf_text = check_cached_pdf(pdf_fname)
        time.sleep(0)

        # Determine the best text for voter/kw analysis:
        rich_text = pdf_text
        voter_fp  = detect_voter_fp(rich_text)
        kws       = triggered_keywords(rich_text)

        print(f"  [1/9] CrossRef...")
        cr_links = check_crossref(doi);    time.sleep(args.delay * 0.4)

        print(f"  [2/9] Semantic Scholar...")
        s2_links = check_semantic_scholar(doi, title); time.sleep(args.delay * 0.4)

        print(f"  [3/9] Landing page...")
        lp_url, lp_links = check_landing_page(doi); time.sleep(args.delay * 0.4)

        print(f"  [4/9] OpenAlex metadata...")
        oa_links = check_openalex(doi);    time.sleep(args.delay * 0.4)

        print(f"  [5/9] OpenAlex referenced_works...")
        oa_ref_links = check_openalex_refs(doi); time.sleep(args.delay * 0.4)

        print(f"  [6/9] EGAP search...")
        egap_links = check_egap(title); time.sleep(args.delay * 0.4)

        print(f"  [7/9] AEA RCT Registry HTML...")
        aear_links = check_aearctr_html(title); time.sleep(args.delay * 0.4)

        print(f"  [8/9] DataCite...")
        dc_links = check_datacite(title); time.sleep(args.delay * 0.4)

        print(f"  [9/9] OSF title search...")
        osf_title_links = check_osf_search(title); time.sleep(args.delay * 0.4)

        all_links = unique(
            ([pipe_lnk] if pipe_lnk else [])
            + pdf_links + cr_links + s2_links + lp_links
            + oa_links + oa_ref_links + egap_links + aear_links + dc_links
            + osf_title_links
        )
        vrd = get_verdict(all_links, voter_fp, rich_text)

        # ── Improvement 2: validate quality of best found link ────────────────
        best_lq   = ""
        best_title = ""
        best_sim  = ""
        author_chk = ""
        if all_links:
            best_candidate = all_links[0]
            lq = validate_link_quality(best_candidate, title, doi)
            best_lq  = lq["quality"]
            best_title = lq.get("registry_page_title", "")
            best_sim = lq["sim"]
            print(f"  link quality: {best_lq} | sim={best_sim}")

            # ── Improvement 3: author cross-check for uncertain links ─────────
            if best_lq in ("UNCERTAIN", "TITLE_MISMATCH", "NO_TITLE"):
                paper_authors = crossref_authors_by_doi(doi) if doi else []
                if not paper_authors:
                    paper_authors, _ = crossref_authors_by_title(title)
                time.sleep(0.3)
                if paper_authors:
                    overlap, detail = author_overlap(
                        paper_authors, lq.get("page_text", ""), best_candidate
                    )
                    author_chk = detail
                    time.sleep(0.3)
                    if overlap >= AUTHOR_MATCH_THRESHOLD:
                        best_lq = "AUTHOR_CONFIRMED"
                        print(f"  \u2191 AUTHOR_CONFIRMED: {detail}")
                    else:
                        print(f"  authors: {detail}")
                else:
                    author_chk = "no_cr_data"
                    print(f"  authors: no CrossRef data")

        print(f"  voter_fp={int(voter_fp)} | triggered=[{kws}]")
        if pdf_links:       print(f"  cached_pdf  : {pdf_links}")
        if cr_links:        print(f"  crossref    : {cr_links}")
        if s2_links:        print(f"  s2          : {s2_links}")
        if lp_links:        print(f"  landing     : {lp_links}")
        if oa_links:        print(f"  openalex    : {oa_links}")
        if oa_ref_links:    print(f"  oa_refs     : {oa_ref_links}")
        if egap_links:      print(f"  egap        : {egap_links}")
        if aear_links:      print(f"  aearctr     : {aear_links}")
        if dc_links:        print(f"  datacite    : {dc_links}")
        if osf_title_links: print(f"  osf_title   : {osf_title_links}")
        print(f"  → {vrd} | all links: {all_links or '(none)'}")
        print()

        out_rows.append({
            "filename":             r.get("filename", ""),
            "pdf_path":             r.get("pdf_path", ""),
            "journal":              r.get("journal", ""),
            "title":               title,
            "doi":                 doi,
            "text_source":         text_source,
            "triggered_by":        kws,
            "voter_fp_signal":     int(voter_fp),
            "pipeline_links":      pipe_lnk,
            "cached_pdf_links":    "; ".join(pdf_links),
            "crossref_links":      "; ".join(cr_links),
            "s2_links":            "; ".join(s2_links),
            "landing_page_url":    lp_url,
            "landing_page_links":  "; ".join(lp_links),
            "openalex_links":      "; ".join(oa_links),
            "openalex_refs_links": "; ".join(oa_ref_links),
            "egap_links":          "; ".join(egap_links),
            "aearctr_html_links":  "; ".join(aear_links),
            "datacite_links":      "; ".join(dc_links),
            "osf_title_links":     "; ".join(osf_title_links),
            "all_found_links":     "; ".join(all_links),
            "best_link_quality":   best_lq,
            "best_link_title":     best_title,
            "best_link_sim":       best_sim,
            "author_check":        author_chk,
            "verdict":             vrd,
        })

    output_csv.parent.mkdir(exist_ok=True)
    with open(output_csv, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=FIELDS)
        writer.writeheader()
        writer.writerows(out_rows)

    confirmed      = sum(1 for x in out_rows if x["verdict"] == "CONFIRMED_link_found")
    probable       = sum(1 for x in out_rows if "PROBABLE" in x["verdict"])
    possible       = sum(1 for x in out_rows if "POSSIBLE" in x["verdict"])
    fp_voter       = sum(1 for x in out_rows if "FP_voter" in x["verdict"])
    lq_verified    = sum(1 for x in out_rows if x["best_link_quality"] in ("VERIFIED", "DOI_CONFIRMED"))
    lq_author      = sum(1 for x in out_rows if x["best_link_quality"] == "AUTHOR_CONFIRMED")
    lq_uncertain   = sum(1 for x in out_rows if x["best_link_quality"] in ("UNCERTAIN", "TITLE_MISMATCH", "NO_TITLE"))

    print(f"\nWritten {len(out_rows)} rows \u2192 {output_csv}")
    print(f"  CONFIRMED (link found)      : {confirmed}")
    print(f"  PROBABLE  (strong kw)       : {probable}")
    print(f"  POSSIBLE  (weak kw)         : {possible}")
    print(f"  LIKELY FP (voter ctx)       : {fp_voter}")
    print(f"  --- link quality (confirmed rows) ---")
    print(f"  VERIFIED / DOI_CONFIRMED    : {lq_verified}")
    print(f"  AUTHOR_CONFIRMED            : {lq_author}")
    print(f"  UNCERTAIN / MISMATCH        : {lq_uncertain}")


if __name__ == "__main__":
    main()
