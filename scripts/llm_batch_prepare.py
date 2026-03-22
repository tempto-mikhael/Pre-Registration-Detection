#!/usr/bin/env python3
"""
Prepare ambiguous papers for LLM batch verification.

Creates JSONL files compatible with OpenAI Batch API format.
Each request asks the LLM to read the paper text and determine whether
the paper reports its OWN pre-registration (not just citing others').

Three groups are prepared:
  Group A – auto_prereg=1 but no link found anywhere (956 papers)
            → "Is this paper actually pre-registered?"
  Group B – xlsx prereg=1 but our keyword scan missed it (auto_prereg=0)
            → "Our scanner missed this — can the LLM find evidence?"
  Group C – Disputed: link evidence found but xlsx says prereg=0 (84 papers)
            → "Does this link belong to THIS paper's pre-registration?"

Usage:
    python llm_batch_prepare.py [--max-chars 12000] [--model gpt-4o-mini]
                                [--group A] [--group B] [--group C] [--group all]

Output:
    output/llm_batch_group_a.jsonl   (largest set)
    output/llm_batch_group_b.jsonl
    output/llm_batch_group_c.jsonl
    output/llm_batch_manifest.csv    (tracking file)
"""

import argparse
import csv
import json
import os
import sys
from pathlib import Path

import fitz  # PyMuPDF
import openpyxl

ROOT = Path(__file__).parent.parent
OUTPUT_DIR = ROOT / "output"

SCAN_CSV     = OUTPUT_DIR / "pdf_scan_results.csv"
ENRICHED_CSV = OUTPUT_DIR / "pdf_scan_prereg_links_dedup.csv"
XLSX_PATH    = ROOT / "journal_articles_with_pap_2025-03-14.xlsx"

DEFAULT_MAX_CHARS = 12_000   # ~3k tokens, fits easily in 16k context
DEFAULT_MODEL     = "gpt-4o-mini"

VERIFIED_QUALITIES = {"VERIFIED", "DOI_CONFIRMED", "AUTHOR_CONFIRMED"}


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

# ── System prompts ────────────────────────────────────────────────────────────

SYSTEM_PROMPT = """\
You are an expert research assistant specializing in academic economics papers.
Your task is to determine whether a given paper reports that the study itself
was pre-registered (i.e., the authors registered a pre-analysis plan or
pre-registration BEFORE conducting the study described in the paper).

IMPORTANT DISTINCTIONS:
- A paper that CITES another pre-registered study is NOT itself pre-registered.
- A paper that DISCUSSES pre-registration as a methodology is NOT itself pre-registered.
- A paper that mentions "registered report" for its OWN submission IS pre-registered.
- A paper that says "we pre-registered our hypotheses at [URL]" IS pre-registered.
- A paper about a randomized controlled trial registered at clinicaltrials.gov
  or a social science registry IS pre-registered only if it says so explicitly.

Respond with a JSON object (no markdown fences) with these fields:
{
  "prereg": true or false,
  "confidence": "high" or "medium" or "low",
  "evidence": "brief quote or description of the evidence (max 150 chars)",
  "registry_url": "URL if found, else null",
  "reasoning": "1-2 sentence explanation of your decision"
}
"""

USER_PROMPT_A = """\
Paper filename: {filename}
Journal: {journal}
Keywords that triggered detection: {keywords}

The following is the extracted text from this academic paper.
Determine whether THIS paper reports its OWN pre-registration.

--- PAPER TEXT ---
{text}
"""

USER_PROMPT_B = """\
Paper filename: {filename}
Journal: {journal}

Our automated keyword scanner did NOT flag this paper, but a human reviewer
marked it as pre-registered (prereg=1). Please carefully read the text and
determine whether this paper reports its own pre-registration. Look for
unusual phrasings, footnotes, or appendix references that a keyword scan
might miss.

--- PAPER TEXT ---
{text}
"""

USER_PROMPT_C = """\
Paper filename: {filename}
Journal: {journal}

Our automated pipeline found these pre-registration links associated with
this paper:
{links_section}

However, a human reviewer marked this paper as NOT pre-registered (prereg=0).
Please read the paper text and determine:
1. Does this paper report its OWN pre-registration?
2. Do the links above belong to THIS paper's pre-registration, or are they
   from cited/referenced studies?

--- PAPER TEXT ---
{text}
"""

# ── Data loading ──────────────────────────────────────────────────────────────

def load_xlsx():
    wb = openpyxl.load_workbook(str(XLSX_PATH), read_only=True, data_only=True)
    ws = wb.active
    headers = [c.value for c in list(ws.rows)[1]]
    by_file = {}
    for row in ws.iter_rows(min_row=3, values_only=True):
        d = dict(zip(headers, row))
        pdf = (d.get("pdf") or "").strip()
        if pdf:
            by_file[pdf] = {
                "prereg": d.get("prereg"),
                "link_prereg": d.get("link_prereg") or "",
                "journal": d.get("journal") or "",
            }
    wb.close()
    return by_file


def load_scan():
    result = {}
    with open(SCAN_CSV, newline="", encoding="utf-8") as f:
        for r in csv.DictReader(f):
            fname = Path(r["pdf_path"]).name
            result[fname] = r
    return result


def load_enriched():
    result = {}
    if not ENRICHED_CSV.exists():
        return result
    with open(ENRICHED_CSV, newline="", encoding="utf-8") as f:
        for r in csv.DictReader(f):
            fname = Path(r["pdf_path"]).name
            result[fname] = r
    return result


def extract_text(pdf_path: str, max_chars: int) -> str:
    """Extract text from PDF, truncated to max_chars."""
    try:
        doc = fitz.open(pdf_path)
        pages = []
        total = 0
        for page in doc:
            t = page.get_text("text")
            pages.append(t)
            total += len(t)
            if total > max_chars * 1.2:  # slight overshoot ok, we truncate later
                break
        doc.close()
        text = "\n".join(pages)
        if len(text) > max_chars:
            text = text[:max_chars] + "\n\n[... text truncated ...]"
        return text
    except Exception as e:
        return f"[ERROR extracting text: {e}]"


# ── Group assembly ────────────────────────────────────────────────────────────

def build_group_a(scan, enriched):
    """auto_prereg=1 but no link found anywhere."""
    papers = []
    for fname, s in scan.items():
        if str(s.get("auto_prereg", "")).strip() != "1":
            continue
        if (s.get("auto_link_prereg") or "").strip():
            continue
        e = enriched.get(fname, {})
        if (e.get("all_found_links") or "").strip():
            continue
        papers.append({
            "filename": fname,
            "pdf_path": s["pdf_path"],
            "journal": s.get("journal", ""),
            "keywords": s.get("triggered_keywords", ""),
            "group": "A",
        })
    return papers


def build_group_b(scan, xlsx):
    """xlsx prereg=1 but auto_prereg=0."""
    papers = []
    for fname, x in xlsx.items():
        if x["prereg"] != 1:
            continue
        s = scan.get(fname, {})
        if str(s.get("auto_prereg", "")).strip() == "1":
            continue
        if not s.get("pdf_path"):
            continue
        papers.append({
            "filename": fname,
            "pdf_path": s["pdf_path"],
            "journal": x.get("journal") or s.get("journal", ""),
            "keywords": "",
            "group": "B",
        })
    return papers


def build_group_c(scan, enriched, xlsx):
    """Link evidence found but xlsx says prereg=0."""
    papers = []
    for fname in set(list(scan.keys()) + list(enriched.keys())):
        x = xlsx.get(fname, {})
        if x.get("prereg") != 0:
            continue
        s = scan.get(fname, {})
        e = enriched.get(fname, {})
        pdf_link = (s.get("auto_link_prereg") or "").strip()
        enrich_links = (e.get("all_found_links") or "").strip()
        if not pdf_link and not enrich_links:
            continue
        quality = (e.get("best_link_quality") or "").strip()

        links_section = ""
        if pdf_link:
            links_section += f"  From PDF text: {pdf_link}\n"
        if enrich_links:
            links_section += f"  From API enrichment ({quality}): {enrich_links}\n"

        papers.append({
            "filename": fname,
            "pdf_path": s.get("pdf_path") or e.get("pdf_path", ""),
            "journal": x.get("journal") or s.get("journal", ""),
            "keywords": s.get("triggered_keywords", ""),
            "links_section": links_section,
            "group": "C",
        })
    return papers


# ── JSONL generation ──────────────────────────────────────────────────────────

def make_request(paper: dict, model: str, max_chars: int) -> dict:
    """Build one OpenAI Batch API request dict."""
    text = extract_text(paper["pdf_path"], max_chars)

    if paper["group"] == "A":
        user_msg = USER_PROMPT_A.format(
            filename=paper["filename"],
            journal=paper["journal"],
            keywords=paper["keywords"],
            text=text,
        )
    elif paper["group"] == "B":
        user_msg = USER_PROMPT_B.format(
            filename=paper["filename"],
            journal=paper["journal"],
            text=text,
        )
    else:  # C
        user_msg = USER_PROMPT_C.format(
            filename=paper["filename"],
            journal=paper["journal"],
            links_section=paper.get("links_section", ""),
            text=text,
        )

    custom_id = f"group_{paper['group'].lower()}_{paper['filename']}"
    # Sanitize custom_id (max 64 chars for some APIs)
    if len(custom_id) > 512:
        custom_id = custom_id[:512]

    return {
        "custom_id": custom_id,
        "method": "POST",
        "url": "/v1/chat/completions",
        "body": {
            "model": model,
            "temperature": 0.1,
            "max_tokens": 300,
            "response_format": {"type": "json_object"},
            "messages": [
                {"role": "system", "content": SYSTEM_PROMPT},
                {"role": "user", "content": user_msg},
            ],
        },
    }


def write_jsonl(papers: list, output_path: Path, model: str, max_chars: int) -> int:
    """Write JSONL batch file. Returns number of requests written."""
    count = 0
    with open(output_path, "w", encoding="utf-8") as f:
        for paper in papers:
            req = make_request(paper, model, max_chars)
            f.write(json.dumps(req, ensure_ascii=False) + "\n")
            count += 1
    return count


def write_manifest(all_papers: list, manifest_path: Path):
    """Write a CSV manifest tracking which papers go to which group."""
    with open(manifest_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=[
            "group", "filename", "journal", "pdf_path", "keywords",
        ])
        writer.writeheader()
        for p in all_papers:
            writer.writerow({
                "group": p["group"],
                "filename": p["filename"],
                "journal": p["journal"],
                "pdf_path": p["pdf_path"],
                "keywords": p.get("keywords", ""),
            })


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Prepare papers for LLM batch verification")
    parser.add_argument("--max-chars", type=int, default=DEFAULT_MAX_CHARS,
                        help=f"Max chars of PDF text per paper (default {DEFAULT_MAX_CHARS})")
    parser.add_argument("--model", default=DEFAULT_MODEL,
                        help=f"Model name for batch requests (default {DEFAULT_MODEL})")
    parser.add_argument("--group", action="append", default=[],
                        help="Which groups to prepare: A, B, C, or all (can repeat)")
    args = parser.parse_args()

    load_env_file(ROOT / ".env")

    groups = [g.upper() for g in args.group] if args.group else ["ALL"]
    if "ALL" in groups:
        groups = ["A", "B", "C"]

    print("Loading data...")
    scan = load_scan()
    enriched = load_enriched()
    xlsx = load_xlsx()

    all_papers = []
    group_counts = {}

    if "A" in groups:
        ga = build_group_a(scan, enriched)
        ga.sort(key=lambda p: p["filename"])
        all_papers.extend(ga)
        group_counts["A"] = len(ga)
        print(f"  Group A (keyword hit, no link): {len(ga)} papers")

    if "B" in groups:
        gb = build_group_b(scan, xlsx)
        gb.sort(key=lambda p: p["filename"])
        all_papers.extend(gb)
        group_counts["B"] = len(gb)
        print(f"  Group B (xlsx=1, scanner missed): {len(gb)} papers")

    if "C" in groups:
        gc = build_group_c(scan, enriched, xlsx)
        gc.sort(key=lambda p: p["filename"])
        all_papers.extend(gc)
        group_counts["C"] = len(gc)
        print(f"  Group C (disputed, link vs xlsx=0): {len(gc)} papers")

    total = len(all_papers)
    print(f"\n  Total papers to process: {total}")

    # Estimate costs
    avg_input_tokens = args.max_chars // 4 + 500  # rough estimate
    avg_output_tokens = 200
    # gpt-4o-mini batch: $0.075/1M input, $0.30/1M output
    # gpt-4o batch: $1.25/1M input, $5.00/1M output
    if "mini" in args.model:
        cost_in = total * avg_input_tokens * 0.075 / 1_000_000
        cost_out = total * avg_output_tokens * 0.30 / 1_000_000
    else:
        cost_in = total * avg_input_tokens * 1.25 / 1_000_000
        cost_out = total * avg_output_tokens * 5.00 / 1_000_000
    est_cost = cost_in + cost_out
    print(f"  Estimated cost ({args.model}, batch): ~${est_cost:.2f}")
    print(f"  (at ~{avg_input_tokens} input tokens + ~{avg_output_tokens} output tokens per request)")

    # Generate JSONL files
    OUTPUT_DIR.mkdir(exist_ok=True)

    for group_letter in sorted(group_counts.keys()):
        group_papers = [p for p in all_papers if p["group"] == group_letter]
        out_path = OUTPUT_DIR / f"llm_batch_group_{group_letter.lower()}.jsonl"
        print(f"\nGenerating {out_path.name}...")
        n = write_jsonl(group_papers, out_path, args.model, args.max_chars)
        size_mb = out_path.stat().st_size / (1024 * 1024)
        print(f"  {n} requests, {size_mb:.1f} MB")

    # Write manifest
    manifest = OUTPUT_DIR / "llm_batch_manifest.csv"
    write_manifest(all_papers, manifest)
    print(f"\nManifest: {manifest} ({total} rows)")

    print(f"""
{'='*60}
NEXT STEPS:
{'='*60}
1. Upload JSONL file(s) to OpenAI Batch API:
   curl https://api.openai.com/v1/files \\
     -H "Authorization: Bearer $OPENAI_API_KEY" \\
     -F purpose="batch" \\
     -F file="@output/llm_batch_group_a.jsonl"

2. Create batch job:
   curl https://api.openai.com/v1/batches \\
     -H "Authorization: Bearer $OPENAI_API_KEY" \\
     -H "Content-Type: application/json" \\
     -d '{{"input_file_id": "file-xxx", "endpoint": "/v1/chat/completions", "completion_window": "24h"}}'

3. After completion, download results and run:
   python llm_batch_parse_results.py output/batch_results.jsonl

Or use the Python SDK:
   from openai import OpenAI
   client = OpenAI()
   batch_file = client.files.create(file=open("output/llm_batch_group_a.jsonl","rb"), purpose="batch")
   batch = client.batches.create(input_file_id=batch_file.id, endpoint="/v1/chat/completions", completion_window="24h")
""")


if __name__ == "__main__":
    main()
