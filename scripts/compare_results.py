"""
compare_results.py
------------------
Combines automated detections from three tiers, then compares against
the manually-coded xlsx ground truth.

Tier 1 — auto_prereg=1 (pdf_scan_results.csv)
    Keyword / phrase hit in full PDF text. Broadest net.
    1484 papers.

Tier 2 — explicit link found in PDF text (pdf_scan_results.csv, auto_link_prereg non-empty)
    A registry URL or ID (AEARCTR-…, osf.io/…, etc.) was literally present in the PDF.
    Sub-set of Tier 1: 498 papers.

Tier 3 — enrichment-verified (pdf_scan_prereg_links_dedup.csv, all_found_links non-empty)
    The enrichment pass found at least one registry link via CrossRef / OpenAlex /
    Semantic Scholar / landing page / registry title search.
    Sub-set of Tier 1: 528 papers.
    Of those, 254 reached VERIFIED / DOI_CONFIRMED / AUTHOR_CONFIRMED quality.

Ground truth: journal_articles_with_pap_2025-03-14.xlsx
  prereg = 1   → manually confirmed to have pre-registration
  prereg = 0   → manually confirmed NOT to have pre-registration
  prereg = None/blank → not yet manually reviewed

Output:
  output/comparison_report.csv  — one row per unique paper, all tiers side-by-side
  Console summary table

Usage:
  python scripts/compare_results.py
"""

import csv
import argparse
import sys
from pathlib import Path

import openpyxl
from path_utils import resolve_existing_path, resolve_output_path

PROJECT_ROOT = Path(__file__).parent.parent

DEFAULT_XLSX_PATH = PROJECT_ROOT / "journal_articles_with_pap_2025-03-14.xlsx"
DEFAULT_SCAN_CSV = PROJECT_ROOT / "output" / "pdf_scan_results.csv"
DEFAULT_ENRICHED_CSV = PROJECT_ROOT / "output" / "pdf_scan_prereg_links_dedup.csv"
FALLBACK_ENRICHED_CSV = PROJECT_ROOT / "output" / "pdf_scan_prereg_links.csv"
DEFAULT_OUTPUT_CSV = PROJECT_ROOT / "output" / "comparison_report.csv"

VERIFIED_QUALITIES = {"VERIFIED", "DOI_CONFIRMED", "AUTHOR_CONFIRMED"}

OUTPUT_FIELDS = [
    "filename",
    "journal",
    # xlsx ground truth
    "xlsx_prereg",
    "xlsx_link_prereg",
    # Tier 1: keyword hit in PDF (auto_prereg=1)
    "tier1_auto_prereg",
    # Tier 2: direct link found in PDF text
    "tier2_link_in_pdf",
    "tier2_auto_link_prereg",
    # Tier 3: enrichment pass found any link
    "tier3_enrichment_any_link",
    "tier3_all_found_links",
    # Tier 3a: enrichment link reached verified quality
    "tier3a_verified",
    "tier3a_best_link_quality",
    # combined flag (has any link evidence from Tier 2 or Tier 3)
    "has_link_evidence",
    # agreement flags
    "agreement",
]


# ── Load xlsx ground truth ────────────────────────────────────────────────────

def load_xlsx(path: Path) -> dict:
    wb = openpyxl.load_workbook(str(path), read_only=True, data_only=True)
    ws = wb.active
    headers = [c.value for c in list(ws.rows)[1]]
    by_file = {}
    for row in ws.iter_rows(min_row=3, values_only=True):
        d = dict(zip(headers, row))
        pdf = (d.get("pdf") or "").strip()
        if not pdf:
            continue
        by_file[pdf] = {
            "prereg":      d.get("prereg"),
            "link_prereg": d.get("link_prereg") or "",
            "journal":     d.get("journal") or "",
        }
    wb.close()
    return by_file


# ── Load full scan results (all rows) ────────────────────────────────────────

def load_scan_all(path: Path) -> dict:
    """Returns dict keyed by bare filename → row dict for ALL scanned papers."""
    result = {}
    if not path.exists():
        print(f"WARNING: {path} not found, skipping.", file=sys.stderr)
        return result
    with open(path, newline="", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            fname = Path(row.get("pdf_path", "")).name or row.get("filename", "")
            result[fname] = row
    return result


# ── Load enrichment results (all rows) ───────────────────────────────────────

def load_enriched_all(path: Path) -> dict:
    """Returns dict keyed by bare filename → row dict for ALL enriched papers."""
    result = {}
    if not path.exists():
        print(f"WARNING: {path} not found, skipping.", file=sys.stderr)
        return result
    with open(path, newline="", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            fname = Path(row.get("pdf_path", "")).name or row.get("filename", "")
            result[fname] = row
    return result


# ── Build full comparison ─────────────────────────────────────────────────────

def build_comparison(scan: dict, enriched: dict, xlsx: dict) -> list:
    all_fnames = set(scan) | set(enriched) | set(xlsx)
    rows = []
    for fname in sorted(all_fnames):
        s = scan.get(fname, {})
        e = enriched.get(fname, {})
        x = xlsx.get(fname, {})

        # Tier 1: auto_prereg=1 from keyword scan
        t1 = str(s.get("auto_prereg", "")).strip() == "1"

        # Tier 2: explicit link found directly in PDF text
        t2_link = (s.get("auto_link_prereg") or "").strip()
        t2 = bool(t2_link)

        # Tier 3: enrichment found any link
        t3_links = (e.get("all_found_links") or "").strip()
        t3 = bool(t3_links)

        # Tier 3a: enrichment link reached verified quality
        t3a_quality = (e.get("best_link_quality") or "").strip()
        t3a = t3a_quality in VERIFIED_QUALITIES

        # Combined: has link evidence from Tier 2 OR Tier 3
        has_link = t2 or t3

        xlsx_prereg = x.get("prereg")

        # Agreement against xlsx — based on has_link_evidence
        if xlsx_prereg is None:
            agreement = "UNREVIEWED"
        elif has_link and xlsx_prereg == 1:
            agreement = "MATCH"
        elif has_link and xlsx_prereg == 0:
            agreement = "FALSE_POSITIVE"
        elif not has_link and xlsx_prereg == 1:
            agreement = "FALSE_NEGATIVE"
        else:
            agreement = "MATCH_NEGATIVE"

        rows.append({
            "filename":                fname,
            "journal":                 x.get("journal") or s.get("journal") or e.get("journal", ""),
            "xlsx_prereg":             xlsx_prereg,
            "xlsx_link_prereg":        x.get("link_prereg", ""),
            "tier1_auto_prereg":       int(t1),
            "tier2_link_in_pdf":       int(t2),
            "tier2_auto_link_prereg":  t2_link,
            "tier3_enrichment_any_link": int(t3),
            "tier3_all_found_links":   t3_links,
            "tier3a_verified":         int(t3a),
            "tier3a_best_link_quality":t3a_quality,
            "has_link_evidence":       int(has_link),
            "agreement":               agreement,
        })
    return rows


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Compare scan and enrichment results against a reference spreadsheet")
    parser.add_argument("--xlsx", type=str, default=None,
                        help=f"Path to reference spreadsheet (default: {DEFAULT_XLSX_PATH})")
    parser.add_argument("--scan", type=str, default=None,
                        help=f"Path to scan CSV (default: {DEFAULT_SCAN_CSV})")
    parser.add_argument("--enriched", type=str, default=None,
                        help=f"Path to enriched CSV (default: {DEFAULT_ENRICHED_CSV})")
    parser.add_argument("--output", type=str, default=None,
                        help=f"Path to output CSV (default: {DEFAULT_OUTPUT_CSV})")
    args = parser.parse_args()

    xlsx_path = resolve_existing_path(args.xlsx, DEFAULT_XLSX_PATH, "reference spreadsheet")
    scan_csv = resolve_existing_path(args.scan, DEFAULT_SCAN_CSV, "scan CSV")
    enriched_csv = resolve_existing_path(
        args.enriched,
        DEFAULT_ENRICHED_CSV,
        "enriched CSV",
        fallbacks=[FALLBACK_ENRICHED_CSV],
        required=False,
    )
    output_csv = resolve_output_path(args.output, DEFAULT_OUTPUT_CSV)

    print("Loading xlsx ground truth...")
    xlsx = load_xlsx(xlsx_path)
    print(f"  {len(xlsx)} papers in xlsx")

    print("Loading pdf_scan_results.csv (full scan)...")
    scan = load_scan_all(scan_csv)
    print(f"  {len(scan)} papers scanned total")

    print(f"Loading {enriched_csv.name} (enrichment pass)...")
    enriched = load_enriched_all(enriched_csv)
    print(f"  {len(enriched)} papers in enrichment output")

    # Tier counts
    t1_set = {f for f, r in scan.items() if str(r.get("auto_prereg", "")).strip() == "1"}
    t2_set = {f for f, r in scan.items() if (r.get("auto_link_prereg") or "").strip()}
    t3_set = {f for f, r in enriched.items() if (r.get("all_found_links") or "").strip()}
    t3a_set = {f for f, r in enriched.items()
               if (r.get("best_link_quality") or "") in VERIFIED_QUALITIES}
    link_union = t2_set | t3_set

    print(f"\n── Detection tiers ──")
    print(f"  Tier 1  auto_prereg=1 (keyword hit in PDF)    : {len(t1_set)}")
    print(f"  Tier 2  direct link/ID found in PDF text      : {len(t2_set)}")
    print(f"  Tier 3  enrichment found any link              : {len(t3_set)}")
    print(f"  Tier 3a enrichment link verified quality        : {len(t3a_set)}")
    print(f"  Tier 2 ∪ 3 (any link evidence)                : {len(link_union)}")
    print(f"    Tier 2 only (in-PDF, not in enrichment)      : {len(t2_set - t3_set)}")
    print(f"    Tier 3 only (enrichment, not in-PDF)         : {len(t3_set - t2_set)}")
    print(f"    In both Tier 2 and 3                         : {len(t2_set & t3_set)}")

    print("\nBuilding comparison rows...")
    rows = build_comparison(scan, enriched, xlsx)

    # ── Summary stats against xlsx ──────────────────────────────────────────
    xlsx_prereg_1   = sum(1 for r in rows if r["xlsx_prereg"] == 1)
    xlsx_prereg_0   = sum(1 for r in rows if r["xlsx_prereg"] == 0)
    xlsx_unreviewed = sum(1 for r in rows if r["xlsx_prereg"] is None)

    matches         = sum(1 for r in rows if r["agreement"] == "MATCH")
    false_positives = sum(1 for r in rows if r["agreement"] == "FALSE_POSITIVE")
    false_negatives = sum(1 for r in rows if r["agreement"] == "FALSE_NEGATIVE")
    unreviewed_link = sum(1 for r in rows if r["agreement"] == "UNREVIEWED" and r["has_link_evidence"] == 1)

    # Also compute agreement for Tier 1 alone
    t1_matches = sum(1 for r in rows if r["tier1_auto_prereg"] == 1 and r["xlsx_prereg"] == 1)
    t1_fp      = sum(1 for r in rows if r["tier1_auto_prereg"] == 1 and r["xlsx_prereg"] == 0)
    t1_fn      = sum(1 for r in rows if r["tier1_auto_prereg"] == 0 and r["xlsx_prereg"] == 1)
    t1_prec    = t1_matches / (t1_matches + t1_fp) if (t1_matches + t1_fp) else float("nan")
    t1_rec     = t1_matches / (t1_matches + t1_fn) if (t1_matches + t1_fn) else float("nan")

    link_prec  = matches / (matches + false_positives) if (matches + false_positives) else float("nan")
    link_rec   = matches / (matches + false_negatives) if (matches + false_negatives) else float("nan")

    print(f"""
┌──────────────────────────────────────────────────────────────────┐
│               COMPARISON REPORT SUMMARY                          │
├──────────────────────────────────────────────────────────────────┤
│ xlsx ground truth                                                │
│   prereg = 1  (manually confirmed)       : {xlsx_prereg_1:>6}              │
│   prereg = 0  (manually confirmed)       : {xlsx_prereg_0:>6}              │
│   not yet reviewed (blank)               : {xlsx_unreviewed:>6}              │
├──────────────────────────────────────────────────────────────────┤
│ OUR AUTOMATED DETECTIONS                                         │
│   Tier 1  auto_prereg=1 (keyword scan)   : {len(t1_set):>6}              │
│   Tier 2  direct link found in PDF       : {len(t2_set):>6}              │
│   Tier 3  enrichment found any link      : {len(t3_set):>6}              │
│   Tier 3a enrichment verified-quality    : {len(t3a_set):>6}              │
│   Combined with link (Tier 2 ∪ 3)       : {len(link_union):>6}              │
├──────────────────────────────────────────────────────────────────┤
│ TIER 1 (auto_prereg=1) vs xlsx                                   │
│   Match  (auto=1, xlsx=1)                : {t1_matches:>6}              │
│   FP     (auto=1, xlsx=0)                : {t1_fp:>6}              │
│   FN     (auto=0, xlsx=1)                : {t1_fn:>6}              │
│   Precision                              :  {t1_prec:.1%}              │
│   Recall                                 :  {t1_rec:.1%}              │
├──────────────────────────────────────────────────────────────────┤
│ LINK EVIDENCE (Tier 2 ∪ 3) vs xlsx                               │
│   Match  (link found, xlsx=1)            : {matches:>6}              │
│   FP     (link found, xlsx=0)            : {false_positives:>6}              │
│   FN     (no link, xlsx=1)               : {false_negatives:>6}              │
│   Hits in unreviewed papers              : {unreviewed_link:>6}              │
│   Precision                              :  {link_prec:.1%}              │
│   Recall                                 :  {link_rec:.1%}              │
└──────────────────────────────────────────────────────────────────┘""")

    # ── Write output CSV ────────────────────────────────────────────────────
    output_csv.parent.mkdir(exist_ok=True)
    relevant = [r for r in rows
                if r["tier1_auto_prereg"] == 1 or r["has_link_evidence"] == 1 or r["xlsx_prereg"] == 1]
    with open(output_csv, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=OUTPUT_FIELDS)
        writer.writeheader()
        writer.writerows(relevant)

    print(f"\nWrote {len(relevant)} relevant rows → {output_csv}")
    print("  (rows where auto_prereg=1 OR has_link_evidence=1 OR xlsx_prereg=1)")


if __name__ == "__main__":
    main()
