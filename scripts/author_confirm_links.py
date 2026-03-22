"""
author_confirm_links.py
-----------------------
Post-processing pass over pdf_scan_prereg_links_dedup.csv.

For every row whose best_link_quality is UNCERTAIN, TITLE_MISMATCH, or
NO_TITLE (and that has at least one link in all_found_links):

  1. Re-fetch the registry page (via validate_link_quality).
  2. Look up paper author family names from CrossRef (by DOI, then by title).
  3. Check what fraction of those names appear on the registry page.
  4. If >= 50 % → upgrade best_link_quality to AUTHOR_CONFIRMED.
  5. Write author_match detail ("2/3 (smith, jones)") in a new column.

The dedup CSV is updated in-place (author_match + best_link_quality columns added/updated).
The script is resumable: rows that already have author_match populated are skipped.

Usage:
  python scripts/author_confirm_links.py
  python scripts/author_confirm_links.py --delay 1.2
"""

import argparse
import csv
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

from find_prereg_links import (
    validate_link_quality,
    crossref_authors_by_doi,
    crossref_authors_by_title,
    author_overlap,
)

PROJECT_ROOT = Path(__file__).parent.parent
DEDUP_CSV    = PROJECT_ROOT / "output" / "pdf_scan_prereg_links_dedup.csv"

CANDIDATES = {"UNCERTAIN", "TITLE_MISMATCH", "NO_TITLE"}
AUTHOR_MATCH_THRESHOLD = 0.50


def main():
    ap = argparse.ArgumentParser(description="Author-confirm uncertain enrichment links")
    ap.add_argument("--delay", type=float, default=1.0,
                    help="Delay seconds between API calls (default 1.0)")
    ap.add_argument("--overwrite", action="store_true", default=False,
                    help="Re-run author check even if already done")
    args = ap.parse_args()

    if not DEDUP_CSV.exists():
        sys.exit(f"ERROR: {DEDUP_CSV} not found. Run enrich_pdf_scan_links.py first.")

    with open(DEDUP_CSV, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        orig_fields = list(reader.fieldnames or [])
        rows = list(reader)

    # Add new columns if not present
    if "author_match" not in orig_fields:
        orig_fields.append("author_match")
    if "author_checked" not in orig_fields:
        orig_fields.append("author_checked")

    # Ensure all rows have the new fields
    for r in rows:
        r.setdefault("author_match", "")
        r.setdefault("author_checked", "")

    # Identify candidates
    candidates = [
        r for r in rows
        if (r.get("best_link_quality", "") in CANDIDATES)
        and (r.get("all_found_links", "").strip())
        and (args.overwrite or not r.get("author_checked", "").strip())
    ]

    print(f"Total rows in dedup CSV   : {len(rows)}")
    print(f"Candidate rows (uncertain): {len(candidates)}")
    if not candidates:
        print("Nothing to process — all uncertain rows already checked.")
        return

    upgraded = 0

    for i, r in enumerate(candidates, 1):
        filename = r.get("filename", "")
        title    = (r.get("title_guess") or "").strip()
        doi      = (r.get("doi_from_pdf") or "").strip()
        links    = [x.strip() for x in r.get("all_found_links", "").split(";") if x.strip()]
        quality  = r.get("best_link_quality", "")

        print(f"[{i}/{len(candidates)}] {filename[:70]}  quality={quality}")

        if not title:
            r["author_checked"] = "skipped_no_title"
            print("  skipped: no title")
            continue

        # Use the first (best) link
        url = links[0]

        # Re-fetch registry page to get page_text
        lq = validate_link_quality(url, title, doi)
        page_text = lq.get("page_text", "")
        time.sleep(args.delay * 0.4)

        if not page_text:
            r["author_checked"] = "skipped_unreachable"
            print("  skipped: page unreachable")
            continue

        # Get authors from CrossRef
        paper_authors = crossref_authors_by_doi(doi) if doi else []
        if not paper_authors:
            paper_authors, _ = crossref_authors_by_title(title)
        time.sleep(args.delay * 0.4)

        if not paper_authors:
            r["author_checked"] = "skipped_no_cr_authors"
            print("  skipped: no CrossRef authors found")
            continue

        # Author overlap check
        overlap, detail = author_overlap(paper_authors, page_text, url)
        r["author_match"]   = detail
        r["author_checked"] = "done"

        print(f"  authors: {detail}  overlap={overlap:.2f}")

        if overlap >= AUTHOR_MATCH_THRESHOLD:
            r["best_link_quality"] = "AUTHOR_CONFIRMED"
            upgraded += 1
            print(f"  ↑ AUTHOR_CONFIRMED")

        time.sleep(args.delay * 0.2)

    # Write updated CSV back
    with open(DEDUP_CSV, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=orig_fields, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)

    print(f"\nDone.")
    print(f"  Processed          : {len(candidates)}")
    print(f"  Upgraded to AUTHOR_CONFIRMED: {upgraded}")
    print(f"  Updated: {DEDUP_CSV}")
    print(f"\nNow rebuild the XLSX:")
    print(f"  python scripts/build_pipeline_findings_xlsx.py")


if __name__ == "__main__":
    main()
