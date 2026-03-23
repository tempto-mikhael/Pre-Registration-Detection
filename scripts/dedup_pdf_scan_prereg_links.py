#!/usr/bin/env python3
"""
Deduplicate enrichment output so downstream scripts can work from one row per paper.

Default input:
  output/pdf_scan_prereg_links.csv

Default output:
  output/pdf_scan_prereg_links_dedup.csv
"""

import argparse
import csv
from pathlib import Path

from path_utils import resolve_existing_path, resolve_output_path


ROOT = Path(__file__).parent.parent
OUTPUT_DIR = ROOT / "output"
DEFAULT_INPUT_CSV = OUTPUT_DIR / "pdf_scan_prereg_links.csv"
DEFAULT_OUTPUT_CSV = OUTPUT_DIR / "pdf_scan_prereg_links_dedup.csv"

QUALITY_RANK = {
    "AUTHOR_CONFIRMED": 6,
    "DOI_CONFIRMED": 5,
    "VERIFIED": 4,
    "AI_LINK_CONFIRMED": 3,
    "UNCERTAIN": 2,
    "NO_TITLE": 1,
    "TITLE_MISMATCH": 1,
    "UNREACHABLE": 0,
    "ai": 0,
    "": 0,
}

MULTI_VALUE_FIELDS = {
    "auto_link_prereg",
    "crossref_links",
    "s2_links",
    "landing_page_links",
    "openalex_links",
    "openalex_refs_links",
    "egap_links",
    "aearctr_html_links",
    "datacite_links",
    "osf_title_links",
    "all_found_links",
}


def split_multi(raw: str | None) -> list[str]:
    if not raw:
        return []
    return [part.strip() for part in str(raw).split(";") if part.strip()]


def merge_multi(rows: list[dict], field: str) -> str:
    seen = set()
    merged = []
    for row in rows:
        for value in split_multi(row.get(field)):
            if value not in seen:
                seen.add(value)
                merged.append(value)
    return "; ".join(merged)


def row_score(row: dict) -> tuple[int, int, int]:
    quality = QUALITY_RANK.get((row.get("best_link_quality") or "").strip(), 0)
    non_empty = sum(1 for value in row.values() if str(value or "").strip())
    links = len(split_multi(row.get("all_found_links")))
    return quality, links, non_empty


def row_key(row: dict) -> str:
    filename = (row.get("filename") or "").strip()
    if filename:
        return filename
    pdf_path = (row.get("pdf_path") or "").strip()
    if pdf_path:
        return Path(pdf_path).name or pdf_path
    return ""


def choose_best_row(rows: list[dict]) -> dict:
    return max(rows, key=row_score)


def main():
    parser = argparse.ArgumentParser(description="Deduplicate pdf_scan_prereg_links.csv")
    parser.add_argument("--input", type=str, default=None,
                        help=f"Path to raw enriched CSV (default: {DEFAULT_INPUT_CSV})")
    parser.add_argument("--output", type=str, default=None,
                        help=f"Path to deduplicated CSV (default: {DEFAULT_OUTPUT_CSV})")
    args = parser.parse_args()

    input_csv = resolve_existing_path(args.input, DEFAULT_INPUT_CSV, "raw enriched CSV")
    output_csv = resolve_output_path(args.output, DEFAULT_OUTPUT_CSV)

    with open(input_csv, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        fieldnames = list(reader.fieldnames or [])
        rows = list(reader)

    grouped = {}
    for row in rows:
        key = row_key(row)
        if not key:
            continue
        grouped.setdefault(key, []).append(row)

    deduped_rows = []
    for key in sorted(grouped):
        group = grouped[key]
        best_row = choose_best_row(group)
        best = dict(best_row)
        preferred_group = [best_row] + [row for row in group if row is not best_row]
        for field in MULTI_VALUE_FIELDS:
            if field in best:
                # Preserve the best row's link ordering first so downstream
                # scripts keep the strongest candidate at the front.
                best[field] = merge_multi(preferred_group, field)
        best["filename"] = (best.get("filename") or key).strip()
        best["dedup_row_count"] = len(group)
        deduped_rows.append(best)

    if "dedup_row_count" not in fieldnames:
        fieldnames.append("dedup_row_count")

    output_csv.parent.mkdir(exist_ok=True)
    with open(output_csv, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(deduped_rows)

    print(f"Input rows  : {len(rows)}")
    print(f"Unique rows : {len(deduped_rows)}")
    print(f"Output      : {output_csv}")


if __name__ == "__main__":
    main()
