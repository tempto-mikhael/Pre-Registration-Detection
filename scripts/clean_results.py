#!/usr/bin/env python3
"""Remove empty rows from a results CSV while creating a backup."""

import argparse
import csv
import shutil
from pathlib import Path

from path_utils import resolve_existing_path


ROOT = Path(__file__).parent.parent
DEFAULT_RESULTS_CSV = ROOT / "output" / "results.csv"


def main():
    parser = argparse.ArgumentParser(description="Remove empty rows from a results CSV")
    parser.add_argument("--results", type=str, default=None,
                        help=f"Path to results CSV (default: {DEFAULT_RESULTS_CSV})")
    parser.add_argument("--backup", type=str, default=None,
                        help="Optional backup path (default: <results>.bak)")
    args = parser.parse_args()

    results_csv = resolve_existing_path(args.results, DEFAULT_RESULTS_CSV, "results CSV")
    backup_csv = Path(args.backup) if args.backup else results_csv.with_suffix(".csv.bak")

    with open(results_csv, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        fieldnames = list(reader.fieldnames or [])
        rows = list(reader)

    before = len(rows)
    clean = [
        row for row in rows
        if row.get("journal") or row.get("doi") or row.get("pdf_filename")
    ]
    after = len(clean)

    shutil.copy(results_csv, backup_csv)

    with open(results_csv, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(clean)

    print(f"Removed {before - after} empty rows ({before} -> {after})")
    print(f"Backup saved to {backup_csv}")


if __name__ == "__main__":
    main()
