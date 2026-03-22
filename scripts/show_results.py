#!/usr/bin/env python3
import argparse
import csv
from collections import Counter
from pathlib import Path

from path_utils import resolve_existing_path


ROOT = Path(__file__).parent.parent
DEFAULT_RESULTS_CSV = ROOT / "output" / "results.csv"


def main():
    parser = argparse.ArgumentParser(description="Quick console summary of results.csv")
    parser.add_argument("--results", type=str, default=None,
                        help=f"Path to results CSV (default: {DEFAULT_RESULTS_CSV})")
    args = parser.parse_args()

    results_csv = resolve_existing_path(args.results, DEFAULT_RESULTS_CSV, "results CSV")

    with open(results_csv, newline="", encoding="utf-8") as f:
        rows = list(csv.DictReader(f))

    print(f"{'Row':<5} {'DOI':<30} {'DL':<3} {'src':<10} | {'auto_pre':<9} {'xlsx_pre':<9} | {'FP/FN/OK':<8} | {'nodata':<8}")
    print("-" * 100)
    for r in rows:
        dl = r.get("oa_pdf_downloaded", "")
        src = r.get("text_source", "?")
        a_pr = r.get("auto_prereg", "")
        x_pr = r.get("xlsx_prereg", "")
        if a_pr == "1" and x_pr == "1":
            verdict = "TP"
        elif a_pr == "0" and x_pr == "0":
            verdict = "TN"
        elif a_pr == "1" and x_pr == "0":
            verdict = "FP !!"
        else:
            verdict = "FN !!"
        doi_short = (r.get("doi") or "")[:29]
        print(f"{r.get('row_num', ''):<5} {doi_short:<30} {dl:<3} {src:<10} | {a_pr:<9} {x_pr:<9} | {verdict:<8} | {r.get('auto_no_data', '')}")

    verdicts = []
    for r in rows:
        a = r.get("auto_prereg", "")
        x = r.get("xlsx_prereg", "")
        if a == "1" and x == "1":
            verdicts.append("TP")
        elif a == "0" and x == "0":
            verdicts.append("TN")
        elif a == "1" and x == "0":
            verdicts.append("FP")
        else:
            verdicts.append("FN")
    counts = Counter(verdicts)
    dl_count = sum(1 for r in rows if r.get("oa_pdf_downloaded") == "1")

    print()
    print(f"Summary: TP={counts['TP']} TN={counts['TN']} FP={counts['FP']} FN={counts['FN']}")
    print(f"PDFs downloaded: {dl_count}/{len(rows)}")


if __name__ == "__main__":
    main()
