#!/usr/bin/env python3
"""
Run the full PDF-first preregistration detection pipeline end to end.

This entrypoint is intended for a brand-new local PDF folder and produces:
  - output/results.csv
  - output/results.xlsx

It runs the reusable pipeline steps in order:
  1. scan_pdf_folder.py
  2. enrich_pdf_scan_links.py
  3. dedup_pdf_scan_prereg_links.py
  4. author_confirm_links.py
  5. llm_verify.py
  6. build_pipeline_findings_xlsx.py
"""

from __future__ import annotations

import argparse
import os
import subprocess
import sys
from datetime import datetime
from pathlib import Path


ROOT = Path(__file__).parent.parent
OUTPUT_DIR = ROOT / "output"
DEFAULT_SCAN_CSV = OUTPUT_DIR / "pdf_scan_results.csv"
DEFAULT_LINKS_CSV = OUTPUT_DIR / "pdf_scan_prereg_links.csv"
DEFAULT_DEDUP_CSV = OUTPUT_DIR / "pdf_scan_prereg_links_dedup.csv"
DEFAULT_VERDICTS_CSV = OUTPUT_DIR / "llm_verdicts.csv"
DEFAULT_RESULTS_CSV = OUTPUT_DIR / "results.csv"
DEFAULT_RESULTS_XLSX = OUTPUT_DIR / "results.xlsx"
DEFAULT_LOG = OUTPUT_DIR / "pipeline_run.log"


def append_log(log_path: Path, text: str) -> None:
    log_path.parent.mkdir(exist_ok=True)
    with open(log_path, "a", encoding="utf-8", errors="ignore") as handle:
        handle.write(text)


def quote_cmd(parts: list[str]) -> str:
    rendered = []
    for part in parts:
        if any(ch.isspace() for ch in part):
            rendered.append(f'"{part}"')
        else:
            rendered.append(part)
    return " ".join(rendered)


def run_step(name: str, cmd: list[str], log_path: Path) -> None:
    started = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    header = f"\n[{started}] STEP: {name}\nCMD: {quote_cmd(cmd)}\n"
    print(f"\n== {name} ==")
    print(quote_cmd(cmd))
    append_log(log_path, header)

    proc = subprocess.run(
        cmd,
        cwd=str(ROOT),
        text=True,
        capture_output=True,
        env=os.environ.copy(),
    )

    if proc.stdout:
        print(proc.stdout, end="" if proc.stdout.endswith("\n") else "\n")
        append_log(log_path, proc.stdout if proc.stdout.endswith("\n") else proc.stdout + "\n")
    if proc.stderr:
        print(proc.stderr, file=sys.stderr, end="" if proc.stderr.endswith("\n") else "\n")
        append_log(log_path, proc.stderr if proc.stderr.endswith("\n") else proc.stderr + "\n")

    finished = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    footer = f"[{finished}] EXIT CODE: {proc.returncode}\n"
    append_log(log_path, footer)

    if proc.returncode != 0:
        raise SystemExit(f"Pipeline step failed: {name}")


def main() -> None:
    parser = argparse.ArgumentParser(description="Run the full PDF-first preregistration pipeline")
    parser.add_argument("--folder", required=True, help="Root folder containing PDFs to scan")
    parser.add_argument("--provider", choices=["gemini", "openrouter"], default="gemini",
                        help="LLM provider for llm_verify.py")
    parser.add_argument("--model", default=None, help="Optional explicit model for llm_verify.py")
    parser.add_argument("--openrouter-models", default="auto",
                        help="OpenRouter model pool passed to llm_verify.py")
    parser.add_argument("--max-chars", type=int, default=0,
                        help="Max characters of PDF text passed to llm_verify.py (0 = full text)")
    parser.add_argument("--batch-size", type=int, default=10,
                        help="Batch size passed to llm_verify.py")
    parser.add_argument("--scan-sample", type=int, default=None,
                        help="Optional sample size for scan_pdf_folder.py")
    parser.add_argument("--enrich-sample", type=int, default=None,
                        help="Optional sample size for enrich_pdf_scan_links.py")
    parser.add_argument("--delay", type=float, default=1.0,
                        help="Base delay for enrichment and author-confirm steps")
    parser.add_argument("--prereg-only", action="store_true",
                        help="Pass --prereg-only to scan_pdf_folder.py")
    parser.add_argument("--reset-llm", action="store_true",
                        help="Reset llm_verify.py verdict output before running")
    parser.add_argument("--scan-output", default=str(DEFAULT_SCAN_CSV))
    parser.add_argument("--links-output", default=str(DEFAULT_LINKS_CSV))
    parser.add_argument("--dedup-output", default=str(DEFAULT_DEDUP_CSV))
    parser.add_argument("--verdicts-output", default=str(DEFAULT_VERDICTS_CSV))
    parser.add_argument("--results-csv", default=str(DEFAULT_RESULTS_CSV))
    parser.add_argument("--results-xlsx", default=str(DEFAULT_RESULTS_XLSX))
    parser.add_argument("--log", default=str(DEFAULT_LOG), help="Path to pipeline run log")
    args = parser.parse_args()

    folder = Path(args.folder)
    if not folder.exists():
        raise SystemExit(f"PDF folder not found: {folder}")

    log_path = Path(args.log)
    append_log(
        log_path,
        f"\n=== New pipeline run: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} ===\n",
    )

    python = sys.executable

    scan_cmd = [
        python,
        str(ROOT / "scripts" / "scan_pdf_folder.py"),
        "--folder",
        str(folder),
        "--output",
        args.scan_output,
    ]
    if args.scan_sample:
        scan_cmd.extend(["--sample", str(args.scan_sample)])
    if args.prereg_only:
        scan_cmd.append("--prereg-only")

    enrich_cmd = [
        python,
        str(ROOT / "scripts" / "enrich_pdf_scan_links.py"),
        "--scan",
        args.scan_output,
        "--output",
        args.links_output,
        "--delay",
        str(args.delay),
    ]
    if args.enrich_sample:
        enrich_cmd.extend(["--sample", str(args.enrich_sample)])

    dedup_cmd = [
        python,
        str(ROOT / "scripts" / "dedup_pdf_scan_prereg_links.py"),
        "--input",
        args.links_output,
        "--output",
        args.dedup_output,
    ]

    author_cmd = [
        python,
        str(ROOT / "scripts" / "author_confirm_links.py"),
        "--enriched",
        args.dedup_output,
        "--output",
        args.dedup_output,
        "--delay",
        str(args.delay),
    ]

    llm_cmd = [
        python,
        str(ROOT / "scripts" / "llm_verify.py"),
        "--group",
        "all",
        "--provider",
        args.provider,
        "--max-chars",
        str(args.max_chars),
        "--batch-size",
        str(args.batch_size),
        "--scan",
        args.scan_output,
        "--enriched",
        args.dedup_output,
        "--results-csv",
        args.verdicts_output,
    ]
    if args.provider == "openrouter":
        llm_cmd.extend(["--openrouter-models", args.openrouter_models])
    if args.model:
        llm_cmd.extend(["--model", args.model])
    if args.reset_llm:
        llm_cmd.append("--reset")

    build_cmd = [
        python,
        str(ROOT / "scripts" / "build_pipeline_findings_xlsx.py"),
        "--scan",
        args.scan_output,
        "--links",
        args.dedup_output,
        "--verdicts",
        args.verdicts_output,
        "--output-csv",
        args.results_csv,
        "--output-xlsx",
        args.results_xlsx,
    ]

    for name, cmd in [
        ("Scan PDFs", scan_cmd),
        ("Enrich Registry Links", enrich_cmd),
        ("Deduplicate Links", dedup_cmd),
        ("Author Confirm Links", author_cmd),
        ("LLM Verify", llm_cmd),
        ("Build Results", build_cmd),
    ]:
        run_step(name, cmd, log_path)

    print("\nPipeline complete.")
    print(f"CSV : {args.results_csv}")
    print(f"XLSX: {args.results_xlsx}")
    print(f"Log : {log_path}")


if __name__ == "__main__":
    main()
