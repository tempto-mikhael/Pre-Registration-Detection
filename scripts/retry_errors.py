"""
retry_errors.py
---------------
Re-run any rows in llm_gemini_verdicts.csv that have llm_confidence='error',
patch them in-place, then preserve all other rows in their original order.

Usage:
    python scripts/retry_errors.py --provider openrouter --openrouter-models auto
"""

import sys
import os
import csv
import json
import argparse
from pathlib import Path

# ── Resolve paths ─────────────────────────────────────────────────────────────
SCRIPT_DIR = Path(__file__).resolve().parent
ROOT       = SCRIPT_DIR.parent
sys.path.insert(0, str(SCRIPT_DIR))

# Import helpers from the main script
import llm_verify as _lv

# Use much shorter waits for retry — fail fast and rotate to next model
_lv.OPENROUTER_TRANSIENT_WAIT_BASE = 5   # was 45s
_lv.OPENROUTER_TRANSIENT_WAIT_MAX  = 15  # was 300s
_lv.OPENROUTER_ROTATION_WAIT_SECONDS = 5 # was 25s
_lv.MAX_RETRIES = 2                       # was 5

from llm_verify import (
    load_env_file,
    discover_openrouter_free_models,
    DEFAULT_OPENROUTER_FREE_MODELS,
    call_openrouter_single_with_fallback,
    build_groups,
    load_scan, load_enriched, load_xlsx,
    RESULTS_CSV, FIELDS,
)

OUTPUT_DIR = ROOT / "output"


def main():
    parser = argparse.ArgumentParser(description="Retry error rows in llm_gemini_verdicts.csv")
    parser.add_argument("--openrouter-models", default="auto")
    parser.add_argument("--max-chars", type=int, default=0,
                        help="Max chars of PDF text (0 = full PDF)")
    args = parser.parse_args()

    load_env_file(ROOT / ".env")

    api_key = os.environ.get("OPENROUTER_API_KEY")
    if not api_key:
        print("ERROR: OPENROUTER_API_KEY not set.")
        sys.exit(1)

    if args.openrouter_models.strip().lower() == "auto":
        model_pool = discover_openrouter_free_models(api_key)
    else:
        model_pool = [m.strip() for m in args.openrouter_models.split(",") if m.strip()]
    if not model_pool:
        model_pool = DEFAULT_OPENROUTER_FREE_MODELS[:]

    # ── Load CSV ──────────────────────────────────────────────────────────────
    if not RESULTS_CSV.exists():
        print("No verdicts CSV found — nothing to retry.")
        return

    with open(RESULTS_CSV, newline="", encoding="utf-8") as f:
        rows = list(csv.DictReader(f))

    error_indices = [i for i, r in enumerate(rows) if r.get("llm_confidence") == "error"]
    print(f"Total rows   : {len(rows)}")
    print(f"Error rows   : {len(error_indices)}")

    if not error_indices:
        print("No errors to retry.")
        return

    # ── Build paper lookup (filename → paper dict with pdf_path etc.) ─────────
    print("Loading paper metadata...")
    scan     = load_scan()
    enriched = load_enriched()
    xlsx     = load_xlsx()
    all_papers = build_groups(scan, enriched, xlsx, ["A", "B", "C", "D"])
    paper_by_filename = {p["filename"]: p for p in all_papers}

    # ── Retry each error row ──────────────────────────────────────────────────
    patched = 0
    still_errors = 0
    for i, row_idx in enumerate(error_indices):
        row = rows[row_idx]
        fname = row["filename"]
        paper = paper_by_filename.get(fname)
        if not paper:
            print(f"  [{i+1}/{len(error_indices)}] SKIP — paper not found in groups: {fname[:60]}")
            still_errors += 1
            continue

        print(f"  [{i+1}/{len(error_indices)}] Retrying: {fname[:60]}", flush=True)
        result, used_model, _ = call_openrouter_single_with_fallback(
            api_key=api_key,
            model_pool=model_pool[:],   # pass a copy so removal doesn't affect pool
            paper=paper,
            max_chars=args.max_chars,
        )

        if result.get("_retryable"):
            print(f"    -> STILL UNREACHABLE (all models failed) — leaving as error")
            still_errors += 1
            continue

        # Stamp model used
        result["llm_model"] = used_model or ""

        prereg_str = ("YES" if result["llm_prereg"] is True
                      else "NO" if result["llm_prereg"] is False
                      else "ERR")
        print(f"    -> {prereg_str} ({result['llm_confidence']}) via {used_model}")

        # Patch the row in-place — only update LLM fields, preserve group/filename/journal
        for field in FIELDS:
            if field in result:
                rows[row_idx][field] = result[field]
        patched += 1

    # ── Write back full CSV ───────────────────────────────────────────────────
    print(f"\nPatching CSV: {patched} fixed, {still_errors} still errored...")
    with open(RESULTS_CSV, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=FIELDS)
        writer.writeheader()
        for row in rows:
            writer.writerow({k: row.get(k, "") for k in FIELDS})

    print(f"Done. CSV updated: {RESULTS_CSV}")
    print(f"  Patched : {patched}")
    print(f"  Remaining errors: {still_errors}")


if __name__ == "__main__":
    main()
