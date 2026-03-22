#!/usr/bin/env python3
"""
Parse LLM batch results and merge back with our detection data.

Takes one or more OpenAI Batch API result JSONL files and produces
a consolidated CSV with the LLM verdicts alongside our existing tiers.

Usage:
    python llm_batch_parse_results.py output/batch_result_a.jsonl [output/batch_result_b.jsonl ...]
    python llm_batch_parse_results.py output/batch_result_*.jsonl

Output:
    output/llm_verdicts.csv
    output/llm_summary.txt
"""

import csv
import json
import sys
from pathlib import Path

ROOT = Path(__file__).parent.parent
OUTPUT_DIR = ROOT / "output"


def parse_result_files(paths: list[str]) -> list[dict]:
    results = []
    for path in paths:
        with open(path, encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                obj = json.loads(line)
                custom_id = obj.get("custom_id", "")

                # Extract group and filename from custom_id
                # format: group_{a|b|c}_{filename}
                parts = custom_id.split("_", 2)
                group = parts[1].upper() if len(parts) > 1 else "?"
                filename = parts[2] if len(parts) > 2 else custom_id

                # Extract the LLM response
                resp = obj.get("response", {})
                body = resp.get("body", {})
                choices = body.get("choices", [])

                if choices:
                    content = choices[0].get("message", {}).get("content", "")
                    try:
                        verdict = json.loads(content)
                    except json.JSONDecodeError:
                        verdict = {"prereg": None, "confidence": "error",
                                   "evidence": content[:150], "reasoning": "JSON parse failed"}
                else:
                    error = obj.get("error", {})
                    verdict = {"prereg": None, "confidence": "error",
                               "evidence": "", "reasoning": str(error)}

                usage = body.get("usage", {})

                results.append({
                    "group": group,
                    "filename": filename,
                    "llm_prereg": verdict.get("prereg"),
                    "llm_confidence": verdict.get("confidence", ""),
                    "llm_evidence": verdict.get("evidence", ""),
                    "llm_registry_url": verdict.get("registry_url") or "",
                    "llm_reasoning": verdict.get("reasoning", ""),
                    "prompt_tokens": usage.get("prompt_tokens", 0),
                    "completion_tokens": usage.get("completion_tokens", 0),
                })
    return results


def write_csv(results, path):
    fields = ["group", "filename", "llm_prereg", "llm_confidence",
              "llm_evidence", "llm_registry_url", "llm_reasoning",
              "prompt_tokens", "completion_tokens"]
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fields)
        writer.writeheader()
        writer.writerows(results)


def print_summary(results):
    total = len(results)
    by_group = {}
    for r in results:
        g = r["group"]
        if g not in by_group:
            by_group[g] = {"yes": 0, "no": 0, "error": 0, "high": 0, "medium": 0, "low": 0}
        if r["llm_prereg"] is True:
            by_group[g]["yes"] += 1
        elif r["llm_prereg"] is False:
            by_group[g]["no"] += 1
        else:
            by_group[g]["error"] += 1
        conf = r["llm_confidence"]
        if conf in by_group[g]:
            by_group[g][conf] += 1

    total_tokens = sum(r["prompt_tokens"] + r["completion_tokens"] for r in results)

    print(f"\n{'='*60}")
    print(f"LLM BATCH RESULTS SUMMARY")
    print(f"{'='*60}")
    print(f"Total responses: {total}")
    print(f"Total tokens used: {total_tokens:,}")

    for g in sorted(by_group.keys()):
        s = by_group[g]
        g_total = s["yes"] + s["no"] + s["error"]
        print(f"\n  Group {g} ({g_total} papers):")
        print(f"    prereg=true  : {s['yes']}")
        print(f"    prereg=false : {s['no']}")
        print(f"    errors       : {s['error']}")
        print(f"    high conf    : {s['high']}")
        print(f"    medium conf  : {s['medium']}")
        print(f"    low conf     : {s['low']}")

    # Group A: how many keyword-only papers does LLM confirm?
    if "A" in by_group:
        a = by_group["A"]
        a_total = a["yes"] + a["no"] + a["error"]
        if a_total:
            print(f"\n  Group A takeaway: {a['yes']}/{a_total} keyword-hit papers "
                  f"confirmed as pre-registered by LLM ({a['yes']/a_total:.1%})")

    # Group B: how many missed papers does LLM find evidence for?
    if "B" in by_group:
        b = by_group["B"]
        b_total = b["yes"] + b["no"] + b["error"]
        if b_total:
            print(f"  Group B takeaway: {b['yes']}/{b_total} scanner-missed papers "
                  f"confirmed by LLM ({b['yes']/b_total:.1%})")

    # Group C: how many disputed papers does LLM side with us vs xlsx?
    if "C" in by_group:
        c = by_group["C"]
        c_total = c["yes"] + c["no"] + c["error"]
        if c_total:
            print(f"  Group C takeaway: {c['yes']}/{c_total} disputed papers — "
                  f"LLM says pre-registered ({c['yes']/c_total:.1%}), "
                  f"siding with our links over xlsx")


def main():
    if len(sys.argv) < 2:
        print("Usage: python llm_batch_parse_results.py <result_file.jsonl> [...]")
        sys.exit(1)

    paths = sys.argv[1:]
    print(f"Parsing {len(paths)} result file(s)...")
    results = parse_result_files(paths)
    results.sort(key=lambda r: (r["group"], r["filename"]))

    out_csv = OUTPUT_DIR / "llm_verdicts.csv"
    write_csv(results, out_csv)
    print(f"Wrote {len(results)} rows → {out_csv}")

    print_summary(results)


if __name__ == "__main__":
    main()
