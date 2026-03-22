"""
no_title_ai_check.py
--------------------
For every row in pdf_scan_prereg_links_dedup.csv whose best_link_quality is
NO_TITLE or AI_LINK_REJECTED (from a previous bogus run):

  Reads the PAPER's own PDF text and asks the LLM whether the paper reports
  its OWN pre-registration at the registry URL(s) found by the scanner.

    This mirrors llm_verify's approach: analyse the paper text, not the
  (often inaccessible) registry page.

  If confirmed -> upgrade best_link_quality to AI_LINK_CONFIRMED.
  If rejected  -> downgrade to AI_LINK_REJECTED.

Resumable: rows whose ai_link_check starts with "confirmed_" or "skipped_"
are skipped on rerun.  Error / rejected rows are retried automatically.

Usage:
  python scripts/no_title_ai_check.py
  python scripts/no_title_ai_check.py --delay 2.0 --model deepseek/deepseek-chat-v3-0324:free
  python scripts/no_title_ai_check.py --overwrite   # redo everything
"""

import argparse
import csv
import json
import os
import re
import sys
import time
from pathlib import Path

import fitz  # PyMuPDF

sys.path.insert(0, str(Path(__file__).parent))

from llm_verify import (
    load_env_file,
    DEFAULT_OPENROUTER_FREE_MODELS,
    SYSTEM_INSTRUCTION,
    openrouter_chat_completion,
    RateLimitError,
    clean_llm_content,
    discover_openrouter_free_models,
    is_no_endpoint_message,
    OPENROUTER_ROTATION_WAIT_SECONDS,
    OPENROUTER_TRANSIENT_WAIT_BASE,
    OPENROUTER_TRANSIENT_WAIT_MAX,
)

PROJECT_ROOT = Path(__file__).parent.parent
DEDUP_CSV    = PROJECT_ROOT / "output" / "pdf_scan_prereg_links_dedup.csv"
ENV_FILE     = PROJECT_ROOT / ".env"

# Rows to target -- both original NO_TITLE and previous bad AI_LINK_REJECTED
TARGET_QUALITIES = {"NO_TITLE", "AI_LINK_REJECTED"}
CONFIRMED_QUALITY = "AI_LINK_CONFIRMED"
REJECTED_QUALITY  = "AI_LINK_REJECTED"

# Maximum paper text to send (chars).  0 = full PDF.
MAX_CHARS = 40_000

PROMPT_TEMPLATE = """\
Paper filename: {filename}

Our automated pipeline found these pre-registration links associated with
this paper:
{links_section}

However, we could not verify these links by fetching the registry page
(the page may be private, JavaScript-rendered, or unavailable).

Please read the paper text carefully and determine:
1. Does this paper report its OWN pre-registration?
2. Do the links above plausibly belong to THIS paper's pre-registration,
   or are they from cited/referenced studies?

Look for explicit mentions of the URL(s) above, or references to
pre-registration, pre-analysis plans, registered reports, or trial
registries in the context of THIS paper's own study.
Do not reject a link only because the paper describes it briefly as
"materials", "supplementary", or "replication" if the linked page still
appears to be the paper's own registry record.

--- PAPER TEXT (beginning + end sample) ---
{text}"""


def extract_text(pdf_path: str, max_chars: int = MAX_CHARS) -> str:
    """Extract PDF text, beginning + end sample if over budget."""
    try:
        doc = fitz.open(pdf_path)
        all_pages = [page.get_text("text") for page in doc]
        doc.close()
        full_text = "\n".join(all_pages)

        if max_chars == 0 or len(full_text) <= max_chars:
            return full_text

        head_budget = int(max_chars * 0.65)
        tail_budget = max_chars - head_budget
        head = full_text[:head_budget]
        tail = full_text[-tail_budget:]
        return (
            head
            + "\n\n[... middle section omitted for length ...\n"
            + " Note: text continues from end of paper below ...]\n\n"
            + tail
        )
    except Exception as e:
        return f"[ERROR extracting text: {e}]"


def _parse_verdict(content: str) -> dict:
    """Robustly extract a verdict dict from LLM output.

    Handles: valid JSON, truncated JSON, unquoted values, plain-text fallback.
    Returns dict with keys: confirmed (bool|None), confidence, reasoning.
    """
    if not content or not content.strip():
        return {"confirmed": None, "confidence": "error", "reasoning": "(empty LLM response)"}

    # Strategy 1: find a complete JSON object with "prereg" key
    m = re.search(r'\{[^{}]*"prereg"\s*:', content, re.DOTALL)
    if m:
        # Try to find the matching closing brace
        start = m.start()
        brace_text = content[start:]
        # Find first } after the match
        close = brace_text.find("}")
        if close > 0:
            candidate = brace_text[:close + 1]
            try:
                obj = json.loads(candidate)
                return {
                    "confirmed": obj.get("prereg"),
                    "confidence": obj.get("confidence", ""),
                    "reasoning": obj.get("reasoning", ""),
                }
            except json.JSONDecodeError:
                pass
            # Try fixing truncated/unquoted JSON with regex extraction
            prereg_m = re.search(r'"prereg"\s*:\s*(true|false)', candidate, re.I)
            conf_m = re.search(r'"confidence"\s*:\s*"?(high|medium|low)"?', candidate, re.I)
            if prereg_m:
                return {
                    "confirmed": prereg_m.group(1).lower() == "true",
                    "confidence": conf_m.group(1).lower() if conf_m else "",
                    "reasoning": content[:200],
                }

    # Strategy 2: find "confirmed" key
    m = re.search(r'\{[^{}]*"confirmed"\s*:', content, re.DOTALL)
    if m:
        start = m.start()
        brace_text = content[start:]
        close = brace_text.find("}")
        if close > 0:
            candidate = brace_text[:close + 1]
            try:
                return json.loads(candidate)
            except json.JSONDecodeError:
                pass

    # Strategy 3: regex extraction from anywhere in text
    prereg_m = re.search(r'"prereg"\s*:\s*(true|false)', content, re.I)
    if prereg_m:
        conf_m = re.search(r'"confidence"\s*:\s*"?(high|medium|low)"?', content, re.I)
        return {
            "confirmed": prereg_m.group(1).lower() == "true",
            "confidence": conf_m.group(1).lower() if conf_m else "",
            "reasoning": content[:200],
        }

    # Strategy 4: plain-text heuristic — look for clear statements
    lower = content.lower()
    if any(phrase in lower for phrase in [
        "paper reports its own pre-registration",
        "paper explicitly states",
        "pre-registered",
        "preregistered",
        "pre-analysis plan",
    ]):
        # Seems like the LLM said yes but didn't output JSON
        is_negative = any(neg in lower for neg in [
            "does not report",
            "does not mention",
            "no pre-registration",
            "not pre-registered",
            "does not reference",
            "no references to pre-reg",
            "no evidence",
        ])
        return {
            "confirmed": not is_negative,
            "confidence": "medium",
            "reasoning": content[:200],
        }

    return {"confirmed": None, "confidence": "error", "reasoning": content[:200]}


def call_verify(api_key: str, model: str, filename: str,
                links: list, pdf_text: str) -> dict:
    """Ask LLM if the paper text reports its own pre-registration at the given links.

    Uses openrouter_chat_completion() from llm_verify so all retry /
    error-handling logic is shared.
    """
    links_section = "\n".join(f"  - {url}" for url in links)
    prompt = PROMPT_TEMPLATE.format(
        filename=filename,
        links_section=links_section,
        text=pdf_text,
    )

    text, _in, _out = openrouter_chat_completion(api_key, model, prompt, 1500)

    # Clean thinking blocks & markdown fences
    content = clean_llm_content(text.strip())

    return _parse_verdict(content)


def main():
    ap = argparse.ArgumentParser(
        description="AI-check NO_TITLE / AI_LINK_REJECTED registry links via OpenRouter"
    )
    ap.add_argument("--delay", type=float, default=2.0,
                    help="Delay between API calls (default 2.0s)")
    ap.add_argument("--model", type=str, default=None,
                    help="Primary OpenRouter model (default: auto-discover free models)")
    ap.add_argument("--overwrite", action="store_true", default=False,
                    help="Re-check all rows, even previously confirmed ones")
    args = ap.parse_args()

    load_env_file(ENV_FILE)
    api_key = os.environ.get("OPENROUTER_API_KEY", "").strip()
    if not api_key:
        sys.exit("ERROR: Set OPENROUTER_API_KEY env var or add it to .env file.")

    if not DEDUP_CSV.exists():
        sys.exit(f"ERROR: {DEDUP_CSV} not found.")

    with open(DEDUP_CSV, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        orig_fields = list(reader.fieldnames or [])
        rows = list(reader)

    # Add new columns if missing
    for col in ("ai_link_check", "ai_link_reasoning"):
        if col not in orig_fields:
            orig_fields.append(col)
    for r in rows:
        r.setdefault("ai_link_check", "")
        r.setdefault("ai_link_reasoning", "")

    # Select candidates: NO_TITLE or AI_LINK_REJECTED rows with links & pdf_path
    # Skip rows already confirmed or rejected (unless --overwrite)
    DONE_PREFIXES = ("confirmed_", "rejected_", "skipped_no_pdf")
    candidates = [
        r for r in rows
        if r.get("best_link_quality", "") in TARGET_QUALITIES
        and r.get("all_found_links", "").strip()
        and r.get("pdf_path", "").strip()
        and (args.overwrite or not r.get("ai_link_check", "").strip().startswith(DONE_PREFIXES))
    ]

    # Build model pool
    discovered = discover_openrouter_free_models(api_key)
    if args.model:
        model_pool = [args.model] + [m for m in discovered if m != args.model]
    else:
        model_pool = list(discovered)
    if not model_pool:
        model_pool = list(DEFAULT_OPENROUTER_FREE_MODELS)

    print(f"Total rows in dedup CSV   : {len(rows)}")
    print(f"Rows to check             : {len(candidates)}")
    print(f"Model pool                : {model_pool[0]} (+{len(model_pool)-1} fallbacks)")
    if not candidates:
        print("Nothing to process.")
        return

    confirmed_count = 0
    rejected_count  = 0
    error_count     = 0

    for i, r in enumerate(candidates, 1):
        filename  = r.get("filename", "")
        pdf_path  = r.get("pdf_path", "").strip()
        links     = [x.strip() for x in r.get("all_found_links", "").split(";") if x.strip()]

        print(f"[{i}/{len(candidates)}] {filename[:65]}")

        if not pdf_path or not Path(pdf_path).exists():
            r["ai_link_check"] = "skipped_no_pdf"
            print("  skipped: PDF not found")
            continue

        # Extract paper text
        pdf_text = extract_text(pdf_path, MAX_CHARS)
        if pdf_text.startswith("[ERROR"):
            r["ai_link_check"] = "error_pdf_extraction"
            print(f"  error: {pdf_text[:80]}")
            error_count += 1
            continue

        # Call AI -- rotate through model pool (matches llm_verify logic)
        verdict = None
        attempts_left = len(model_pool)
        idx = 0
        last_error = None
        while attempts_left > 0 and model_pool:
            idx = idx % len(model_pool)
            model = model_pool[idx]
            try:
                verdict = call_verify(api_key, model, filename, links, pdf_text)
                # If we got an empty/unparsable response, try another model
                if verdict.get("confirmed") is None and attempts_left > 1:
                    reason = verdict.get("reasoning", "")
                    print(f"  empty/unparsable from {model.split('/')[-1][:25]}: {reason[:60]}")
                    idx = (idx + 1) % len(model_pool)
                    attempts_left -= 1
                    time.sleep(args.delay)
                    continue
                # Success -- rotate pool so this model is first next time
                model_pool[:] = model_pool[idx:] + model_pool[:idx]
                print(f"  model: {model.split('/')[-1][:30]}")
                break
            except RateLimitError:
                last_error = "rate-limited"
                print(f"  rate-limited on {model}, rotating...")
                if attempts_left > 1:
                    time.sleep(OPENROUTER_ROTATION_WAIT_SECONDS)
                idx = (idx + 1) % len(model_pool)
                attempts_left -= 1
            except RuntimeError as e:
                last_error = str(e)
                if is_no_endpoint_message(last_error):
                    print(f"  removing unavailable model: {model}")
                    model_pool.pop(idx)
                    attempts_left = min(attempts_left - 1, len(model_pool))
                    if not model_pool:
                        break
                    idx = idx % len(model_pool)
                    continue
                print(f"  error on {model}: {last_error[:100]}")
                if attempts_left > 1:
                    wait = min(
                        OPENROUTER_TRANSIENT_WAIT_BASE * (len(model_pool) - attempts_left + 1),
                        OPENROUTER_TRANSIENT_WAIT_MAX,
                    )
                    print(f"  cooling down {wait}s before next model...")
                    time.sleep(wait)
                idx = (idx + 1) % len(model_pool)
                attempts_left -= 1

        if verdict is None:
            r["ai_link_check"] = "error_all_models_failed"
            print(f"  all models failed: {(last_error or '')[:80]}")
            error_count += 1
            continue

        confirmed  = verdict.get("confirmed")
        confidence = verdict.get("confidence", "")
        reasoning  = verdict.get("reasoning", "")

        r["ai_link_reasoning"] = reasoning[:300]

        if confirmed is True:
            r["best_link_quality"] = CONFIRMED_QUALITY
            r["ai_link_check"] = f"confirmed_{confidence}"
            confirmed_count += 1
            print(f"  CONFIRMED ({confidence}) -- {reasoning[:80]}")
        elif confirmed is False:
            r["best_link_quality"] = REJECTED_QUALITY
            r["ai_link_check"] = f"rejected_{confidence}"
            rejected_count += 1
            print(f"  REJECTED ({confidence}) -- {reasoning[:80]}")
        else:
            r["ai_link_check"] = "error_parse"
            error_count += 1
            print(f"  ? parse error: {reasoning[:80]}")

        # Save after every row (resumable)
        with open(DEDUP_CSV, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=orig_fields, extrasaction="ignore")
            writer.writeheader()
            writer.writerows(rows)

        time.sleep(args.delay)

    print(f"\nDone.")
    print(f"  Checked    : {len(candidates)}")
    print(f"  Confirmed  : {confirmed_count}  -> AI_LINK_CONFIRMED")
    print(f"  Rejected   : {rejected_count}   -> AI_LINK_REJECTED")
    print(f"  Errors     : {error_count}")
    print(f"  Updated    : {DEDUP_CSV}")
    print(f"\nRebuild the XLSX:")
    print(f"  python scripts/build_pipeline_findings_xlsx.py")


if __name__ == "__main__":
    main()
