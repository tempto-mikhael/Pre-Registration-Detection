"""
find_title_mismatches.py
------------------------
Detect potential false negatives caused by a changed paper title.

A pre-registered study sometimes gets published under a title that differs from
the one registered, so keyword detection (find_prereg_links.py) misses it.

Strategy
--------
For each paper in results.csv with auto_prereg=0 (not yet detected):
  1.  Search the AEA RCT Registry and EGAP for the paper's published title.
  2.  Compare the paper title against registry entry titles using a normalised
      sequence-similarity ratio.
  3.  Flag entries that exceed SIMILARITY_THRESHOLD (default 0.60) as
      "possible title-change false negative".
  4.  Also run an author-level cross-check when a CrossRef author list is
      available (looks for author surname overlap with the registry hit).

Output:  output/title_mismatch_candidates.csv

Usage:
  python scripts/find_title_mismatches.py [options]

Options:
  --min-sim  SIM    Minimum similarity score to include (default 0.60)
  --delay    SEC    Seconds between external API calls (default 1.2)
  --limit    N      Only process first N undetected rows (default 0 = all)
  --journal  NAME   Filter to a single journal slug (optional)

Notes:
  - Registry search APIs are queried at ~1 req/s; for all ~2 700 undetected
    papers expect ~2 h runtime.  Use --limit for a quick test.
  - The script is resumable: set --limit and re-run from where you left off
    using the existing output file (rows not yet in the file are skipped).
"""

import argparse
import csv
import re
import sys
import time
from difflib import SequenceMatcher
from pathlib import Path

import requests
from bs4 import BeautifulSoup

PROJECT_ROOT = Path(__file__).parent.parent
RESULTS_CSV  = PROJECT_ROOT / "output" / "results.csv"
OUTPUT_CSV   = PROJECT_ROOT / "output" / "title_mismatch_candidates.csv"
CONTACT_EMAIL = "makgyumyush22@ku.edu.tr"

SESSION = requests.Session()
SESSION.headers.update({"User-Agent": f"ercautomation/1.0 (mailto:{CONTACT_EMAIL})"})
BROWSER_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                  "AppleWebKit/537.36 Chrome/120.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,*/*",
}

SIMILARITY_THRESHOLD = 0.60   # minimum ratio to report a candidate

OUTPUT_FIELDS = [
    "row_num", "journal", "title", "doi", "pub_year",
    "source", "registry_url", "registry_title",
    "sim_score", "author_overlap",
    "verdict",
]


# ─────────────────────────────────────────────────────────────────────────────
# Text normalisation
# ─────────────────────────────────────────────────────────────────────────────

def normalise_title(t: str) -> str:
    """Lower-case, strip punctuation/articles for comparison."""
    t = (t or "").lower()
    t = re.sub(r"[^\w\s]", " ", t)
    t = re.sub(r"\b(a|an|the|and|or|in|on|of|for|to|with|from|by|at)\b", " ", t)
    return re.sub(r"\s+", " ", t).strip()


def similarity(a: str, b: str) -> float:
    """Normalised sequence similarity in [0, 1]."""
    na = normalise_title(a)
    nb = normalise_title(b)
    if not na or not nb:
        return 0.0
    return SequenceMatcher(None, na, nb).ratio()


# ─────────────────────────────────────────────────────────────────────────────
# Author helpers
# ─────────────────────────────────────────────────────────────────────────────

def crossref_authors(doi: str) -> list[str]:
    """Return list of lowercase author family names from CrossRef."""
    if not doi or doi.startswith(("TITLE_SLUG:", "PII:")):
        return []
    try:
        r = SESSION.get(f"https://api.crossref.org/works/{doi}", timeout=15)
        if r.status_code != 200:
            return []
        authors = r.json().get("message", {}).get("author", []) or []
        return [a.get("family", "").lower() for a in authors if a.get("family")]
    except Exception:
        return []


def author_overlap_score(paper_authors: list[str], registry_text: str) -> str:
    """Return 'X/N match' if paper authors appear in registry text, else ''."""
    if not paper_authors or not registry_text:
        return ""
    rt_lower = registry_text.lower()
    matches = [a for a in paper_authors if a and a in rt_lower]
    if not matches:
        return ""
    return f"{len(matches)}/{len(paper_authors)} surname(s) match: {', '.join(matches)}"


# ─────────────────────────────────────────────────────────────────────────────
# Registry searches
# ─────────────────────────────────────────────────────────────────────────────

def search_aearctr(title: str) -> list[dict]:
    """
    Search AEA RCT Registry public HTML.
    Returns list of {url, title, raw_text}.
    """
    if not title:
        return []
    try:
        r = requests.get(
            "https://www.socialscienceregistry.org/trials",
            params={"search": title[:80]},
            headers=BROWSER_HEADERS,
            timeout=25,
        )
        if r.status_code != 200:
            return []
        soup = BeautifulSoup(r.text, "lxml")
        hits = []
        # Each result is typically a <div class="trial-result"> or similar
        for card in soup.select(".trial-title, h3.trial-title, .trial-result, article"):
            a_tag = card.find("a", href=True)
            if not a_tag:
                continue
            href = a_tag.get("href", "")
            if "socialscienceregistry.org/trials/" in href:
                url = href if href.startswith("http") else f"https://www.socialscienceregistry.org{href}"
                reg_title = a_tag.get_text(strip=True)
                hits.append({"source": "aearctr", "url": url,
                             "title": reg_title, "text": card.get_text(" ", strip=True)})
        # Fallback: scan all links
        if not hits:
            for a in soup.find_all("a", href=True):
                href = a.get("href", "")
                num_part = href.split("/trials/")[-1] if "/trials/" in href else ""
                if num_part.isdigit():
                    url = href if href.startswith("http") else f"https://www.socialscienceregistry.org{href}"
                    reg_title = a.get_text(strip=True)
                    hits.append({"source": "aearctr", "url": url,
                                 "title": reg_title, "text": reg_title})
        return hits
    except Exception:
        return []


def search_egap(title: str) -> list[dict]:
    """
    Search EGAP research-designs public page.
    Returns list of {url, title, raw_text}.
    """
    if not title:
        return []
    try:
        r = requests.get(
            "https://egap.org/research-designs/",
            params={"s": title[:80]},
            headers=BROWSER_HEADERS,
            timeout=25,
        )
        if r.status_code != 200:
            return []
        soup = BeautifulSoup(r.text, "lxml")
        hits = []
        for a in soup.find_all("a", href=True):
            href = a["href"]
            if "egap.org" in href and "/registration/" in href:
                reg_title = a.get_text(strip=True)
                parent_text = (a.parent or a).get_text(" ", strip=True) if a.parent else reg_title
                hits.append({"source": "egap", "url": href,
                             "title": reg_title, "text": parent_text})
        return hits
    except Exception:
        return []


def search_osf_prereg(title: str) -> list[dict]:
    """
    Search the OSF API for pre-registrations matching the title.
    Uses the public /api/v2 endpoint (no auth for public registrations).
    """
    if not title:
        return []
    try:
        r = SESSION.get(
            "https://api.osf.io/v2/registrations/",
            params={"filter[title]": title[:80], "page[size]": 5},
            timeout=20,
        )
        if r.status_code != 200:
            return []
        hits = []
        for item in r.json().get("data", []) or []:
            attrs  = item.get("attributes", {})
            reg_id = item.get("id", "")
            reg_title = attrs.get("title", "")
            url   = f"https://osf.io/{reg_id}/" if reg_id else ""
            hits.append({"source": "osf", "url": url,
                         "title": reg_title, "text": attrs.get("description", "")})
        return hits
    except Exception:
        return []


# ─────────────────────────────────────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────────────────────────────────────

def load_already_done() -> set[str]:
    """Return set of row_nums already written to the output CSV."""
    if not OUTPUT_CSV.exists():
        return set()
    done = set()
    with open(OUTPUT_CSV, newline="", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            done.add(row.get("row_num", ""))
    return done


def main():
    ap = argparse.ArgumentParser(description="Find title-change false negatives")
    ap.add_argument("--min-sim",  type=float, default=SIMILARITY_THRESHOLD)
    ap.add_argument("--delay",    type=float, default=1.2)
    ap.add_argument("--limit",    type=int,   default=0)
    ap.add_argument("--journal",  type=str,   default="")
    args = ap.parse_args()

    if not RESULTS_CSV.exists():
        print(f"ERROR: {RESULTS_CSV} not found. Run pipeline.py first.")
        sys.exit(1)

    with open(RESULTS_CSV, newline="", encoding="utf-8") as f:
        all_rows = list(csv.DictReader(f))

    # Only undetected papers (no pipeline auto_prereg, no xlsx_prereg)
    candidates = [
        r for r in all_rows
        if str(r.get("auto_prereg", "0")).strip() != "1"
        and str(r.get("xlsx_prereg", "")).strip() != "1"
    ]
    if args.journal:
        candidates = [r for r in candidates if args.journal in r.get("journal", "")]
    if args.limit:
        candidates = candidates[:args.limit]

    already_done = load_already_done()
    print(f"Undetected candidates: {len(candidates)}")
    print(f"Already processed:     {len(already_done)}")
    todo = [r for r in candidates if r.get("row_num", "") not in already_done]
    print(f"Remaining:             {len(todo)}")
    print(f"Similarity threshold:  {args.min_sim}")
    print()

    # Open in append mode for resumability
    is_new = not OUTPUT_CSV.exists()
    OUTPUT_CSV.parent.mkdir(exist_ok=True)
    out_f  = open(OUTPUT_CSV, "a", newline="", encoding="utf-8")
    writer = csv.DictWriter(out_f, fieldnames=OUTPUT_FIELDS)
    if is_new:
        writer.writeheader()

    total_candidates_found = 0

    for i, r in enumerate(todo, 1):
        row_num = r.get("row_num", "")
        title   = (r.get("title_fetched") or r.get("title_xlsx") or "").strip()
        doi     = r.get("doi", "") or ""
        journal = r.get("journal", "")
        year    = r.get("pub_year", "")

        if not title:
            continue

        if i % 50 == 0 or i <= 3:
            print(f"[{i}/{len(todo)}] row={row_num}  {title[:60]}")

        # Search registries
        hits: list[dict] = []
        hits += search_aearctr(title);        time.sleep(args.delay * 0.5)
        hits += search_egap(title);           time.sleep(args.delay * 0.5)
        hits += search_osf_prereg(title);     time.sleep(args.delay * 0.3)

        # Filter by similarity
        paper_authors: list[str] = []  # lazy-loaded below if needed
        for hit in hits:
            score = similarity(title, hit["title"])
            if score < args.min_sim:
                continue

            # Lazy fetch author list
            if not paper_authors and doi:
                paper_authors = crossref_authors(doi)
                time.sleep(args.delay * 0.2)

            overlap = author_overlap_score(paper_authors, hit["text"])
            verdict = "POSSIBLE_TITLE_CHANGE"
            if score >= 0.80:
                verdict = "LIKELY_TITLE_CHANGE"
            if score >= 0.80 and overlap:
                verdict = "STRONG_TITLE_CHANGE"

            writer.writerow({
                "row_num":        row_num,
                "journal":        journal,
                "title":          title,
                "doi":            doi,
                "pub_year":       year,
                "source":         hit["source"],
                "registry_url":   hit["url"],
                "registry_title": hit["title"],
                "sim_score":      f"{score:.3f}",
                "author_overlap": overlap,
                "verdict":        verdict,
            })
            out_f.flush()
            total_candidates_found += 1

            # Brief log for high-confidence hits
            if score >= 0.75:
                print(f"  *** {verdict}  sim={score:.2f}  {doi}")
                print(f"      published: {title[:70]}")
                print(f"      registry:  {hit['title'][:70]}")
                print(f"      url:       {hit['url']}")
                if overlap:
                    print(f"      authors:   {overlap}")
                print()

    out_f.close()
    print(f"\nDone.  {total_candidates_found} candidates written → {OUTPUT_CSV}")
    print(
        "\nTip: open output/title_mismatch_candidates.csv and sort by sim_score desc.\n"
        "     Focus on STRONG_TITLE_CHANGE and LIKELY_TITLE_CHANGE rows first."
    )


if __name__ == "__main__":
    main()
