"""
enrich_pdf_scan_links.py
------------------------
Enrich folder-scan detections by verifying and finding as many registry links
as possible for auto_prereg=1 papers.

This script reuses source checks from find_prereg_links.py and adds PDF-based
DOI/title extraction so it can run directly on local PDF scan outputs.

Input:
  output/pdf_scan_results.csv  (default; override with --scan)

Output:
  output/pdf_scan_prereg_links.csv  (resumable — already-done pdf_path rows skipped)

Usage:
  python scripts/enrich_pdf_scan_links.py
  python scripts/enrich_pdf_scan_links.py --scan output/pdf_scan_results.csv
  python scripts/enrich_pdf_scan_links.py --delay 0.8
  python scripts/enrich_pdf_scan_links.py --sample 100
"""

import argparse
import csv
import re
import sys
import time
from pathlib import Path

import fitz
from path_utils import resolve_existing_path, resolve_output_path

# Reuse mature logic from existing enrichment script
from find_prereg_links import (
    unique,
    extract_links,
    detect_voter_fp,
    triggered_keywords,
    get_verdict,
    check_crossref,
    check_semantic_scholar,
    check_landing_page,
    check_openalex,
    check_openalex_refs,
    check_egap,
    check_aearctr_html,
    check_datacite,
    check_osf_search,
    validate_link_quality,
    is_generic_link,
)

PROJECT_ROOT = Path(__file__).parent.parent
DEFAULT_INPUT_CSV = PROJECT_ROOT / "output" / "pdf_scan_results.csv"
DEFAULT_OUTPUT_CSV = PROJECT_ROOT / "output" / "pdf_scan_prereg_links.csv"

DOI_RE = re.compile(r"\b10\.\d{4,9}/[-._;()/:A-Za-z0-9]+\b")

FIELDS = [
    "pdf_path",
    "filename",
    "journal",
    "auto_prereg",
    "auto_link_prereg",
    "doi_from_pdf",
    "title_guess",
    "triggered_by",
    "voter_fp_signal",
    "crossref_links",
    "s2_links",
    "landing_page_url",
    "landing_page_links",
    "openalex_links",
    "openalex_refs_links",
    "egap_links",
    "aearctr_html_links",
    "datacite_links",
    "osf_title_links",
    "all_found_links",
    "best_link_quality",
    "best_link_title",
    "best_link_sim",
    "verdict",
]

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


def parse_existing_links(raw: str) -> list[str]:
    if not raw:
        return []
    return [x.strip() for x in raw.split(";") if x.strip()]


def clean_doi(value: str) -> str:
    if not value:
        return ""
    d = value.strip().rstrip(".,;)")
    d = d.replace("doi:", "").replace("https://doi.org/", "").replace("http://doi.org/", "")
    return d


def extract_pdf_text_and_meta(pdf_path: Path, max_pages: int = 8) -> tuple[str, str]:
    """Return (snippet_text, title_guess) from the first pages of a PDF."""
    try:
        doc = fitz.open(str(pdf_path))
        metadata_title = (doc.metadata or {}).get("title", "") or ""

        page_texts = []
        for i, page in enumerate(doc):
            if i >= max_pages:
                break
            page_texts.append(page.get_text() or "")
        doc.close()

        text = "\n".join(page_texts)
        title_guess = ""

        if metadata_title and len(metadata_title.strip()) >= 12:
            title_guess = metadata_title.strip()

        if not title_guess and text:
            for line in text.splitlines()[:120]:
                s = " ".join(line.split()).strip()
                if len(s) < 20:
                    continue
                if s.lower().startswith(("abstract", "keywords", "jel", "doi", "published")):
                    continue
                if re.search(r"^\d+$", s):
                    continue
                title_guess = s
                break

        return text, title_guess
    except Exception:
        return "", ""


def extract_doi_from_text(text: str) -> str:
    if not text:
        return ""
    m = DOI_RE.search(text)
    if not m:
        return ""
    return clean_doi(m.group(0))


def best_link_metadata(links: list[str], paper_title: str, paper_doi: str) -> tuple[list[str], str, str, str]:
    """Validate candidate links and move the strongest one to the front.

    We only re-check a small prefix of links because the enrichment step already
    performs many outbound requests. This is enough to stabilize downstream
    title propagation and final-link selection.
    """
    if not links:
        return links, "", "", ""

    scored = []
    for pos, url in enumerate(links[:5]):
        lq = validate_link_quality(url, paper_title, paper_doi)
        sim_text = str(lq.get("sim", "") or "").strip()
        try:
            sim_num = float(sim_text)
        except ValueError:
            sim_num = -1.0
        title = (lq.get("registry_page_title") or "").strip()
        score = (QUALITY_RANK.get(lq.get("quality", ""), 0), sim_num, len(title), -pos)
        scored.append((score, url, lq))

    if not scored:
        return links, "", "", ""

    _, best_url, best_lq = max(scored, key=lambda item: item[0])
    reordered = [best_url] + [url for url in links if url != best_url]
    return (
        reordered,
        best_lq.get("quality", "") or "",
        (best_lq.get("registry_page_title") or "").strip(),
        str(best_lq.get("sim", "") or "").strip(),
    )


def load_done_pdf_paths(csv_path: Path) -> set[str]:
    done = set()
    if not csv_path.exists():
        return done
    with open(csv_path, newline="", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            p = (row.get("pdf_path") or "").strip()
            if p:
                done.add(p)
    return done


def main():
    ap = argparse.ArgumentParser(
        description="Enrich auto_prereg=1 rows from a scan CSV with verified/found links"
    )
    ap.add_argument("--scan", type=str, default=None,
                    help=f"Path to scan CSV (default: {DEFAULT_INPUT_CSV})")
    ap.add_argument("--output", type=str, default=None,
                    help=f"Path to enriched output CSV (default: {DEFAULT_OUTPUT_CSV})")
    ap.add_argument("--delay", type=float, default=1.0,
                    help="Delay base between source requests (default: 1.0)")
    ap.add_argument("--sample", type=int, default=None,
                    help="Process only first N detections")
    ap.add_argument("--overwrite", action="store_true", default=False,
                    help="Overwrite output CSV instead of resuming append mode")
    args = ap.parse_args()

    input_csv = resolve_existing_path(args.scan, DEFAULT_INPUT_CSV, "scan CSV")
    output_csv = resolve_output_path(args.output, DEFAULT_OUTPUT_CSV)

    with open(input_csv, newline="", encoding="utf-8") as f:
        rows = list(csv.DictReader(f))

    detections = [r for r in rows if str(r.get("auto_prereg", "")).strip() == "1"]
    done_paths = set()
    if not args.overwrite:
        done_paths = load_done_pdf_paths(output_csv)

    pending = [r for r in detections if (r.get("pdf_path", "") or "") not in done_paths]
    if args.sample:
        pending = pending[:args.sample]

    print(f"Input file  : {input_csv} ({len(rows)} rows)")
    print(f"Detections  : {len(detections)} rows with auto_prereg=1")
    print(f"Already done: {len(done_paths)}")
    print(f"Processing  : {len(pending)}")

    mode = "w" if args.overwrite else "a"
    write_header = args.overwrite or (not output_csv.exists()) or output_csv.stat().st_size == 0

    output_csv.parent.mkdir(exist_ok=True)
    with open(output_csv, mode, newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=FIELDS)
        if write_header:
            writer.writeheader()

        for i, r in enumerate(pending, 1):
            pdf_path = Path(r.get("pdf_path", ""))
            filename = r.get("filename", "")
            journal = r.get("journal", "")
            pipeline_links = parse_existing_links(r.get("auto_link_prereg", ""))

            print(f"[{i}/{len(pending)}] {filename[:80]}")

            text, title = extract_pdf_text_and_meta(pdf_path)
            doi = extract_doi_from_text(text)

            rich_text = text
            voter_fp = detect_voter_fp(rich_text) if rich_text else False
            kws = triggered_keywords(rich_text) if rich_text else "(no_text)"

            cr_links = []
            s2_links = []
            lp_url, lp_links = "", []
            oa_links = []
            oa_ref_links = []

            if doi:
                print("  [1/9] CrossRef")
                cr_links = check_crossref(doi)
                time.sleep(args.delay * 0.35)

                print("  [2/9] Semantic Scholar")
                s2_links = check_semantic_scholar(doi, title)
                time.sleep(args.delay * 0.35)

                print("  [3/9] Landing page")
                lp_url, lp_links = check_landing_page(doi)
                time.sleep(args.delay * 0.35)

                print("  [4/9] OpenAlex")
                oa_links = check_openalex(doi)
                time.sleep(args.delay * 0.35)

                print("  [5/9] OpenAlex refs")
                oa_ref_links = check_openalex_refs(doi)
                time.sleep(args.delay * 0.35)
            else:
                print("  DOI not found in PDF snippet; skipping DOI-based APIs")

            print("  [6/9] EGAP search")
            egap_links = check_egap(title)
            time.sleep(args.delay * 0.35)

            print("  [7/9] AEA RCT HTML search")
            aear_links = check_aearctr_html(title)
            time.sleep(args.delay * 0.35)

            print("  [8/9] DataCite")
            dc_links = check_datacite(title)
            time.sleep(args.delay * 0.35)

            print("  [9/9] OSF title search")
            osf_links = check_osf_search(title)
            time.sleep(args.delay * 0.35)

            all_links = unique([
                u for u in (
                    pipeline_links
                    + extract_links(text)
                    + cr_links + s2_links + lp_links
                    + oa_links + oa_ref_links
                    + egap_links + aear_links + dc_links + osf_links
                )
                if not is_generic_link(u)
            ])

            best_quality = ""
            best_title = ""
            best_sim = ""
            if all_links:
                all_links, best_quality, best_title, best_sim = best_link_metadata(all_links, title, doi)

            verdict = get_verdict(all_links, voter_fp, rich_text)

            out_row = {
                "pdf_path":            str(pdf_path),
                "filename":            filename,
                "journal":             journal,
                "auto_prereg":         r.get("auto_prereg", ""),
                "auto_link_prereg":    r.get("auto_link_prereg", ""),
                "doi_from_pdf":        doi,
                "title_guess":         title,
                "triggered_by":        kws,
                "voter_fp_signal":     int(voter_fp),
                "crossref_links":      "; ".join(cr_links),
                "s2_links":            "; ".join(s2_links),
                "landing_page_url":    lp_url,
                "landing_page_links":  "; ".join(lp_links),
                "openalex_links":      "; ".join(oa_links),
                "openalex_refs_links": "; ".join(oa_ref_links),
                "egap_links":          "; ".join(egap_links),
                "aearctr_html_links":  "; ".join(aear_links),
                "datacite_links":      "; ".join(dc_links),
                "osf_title_links":     "; ".join(osf_links),
                "all_found_links":     "; ".join(all_links),
                "best_link_quality":   best_quality,
                "best_link_title":     best_title,
                "best_link_sim":       best_sim,
                "verdict":             verdict,
            }
            writer.writerow(out_row)
            f.flush()

    with open(output_csv, newline="", encoding="utf-8") as f:
        out_rows = list(csv.DictReader(f))

    total = len(out_rows)
    confirmed = sum(1 for x in out_rows if x.get("all_found_links", "").strip())
    verified = sum(1 for x in out_rows if x.get("best_link_quality", "") in ("VERIFIED", "DOI_CONFIRMED", "AUTHOR_CONFIRMED"))

    print(f"\nWritten {total} rows -> {output_csv}")
    print(f"Rows with any found link       : {confirmed}")
    print(f"Rows with verified-quality link: {verified}")


if __name__ == "__main__":
    main()
