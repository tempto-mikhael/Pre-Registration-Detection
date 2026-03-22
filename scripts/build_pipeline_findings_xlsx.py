#!/usr/bin/env python3
"""
Build a pipeline-centered findings workbook with explicit final decisions.

This version keeps the raw pipeline signals for transparency, but also writes
clear final columns so review is easier:

- `final_link_url`
- `final_link_decision`
- `final_prereg_decision`
- `prereg_inconsistent`
- `link_inconsistent`
- `experiment`

It also brings selected reference columns from the original XLSX into the same
sheet so you can filter directly on disagreements instead of checking papers
one by one.
"""

import argparse
import csv
import re
import sys
from collections import defaultdict
from datetime import date
from pathlib import Path

try:
    import openpyxl
    from openpyxl.styles import Font, PatternFill
    from openpyxl.utils import get_column_letter
except ImportError:
    sys.exit("openpyxl not found - run: pip install openpyxl")


ROOT = Path(__file__).parent.parent
OUTPUT_DIR = ROOT / "output"
DEFAULT_SCAN = OUTPUT_DIR / "pdf_scan_results_v2.csv"
LINKS_CSV = OUTPUT_DIR / "pdf_scan_prereg_links_dedup.csv"
VERDICTS_CSV = OUTPUT_DIR / "llm_gemini_verdicts.csv"
SOURCE_XLSX = ROOT / "journal_articles_with_pap_2025-03-14.xlsx"
OUT_XLSX = OUTPUT_DIR / f"pipeline_findings_{date.today()}.xlsx"

CONFIDENCE_MAP = {"high": 0.9, "medium": 0.6, "low": 0.25}
FINAL_ACCEPTED_LINK_QUALITIES = {
    "VERIFIED",
    "DOI_CONFIRMED",
    "AUTHOR_CONFIRMED",
    "AI_LINK_CONFIRMED",
}


# Styles
META_FILL = PatternFill("solid", fgColor="E8E8E8")
XLSX_FILL = PatternFill("solid", fgColor="E1F0FF")
SCAN_FILL = PatternFill("solid", fgColor="FFF2CC")
LINK_FILL = PatternFill("solid", fgColor="FCE5CD")
AI_FILL = PatternFill("solid", fgColor="DDEEFF")
FINAL_FILL = PatternFill("solid", fgColor="D9EAD3")
COMPARE_FILL = PatternFill("solid", fgColor="F4CCCC")
BOLD = Font(bold=True)


# Column definitions: (name, fill, width)
COLUMNS = [
    ("id", META_FILL, 7),
    ("filename", META_FILL, 24),
    ("file_name", META_FILL, 48),
    ("journal", META_FILL, 35),
    ("title", META_FILL, 60),
    ("doi", META_FILL, 35),
    ("xlsx_prereg", XLSX_FILL, 11),
    ("xlsx_link_prereg", XLSX_FILL, 42),
    ("xlsx_use_aearct", XLSX_FILL, 14),
    ("xlsx_use_osf", XLSX_FILL, 12),
    ("xlsx_use_aspredicted", XLSX_FILL, 16),
    ("xlsx_use_other", XLSX_FILL, 13),
    ("xlsx_type_lab", XLSX_FILL, 11),
    ("xlsx_type_field", XLSX_FILL, 12),
    ("xlsx_type_online", XLSX_FILL, 13),
    ("xlsx_type_survey", XLSX_FILL, 13),
    ("xlsx_type_obs", XLSX_FILL, 11),
    ("text_source", META_FILL, 20),
    ("no_data", SCAN_FILL, 9),
    ("auto_prereg", SCAN_FILL, 12),
    ("auto_use_aearct", SCAN_FILL, 14),
    ("auto_use_osf", SCAN_FILL, 12),
    ("auto_use_aspredicted", SCAN_FILL, 16),
    ("auto_use_other", SCAN_FILL, 13),
    ("auto_link_prereg", SCAN_FILL, 42),
    ("auto_type_lab", SCAN_FILL, 11),
    ("auto_type_field", SCAN_FILL, 12),
    ("auto_type_online", SCAN_FILL, 13),
    ("auto_type_survey", SCAN_FILL, 13),
    ("auto_type_obs", SCAN_FILL, 11),
    ("all_found_links", LINK_FILL, 60),
    ("best_link_quality", LINK_FILL, 18),
    ("best_link_title", LINK_FILL, 40),
    ("author_match", LINK_FILL, 24),
    ("final_use_aearct", FINAL_FILL, 14),
    ("final_use_osf", FINAL_FILL, 12),
    ("final_use_aspredicted", FINAL_FILL, 16),
    ("final_use_other", FINAL_FILL, 13),
    ("final_link_url", FINAL_FILL, 42),
    ("final_link_decision", FINAL_FILL, 14),
    ("final_link_source", FINAL_FILL, 18),
    ("ai_prereg", AI_FILL, 12),
    ("ai_confidence", AI_FILL, 14),
    ("ai_evidence", AI_FILL, 50),
    ("ai_registry_url", AI_FILL, 42),
    ("ai_reasoning", AI_FILL, 60),
    ("ai_evidence_location", AI_FILL, 28),
    ("experiment", FINAL_FILL, 11),
    ("final_prereg_decision", FINAL_FILL, 18),
    ("final_prereg_source", FINAL_FILL, 18),
    ("pipeline_prereg", FINAL_FILL, 16),
    ("prereg_inconsistent", COMPARE_FILL, 18),
    ("prereg_match_status", COMPARE_FILL, 20),
    ("link_inconsistent", COMPARE_FILL, 16),
    ("link_match_status", COMPARE_FILL, 24),
]


_LOCATION_PATTERNS = [
    ("footnote", re.compile(r"\bfootnote", re.I)),
    ("acknowledgments", re.compile(r"\backnowledg", re.I)),
    ("abstract", re.compile(r"\babstract\b", re.I)),
    ("introduction", re.compile(r"\bintroduction\b", re.I)),
    ("data section", re.compile(r"\bdata\s+(?:section|appendix|subsection)\b", re.I)),
    ("methods section", re.compile(r"\bmethods?\s*(?:section|subsection)?\b|methodology\b", re.I)),
    ("empirical strategy", re.compile(r"\bempirical\s+strategy\b", re.I)),
    ("appendix", re.compile(r"\bappendix\b", re.I)),
    ("main text", re.compile(r"\bmain\s+text\b", re.I)),
    ("conclusion", re.compile(r"\bconclusion\b", re.I)),
    ("results", re.compile(r"\bresults?\s+section\b", re.I)),
    ("body", re.compile(r"\bpaper\s+body\b|\bbody\s+of\s+the\s+paper\b", re.I)),
    ("contract/PAP", re.compile(r"\bcontract\b|\bpre.?analysis\s+plan\b", re.I)),
    ("ethics certificate", re.compile(r"\bethics\s+certif", re.I)),
]


def load_csv(path: Path, key_col: str = "filename") -> dict:
    if not path.exists():
        print(f"WARNING: not found - {path}")
        return {}
    out = {}
    with open(path, encoding="utf-8", errors="ignore") as f:
        for row in csv.DictReader(f):
            key = (row.get(key_col) or "").strip()
            if key and key not in out:
                out[key] = row
    return out


def load_reference_rows(xlsx_path: Path) -> dict:
    """Return original XLSX fields keyed by bare pdf filename."""
    if not xlsx_path.exists():
        print(f"WARNING: source XLSX not found - {xlsx_path}")
        return {}

    wb = openpyxl.load_workbook(str(xlsx_path), read_only=True, data_only=True)
    ws = wb.active
    rows = list(ws.iter_rows(values_only=True))
    headers = list(rows[1])
    idx = {name: headers.index(name) for name in headers if name is not None}

    wanted = [
        "id",
        "file_name",
        "file_title",
        "pdf",
        "prereg",
        "link_prereg",
        "use_aearct",
        "use_osf",
        "use_aspredicted",
        "use_other",
        "type_lab",
        "type_field",
        "type_online",
        "type_survey",
        "type_obs",
    ]

    out = {}
    for row in rows[2:]:
        pdf_name = row[idx["pdf"]] if "pdf" in idx else None
        if not pdf_name:
            continue
        pdf_name = str(pdf_name).strip()
        out[pdf_name] = {
            "id": row[idx["id"]] if "id" in idx else None,
            "file_name": row[idx["file_name"]] if "file_name" in idx else None,
            "title": row[idx["file_title"]] if "file_title" in idx else None,
            "prereg": row[idx["prereg"]] if "prereg" in idx else None,
            "link_prereg": row[idx["link_prereg"]] if "link_prereg" in idx else None,
            "use_aearct": row[idx["use_aearct"]] if "use_aearct" in idx else None,
            "use_osf": row[idx["use_osf"]] if "use_osf" in idx else None,
            "use_aspredicted": row[idx["use_aspredicted"]] if "use_aspredicted" in idx else None,
            "use_other": row[idx["use_other"]] if "use_other" in idx else None,
            "type_lab": row[idx["type_lab"]] if "type_lab" in idx else None,
            "type_field": row[idx["type_field"]] if "type_field" in idx else None,
            "type_online": row[idx["type_online"]] if "type_online" in idx else None,
            "type_survey": row[idx["type_survey"]] if "type_survey" in idx else None,
            "type_obs": row[idx["type_obs"]] if "type_obs" in idx else None,
        }

    wb.close()
    return out


def int_or_none(val):
    try:
        return int(val)
    except (TypeError, ValueError):
        return None


def to_bool_or_none(val):
    if val is None:
        return None
    text = str(val).strip().lower()
    if text == "true":
        return True
    if text == "false":
        return False
    return None


def clean_text_or_none(val):
    if val is None:
        return None
    text = str(val).strip()
    return text or None


def split_links(raw: str | None) -> list[str]:
    if not raw:
        return []
    return [part.strip() for part in str(raw).split(";") if part and part.strip()]


def unique_preserve(items: list[str]) -> list[str]:
    seen = set()
    out = []
    for item in items:
        if item not in seen:
            seen.add(item)
            out.append(item)
    return out


def canonicalize_link(url: str) -> str:
    text = (url or "").strip().lower()
    if not text:
        return ""

    aea_match = re.search(r"(?:socialscienceregistry\.org/trials/|aearctr[-:])0*(\d+)", text)
    if aea_match:
        return f"aearctr:{int(aea_match.group(1))}"

    asp_match = re.search(r"aspredicted\.org/(?:blind\.php\?x=)?([a-z0-9]+)", text)
    if asp_match:
        return f"aspredicted:{asp_match.group(1)}"

    osf_match = re.search(r"osf\.io/(?:preprints/osf/)?([a-z0-9]+)", text)
    if osf_match:
        return f"osf:{osf_match.group(1)}"

    text = re.sub(r"^https?://", "", text)
    text = re.sub(r"^www\.", "", text)
    text = text.rstrip("/")
    return text


def compare_link_sets(xlsx_raw: str | None, final_links: list[str]) -> tuple[int, str]:
    xlsx_links = split_links(xlsx_raw)
    xlsx_norm = {canonicalize_link(x) for x in xlsx_links if canonicalize_link(x)}
    final_norm = {canonicalize_link(x) for x in final_links if canonicalize_link(x)}

    if not xlsx_norm and not final_norm:
        return 0, "both_empty"
    if not xlsx_norm and final_norm:
        return 1, "xlsx_missing_final_present"
    if xlsx_norm and not final_norm:
        return 1, "xlsx_present_final_missing"
    if xlsx_norm & final_norm:
        return 0, "match"
    return 1, "different_link"


def normalized_title(text: str | None) -> str:
    title = (text or "").lower()
    title = re.sub(r"[^\w\s]", " ", title)
    return re.sub(r"\s+", " ", title).strip()


def prefer_longer_matching_title(current: str | None, candidate: str | None) -> str | None:
    if not candidate:
        return current
    if not current:
        return candidate

    cur_norm = normalized_title(current)
    cand_norm = normalized_title(candidate)
    if not cur_norm or not cand_norm:
        return current

    if cur_norm in cand_norm or cand_norm in cur_norm:
        return candidate if len(candidate) > len(current) else current
    return current


def extract_evidence_location(evidence: str | None, reasoning: str | None) -> str | None:
    combined = f"{evidence or ''} {reasoning or ''}".strip()
    if not combined:
        return None
    found = []
    for label, pattern in _LOCATION_PATTERNS:
        if pattern.search(combined) and label not in found:
            found.append(label)
    return "; ".join(found) if found else None


def pick_final_link(
    auto_link_raw: str | None,
    all_found_raw: str | None,
    ai_registry_url: str | None,
    best_quality: str | None,
) -> tuple[str | None, int, str, list[str]]:
    auto_links = split_links(auto_link_raw)
    enriched_links = split_links(all_found_raw)
    ai_links = split_links(ai_registry_url)

    accepted_enriched = (
        enriched_links if (best_quality or "").strip() in FINAL_ACCEPTED_LINK_QUALITIES else []
    )
    final_links = unique_preserve(auto_links + accepted_enriched + ai_links)

    if auto_links:
        return auto_links[0], 1, "pdf_text", final_links
    if accepted_enriched:
        return accepted_enriched[0], 1, "enrichment_verified", final_links
    if ai_links:
        return ai_links[0], 1, "ai_registry", final_links
    return None, 0, "none", []


def derive_platform_flags(
    auto_use_aearct: int | None,
    auto_use_osf: int | None,
    auto_use_asp: int | None,
    auto_use_other: int | None,
    final_links: list[str],
) -> tuple[int, int, int, int]:
    final_aearct = 1 if auto_use_aearct == 1 else 0
    final_osf = 1 if auto_use_osf == 1 else 0
    final_asp = 1 if auto_use_asp == 1 else 0
    final_other = 1 if auto_use_other == 1 else 0

    for link in final_links:
        canon = canonicalize_link(link)
        if canon.startswith("aearctr:"):
            final_aearct = 1
        elif canon.startswith("osf:"):
            final_osf = 1
        elif canon.startswith("aspredicted:"):
            final_asp = 1
        elif any(domain in canon for domain in ("clinicaltrials.gov", "egap.org", "ridie")):
            final_other = 1

    return final_aearct, final_osf, final_asp, final_other


def write_sheet_header(ws, columns):
    for col_idx, (col_name, fill, width) in enumerate(columns, start=1):
        cell = ws.cell(row=1, column=col_idx, value=col_name)
        cell.font = BOLD
        cell.fill = fill
        ws.column_dimensions[get_column_letter(col_idx)].width = width


def write_summary_sheets(wb, rows: list[dict]):
    summary_ws = wb.create_sheet("comparison_summary")
    summary_ws.append(["metric", "value"])
    summary_ws["A1"].font = BOLD
    summary_ws["B1"].font = BOLD
    summary_ws.column_dimensions["A"].width = 34
    summary_ws.column_dimensions["B"].width = 14

    metrics = [
        ("total_papers", len(rows)),
        ("final_prereg_1", sum(1 for r in rows if r["final_prereg_decision"] == 1)),
        ("final_link_1", sum(1 for r in rows if r["final_link_decision"] == 1)),
        ("experiment_1", sum(1 for r in rows if r["experiment"] == 1)),
        ("xlsx_prereg_1", sum(1 for r in rows if r["xlsx_prereg"] == 1)),
        ("xlsx_prereg_0", sum(1 for r in rows if r["xlsx_prereg"] == 0)),
        ("prereg_matches_1", sum(1 for r in rows if r["prereg_match_status"] == "match_1")),
        ("prereg_matches_0", sum(1 for r in rows if r["prereg_match_status"] == "match_0")),
        ("prereg_inconsistent", sum(1 for r in rows if r["prereg_inconsistent"] == 1)),
        ("link_inconsistent", sum(1 for r in rows if r["link_inconsistent"] == 1)),
    ]
    for metric, value in metrics:
        summary_ws.append([metric, value])

    journal_ws = wb.create_sheet("journal_summary")
    journal_headers = [
        "journal",
        "total_papers",
        "experiment_papers",
        "final_prereg_papers",
        "final_prereg_and_experiment",
        "prereg_share_all",
        "prereg_share_experiment",
        "prereg_inconsistent",
        "link_inconsistent",
    ]
    journal_ws.append(journal_headers)
    for cell in journal_ws[1]:
        cell.font = BOLD

    stats_by_journal = defaultdict(lambda: {
        "total": 0,
        "experiment": 0,
        "final_prereg": 0,
        "exp_prereg": 0,
        "prereg_inconsistent": 0,
        "link_inconsistent": 0,
    })

    for row in rows:
        journal = row["journal"] or "(missing)"
        stats = stats_by_journal[journal]
        stats["total"] += 1
        if row["experiment"] == 1:
            stats["experiment"] += 1
        if row["final_prereg_decision"] == 1:
            stats["final_prereg"] += 1
        if row["experiment"] == 1 and row["final_prereg_decision"] == 1:
            stats["exp_prereg"] += 1
        if row["prereg_inconsistent"] == 1:
            stats["prereg_inconsistent"] += 1
        if row["link_inconsistent"] == 1:
            stats["link_inconsistent"] += 1

    for journal in sorted(stats_by_journal):
        stats = stats_by_journal[journal]
        prereg_share_all = stats["final_prereg"] / stats["total"] if stats["total"] else None
        prereg_share_exp = stats["exp_prereg"] / stats["experiment"] if stats["experiment"] else None
        journal_ws.append([
            journal,
            stats["total"],
            stats["experiment"],
            stats["final_prereg"],
            stats["exp_prereg"],
            prereg_share_all,
            prereg_share_exp,
            stats["prereg_inconsistent"],
            stats["link_inconsistent"],
        ])

    for col_idx, width in enumerate((35, 12, 16, 18, 24, 16, 22, 18, 16), start=1):
        journal_ws.column_dimensions[get_column_letter(col_idx)].width = width


def main():
    parser = argparse.ArgumentParser(description="Build pipeline findings XLSX")
    parser.add_argument(
        "--scan",
        type=str,
        default=None,
        help=f"Path to scan CSV (default: {DEFAULT_SCAN})",
    )
    args = parser.parse_args()

    scan_csv = Path(args.scan) if args.scan else DEFAULT_SCAN
    if not scan_csv.exists():
        fallback = OUTPUT_DIR / "pdf_scan_results.csv"
        if fallback.exists():
            print(f"WARNING: {scan_csv.name} not found - falling back to {fallback.name}")
            scan_csv = fallback
        else:
            sys.exit(f"ERROR: scan CSV not found: {scan_csv}")

    print(f"Using scan CSV: {scan_csv.name}")

    scan = load_csv(scan_csv)
    links = load_csv(LINKS_CSV)
    verdicts = load_csv(VERDICTS_CSV)
    refs = load_reference_rows(SOURCE_XLSX)

    print(f"Scan rows:     {len(scan)}")
    print(f"Link rows:     {len(links)}")
    print(f"Verdict rows:  {len(verdicts)}")
    print(f"XLSX refs:     {len(refs)}")

    out_wb = openpyxl.Workbook()
    out_ws = out_wb.active
    out_ws.title = "pipeline_findings"
    write_sheet_header(out_ws, COLUMNS)
    out_ws.freeze_panes = "A2"

    rows_for_summary = []

    for filename, scan_row in sorted(scan.items()):
        link_row = links.get(filename, {})
        verdict_row = verdicts.get(filename, {})
        ref = refs.get(filename, {})

        paper_id = ref.get("id")
        file_name = clean_text_or_none(ref.get("file_name"))
        title = ref.get("title") or link_row.get("title_guess") or None
        title = prefer_longer_matching_title(title, link_row.get("best_link_title"))
        doi = clean_text_or_none(link_row.get("doi_from_pdf"))
        text_source = clean_text_or_none(scan_row.get("text_source"))

        xlsx_prereg = ref.get("prereg")
        xlsx_link_prereg = clean_text_or_none(ref.get("link_prereg"))
        xlsx_use_aearct = int_or_none(ref.get("use_aearct"))
        xlsx_use_osf = int_or_none(ref.get("use_osf"))
        xlsx_use_asp = int_or_none(ref.get("use_aspredicted"))
        xlsx_use_other = int_or_none(ref.get("use_other"))
        xlsx_type_lab = int_or_none(ref.get("type_lab"))
        xlsx_type_field = int_or_none(ref.get("type_field"))
        xlsx_type_online = int_or_none(ref.get("type_online"))
        xlsx_type_survey = int_or_none(ref.get("type_survey"))
        xlsx_type_obs = int_or_none(ref.get("type_obs"))

        no_data = int_or_none(scan_row.get("auto_no_data"))
        auto_prereg = int_or_none(scan_row.get("auto_prereg"))
        auto_use_aearct = int_or_none(scan_row.get("auto_use_aearct"))
        auto_use_osf = int_or_none(scan_row.get("auto_use_osf"))
        auto_use_asp = int_or_none(scan_row.get("auto_use_aspredicted"))
        auto_use_other = int_or_none(scan_row.get("auto_use_other"))
        auto_link_prereg = clean_text_or_none(scan_row.get("auto_link_prereg"))
        auto_type_lab = int_or_none(scan_row.get("auto_type_lab"))
        auto_type_field = int_or_none(scan_row.get("auto_type_field"))
        auto_type_online = int_or_none(scan_row.get("auto_type_online"))
        auto_type_survey = int_or_none(scan_row.get("auto_type_survey"))
        auto_type_obs = int_or_none(scan_row.get("auto_type_obs"))
        if auto_type_obs == 1 and any(value == 1 for value in (auto_type_lab, auto_type_field, auto_type_online)):
            auto_type_obs = 0

        all_found_links = clean_text_or_none(link_row.get("all_found_links"))
        best_link_quality = clean_text_or_none(link_row.get("best_link_quality"))
        best_link_title = clean_text_or_none(link_row.get("best_link_title"))
        author_match = clean_text_or_none(link_row.get("author_match"))
        if not auto_link_prereg:
            auto_link_prereg = clean_text_or_none(link_row.get("auto_link_prereg"))

        ai_prereg_bool = to_bool_or_none(verdict_row.get("llm_prereg"))
        if ai_prereg_bool is True:
            ai_prereg = "True"
        elif ai_prereg_bool is False:
            ai_prereg = "False"
        else:
            ai_prereg = None

        conf_str = (clean_text_or_none(verdict_row.get("llm_confidence")) or "").lower()
        ai_confidence = CONFIDENCE_MAP.get(conf_str)
        ai_evidence = clean_text_or_none(verdict_row.get("llm_evidence"))
        ai_registry_url = clean_text_or_none(verdict_row.get("llm_registry_url"))
        ai_reasoning = clean_text_or_none(verdict_row.get("llm_reasoning"))

        ai_link_check = clean_text_or_none(link_row.get("ai_link_check")) or ""
        ai_link_reason = clean_text_or_none(link_row.get("ai_link_reasoning")) or ""
        if ai_link_check and not ai_reasoning:
            ai_reasoning = ai_link_reason or None
        if ai_link_check and not ai_evidence:
            ai_evidence = ai_link_reason or None
        if ai_link_check and ai_prereg is None:
            if ai_link_check.startswith("confirmed_"):
                ai_prereg = "True"
                ai_prereg_bool = True
            elif ai_link_check.startswith("rejected_"):
                ai_prereg = "False"
                ai_prereg_bool = False
        if ai_link_check and ai_confidence is None:
            ai_link_conf = ai_link_check.replace("confirmed_", "").replace("rejected_", "")
            ai_confidence = CONFIDENCE_MAP.get(ai_link_conf)

        if ai_registry_url and not all_found_links:
            all_found_links = ai_registry_url
        elif ai_registry_url:
            merged_links = unique_preserve(split_links(all_found_links) + split_links(ai_registry_url))
            all_found_links = "; ".join(merged_links)
            if not best_link_quality:
                best_link_quality = "ai"

        ai_evidence_location = extract_evidence_location(ai_evidence, ai_reasoning)

        final_link_url, final_link_decision, final_link_source, final_links = pick_final_link(
            auto_link_prereg,
            all_found_links,
            ai_registry_url,
            best_link_quality,
        )

        final_use_aearct, final_use_osf, final_use_asp, final_use_other = derive_platform_flags(
            auto_use_aearct,
            auto_use_osf,
            auto_use_asp,
            auto_use_other,
            final_links,
        )

        experiment = 1 if any(
            value == 1
            for value in (
                auto_type_lab,
                auto_type_field,
                auto_type_online,
                auto_type_survey,
                auto_type_obs,
            )
        ) else 0

        if final_link_decision == 1 and ai_prereg_bool is True:
            final_prereg_source = "link+ai"
        elif final_link_decision == 1:
            final_prereg_source = f"link:{final_link_source}"
        elif ai_prereg_bool is True:
            final_prereg_source = "ai_only"
        else:
            final_prereg_source = "none"

        final_prereg_decision = 1 if (final_link_decision == 1 or ai_prereg_bool is True) else 0
        pipeline_prereg = final_prereg_decision  # keep legacy column name for continuity

        if xlsx_prereg in (0, 1):
            prereg_inconsistent = 1 if final_prereg_decision != xlsx_prereg else 0
            if final_prereg_decision == 1 and xlsx_prereg == 1:
                prereg_match_status = "match_1"
            elif final_prereg_decision == 0 and xlsx_prereg == 0:
                prereg_match_status = "match_0"
            else:
                prereg_match_status = "different"
        else:
            prereg_inconsistent = 0
            prereg_match_status = "xlsx_blank"

        link_inconsistent, link_match_status = compare_link_sets(xlsx_link_prereg, final_links)

        row_dict = {
            "id": paper_id,
            "filename": filename,
            "file_name": file_name,
            "journal": scan_row.get("journal") or None,
            "title": title,
            "doi": doi,
            "xlsx_prereg": xlsx_prereg,
            "xlsx_link_prereg": xlsx_link_prereg,
            "xlsx_use_aearct": xlsx_use_aearct,
            "xlsx_use_osf": xlsx_use_osf,
            "xlsx_use_aspredicted": xlsx_use_asp,
            "xlsx_use_other": xlsx_use_other,
            "xlsx_type_lab": xlsx_type_lab,
            "xlsx_type_field": xlsx_type_field,
            "xlsx_type_online": xlsx_type_online,
            "xlsx_type_survey": xlsx_type_survey,
            "xlsx_type_obs": xlsx_type_obs,
            "text_source": text_source,
            "no_data": no_data,
            "auto_prereg": auto_prereg,
            "auto_use_aearct": auto_use_aearct,
            "auto_use_osf": auto_use_osf,
            "auto_use_aspredicted": auto_use_asp,
            "auto_use_other": auto_use_other,
            "auto_link_prereg": auto_link_prereg,
            "auto_type_lab": auto_type_lab,
            "auto_type_field": auto_type_field,
            "auto_type_online": auto_type_online,
            "auto_type_survey": auto_type_survey,
            "auto_type_obs": auto_type_obs,
            "all_found_links": all_found_links,
            "best_link_quality": best_link_quality,
            "best_link_title": best_link_title,
            "author_match": author_match,
            "final_use_aearct": final_use_aearct,
            "final_use_osf": final_use_osf,
            "final_use_aspredicted": final_use_asp,
            "final_use_other": final_use_other,
            "final_link_url": final_link_url,
            "final_link_decision": final_link_decision,
            "final_link_source": final_link_source,
            "ai_prereg": ai_prereg,
            "ai_confidence": ai_confidence,
            "ai_evidence": ai_evidence,
            "ai_registry_url": ai_registry_url,
            "ai_reasoning": ai_reasoning,
            "ai_evidence_location": ai_evidence_location,
            "experiment": experiment,
            "final_prereg_decision": final_prereg_decision,
            "final_prereg_source": final_prereg_source,
            "pipeline_prereg": pipeline_prereg,
            "prereg_inconsistent": prereg_inconsistent,
            "prereg_match_status": prereg_match_status,
            "link_inconsistent": link_inconsistent,
            "link_match_status": link_match_status,
        }

        out_ws.append([row_dict[name] for name, _, _ in COLUMNS])
        rows_for_summary.append(row_dict)

    out_ws.auto_filter.ref = out_ws.dimensions
    write_summary_sheets(out_wb, rows_for_summary)
    out_wb.save(str(OUT_XLSX))

    final_prereg_1 = sum(1 for r in rows_for_summary if r["final_prereg_decision"] == 1)
    final_link_1 = sum(1 for r in rows_for_summary if r["final_link_decision"] == 1)
    experiment_1 = sum(1 for r in rows_for_summary if r["experiment"] == 1)
    prereg_inconsistent = sum(1 for r in rows_for_summary if r["prereg_inconsistent"] == 1)
    link_inconsistent = sum(1 for r in rows_for_summary if r["link_inconsistent"] == 1)

    print(f"\nDone - {len(rows_for_summary)} papers written")
    print(f"  final_prereg_decision=1 : {final_prereg_1}")
    print(f"  final_link_decision=1   : {final_link_1}")
    print(f"  experiment=1            : {experiment_1}")
    print(f"  prereg_inconsistent=1   : {prereg_inconsistent}")
    print(f"  link_inconsistent=1     : {link_inconsistent}")
    print(f"  Output: {OUT_XLSX}")


if __name__ == "__main__":
    main()
