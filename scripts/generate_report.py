"""
generate_report.py
------------------
Generates a PDF report of the pre-registration detection project.
Output: output/prereg_detection_report.pdf
"""

import csv
import re
from datetime import date
from pathlib import Path

from reportlab.lib import colors
from reportlab.lib.enums import TA_CENTER, TA_LEFT, TA_JUSTIFY
from reportlab.lib.pagesizes import A4
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.units import cm
from reportlab.platypus import (
    BaseDocTemplate, Frame, HRFlowable, KeepTogether, NextPageTemplate, PageBreak,
    PageTemplate, Paragraph, Spacer, Table, TableStyle,
)

from collections import Counter

PROJECT_ROOT    = Path(__file__).parent.parent
XLSX_PATH       = PROJECT_ROOT / "journal_articles_with_pap_2025-03-14.xlsx"
RESULTS_CSV     = PROJECT_ROOT / "output" / "results.csv"
PREREGFIND_CSV  = PROJECT_ROOT / "output" / "preregfind.csv"
VERIFY_CSV      = PROJECT_ROOT / "output" / "prelabeled_verify.csv"
OUTPUT_PDF      = PROJECT_ROOT / "output" / "prereg_detection_report.pdf"

# ── Colours ────────────────────────────────────────────────────────────────────
DARK_BLUE  = colors.HexColor("#1a3a5c")
MID_BLUE   = colors.HexColor("#2e6da4")
LIGHT_BLUE = colors.HexColor("#d6e8f7")
LIGHT_GREY = colors.HexColor("#f4f4f4")
MID_GREY   = colors.HexColor("#888888")
GREEN      = colors.HexColor("#2e7d32")
AMBER      = colors.HexColor("#f57c00")
RED        = colors.HexColor("#c62828")
WHITE      = colors.white
BLACK      = colors.black

PAGE_W, PAGE_H = A4
MARGIN = 2.2 * cm


# ── Styles ─────────────────────────────────────────────────────────────────────

def build_styles():
    base = getSampleStyleSheet()
    s = {}

    s["title"] = ParagraphStyle("title",
        fontName="Helvetica-Bold", fontSize=26, textColor=WHITE,
        alignment=TA_CENTER, leading=32)
    s["subtitle"] = ParagraphStyle("subtitle",
        fontName="Helvetica", fontSize=13, textColor=LIGHT_BLUE,
        alignment=TA_CENTER, leading=18)
    s["date"] = ParagraphStyle("date",
        fontName="Helvetica", fontSize=10, textColor=LIGHT_BLUE,
        alignment=TA_CENTER, leading=14)

    s["h1"] = ParagraphStyle("h1",
        fontName="Helvetica-Bold", fontSize=16, textColor=DARK_BLUE,
        spaceBefore=14, spaceAfter=6, leading=20)
    s["h2"] = ParagraphStyle("h2",
        fontName="Helvetica-Bold", fontSize=12, textColor=MID_BLUE,
        spaceBefore=10, spaceAfter=4, leading=16)
    s["body"] = ParagraphStyle("body",
        fontName="Helvetica", fontSize=10, textColor=BLACK,
        leading=15, spaceAfter=4, alignment=TA_JUSTIFY)
    s["bullet"] = ParagraphStyle("bullet",
        fontName="Helvetica", fontSize=10, textColor=BLACK,
        leading=14, leftIndent=16, spaceAfter=2,
        bulletIndent=6, bulletText="•")
    s["small"] = ParagraphStyle("small",
        fontName="Helvetica", fontSize=8.5, textColor=MID_GREY,
        leading=12, alignment=TA_CENTER)
    s["caption"] = ParagraphStyle("caption",
        fontName="Helvetica-Oblique", fontSize=9, textColor=MID_GREY,
        spaceAfter=6, alignment=TA_CENTER)
    s["th"] = ParagraphStyle("th",
        fontName="Helvetica-Bold", fontSize=9, textColor=WHITE,
        leading=12, alignment=TA_CENTER)
    s["td"] = ParagraphStyle("td",
        fontName="Helvetica", fontSize=9, textColor=BLACK,
        leading=12, alignment=TA_LEFT)
    s["td_c"] = ParagraphStyle("td_c",
        fontName="Helvetica", fontSize=9, textColor=BLACK,
        leading=12, alignment=TA_CENTER)
    s["verdict_confirmed"] = ParagraphStyle("vc",
        fontName="Helvetica-Bold", fontSize=9, textColor=GREEN,
        leading=12, alignment=TA_CENTER)
    s["verdict_probable"] = ParagraphStyle("vp",
        fontName="Helvetica-Bold", fontSize=9, textColor=AMBER,
        leading=12, alignment=TA_CENTER)
    s["verdict_possible"] = ParagraphStyle("vpos",
        fontName="Helvetica", fontSize=9, textColor=MID_GREY,
        leading=12, alignment=TA_CENTER)
    s["verdict_fp"] = ParagraphStyle("vfp",
        fontName="Helvetica", fontSize=9, textColor=RED,
        leading=12, alignment=TA_CENTER)
    return s


# ── Table helpers ──────────────────────────────────────────────────────────────

def header_row(cells, s):
    return [Paragraph(c, s["th"]) for c in cells]


def tbl_style(row_count, zebra=True, nosplit=False):
    cmds = [
        ("BACKGROUND",  (0, 0), (-1, 0),  DARK_BLUE),
        ("ROWBACKGROUNDS", (0, 1), (-1, -1),
         [LIGHT_GREY, WHITE] if zebra else [WHITE]),
        ("GRID",        (0, 0), (-1, -1),  0.4, colors.HexColor("#cccccc")),
        ("TOPPADDING",  (0, 0), (-1, -1),  5),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 5),
        ("LEFTPADDING", (0, 0), (-1, -1),  6),
        ("RIGHTPADDING", (0, 0), (-1, -1), 6),
        ("VALIGN",      (0, 0), (-1, -1),  "MIDDLE"),
    ]
    if nosplit:
        cmds.append(("NOSPLIT", (0, 0), (-1, -1)))
    return TableStyle(cmds)


def verdict_style(verdict: str, s: dict):
    if "CONFIRMED" in verdict:
        return s["verdict_confirmed"]
    if "PROBABLE" in verdict:
        return s["verdict_probable"]
    if "POSSIBLE" in verdict:
        return s["verdict_possible"]
    return s["verdict_fp"]


def short_verdict(v: str) -> str:
    return {
        "CONFIRMED_link_found":    "CONFIRMED",
        "PROBABLE_no_url_strong_kw": "PROBABLE",
        "POSSIBLE_no_url_weak_kw": "POSSIBLE",
        "LIKELY_FP_voter_context": "LIKELY FP",
    }.get(v, v)


def short_journal(j: str) -> str:
    MAP = {
        "american_economic_review":                       "AER",
        "american_economic_journal_economic_policy":      "AEJ:Policy",
        "american_economic_journal_applied_economics":    "AEJ:Applied",
        "american_economic_journal_microeconomics":       "AEJ:Micro",
        "econometrica":                                   "ECTA",
        "economic_journal":                               "EJ",
        "experimental_economics":                        "Exp.Econ",
        "games_and_economic_behavior":                    "GEB",
        "journal_of_development_economics":               "JDE",
        "journal_of_economic_behavior_and_organization":  "JEBO",
        "journal_of_economic_perspectives":               "JEP",
        "journal_of_political_economy":                   "JPE",
        "journal_of_public_economics":                    "JPubE",
        "journal_of_the_economic_science_association":    "JESA",
        "journal_of_the_european_economic_association":   "JEEA",
        "management_science":                             "ManSci",
        "quarterly_journal_of_economics":                 "QJE",
        "review_of_economic_studies":                     "ReStud",
        "review_of_economics_and_statistics":             "ReStat",
    }
    return MAP.get(j, j.replace("_", " ").title()[:18])


# ── Page templates ─────────────────────────────────────────────────────────────

class CoverCanvas:
    def __call__(self, canvas, doc):
        canvas.saveState()
        # White background
        canvas.setFillColor(WHITE)
        canvas.rect(0, 0, PAGE_W, PAGE_H, fill=1, stroke=0)
        # Dark blue header band across the top
        canvas.setFillColor(DARK_BLUE)
        canvas.rect(0, PAGE_H - 4.5 * cm, PAGE_W, 4.5 * cm, fill=1, stroke=0)
        # Accent bar below the header band
        canvas.setFillColor(MID_BLUE)
        canvas.rect(0, PAGE_H - 4.5 * cm - 0.35 * cm, PAGE_W, 0.35 * cm, fill=1, stroke=0)
        # Light blue bottom strip
        canvas.setFillColor(LIGHT_BLUE)
        canvas.rect(0, 0, PAGE_W, 1.8 * cm, fill=1, stroke=0)
        canvas.restoreState()


class BodyCanvas:
    def __call__(self, canvas, doc):
        canvas.saveState()
        # header line
        canvas.setStrokeColor(MID_BLUE)
        canvas.setLineWidth(1.2)
        canvas.line(MARGIN, PAGE_H - 1.4 * cm, PAGE_W - MARGIN, PAGE_H - 1.4 * cm)
        # header text
        canvas.setFont("Helvetica", 8)
        canvas.setFillColor(MID_GREY)
        canvas.drawString(MARGIN, PAGE_H - 1.15 * cm,
                          "Pre-Registration Detection in Economics Journals")
        canvas.drawRightString(PAGE_W - MARGIN, PAGE_H - 1.15 * cm,
                               f"ERC Automation Project")
        # footer
        canvas.line(MARGIN, 1.4 * cm, PAGE_W - MARGIN, 1.4 * cm)
        canvas.drawCentredString(PAGE_W / 2, 0.9 * cm,
                                 f"Page {doc.page}")
        canvas.restoreState()


# ── Data loading ───────────────────────────────────────────────────────────────

def load_data():
    with open(RESULTS_CSV, newline="", encoding="utf-8") as f:
        results = list(csv.DictReader(f))
    with open(PREREGFIND_CSV, newline="", encoding="utf-8") as f:
        finds = list(csv.DictReader(f))
    verify = []
    if VERIFY_CSV.exists():
        with open(VERIFY_CSV, newline="", encoding="utf-8") as f:
            verify = list(csv.DictReader(f))
    # Count pre-labeled rows from xlsx (results.csv only mirrors a subset)
    n_prelabeled = 0
    if XLSX_PATH.exists():
        try:
            import openpyxl as _xl
            wb = _xl.load_workbook(XLSX_PATH, read_only=True, data_only=True)
            ws = wb.active
            n_prelabeled = sum(
                1 for row in ws.iter_rows(min_row=3, values_only=True)
                if row[9] == 1
            )
            wb.close()
        except Exception:
            n_prelabeled = sum(1 for r in results if r.get("xlsx_prereg", "") == "1")
    else:
        n_prelabeled = sum(1 for r in results if r.get("xlsx_prereg", "") == "1")
    return results, finds, verify, n_prelabeled


# ── Content builders ───────────────────────────────────────────────────────────

def cover_section(s):
    # Styles scoped to cover colours
    title_in_band = ParagraphStyle("title_band",
        fontName="Helvetica-Bold", fontSize=26, textColor=WHITE,
        alignment=TA_CENTER, leading=32)
    subtitle_dark = ParagraphStyle("subtitle_dark",
        fontName="Helvetica", fontSize=13, textColor=DARK_BLUE,
        alignment=TA_CENTER, leading=18)
    date_dark = ParagraphStyle("date_dark",
        fontName="Helvetica", fontSize=10, textColor=MID_GREY,
        alignment=TA_CENTER, leading=14)
    label_dark = ParagraphStyle("label_dark",
        fontName="Helvetica-Bold", fontSize=11, textColor=MID_BLUE,
        alignment=TA_CENTER, leading=15)

    elems = []
    # Spacer to push into the dark header band (band is top 4.5 cm of page)
    elems.append(Spacer(1, 1.2 * cm))
    elems.append(Paragraph("Pre-Registration Detection", title_in_band))
    elems.append(Spacer(1, 0.35 * cm))
    elems.append(Paragraph("in Economics Journals", title_in_band))
    # Drop below the band into the white area
    elems.append(Spacer(1, 5.2 * cm))
    elems.append(Paragraph(
        "Automated pipeline for identifying pre-registered studies<br/>"
        "across 19 leading economics journals", subtitle_dark))
    elems.append(Spacer(1, 1.0 * cm))
    elems.append(Paragraph("ERC Automation Project", label_dark))
    elems.append(Spacer(1, 0.3 * cm))
    elems.append(Paragraph(date.today().strftime("%B %Y"), date_dark))
    # Switch to Body template BEFORE the page break so page 2 uses the Body canvas
    elems.append(NextPageTemplate("Body"))
    elems.append(PageBreak())
    return elems


def summary_boxes(results, finds, n_prelabeled, s):
    """Four key-number boxes in a single row — computed from live data."""
    n_confirmed = sum(1 for f in finds if "CONFIRMED" in f.get("verdict", ""))
    box_data = [
        (f"{len(results):,}", "Papers\nProcessed"),
        (f"{n_prelabeled:,}",  "Pre-labeled\nin xlsx"),
        (str(len(finds)),       "Newly\nDetected"),
        (str(n_confirmed),      "Registry URLs\nConfirmed"),
    ]
    box_colours = [DARK_BLUE, MID_BLUE, colors.HexColor("#1565c0"), GREEN]
    box_w = (PAGE_W - 2 * MARGIN) / 4 - 0.3 * cm

    cells = []
    for val, lbl in box_data:
        cells.append(
            Table(
                [[Paragraph(f'<font size="17"><b>{val}</b></font>', ParagraphStyle(
                    f"bv{len(cells)}", fontName="Helvetica-Bold", fontSize=17,
                    textColor=WHITE, alignment=TA_CENTER, leading=20))],
                 [Paragraph(lbl, ParagraphStyle(
                    f"bl{len(cells)}", fontName="Helvetica", fontSize=8,
                    textColor=LIGHT_BLUE, alignment=TA_CENTER, leading=10))]],
                colWidths=[box_w],
                style=TableStyle([
                    ("BACKGROUND",    (0,0), (-1,-1), box_colours[len(cells)]),
                    ("TOPPADDING",    (0,0), (-1,-1), 8),
                    ("BOTTOMPADDING", (0,0), (-1,-1), 8),
                    ("ALIGN",         (0,0), (-1,-1), "CENTER"),
                ])
            )
        )

    row_table = Table([cells],
        colWidths=[(PAGE_W - 2 * MARGIN) / 4] * 4,
        style=TableStyle([
            ("LEFTPADDING",  (0,0), (-1,-1), 2),
            ("RIGHTPADDING", (0,0), (-1,-1), 2),
        ]))
    return [row_table, Spacer(1, 0.4 * cm)]


def methods_section(s):
    elems = []
    elems.append(Paragraph("1. Methods", s["h1"]))
    elems.append(HRFlowable(width="100%", thickness=1, color=LIGHT_BLUE, spaceAfter=8))

    # Stage 1
    elems.append(Paragraph("Stage 1 — Detection Pipeline (pipeline.py)", s["h2"]))
    elems.append(Paragraph(
        "Each paper in the dataset was processed through a five-step automated pipeline:",
        s["body"]))

    steps = [
        ("<b>DOI reconstruction</b>  –  Journal-specific rules parsed PDF filenames "
         "(AER slug lookup via CrossRef title search, Elsevier PII extraction, "
         "Springer/Wiley/Oxford URL patterns, etc.) to recover a canonical DOI."),
        ("<b>Metadata retrieval</b>  –  OpenAlex (primary) then CrossRef (fallback) "
         "supplied abstract, publication year, and OA PDF URL."),
        ("<b>PDF download</b>  –  Up to four OA candidate URLs per paper were tried "
         "(Unpaywall, OpenAlex best_oa_location, arXiv, NBER). "
         "227 PDFs were successfully cached to disk."),
        ("<b>Text extraction</b>  –  PyMuPDF (fitz) extracted full plain text from "
         "each downloaded PDF. Abstract-only text was used when no PDF was available."),
        ("<b>Keyword detection</b>  –  The <i>auto_check()</i> function searched for "
         "registry phrases (<i>pre-analysis plan, pre-registered, aearctr-, osf.io, "
         "aspredicted.org, egap.org, \\bPAP\\b, open science framework</i>). "
         "A false-positive suppression layer excluded matches appearing exclusively "
         "in voter/election contexts (<i>voter preregistration, preregistration law</i>, etc.) "
         "without any co-occurring registry-specific signal."),
    ]
    for st in steps:
        elems.append(Paragraph(st, s["bullet"]))
    elems.append(Spacer(1, 0.3 * cm))

    # Stage 2
    elems.append(Paragraph("Stage 2 — Registry Link Enrichment (find_prereg_links.py)", s["h2"]))
    elems.append(Paragraph(
        "All 44 new detections were passed through a second enrichment pass "
        "querying eight separate sources for the actual registry URL:",
        s["body"]))

    src_data = [
        header_row(["#", "Source", "Method"], s),
        [Paragraph("0", s["td_c"]),
         Paragraph("<b>Cached PDF text</b>", s["td"]),
         Paragraph("PyMuPDF re-scan of downloaded PDF for registry URLs in footnotes and data sections", s["td"])],
        [Paragraph("1", s["td_c"]),
         Paragraph("<b>CrossRef</b>", s["td"]),
         Paragraph("relation field + references[] raw citation strings", s["td"])],
        [Paragraph("2", s["td_c"]),
         Paragraph("<b>Semantic Scholar</b>", s["td"]),
         Paragraph("Abstract and externalIds via free API (DOI lookup + title fallback)", s["td"])],
        [Paragraph("3", s["td_c"]),
         Paragraph("<b>Landing page HTML</b>", s["td"]),
         Paragraph("BeautifulSoup scan of journal page: &lt;a&gt; links, JSON-LD blocks, &lt;meta&gt; tags", s["td"])],
        [Paragraph("4", s["td_c"]),
         Paragraph("<b>OpenAlex metadata</b>", s["td"]),
         Paragraph("Full JSON blob pattern-matched against registry domains", s["td"])],
        [Paragraph("5", s["td_c"]),
         Paragraph("<b>OpenAlex referenced_works</b>", s["td"]),
         Paragraph("Batch-fetched cited works' DOIs checked for osf.io/*, aspredicted, socialscienceregistry patterns", s["td"])],
        [Paragraph("6", s["td_c"]),
         Paragraph("<b>EGAP registry</b>", s["td"]),
         Paragraph("Title search on egap.org/research-designs/", s["td"])],
        [Paragraph("7", s["td_c"]),
         Paragraph("<b>AEA RCT Registry</b>", s["td"]),
         Paragraph("Title search on socialscienceregistry.org/trials (public HTML, not auth API)", s["td"])],
        [Paragraph("8", s["td_c"]),
         Paragraph("<b>DataCite</b>", s["td"]),
         Paragraph("Title query with resource-type-id=preregistration filter", s["td"])],
    ]
    src_tbl = Table(src_data,
        colWidths=[0.8 * cm, 4.2 * cm, PAGE_W - 2 * MARGIN - 0.8 * cm - 4.2 * cm],
        style=tbl_style(len(src_data)))
    elems.append(src_tbl)
    elems.append(Spacer(1, 0.3 * cm))

    elems.append(Paragraph("Verdict classification", s["h2"]))
    vd_data = [
        header_row(["Verdict", "Criteria"], s),
        [Paragraph("<font color='#2e7d32'><b>CONFIRMED</b></font>", s["td"]),
         Paragraph("At least one registry URL found from any source", s["td"])],
        [Paragraph("<font color='#f57c00'><b>PROBABLE</b></font>", s["td"]),
         Paragraph("Strong keyword signal (osf.io, pre-analysis plan, etc.) but no resolvable URL", s["td"])],
        [Paragraph("POSSIBLE", s["td"]),
         Paragraph("Weak keyword only (pre-registered, pre-registration) with no URL", s["td"])],
        [Paragraph("<font color='#c62828'>LIKELY FP</font>", s["td"]),
         Paragraph("Match appears exclusively in voter/election context", s["td"])],
    ]
    vd_tbl = Table(vd_data,
        colWidths=[3.5 * cm, PAGE_W - 2 * MARGIN - 3.5 * cm],
        style=tbl_style(len(vd_data)))
    elems.append(vd_tbl)
    return elems


def findings_section(finds, s):
    elems = []
    elems.append(Spacer(1, 0.3 * cm))
    elems.append(Paragraph("2. Findings", s["h1"]))
    elems.append(HRFlowable(width="100%", thickness=1, color=LIGHT_BLUE, spaceAfter=8))

    # Verdict summary table
    elems.append(Paragraph("2.1  Detection Summary", s["h2"]))

    counts = {"CONFIRMED_link_found": 0, "PROBABLE_no_url_strong_kw": 0,
              "POSSIBLE_no_url_weak_kw": 0, "LIKELY_FP_voter_context": 0}
    for f in finds:
        v = f.get("verdict", "")
        if v in counts:
            counts[v] += 1

    summ_data = [
        header_row(["Verdict", "Count", "Interpretation"], s),
        [Paragraph("<font color='#2e7d32'><b>CONFIRMED</b></font>", s["td"]),
         Paragraph(f"<b>{counts['CONFIRMED_link_found']}</b>", s["td_c"]),
         Paragraph("Registry URL retrieved and verified", s["td"])],
        [Paragraph("<font color='#f57c00'><b>PROBABLE</b></font>", s["td"]),
         Paragraph(f"<b>{counts['PROBABLE_no_url_strong_kw']}</b>", s["td_c"]),
         Paragraph("Strong evidence of pre-registration; URL not publicly accessible", s["td"])],
        [Paragraph("POSSIBLE", s["td"]),
         Paragraph(str(counts["POSSIBLE_no_url_weak_kw"]), s["td_c"]),
         Paragraph("Pre-registration mentioned; no registry link found via any source", s["td"])],
        [Paragraph("<font color='#c62828'>LIKELY FP</font>", s["td"]),
         Paragraph(str(counts["LIKELY_FP_voter_context"]), s["td_c"]),
         Paragraph("Keyword match in voter/election context, not research pre-reg", s["td"])],
        [Paragraph("<b>Total new detections</b>", s["td"]),
         Paragraph(f"<b>{len(finds)}</b>", s["td_c"]),
         Paragraph("Papers not flagged in xlsx that triggered the pipeline detector", s["td"])],
    ]
    summ_tbl = Table(summ_data,
        colWidths=[3.5 * cm, 1.6 * cm, PAGE_W - 2 * MARGIN - 5.1 * cm],
        style=tbl_style(len(summ_data)))
    elems.append(summ_tbl)
    elems.append(Spacer(1, 0.4 * cm))

    # Journal breakdown
    jrnl_counts = Counter(short_journal(f.get("journal","")) for f in finds)
    jrnl_data = [header_row(["Journal", "New Detections", "Confirmed URLs"], s)]
    confirmed_by_j = Counter(
        short_journal(f.get("journal",""))
        for f in finds if "CONFIRMED" in f.get("verdict","")
    )
    for jrnl, cnt in jrnl_counts.most_common():
        jrnl_data.append([
            Paragraph(jrnl, s["td"]),
            Paragraph(str(cnt), s["td_c"]),
            Paragraph(str(confirmed_by_j.get(jrnl, 0)), s["td_c"]),
        ])
    jrnl_tbl = Table(jrnl_data,
        colWidths=[6 * cm, 3.5 * cm, 3.5 * cm],
        style=tbl_style(len(jrnl_data)))
    elems.append(KeepTogether([Paragraph("2.2  Detections by Journal", s["h2"]), jrnl_tbl]))
    elems.append(Spacer(1, 0.4 * cm))

    # Confirmed detections detail
    elems.append(Paragraph("2.3  Confirmed Pre-Registrations — Detail", s["h2"]))
    elems.append(Paragraph(
        "The following 12 papers were newly identified as pre-registered with a "
        "verified registry URL. None were flagged in the original xlsx dataset.",
        s["body"]))

    confirmed = [f for f in finds if "CONFIRMED" in f.get("verdict","")]
    det_data = [header_row(["Row", "Journal", "Title", "Registry URL"], s)]
    for f in confirmed:
        url = f.get("all_found_links", "")
        first_url = url.split(";")[0].strip()
        if len(first_url) > 38:
            first_url = first_url[:36] + "…"
        det_data.append([
            Paragraph(str(f["row_num"]), s["td_c"]),
            Paragraph(short_journal(f.get("journal","")), s["td"]),
            Paragraph(f.get("title","")[:62] + ("…" if len(f.get("title","")) > 62 else ""), s["td"]),
            Paragraph(first_url, ParagraphStyle("url",
                fontName="Courier", fontSize=7.5, textColor=MID_BLUE, leading=10)),
        ])

    w_det_row  = 1.2 * cm
    w_det_jrnl = 1.8 * cm
    w_det_url  = 4.2 * cm
    w_det_title = PAGE_W - 2 * MARGIN - w_det_row - w_det_jrnl - w_det_url
    det_tbl = Table(det_data,
        colWidths=[w_det_row, w_det_jrnl, w_det_title, w_det_url],
        style=tbl_style(len(det_data)))
    elems.append(det_tbl)
    elems.append(Spacer(1, 0.4 * cm))

    # All 44 detections
    elems.append(Paragraph("2.4  All New Detections", s["h2"]))
    elems.append(Paragraph(
        "Complete list of all 44 papers newly detected by the pipeline "
        "(xs_prereg=0 or blank in xlsx).",
        s["body"]))

    all_data = [header_row(["Row", "Journal", "Title", "Year", "Source", "Verdict"], s)]
    for f in finds:
        v = f.get("verdict", "")
        vstyle = verdict_style(v, s)
        src = "PDF" if f.get("text_source","") == "full_pdf" else "Abstract"
        all_data.append([
            Paragraph(str(f["row_num"]), s["td_c"]),
            Paragraph(short_journal(f.get("journal","")), s["td"]),
            Paragraph(f.get("title","")[:60] + ("…" if len(f.get("title","")) > 60 else ""), s["td"]),
            Paragraph(str(f.get("pub_year","")), s["td_c"]),
            Paragraph(src, s["td_c"]),
            Paragraph(short_verdict(v), vstyle),
        ])

    w_row     = 1.2 * cm   # wider to fit 4-digit row numbers without splitting
    w_jrnl    = 2.2 * cm
    w_year    = 1.2 * cm
    w_src     = 2.0 * cm   # wide enough for "Abstract" on one line
    w_verdict = 2.4 * cm
    w_title   = PAGE_W - 2 * MARGIN - w_row - w_jrnl - w_year - w_src - w_verdict
    all_tbl = Table(all_data,
        colWidths=[w_row, w_jrnl, w_title, w_year, w_src, w_verdict],
        repeatRows=1,
        style=tbl_style(len(all_data)))
    elems.append(all_tbl)
    return elems


# ── OA coverage section ───────────────────────────────────────────────────────

JOURNAL_ORDER = [
    "american_economic_review",
    "american_economic_review_insights",
    "american_economic_journal_applied_economics",
    "american_economic_journal_economic_policy",
    "american_economic_journal_microeconomics",
    "econometrica",
    "quarterly_journal_of_economics",
    "review_of_economic_studies",
    "review_of_economics_statistics",
    "economic_journal",
    "journal_of_political_economy",
    "journal_of_public_economics",
    "journal_of_development_economics",
    "journal_of_economic_behavior_and_organization",
    "journal_of_the_european_economic_association",
    "journal_of_the_economic_science_association",
    "games_and_economic_behavior",
    "management_science",
    "experimental_economics",
]


def oa_section(results, s):
    elems = []
    elems.append(Spacer(1, 0.4 * cm))
    elems.append(Paragraph("4. Open Access Coverage", s["h1"]))
    elems.append(HRFlowable(width="100%", thickness=1, color=LIGHT_BLUE, spaceAfter=8))
    elems.append(Paragraph(
        "The table below shows, per journal, how many papers had an open-access PDF URL "
        "available (via Unpaywall / OpenAlex) and how many PDFs were successfully "
        "downloaded for full-text extraction. Coverage varies widely: highly commercial "
        "journals (Management Science, Review of Economic Studies, AEA titles) have lower "
        "availability because their PDFs are served from authenticated, JavaScript-rendered "
        "pages that cannot be retrieved programmatically. AEA journals specifically serve "
        "PDFs from <i>aeaweb.org</i>, which requires browser-rendered authentication; "
        "our downloader found URLs for ~55\u201360\u202f% of AEA papers but successfully "
        "downloaded only \u223c12\u202f% \u2014 most URL attempts returned a 403 or "
        "redirected to a paywall.",
        s["body"]))
    elems.append(Spacer(1, 0.3 * cm))

    total_by_j   = Counter(r["journal"] for r in results)
    oa_url_by_j  = Counter(r["journal"] for r in results if r.get("oa_pdf_url", "").strip())
    oa_dl_by_j   = Counter(r["journal"] for r in results if r.get("oa_pdf_downloaded", "") == "1")

    tbl_data = [header_row(["Journal", "Total", "OA URL\n(found)", "OA URL %",
                             "PDF\n(downloaded)", "PDF %"], s)]

    journals = [j for j in JOURNAL_ORDER if j in total_by_j]
    # Append any journals not in JOURNAL_ORDER (safety)
    for j in sorted(total_by_j):
        if j not in journals:
            journals.append(j)

    for j in journals:
        tot  = total_by_j[j]
        oa_u = oa_url_by_j.get(j, 0)
        oa_d = oa_dl_by_j.get(j, 0)
        pct_u = f"{100 * oa_u / tot:.0f}%" if tot else "—"
        pct_d = f"{100 * oa_d / tot:.0f}%" if tot else "—"
        # Colour-code PDF % cell
        pct_d_style = s["td_c"]
        if oa_d > 0 and tot > 0:
            ratio = oa_d / tot
            if ratio >= 0.30:
                pct_d_style = ParagraphStyle("td_green", parent=s["td_c"],
                                             textColor=GREEN)
            elif ratio <= 0.05:
                pct_d_style = ParagraphStyle("td_red", parent=s["td_c"],
                                             textColor=RED)
        tbl_data.append([
            Paragraph(short_journal(j), s["td"]),
            Paragraph(str(tot),   s["td_c"]),
            Paragraph(str(oa_u),  s["td_c"]),
            Paragraph(pct_u,      s["td_c"]),
            Paragraph(str(oa_d),  s["td_c"]),
            Paragraph(pct_d,      pct_d_style),
        ])

    # Totals row
    grand_tot  = sum(total_by_j.values())
    grand_url  = sum(oa_url_by_j.values())
    grand_dl   = sum(oa_dl_by_j.values())
    tbl_data.append([
        Paragraph("<b>Total</b>", s["td"]),
        Paragraph(f"<b>{grand_tot}</b>",  s["td_c"]),
        Paragraph(f"<b>{grand_url}</b>",  s["td_c"]),
        Paragraph(f"<b>{100*grand_url//grand_tot}%</b>", s["td_c"]),
        Paragraph(f"<b>{grand_dl}</b>",   s["td_c"]),
        Paragraph(f"<b>{100*grand_dl//grand_tot}%</b>",  s["td_c"]),
    ])

    col_w = PAGE_W - 2 * MARGIN
    col_widths = [4.5 * cm, 1.4 * cm, 1.8 * cm, 1.5 * cm, 2.1 * cm, 1.5 * cm]
    # Adjust last col to fill page
    col_widths[0] = col_w - sum(col_widths[1:])
    oa_tbl = Table(tbl_data, colWidths=col_widths,
                   repeatRows=1, style=tbl_style(len(tbl_data)))
    elems.append(oa_tbl)
    elems.append(Spacer(1, 0.3 * cm))
    elems.append(Paragraph(
        "Green PDF % = ≥30\u202f% of papers downloaded; red = ≤5\u202f%. "
        "Journals with 0 downloads have no green-access PDFs and relied on abstract text only.",
        s["caption"]))
    return elems


# ── Pre-labeled verification section ─────────────────────────────────────────

def verify_section(verify, s):
    """Optional section — only rendered if prelabeled_verify.csv exists."""
    if not verify:
        return []

    elems = []
    elems.append(Spacer(1, 0.4 * cm))
    elems.append(Paragraph("5. Pre-labeled Paper Link Verification", s["h1"]))
    elems.append(HRFlowable(width="100%", thickness=1, color=LIGHT_BLUE, spaceAfter=8))
    elems.append(Paragraph(
        f"All {len(verify)} papers already labelled \u2018prereg\u2009=\u20091\u2019 in the "
        "original spreadsheet were validated by directly fetching each stored registry URL, "
        "extracting the pre-registration title from the page, and computing string similarity "
        "against the published paper title. For OSF links the JSON API was used to retrieve "
        "the title reliably. Papers whose DOI appeared in the registry page HTML were "
        "classified as <b>DOI CONFIRMED</b> even when titles diverged. "
        "A second-pass <b>author cross-check</b> queried CrossRef for each paper\u2019s "
        "author list and verified that \u2265\u200950% of surnames appear on the registry page; "
        "matches were classified as <b>AUTHOR CONFIRMED</b>.",
        s["body"]))
    elems.append(Spacer(1, 0.2 * cm))

    # Ordered display of verdicts: positive first, then issues
    level_labels = {
        "VERIFIED":        "VERIFIED",
        "DOI_CONFIRMED":   "DOI CONFIRMED",
        "AUTHOR_CONFIRMED": "AUTHOR CONFIRMED",
        "UNCERTAIN":       "UNCERTAIN",
        "TITLE_MISMATCH":  "TITLE MISMATCH",
        "BROKEN_LINK":     "BROKEN LINK",
        "UNREACHABLE":     "UNREACHABLE",
        "NEW_LINK_FOUND":  "NEW LINK FOUND",
        "NO_LINK_FOUND":   "NO LINK FOUND",
    }
    level_colours = {
        "VERIFIED":        GREEN,
        "DOI_CONFIRMED":   GREEN,
        "AUTHOR_CONFIRMED": GREEN,
        "UNCERTAIN":       AMBER,
        "TITLE_MISMATCH":  RED,
        "BROKEN_LINK":     MID_GREY,
        "UNREACHABLE":     MID_GREY,
        "NEW_LINK_FOUND":  MID_BLUE,
        "NO_LINK_FOUND":   MID_GREY,
    }
    level_interp = {
        "VERIFIED":        "Link resolves; registry title similarity \u2265 0.45",
        "DOI_CONFIRMED":   "Title similarity low but paper DOI found in registry page HTML",
        "AUTHOR_CONFIRMED": "\u2265\u200950% of paper authors (via CrossRef) found on registry page",
        "UNCERTAIN":       "Link resolves; title similarity 0.25\u20130.45; author check inconclusive",
        "TITLE_MISMATCH":  "Link resolves but title similarity < 0.25 \u2014 manual review recommended",
        "BROKEN_LINK":     "HTTP 4xx/5xx response; link may be outdated",
        "UNREACHABLE":     "Could not connect; bare identifier or malformed URL in xlsx",
        "NEW_LINK_FOUND":  "xlsx had no link; a new registry URL was discovered",
        "NO_LINK_FOUND":   "xlsx has no link; none found by API search",
    }

    counts = Counter(r.get("match_level", "") for r in verify)
    confirmed = (counts.get("VERIFIED", 0) + counts.get("DOI_CONFIRMED", 0)
                 + counts.get("AUTHOR_CONFIRMED", 0))
    needs_review = counts.get("TITLE_MISMATCH", 0) + counts.get("BROKEN_LINK", 0)

    elems.append(Paragraph(
        f"<b>Summary:</b> {confirmed} links confirmed ({100*confirmed/len(verify):.0f}%); "
        f"{counts.get('UNCERTAIN', 0)} uncertain (likely title changes); "
        f"{needs_review} require manual review.",
        s["body"]))
    elems.append(Spacer(1, 0.15 * cm))

    sum_data = [header_row(["Verdict", "Count", "Interpretation"], s)]
    for lv, label in level_labels.items():
        cnt = counts.get(lv, 0)
        if cnt == 0:
            continue
        colour = level_colours[lv]
        sum_data.append([
            Paragraph(f'<font color="#{colour.hexval()[2:]}"><b>{label}</b></font>', s["td"]),
            Paragraph(str(cnt), s["td_c"]),
            Paragraph(level_interp[lv], s["td"]),
        ])
    sum_data.append([
        Paragraph("<b>Total</b>", s["td"]),
        Paragraph(f"<b>{len(verify)}</b>", s["td_c"]),
        Paragraph("", s["td"]),
    ])
    sum_tbl = Table(sum_data,
        colWidths=[3.8 * cm, 1.4 * cm, PAGE_W - 2 * MARGIN - 5.2 * cm],
        style=tbl_style(len(sum_data)))
    elems.append(sum_tbl)
    elems.append(Spacer(1, 0.4 * cm))

    # Show TITLE_MISMATCH rows — may be genuine title changes or wrong links
    mismatches = [r for r in verify if r.get("match_level") == "TITLE_MISMATCH"]
    if mismatches:
        elems.append(Paragraph("5.1  Title Mismatch \u2014 Manual Review Required", s["h2"]))
        elems.append(Paragraph(
            f"The following {len(mismatches)} paper(s) have a stored registry link that "
            "resolves but whose page title shows low similarity to the published paper title "
            "(sim\u2009&lt;\u20090.25). This may indicate a genuine title change between "
            "pre-registration and publication, or an incorrectly assigned link.",
            s["body"]))
        mm_data = [header_row(["id", "Journal", "Paper Title", "Registry Title", "Sim", "Link"], s)]
        for r in mismatches:
            link = (r.get("verified_url") or r.get("xlsx_link_prereg", "")).strip()
            # Show only the trial number / short ID to keep column narrow
            m = re.search(r"trials/(\d+)", link)
            short_link = f"trials/{m.group(1)}" if m else link[:28]
            reg_title = (r.get("registry_page_title") or "")[:48]
            mm_data.append([
                Paragraph(str(r.get("xlsx_id", "")), s["td_c"]),
                Paragraph(short_journal(r.get("journal", "")), s["td"]),
                Paragraph((r.get("title", ""))[:50] +
                           ("\u2026" if len(r.get("title", "")) > 50 else ""), s["td"]),
                Paragraph(reg_title + ("\u2026" if len(r.get("registry_page_title","")) > 48 else ""),
                           s["td"]),
                Paragraph(str(r.get("title_sim", "")), s["td_c"]),
                Paragraph(short_link, ParagraphStyle("urlmm", fontName="Courier",
                           fontSize=6.5, textColor=RED, leading=8)),
            ])
        w_id   = 0.9 * cm
        w_jrnl = 1.8 * cm
        w_sim  = 1.2 * cm
        w_url  = 2.8 * cm
        w_half = (PAGE_W - 2 * MARGIN - w_id - w_jrnl - w_sim - w_url) / 2
        mm_tbl = Table(mm_data,
            colWidths=[w_id, w_jrnl, w_half, w_half, w_sim, w_url],
            style=tbl_style(len(mm_data)))
        elems.append(mm_tbl)
        elems.append(Spacer(1, 0.3 * cm))

    # Show BROKEN_LINK rows
    broken = [r for r in verify if r.get("match_level") == "BROKEN_LINK"]
    if broken:
        elems.append(Paragraph("5.2  Broken Links (HTTP 4xx/5xx)", s["h2"]))
        elems.append(Paragraph(
            f"{len(broken)} stored link(s) returned an HTTP error. "
            "The registry entry may have been deleted or the URL has changed.",
            s["body"]))
        br_data = [header_row(["id", "Journal", "Title", "HTTP", "Link"], s)]
        for r in broken:
            link = (r.get("xlsx_link_prereg", "") or "")
            first_link = link.split(";")[0].strip()[:42]
            br_data.append([
                Paragraph(str(r.get("xlsx_id", "")), s["td_c"]),
                Paragraph(short_journal(r.get("journal", "")), s["td"]),
                Paragraph((r.get("title", ""))[:55] +
                           ("\u2026" if len(r.get("title", "")) > 55 else ""), s["td"]),
                Paragraph(str(r.get("http_status", "")), s["td_c"]),
                Paragraph(first_link, ParagraphStyle("urlbr", fontName="Courier",
                           fontSize=6.5, textColor=MID_GREY, leading=8)),
            ])
        w_id   = 0.9 * cm
        w_jrnl = 1.8 * cm
        w_http = 1.3 * cm
        w_url  = 3.8 * cm
        w_title = PAGE_W - 2 * MARGIN - w_id - w_jrnl - w_http - w_url
        br_tbl = Table(br_data,
            colWidths=[w_id, w_jrnl, w_title, w_http, w_url],
            style=tbl_style(len(br_data)))
        elems.append(br_tbl)
        elems.append(Spacer(1, 0.3 * cm))

    # Show NEW_LINK_FOUND rows
    new_links = [r for r in verify if r.get("match_level") == "NEW_LINK_FOUND"]
    if new_links:
        elems.append(Paragraph("5.3  New Links Found for Previously Unlinked Papers", s["h2"]))
        elems.append(Paragraph(
            f"{len(new_links)} paper(s) had prereg=1 but no link_prereg in the xlsx. "
            "The verification pipeline discovered registry URLs for them:",
            s["body"]))
        nl_data = [header_row(["id", "Journal", "Title", "Newly Found Link"], s)]
        for r in new_links:
            found_lnk = (r.get("all_found_links") or r.get("verified_url") or "").split(";")[0].strip()[:48]
            nl_data.append([
                Paragraph(str(r.get("xlsx_id", r.get("row_num", ""))), s["td_c"]),
                Paragraph(short_journal(r.get("journal", "")), s["td"]),
                Paragraph((r.get("title", ""))[:60] +
                           ("\u2026" if len(r.get("title", "")) > 60 else ""), s["td"]),
                Paragraph(found_lnk, ParagraphStyle("urlnl", fontName="Courier",
                           fontSize=7.5, textColor=MID_BLUE, leading=9)),
            ])
        w_id   = 1.0 * cm
        w_jrnl = 2.0 * cm
        w_url  = 4.0 * cm
        w_title = PAGE_W - 2 * MARGIN - w_id - w_jrnl - w_url
        nl_tbl = Table(nl_data,
            colWidths=[w_id, w_jrnl, w_title, w_url],
            style=tbl_style(len(nl_data)))
        elems.append(nl_tbl)

    # Show UNCERTAIN rows
    uncertain = [r for r in verify if r.get("match_level") == "UNCERTAIN"]
    if uncertain:
        elems.append(Spacer(1, 0.3 * cm))
        elems.append(Paragraph("5.4\u2002 Uncertain Links \u2014 Manual Review Recommended", s["h2"]))
        elems.append(Paragraph(
            f"The following {len(uncertain)} paper(s) have a registry link that resolves but whose "
            "page title shows low-to-moderate similarity to the paper title (sim\u2009between "
            "0.25\u2009and\u20090.45) and could not be confirmed by author cross-matching. "
            "Manual inspection is recommended.",
            s["body"]))
        unc_data = [header_row(["id", "Journal", "Paper Title", "Registry Title", "Sim", "Author Match", "Link"], s)]
        for r in uncertain:
            link = (r.get("verified_url") or r.get("xlsx_link_prereg", "")).strip()
            m = re.search(r"trials/(\d+)", link)
            short_link = f"trials/{m.group(1)}" if m else link[:28]
            reg_title = (r.get("registry_page_title") or "")[:40]
            auth = (r.get("author_match") or "")[:18]
            unc_data.append([
                Paragraph(str(r.get("xlsx_id", "")), s["td_c"]),
                Paragraph(short_journal(r.get("journal", "")), s["td"]),
                Paragraph((r.get("title", ""))[:45] +
                           ("\u2026" if len(r.get("title", "")) > 45 else ""), s["td"]),
                Paragraph(reg_title + ("\u2026" if len(r.get("registry_page_title", "")) > 40 else ""),
                           s["td"]),
                Paragraph(str(r.get("title_sim", "")), s["td_c"]),
                Paragraph(auth, s["td_c"]),
                Paragraph(short_link, ParagraphStyle("urlunc", fontName="Courier",
                           fontSize=6.5, textColor=MID_GREY, leading=8)),
            ])
        w_id   = 0.9 * cm
        w_jrnl = 1.8 * cm
        w_sim  = 1.2 * cm
        w_auth = 1.5 * cm
        w_url  = 2.8 * cm
        remaining = PAGE_W - 2 * MARGIN - w_id - w_jrnl - w_sim - w_auth - w_url
        w_ptitle = remaining * 0.52
        w_rtitle = remaining * 0.48
        unc_tbl = Table(unc_data,
            colWidths=[w_id, w_jrnl, w_ptitle, w_rtitle, w_sim, w_auth, w_url],
            style=tbl_style(len(unc_data)))
        elems.append(unc_tbl)
        elems.append(Spacer(1, 0.3 * cm))

    # Show UNREACHABLE rows
    unreachable = [r for r in verify if r.get("match_level") == "UNREACHABLE"]
    if unreachable:
        elems.append(Paragraph("5.5\u2002 Unreachable Links", s["h2"]))
        elems.append(Paragraph(
            f"{len(unreachable)} stored link(s) could not be reached (connection timeout, "
            "DNS failure, or SSL error). The registry may be temporarily down or the URL invalid.",
            s["body"]))
        ur_data = [header_row(["id", "Journal", "Title", "Link"], s)]
        for r in unreachable:
            link = (r.get("xlsx_link_prereg", "") or "").split(";")[0].strip()[:48]
            ur_data.append([
                Paragraph(str(r.get("xlsx_id", "")), s["td_c"]),
                Paragraph(short_journal(r.get("journal", "")), s["td"]),
                Paragraph((r.get("title", ""))[:65] +
                           ("\u2026" if len(r.get("title", "")) > 65 else ""), s["td"]),
                Paragraph(link, ParagraphStyle("urlur", fontName="Courier",
                           fontSize=6.5, textColor=MID_GREY, leading=8)),
            ])
        w_id   = 0.9 * cm
        w_jrnl = 1.8 * cm
        w_url  = 4.2 * cm
        w_t    = PAGE_W - 2 * MARGIN - w_id - w_jrnl - w_url
        ur_tbl = Table(ur_data,
            colWidths=[w_id, w_jrnl, w_t, w_url],
            style=tbl_style(len(ur_data)))
        elems.append(ur_tbl)
        elems.append(Spacer(1, 0.3 * cm))

    # Show NO_LINK_FOUND rows
    no_link = [r for r in verify if r.get("match_level") == "NO_LINK_FOUND"]
    if no_link:
        elems.append(Paragraph("5.6\u2002 No Link Found", s["h2"]))
        elems.append(Paragraph(
            f"{len(no_link)} paper(s) are labelled prereg\u2009=\u20091 in the dataset but "
            "have no registry link stored and none could be found automatically.",
            s["body"]))
        nf_data = [header_row(["id", "Journal", "Title"], s)]
        for r in no_link:
            nf_data.append([
                Paragraph(str(r.get("xlsx_id", "")), s["td_c"]),
                Paragraph(short_journal(r.get("journal", "")), s["td"]),
                Paragraph((r.get("title", ""))[:80] +
                           ("\u2026" if len(r.get("title", "")) > 80 else ""), s["td"]),
            ])
        w_id   = 0.9 * cm
        w_jrnl = 1.8 * cm
        w_t    = PAGE_W - 2 * MARGIN - w_id - w_jrnl
        nf_tbl = Table(nf_data,
            colWidths=[w_id, w_jrnl, w_t],
            style=tbl_style(len(nf_data)))
        elems.append(nf_tbl)

    return elems


def limitations_section(s):
    elems = []
    elems.append(Spacer(1, 0.4 * cm))
    elems.append(Paragraph("6. Limitations & Notes", s["h1"]))
    elems.append(HRFlowable(width="100%", thickness=1, color=LIGHT_BLUE, spaceAfter=8))
    points = [
        "<b>Paywalled PDFs</b>  –  227 of 2,743 papers had a downloadable OA PDF. "
        "For the remaining ~91%, detection relied on the abstract only, which under-reports "
        "pre-registration mentions (registry URLs are typically in data sections or footnotes).",
        "<b>POSSIBLE category</b>  –  28 papers show weak keyword evidence with no recoverable URL. "
        "Most triggered detection via the full PDF text, suggesting the pre-reg was mentioned "
        "in a footnote or data section without a hyperlink. Manual inspection is recommended.",
        "<b>AEA landing pages</b>  –  AEA journal pages (AER, AEJ) are JavaScript-rendered; "
        "the HTML scraper receives minimal content. Registry links embedded in JS-rendered "
        "content may be missed.",
        "<b>Registry search precision</b>  –  EGAP and AEA RCT Registry HTML searches use title "
        "substring matching and may return false positives if a common title phrase matches "
        "an unrelated registration.",
        "<b>False negatives</b>  –  Papers that pre-registered under a different title, or whose "
        "pre-reg is only mentioned in an online appendix not captured as OA, will not be detected.",
    ]
    for p in points:
        elems.append(Paragraph(p, s["bullet"]))
    return elems


# ── Main ───────────────────────────────────────────────────────────────────────

def main():
    results, finds, verify, n_prelabeled = load_data()
    s = build_styles()

    doc = BaseDocTemplate(
        str(OUTPUT_PDF),
        pagesize=A4,
        leftMargin=MARGIN, rightMargin=MARGIN,
        topMargin=2.2 * cm, bottomMargin=2.2 * cm,
        title="Pre-Registration Detection Report",
        author="ERC Automation Project",
    )

    cover_frame = Frame(0, 0, PAGE_W, PAGE_H, id="cover")
    body_frame  = Frame(MARGIN, MARGIN, PAGE_W - 2 * MARGIN,
                        PAGE_H - 2 * MARGIN - 0.8 * cm,
                        id="body", topPadding=1.0 * cm)

    doc.addPageTemplates([
        PageTemplate(id="Cover", frames=[cover_frame], onPage=CoverCanvas()),
        PageTemplate(id="Body",  frames=[body_frame],  onPage=BodyCanvas()),
    ])

    story = []
    story += cover_section(s)  # NextPageTemplate("Body") embedded before PageBreak

    # Executive summary heading on first body page
    story.append(Paragraph("Executive Summary", s["h1"]))
    story.append(HRFlowable(width="100%", thickness=1, color=LIGHT_BLUE, spaceAfter=8))
    story.append(Paragraph(
        "This report documents the automated detection of pre-registered studies across "
        "<b>19 leading economics journals</b> using a custom Python pipeline. "
        "Starting from a spreadsheet of 3,333 papers "
        f"({n_prelabeled} already confirmed as pre-registered), "
        "the pipeline reconstructed DOIs, fetched metadata, downloaded open-access PDFs where "
        "available, extracted text, and applied keyword-based detection with false-positive "
        "suppression for voter/election contexts. "
        "A second enrichment pass queried eight external sources to recover actual registry URLs "
        "for each new detection.",
        s["body"]))
    story.append(Spacer(1, 0.4 * cm))
    story += summary_boxes(results, finds, n_prelabeled, s)
    story.append(Spacer(1, 0.3 * cm))
    story += methods_section(s)
    story += findings_section(finds, s)
    story += oa_section(results, s)
    story += verify_section(verify, s)
    story += limitations_section(s)

    doc.build(story)
    print(f"Report written -> {OUTPUT_PDF}")


if __name__ == "__main__":
    main()
