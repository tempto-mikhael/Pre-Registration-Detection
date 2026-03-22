"""
scan_pdf_folder.py
------------------
Scan a folder of journal PDFs (organized in sub-folders by journal name) and
run the pre-registration detection pipeline on each PDF.

Folder structure expected:
    <root>/
        <journal_name>/
            paper1.pdf
            paper2.pdf
            ...
        <another_journal>/
            ...

Nested sub-folders are also supported (e.g. root/journal/volume/issue/paper.pdf).
The journal name is taken from the first-level sub-folder directly under root.

Output:
    Pre-Registration-Detection/output/pdf_scan_results.csv   (resumable)

Usage:
    python scripts/scan_pdf_folder.py --folder "F:\\paperpdfs\\hit_pap"
    python scripts/scan_pdf_folder.py --folder "F:\\paperpdfs\\hit_pap" --sample 20
    python scripts/scan_pdf_folder.py --folder "F:\\paperpdfs\\hit_pap" --prereg-only
"""

import argparse
import csv
import io
import re
import sys
from pathlib import Path

from tqdm import tqdm

# ── Paths ─────────────────────────────────────────────────────────────────────
SCRIPT_DIR   = Path(__file__).parent
PROJECT_ROOT = SCRIPT_DIR.parent
OUTPUT_DIR   = PROJECT_ROOT / "output"
OUTPUT_CSV   = OUTPUT_DIR / "pdf_scan_results.csv"

OUTPUT_DIR.mkdir(exist_ok=True)

# ── Output columns ─────────────────────────────────────────────────────────────
CSV_FIELDS = [
    "pdf_path",
    "filename",
    "journal",
    "text_length",
    "text_source",
    # automated checks
    "auto_no_data",
    "auto_prereg",
    "auto_use_aearct",
    "auto_use_osf",
    "auto_use_aspredicted",
    "auto_use_other",
    "auto_link_prereg",
    "auto_type_lab",
    "auto_type_field",
    "auto_type_online",
    "auto_type_survey",
    "auto_type_obs",
]

# ── Keywords (mirrored from pipeline.py) ─────────────────────────────────────
PREREG_PHRASES = [
    "analysis plan",
    "pre-analysis plan",
    "pre-analysis-plan",
    "pre analysis plan",
    "preanalysis plan",
    "pre-registration",
    "preregistration",
    "pre registration",
    "pre-register",
    "preregister",
    "pre-registered",
    "preregistered",
    "pre-registering",
    "aea rct",
    "aearctr-",
    "socialscienceregistry.org",
    "open science framework",
    "aspredicted.org",
    "osf.io",
    "clinicaltrials.gov",
    "egap.org",
    "ridie",
]

PREREG_WORD_TOKENS = [
    r"\bpap\b",
    r"\baearct\b",
    r"\bosf\b",
    r"\begap\b",
    r"\baspredicted\b",
    r"\bregister\b",
    r"\bregistration\b",
]
COMPILED_PREREG_WORDS = [re.compile(p, re.IGNORECASE) for p in PREREG_WORD_TOKENS]

EXPERIMENT_PHRASES = [
    "field experiment",
    "laboratory experiment",
    "lab experiment",
    "online experiment",
    "randomized experiment",
    "randomized controlled trial",
    "randomized control trial",
    "randomised controlled trial",
    "randomized evaluation",
    "randomized trial",
    "rct ",
    " rct",
    "(rct)",
    "intervention",
]
EXPERIMENT_WORD_TOKENS = [r"\bexperiment\b", r"\blaboratory\b"]
COMPILED_EXPERIMENT_WORDS = [re.compile(p, re.IGNORECASE) for p in EXPERIMENT_WORD_TOKENS]

DATA_PHRASES = [
    "regression", "coefficient", "observational data",
    "administrative data", "panel data", "cross-section",
    "survey data", "empirical", "estimation",
]
DATA_WORD_TOKENS = [r"\bdata\b", r"\bsample\b", r"\bobservations\b", r"\bsurvey\b"]
COMPILED_DATA_WORDS = [re.compile(p, re.IGNORECASE) for p in DATA_WORD_TOKENS]

PREREG_URL_PATTERNS = [
    r"https?://(?:www\.)?socialscienceregistry\.org/trials/[\d ]+\d",
    r"AEARCTR-[\d ]+\d",
    r"https?://(?:www\.)?osf\.io/[A-Za-z0-9]+",
    r"https?://(?:www\.)?aspredicted\.org/\S+",
    r"https?://(?:www\.)?clinicaltrials\.gov/\S+",
    r"https?://(?:www\.)?egap\.org/\S+",
    r"\bAsPredicted\s*#\s*[\d ]+\d",
    r"\bAsPredicted\s*\([^)]*\d[^)]*\)",
]
COMPILED_PREREG_URLS = [re.compile(p, re.IGNORECASE) for p in PREREG_URL_PATTERNS]

# Links that are just registry homepages / generic pages (not paper-specific)
GENERIC_LINK_PATTERNS = [
    re.compile(r'^https?://(www\.)?aspredicted\.org/blind\s*$', re.I),
    re.compile(r'^https?://(www\.)?aspredicted\.org/?$', re.I),
    re.compile(r'^https?://(www\.)?egap\.org/?$', re.I),
    re.compile(r'^https?://(www\.)?osf\.io/?$', re.I),
    re.compile(r'^https?://(www\.)?osf\.io/(download|preprints|registries|search|meetings|institutions)\s*$', re.I),
    re.compile(r'^https?://(www\.)?socialscienceregistry\.org/?$', re.I),
    re.compile(r'^https?://(www\.)?socialscienceregistry\.org/trials/?$', re.I),
    re.compile(r'^https?://(www\.)?socialscienceregistry\.org/trials/0\s*$', re.I),
]

def is_generic_link(url: str) -> bool:
    """Return True if the URL is a generic registry homepage, not a paper-specific link."""
    return any(p.match(url.strip()) for p in GENERIC_LINK_PATTERNS)

PREREG_VOTER_PHRASES = [
    "preregistration law", "pre-registration law", "preregistration statute",
    "voter preregistration", "voting preregistration", "youth preregistration",
    "election preregistration", "preregistration requirement",
    "preregistration program", "preregistration policy",
]


# ── Helpers ───────────────────────────────────────────────────────────────────

def phrase_hit(text: str, phrases: list) -> bool:
    tl = text.lower()
    return any(p.lower() in tl for p in phrases)


def regex_hit(text: str, patterns: list) -> bool:
    return any(p.search(text) for p in patterns)


def _strip_spaces(s: str) -> str:
    """Remove spaces inserted by PDF renderers inside digit strings."""
    return re.sub(r'(?<=\d) (?=\d)', '', s)


def extract_prereg_urls(text: str) -> list:
    found = []
    for pat in COMPILED_PREREG_URLS:
        for match in pat.findall(text):
            clean = _strip_spaces(match)
            if re.match(r"^AEARCTR-\d+$", clean, re.IGNORECASE):
                trial_num = int(re.search(r"\d+", clean).group())
                found.append(f"https://www.socialscienceregistry.org/trials/{trial_num}")
            elif re.match(r"^AsPredicted\s*#\s*\d+$", clean, re.IGNORECASE):
                num = re.search(r"\d+", clean).group()
                found.append(f"https://aspredicted.org/blind.php?x={num}")
            else:
                # Strip internal digit-spaces from URLs too
                found.append(clean)
    seen = set()
    return [x for x in found if not (x in seen or seen.add(x)) and not is_generic_link(x)]


def auto_check(text: str) -> dict:
    has_data = phrase_hit(text, DATA_PHRASES) or regex_hit(text, COMPILED_DATA_WORDS)
    has_exp  = phrase_hit(text, EXPERIMENT_PHRASES) or regex_hit(text, COMPILED_EXPERIMENT_WORDS)
    no_data  = 0 if (has_data or has_exp) else 1

    has_prereg_phrase = phrase_hit(text, PREREG_PHRASES)
    has_prereg_token  = regex_hit(text, COMPILED_PREREG_WORDS)
    prereg = 1 if (has_prereg_phrase or has_prereg_token) else 0

    if prereg == 1 and phrase_hit(text, PREREG_VOTER_PHRASES):
        registry_signal = (
            phrase_hit(text, ["analysis plan", "pre-analysis plan", "preanalysis plan",
                               "aearctr-", "socialscienceregistry.org",
                               "osf.io", "aspredicted.org", "clinicaltrials.gov",
                               "egap.org", "open science framework"])
            or regex_hit(text, [re.compile(r"AEARCTR-\d+", re.I),
                                 re.compile(r"\bpap\b", re.I),
                                 re.compile(r"\bosf\b", re.I)])
        )
        prereg = 1 if registry_signal else 0

    use_aearct = 1 if (phrase_hit(text, ["aea rct", "aearctr-", "socialscienceregistry.org"])
                       or regex_hit(text, [re.compile(r"\baearct\b", re.I)])) else 0
    use_osf    = 1 if phrase_hit(text, ["open science framework", "osf.io"]) else 0
    use_asp    = 1 if phrase_hit(text, ["aspredicted.org", "aspredicted"]) else 0
    use_other  = 1 if phrase_hit(text, ["clinicaltrials.gov", "egap.org", "ridie"]) else 0

    prereg_urls = extract_prereg_urls(text)

    type_lab    = 1 if phrase_hit(text, ["laboratory experiment", "lab experiment",
                                         "laboratory setting", "lab setting"]) \
                       or regex_hit(text, [re.compile(r"\blaboratory\b", re.I)]) else 0
    type_field  = 1 if phrase_hit(text, ["field experiment", "randomized controlled trial",
                                         "randomized control trial", "rct ", " rct",
                                         "(rct)", "randomized evaluation"]) else 0
    type_online = 1 if phrase_hit(text, ["online experiment", "mechanical turk",
                                         "mturk", "prolific", "amazon turk"]) else 0
    type_survey = 1 if (regex_hit(text, [re.compile(r"\bsurvey\b", re.I)])
                        and not type_lab and not type_field and not type_online) else 0
    type_obs    = 1 if (phrase_hit(text, ["observational data", "administrative data",
                                          "panel data", "census data",
                                          "administrative records"])
                        and not type_lab and not type_field and not type_online) else 0

    return {
        "auto_no_data":         no_data,
        "auto_prereg":          prereg,
        "auto_use_aearct":      use_aearct,
        "auto_use_osf":         use_osf,
        "auto_use_aspredicted": use_asp,
        "auto_use_other":       use_other,
        "auto_link_prereg":     "; ".join(prereg_urls),
        "auto_type_lab":        type_lab,
        "auto_type_field":      type_field,
        "auto_type_online":     type_online,
        "auto_type_survey":     type_survey,
        "auto_type_obs":        type_obs,
    }


def extract_text_from_pdf(pdf_path: Path) -> tuple[str, str]:
    """
    Extract text from a PDF file using both PyMuPDF and pdfminer.six.
    Both engines run on every PDF and their outputs are combined so that
    ligature/encoding gaps in one are covered by the other.
    Returns (text, source) where source is 'pymupdf+pdfminer', 'pymupdf',
    'pdfminer', or 'none'.
    """
    pymupdf_text = ""
    pdfminer_text = ""

    try:
        import fitz
        doc = fitz.open(str(pdf_path))
        pymupdf_text = "\n".join(page.get_text() for page in doc)
        doc.close()
    except Exception:
        pass

    try:
        from pdfminer.high_level import extract_text as pm_extract
        result = pm_extract(str(pdf_path))
        if result:
            pdfminer_text = result
    except Exception:
        pass

    if pymupdf_text.strip() and pdfminer_text.strip():
        combined = pymupdf_text + "\n" + pdfminer_text
        return combined, "pymupdf+pdfminer"
    if pymupdf_text.strip():
        return pymupdf_text, "pymupdf"
    if pdfminer_text.strip():
        return pdfminer_text, "pdfminer"
    return "", "none"


def get_journal_name(pdf_path: Path, root: Path) -> str:
    """
    Return the name of the first-level sub-folder under root that contains
    this PDF. Falls back to the PDF's direct parent folder name.
    """
    try:
        rel = pdf_path.relative_to(root)
        parts = rel.parts
        if len(parts) >= 2:
            return parts[0]  # first-level sub-folder = journal
        return pdf_path.parent.name
    except ValueError:
        return pdf_path.parent.name


def load_done_paths(csv_path: Path) -> set:
    done = set()
    if csv_path.exists():
        with open(csv_path, newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                p = row.get("pdf_path", "").strip()
                if p:
                    done.add(p)
    return done


# ── Main ──────────────────────────────────────────────────────────────────────

def scan_folder(root: Path, sample: int = None, prereg_only: bool = False,
                output_csv: Path = OUTPUT_CSV):
    """Walk root recursively, process every PDF found."""

    all_pdfs = sorted(root.rglob("*.pdf"))
    if not all_pdfs:
        print(f"No PDF files found under: {root}")
        return

    done_paths = load_done_paths(output_csv)
    pending = [p for p in all_pdfs if str(p) not in done_paths]

    if sample:
        pending = pending[:sample]

    write_header = not output_csv.exists() or len(done_paths) == 0

    total_pdfs    = len(all_pdfs)
    already_done  = len(all_pdfs) - len([p for p in all_pdfs if str(p) not in done_paths])
    print(f"Found {total_pdfs} PDFs total  |  {already_done} already processed  |  "
          f"Processing {len(pending)} now")
    if prereg_only:
        print("  [--prereg-only mode: writing only papers with auto_prereg=1 to CSV]")

    with open(output_csv, "a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=CSV_FIELDS, extrasaction="ignore")
        if write_header:
            writer.writeheader()

        for pdf_path in tqdm(pending, unit="pdf"):
            journal = get_journal_name(pdf_path, root)
            text, source = extract_text_from_pdf(pdf_path)

            if text.strip():
                checks = auto_check(text)
            else:
                checks = {k: "" for k in CSV_FIELDS if k.startswith("auto_")}

            row = {
                "pdf_path":    str(pdf_path),
                "filename":    pdf_path.name,
                "journal":     journal,
                "text_length": len(text),
                "text_source": source,
            }
            row.update(checks)

            if prereg_only and checks.get("auto_prereg") != 1:
                continue  # skip non-prereg papers in output

            writer.writerow(row)
            f.flush()

    print(f"\nDone. Results saved to: {output_csv}")

    # Summary stats
    if output_csv.exists():
        with open(output_csv, newline="", encoding="utf-8") as f:
            rows = list(csv.DictReader(f))
        n_prereg = sum(1 for r in rows if r.get("auto_prereg") == "1")
        n_no_text = sum(1 for r in rows if r.get("text_source") == "none")
        print(f"\nSummary ({len(rows)} papers in CSV):")
        print(f"  auto_prereg = 1 : {n_prereg}")
        print(f"  No text extracted: {n_no_text}")


# ── CLI ───────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Scan a folder of journal PDFs for pre-registration mentions."
    )
    parser.add_argument(
        "--folder", type=str,
        default=r"F:\paperpdfs\hit_pap",
        help="Root folder containing journal subfolders with PDFs "
             r"(default: F:\paperpdfs\hit_pap)",
    )
    parser.add_argument(
        "--sample", type=int, default=None,
        help="Process at most N PDFs (useful for testing)",
    )
    parser.add_argument(
        "--prereg-only", action="store_true", default=False,
        help="Only write rows where auto_prereg=1 to the CSV",
    )
    parser.add_argument(
        "--output", type=str, default=None,
        help="Path to output CSV (default: Pre-Registration-Detection/output/pdf_scan_results.csv)",
    )
    args = parser.parse_args()

    root_folder = Path(args.folder)
    if not root_folder.exists():
        print(f"ERROR: Folder not found: {root_folder}", file=sys.stderr)
        sys.exit(1)

    out_csv = Path(args.output) if args.output else OUTPUT_CSV

    scan_folder(
        root=root_folder,
        sample=args.sample,
        prereg_only=args.prereg_only,
        output_csv=out_csv,
    )
