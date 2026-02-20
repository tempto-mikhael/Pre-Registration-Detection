"""
verify_prelabeled.py  [v4 – author cross-matching]
---------------------------------------------------
For each pre-labelled (prereg=1) paper in the xlsx:

  BRANCH A  (xlsx_link_prereg is present)
    1. Normalize the stored link text (bare IDs, missing scheme, multi-URL cells)
    2. Try each URL; for each:
         a. HTTP GET with browser headers
         b. Extract registry page title:
              - OSF: JSON API   (not scraping — avoids JS-render problem)
              - AsPredicted: look for "Title:" label in body HTML
              - AEA-RCT: h1 or og:title
         c. SequenceMatcher similarity vs. paper title
    3. Best-match URL wins; also cross-check: did paper DOI appear in trial page?
    4. Verdicts:
         VERIFIED          – sim >= 0.45, or DOI found in trial page
         DOI_CONFIRMED     – title sim low, but paper DOI found in trial page HTML
         AUTHOR_CONFIRMED  – title sim < 0.45 but ≥ 50% of paper authors (from CrossRef)
                             appear on the registry page  [NEW in v4]
         UNCERTAIN         – sim 0.25-0.45, author check inconclusive
         TITLE_MISMATCH    – sim < 0.25  (possible genuine title change)
         BROKEN_LINK       – all URLs return 4xx/5xx
         UNREACHABLE       – all URLs fail with connection error

  BRANCH B  (xlsx_link_prereg is EMPTY, ~9/577 papers)
    API re-discovery: AEA-RCT HTML, EGAP, OSF
    Verdict: NEW_LINK_FOUND / NO_LINK_FOUND

  AUTHOR CROSS-CHECK  (v4 enhancement, runs on UNCERTAIN / TITLE_MISMATCH)
    a. Derive paper DOI from xlsx file_name path or CrossRef title search
    b. Look up author family names via CrossRef
    c. Check if ≥ 50% of names appear on the registry page (AEA-RCT page text,
       OSF /contributors/ API, or AsPredicted page text)
    d. If yes → upgrade verdict to AUTHOR_CONFIRMED

Output:  output/prelabeled_verify.csv
Resumable: already-processed xlsx_ids are skipped.

Usage:
  python scripts/verify_prelabeled.py [--delay 0.5] [--limit N]
"""

import argparse
import csv
import sys
import time
import re
import unicodedata
from difflib import SequenceMatcher
from pathlib import Path
from urllib.parse import urlparse, quote as urlquote

try:
    import openpyxl
    import requests
    from bs4 import BeautifulSoup
except ImportError as e:
    print(f"Missing package: {e}. Run: pip install openpyxl requests beautifulsoup4")
    sys.exit(1)

sys.path.insert(0, str(Path(__file__).parent))

from find_prereg_links import (
    check_egap,
    check_aearctr_html,
    unique,
    SESSION,
    CONTACT_EMAIL,
)

# ──────────────────────────────────────────────────────────────────────────────
PROJECT_ROOT = Path(__file__).parent.parent
XLSX_PATH    = PROJECT_ROOT / "journal_articles_with_pap_2025-03-14.xlsx"
RESULTS_CSV  = PROJECT_ROOT / "output" / "results.csv"
OUTPUT_CSV   = PROJECT_ROOT / "output" / "prelabeled_verify.csv"

OUTPUT_FIELDS = [
    "xlsx_id", "row_num", "journal", "title", "doi", "pub_year",
    "xlsx_link_prereg",
    "verified_url",
    "http_status",
    "registry_page_title",
    "title_sim",
    "doi_in_page",
    "author_match",
    "all_found_links",
    "match_level",
    "match_notes",
]

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.5",
    # Omit Accept-Encoding: requests handles gzip/deflate automatically;
    # including 'br' (Brotli) would make the server send content that requests
    # cannot decompress without the optional 'brotli' package → empty body.
}

SIM_VERIFIED  = 0.45
SIM_UNCERTAIN = 0.25

AUTHOR_MATCH_THRESHOLD = 0.50   # fraction of author surnames that must appear
MIN_SURNAME_LENGTH     = 3     # skip very short names ("Li") to avoid false positives

# CrossRef polite-pool headers
CROSSREF_HEADERS = {
    "User-Agent": f"verify-prelabeled/4.0; mailto:{CONTACT_EMAIL}",
}


# ──────────────────────────────────────────────────────────────────────────────
# 0. DOI derivation from xlsx file_name & CrossRef author lookup
# ──────────────────────────────────────────────────────────────────────────────

# Mapping of journal-folder prefix → DOI prefix
_JOURNAL_DOI_MAP = {
    "american_economic_journal_applied":     "10.1257",
    "american_economic_journal_economic":    "10.1257",
    "american_economic_journal_macro":       "10.1257",
    "american_economic_journal_micro":       "10.1257",
    "american_economic_review":              "10.1257",
    "american_economic_review_insights":     "10.1257",
    "econometrica":                          "10.3982",
    "quarterly_journal_of_economics":        "10.1093/qje",
    "review_of_economic_studies":            "10.1093/restud",
    "review_of_economics_statistics":        "10.1162/rest_a",
    "journal_of_political_economy":          "10.1086",
}


def extract_doi_from_filename(file_name: str) -> str:
    """
    Derive a probable DOI from the xlsx file_name column.
    e.g. 'journal/american_economic_journal_applied_economics/Vol-11/.../app.20170008.pdf'
         → '10.1257/app.20170008'
    Returns empty string if the journal is not in _JOURNAL_DOI_MAP.
    """
    if not file_name:
        return ""
    basename = file_name.rsplit("/", 1)[-1].replace(".pdf", "").strip()
    parts = file_name.split("/")
    jfolder = parts[1] if len(parts) >= 2 else ""
    jfolder_lower = jfolder.lower()
    for prefix, doi_prefix in _JOURNAL_DOI_MAP.items():
        if jfolder_lower.startswith(prefix):
            if "/" in doi_prefix:
                return f"{doi_prefix}/{basename}"
            else:
                return f"{doi_prefix}/{basename}"
    return ""


def crossref_authors_by_doi(doi: str) -> list[str]:
    """Return list of family names from CrossRef for a given DOI."""
    if not doi:
        return []
    try:
        r = requests.get(
            f"https://api.crossref.org/works/{doi}",
            headers=CROSSREF_HEADERS, timeout=15,
        )
        if r.status_code != 200:
            return []
        msg = r.json().get("message", {})
        return [a["family"] for a in msg.get("author", []) if a.get("family")]
    except Exception:
        return []


def crossref_authors_by_title(title: str) -> tuple[list[str], str]:
    """
    Search CrossRef by title. Returns (family_names, doi).
    Falls back to empty when the top result clearly doesn't match.
    """
    if not title:
        return [], ""
    try:
        r = requests.get(
            "https://api.crossref.org/works",
            params={"query.title": title[:120], "rows": 1},
            headers=CROSSREF_HEADERS, timeout=15,
        )
        if r.status_code != 200:
            return [], ""
        items = r.json().get("message", {}).get("items", [])
        if not items:
            return [], ""
        item = items[0]
        # Quick sanity: make sure the top result's title is close enough
        cr_title = " ".join(item.get("title", [])).lower()
        if SequenceMatcher(None, title.lower()[:100], cr_title[:100]).ratio() < 0.55:
            return [], ""  # top result is for a different paper
        families = [a["family"] for a in item.get("author", []) if a.get("family")]
        doi = item.get("DOI", "")
        return families, doi
    except Exception:
        return [], ""


def _normalize_name(name: str) -> str:
    """Strip accents and lowercase for fuzzy name matching."""
    nfkd = unicodedata.normalize("NFKD", name)
    return "".join(c for c in nfkd if not unicodedata.combining(c)).lower()


def osf_api_contributors(url: str) -> list[str]:
    """
    Get contributor family names from OSF API for a registration / node / preprint.
    Returns list of family names.
    """
    m = re.search(r"osf\.io/(?:preprints/osf/)?([a-z0-9]+)", url, re.IGNORECASE)
    if not m:
        return []
    node_id = m.group(1).lower()
    for endpoint in ("registrations", "nodes", "preprints"):
        try:
            api_url = f"https://api.osf.io/v2/{endpoint}/{node_id}/contributors/?embed=users"
            r = requests.get(api_url, timeout=15)
            if r.status_code != 200:
                continue
            names = []
            for item in r.json().get("data", []):
                user_data = item.get("embeds", {}).get("users", {}).get("data", {})
                if isinstance(user_data, dict):
                    attrs = user_data.get("attributes", {})
                    family = attrs.get("family_name", "")
                    if family:
                        names.append(family)
                    elif attrs.get("full_name"):
                        names.append(attrs["full_name"].split()[-1])
            if names:
                return names
        except Exception:
            pass
    return []


def author_overlap(
    paper_authors: list[str],
    registry_page_text: str,
    registry_url: str,
) -> tuple[float, str]:
    """
    Compute what fraction of paper authors appear on the registry page.
    Uses page text for AEA-RCT / AsPredicted; OSF API for OSF pages.
    Returns (overlap_ratio, detail_string).
    """
    if not paper_authors:
        return 0.0, "no_authors"

    # For OSF pages, use the API contributor list instead of JS-rendered page
    osf_contribs: list[str] = []
    if "osf.io" in registry_url.lower():
        osf_contribs = osf_api_contributors(registry_url)

    matchable = [a for a in paper_authors if len(a) >= MIN_SURNAME_LENGTH]
    if not matchable:
        return 0.0, "short_names_only"

    matched: list[str] = []
    page_lower = _normalize_name(registry_page_text)
    osf_lower = " ".join(_normalize_name(n) for n in osf_contribs)

    for surname in matchable:
        sn = _normalize_name(surname)
        if sn in page_lower or (osf_lower and sn in osf_lower):
            matched.append(surname)

    ratio = len(matched) / len(matchable)
    detail = f"{len(matched)}/{len(matchable)} ({', '.join(matched) if matched else 'none'})"
    return ratio, detail


# ──────────────────────────────────────────────────────────────────────────────
# 1. URL normalization  (Fix #5: handle bare IDs, missing scheme, etc.)
# ──────────────────────────────────────────────────────────────────────────────

_AEARCTR_URL = "https://www.socialscienceregistry.org/trials/{}"


def _aearctr_id_to_url(raw_id: str) -> str:
    """Convert a bare trial number or AEARCTR-XXXXXXX string to a full URL."""
    num = str(raw_id).lstrip("0") or "0"
    return _AEARCTR_URL.format(num)


def _normalize_single(text: str) -> list[str]:
    """
    Given a single (non-semicolon-split) cell fragment, return 0-N valid URLs.
    """
    text = text.strip().strip('"').strip("'").rstrip(".")

    # Already a valid URL
    if re.match(r"^https?://", text):
        return [text]

    # Missing scheme: www.socialscienceregistry.org/...
    if re.match(r"^www\.", text):
        return ["https://" + text]

    # DOI.org shorthand that might have been stored without scheme
    if re.match(r"^doi\.org/", text):
        return ["https://" + text]

    # Bare AEARCTR-XXXXXXX  (with or without leading word)
    m = re.search(r"AEARCTR[-–]0*(\d+)", text, re.IGNORECASE)
    if m:
        return [_aearctr_id_to_url(m.group(1))]

    # AEA RCT Registry (XXXXXXX)  or  AEA registry (ID XXXXXXX)
    m = re.search(r"(?:registry|AEA)[^()]*\(?(?:ID\s*)?(\d{4,7})\)?", text, re.IGNORECASE)
    if m:
        return [_aearctr_id_to_url(m.group(1))]

    # Bare integer 3-7 digits → assume socialscienceregistry.org trial
    if re.fullmatch(r"\d{3,7}", text):
        return [_aearctr_id_to_url(text)]

    # AsPredicted bare number: "#15066" or "#15066, #42342"
    asp_ids = re.findall(r"#(\d+)", text)
    if asp_ids:
        return [f"https://aspredicted.org/{i}" for i in asp_ids]

    # Free-text sentences containing AEARCTR IDs
    ids = re.findall(r"AEARCTR[-–]0*(\d+)", text, re.IGNORECASE)
    if ids:
        return [_aearctr_id_to_url(i) for i in ids]

    return []  # couldn't normalize


def split_and_normalize(cell_value: str) -> list[str]:
    """
    Split a xlsx_link_prereg cell that may contain multiple URLs/identifiers
    (separated by ; or newline) and normalize each to a full URL.
    Returns de-duplicated list of proper URLs.
    """
    if not cell_value or not cell_value.strip():
        return []

    seen: list[str] = []
    # Split on semicolons (and surrounding whitespace)
    parts = re.split(r"\s*;\s*", cell_value)
    for part in parts:
        part = part.strip()
        if not part:
            continue
        normalized = _normalize_single(part)
        for url in normalized:
            if url not in seen:
                seen.append(url)
    return seen


# ──────────────────────────────────────────────────────────────────────────────
# 2. Registry page title extraction
# ──────────────────────────────────────────────────────────────────────────────

def osf_api_title(url: str) -> str:
    """
    Fetch OSF registration title via the JSON API instead of scraping
    (OSF is a JS-rendered SPA — HTML scraping only returns 'OSF').
    Handles both registrations and preprints.
    """
    # Extract OSF node ID from URL
    m = re.search(r"osf\.io/(?:preprints/osf/)?([a-z0-9]+)", url, re.IGNORECASE)
    if not m:
        return ""
    node_id = m.group(1).lower()

    api_urls = [
        f"https://api.osf.io/v2/registrations/{node_id}/",
        f"https://api.osf.io/v2/nodes/{node_id}/",
        f"https://api.osf.io/v2/preprints/{node_id}/",
    ]
    for api_url in api_urls:
        try:
            r = requests.get(
                api_url,
                headers={"User-Agent": f"verify-prelabeled/3.0; mailto:{CONTACT_EMAIL}"},
                timeout=15,
            )
            if r.status_code == 200:
                data = r.json()
                if "data" in data and "attributes" in data["data"]:
                    title = data["data"]["attributes"].get("title", "")
                    if title:
                        return title.strip()
        except Exception:
            pass
    return ""


def aspredicted_title(soup: BeautifulSoup) -> str:
    """
    Extract study title from an AsPredicted page.
    AsPredicted pages put the study title in <h3><b><i>TITLE</i></b>...</h3>.
    """
    # Strategy 1: h3 → <i> tag inside it (the title is italicised)
    h3 = soup.find("h3")
    if h3:
        italic = h3.find("i")
        if italic:
            t = italic.get_text(strip=True).strip("'\"")
            if t and len(t) > 5:
                return t
        # Fallback: h3 full text, strip "(AsPredicted #NNN)" suffix
        h3_text = h3.get_text(" ", strip=True)
        h3_text = re.sub(r"\(AsPredicted\s*#[\d,]+\).*", "", h3_text, flags=re.IGNORECASE).strip("'\" ")
        if h3_text and len(h3_text) > 5 and not h3_text.startswith("#"):
            return h3_text

    # Strategy 2: look for a table row whose first cell says "Title"
    for tr in soup.find_all("tr"):
        cells = tr.find_all(["td", "th"])
        if len(cells) >= 2 and "title" in cells[0].get_text(strip=True).lower():
            candidate = cells[1].get_text(strip=True)
            if len(candidate) > 10:
                return candidate

    return ""


def _extract_registry_title(soup: BeautifulSoup, url: str) -> str:
    """Extract the pre-registration title from a registry page's HTML."""
    domain = urlparse(url).netloc.lower()

    # ── AEA-RCT (socialscienceregistry.org) ──────────────────────────────────
    if "socialscienceregistry" in domain:
        for sel in [
            ".trial-title", "h1.trial-title", "h1.title", "h1",
            "[class*='trial'][class*='title']",
        ]:
            el = soup.select_one(sel)
            if el:
                t = el.get_text(strip=True)
                if t and t != "AEA RCT Registry":
                    return t
        og = soup.find("meta", property="og:title")
        if og and og.get("content"):
            c = og["content"].strip()
            if c and c != "AEA RCT Registry":
                return c

    # ── AsPredicted (aspredicted.org) ────────────────────────────────────────
    elif "aspredicted" in domain:
        t = aspredicted_title(soup)
        if t and t not in ("Pre-registrations", "AsPredicted"):
            return t

    # ── EGAP ─────────────────────────────────────────────────────────────────
    elif "egap" in domain:
        for sel in [".plan-title", "h1.plan-title", "h1", "h2"]:
            el = soup.select_one(sel)
            if el and el.get_text(strip=True):
                return el.get_text(strip=True)

    # ── Fallback: <title> tag, strip site suffix ─────────────────────────────
    title_tag = soup.find("title")
    if title_tag:
        t = title_tag.get_text(strip=True)
        for suffix in [" | AEA RCT Registry", " | OSF", " | AsPredicted", " - EGAP"]:
            if t.lower().endswith(suffix.lower()):
                t = t[: -len(suffix)].strip()
        if t and t not in ("AEA RCT Registry", "OSF", "AsPredicted"):
            return t

    return ""


# ──────────────────────────────────────────────────────────────────────────────
# 3. DOI cross-check: does the paper's DOI appear in the trial page HTML?
#    (Fix #4: confirm connection even when title changed between prereg/pub)
# ──────────────────────────────────────────────────────────────────────────────

def doi_in_page_html(html_text: str, paper_doi: str) -> bool:
    """Return True if the paper DOI appears anywhere in the registry page HTML."""
    if not paper_doi:
        return False
    doi_clean = paper_doi.strip().lower().lstrip("https://doi.org/").lstrip("doi:")
    return doi_clean in html_text.lower()


# ──────────────────────────────────────────────────────────────────────────────
# 4. Single URL validation
# ──────────────────────────────────────────────────────────────────────────────

def validate_single_url(url: str, paper_title: str, paper_doi: str = "",
                        timeout: int = 20) -> dict:
    """
    HTTP GET a single URL. Returns:
      http_status, registry_page_title, title_sim, doi_in_page, verdict, notes
    """
    result = {
        "http_status": "",
        "registry_page_title": "",
        "title_sim": "",
        "doi_in_page": False,
        "verdict": "UNREACHABLE",
        "notes": "",
    }

    # Special handling: OSF → use JSON API for title (avoids JS-render problem)
    is_osf = "osf.io" in url.lower()

    try:
        resp = requests.get(url, headers=HEADERS, timeout=timeout, allow_redirects=True)
        result["http_status"] = str(resp.status_code)

        if resp.status_code >= 400:
            result["verdict"] = "BROKEN_LINK"
            result["notes"]   = f"HTTP {resp.status_code}"
            return result

        content_type = resp.headers.get("content-type", "")
        if "html" not in content_type and "text/" not in content_type:
            result["verdict"] = "BROKEN_LINK"
            result["notes"]   = f"Non-HTML response: {content_type}"
            return result

        html_text = resp.text
        doi_found = doi_in_page_html(html_text, paper_doi)
        result["doi_in_page"] = doi_found

        # Use final URL after redirects for domain-specific title extraction
        final_url = resp.url or url

        # Get page title
        if is_osf or "osf.io" in final_url.lower():
            page_title = osf_api_title(final_url)   # API — reliable
            if not page_title:
                soup = BeautifulSoup(html_text, "html.parser")
                page_title = _extract_registry_title(soup, final_url)
        else:
            soup = BeautifulSoup(html_text, "html.parser")
            page_title = _extract_registry_title(soup, final_url)

        result["registry_page_title"] = page_title

        # Compute similarity
        if not page_title or not paper_title:
            sim_str = "N/A"
            sim     = 0.0
        else:
            sim = SequenceMatcher(None,
                                  paper_title.lower().strip(),
                                  page_title.lower().strip()).ratio()
            sim_str = f"{sim:.3f}"
        result["title_sim"] = sim_str

        # Verdict
        if doi_found and sim < SIM_VERIFIED:
            result["verdict"] = "DOI_CONFIRMED"
            result["notes"]   = f"DOI found in page; sim={sim_str}"
        elif page_title and sim >= SIM_VERIFIED:
            result["verdict"] = "VERIFIED"
            result["notes"]   = f"sim={sim_str}"
        elif sim_str == "N/A":
            # Could not extract page title — treat as uncertain, not verified
            result["verdict"] = "UNCERTAIN"
            result["notes"]   = "No title extracted from page"
        elif sim >= SIM_UNCERTAIN:
            result["verdict"] = "UNCERTAIN"
            result["notes"]   = (
                f"sim={sim_str}; "
                f"paper='{paper_title[:55]}'; "
                f"registry='{page_title[:55]}'"
            )
        else:
            result["verdict"] = "TITLE_MISMATCH"
            result["notes"]   = (
                f"sim={sim_str}; "
                f"paper='{paper_title[:55]}'; "
                f"registry='{page_title[:55]}'"
            )
        if doi_found:
            result["notes"] += " [DOI in page]"

    except requests.exceptions.Timeout:
        result["verdict"] = "UNREACHABLE"
        result["notes"]   = "Timeout"
    except requests.exceptions.ConnectionError as e:
        result["verdict"] = "UNREACHABLE"
        result["notes"]   = f"ConnectionError: {str(e)[:80]}"
    except Exception as e:
        result["verdict"] = "UNREACHABLE"
        result["notes"]   = f"Error: {str(e)[:80]}"

    return result


# ──────────────────────────────────────────────────────────────────────────────
# 5. Multi-URL validation  (try each URL in the cell, take best result)
# ──────────────────────────────────────────────────────────────────────────────

_VERDICT_RANK = {
    "VERIFIED": 0,
    "DOI_CONFIRMED": 1,
    "AUTHOR_CONFIRMED": 2,
    "UNCERTAIN": 3,
    "TITLE_MISMATCH": 4,
    "BROKEN_LINK": 5,
    "UNREACHABLE": 6,
}


def validate_best(urls: list[str], paper_title: str, paper_doi: str,
                  delay: float) -> tuple[str, dict]:
    """
    Try each URL and return (best_url, best_result) using verdict rank.
    Stops early if VERIFIED or DOI_CONFIRMED is achieved.
    """
    best_url    = urls[0] if urls else ""
    best_result = {
        "http_status": "", "registry_page_title": "",
        "title_sim": "", "doi_in_page": False,
        "verdict": "UNREACHABLE", "notes": "No valid URLs derived from cell",
    }

    for i, url in enumerate(urls):
        if i > 0:
            time.sleep(delay)
        r = validate_single_url(url, paper_title, paper_doi)
        current_rank = _VERDICT_RANK.get(r["verdict"], 99)
        best_rank    = _VERDICT_RANK.get(best_result["verdict"], 99)
        if current_rank < best_rank:
            best_url    = url
            best_result = r
        if best_result["verdict"] in ("VERIFIED", "DOI_CONFIRMED"):
            break   # good enough — stop trying

    return best_url, best_result


# ──────────────────────────────────────────────────────────────────────────────
# Branch B: API re-discovery for papers with no xlsx link
# ──────────────────────────────────────────────────────────────────────────────

def osf_title_search(title: str) -> list[str]:
    """Search OSF Registries by title, return list of matching URLs."""
    if not title:
        return []
    try:
        r = requests.get(
            "https://api.osf.io/v2/registrations/",
            params={"filter[title]": title[:120], "page[size]": 5},
            headers={"User-Agent": f"verify-prelabeled/3.0; mailto:{CONTACT_EMAIL}"},
            timeout=20,
        )
        if r.status_code != 200:
            return []
        items = r.json().get("data", []) or []
        return ["https://osf.io/" + item["id"] for item in items if item.get("id")]
    except Exception:
        return []


def discover_links(title: str, delay: float) -> list[str]:
    """Branch B: try AEA-RCT HTML, EGAP, OSF for papers with no xlsx link."""
    links: list[str] = []
    for fn in (check_aearctr_html, check_egap):
        try:
            links += fn(title)
        except Exception:
            pass
        time.sleep(delay)
    try:
        links += osf_title_search(title)
    except Exception:
        pass
    time.sleep(delay * 0.5)
    return unique(links)


# ──────────────────────────────────────────────────────────────────────────────
# Helper: load results.csv id → {doi, pub_year, row_num}
# ──────────────────────────────────────────────────────────────────────────────

def load_results_doi_map() -> dict[int, dict]:
    if not RESULTS_CSV.exists():
        return {}
    doi_map: dict[int, dict] = {}
    with open(RESULTS_CSV, newline="", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            try:
                rid = int(row.get("id") or 0)
            except ValueError:
                rid = 0
            if rid:
                doi_map[rid] = {
                    "doi":      row.get("doi", "") or "",
                    "pub_year": row.get("pub_year", "") or "",
                    "row_num":  row.get("row_num", "") or "",
                }
    return doi_map


# ──────────────────────────────────────────────────────────────────────────────
# Main
# ──────────────────────────────────────────────────────────────────────────────

def main():
    ap = argparse.ArgumentParser(description="Verify pre-labelled prereg=1 papers (v4)")
    ap.add_argument("--delay", type=float, default=0.5,
                    help="Seconds to wait between requests (default 0.5)")
    ap.add_argument("--limit", type=int, default=0,
                    help="Only process first N rows (0 = all)")
    args = ap.parse_args()

    if not XLSX_PATH.exists():
        print(f"ERROR: xlsx not found at {XLSX_PATH}")
        sys.exit(1)

    doi_map = load_results_doi_map()
    print(f"Loaded {len(doi_map)} DOI mappings from results.csv")

    # ── Read xlsx ─────────────────────────────────────────────────────────────
    wb = openpyxl.load_workbook(XLSX_PATH, read_only=True, data_only=True)
    ws = wb.active
    prereg_rows = []
    xlsx_row_num = 2
    for row in ws.iter_rows(min_row=3, values_only=True):
        xlsx_row_num += 1
        if row[4] is None:
            continue
        if row[9] != 1:
            continue
        prereg_rows.append({
            "xlsx_id":           int(row[4]),
            "xlsx_row":          xlsx_row_num,
            "file_name":         str(row[5] or "").strip(),
            "journal":           str(row[7] or "").strip(),
            "title":             str(row[6] or "").strip(),
            "xlsx_link_prereg":  str(row[10] or "").strip(),
        })
    wb.close()

    total = len(prereg_rows)
    print(f"Found {total} prereg=1 rows in xlsx")
    if args.limit:
        prereg_rows = prereg_rows[:args.limit]
        print(f"Limited to first {args.limit}")

    # ── Resumability ──────────────────────────────────────────────────────────
    done_ids: set[int] = set()
    if OUTPUT_CSV.exists():
        with open(OUTPUT_CSV, newline="", encoding="utf-8") as f:
            for row in csv.DictReader(f):
                try:
                    done_ids.add(int(row.get("xlsx_id", 0)))
                except ValueError:
                    pass
        remaining = sum(1 for r in prereg_rows if r["xlsx_id"] not in done_ids)
        print(f"Resuming: {len(done_ids)} done, {remaining} remaining")

    todo = [r for r in prereg_rows if r["xlsx_id"] not in done_ids]
    if not todo:
        print("Nothing left to process.")
        return

    OUTPUT_CSV.parent.mkdir(exist_ok=True)
    is_new = not OUTPUT_CSV.exists() or len(done_ids) == 0
    out_f  = open(OUTPUT_CSV, "a" if not is_new else "w", newline="", encoding="utf-8")
    writer = csv.DictWriter(out_f, fieldnames=OUTPUT_FIELDS)
    if is_new:
        writer.writeheader()
        out_f.flush()

    stats: dict[str, int] = {}

    for i, r in enumerate(todo, 1):
        xlsx_id   = r["xlsx_id"]
        journal   = r["journal"]
        title     = r["title"]
        raw_link  = r["xlsx_link_prereg"]

        doi_entry = doi_map.get(xlsx_id, {})
        doi      = doi_entry.get("doi", "")
        pub_year = doi_entry.get("pub_year", "")
        row_num  = doi_entry.get("row_num", "")

        short_title = title[:65] + ("..." if len(title) > 65 else "")
        print(f"[{i}/{len(todo)}] id={xlsx_id}  {short_title}")

        if raw_link:
            # ── BRANCH A: validate stored link ────────────────────────────
            urls = split_and_normalize(raw_link)
            if not urls:
                # Could not parse any URL from the cell
                match_level = "UNREACHABLE"
                notes = f"Could not normalize link: '{raw_link[:60]}'"
                row_out = {
                    "xlsx_id": xlsx_id, "row_num": row_num, "journal": journal,
                    "title": title, "doi": doi, "pub_year": pub_year,
                    "xlsx_link_prereg": raw_link, "verified_url": "",
                    "http_status": "", "registry_page_title": "", "title_sim": "",
                    "doi_in_page": "", "author_match": "",
                    "all_found_links": "",
                    "match_level": match_level, "match_notes": notes,
                }
                print(f"  Could not normalize: {raw_link[:60]}")
            else:
                print(f"  Trying {len(urls)} URL(s): {urls[0]}" +
                      (f" [+{len(urls)-1} more]" if len(urls) > 1 else ""))
                best_url, vr = validate_best(urls, title, doi, args.delay)
                match_level  = vr["verdict"]
                print(f"  HTTP {vr['http_status']} | sim={vr['title_sim']} "
                      f"| doi_in_page={vr['doi_in_page']} | {match_level}")

                # ── v4: Author cross-check for UNCERTAIN / TITLE_MISMATCH ──
                author_info = ""
                if match_level in ("UNCERTAIN", "TITLE_MISMATCH"):
                    file_name = r.get("file_name", "")
                    paper_doi = extract_doi_from_filename(file_name) or doi
                    paper_authors = crossref_authors_by_doi(paper_doi) if paper_doi else []
                    if not paper_authors:
                        paper_authors, _ = crossref_authors_by_title(title)
                    time.sleep(0.3)

                    if paper_authors:
                        # Get registry page text (we already fetched it above,
                        # but validate_single_url doesn't return raw HTML.
                        # Re-fetch is cheap since it'll be cached by the server.)
                        try:
                            page_resp = requests.get(best_url, headers=HEADERS,
                                                     timeout=20, allow_redirects=True)
                            page_text = page_resp.text if page_resp.status_code < 400 else ""
                        except Exception:
                            page_text = ""

                        overlap, detail = author_overlap(paper_authors, page_text, best_url)
                        author_info = detail
                        time.sleep(0.3)

                        if overlap >= AUTHOR_MATCH_THRESHOLD:
                            match_level = "AUTHOR_CONFIRMED"
                            vr["notes"] += f" [AUTHORS {detail}]"
                            print(f"  \u2191 AUTHOR_CONFIRMED: {detail}")
                        else:
                            vr["notes"] += f" [authors {detail}]"
                            print(f"  authors: {detail}")
                    else:
                        author_info = "no_cr_data"
                        print(f"  authors: no CrossRef data")

                row_out = {
                    "xlsx_id":             xlsx_id,
                    "row_num":             row_num,
                    "journal":             journal,
                    "title":               title,
                    "doi":                 doi,
                    "pub_year":            pub_year,
                    "xlsx_link_prereg":    raw_link,
                    "verified_url":        best_url,
                    "http_status":         vr["http_status"],
                    "registry_page_title": vr["registry_page_title"],
                    "title_sim":           vr["title_sim"],
                    "doi_in_page":         "1" if vr["doi_in_page"] else "",
                    "author_match":        author_info,
                    "all_found_links":     "; ".join(urls),
                    "match_level":         match_level,
                    "match_notes":         vr["notes"],
                }
        else:
            # ── BRANCH B: no xlsx link — try to find one ──────────────────
            print(f"  No link in xlsx — running discovery...")
            found = discover_links(title, args.delay)
            if found:
                match_level = "NEW_LINK_FOUND"
                notes       = "; ".join(found[:3])
            else:
                match_level = "NO_LINK_FOUND"
                notes       = "No link found by API search"
            print(f"  {match_level}")
            row_out = {
                "xlsx_id":             xlsx_id,
                "row_num":             row_num,
                "journal":             journal,
                "title":               title,
                "doi":                 doi,
                "pub_year":            pub_year,
                "xlsx_link_prereg":    "",
                "verified_url":        found[0] if found else "",
                "http_status":         "",
                "registry_page_title": "",
                "title_sim":           "",
                "doi_in_page":         "",
                "author_match":        "",
                "all_found_links":     "; ".join(found) if found else "",
                "match_level":         match_level,
                "match_notes":         notes,
            }

        stats[match_level] = stats.get(match_level, 0) + 1
        writer.writerow(row_out)
        out_f.flush()

        time.sleep(args.delay)

    out_f.close()

    total_done = len(done_ids) + len(todo)
    print()
    print(f"Done. {total_done} rows written to {OUTPUT_CSV}")
    print()
    print("Session stats:")
    order = ["VERIFIED", "DOI_CONFIRMED", "AUTHOR_CONFIRMED", "UNCERTAIN",
             "TITLE_MISMATCH", "BROKEN_LINK", "UNREACHABLE",
             "NEW_LINK_FOUND", "NO_LINK_FOUND"]
    for level in order:
        if level in stats:
            pct = stats[level] / len(todo) * 100
            print(f"  {level:<20s}: {stats[level]:4d}  ({pct:.1f}%)")


if __name__ == "__main__":
    main()
