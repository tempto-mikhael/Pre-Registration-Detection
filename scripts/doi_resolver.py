"""
doi_resolver.py
---------------
Reconstruct DOIs from PDF filenames using known journal-specific patterns.
Returns None when the pattern is not directly resolvable (needs CrossRef lookup).
"""

import re


# ---------------------------------------------------------------------------
# Journal publisher groups
# ---------------------------------------------------------------------------

# AEA journals: filename stem → DOI prefix 10.1257
AEA_STEMS = {
    "american_economic_review":                  r"^(aer\.\d+)\.pdf$",
    "american_economic_journal_applied_economics": r"^(app\.\d+)\.pdf$",
    "american_economic_journal_microeconomics":  r"^(mic\.\d+)\.pdf$",
    "american_economic_review_insights":         r"^(aeri\.\d+)\.pdf$",
    # AEJ:Policy uses slug-style filenames → no direct DOI
}

# Oxford Academic journals
OXFORD_PATTERNS = {
    "quarterly_journal_of_economics":            (r"^(qj[a-z]\d+)\.pdf$",  "10.1093/qje/"),
    "review_of_economic_studies":                (r"^(rd[a-z]\d+)\.pdf$",  "10.1093/restud/"),
    "journal_of_the_european_economic_association": (r"^(jv[a-z]\d+)\.pdf$", "10.1093/jeea/"),
    # Economic Journal: two eras
    # Wiley era (2017-2018): ecoj12629.pdf → 10.1111/ecoj.12629
    # Oxford era (2019+):   ej0001.pdf    → 10.1093/ej/ej0001
}


def resolve_doi(journal: str, pdf_filename: str) -> str | None:
    """
    Try to reconstruct a DOI from the PDF filename.

    Parameters
    ----------
    journal : str
        Journal identifier as it appears in column H of the spreadsheet.
    pdf_filename : str
        Bare filename from column I (e.g. "aer.20131026.pdf").

    Returns
    -------
    str | None
        DOI string (without URL prefix) if resolvable, else None.
    """
    if not pdf_filename:
        return None

    fn = pdf_filename.strip()

    # ------------------------------------------------------------------
    # 1. AEA journals  →  10.1257/<stem>
    # ------------------------------------------------------------------
    if journal in AEA_STEMS:
        m = re.match(AEA_STEMS[journal], fn, re.IGNORECASE)
        if m:
            return f"10.1257/{m.group(1)}"

    # AEJ:Economic Policy — slug filenames, no clean DOI extraction
    # Fall through to CrossRef.

    # ------------------------------------------------------------------
    # 2. Econometrica  →  10.3982/ECTA<number>
    # ------------------------------------------------------------------
    if journal == "econometrica":
        m = re.match(r"^(ECTA\d+)\.pdf$", fn, re.IGNORECASE)
        if m:
            return f"10.3982/{m.group(1).upper()}"

    # ------------------------------------------------------------------
    # 3. Management Science  →  10.1287/<stem>
    # ------------------------------------------------------------------
    if journal == "management_science":
        m = re.match(r"^(mnsc\..+)\.pdf$", fn, re.IGNORECASE)
        if m:
            return f"10.1287/{m.group(1)}"

    # ------------------------------------------------------------------
    # 4. Review of Economics and Statistics  →  10.1162/<stem>
    # ------------------------------------------------------------------
    if journal == "review_of_economics_statistics":
        m = re.match(r"^(rest_a_\d+)\.pdf$", fn, re.IGNORECASE)
        if m:
            return f"10.1162/{m.group(1)}"

    # ------------------------------------------------------------------
    # 5. Oxford Academic journals  →  10.1093/<journal>/<stem>
    # ------------------------------------------------------------------
    if journal in OXFORD_PATTERNS:
        pattern, prefix = OXFORD_PATTERNS[journal]
        m = re.match(pattern, fn, re.IGNORECASE)
        if m:
            return f"{prefix}{m.group(1)}"

    # ------------------------------------------------------------------
    # 6. Economic Journal — dual publisher
    #    Wiley era: ecoj12629.pdf → 10.1111/ecoj.12629
    #    Oxford era: ej0001.pdf  → 10.1093/ej/ej0001
    # ------------------------------------------------------------------
    if journal == "economic_journal":
        m = re.match(r"^ecoj(\d+)\.pdf$", fn, re.IGNORECASE)
        if m:
            return f"10.1111/ecoj.{m.group(1)}"
        m = re.match(r"^(ej\d+)\.pdf$", fn, re.IGNORECASE)
        if m:
            return f"10.1093/ej/{m.group(1)}"

    # ------------------------------------------------------------------
    # 7. Elsevier journals (GEB, JDE, JEBO, JPubEcon, JESA via PII)
    #    Pattern: 1-s2.0-S<PII>-main.pdf
    #    We return the PII so callers can use the Elsevier/CrossRef API.
    # ------------------------------------------------------------------
    m = re.match(r"^1-s2\.0-(S\d+)-main\.pdf$", fn, re.IGNORECASE)
    if m:
        # Not a real DOI — signal with "PII:" prefix; main.py handles this
        return f"PII:{m.group(1)}"

    # ------------------------------------------------------------------
    # 8.  Journal of Public Economics — Elsevier slug-title filenames
    #     Pattern: Title-words_Year_Journal-of-Public.pdf
    #     Extract title slug for CrossRef search.
    # ------------------------------------------------------------------
    if journal == "journal_of_public_economics":
        # slug up to first underscore followed by 4-digit year
        m = re.match(r"^(.+?)_\d{4}_", fn)
        if m:
            title_slug = m.group(1).replace("-", " ").replace("_", " ")
            return f"TITLE_SLUG:{title_slug}"

    # ------------------------------------------------------------------
    # 9. Springer journals (Experimental Economics, JESA)
    #    Pattern: Author2017_Article_TitleParts.pdf
    #    Extract year + title slug.
    # ------------------------------------------------------------------
    if journal in ("experimental_economics", "journal_of_the_economic_science_association"):
        m = re.match(r"^.+?(\d{4})_Article_(.+)\.pdf$", fn)
        if m:
            title_slug = re.sub(r"([A-Z])", r" \1", m.group(2)).strip()
            return f"TITLE_SLUG:{title_slug}"

    # All other cases (JPE numeric IDs, AEJ:Policy slugs, etc.): None
    return None


def doi_to_url(doi: str) -> str:
    """Return the canonical https://doi.org URL for a DOI."""
    return f"https://doi.org/{doi}"
