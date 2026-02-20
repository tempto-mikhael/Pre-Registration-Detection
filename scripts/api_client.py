"""
api_client.py
-------------
Fetch paper metadata (DOI, title, abstract, OA PDF URL) from:
  1. OpenAlex  — primary source, free, no auth
  2. CrossRef  — DOI lookup by title+journal or by PII
  3. Unpaywall — OA PDF URL lookup for a known DOI

All functions return dicts; missing fields are None.
Set CONTACT_EMAIL to a real address to get polite-pool rate limits from APIs.
"""

import time
import re
import requests

# ── Configuration ────────────────────────────────────────────────────────────
CONTACT_EMAIL = "your@email.com"   # <-- change to your e-mail

OPENALEX_BASE  = "https://api.openalex.org"
CROSSREF_BASE  = "https://api.crossref.org"
UNPAYWALL_BASE = "https://api.unpaywall.org/v2"

SESSION = requests.Session()
SESSION.headers.update({
    "User-Agent": f"ercautomation/1.0 (mailto:{CONTACT_EMAIL})"
})

RETRY_WAIT   = 5   # seconds between retries
MAX_RETRIES  = 3


# ── Helpers ──────────────────────────────────────────────────────────────────

def _get(url: str, params: dict = None, retry: int = MAX_RETRIES) -> dict | None:
    """GET a JSON endpoint; returns parsed dict or None on failure."""
    for attempt in range(retry):
        try:
            r = SESSION.get(url, params=params, timeout=20)
            if r.status_code == 200:
                return r.json()
            if r.status_code == 429:          # rate-limited
                wait = int(r.headers.get("Retry-After", RETRY_WAIT * (attempt + 1)))
                print(f"    [rate-limit] waiting {wait}s ...")
                time.sleep(wait)
                continue
            if r.status_code in (404, 400, 422):  # unrecoverable
                return None
            print(f"    [HTTP {r.status_code}] {url}")
        except requests.RequestException as exc:
            print(f"    [error] {exc}")
            time.sleep(RETRY_WAIT)
    return None


def _clean_abstract(inverted_index: dict | None) -> str | None:
    """Reconstruct abstract text from OpenAlex inverted index."""
    if not inverted_index:
        return None
    positions = {}
    for word, pos_list in inverted_index.items():
        for pos in pos_list:
            positions[pos] = word
    if not positions:
        return None
    return " ".join(positions[i] for i in sorted(positions))


# ── OpenAlex ─────────────────────────────────────────────────────────────────

def openalex_by_doi(doi: str) -> dict | None:
    """
    Fetch OpenAlex work record by DOI.

    Returns dict with keys: doi, title, abstract, oa_pdf_url, openalex_id
    """
    encoded = requests.utils.quote(doi, safe="/")
    data = _get(f"{OPENALEX_BASE}/works/https://doi.org/{encoded}",
                params={"mailto": CONTACT_EMAIL})
    if not data:
        return None
    return _parse_openalex(data)


def openalex_by_title(title: str, journal: str = None) -> dict | None:
    """
    Search OpenAlex by title (and optionally journal).
    Returns the best matching result or None.
    """
    params = {
        "search": title,
        "filter": "type:article",
        "per_page": 3,
        "mailto": CONTACT_EMAIL,
    }
    data = _get(f"{OPENALEX_BASE}/works", params=params)
    if not data or not data.get("results"):
        return None

    results = data["results"]

    # Try to match by journal if provided
    if journal and len(results) > 1:
        j_clean = journal.lower().replace("_", " ")
        for r in results:
            venue = (r.get("primary_location") or {}).get("source") or {}
            venue_name = (venue.get("display_name") or "").lower()
            if j_clean in venue_name or venue_name in j_clean:
                return _parse_openalex(r)

    return _parse_openalex(results[0]) if results else None


def _parse_openalex(data: dict) -> dict:
    """
    Parse an OpenAlex work record.
    Collects ALL candidate OA PDF URLs, ordered by reliability:
      1. open_access.oa_url  (often the 'attachments' URL on publisher sites —
         this is the most reliable for AEA and similar bronze/green OA papers)
      2. pdf_url from every location that is OA
      3. landing_page_url from OA locations as last resort
    """
    candidates = []

    # Priority 1: top-level open_access.oa_url
    oa_block = data.get("open_access") or {}
    top_url = oa_block.get("oa_url")
    if top_url:
        candidates.append(top_url)

    # Priority 2 & 3: iterate all locations
    for loc in data.get("locations", []):
        if not loc.get("is_oa"):
            continue
        pdf = loc.get("pdf_url")
        landing = loc.get("landing_page_url")
        if pdf and pdf not in candidates:
            candidates.append(pdf)
        if landing and landing not in candidates:
            candidates.append(landing)

    return {
        "doi":             data.get("doi", "").replace("https://doi.org/", "") or None,
        "title":           data.get("title"),
        "abstract":        _clean_abstract(data.get("abstract_inverted_index")),
        "oa_pdf_candidates": candidates,          # full ordered list
        "oa_pdf_url":      candidates[0] if candidates else None,  # best guess
        "any_repo_fulltext": oa_block.get("any_repository_has_fulltext", False),
        "openalex_id":     data.get("id"),
        "pub_year":        data.get("publication_year"),
        "journal":         ((data.get("primary_location") or {})
                            .get("source") or {})
                           .get("display_name"),
    }


# ── CrossRef ─────────────────────────────────────────────────────────────────

def crossref_by_doi(doi: str) -> dict | None:
    """Look up a DOI in CrossRef and return normalised metadata."""
    encoded = requests.utils.quote(doi, safe="")
    data = _get(f"{CROSSREF_BASE}/works/{encoded}")
    if not data or data.get("status") != "ok":
        return None
    return _parse_crossref(data["message"])


def crossref_by_pii(pii: str) -> dict | None:
    """
    Find a DOI using an Elsevier PII (e.g. 'S0899825615001335').
    Searches CrossRef filter alternative-id.
    """
    data = _get(f"{CROSSREF_BASE}/works",
                params={"filter": f"alternative-id:{pii}",
                        "rows": 1,
                        "mailto": CONTACT_EMAIL})
    if not data:
        return None
    items = data.get("message", {}).get("items", [])
    if not items:
        return None
    return _parse_crossref(items[0])


def crossref_by_title(title: str, journal: str = None) -> dict | None:
    """
    Search CrossRef by title. Optionally restrict by container-title.
    Returns the best match or None.
    """
    params = {
        "query.title": title,
        "rows": 3,
        "mailto": CONTACT_EMAIL,
    }
    if journal:
        readable = journal.replace("_", " ")
        params["query.container-title"] = readable

    data = _get(f"{CROSSREF_BASE}/works", params=params)
    if not data:
        return None
    items = data.get("message", {}).get("items", [])
    if not items:
        return None
    return _parse_crossref(items[0])


def _parse_crossref(msg: dict) -> dict:
    doi = msg.get("DOI")
    titles = msg.get("title", [])
    title = titles[0] if titles else None
    container = msg.get("container-title", [])
    journal = container[0] if container else None
    year = None
    issued = msg.get("issued", {}).get("date-parts", [[]])
    if issued and issued[0]:
        year = issued[0][0]
    return {
        "doi":        doi,
        "title":      title,
        "abstract":   msg.get("abstract"),
        "oa_pdf_url": None,   # CrossRef doesn't provide OA URLs
        "openalex_id": None,
        "pub_year":   year,
        "journal":    journal,
    }


# ── Unpaywall ────────────────────────────────────────────────────────────────

def unpaywall_oa_url(doi: str) -> str | None:
    """
    Return the best open-access PDF URL for a DOI via Unpaywall.
    Returns None if no OA version is available or DOI not found.
    """
    encoded = requests.utils.quote(doi, safe="/")
    data = _get(f"{UNPAYWALL_BASE}/{encoded}",
                params={"email": CONTACT_EMAIL})
    if not data or data.get("error"):
        return None
    best = data.get("best_oa_location") or {}
    return best.get("url_for_pdf") or best.get("url")


# ── High-level resolver ───────────────────────────────────────────────────────

def fetch_metadata(doi: str = None,
                   pii: str = None,
                   title_slug: str = None,
                   title: str = None,
                   journal: str = None) -> dict:
    """
    Master function: try multiple sources and return best result.

    Priority:
      1. OpenAlex by DOI      (returns oa_pdf_candidates list)
      2. Unpaywall supplement (appends its URL to candidates)
      3. CrossRef by DOI      (fallback for metadata)
      4. CrossRef by PII      (Elsevier papers)
      5. OpenAlex / CrossRef by title

    The returned dict always contains 'oa_pdf_candidates': list[str]
    so the caller can try each URL in order until a real PDF downloads.
    """
    result = {
        "doi": doi, "title": title, "abstract": None,
        "oa_pdf_url": None, "oa_pdf_candidates": [],
        "any_repo_fulltext": False,
        "openalex_id": None, "pub_year": None, "journal": None,
        "source": None,
    }

    def _add_candidates(urls):
        existing = set(result["oa_pdf_candidates"])
        for u in (urls or []):
            if u and u not in existing:
                result["oa_pdf_candidates"].append(u)
                existing.add(u)

    def _supplement_unpaywall(d):
        if not d:
            return
        up = unpaywall_oa_url(d)
        if up:
            _add_candidates([up])

    # --- 1. OpenAlex by DOI ---
    if doi:
        oa = openalex_by_doi(doi)
        if oa:
            result.update({k: v for k, v in oa.items() if v is not None})
            result["source"] = "openalex_doi"
            _supplement_unpaywall(doi)
            if result["oa_pdf_candidates"]:
                result["oa_pdf_url"] = result["oa_pdf_candidates"][0]
            return result

        # --- 3. CrossRef by DOI as fallback ---
        cr = crossref_by_doi(doi)
        if cr:
            result.update({k: v for k, v in cr.items() if v is not None})
            result["source"] = "crossref_doi"
            _supplement_unpaywall(doi)
            if result["oa_pdf_candidates"]:
                result["oa_pdf_url"] = result["oa_pdf_candidates"][0]
            return result

    # --- 4. CrossRef by PII ---
    if pii:
        cr = crossref_by_pii(pii)
        if cr and cr.get("doi"):
            resolved_doi = cr["doi"]
            result.update({k: v for k, v in cr.items() if v is not None})
            result["doi"] = resolved_doi
            result["source"] = "crossref_pii"
            # now try OpenAlex with the resolved DOI
            oa = openalex_by_doi(resolved_doi)
            if oa:
                result.update({k: v for k, v in oa.items() if v is not None})
                result["source"] = "crossref_pii+openalex"
            _supplement_unpaywall(resolved_doi)
            if result["oa_pdf_candidates"]:
                result["oa_pdf_url"] = result["oa_pdf_candidates"][0]
            return result

    # --- 5. Title-based lookup ---
    search_title = title or title_slug
    if search_title:
        oa = openalex_by_title(search_title, journal)
        if oa:
            result.update({k: v for k, v in oa.items() if v is not None})
            result["source"] = "openalex_title"
            if result.get("doi"):
                _supplement_unpaywall(result["doi"])
            if result["oa_pdf_candidates"]:
                result["oa_pdf_url"] = result["oa_pdf_candidates"][0]
            return result

        cr = crossref_by_title(search_title, journal)
        if cr:
            result.update({k: v for k, v in cr.items() if v is not None})
            result["source"] = "crossref_title"
            if result.get("doi"):
                _supplement_unpaywall(result["doi"])
            if result["oa_pdf_candidates"]:
                result["oa_pdf_url"] = result["oa_pdf_candidates"][0]
            return result

    return result  # empty shell — not found
