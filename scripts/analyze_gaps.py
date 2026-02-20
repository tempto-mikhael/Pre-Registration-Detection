"""
Analyze gaps in find_prereg_links.py and recommend new methods.
"""
import csv, re
from pathlib import Path
from collections import Counter

PDF_DIR    = Path(r"d:\Projects\ercautomation\output\oa_pdfs")
RESULTS    = Path(r"d:\Projects\ercautomation\output\results.csv")
PREREGFIND = Path(r"d:\Projects\ercautomation\output\preregfind.csv")

with open(RESULTS, encoding="utf-8") as f:
    results = {r["row_num"]: r for r in csv.DictReader(f)}

with open(PREREGFIND, encoding="utf-8") as f:
    finds = list(csv.DictReader(f))

cached_pdfs = list(PDF_DIR.glob("*.pdf"))
print(f"Cached PDFs in output/oa_pdfs/ : {len(cached_pdfs)}")

possible = [r for r in finds if "POSSIBLE" in r["verdict"]]
full_pdf_rows = [r for r in possible if r["text_source"] == "full_pdf"]

print(f"POSSIBLE rows (no link found)   : {len(possible)}")
print(f"  of which text_source=full_pdf : {len(full_pdf_rows)}")
print()

# For full_pdf POSSIBLE rows, check what PDF file was used
print("=== full_pdf POSSIBLE rows — checking cached PDF filenames ===")
for r in full_pdf_rows[:5]:
    res = results.get(r["row_num"], {})
    pdf_fn = res.get("pdf_filename","") or ""
    oa_url = res.get("oa_pdf_url","") or ""
    safe   = re.sub(r"[^\w]", "_", pdf_fn) + ".pdf"
    exists = (PDF_DIR / safe).exists()
    print(f"  row {r['row_num']:>5} | pdf={pdf_fn[:40]:<40} | cached={exists} | oa_url={oa_url[:50]}")

print()
print("=== Summary: what new methods could help ===")
print("""
GAP 1 — BIGGEST: Full PDF text not scanned for registry URLs in find_prereg_links.py
  - The pipeline extracts text and stores it in oa_pdfs/ — but find_prereg_links.py
    only looks at the abstract column. Re-running extract_links() on the cached
    PDF text would immediately find URLs in footnotes/data sections.
  - Affected: 27 of 29 POSSIBLE rows (all text_source=full_pdf)
  - Effort: LOW — just load cached PDF and run existing extract_links()

GAP 2: OpenAlex referenced_works — scan *reference list* for registry DOIs
  - Pre-reg DOIs often have the form 10.31222/osf.io/xxxxx or 10.17605/osf.io/xxxxx
  - OpenAlex referenced_works field lists all citations as work IDs with DOIs
  - Scanning these for osf.io/*, aspredicted, socialscienceregistry DOIs would
    catch cases where the paper formally cites its pre-reg as a reference
  - Affected: all 29 POSSIBLE rows (all have real DOIs)
  - Effort: LOW — one extra API call per paper

GAP 3: CrossRef references field — same as above but via CrossRef
  - CrossRef work objects sometimes include a references[] array with raw citation text
  - Raw citation text like "pre-registered at osf.io/abc123" would be scannable
  - Requires CrossRef Metadata Plus for full refs, but free tier sometimes returns them
  - Effort: LOW — add one scan in check_crossref()

GAP 4: EGAP registry direct title/DOI search
  - egap.org/research-designs/ has a search page not previously tried
  - Can try GET https://egap.org/registration/?s=<title>
  - Relevant for development economics papers (3 POSSIBLE in JDE)
  - Effort: LOW

GAP 5: AEA RCT Registry — try trial detail page by DOI
  - https://www.socialscienceregistry.org/trials?search=<doi_or_title> (HTML, no auth)
  - Our previous attempt hit the JSON API which requires auth
  - The HTML search page is publicly accessible
  - Relevant: some JDE / AEA papers may be on this registry
  - Effort: MEDIUM — need to parse HTML search results

GAP 6: DataCite search — pre-regs as citable objects
  - Pre-registrations filed on OSF/Zenodo get DataCite DOIs
  - Can search: https://api.datacite.org/works?query=<title>&resource-type-id=preregistration
  - Effort: LOW — clean REST API

GAP 7: Supplement/appendix PDFs already downloaded
  - Some OA URLs resolved to *online appendix* PDFs rather than main paper
  - The pre-reg info might be in the appendix — we could scan those too
  - Effort: LOW — reuse cached PDFs
""")
