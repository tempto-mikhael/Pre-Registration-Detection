# Pre-Registration-Detection

This project helps identify research papers that appear to use empirical or experimental data, mention pre-registration, and link to a registry entry or pre-analysis plan. It is built as a set of small scripts that can be run independently or as a staged pipeline.

The repository currently supports two related workflows:

1. Spreadsheet-first workflow:
   Starts from a spreadsheet of papers, resolves metadata, downloads article PDFs when possible, runs keyword detection, and writes `output/results.csv`.
2. Folder-first workflow:
   Starts from a local folder full of PDFs, scans the papers directly, enriches likely hits with external link searches, and writes `output/pdf_scan_results.csv` plus follow-up outputs.

## How The Project Works

At a high level the code does six things:

1. It reconstructs or resolves article DOIs from filenames and APIs.
2. It fetches metadata from OpenAlex, CrossRef, and Unpaywall.
3. It extracts text from PDFs with PyMuPDF and sometimes `pdfminer.six`.
4. It looks for pre-registration phrases, registry names, and registry URLs.
5. It validates candidate links by comparing paper titles, DOI mentions, and author overlap.
6. It optionally asks an LLM to review ambiguous cases, then writes CSV and XLSX outputs.

## Repository Layout

- `scripts/`: all executable Python scripts.
- `.env.example`: example environment variables for Gemini and OpenRouter.
- `instructions_v1.txt`: original coding instructions that motivated the heuristics.
- `requirements.txt`: base Python dependencies.
- `output/`: generated CSV/XLSX/PDF artifacts. This folder is ignored by Git.

## Setup

```powershell
cd <repo-root>
python -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install --upgrade pip
pip install -r requirements.txt
```

Optional dependencies:

- Gemini support in `scripts/llm_verify.py` needs `google-genai`.
- OpenAI Batch submission itself is not performed by the repo, but `scripts/llm_batch_prepare.py` generates JSONL files for that workflow.

## Environment Variables

Copy the example file:

```powershell
Copy-Item .env.example .env
```

Supported keys:

- `GEMINI_API_KEY`: Gemini API key.
- `CHATGPT_API_KEY`:ChatGPT API key.
- `CLAUDE_API_KEY`: Claude API key.
- `OPENROUTER_API_KEY`: OpenRouter API key.
- Note: This supports further additions of different api keys. 

## Important Inputs

- A source spreadsheet:
  Used by the spreadsheet-first workflow. In practice this should contain paper-level metadata such as an identifier, title, source or venue name, and a document filename, plus any optional human-coded fields you want to compare against.
- Local PDF folders:
  Used by `scan_pdf_folder.py`. The folder-first workflow works with any set of PDFs that match the project's expected scanning assumptions.
- `output/oa_pdfs/`:
  Cache of downloaded open-access PDFs created by `pipeline.py`.

## Input Expectations

The code is not limited to one specific spreadsheet or one specific PDF collection.

- If you use the spreadsheet-first workflow, the workbook should provide enough information for DOI/title resolution and row-level output tracking.
- If you use the folder-first workflow, any PDF set can be used as long as the files are reachable locally and contain extractable text.
- Several scripts ship with default path constants or default filenames in the code. If your files use different names, either pass a CLI argument where supported or update the relevant constant in the script.

## Important Outputs

- `output/results.csv`: main spreadsheet-first pipeline output.
- `output/preregfind.csv`: enriched link search for `results.csv` detections.
- `output/prelabeled_verify.csv`: validation of already human-labeled preregistered papers.
- `output/pdf_scan_results.csv`: folder scan output.
- `output/pdf_scan_prereg_links.csv`: enrichment of folder-scan hits.
- `output/pdf_scan_prereg_links_dedup.csv`: expected deduplicated enrichment file used by later scripts.
- `output/llm_gemini_verdicts.csv`: LLM review results, even when OpenRouter is used.
- `output/comparison_report.csv`: side-by-side comparison of automated results and spreadsheet labels.
- `output/findings_*.xlsx` and `output/pipeline_findings_*.xlsx`: reporting workbooks.

## Suggested End-To-End Runs

Spreadsheet-first:

```powershell
.\.venv\Scripts\python.exe -u scripts\pipeline.py --delay 0.3
.\.venv\Scripts\python.exe -u scripts\find_prereg_links.py --delay 1.0
.\.venv\Scripts\python.exe -u scripts\verify_prelabeled.py --delay 0.5
.\.venv\Scripts\python.exe scripts\llm_verify.py --group all
.\.venv\Scripts\python.exe scripts\build_findings_xlsx.py
```

Folder-first:

```powershell
.\.venv\Scripts\python.exe scripts\scan_pdf_folder.py --folder "<path-to-pdf-root>"
.\.venv\Scripts\python.exe scripts\enrich_pdf_scan_links.py
.\.venv\Scripts\python.exe scripts\author_confirm_links.py
.\.venv\Scripts\python.exe scripts\no_title_ai_check.py
.\.venv\Scripts\python.exe scripts\llm_verify.py --group all
.\.venv\Scripts\python.exe scripts\build_pipeline_findings_xlsx.py
```

## Data Flow

Spreadsheet-first path:

1. `pipeline.py` reads the source spreadsheet, resolves DOI and metadata, downloads PDFs when possible, and runs keyword checks.
2. `find_prereg_links.py` revisits `auto_prereg=1` rows and searches external sources for registry links.
3. `verify_prelabeled.py` checks papers that humans already marked `prereg=1`.
4. `llm_verify.py` reviews ambiguous cases.
5. `build_findings_xlsx.py` merges original data with automated outputs.

Folder-first path:

1. `scan_pdf_folder.py` scans local PDFs directly.
2. `enrich_pdf_scan_links.py` tries to find and validate registry links for the detected papers.
3. `author_confirm_links.py` upgrades weak matches via author overlap.
4. `no_title_ai_check.py` uses an LLM when title-based validation is impossible.
5. `build_pipeline_findings_xlsx.py` creates a workbook based only on pipeline outputs.

## Script And Function Reference

### `scripts/api_client.py`

Purpose: central metadata resolver used by `pipeline.py`.

Functions:

- `_get(url, params=None, retry=MAX_RETRIES)`: shared JSON GET helper with retry and rate-limit handling.
- `_clean_abstract(inverted_index)`: rebuilds OpenAlex abstract text from an inverted-index structure.
- `openalex_by_doi(doi)`: fetches a work from OpenAlex by DOI.
- `openalex_by_title(title, journal=None)`: searches OpenAlex by title, optionally nudged by journal name.
- `_parse_openalex(data)`: normalizes OpenAlex records into the project's metadata schema.
- `crossref_by_doi(doi)`: fetches CrossRef metadata for a DOI.
- `crossref_by_pii(pii)`: resolves Elsevier-style PII values through CrossRef.
- `crossref_by_title(title, journal=None)`: title-based CrossRef lookup.
- `_parse_crossref(msg)`: normalizes CrossRef output into the project's metadata schema.
- `unpaywall_oa_url(doi)`: asks Unpaywall for the best OA URL for a DOI.
- `fetch_metadata(doi=None, pii=None, title_slug=None, title=None, journal=None)`: master resolver that combines OpenAlex, CrossRef, and Unpaywall and returns the best available metadata plus OA PDF candidates.

### `scripts/doi_resolver.py`

Purpose: reconstruct DOI-like identifiers from known journal filename patterns.

Functions:

- `resolve_doi(journal, pdf_filename)`: converts a journal slug and PDF filename into a DOI, PII marker, title slug marker, or `None`.
- `doi_to_url(doi)`: returns the canonical `https://doi.org/...` URL for a DOI.

### `scripts/pipeline.py`

Purpose: spreadsheet-first pipeline from a source workbook row to `output/results.csv`.

Functions:

- `is_pdf(content)`: checks whether returned bytes look like a PDF.
- `download_pdf(url, dest_path)`: downloads and caches a PDF candidate.
- `try_download_any(candidates, pdf_filename)`: tries candidate URLs in order until one yields a real PDF.
- `extract_text_from_pdf_bytes(pdf_bytes)`: extracts text from PDF bytes using PyMuPDF, then `pdfminer` as fallback.
- `_unique(lst)`: de-duplicates a list while preserving order.
- `scrape_landing_page_links(doi)`: scans a DOI landing page for registry URLs when text detection found prereg keywords but no direct link.
- `phrase_hit(text, phrases)`: case-insensitive substring matcher for clear phrases.
- `regex_hit(text, patterns)`: regex matcher for short or ambiguous terms.
- `extract_prereg_urls(text)`: extracts registry URLs or registry IDs from text and normalizes AEA IDs into URLs.
- `auto_check(text)`: runs the project's core rule-based checks for data presence, prereg mention, registry platform, direct link, and experiment type.
- `load_done_rows(csv_path)`: loads already-processed row numbers so the run can resume safely.
- `load_xlsx(path)`: opens the source workbook and builds a column-name lookup.
- `process_row(ws_row, name_to_col, row_num)`: processes one spreadsheet row end to end, including DOI resolution, metadata fetch, optional PDF download, text extraction, and keyword checks.
- `run(start_row=3, end_row=None, sample=None, delay=0.3, skip_prereg=False)`: top-level batch runner that iterates rows and appends results.

How it works:

1. Resolve DOI or a DOI-like fallback (`PII:` or `TITLE_SLUG:`).
2. Fetch metadata and OA PDF candidates.
3. Download PDF when possible and extract text.
4. Run `auto_check`.
5. If prereg is detected but no direct link is present, scan the article landing page.

### `scripts/scan_pdf_folder.py`

Purpose: folder-first PDF scanner that does not depend on the spreadsheet.

Functions:

- `is_generic_link(url)`: filters out registry homepages and other non-paper-specific URLs.
- `phrase_hit(text, phrases)`: substring phrase matcher.
- `regex_hit(text, patterns)`: regex matcher.
- `_strip_spaces(s)`: fixes digit spacing artifacts introduced by PDF extraction.
- `extract_prereg_urls(text)`: extracts registry links and normalized IDs from PDF text.
- `auto_check(text)`: folder-scan version of the main rule-based detector.
- `extract_text_from_pdf(pdf_path)`: extracts text from a PDF using both PyMuPDF and `pdfminer`, then combines them when possible.
- `get_journal_name(pdf_path, root)`: infers a source or venue label from the first folder below the scan root.
- `load_done_paths(csv_path)`: supports resumable scans by loading already-written PDF paths.
- `scan_folder(root, sample=None, prereg_only=False, output_csv=OUTPUT_CSV)`: recursively scans PDFs, runs extraction and heuristics, and writes `pdf_scan_results.csv`.

How it differs from `pipeline.py`:

- It starts from local PDF files instead of a spreadsheet row.
- It never needs DOI resolution for the first pass.
- It stores `pdf_path` as the main identifier.

### `scripts/find_prereg_links.py`

Purpose: enrich `output/results.csv` by searching for real registry links for `auto_prereg=1` papers.

Functions:

- `is_generic_link(url)`: rejects generic registry homepages and placeholder links.
- `unique(lst)`: de-duplicates while preserving order.
- `_strip_spaces(s)`: repairs digit spacing artifacts.
- `normalise(url)`: converts bare AEA or AsPredicted IDs into full URLs.
- `extract_links(text)`: scans any text blob for known registry patterns.
- `detect_voter_fp(text)`: flags likely false positives caused by voter-registration context.
- `has_strong_signal(text)`: checks for strong research-specific prereg signals.
- `triggered_keywords(text)`: reports which important keywords fired in the source text.
- `_osf_api_title(url)`: fetches OSF titles through the API to avoid JS-rendering problems.
- `_aspredicted_title(soup)`: extracts an AsPredicted title from page HTML.
- `_extract_registry_title(soup, url)`: extracts registry titles from AEA, AsPredicted, EGAP, or fallback HTML structures.
- `validate_link_quality(url, paper_title, paper_doi="")`: evaluates a candidate link using title similarity and DOI-in-page checks.
- `_normalize_name(name)`: strips accents and lowercases author surnames for matching.
- `crossref_authors_by_doi(doi)`: gets paper author family names from CrossRef.
- `crossref_authors_by_title(title)`: title-based author lookup when DOI lookup is unavailable.
- `_osf_api_contributors(url)`: gets OSF contributors via API.
- `author_overlap(paper_authors, registry_page_text, registry_url)`: measures whether paper authors also appear on the registry page.
- `check_cached_pdf(pdf_filename)`: rescans cached OA PDFs for embedded registry links.
- `check_crossref(doi)`: looks for registry links in CrossRef relation, link, and reference data.
- `check_semantic_scholar(doi, title)`: searches Semantic Scholar fields for registry URLs.
- `check_landing_page(doi)`: scans publisher landing pages, HTML, JSON-LD, and meta tags.
- `check_openalex(doi)`: scans the full OpenAlex metadata blob for registry patterns.
- `check_openalex_refs(doi)`: scans referenced works from OpenAlex for registry-related DOIs and URLs.
- `check_egap(title)`: title search against EGAP registrations.
- `check_aearctr_html(title)`: title search against the public AEA RCT HTML search page.
- `check_datacite(title)`: searches DataCite preregistration records.
- `check_osf_search(title)`: searches OSF registrations by title.
- `get_verdict(all_links, voter_fp, text)`: converts source evidence into a coarse final verdict string.
- `main()`: reads `results.csv`, runs all enrichment sources, validates the best link, optionally upgrades uncertain links using authors, and writes `output/preregfind.csv`.

How it works:

1. Start from papers already flagged `auto_prereg=1`.
2. Search cached PDF text, APIs, landing pages, and registry title searches.
3. Merge all found links.
4. Validate the best candidate by title and DOI.
5. Use author overlap for weak cases.

### `scripts/verify_prelabeled.py`

Purpose: validate papers that humans already coded as `prereg=1` in a source spreadsheet.

Functions:

- `extract_doi_from_filename(file_name)`: infers a DOI from the original spreadsheet `file_name` path when possible.
- `crossref_authors_by_doi(doi)`: fetches author surnames by DOI.
- `crossref_authors_by_title(title)`: title-based author lookup fallback.
- `_normalize_name(name)`: accent-stripping lowercase normalizer for author matching.
- `osf_api_contributors(url)`: gets OSF contributor surnames from the API.
- `author_overlap(paper_authors, registry_page_text, registry_url)`: checks whether registry page authors overlap with paper authors.
- `_aearctr_id_to_url(raw_id)`: normalizes an AEA trial number into a full URL.
- `_normalize_single(text)`: converts one raw link fragment or identifier into zero or more valid URLs.
- `split_and_normalize(cell_value)`: splits multi-link spreadsheet cells and normalizes every fragment.
- `osf_api_title(url)`: gets OSF titles via API.
- `aspredicted_title(soup)`: extracts an AsPredicted page title.
- `_extract_registry_title(soup, url)`: domain-aware registry title extraction.
- `doi_in_page_html(html_text, paper_doi)`: checks whether a paper DOI appears in registry page HTML.
- `validate_single_url(url, paper_title, paper_doi="", timeout=20)`: validates one registry URL and assigns a verdict.
- `validate_best(urls, paper_title, paper_doi, delay)`: tries multiple URLs and keeps the strongest result.
- `osf_title_search(title)`: searches OSF Registries by title for missing-link cases.
- `discover_links(title, delay)`: branch-B fallback that searches AEA, EGAP, and OSF when the spreadsheet has no stored link.
- `load_results_doi_map()`: loads DOI and year information from `results.csv` keyed by spreadsheet ID.
- `main()`: reads `prereg=1` spreadsheet rows, validates existing links or rediscovers missing ones, and writes `output/prelabeled_verify.csv`.

### `scripts/enrich_pdf_scan_links.py`

Purpose: enrich folder-scan detections with external link searches.

Functions:

- `parse_existing_links(raw)`: splits semicolon-separated links from the scan CSV.
- `clean_doi(value)`: strips DOI prefixes and punctuation.
- `extract_pdf_text_and_meta(pdf_path, max_pages=8)`: extracts an early PDF text snippet plus a best-effort title guess.
- `extract_doi_from_text(text)`: extracts the first DOI-like match from snippet text.
- `load_done_pdf_paths(csv_path)`: supports resumable enrichment runs.
- `main()`: reads `pdf_scan_results.csv` or `pdf_scan_results_v2.csv`, enriches `auto_prereg=1` rows with external searches, validates the best link, and writes `pdf_scan_prereg_links.csv`.

This script reuses many functions from `find_prereg_links.py` rather than reimplementing them.

### `scripts/author_confirm_links.py`

Purpose: upgrade weak link matches using author overlap.

Functions:

- `main()`: loads `pdf_scan_prereg_links_dedup.csv`, re-checks candidate rows with weak link quality, computes author overlap, upgrades strong matches to `AUTHOR_CONFIRMED`, and writes the CSV back in place.

### `scripts/find_title_mismatches.py`

Purpose: find false negatives where the publication title differs from the preregistration title.

Functions:

- `normalise_title(t)`: lowercases and strips punctuation and stopwords for title comparison.
- `similarity(a, b)`: computes normalized title similarity.
- `crossref_authors(doi)`: gets lowercase author surnames from CrossRef.
- `author_overlap_score(paper_authors, registry_text)`: reports how many paper authors appear in registry text.
- `search_aearctr(title)`: searches the AEA RCT registry and returns structured hits.
- `search_egap(title)`: searches EGAP and returns structured hits.
- `search_osf_prereg(title)`: searches OSF registrations and returns structured hits.
- `load_already_done()`: supports resumable output generation.
- `main()`: searches undetected papers for registry entries with similar titles and writes `output/title_mismatch_candidates.csv`.

### `scripts/compare_results.py`

Purpose: compare folder-scan tiers against reference spreadsheet labels.

Functions:

- `load_xlsx(path)`: loads ground-truth prereg and link fields from a reference spreadsheet.
- `load_scan_all(path)`: loads the full scan CSV keyed by filename.
- `load_enriched_all(path)`: loads the enrichment CSV keyed by filename.
- `build_comparison(scan, enriched, xlsx)`: creates one merged row per paper and computes agreement categories.
- `main()`: prints tier summaries, computes precision/recall-style summaries, and writes `output/comparison_report.csv`.

### `scripts/build_findings_xlsx.py`

Purpose: merge a source workbook with scan, link, and LLM outputs.

Functions:

- `load_csv_by_filename(path, fname_col="filename")`: generic CSV loader keyed by filename.
- `load_scan(path)`: convenience wrapper for the scan CSV.
- `load_best_links(path)`: legacy helper for a single best-link-per-file mapping.
- `is_type_empty(row_values, type_idxs)`: checks whether all original `type_*` columns are empty or zero.
- `to_bool_or_none(val)`: turns string booleans into Python booleans or `None`.
- `main()`: loads the source workbook and pipeline outputs, appends new pipeline columns, styles the workbook, and writes `output/findings_pipeline_<date>.xlsx`.

### `scripts/build_pipeline_findings_xlsx.py`

Purpose: build a workbook from pipeline outputs only, with final decision columns derived from automated evidence.

Functions:

- `load_csv(path, key_col="filename")`: generic CSV loader keyed by a chosen column.
- `load_identifiers(xlsx_path)`: loads only paper identifiers and titles from a reference spreadsheet.
- `int_or_none(val)`: safe integer coercion.
- `to_bool_or_none(val)`: safe boolean coercion from string values.
- `extract_evidence_location(evidence, reasoning)`: infers where in the paper the LLM seems to have found prereg evidence.
- `main()`: merges scan, link, AI, and identifier data into `output/pipeline_findings_<date>.xlsx`.

### `scripts/llm_batch_prepare.py`

Purpose: create JSONL request files for a batch LLM workflow.

Functions:

- `load_env_file(env_path)`: loads `.env` keys into the process environment without overwriting existing values.
- `load_xlsx()`: loads reference prereg labels and source names keyed by filename.
- `load_scan()`: loads the folder scan CSV.
- `load_enriched()`: loads the enrichment CSV.
- `extract_text(pdf_path, max_chars)`: extracts PDF text and truncates it to a budget.
- `build_group_a(scan, enriched)`: builds papers with keyword hits but no link evidence.
- `build_group_b(scan, xlsx)`: builds papers humans marked preregistered that the scanner missed.
- `build_group_c(scan, enriched, xlsx)`: builds disputed cases where link evidence exists but spreadsheet prereg is zero.
- `make_request(paper, model, max_chars)`: builds one Chat Completions style batch request object.
- `write_jsonl(papers, output_path, model, max_chars)`: writes request objects to a JSONL file.
- `write_manifest(all_papers, manifest_path)`: writes a CSV manifest of all prepared papers.
- `main()`: loads data, builds the selected groups, estimates cost, writes JSONL files, and prints next-step instructions.

### `scripts/llm_batch_parse_results.py`

Purpose: parse returned batch JSONL result files into a CSV.

Functions:

- `parse_result_files(paths)`: reads batch result JSONL files and extracts each model verdict.
- `write_csv(results, path)`: writes parsed verdicts to CSV.
- `print_summary(results)`: prints grouped counts and quick takeaways.
- `main()`: CLI entry point that parses files and writes `output/llm_verdicts.csv`.

### `scripts/llm_verify.py`

Purpose: live LLM verification for ambiguous cases using Gemini or OpenRouter.

Core data and prompt helpers:

- `load_env_file(env_path)`: loads `.env` values.
- `build_paper_section(paper, index, text)`: builds one paper section for a multi-paper batch prompt.
- `load_xlsx()`: loads spreadsheet labels keyed by filename.
- `load_scan()`: loads scan output keyed by filename.
- `load_enriched()`: loads enrichment output keyed by filename.
- `load_done()`: loads already-processed filenames from `llm_gemini_verdicts.csv`.
- `extract_text(pdf_path, max_chars)`: extracts full text or a head-plus-tail sample for LLM review.
- `build_groups(scan, enriched, xlsx, requested_groups)`: builds groups A, B, C, and D.

Provider helpers:

- `RateLimitError`: custom error used to distinguish rate-limit failures from other failures.
- `is_rate_limited_message(message)`: checks whether an error string looks like a rate limit.
- `is_no_endpoint_message(message)`: checks whether an OpenRouter error means a model endpoint disappeared.
- `build_single_prompt(paper, max_chars, _preextracted=None)`: builds the correct prompt for one paper.
- `discover_openrouter_free_models(api_key, limit=16)`: discovers free OpenRouter models and merges them with defaults.
- `extract_openrouter_text(response_json)`: extracts assistant text from OpenRouter chat output.
- `clean_llm_content(content)`: removes `<think>` blocks and markdown fences from model output.
- `openrouter_chat_completion(api_key, model, prompt, max_output_tokens, response_format_type=None)`: low-level OpenRouter request wrapper.

Gemini execution:

- `call_gemini(client, paper, max_chars)`: sends one paper to Gemini and parses a verdict.
- `call_gemini_batch(client, papers, max_chars)`: sends multiple papers in one Gemini request and splits the results back out.

OpenRouter execution:

- `_make_error_result(paper, reason)`: returns a standardized error result.
- `call_openrouter_batch_once(api_key, model, papers, max_chars)`: one batch attempt against one OpenRouter model.
- `call_openrouter_single_once(api_key, model, paper, max_chars)`: one single-paper attempt against one model.
- `call_openrouter_single_with_fallback(api_key, model_pool, paper, max_chars)`: rotates through models until one works for a single paper.
- `call_openrouter_batch_with_fallback(api_key, model_pool, papers, max_chars)`: rotates through models until a batch succeeds and falls back to single-paper recovery for missing items.

Rate limiting and output:

- `TokenBucket`: simple token-per-minute limiter used to avoid provider limits.
- `append_result(result)`: appends one result row to the results CSV.
- `print_summary()`: prints a grouped summary of accumulated LLM results.
- `main()`: parses CLI options, loads data, builds groups, sends batches, and writes `output/llm_gemini_verdicts.csv`.

Group meanings:

- Group A: keyword hit, no link found.
- Group B: the reference spreadsheet says preregistered, scanner missed it.
- Group C: link evidence exists, the reference spreadsheet says not preregistered.
- Group D: original search hit but the later PyMuPDF re-scan missed the keywords.

### `scripts/retry_errors.py`

Purpose: retry only LLM rows that previously ended with `llm_confidence=error`.

Functions:

- `main()`: loads error rows from `llm_gemini_verdicts.csv`, rebuilds paper metadata, retries those papers through OpenRouter with shorter waits, patches successful rows in place, and rewrites the CSV.

### `scripts/no_title_ai_check.py`

Purpose: let an LLM judge link ownership when title-based link validation failed.

Functions:

- `extract_text(pdf_path, max_chars=MAX_CHARS)`: extracts full paper text or a head-plus-tail sample.
- `_parse_verdict(content)`: robustly parses malformed or partial LLM output into a yes/no verdict.
- `call_verify(api_key, model, filename, links, pdf_text)`: builds the prompt and reuses `llm_verify.py` OpenRouter logic.
- `main()`: reads `pdf_scan_prereg_links_dedup.csv`, targets `NO_TITLE` or previous AI-rejected rows, rotates through OpenRouter models, updates AI-related columns in place, and saves the CSV.

### `scripts/show_results.py`

Purpose: quick console-only inspection of `results.csv`.

Notes:

- This is a legacy utility with a hard-coded absolute path from an earlier development setup.
- It is not resumable and is not part of the main pipeline.

Behavior:

- Loads `results.csv`, prints per-row TP/TN/FP/FN style status against spreadsheet labels, then prints a summary.

### `scripts/clean_results.py`

Purpose: remove empty rows from `results.csv`.

Notes:

- This is another legacy utility with a hard-coded absolute path from an earlier development setup.
- It makes a `.bak` backup and rewrites the CSV.

Behavior:

- Filters out rows with no journal, DOI, or PDF filename.

## Practical Notes

- `find_prereg_links.py`, `enrich_pdf_scan_links.py`, `compare_results.py`, `author_confirm_links.py`, `llm_verify.py`, and `build_pipeline_findings_xlsx.py` all expect `output/pdf_scan_prereg_links_dedup.csv` to exist, but the dedup script itself is not currently present in this repo.
- `build_pipeline_findings_xlsx.py` falls back from `pdf_scan_results_v2.csv` to `pdf_scan_results.csv` if needed.
- `show_results.py` and `clean_results.py` still point to old absolute paths and should be edited before use on this machine.
- `.gitignore` excludes `output/`, `.env`, `*.pdf`, and `*.xlsx`, so raw data and generated outputs are intentionally not pushed.
- Some scripts still contain default filenames or path constants that reflect the original development setup. Those defaults can be replaced with any equivalent workbook or PDF collection that matches the expected structure.

## How To Push Only The Current Content To GitHub

If your local repo is already linked to a remote, run these commands from the repository root:

```powershell
git status
git add -A
git status
git commit -m "Update project documentation and current scripts"
git push origin HEAD
```

Why `git add -A` matters:

- It stages modified files.
- It stages new files.
- It stages deletions.
- It still does not add ignored files like `.env`, `output/`, PDFs, or XLSX files.

If you want one extra safety check before committing, use:

```powershell
git diff --cached --stat
git diff --cached
```

If Git says there is nothing to commit, that means your local branch already matches the last commit.

If this is the first time pushing this local branch name, use:

```powershell
git push -u origin HEAD
```

If you want to push only part of the current work instead of everything visible in `git status`, do not use `git add -A`. Stage specific files instead, for example:

```powershell
git add README.md scripts\find_prereg_links.py
git commit -m "Update README and link search logic"
git push origin HEAD
```

## Minimal Safe Push Checklist

1. Run `git status` and confirm the file list is what you expect.
2. Run `git add -A` if you want the full current repo state.
3. Run `git diff --cached --stat` to confirm what will be committed.
4. Commit once.
5. Push to your linked remote branch.
