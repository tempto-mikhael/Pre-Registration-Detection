# Pre-Registration-Detection

This repository now uses a single PDF-first pipeline: scan local PDFs, enrich likely registry links, review ambiguous cases with deterministic rules plus an LLM, and write final `results.csv` and `results.xlsx`.

## Setup

```powershell
cd <repo-root>\Pre-Registration-Detection
python -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install --upgrade pip
pip install -r requirements.txt
```

Optional:

- Gemini support in `scripts/llm_verify.py` needs `google-genai`.

## Environment Variables

Copy the example file:

```powershell
Copy-Item .env.example .env
```

Supported keys:

- `GEMINI_API_KEY`
- `OPENROUTER_API_KEY`
- `CHATGPT_API_KEY`
- `CLAUDE_API_KEY`

The main pipeline currently uses Gemini or OpenRouter.

## One-Command Run

```powershell
.\.venv\Scripts\python.exe scripts\run_pdf_pipeline.py --folder "<path-to-pdf-root>"
```

OpenRouter example:

```powershell
.\.venv\Scripts\python.exe scripts\run_pdf_pipeline.py `
  --folder "<path-to-pdf-root>" `
  --provider openrouter `
  --openrouter-models auto
```

Useful options:

- `--reset-llm`
- `--max-chars 40000`
- `--scan-sample N`
- `--enrich-sample N`
- `--prereg-only`
- `--log output/pipeline_run.log`

## Manual Steps

```powershell
.\.venv\Scripts\python.exe scripts\scan_pdf_folder.py --folder "<path-to-pdf-root>"
.\.venv\Scripts\python.exe scripts\enrich_pdf_scan_links.py
.\.venv\Scripts\python.exe scripts\dedup_pdf_scan_prereg_links.py
.\.venv\Scripts\python.exe scripts\author_confirm_links.py
.\.venv\Scripts\python.exe scripts\llm_verify.py --group all
.\.venv\Scripts\python.exe scripts\build_pipeline_findings_xlsx.py
```

## Outputs

- `output/pdf_scan_results.csv`
- `output/pdf_scan_prereg_links.csv`
- `output/pdf_scan_prereg_links_dedup.csv`
- `output/llm_verdicts.csv`
- `output/results.csv`
- `output/results.xlsx`
- `output/pipeline_run.log`

## Remaining Scripts

- `scripts/run_pdf_pipeline.py`: one-command runner for the full workflow.
- `scripts/scan_pdf_folder.py`: PDF scan and first-pass prereg detection.
- `scripts/enrich_pdf_scan_links.py`: registry-link search and validation.
- `scripts/dedup_pdf_scan_prereg_links.py`: one best row per paper.
- `scripts/author_confirm_links.py`: author-overlap upgrade for weak but plausible links.
- `scripts/llm_verify.py`: deterministic + LLM review for ambiguous papers.
- `scripts/build_pipeline_findings_xlsx.py`: final `results.csv` / `results.xlsx` builder.
- `scripts/find_prereg_links.py`: shared registry-link utilities.
- `scripts/path_utils.py`: shared path helpers.

## Prompt Notes

The `llm_verify.py` prompt is short but structured to prevent the main failure modes:

- It tells the model to separate a paper's own preregistration from citations to other preregistered studies.
- It explicitly treats blind `aspredicted.org/blind.php?x=...` links as positive evidence, which prevents blind-link false negatives.
- It pairs the prompt with deterministic rules for direct paper text such as `we preregistered` or `pre-analysis plan`.
- It keeps OSF-style repository links more cautious, so materials pages are not over-counted as preregistration.

## Notes

- The pipeline is resumable at the scan, enrichment, author-confirm, and LLM CSV stages.
- Final outputs only use pipeline-generated fields and do not add run-specific comparison columns.
- Older spreadsheet-first and rerun-specific scripts were removed so the repo stays centered on fresh PDF runs.
