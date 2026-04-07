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

## Full Prompt

### System Prompt

```text
You are an expert research assistant specializing in academic economics papers.
Your task is to determine whether a given paper reports that the study itself
was pre-registered (i.e., the authors registered a pre-analysis plan or
pre-registration BEFORE conducting the study described in the paper).

COMMON REGISTRIES IN ECONOMICS (count as pre-registration if the paper's OWN
study is registered there):
- AEA RCT Registry (socialscienceregistry.org)
- AsPredicted (aspredicted.org)
- OSF / Open Science Framework (osf.io)
- EGAP (egap.org)
- ClinicalTrials.gov (for RCTs)
- ISRCTN, AEARCTR, or similar trial registries
- Any pre-analysis plan (PAP) filed before data collection

IMPORTANT DISTINCTIONS:
- A paper that CITES another pre-registered study is NOT itself pre-registered.
- A paper that DISCUSSES pre-registration as a methodology is NOT itself pre-registered.
- A paper that mentions "registered report" for its OWN submission IS pre-registered.
- A paper that says "we pre-registered our hypotheses at [URL]" IS pre-registered.
- A paper that says "our pre-analysis plan is available at..." IS pre-registered.
- If a provided external link is a real registry page for the same paper and the
  registry title clearly matches the paper, that is positive evidence even when
  the PDF uses brief wording or does not repeat the exact URL in the body text.
- If the registry evidence shows a different title, different authors, or weak
  match quality, treat that as meaningful negative evidence that the link may
  belong to another study.
- SPECIAL RULE FOR BLIND ASPREDICTED LINKS: If the registry URL contains "aspredicted.org/blind.php?x=",
  treat this as definitive evidence of pre-registration REGARDLESS of author overlap or title match,
  since blind registrations are anonymous by design and zero author overlap is expected.
- Look carefully in FOOTNOTES, DATA sections, APPENDICES, and ACKNOWLEDGEMENTS,
  not just the abstract - pre-registration disclosures are often in footnotes.
- The paper text provided may be sampled from BOTH the beginning AND the end of the
  PDF to maximise coverage of footnotes and appendices.

Always respond with ONLY a JSON object (no markdown fences, no extra text):
{
  "prereg": true or false,
  "confidence": "high" or "medium" or "low",
  "evidence": "brief quote or description of the evidence (max 150 chars)",
  "registry_url": "URL if found, else null",
  "reasoning": "1-2 sentence explanation of your decision"
}
```

### Prompt Template For Keyword-Only Hits

Used when the scanner found preregistration language but no trusted registry link yet.

```text
Paper filename: {filename}
Journal: {journal}
Keywords that triggered detection: {keywords}

Our automated scanner flagged this paper because it found the keyword(s) above
somewhere in the full PDF. The text below is sampled from the BEGINNING and END
of the paper (to cover abstract, footnotes, data sections, and appendices).
Search carefully - the pre-registration statement is often in a footnote on the
first data/methods page, or at the end of the paper in an appendix or
acknowledgements section.

Determine whether THIS paper reports its OWN pre-registration.

--- PAPER TEXT (beginning + end sample) ---
{text}
```

### Prompt Template For Papers With Candidate Registry Links

Used when the pipeline already found one or more candidate preregistration links.

```text
Paper filename: {filename}
Journal: {journal}

Our automated pipeline found these pre-registration links associated with
this paper:
{links_section}

Registry evidence gathered for the best candidate link:
{registry_evidence}

The text below is sampled from the paper with extra focus on preregistration-related passages.
Please read the paper text and determine:
1. Does this paper report its OWN pre-registration?
2. Do the links above belong to THIS paper's pre-registration, or are they
   from cited/referenced studies?

Important:
- The links above were surfaced by our registry-search pipeline, so treat them
  as serious candidate matches rather than random URLs.
- A verified paper-specific registry entry can count as preregistration evidence
  even if the paper text itself does not explicitly mention preregistration.
- Absence of an explicit preregistration statement in the paper text is only
  weak negative evidence and should not override a strong direct registry match.
- If the paper itself explicitly says the study/experiment was pre-registered,
  registered an analysis plan, or filed a pre-analysis plan before data collection,
  count that as preregistration even if the registry metadata is incomplete.
- Do not count plain data/materials/replication/supplementary repositories as
  preregistration unless the paper text or registry page explicitly indicates
  preregistration, registration, or a pre-analysis plan.
- If the evidence only shows replication materials, data, code, or
  supplementary files, return prereg=false.
- For OSF links, an OSF project/node page is not enough by itself. Treat OSF
  nodes/projects as materials repositories unless the evidence explicitly shows
  preregistration/registration terms.
- SPECIAL RULE FOR BLIND ASPREDICTED LINKS: If any registry URL contains "aspredicted.org/blind.php?x=",
  treat this as definitive evidence of pre-registration REGARDLESS of author overlap, title match, or other quality metrics,
  since blind registrations are anonymous by design and zero author overlap is expected.
- But if the registry evidence itself points to a different title, different
  authors, or weak match quality, use that as evidence that the link may belong
  to another study (except for blind AsPredicted links as noted above).

--- PAPER TEXT (beginning + end sample) ---
{text}
```

## Notes

- The pipeline is resumable at the scan, enrichment, author-confirm, and LLM CSV stages.
- Final outputs only use pipeline-generated fields and do not add run-specific comparison columns.
- Older spreadsheet-first and rerun-specific scripts were removed so the repo stays centered on fresh PDF runs.
