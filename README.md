# ERC Automation

Scripts for detecting pre-registration mentions and discovering/validating registry links.

## What’s in Git vs not
This repo intentionally does **not** track large/local data artifacts:
- The xlsx dataset (e.g. `journal_articles_with_pap_2025-03-14.xlsx`)
- Downloaded PDFs
- Generated outputs in `output/` (CSVs, logs, report PDF)

Those files are ignored via `.gitignore` so you can safely push code to GitHub.

## Setup (Windows)
```powershell
cd D:\Projects\ercautomation
python -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install --upgrade pip
pip install -r requirements.txt
```

## Data you must provide on the target PC
Copy these onto the target machine (outside git), in the same project root structure:
- `journal_articles_with_pap_2025-03-14.xlsx`
- Your PDFs (from Dropbox), if you want full-text scanning

If your PDFs are in a Dropbox-synced folder, you can keep them there and point scripts at that folder (if/when you add a CLI option for it), or copy them under `output/oa_pdfs/`.

## Common runs
```powershell
# Main pipeline (writes output/results.csv)
$env:PYTHONIOENCODING = "utf-8"
$env:PYTHONUNBUFFERED = "1"
.\.venv\Scripts\python.exe -u scripts\pipeline.py --delay 0.3

# Enrichment pass to find prereg links for auto_prereg=1 rows
.\.venv\Scripts\python.exe -u scripts\find_prereg_links.py --delay 1.0

# Verify pre-labeled prereg=1 rows
.\.venv\Scripts\python.exe -u scripts\verify_prelabeled.py --delay 0.5

# Build PDF report
.\.venv\Scripts\python.exe scripts\generate_report.py
```

## Publish to GitHub
From the project root:
```powershell
git init
git add .
git commit -m "Initial commit"

# Create a new empty repo on GitHub, then:
git branch -M main
git remote add origin https://github.com/<your-username>/<repo-name>.git
git push -u origin main
```

Notes:
- If Git warns about large files, double-check `git status` before committing.
- If you *do* want to version PDFs/XLSX (not recommended), remove the relevant entries from `.gitignore` and consider Git LFS.
