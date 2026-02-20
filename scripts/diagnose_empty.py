import csv, openpyxl
from pathlib import Path

CSV  = Path(r"d:\Projects\ercautomation\output\results.csv")
XLSX = Path(r"d:\Projects\ercautomation\journal_articles_with_pap_2025-03-14.xlsx")

with open(CSV, newline="", encoding="utf-8") as f:
    rows = list(csv.DictReader(f))

empty   = [r for r in rows if not r.get("journal") and not r.get("doi") and r.get("text_source") == "none"]
normal  = [r for r in rows if r.get("journal") or r.get("doi")]

print(f"Total rows      : {len(rows)}")
print(f"Normal rows     : {len(normal)}")
print(f"Empty rows      : {len(empty)}")
print()

if empty:
    nums = [int(r["row_num"]) for r in empty]
    print(f"Empty row_num range : {min(nums)} – {max(nums)}")
    print(f"First 5 row_nums    : {nums[:5]}")
    print(f"Last  5 row_nums    : {nums[-5:]}")
    print()

    # Cross-reference with xlsx to see what journals these are
    wb = openpyxl.load_workbook(str(XLSX), read_only=True)
    ws = wb.active
    # Row 2 = headers
    headers = [ws.cell(2, c).value for c in range(1, ws.max_column + 1)]
    journal_col = next((i+1 for i, h in enumerate(headers) if h and "journal" in str(h).lower()), None)
    file_col    = next((i+1 for i, h in enumerate(headers) if h and h == "file_name"), None)

    print("Sample empty rows from xlsx:")
    for rn in nums[:15]:
        journal = ws.cell(rn, journal_col).value if journal_col else "?"
        fname   = ws.cell(rn, file_col).value if file_col else "?"
        print(f"  xlsx row {rn:>5} | journal={journal} | file={fname}")
    wb.close()
