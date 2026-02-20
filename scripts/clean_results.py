"""Remove empty rows (no journal and no doi) from results.csv."""
import csv, shutil
from pathlib import Path

CSV = Path(r"d:\Projects\ercautomation\output\results.csv")
BAK = CSV.with_suffix(".csv.bak")

with open(CSV, newline="", encoding="utf-8") as f:
    rows = list(csv.DictReader(f))
    fieldnames = csv.DictReader(open(CSV, encoding="utf-8")).fieldnames

before = len(rows)
clean  = [r for r in rows if r.get("journal") or r.get("doi") or r.get("pdf_filename")]
after  = len(clean)

shutil.copy(CSV, BAK)

with open(CSV, "w", newline="", encoding="utf-8") as f:
    writer = csv.DictWriter(f, fieldnames=fieldnames)
    writer.writeheader()
    writer.writerows(clean)

print(f"Removed {before - after} empty rows ({before} → {after})")
print(f"Backup saved to {BAK}")
