import csv

CSV = r"d:\Projects\ercautomation\output\results.csv"

with open(CSV, newline="", encoding="utf-8") as f:
    rows = list(csv.DictReader(f))

total = len(rows)
hits  = [r for r in rows if str(r.get("auto_prereg", "")).strip() == "1"]
tp    = [r for r in hits if str(r.get("xlsx_prereg", "")).strip() == "1"]
new   = [r for r in hits if str(r.get("xlsx_prereg", "")).strip() != "1"]

print(f"Total rows processed : {total}")
print(f"auto_prereg=1 total  : {len(hits)}")
print(f"  confirmed (xlsx=1) : {len(tp)}")
print(f"  NEW finds (xlsx!=1): {len(new)}")

def show_hit(r, label=""):
    title = (r.get("title_fetched") or r.get("title_xlsx") or "")[:80]
    url   = r.get("auto_link_prereg", "").strip() or "(no URL extracted)"
    print(f"  row {r['row_num']:>5} | {r['journal']:<35} | src={r['text_source']:<12} | {title}")
    print(f"         pre-reg URL : {url}")
    print()

if new:
    print("\n--- NEW detections (auto_prereg=1 but xlsx_prereg!=1) ---")
    for r in new:
        show_hit(r)
else:
    print("\nNo new prereg detections beyond what is already coded in the xlsx.")

if tp:
    print(f"\n--- CONFIRMED (auto caught existing xlsx_prereg=1 rows) ---")
    for r in tp:
        show_hit(r)
