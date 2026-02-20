import csv

with open('d:/Projects/ercautomation/output/results.csv', newline='', encoding='utf-8') as f:
    rows = list(csv.DictReader(f))

print(f"{'Row':<5} {'DOI':<30} {'DL':<3} {'src':<10} | {'auto_pre':<9} {'xlsx_pre':<9} | {'FP/FN/OK':<8} | {'nodata':<8}")
print('-' * 100)
for r in rows:
    dl   = r['oa_pdf_downloaded']
    src  = r.get('text_source', '?')
    a_pr = r['auto_prereg']
    x_pr = r['xlsx_prereg']
    if a_pr == '1' and x_pr == '1': verdict = 'TP'
    elif a_pr == '0' and x_pr == '0': verdict = 'TN'
    elif a_pr == '1' and x_pr == '0': verdict = 'FP !!'
    else: verdict = 'FN !!'
    doi_short = (r['doi'] or '')[:29]
    print(f"{r['row_num']:<5} {doi_short:<30} {dl:<3} {src:<10} | {a_pr:<9} {x_pr:<9} | {verdict:<8} | {r['auto_no_data']}")

# Summary
from collections import Counter
verdicts = []
for r in rows:
    a, x = r['auto_prereg'], r['xlsx_prereg']
    if a=='1' and x=='1': verdicts.append('TP')
    elif a=='0' and x=='0': verdicts.append('TN')
    elif a=='1' and x=='0': verdicts.append('FP')
    else: verdicts.append('FN')
c = Counter(verdicts)
print()
print(f"Summary: TP={c['TP']} TN={c['TN']} FP={c['FP']} FN={c['FN']}")
dl_count = sum(1 for r in rows if r['oa_pdf_downloaded']=='1')
print(f"PDFs downloaded: {dl_count}/{len(rows)}")
