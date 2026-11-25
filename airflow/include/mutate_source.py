import csv, random
from pathlib import Path
p = Path(__file__).parent / "data/home_depot_data_1_2021_12.csv"
rows = list(csv.DictReader(p.open(newline="")))
picked = random.sample(range(len(rows)), k=len(rows) // 20)
for i in picked:
    r = rows[i]
    if random.random() < 0.7:
        r["price"] = f"{float(r['price']) * random.uniform(0.8, 1.2):.2f}"
    else:
        r["title"] += " (updated)"
with p.open("w", newline="") as f:
    w = csv.DictWriter(f, fieldnames=rows[0].keys()); w.writeheader(); w.writerows(rows)
print(f"mutated {len(picked)} rows, e.g. product_id {rows[picked[0]]['product_id']}")
