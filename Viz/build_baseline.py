"""Build baseline_1951_1980.json from streamflow.csv for the app."""
import csv
import json
import sys
from collections import defaultdict

INPUT = "Visualization/app/public/ganga/streamflow.csv"
OUTPUT = "Visualization/app/public/ganga/baseline_1951_1980.json"

sums = defaultdict(float)
counts = defaultdict(int)

with open(INPUT) as f:
    reader = csv.DictReader(f)
    for row in reader:
        date = row["date"]
        year = int(date.split("-")[0])
        if year < 1951 or year > 1980:
            continue
        seg_id = row["seg_id"].split(".")[0]  # strip ".0"
        flow = float(row["streamflow_m3s"])
        sums[seg_id] += flow
        counts[seg_id] += 1

baseline = {seg: sums[seg] / counts[seg] for seg in sums}
print(f"Computed baseline for {len(baseline)} segments")

with open(OUTPUT, "w") as f:
    json.dump(baseline, f)

print(f"Written to {OUTPUT}")
