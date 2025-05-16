# summarize_global_usage.py
import os
import sys
import pandas as pd
from collections import defaultdict

if len(sys.argv) != 3:
    print("Usage: summarize_global_usage.py <input_dir> <output_csv>")
    sys.exit(1)

input_dir = sys.argv[1]
output_path = sys.argv[2]

usage_summary = defaultdict(lambda: {'stores': set(), 'total_runs': 0})

for prefix in os.listdir(input_dir):
    path = os.path.join(input_dir, prefix, "weekly_report_usage.csv")
    if not os.path.isfile(path) or os.path.getsize(path) == 0:
        continue

    try:
        df = pd.read_csv(path)
        for _, row in df.iterrows():
            key = (row['cappname'], row['crepname'])
            usage_summary[key]['stores'].add(row['store'])
            usage_summary[key]['total_runs'] += row['days_ran']
    except Exception as e:
        print(f"⚠️ Failed to process {path}: {e}")

# Build summary output
summary_rows = []
for (cappname, crepname), data in usage_summary.items():
    summary_rows.append({
        'cappname': cappname,
        'crepname': crepname,
        'stores_used': len(data['stores']),
        'total_days_ran': data['total_runs']
    })

if summary_rows:
    pd.DataFrame(summary_rows).to_csv(output_path, index=False)
    print(f"✅ Global summary written to {output_path}")
else:
    print("⚠️ No usage data found across stores.")
    sys.exit(1)
