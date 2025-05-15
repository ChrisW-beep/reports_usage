import pandas as pd
import os

input_file = "/tmp/weekly_report_aggregated.csv"
output_file = "/tmp/weekly_report_global_summary.csv"

if not os.path.exists(input_file):
    print(f"❌ Input file not found: {input_file}")
    exit(1)

df = pd.read_csv(input_file)

if df.empty:
    print("⚠️ Input CSV is empty.")
    exit(0)

# Group by cappname + crepname to calculate total days and unique store count
grouped = df.groupby(['cappname', 'crepname'])

summary = grouped.agg(
    stores_ran=('store', 'nunique'),
    total_days=('days_ran', 'sum')
).reset_index()

summary.to_csv(output_file, index=False)
print(f"✅ Global summary written to {output_file}")
