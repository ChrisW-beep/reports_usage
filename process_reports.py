import os
import pandas as pd
from dbfread import DBF
from collections import defaultdict

prefix = os.environ.get("PREFIX")
if not prefix:
    print("❌ No prefix set.")
    exit(1)

base_path = f"/tmp/extracted/{prefix}"
report_data = {}

# === Step 1: Get Store Name from Jenkins parameter ===
store_name = os.environ.get("STORE_NAME", "Unknown")

# === Step 2: Process each subfolder's reports.csv ===
for subdir in sorted(os.listdir(base_path)):
    csv_path = os.path.join(base_path, subdir, "reports.csv")
    if not os.path.exists(csv_path):
        continue
   
    try:
        df = pd.read_csv(csv_path)
        df.columns = [col.lower() for col in df.columns]  # normalize to lowercase
        print(f"📄 Folder {subdir} - {len(df)} rows")        
        
        # Parse dates
        df['rundate'] = pd.to_datetime(df['rundate'], errors='coerce')
        
        # Validate required columns
        if not all(col in df.columns for col in ['cappname', 'crepname', 'rundate']):
            print(f"⚠️ Missing required columns in {csv_path}")
            continue
        
        # Drop rows missing key fields
        df = df.dropna(subset=['cappname', 'rundate'])
        
        print(df[['cappname', 'crepname', 'rundate']].head())

        for _, row in df.iterrows():
            key = (row['cappname'], row['crepname'])
            report_data.setdefault(key, set()).add(row['rundate'])

    except Exception as e:
        print(f"⚠️ Could not process {csv_path}: {e}")

# === Step 3: Write summary CSV ===
summary = [
    {
        'store': store_name,
        'cappname': k[0],
        'crepname': k[1],
        'days_ran': len(v)
    }
    for k, v in report_data.items()
]

out_path = os.path.join(base_path, "weekly_report_usage.csv")
if summary:
    pd.DataFrame(summary).to_csv(out_path, index=False)
    print(f"✅ Wrote {len(summary)} rows to {out_path}")
else:
    print("⚠️ No data to write — check source files.")
