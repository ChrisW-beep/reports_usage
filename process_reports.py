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

# === Step 1: Get Store Name from str.dbf ===
store_name = "Unknown"
str_path = os.path.join(base_path, "str.dbf")
if os.path.exists(str_path):
    try:
        str_df = pd.DataFrame(iter(DBF(str_path, load=True)))
        if not str_df.empty and 'name' in str_df.columns:
            store_name = str_df.iloc[0]['name']
    except Exception as e:
        print(f"⚠️ Could not read str.dbf: {e}")

# === Step 2: Process each day's reports.csv ===
for i in range(1, 8):
    csv_path = os.path.join(base_path, str(i), "reports.csv")
    if not os.path.exists(csv_path):
        continue

    try:
        df = pd.read_csv(csv_path)
        print(f"📄 Day {i} - {len(df)} rows")
        
        # ✅ Ensure 'rundate' is properly parsed
        df['rundate'] = pd.to_datetime(df['rundate'], errors='coerce')
        
        # ✅ Validate required columns
        if not all(col in df.columns for col in ['cappname', 'crepname', 'rundate']):
            print(f"⚠️ Missing required columns in {csv_path}")
            continue
        
        # ✅ Drop rows missing any of the key fields
        df = df.dropna(subset=['cappname', 'rundate'])
        
        # ✅ Optional: Show what you're working with
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
