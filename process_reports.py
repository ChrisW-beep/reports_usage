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
        print(f"⚠️ Could not read str.dbf for store name: {e}")

# === Step 2: Collect (cappname, crepname) pairs and their rundates ===
for i in range(1, 8):
    csv_path = os.path.join(base_path, str(i), "reports.csv")
    if not os.path.exists(csv_path):
        continue
    try:
        df = pd.read_csv(csv_path)
    except Exception as e:
        print(f"⚠️ Could not read {csv_path}: {e}")
        continue

    for _, row in df.iterrows():
        capp = row.get('cappname')
        crep = row.get('crepname')
        run = row.get('rundate')

        if pd.notnull(capp) and pd.notnull(run):
            key = (capp, crep)
            if key not in report_data:
                report_data[key] = set()
            report_data[key].add(run)

# === Step 3: Build summary ===
summary = []
for (capp, crep), rundates in report_data.items():
    summary.append({
        'store': store_name,
        'cappname': capp,
        'crepname': crep if pd.notnull(crep) else '',
        'days_ran': len(rundates)
    })

# === Step 4: Save summary ===
out_path = os.path.join(base_path, "weekly_report_usage.csv")
pd.DataFrame(summary).to_csv(out_path, index=False)
print(f"✅ Saved summary to {out_path}")
