from collections import defaultdict
import os
import pandas as pd
from dbfread import DBF

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
        str_df.columns = [col.lower() for col in str_df.columns]
        if not str_df.empty and 'name' in str_df.columns:
            first_nonempty = str_df['name'].dropna().astype(str).str.strip()
            if not first_nonempty.empty and first_nonempty.iloc[0]:
                store_name = first_nonempty.iloc[0]
                print(f"🏪 Store name found: {store_name}")
            else:
                print("⚠️ 'name' column exists but is empty.")
        else:
            print("⚠️ str.dbf loaded but 'name' column not found.")
    except Exception as e:
        print(f"⚠️ Could not read str.dbf: {e}")
else:
    print(f"⚠️ str.dbf not found at {str_path}")

# === Step 2: Process each day's reports.csv ===
for subdir in sorted(os.listdir(base_path)):
    csv_path = os.path.join(base_path, subdir, "reports.csv")
    if not os.path.exists(csv_path):
        continue

    try:
        df = pd.read_csv(csv_path)
        df.columns = [col.lower() for col in df.columns]  # Normalize column names

        print(f"📄 Folder {subdir} - {len(df)} rows")

        if not all(col in df.columns for col in ['cappname', 'crepname', 'rundate']):
            print(f"⚠️ Missing required columns in {csv_path}")
            continue

        df['rundate'] = pd.to_datetime(df['rundate'], errors='coerce')
        before = len(df)
        df = df.dropna(subset=['cappname', 'rundate'])
        print(f"✅ Valid rows after dropna: {len(df)} (discarded {before - len(df)})")

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
summary_df = pd.DataFrame(summary)

if not summary_df.empty:
    summary_df.to_csv(out_path, index=False)
    print(f"✅ Wrote {len(summary_df)} rows to {out_path}")
else:
    if os.path.exists(out_path):
        os.remove(out_path)  # ensure no empty file exists
    print("⚠️ No data to write — check source files.")
