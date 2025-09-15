"""
Cleaning script for properties dataset — converts fields to ML-friendly types and drops unwanted columns.

Changes implemented (as requested):
- price -> integer (int64)
- garage -> boolean (True if value != 'non', else False)
- living area -> integer (nullable Int64) (removes m2 / non-digits)
- drop land area columns (if found)
- year_built -> integer (nullable Int64)
- drop epc_consumption (if present)
- terrace -> boolean (False if empty or 'non', else True)
- swimming_pool -> boolean (False if empty or 'non', else True)

Additionally:
- keeps your postal code → province/region mapping
- keeps bedrooms/bathrooms defaulting to 1 where missing
- simple daily backup of previous dashboard file: if ./data/db_dashboard.csv exists it will be copied to ./data/db_dashboard_YYYY-MM-DD.csv and the previous day's backup (if present) will be removed.

Notes:
- The script is defensive: it checks whether columns exist before operating and searches for plausible column names for the living area and land area where relevant.
- Missing numeric conversions are kept as pandas nullable Int64 so you keep track of missing values.

"""

import os
import shutil
from datetime import datetime, timedelta

import numpy as np
import pandas as pd

try:
    from zoneinfo import ZoneInfo
except Exception:
    ZoneInfo = None

# --- File paths ---
INPUT_FILE = "./data/properties_enriched_parallel.csv"
OUTPUT_FILE = "./data/db_dashboard.csv"

# --- Load ---
if not os.path.exists(INPUT_FILE):
    raise FileNotFoundError(f"Input file not found: {INPUT_FILE}")

df = pd.read_csv(INPUT_FILE)
print(f"Loaded {len(df)} rows from {INPUT_FILE}")

# --- Remove unwanted rows ---
if "link" in df.columns:
    df = df[~df["link"].astype(str).str.contains("projectdetail", na=False)].copy()

# Drop rows without price column or where price is NaN
if "price" not in df.columns:
    raise KeyError("Column 'price' not found in dataframe")

# --- Price cleaning: keep only digits and convert to int64 ---
price_clean = (
    df["price"].astype(str)
    .str.replace(r"[^\d]", "", regex=True)
    .replace("", pd.NA)
)
price_clean = pd.to_numeric(price_clean, errors="coerce")
# Keep only rows where price could be parsed
mask_price_ok = price_clean.notna()
if not mask_price_ok.all():
    removed = (~mask_price_ok).sum()
    print(f"Removing {removed} rows with unparseable price")

df = df[mask_price_ok].copy()
# final integer price column
df["price"] = price_clean[mask_price_ok].astype("int64")

# --- Postal codes → Province & Region (keeps your mapping) ---
province_region_map = {
    "1000-1299": ("Bruxelles", "Bruxelles-Capitale"),
    "1300-1499": ("Brabant wallon", "Wallonie"),
    "1500-1999": ("Brabant flamand", "Flandre"),
    "2000-2999": ("Anvers", "Flandre"),
    "3000-3499": ("Brabant flamand", "Flandre"),
    "3500-3999": ("Limbourg", "Flandre"),
    "4000-4999": ("Liège", "Wallonie"),
    "5000-5999": ("Namur", "Wallonie"),
    "6000-6599": ("Hainaut", "Wallonie"),
    "6600-6999": ("Luxembourg", "Wallonie"),
    "7000-7999": ("Hainaut", "Wallonie"),
    "8000-8999": ("Flandre occidentale", "Flandre"),
    "9000-9999": ("Flandre orientale", "Flandre"),
}


def get_province_region(postal_code):
    try:
        pc = int(postal_code)
        for rng, (prov, reg) in province_region_map.items():
            start, end = map(int, rng.split("-"))
            if start <= pc <= end:
                return prov, reg
    except Exception:
        return None, None
    return None, None

if "postal_code" in df.columns:
    df["province"], df["region"] = zip(*df["postal_code"].apply(get_province_region))
else:
    df["province"] = None
    df["region"] = None

# --- Default values for bedrooms & bathrooms ---
for col in ["bedrooms", "bathrooms"]:
    if col in df.columns:
        df[col] = df[col].fillna(1).astype(int)

# --- Helper: find columns by keywords ---

def find_column(df, *keywords):
    """Return first column name containing all keywords (case-insensitive), or None."""
    keys = [k.lower() for k in keywords]
    for c in df.columns:
        cn = c.lower()
        if all(k in cn for k in keys):
            return c
    return None

# --- Garage, garden & swimming_pool: default to 'non' then convert to boolean ---
for col in ["garage", "swimming_pool", "garden"]:
    if col in df.columns:
        df[col] = df[col].fillna("non").astype(str)
        df[col] = df[col].apply(lambda x: False if str(x).strip().lower() == "non" or str(x).strip() == "" else True)

# --- Terrace cleanup -> boolean ---
if "terrace" in df.columns:
    df["terrace"] = df["terrace"].fillna("non").astype(str)
    df["terrace"] = df["terrace"].apply(lambda x: False if str(x).strip().lower() == "non" or str(x).strip() == "" else True)

# --- Living area:convert to integer (nullable) ---
if "living_area" in df.columns:
    s = df["living_area"].astype(str).str.replace(r"[^\d]", "", regex=True).replace("", pd.NA)
    s = pd.to_numeric(s, errors="coerce")
    df["living_area"] = s.astype("Int64")
    print("Converted 'living_area' to integers (nullable)")
else:
    print("No 'living_area' column detected — skipping conversion")


# --- Drop land area columns (search for obvious names) ---
land_cols = [c for c in df.columns if "land" in c.lower() or "terrain" in c.lower()]
if land_cols:
    df.drop(columns=land_cols, inplace=True)
    print(f"Dropped land-related columns: {land_cols}")

# --- Drop epc_consumption if present ---
if "epc_consumption" in df.columns:
    df.drop(columns=["epc_consumption"], inplace=True)
    print("Dropped column: epc_consumption")

# --- year_built -> integer (nullable) ---
if "year_built" in df.columns:
    df["year_built"] = pd.to_numeric(df["year_built"], errors="coerce").astype("Int64")
    print("Converted year_built to nullable integer (Int64)")

# --- Daily backup of existing dashboard and remove yesterday's backup ---
try:
    if ZoneInfo is not None:
        now = datetime.now(ZoneInfo("Europe/Brussels"))
    else:
        now = datetime.now()
    today = now.date()
    yesterday = today - timedelta(days=1)

    if os.path.exists(OUTPUT_FILE):
        backup_today = OUTPUT_FILE.replace(".csv", f"_{today.isoformat()}.csv")
        shutil.copy2(OUTPUT_FILE, backup_today)
        # remove yesterday's backup if present
        backup_yesterday = OUTPUT_FILE.replace(".csv", f"_{yesterday.isoformat()}.csv")
        if os.path.exists(backup_yesterday):
            os.remove(backup_yesterday)
            print(f"Removed yesterday's backup: {backup_yesterday}")
        print(f"Backed up existing {OUTPUT_FILE} to {backup_today}")
except Exception as e:
    print(f"Warning while creating backups: {e}")

# --- Save cleaned dataset ---
df.to_csv(OUTPUT_FILE, index=False, encoding="utf-8")
print(f"Dashboard dataset saved → {OUTPUT_FILE} (rows: {len(df)})")

# --- Quick dtype summary (helpful to confirm conversions) ---
print(df.dtypes)
