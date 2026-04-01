import pandas as pd
from pathlib import Path

# -------------------------
# Config
# -------------------------
GEOCODED_DIR = Path("data/geocoded")
RAW_DIR = Path("data/raw")
MAPBOX_FILE = GEOCODED_DIR / "mapbox_results.csv"
OUTPUT_DIR = Path("data/final")

OUTPUT_DIR.mkdir(exist_ok=True)


# -------------------------
# Helpers
# -------------------------
def check_rows(df, expected, step):
    if len(df) != expected:
        print(f"⚠️ Row mismatch at {step}: {len(df)} vs {expected}")
    else:
        print(f"✅ {step}: row count OK ({len(df)})")


def normalize_zip5(zip_code):
    """Always return 5-digit ZIP for matching"""
    if pd.isna(zip_code):
        return None

    z = str(zip_code).replace(".0", "").strip()

    if z.lower() == "nan" or z == "":
        return None

    return z.split("-")[0].zfill(5)


def normalize_strings(df, cols):
    for c in cols:
        if c in df.columns:
            df[c] = df[c].astype(str).str.strip().str.upper()


# -------------------------
# Load Mapbox
# -------------------------
mapbox = pd.read_csv(MAPBOX_FILE)

mapbox = mapbox.rename(columns={"lat": "mb_lat", "lng": "mb_lng", "relevance": "mapbox_relevance"})

mapbox = mapbox.drop_duplicates(subset=["building_id"])


# -------------------------
# Process batches
# -------------------------
files = list(RAW_DIR.glob("*.xls")) + list(RAW_DIR.glob("*.xlsx"))

print(f"Found {len(files)} Excel files")

for original_file in files:
    base = original_file.stem

    print("\n============================")
    print(f"Processing: {base}")
    print("============================")

    geocoded_file = GEOCODED_DIR / f"{base}_geocoded.csv"
    dropped_file = GEOCODED_DIR / f"{base}_dropped_geocoded.csv"

    if not geocoded_file.exists() or not dropped_file.exists():
        print(f"⚠️ Missing geocoded files for {base}, skipping")
        continue

    # -------------------------
    # Load original Excel
    # -------------------------
    original = pd.read_excel(original_file)

    original.columns = original.columns.str.strip().str.lower().str.replace(" ", "_")

    original = original.rename(
        columns={"res_address_1": "street_address", "addr_non_std": "non_standard_addr", "zip_code": "zip"}
    )

    normalize_strings(original, ["street_address", "non_standard_addr", "city", "state"])

    # Fix "NAN" strings
    original["non_standard_addr"] = original["non_standard_addr"].replace("NAN", "")

    original["zip"] = original["zip"].apply(normalize_zip5)

    original["_row_id"] = range(len(original))
    original_row_count = len(original)

    print(f"Original rows: {original_row_count}")

    # -------------------------
    # Load geocoded + dropped
    # -------------------------
    geocoded = pd.read_csv(geocoded_file)
    dropped = pd.read_csv(dropped_file)

    normalize_strings(geocoded, ["street_address", "addr_non_std", "city", "state"])
    normalize_strings(dropped, ["street_address", "addr_non_std", "city", "state"])

    for df in [geocoded, dropped]:
        df["zip"] = df["zip"].apply(normalize_zip5)

    # Deduplicate
    geocoded = geocoded.drop_duplicates(subset=["street_address", "city", "state", "zip"])
    dropped = dropped.drop_duplicates(subset=["addr_non_std", "city", "state", "zip"])

    # -------------------------
    # Merge building_id
    # -------------------------
    join_std = ["street_address", "city", "state", "zip"]
    join_nonstd = ["addr_non_std", "city", "state", "zip"]

    std = original.merge(geocoded[join_std + ["building_id", "address_id"]], on=join_std, how="left")
    check_rows(std, original_row_count, "standard merge")

    nonstd = original.merge(
        dropped[join_nonstd + ["building_id", "address_id"]],
        left_on=["non_standard_addr", "city", "state", "zip"],
        right_on=join_nonstd,
        how="left",
    )
    check_rows(nonstd, original_row_count, "non-standard merge")

    original["building_id"] = std["building_id"].combine_first(nonstd["building_id"])
    original["address_id"] = std["address_id"].combine_first(nonstd["address_id"])

    original["id_source"] = None
    original.loc[std["building_id"].notna(), "id_source"] = "standard"
    original.loc[(original["id_source"].isna()) & (nonstd["building_id"].notna()), "id_source"] = "non_standard"

    print("Matched building_id:", original["building_id"].notna().sum())
    print("Unmatched:", original["building_id"].isna().sum())

    # -------------------------
    # Prep Census
    # -------------------------
    geocoded = geocoded.rename(columns={"lat": "census_lat", "lng": "census_lng"})

    geocoded = geocoded.drop_duplicates(subset=["building_id"])

    # -------------------------
    # Merge Census
    # -------------------------
    merged = original.merge(geocoded[["building_id", "census_lat", "census_lng"]], on="building_id", how="left")
    check_rows(merged, original_row_count, "census merge")

    # -------------------------
    # Merge Mapbox
    # -------------------------
    merged = merged.merge(mapbox[["building_id", "mb_lat", "mb_lng", "mapbox_relevance"]], on="building_id", how="left")
    check_rows(merged, original_row_count, "mapbox merge")

    # -------------------------
    # Final geocode selection
    # -------------------------
    merged["latitude"] = merged["census_lat"].combine_first(merged["mb_lat"])
    merged["longitude"] = merged["census_lng"].combine_first(merged["mb_lng"])

    merged["geocode_source"] = merged.apply(
        lambda row: "census" if pd.notnull(row["census_lat"]) else ("mapbox" if pd.notnull(row["mb_lat"]) else None),
        axis=1,
    )

    # -------------------------
    # Cleanup
    # -------------------------
    merged = merged.drop(columns=["census_lat", "census_lng", "mb_lat", "mb_lng"], errors="ignore")

    # -------------------------
    # QA
    # -------------------------
    print("\nQA SUMMARY")
    print("Total rows:", len(merged))
    print("Geocoded:", merged["latitude"].notna().sum())
    print("Census:", (merged["geocode_source"] == "census").sum())
    print("Mapbox:", (merged["geocode_source"] == "mapbox").sum())

    # -------------------------
    # Save
    # -------------------------
    output_file = OUTPUT_DIR / f"{base}_final_geocoded.csv"
    merged.to_csv(output_file, index=False)

    print(f"\nSaved: {output_file}")
