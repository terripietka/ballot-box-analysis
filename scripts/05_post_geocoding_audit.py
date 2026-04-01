import pandas as pd
from pathlib import Path
from datetime import datetime

RAW_DIR = Path("data/raw")
FINAL_DIR = Path("data/final")

timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
summary_path = f"data/final/geocoding_summary_{timestamp}.csv"

addr_cols = ["street_address", "city", "state", "zip"]

summary = []


# -------------------------
# Helper
# -------------------------
def normalize_cols(df):
    df.columns = df.columns.str.strip().str.lower().str.replace(" ", "_")

    df = df.rename(columns={"res_address_1": "street_address", "addr_non_std": "non_standard_addr", "zip_code": "zip"})

    for col in ["street_address", "city", "state"]:
        if col in df.columns:
            df[col] = df[col].astype(str).str.strip().str.upper()

    df["zip"] = df["zip"].astype(str).str.replace(".0", "", regex=False).str[:5].str.zfill(5)

    return df


# -------------------------
# Process files
# -------------------------
raw_files = list(RAW_DIR.glob("*.xlsx"))

for raw_file in raw_files:
    base = raw_file.stem
    final_file = FINAL_DIR / f"{base}_final_geocoded.csv"

    if not final_file.exists():
        print(f"⚠️ Missing final file for {base}, skipping")
        continue

    print(f"\nProcessing: {base}")

    # -------------------------
    # Load raw
    # -------------------------
    raw = pd.read_excel(raw_file)
    raw = normalize_cols(raw)

    total_voters = len(raw)
    total_addresses = raw[addr_cols].drop_duplicates().shape[0]

    # -------------------------
    # Junk detection
    # -------------------------
    junk_mask = raw["street_address"].str.contains(r"X{2,}", na=False) | raw["street_address"].str.contains(
        "ACP", na=False
    )

    junk_voters = junk_mask.sum()
    junk_addresses = raw.loc[junk_mask, addr_cols].drop_duplicates().shape[0]

    # -------------------------
    # Load final
    # -------------------------
    final = pd.read_csv(final_file)

    geocoded_mask = final["latitude"].notna()

    geocoded_voters = geocoded_mask.sum()
    geocoded_addresses = final.loc[geocoded_mask, addr_cols].drop_duplicates().shape[0]

    # -------------------------
    # Success rates
    # -------------------------
    voter_success_rate = round(geocoded_voters / total_voters * 100, 2)
    address_success_rate = round(geocoded_addresses / total_addresses * 100, 2)

    # -------------------------
    # Non-junk counts
    # -------------------------
    non_junk_final = final[
        ~final["street_address"].str.contains(r"X{2,}", na=False)
        & ~final["street_address"].str.contains("ACP", na=False)
    ]

    non_junk_voters = len(non_junk_final)
    non_junk_addresses = non_junk_final[addr_cols].drop_duplicates().shape[0]

    # -------------------------
    # Failures (non-junk AND not geocoded)
    # -------------------------
    failed_voters = non_junk_final["latitude"].isna().sum()

    failed_addresses = non_junk_final[non_junk_final["latitude"].isna()][addr_cols].drop_duplicates().shape[0]

    # -------------------------
    # Append
    # -------------------------
    summary.append({
        "file": base,
        "total_voters": total_voters,
        "total_addresses": total_addresses,
        "junk_voters": junk_voters,
        "junk_addresses": junk_addresses,
        "geocoded_voters": geocoded_voters,
        "geocoded_addresses": geocoded_addresses,
        "non_junk_voters": non_junk_voters,
        "non_junk_addresses": non_junk_addresses,
        "failed_voters": failed_voters,
        "failed_addresses": failed_addresses,
        "voter_success_pct": voter_success_rate,
        "address_success_pct": address_success_rate,
    })


# -------------------------
# Build summary table
# -------------------------
summary_df = pd.DataFrame(summary)

print("\n=== Per-file summary ===")
print(summary_df)

print("\n=== Totals ===")
totals = summary_df.sum(numeric_only=True)

totals["file"] = "TOTAL"
totals["voter_success_pct"] = round(totals["geocoded_voters"] / totals["total_voters"] * 100, 2)
totals["address_success_pct"] = round(totals["geocoded_addresses"] / totals["total_addresses"] * 100, 2)

summary_with_total = pd.concat([summary_df, pd.DataFrame([totals])], ignore_index=True)

summary_with_total.to_csv(summary_path, index=False)

print(f"\nSummary saved to: {summary_path}")
