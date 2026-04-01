import os
import pandas as pd

INPUT_FILE = "data/raw/CD06-RegisteredVoters-2026-03-05-16619.xlsx"
OUTPUT_FILE = "data/processed/CD06-RegisteredVoters-2026-03-05-16619_validated.csv"


def main():
    if not os.path.exists(INPUT_FILE):
        raise FileNotFoundError(f"File not found: {INPUT_FILE}")

    df = pd.read_excel(INPUT_FILE)

    print(f"Loaded {len(df)} rows")

    # Standardize column names
    df.columns = df.columns.str.lower()

    df = df.rename(
        columns={
            "res_address_1": "street_address",
            "city": "city",
            "state": "state",
            "zip_code": "zip",
            "zip_plus_four": "zip_plus_four",
        }
    )

    required = ["street_address", "city", "state", "zip"]

    # Check missing columns
    missing_cols = [col for col in required if col not in df.columns]
    if missing_cols:
        raise ValueError(f"Missing required columns: {missing_cols}")

    # Drop rows with missing required fields
    before = len(df)

    # Capture dropped rows
    dropped_df = df[df[required].isnull().any(axis=1)]

    # Drop them from main dataset
    df = df.dropna(subset=required)

    after = len(df)

    print(f"Dropped {before - after} rows with missing required fields")

    # Basic cleanup
    df["street_address"] = df["street_address"].astype(str).str.strip()
    df["city"] = df["city"].astype(str).str.strip()
    df["state"] = df["state"].astype(str).str.strip()

    # Clean ZIP (always keep first 5)
    df["zip"] = df["zip"].astype(str).str[:5]

    # --- ZIP+4 handling ---
    if "zip_plus_four" in df.columns:
        df["zip_plus_four"] = df["zip_plus_four"].astype(str)

        df["zip"] = df.apply(
            lambda x: f"{x['zip']}-{x['zip_plus_four'].zfill(4)}"
            if pd.notna(x["zip_plus_four"]) and x["zip_plus_four"].strip() not in ["", "nan"]
            else x["zip"],
            axis=1,
        )

    # Ensure output directory exists
    os.makedirs(os.path.dirname(OUTPUT_FILE), exist_ok=True)

    df.to_csv(OUTPUT_FILE, index=False)

    print(f"Cleaned file saved to {OUTPUT_FILE}")
    print(f"Final row count: {len(df)}")

    # Save dropped rows
    DROPPED_OUTPUT_FILE = OUTPUT_FILE.replace("_validated.csv", "_dropped.csv")

    if len(dropped_df) > 0:
        dropped_df.to_csv(DROPPED_OUTPUT_FILE, index=False)
        print(f"Dropped rows saved to {DROPPED_OUTPUT_FILE}")


if __name__ == "__main__":
    main()
