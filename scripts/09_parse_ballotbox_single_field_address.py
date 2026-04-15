import os
import sys
import pandas as pd
import re


INPUT_FILE = os.environ.get("GEOCODE_INPUT", "data/raw/PE26_Oregon_Drop_Sites_20260324.csv")
OUTPUT_FILE = os.environ.get("GEOCODE_OUTPUT", "data/processed/PE26_Oregon_Drop_Sites_20260324_processed.csv")


df = pd.read_csv(INPUT_FILE)


def split_address(addr):
    if pd.isna(addr):
        return pd.Series([None, None, None, None])

    addr = str(addr).strip()

    # More robust pattern
    match = re.match(r"^(.*)\s+([A-Za-z.\-\s]+)\s+([A-Z]{2})\s+(\d{5})$", addr)

    if match:
        street, city, state, zip_code = match.groups()
        return pd.Series([street.strip(), city.strip(), state.strip(), zip_code.strip()])
    else:
        # fallback: keep full address as street so geocoder still works
        return pd.Series([addr, None, None, None])


df[["street", "city", "state", "zip"]] = df["Address"].apply(split_address)


# Optional: check failures
failed = df[df["city"].isna()]
print(f"Rows that failed parsing: {len(failed)}")
print(failed[["Address"]].to_string(index=False))


df.to_csv(OUTPUT_FILE, index=False)

print(f"Processed file saved to {OUTPUT_FILE}")
