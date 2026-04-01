"""
Census retry geocoding script (single-line endpoint)

- Reads global_retry_buildings.csv
- Uses Census single-line geocoder (more forgiving than batch)
- Caches results to disk
- Supports resume (won’t redo work)
- Outputs lat/lng + match_type

Run:
    python scripts/06_census_retry.py
"""

import json
import time
from pathlib import Path

import pandas as pd
import requests


# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

INPUT_FILE = "data/geocoded/global_retry_buildings.csv"
OUTPUT_FILE = "data/geocoded/census_retry_results.csv"
CACHE_DIR = Path(".geocoding_cache/census_retry")

CACHE_DIR.mkdir(parents=True, exist_ok=True)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def build_address(row):
    zip5 = str(row["zip"])[:5] if pd.notna(row["zip"]) else ""
    return f"{row['street_address']}, {row['city']}, {row['state']} {zip5}"


def geocode_row(row):
    building_id = row["building_id"]
    cache_file = CACHE_DIR / f"{building_id}.json"

    # --- CACHE HIT ---
    if cache_file.exists():
        with open(cache_file) as f:
            return json.load(f)

    address = build_address(row)

    try:
        r = requests.get(
            "https://geocoding.geo.census.gov/geocoder/locations/onelineaddress",
            params={
                "address": address,
                "benchmark": "Public_AR_Current",
                "vintage": "Current_Current",
                "format": "json",
            },
            timeout=10,
        )

        r.raise_for_status()
        data = r.json()
        matches = data["result"]["addressMatches"]

        if matches:
            m = matches[0]
            result = {
                "building_id": building_id,
                "street_address": row["street_address"],
                "city": row["city"],
                "state": row["state"],
                "zip": row["zip"],
                "lat": m["coordinates"]["y"],
                "lng": m["coordinates"]["x"],
                "match_type": m["matchType"],
                "source": "census_retry",
            }
        else:
            result = None

    except Exception:
        result = None

    # --- SAVE CACHE ---
    with open(cache_file, "w") as f:
        json.dump(result, f)

    return result


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def main():
    df = pd.read_csv(INPUT_FILE)

    # --- Resume support ---
    if Path(OUTPUT_FILE).exists():
        existing = pd.read_csv(OUTPUT_FILE)
        done_ids = set(existing["building_id"])
        print(f"Resuming: {len(done_ids)} already processed")
    else:
        existing = pd.DataFrame()
        done_ids = set()

    results = existing.to_dict("records")

    total = len(df)

    for i, row in df.iterrows():
        building_id = row["building_id"]

        if building_id in done_ids:
            continue

        res = geocode_row(row)

        if res:
            results.append(res)

        # --- progress ---
        if i % 100 == 0:
            print(f"{i}/{total} processed")

            # incremental save
            pd.DataFrame(results).to_csv(OUTPUT_FILE, index=False)

        # --- rate limit ---
        time.sleep(0.1)  # ~10 req/sec (safe)

    # final save
    pd.DataFrame(results).to_csv(OUTPUT_FILE, index=False)

    print("\nDone.")
    print(f"Results saved to {OUTPUT_FILE}")


# ---------------------------------------------------------------------------

if __name__ == "__main__":
    main()
