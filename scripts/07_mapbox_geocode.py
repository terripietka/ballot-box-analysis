"""
Mapbox geocoding script (standalone, cache + resume safe)

- Reads global_retry_buildings.csv
- Uses Mapbox Geocoding API
- Caches results per building_id
- Supports resume
"""

import os
import json
import time
from pathlib import Path

import pandas as pd
import requests
from dotenv import load_dotenv

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

INPUT_FILE = "data/geocoded/global_retry_buildings.csv"
OUTPUT_FILE = "data/geocoded/mapbox_results.csv"
CACHE_DIR = Path(".geocoding_cache/mapbox")

CACHE_DIR.mkdir(parents=True, exist_ok=True)

load_dotenv()
MAPBOX_TOKEN = os.getenv("MAPBOX_API_KEY")

if not MAPBOX_TOKEN:
    raise ValueError("MAPBOX_API_KEY not set in environment")


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def build_query(row):
    zip5 = str(row["zip"])[:5] if pd.notna(row["zip"]) else ""
    return f"{row['street_address']}, {row['city']}, {row['state']} {zip5}"


def geocode_row(row):
    building_id = row["building_id"]
    cache_file = CACHE_DIR / f"{building_id}.json"

    # --- CACHE ---
    if cache_file.exists():
        with open(cache_file) as f:
            return json.load(f)

    query = build_query(row)

    try:
        url = f"https://api.mapbox.com/geocoding/v5/mapbox.places/{query}.json"

        r = requests.get(
            url,
            params={
                "access_token": MAPBOX_TOKEN,
                "limit": 1,
                "country": "us",
            },
            timeout=10,
        )

        r.raise_for_status()
        data = r.json()

        features = data.get("features", [])

        if features:
            f0 = features[0]

            result = {
                "building_id": building_id,
                "street_address": row["street_address"],
                "city": row["city"],
                "state": row["state"],
                "zip": row["zip"],
                "lat": f0["center"][1],
                "lng": f0["center"][0],
                "relevance": f0.get("relevance"),
                "place_type": f0.get("place_type", [None])[0],
                "source": "mapbox",
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

    # --- CLEAN INPUT ---
    for col in ["street_address", "city", "state", "zip"]:
        df[col] = df[col].fillna("").astype(str)

    df["zip"] = df["zip"].str[:5]

    # --- RESUME SUPPORT ---
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

        # --- PROGRESS ---
        if i % 100 == 0:
            print(f"{i}/{total}")
            pd.DataFrame(results).to_csv(OUTPUT_FILE, index=False)

        # --- RATE LIMIT ---
        time.sleep(0.05)  # ~20 req/sec

    # --- FINAL SAVE ---
    pd.DataFrame(results).to_csv(OUTPUT_FILE, index=False)

    print("\nDone.")
    print(f"Saved to {OUTPUT_FILE}")


# ---------------------------------------------------------------------------

if __name__ == "__main__":
    main()
