"""
Compute minimum travel time from each voter address to any ballot drop box
using the TravelTime API, then merge the results back into the full voter file.

This script:
    1. Loads geocoded voters and ballot box coordinates.
    2. Deduplicates voter locations to minimize API cost.
    3. Calls TravelTime's time-filter API in batches.
    4. Returns the minimum travel time (minutes) for each voter.
    5. Merges back into the full voter dataset.
    6. Saves an output CSV.

Environment variables required:
    TRAVELTIME_ID
    TRAVELTIME_KEY

Inputs must contain:
    voters_df: columns ["lat", "lng"]
    boxes_df:  columns ["lat", "lng"]

Usage:
    python compute_travel_times.py
"""

import os
import sys
import time
import requests
from datetime import datetime

import pandas as pd
from tqdm import tqdm


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

TRAVELTIME_ID = os.environ.get("TRAVELTIME_ID")
TRAVELTIME_KEY = os.environ.get("TRAVELTIME_KEY")

# REQUIRED: fill these in or set via environment variables
VOTERS_CSV = os.environ.get("TRAVELTIME_VOTERS_INPUT", "")
BOXES_CSV = os.environ.get("TRAVELTIME_BOXES_INPUT", "")
OUTPUT_CSV = os.environ.get("TRAVELTIME_OUTPUT", "voters_with_travel_times.csv")

URL = "https://api.traveltimeapp.com/v4/time-filter"


# ---------------------------------------------------------------------------
# Validation helpers
# ---------------------------------------------------------------------------


def require_env(var, description):
    if not var:
        raise EnvironmentError(f"Environment variable '{description}' is not set.")


def require_file(path, description):
    if not path:
        raise ValueError(f"{description} path not set.")
    if not os.path.exists(path):
        raise FileNotFoundError(f"{description} not found: {path}")


def require_columns(df, columns, df_name):
    missing = [col for col in columns if col not in df.columns]
    if missing:
        raise ValueError(f"{df_name} is missing required columns: {missing}")


# ---------------------------------------------------------------------------
# TravelTime API logic
# ---------------------------------------------------------------------------


def compute_min_travel_times(voters_df, boxes_df, batch_size=10):
    """
    Compute minimum travel time (in minutes) from each voter → nearest box.
    voters_df must contain columns ["lat", "lng"].
    """

    results = []

    # Prepare box locations
    box_locations = [{"id": f"box{i}", "coords": {"lat": row.lat, "lng": row.lng}} for i, row in boxes_df.iterrows()]
    box_ids = [f"box{i}" for i in boxes_df.index]

    headers = {
        "Content-Type": "application/json",
        "Accept": "application/json",
        "X-Application-Id": TRAVELTIME_ID,
        "X-Api-Key": TRAVELTIME_KEY,
    }

    for start in tqdm(range(0, len(voters_df), batch_size), desc="TravelTime batches"):
        batch = voters_df.iloc[start : start + batch_size]

        # Voter points to include in this batch
        voter_locations = [
            {"id": f"voter{idx}", "coords": {"lat": row.lat, "lng": row.lng}} for idx, row in batch.iterrows()
        ]

        # One search per voter
        departure_searches = [
            {
                "id": f"search_{idx}",
                "departure_location_id": f"voter{idx}",
                "arrival_location_ids": box_ids,
                "transportation": {"type": "driving"},
                "departure_time": datetime.utcnow().isoformat() + "Z",
                "travel_time": 3600,
                "properties": ["travel_time"],
            }
            for idx in batch.index
        ]

        payload = {
            "locations": voter_locations + box_locations,
            "departure_searches": departure_searches,
        }

        try:
            response = requests.post(URL, headers=headers, json=payload, timeout=60)
            response.raise_for_status()
            data = response.json()["results"]
        except Exception as exc:
            print(f"Batch starting at row {start} failed: {exc}")
            continue

        for record in data:
            voter_idx = int(record["search_id"].replace("search_", ""))
            times = [p["travel_time"] for loc in record["locations"] for p in loc["properties"] if "travel_time" in p]
            min_time = min(times) if times else None

            results.append({
                "voter_index": voter_idx,
                "travel_minutes": (min_time / 60) if min_time else None,
            })

        time.sleep(0.25)  # modest throttle

    return pd.DataFrame(results)


# ---------------------------------------------------------------------------
# Merge helpers
# ---------------------------------------------------------------------------


def merge_back(voters_df, travel_df, nearest_df=None):
    """
    Merge travel time results (and optional nearest-box assignments) back
    into the full voter dataset.
    """

    merged = voters_df.reset_index().merge(travel_df, how="left", left_on="index", right_on="voter_index")

    if nearest_df is not None:
        merged = merged.merge(
            nearest_df[["voter_lat", "voter_lng", "nearest_box_id"]],
            how="left",
            left_on=["lat", "lng"],
            right_on=["voter_lat", "voter_lng"],
        )

    return merged


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def main():
    # Validate environment and inputs
    require_env(TRAVELTIME_ID, "TRAVELTIME_ID")
    require_env(TRAVELTIME_KEY, "TRAVELTIME_KEY")

    require_file(VOTERS_CSV, "Voter address file")
    require_file(BOXES_CSV, "Ballot box file")

    voters_df = pd.read_csv(VOTERS_CSV)
    boxes_df = pd.read_csv(BOXES_CSV)

    require_columns(voters_df, ["lat", "lng"], "voters_df")
    require_columns(boxes_df, ["lat", "lng"], "boxes_df")

    print("Deduplicating voter coordinates to reduce API cost...")
    dedup = voters_df.drop_duplicates(subset=["lat", "lng"]).reset_index(drop=True)

    print("Computing TravelTime minimum travel times...")
    travel_df = compute_min_travel_times(dedup, boxes_df)

    print("Merging results...")
    final_df = merge_back(voters_df, travel_df)

    try:
        final_df.to_csv(OUTPUT_CSV, index=False)
    except Exception as exc:
        raise RuntimeError(f"Failed to write output CSV: {exc}")

    print(f"Saved travel time results to {OUTPUT_CSV}")


# ---------------------------------------------------------------------------

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"ERROR: {e}", file=sys.stderr)
        sys.exit(1)
