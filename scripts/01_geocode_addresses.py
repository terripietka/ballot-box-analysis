"""
Geocode voter or address records using the Geocoder class.

This script:
1. Loads an addresses CSV.
2. Uses environment variables (loaded from .env) to authenticate the geocoding client.
3. Runs batch geocoding.
4. Writes a CSV containing latitude, longitude, and any additional metadata.

Usage:
    python geocode_addresses.py

Before running:
    - Ensure you have a `.env` file in the same directory with required API keys.
    - Set INPUT_FILE and OUTPUT_FILE paths below or override via CLI.
"""

import os
import sys
import pandas as pd
from dotenv import load_dotenv

from geocode import Geocoder


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

# Paths should be either edited here or provided via environment variables.
INPUT_FILE = os.environ.get("GEOCODE_INPUT", "")
OUTPUT_FILE = os.environ.get("GEOCODE_OUTPUT", "")

# Load environment variables (.env file must exist in working directory)
load_dotenv()


# ---------------------------------------------------------------------------
# Validation helpers
# ---------------------------------------------------------------------------


def require_file(path: str, description: str) -> None:
    """Check that a file path is provided and exists."""
    if not path:
        raise ValueError(f"{description} path is empty. Set INPUT_FILE/OUTPUT_FILE or use environment variables.")
    if not os.path.exists(path):
        raise FileNotFoundError(f"{description} not found: {path}")


def require_columns(df: pd.DataFrame, required_cols) -> None:
    """Ensure all required columns are present before geocoding."""
    missing = [col for col in required_cols if col not in df.columns]
    if missing:
        raise ValueError(f"Input file is missing required columns: {missing}")


# ---------------------------------------------------------------------------
# Main logic
# ---------------------------------------------------------------------------


def main():
    # Validate file paths
    require_file(INPUT_FILE, "Input CSV")
    if not OUTPUT_FILE:
        raise ValueError("Output path not set. Define OUTPUT_FILE or GEOCODE_OUTPUT.")

    # Load addresses
    try:
        df = pd.read_csv(INPUT_FILE)
    except Exception as exc:
        raise RuntimeError(f"Failed to read input CSV: {exc}")

    # Verify required fields exist
    required = ["street_address", "city", "state", "zip"]
    require_columns(df, required)

    # Initialize geocoder
    try:
        geocoder = Geocoder(
            addresses_df=df,
            address_col="street_address",
            city_col="city",
            state_col="state",
            zip_col="zip",
            unit_col="unit",  # optional column
        )
    except Exception as exc:
        raise RuntimeError(f"Failed to initialize Geocoder: {exc}")

    # Run geocoding
    try:
        gdf = geocoder.geocode(batch_size=200, processes=100)
    except Exception as exc:
        raise RuntimeError(f"Geocoding failed: {exc}")

    # Save results
    try:
        gdf.to_csv(OUTPUT_FILE, index=False)
    except Exception as exc:
        raise RuntimeError(f"Failed to write geocoded output: {exc}")

    print(f"Geocoding complete. Saved to {OUTPUT_FILE}")


# ---------------------------------------------------------------------------

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"ERROR: {e}", file=sys.stderr)
        sys.exit(1)
