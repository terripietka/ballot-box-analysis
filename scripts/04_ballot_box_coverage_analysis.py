"""
Compute coverage statistics for ballot drop box isochrones.

This script:
    1. Loads ballot box locations and geocoded voter addresses.
    2. Generates isochrones for multiple weekday/time scenarios.
    3. Computes percent coverage by precinct (or another grouping field).
    4. Saves isochrone GeoJSONs and a combined coverage summary CSV.

Inputs required:
    - boxes_df must contain ["lat", "lng", "street_address"]
    - voters_df must contain ["lat", "lng", "precinct", "address_id"]

Dependencies:
    - ballot_box_analysis.isochrone.IsochroneGenerator
    - GeoPandas
    - pandas

Before running:
    - Set BOXES_CSV, VOTERS_CSV, and OUTPUT_CSV paths below.
"""

import os
import sys
import pandas as pd
import geopandas as gpd
from ballot_box_analysis.isochrone import IsochroneGenerator


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

BOXES_CSV = ""  # path to ballot box CSV
VOTERS_CSV = ""  # path to geocoded voter addresses CSV (must include precinct)
OUTPUT_CSV = ""  # path to combined summary CSV
OUTPUT_DIR = "isochrones"  # directory to save generated GeoJSON files


# ---------------------------------------------------------------------------
# Validation helpers
# ---------------------------------------------------------------------------


def require_file(path, description):
    if not path:
        raise ValueError(f"{description} path not set.")
    if not os.path.exists(path):
        raise FileNotFoundError(f"{description} not found: {path}")


def require_columns(df, columns, df_name):
    missing = [col for col in columns if col not in df.columns]
    if missing:
        raise ValueError(f"{df_name} missing required columns: {missing}")


# ---------------------------------------------------------------------------
# Coverage computation functions
# ---------------------------------------------------------------------------


def compute_nested_coverage(voters_gdf, isochrones, minutes):
    """
    Compute percent coverage per precinct for nested precinct-only analysis.
    """
    voters_gdf = voters_gdf.to_crs(epsg=4326)
    isochrones = isochrones.to_crs(epsg=4326)

    base = voters_gdf[["address_id", "precinct", "geometry"]].dropna(subset=["geometry", "precinct"])
    joined = gpd.sjoin(base, isochrones[["geometry"]], how="inner", predicate="within")

    nested_summary = (
        joined.groupby("precinct")["address_id"]
        .nunique()
        .reset_index(name="covered")
        .merge(
            base.groupby("precinct")["address_id"].nunique().reset_index(name="total"),
            on="precinct",
            how="right",
        )
    )

    nested_summary["covered"] = nested_summary["covered"].fillna(0)
    nested_summary["percent_covered"] = 100 * nested_summary["covered"] / nested_summary["total"]
    nested_summary["minutes"] = minutes

    return nested_summary


def compute_coverage(voters_gdf, isochrones, minutes, group_field):
    """
    Compute coverage by any grouping field (e.g., precinct) plus a total row.
    """
    voters_gdf = voters_gdf.to_crs(epsg=4326)
    isochrones = isochrones.to_crs(epsg=4326)

    base = voters_gdf[["address_id", group_field, "geometry"]].dropna(subset=["geometry", group_field])

    joined = gpd.sjoin(base, isochrones[["geometry"]], how="inner", predicate="within")

    group_summary = (
        joined.groupby(group_field)["address_id"]
        .nunique()
        .reset_index(name="covered")
        .merge(
            base.groupby(group_field)["address_id"].nunique().reset_index(name="total"),
            on=group_field,
            how="right",
        )
    )

    group_summary["covered"] = group_summary["covered"].fillna(0)
    group_summary["percent_covered"] = 100 * group_summary["covered"] / group_summary["total"]
    group_summary["minutes"] = minutes

    total_row = pd.DataFrame({
        group_field: ["ALL"],
        "covered": [group_summary["covered"].sum()],
        "total": [group_summary["total"].sum()],
        "percent_covered": [100 * group_summary["covered"].sum() / group_summary["total"].sum()],
        "minutes": [minutes],
    })

    return pd.concat([group_summary, total_row], ignore_index=True)


# ---------------------------------------------------------------------------
# Main pipeline
# ---------------------------------------------------------------------------


def main():
    # Validate inputs
    require_file(BOXES_CSV, "Ballot box CSV")
    require_file(VOTERS_CSV, "Voter address CSV")
    if not OUTPUT_CSV:
        raise ValueError("OUTPUT_CSV path not set.")

    os.makedirs(OUTPUT_DIR, exist_ok=True)

    boxes_df = pd.read_csv(BOXES_CSV)
    voters_df = pd.read_csv(VOTERS_CSV)

    require_columns(boxes_df, ["lat", "lng", "street_address"], "boxes_df")
    require_columns(voters_df, ["lat", "lng", "precinct", "address_id"], "voters_df")

    # Convert voters to GeoDataFrame
    voters_gdf = gpd.GeoDataFrame(
        voters_df,
        geometry=gpd.points_from_xy(voters_df["lng"], voters_df["lat"]),
        crs="EPSG:4326",
    )

    scenarios = [
        ("Monday", "08:00"),
        ("Monday", "20:00"),
        ("Tuesday", "08:00"),
        ("Tuesday", "11:00"),
        ("Tuesday", "14:00"),
        ("Tuesday", "17:00"),
        ("Tuesday", "20:00"),
    ]

    minutes_list = [5, 10, 15, 20, 45, 90]

    summary_rows = []

    for weekday, time_str in scenarios:
        box_gen = IsochroneGenerator.from_pandas(
            boxes_df,
            lat_col="lat",
            lng_col="lng",
            name_or_id_col="street_address",
        )
        box_gen.set_travel_type("driving")
        box_gen.set_arrival_time(weekday, time_str, "America/Los_Angeles")

        for minutes in minutes_list:
            isochrones = box_gen.generate_isochrones(travel_minutes=minutes)

            filename = os.path.join(
                OUTPUT_DIR,
                f"isochrones_{weekday}_{time_str.replace(':', '-')}_{minutes}.geojson",
            )
            try:
                isochrones.to_file(filename, driver="GeoJSON")
            except Exception as exc:
                raise RuntimeError(f"Failed to save isochrone file {filename}: {exc}")

            precinct_summary = compute_coverage(voters_gdf, isochrones, minutes, "precinct")
            precinct_summary["Weekday"] = weekday
            precinct_summary["Time"] = time_str
            precinct_summary["GroupType"] = "PRECINCT"

            summary_rows.append(precinct_summary)

            total_row = precinct_summary.loc[precinct_summary["precinct"] == "ALL"].iloc[0]

            print(
                f"{weekday} {time_str}, {minutes} min: "
                f"{int(total_row.covered)}/{int(total_row.total)} "
                f"({total_row.percent_covered:.2f}%)"
            )

    final = pd.concat(summary_rows, ignore_index=True)

    try:
        final.to_csv(OUTPUT_CSV, index=False)
    except Exception as exc:
        raise RuntimeError(f"Failed to write output summary CSV: {exc}")

    print(f"Saved combined precinct coverage summary to {OUTPUT_CSV}")


# ---------------------------------------------------------------------------

if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        sys.exit(1)
