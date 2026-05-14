import os
import glob
import re
import pandas as pd
import geopandas as gpd

# ---------------------------------------------------------------------------
# CONFIG
# ---------------------------------------------------------------------------

VOTER_FILES = [
    "data/final/CD01-RegisteredVoters-2026-03-05-83648_final_geocoded.csv",
    "data/final/CD02-RegisteredVoters-2026-03-05-111836_final_geocoded.csv",
    "data/final/CD03-RegisteredVoters-2026-03-05-132915_final_geocoded.csv",
    "data/final/CD04-RegisteredVoters-2026-03-05-151313_final_geocoded.csv",
    "data/final/CD05-RegisteredVoters-2026-03-05-15366_final_geocoded.csv",
    "data/final/CD06-RegisteredVoters-2026-03-05-16619_final_geocoded.csv",
]

COUNTIES_GEOJSON = "data/BLM_OR_County_Boundaries_Polygon_Hub.geojson"
ISOCHRONE_DIR = "isochrones"
OUTPUT_CSV = "voters_kepler_ready.csv"

COUNTY_FIELD = "county"

# ---------------------------------------------------------------------------
# ISOCHRONE SELECTION
# Target: Tuesday 4pm (16:00) layer
# Driving thresholds:   10, 15, 20 min  → 3 separate ring files
# Transit thresholds:   15, 30, 45 min  → 3 separate ring files
# ---------------------------------------------------------------------------

# Update these to match your exact filenames
DRIVE_FILES = {
    "0-10min": "isochrones/driving_tuesday_18-00_10min.geojson",
    "10-15min": "isochrones/driving_tuesday_18-00_15min.geojson",
    "15-20min": "isochrones/driving_tuesday_18-00_20min.geojson",
}

TRANSIT_FILES = {
    "0-15min": "isochrones/public_transport_tuesday_18-00_15min.geojson",
    "15-30min": "isochrones/public_transport_tuesday_18-00_30min.geojson",
    "30-45min": "isochrones/public_transport_tuesday_18-00_45min.geojson",
}

# ---------------------------------------------------------------------------
# LOAD COUNTIES
# ---------------------------------------------------------------------------

print("Loading counties...")
counties_gdf = gpd.read_file(COUNTIES_GEOJSON).to_crs(epsg=4326)
counties_gdf = counties_gdf.rename(columns={"COUNTY_NAME": "county"})

# ---------------------------------------------------------------------------
# LOAD + COMBINE VOTERS
# ---------------------------------------------------------------------------

dfs = []
for f in VOTER_FILES:
    print(f"Loading {f}")
    df = pd.read_csv(f)
    df["source_file"] = os.path.basename(f)
    dfs.append(df)

voters_df = pd.concat(dfs, ignore_index=True)
voters_df = voters_df.dropna(subset=["latitude", "longitude"])
print(f"\nTotal voter rows: {len(voters_df)}")

# ---------------------------------------------------------------------------
# BUILD GEODATAFRAME
# ---------------------------------------------------------------------------

voters_gdf = gpd.GeoDataFrame(
    voters_df,
    geometry=gpd.points_from_xy(voters_df["longitude"], voters_df["latitude"]),
    crs="EPSG:4326",
)

# Assign county
voters_gdf = gpd.sjoin(voters_gdf, counties_gdf[[COUNTY_FIELD, "geometry"]], how="left", predicate="within").drop(
    columns=["index_right"]
)

print(f"Voters missing county: {voters_gdf[COUNTY_FIELD].isna().sum()}")

# ---------------------------------------------------------------------------
# TAGGING FUNCTION
# Since rings are non-overlapping, a simple sjoin per ring is safe.
# We process rings in order (smallest first) and assign on first match.
# ---------------------------------------------------------------------------


def tag_voters(voters_gdf, zone_files, output_col):
    """
    Tags each voter with the zone label of the isochrone ring they fall in.
    Processes rings in dict order — make sure dict is ordered smallest to largest.
    Voters not matched to any ring get 'Not covered'.
    """
    # Start with all voters untagged
    voters_gdf = voters_gdf.copy()
    voters_gdf[output_col] = "Not covered"

    for label, filepath in zone_files.items():
        if not os.path.exists(filepath):
            print(f"  ⚠️  File not found, skipping: {filepath}")
            continue

        print(f"  Processing zone '{label}' from {os.path.basename(filepath)}")
        iso = gpd.read_file(filepath).to_crs(epsg=4326)

        # Only attempt to tag still-untagged voters (avoids overlap issues)
        untagged_mask = voters_gdf[output_col] == "Not covered"
        untagged = voters_gdf[untagged_mask]

        if len(untagged) == 0:
            print("  All voters already tagged, stopping early.")
            break

        matched = gpd.sjoin(untagged[["geometry"]], iso[["geometry"]], how="inner", predicate="within")

        # Dedupe in case a voter somehow intersects multiple polygons in the same file
        matched = matched.loc[~matched.index.duplicated(keep="first")]

        voters_gdf.loc[matched.index, output_col] = label
        print(f"    → Tagged {len(matched)} voters as '{label}'")

    tagged_count = (voters_gdf[output_col] != "Not covered").sum()
    not_covered = (voters_gdf[output_col] == "Not covered").sum()
    print(f"\n  {output_col} summary: {tagged_count} tagged, {not_covered} not covered\n")

    return voters_gdf


# ---------------------------------------------------------------------------
# RUN TAGGING FOR DRIVE + TRANSIT
# ---------------------------------------------------------------------------

print("=== Tagging drive time zones ===")
voters_gdf = tag_voters(voters_gdf, DRIVE_FILES, "drive_zone")

print("=== Tagging transit time zones ===")
voters_gdf = tag_voters(voters_gdf, TRANSIT_FILES, "transit_zone")

# ---------------------------------------------------------------------------
# EXPORT FOR KEPLER
# Keep only the columns Kepler needs — drop geometry (lat/lon already present)
# ---------------------------------------------------------------------------

output_cols = ["latitude", "longitude", COUNTY_FIELD, "drive_zone", "transit_zone"]

# Include any voter ID / name fields if present
for optional_col in ["voter_id", "address_id", "name", "address"]:
    if optional_col in voters_gdf.columns:
        output_cols.append(optional_col)

output_df = voters_gdf[output_cols].copy()
output_df.to_csv(OUTPUT_CSV, index=False)

print(f"\n✅ Saved Kepler-ready file to: {OUTPUT_CSV}")
print(f"   Rows: {len(output_df)}")
print(f"\nDrive zone breakdown:\n{output_df['drive_zone'].value_counts()}")
print(f"\nTransit zone breakdown:\n{output_df['transit_zone'].value_counts()}")
