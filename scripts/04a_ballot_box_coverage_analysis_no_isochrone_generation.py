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
OUTPUT_CSV = "OR_coverage_summary_v2.csv"

COUNTY_FIELD = "county"


# ---------------------------------------------------------------------------
# LOAD COUNTIES (ONLY ONCE + RENAME)
# ---------------------------------------------------------------------------

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

# drop missing coords
voters_df = voters_df.dropna(subset=["latitude", "longitude"])

print(f"\nTotal voter rows: {len(voters_df)}")


# ---------------------------------------------------------------------------
# OPTIONAL DIAGNOSTIC
# ---------------------------------------------------------------------------

dupe_check = voters_df.groupby("address_id")[["latitude", "longitude"]].nunique().reset_index()
bad_dupes = dupe_check[(dupe_check["latitude"] > 1) | (dupe_check["longitude"] > 1)]

if len(bad_dupes) > 0:
    print(f"⚠️ Found {len(bad_dupes)} address_ids with conflicting coordinates")
else:
    print("No conflicting address coordinate issues found")


# ---------------------------------------------------------------------------
# GEO DATA
# ---------------------------------------------------------------------------

voters_gdf = gpd.GeoDataFrame(
    voters_df,
    geometry=gpd.points_from_xy(voters_df["longitude"], voters_df["latitude"]),
    crs="EPSG:4326",
)

# spatial join to assign county
voters_gdf = gpd.sjoin(voters_gdf, counties_gdf[[COUNTY_FIELD, "geometry"]], how="left", predicate="within").drop(
    columns=["index_right"]
)

missing_county = voters_gdf[COUNTY_FIELD].isna().sum()
print(f"Voters missing county: {missing_county}")


# ---------------------------------------------------------------------------
# PARSE ISOCHRONE METADATA
# ---------------------------------------------------------------------------


def parse_filename(filename):
    base = os.path.basename(filename).replace(".geojson", "")

    pattern = r"(?P<mode>\w+)_(?P<weekday>\w+)_(?P<time>\d{2}-\d{2})_(?P<minutes>\d+)min"
    match = re.match(pattern, base)

    if not match:
        raise ValueError(f"Bad filename format: {filename}")

    d = match.groupdict()
    d["time"] = d["time"].replace("-", ":")
    d["minutes"] = int(d["minutes"])

    return d


# ---------------------------------------------------------------------------
# COVERAGE FUNCTION (VOTER-BASED + FIXED)
# ---------------------------------------------------------------------------


def compute_coverage(voters, isochrones, meta):
    voters = voters.to_crs(epsg=4326)
    isochrones = isochrones.to_crs(epsg=4326)

    covered = gpd.sjoin(voters[[COUNTY_FIELD, "geometry"]], isochrones[["geometry"]], how="inner", predicate="within")

    # ---------------- DEBUG BEFORE DEDUPE ----------------
    print(f"\n[{meta}]")
    print("Rows after spatial join:", len(covered))
    print("Unique voters matched:", covered.index.nunique())

    # ---------------- FIX: DEDUPE VOTERS ----------------
    covered = covered.loc[~covered.index.duplicated(keep="first")]

    print("Rows after dedupe:", len(covered))

    # ---------------- COUNTY-LEVEL ----------------
    county_summary = (
        covered.groupby(COUNTY_FIELD)
        .size()
        .reset_index(name="covered")
        .merge(
            voters.groupby(COUNTY_FIELD).size().reset_index(name="total"),
            on=COUNTY_FIELD,
            how="right",
        )
    )

    county_summary["covered"] = county_summary["covered"].fillna(0)
    county_summary["percent_covered"] = 100 * county_summary["covered"] / county_summary["total"]

    # attach metadata
    for k, v in meta.items():
        county_summary[k] = v

    county_summary["group"] = "county"

    # ---------------- STATEWIDE ----------------
    total_row = pd.DataFrame({
        COUNTY_FIELD: ["ALL"],
        "covered": [len(covered)],
        "total": [len(voters)],
        "percent_covered": [100 * len(covered) / len(voters)],
        "group": ["statewide"],
        **meta,
    })

    return pd.concat([county_summary, total_row], ignore_index=True)


# ---------------------------------------------------------------------------
# PROCESS ISOCHRONES
# ---------------------------------------------------------------------------

results = []

iso_files = glob.glob(os.path.join(ISOCHRONE_DIR, "*.geojson"))

print(f"\nFound {len(iso_files)} isochrone files")

for f in iso_files:
    print(f"\nProcessing {os.path.basename(f)}")

    meta = parse_filename(f)
    isochrones = gpd.read_file(f)

    summary = compute_coverage(voters_gdf, isochrones, meta)
    results.append(summary)

final = pd.concat(results, ignore_index=True)

# reorder columns
cols = [
    COUNTY_FIELD,
    "group",
    "mode",
    "weekday",
    "time",
    "minutes",
    "covered",
    "total",
    "percent_covered",
]

final = final[cols]

final.to_csv(OUTPUT_CSV, index=False)

print(f"\nSaved coverage summary to {OUTPUT_CSV}")
