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
OUTPUT_CSV = "OR_transit_bootstrap_coverage.csv"

COUNTY_FIELD = "county"

# Only process these — transit, Tuesday 18:00, all thresholds
TARGET_MODE = "public_transport"
TARGET_WEEKDAY = "Tuesday"
TARGET_TIME = "18:00"
TARGET_MINUTES = [15, 30, 45, 60, 120, 180, 240]

# Reduce if process is killed
CHUNK_SIZE = 100_000

# ---------------------------------------------------------------------------
# LOAD COUNTIES
# ---------------------------------------------------------------------------

print("Loading counties...")
counties_gdf = gpd.read_file(COUNTIES_GEOJSON).to_crs(epsg=4326)
counties_gdf = counties_gdf.rename(columns={"COUNTY_NAME": "county"})

# ---------------------------------------------------------------------------
# LOAD + COMBINE VOTERS
# ---------------------------------------------------------------------------

print("Loading voters...")
dfs = []
for f in VOTER_FILES:
    print(f"  {os.path.basename(f)}")
    df = pd.read_csv(f)
    dfs.append(df)

voters_df = pd.concat(dfs, ignore_index=True)
voters_df = voters_df.dropna(subset=["latitude", "longitude"])
print(f"  Total voter rows: {len(voters_df):,}")

voters_gdf = gpd.GeoDataFrame(
    voters_df,
    geometry=gpd.points_from_xy(voters_df["longitude"], voters_df["latitude"]),
    crs="EPSG:4326",
)

# Assign county
print("Assigning counties...")
voters_gdf = gpd.sjoin(voters_gdf, counties_gdf[[COUNTY_FIELD, "geometry"]], how="left", predicate="within").drop(
    columns=["index_right"]
)
print(f"  Voters missing county: {voters_gdf[COUNTY_FIELD].isna().sum():,}")

# ---------------------------------------------------------------------------
# PARSE FILENAME
# ---------------------------------------------------------------------------


def parse_filename(filename):
    base = os.path.basename(filename).replace(".geojson", "")
    pattern = r"(?P<mode>\w+)_(?P<weekday>\w+)_(?P<time>\d{2}-\d{2})_(?P<minutes>\d+)min"
    match = re.match(pattern, base)
    if not match:
        return None
    d = match.groupdict()
    d["time"] = d["time"].replace("-", ":")
    d["minutes"] = int(d["minutes"])
    return d


# ---------------------------------------------------------------------------
# CHUNKED SPATIAL JOIN
# ---------------------------------------------------------------------------


def chunked_spatial_join(voters, isochrones, chunk_size=CHUNK_SIZE):
    """
    Joins voters to isochrones in chunks to avoid RAM exhaustion.
    Returns a set of voter indices that fall within any isochrone polygon.
    """
    covered_indices = set()
    n = len(voters)
    n_chunks = (n // chunk_size) + 1

    for i in range(n_chunks):
        chunk = voters.iloc[i * chunk_size : (i + 1) * chunk_size]
        if len(chunk) == 0:
            continue

        try:
            joined = gpd.sjoin(
                chunk[[COUNTY_FIELD, "geometry"]],
                isochrones[["geometry"]],
                how="inner",
                predicate="within",
            )
            covered_indices.update(joined.index.tolist())
        except Exception as e:
            print(f"    ⚠️ Chunk {i + 1}/{n_chunks} error: {e}")
            continue

        if (i + 1) % 5 == 0 or (i + 1) == n_chunks:
            print(f"    Chunk {i + 1}/{n_chunks} done — covered so far: {len(covered_indices):,}")

    return covered_indices


# ---------------------------------------------------------------------------
# COVERAGE FUNCTION
# ---------------------------------------------------------------------------


def compute_coverage(voters, isochrones, minutes):
    voters = voters.to_crs(epsg=4326)
    isochrones = isochrones.to_crs(epsg=4326)

    print(f"  Running chunked spatial join...")
    covered_indices = chunked_spatial_join(voters, isochrones)
    covered = voters.loc[list(covered_indices)]
    print(f"  Covered voters: {len(covered):,} of {len(voters):,} ({100 * len(covered) / len(voters):.2f}%)")

    # County-level
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
    county_summary["mode"] = TARGET_MODE
    county_summary["weekday"] = TARGET_WEEKDAY
    county_summary["time"] = TARGET_TIME
    county_summary["minutes"] = minutes
    county_summary["group"] = "county"

    # Statewide
    total_row = pd.DataFrame({
        COUNTY_FIELD: ["ALL"],
        "covered": [len(covered)],
        "total": [len(voters)],
        "percent_covered": [100 * len(covered) / len(voters)],
        "mode": [TARGET_MODE],
        "weekday": [TARGET_WEEKDAY],
        "time": [TARGET_TIME],
        "minutes": [minutes],
        "group": ["statewide"],
    })

    return pd.concat([county_summary, total_row], ignore_index=True)


# ---------------------------------------------------------------------------
# FIND + FILTER TARGET ISOCHRONE FILES
# ---------------------------------------------------------------------------

all_files = sorted(glob.glob(os.path.join(ISOCHRONE_DIR, "*.geojson")))

target_files = []
for f in all_files:
    meta = parse_filename(f)
    if meta is None:
        continue
    if (
        meta["mode"] == TARGET_MODE
        and meta["weekday"].lower() == TARGET_WEEKDAY.lower()
        and meta["time"] == TARGET_TIME
        and meta["minutes"] in TARGET_MINUTES
    ):
        target_files.append((meta["minutes"], f))

target_files = sorted(target_files, key=lambda x: x[0])

print(f"\nFound {len(target_files)} matching isochrone files:")
for minutes, f in target_files:
    print(f"  {minutes} min → {os.path.basename(f)}")

if len(target_files) == 0:
    print("No matching files found — check ISOCHRONE_DIR and filename format.")
    exit(1)

# ---------------------------------------------------------------------------
# PROCESS
# ---------------------------------------------------------------------------

results = []

for minutes, f in target_files:
    print(f"\n{'=' * 55}")
    print(f"Processing {minutes} min → {os.path.basename(f)}")
    print(f"{'=' * 55}")

    isochrones = gpd.read_file(f)
    summary = compute_coverage(voters_gdf, isochrones, minutes)
    results.append(summary)

# ---------------------------------------------------------------------------
# SAVE
# ---------------------------------------------------------------------------

final = pd.concat(results, ignore_index=True)

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

print(f"\n{'=' * 55}")
print(f"Saved to {OUTPUT_CSV}")
print(f"Rows: {len(final):,}")
print(f"\nStatewide summary:")
statewide = final[final["group"] == "statewide"][["minutes", "covered", "total", "percent_covered"]].sort_values(
    "minutes"
)
print(statewide.to_string(index=False))
