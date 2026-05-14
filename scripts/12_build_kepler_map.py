import contextlib
import os

import geopandas as gpd
import pandas as pd
import pygris

from src.ballot_box_analysis.map import (
    BallotBoxLayer,
    IsochroneMap,
    KeplerField,
    KeplerGeojsonColumns,
    KeplerLayer,
    KeplerLayerConfig,
    KeplerPointColumns,
    KeplerVisConfig,
    TravelTimeRadiusLayer,
    VoterAddressLayer,
)

# ---------------------------------------------------------------------------
# CONFIG
# ---------------------------------------------------------------------------

BALLOT_BOXES_CSV = "data/geocoded/PE26_Oregon_Drop_Sites_20260324_geocoded.csv"
COUNTIES_GEOJSON = "data/BLM_OR_County_Boundaries_Polygon_Hub.geojson"
VOTERS_CSV = "voters_kepler_ready.csv"

OREGON_LAT = 44.0
OREGON_LNG = -120.5

# Per-zone sample fracs — weight 1 is largest so sample most aggressively
SAMPLE_FRACS = {
    1: 0.30,
    2: 0.50,
    3: 0.50,
    4: 0.50,  # not covered — show all
}

# Drive zone config — 3-step green + red
DRIVE_ZONE_CONFIG = {
    1: {"label": "Drive Zone 1 (≤10 min)", "color": [0, 255, 65], "radius": 3, "opacity": 0.7},
    2: {"label": "Drive Zone 2 (10-15 min)", "color": [76, 175, 80], "radius": 3, "opacity": 0.7},
    3: {"label": "Drive Zone 3 (15-20 min)", "color": [183, 228, 183], "radius": 3, "opacity": 0.7},
    4: {"label": "Drive Not Covered", "color": [255, 45, 45], "radius": 4, "opacity": 0.85},
}

# Transit zone config — 3-step blue + red
TRANSIT_ZONE_CONFIG = {
    1: {"label": "Transit Zone 1 (≤15 min)", "color": [0, 200, 255], "radius": 3, "opacity": 0.7},
    2: {"label": "Transit Zone 2 (15-30 min)", "color": [67, 148, 210], "radius": 3, "opacity": 0.7},
    3: {"label": "Transit Zone 3 (30-45 min)", "color": [183, 220, 240], "radius": 3, "opacity": 0.7},
    4: {"label": "Transit Not Covered", "color": [255, 45, 45], "radius": 4, "opacity": 0.85},
}

# ---------------------------------------------------------------------------
# LOAD VOTERS ONCE
# ---------------------------------------------------------------------------

print("Loading voters...")
voters_df = pd.read_csv(VOTERS_CSV)
voters_df = voters_df.drop_duplicates(subset=["address_id"])
voters_df = voters_df[voters_df["county"].str.upper() != "WALLA WALLA"]

drive_order = {"0-10min": 1, "10-15min": 2, "15-20min": 3, "Not covered": 4}
transit_order = {"0-15min": 1, "15-30min": 2, "30-45min": 3, "Not covered": 4}
voters_df["drive_zone_weight"] = voters_df["drive_zone"].map(drive_order)
voters_df["transit_zone_weight"] = voters_df["transit_zone"].map(transit_order)

print(f"  Total unique addresses: {len(voters_df):,}")

# ---------------------------------------------------------------------------
# LOAD STATIC LAYERS (shared between both maps)
# ---------------------------------------------------------------------------

print("Loading ballot boxes...")
boxes_df = pd.read_csv(BALLOT_BOXES_CSV)
boxes_gdf = gpd.GeoDataFrame(
    boxes_df,
    geometry=gpd.points_from_xy(boxes_df["lng"], boxes_df["lat"]),
    crs="EPSG:4326",
)

print("Loading Oregon state boundary...")
with contextlib.redirect_stdout(open(os.devnull, "w")):
    state_gdf = pygris.states(cb=True, cache=True)
state_gdf = state_gdf[state_gdf["STUSPS"] == "OR"].to_crs(epsg=4326)

print("Loading county boundaries...")
counties_gdf = gpd.read_file(COUNTIES_GEOJSON).to_crs(epsg=4326)
counties_gdf = counties_gdf.rename(columns={"COUNTY_NAME": "county"})
counties_gdf = gpd.clip(counties_gdf, state_gdf)
counties_gdf["label_lng"] = counties_gdf.geometry.centroid.x
counties_gdf["label_lat"] = counties_gdf.geometry.centroid.y

county_labels_gdf = gpd.GeoDataFrame(
    counties_gdf[["county", "label_lat", "label_lng"]].copy(),
    geometry=gpd.points_from_xy(counties_gdf["label_lng"], counties_gdf["label_lat"]),
    crs="EPSG:4326",
)

# ---------------------------------------------------------------------------
# HELPER: add a geojson layer
# ---------------------------------------------------------------------------


def add_layer(kepler_map, gdf, layer_id, color, vis_config, tooltip_cols, is_visible=True):
    kepler_map.config.visState.layers.insert(
        0,
        KeplerLayer(
            id=layer_id,
            type="geojson",
            config=KeplerLayerConfig(
                dataId=layer_id,
                label=layer_id,
                color=color,
                columns=KeplerGeojsonColumns(),
                isVisible=is_visible,
                visConfig=vis_config,
            ),
        ),
    )
    kepler_map.config.visState.interactionConfig.tooltip.fieldsToShow[layer_id] = [
        KeplerField(name=col) for col in tooltip_cols
    ]
    kepler_map._update_map_config()
    kepler_map.map.add_data(data=gdf.fillna(""), name=layer_id)


# ---------------------------------------------------------------------------
# CORE MAP BUILDER
# ---------------------------------------------------------------------------


def build_map(mode, zone_config, weight_col, output_html):
    """
    Build and export a Kepler map for a given mode.
    mode:        "drive" or "transit"
    zone_config: DRIVE_ZONE_CONFIG or TRANSIT_ZONE_CONFIG
    weight_col:  "drive_zone_weight" or "transit_zone_weight"
    """
    print(f"\n{'=' * 55}")
    print(f"Building {mode.upper()} map → {output_html}")
    print(f"{'=' * 55}")

    TOOLTIP_COLS = ["drive_zone", "transit_zone", "county"]

    # Split voters by zone weight
    zone_gdfs = {}
    for weight, cfg in zone_config.items():
        zone_df = voters_df[voters_df[weight_col] == weight].sample(frac=SAMPLE_FRACS[weight], random_state=42)
        zone_gdfs[weight] = gpd.GeoDataFrame(
            zone_df,
            geometry=gpd.points_from_xy(zone_df["longitude"], zone_df["latitude"]),
            crs="EPSG:4326",
        )
        print(f"  Weight {weight} ({cfg['label']}): {len(zone_gdfs[weight]):,}")

    # Empty primary layer to satisfy IsochroneMap interface
    empty_gdf = gpd.GeoDataFrame({"geometry": []}, crs="EPSG:4326")

    primary_layer = VoterAddressLayer(
        voter_addresses=empty_gdf,
        tooltip_cols=TOOLTIP_COLS,
        color=[0, 0, 0],
        vis_config=KeplerVisConfig(radius=0, opacity=0.0),
    )

    placeholder_iso = TravelTimeRadiusLayer(
        ballot_box_isochrones=gpd.GeoDataFrame({"geometry": [], "mode": [], "label": []}, crs="EPSG:4326"),
        tooltip_cols=["mode", "label"],
        is_visible=False,
    )

    ballot_box_layer = BallotBoxLayer(
        ballot_boxes=boxes_gdf,
        tooltip_cols=["Drop Site Name"],
    )

    kepler_map = IsochroneMap(
        county="Multnomah, OR",
        voter_address_layer=primary_layer,
        travel_time_radius_layer=placeholder_iso,
        ballot_box_layer=ballot_box_layer,
    )

    # Add zone layers — render order: 3 (bottom), 2, 1 (bright), 4/red (top)
    for weight in [3, 2, 1, 4]:
        cfg = zone_config[weight]
        add_layer(
            kepler_map,
            zone_gdfs[weight],
            layer_id=cfg["label"],
            color=cfg["color"],
            vis_config=KeplerVisConfig(
                radius=cfg["radius"],
                opacity=cfg["opacity"],
            ),
            tooltip_cols=TOOLTIP_COLS,
        )

    # County boundary
    add_layer(
        kepler_map,
        counties_gdf,
        layer_id="County Boundary",
        color=[255, 255, 255],
        vis_config=KeplerVisConfig(
            opacity=0.0,
            strokeOpacity=0.6,
            thickness=1.0,
            strokeColor=[200, 200, 200],
            stroked=True,
            filled=False,
        ),
        tooltip_cols=["county"],
    )

    # County labels
    kepler_map.config.visState.layers.insert(
        0,
        KeplerLayer(
            id="County Labels",
            type="point",
            config=KeplerLayerConfig(
                dataId="County Labels",
                label="County Labels",
                color=[255, 255, 255],
                columns=KeplerPointColumns(lat="label_lat", lng="label_lng"),
                isVisible=True,
                visConfig=KeplerVisConfig(radius=0, opacity=1.0),
            ),
        ),
    )
    kepler_map.config.visState.interactionConfig.tooltip.fieldsToShow["County Labels"] = [KeplerField(name="county")]
    kepler_map._update_map_config()
    kepler_map.map.add_data(data=county_labels_gdf.fillna(""), name="County Labels")

    # State boundary
    add_layer(
        kepler_map,
        state_gdf,
        layer_id="State Boundary",
        color=[255, 255, 255],
        vis_config=KeplerVisConfig(
            opacity=0.0,
            strokeOpacity=1.0,
            thickness=2.5,
            strokeColor=[255, 255, 255],
            stroked=True,
            filled=False,
        ),
        tooltip_cols=["NAME"],
    )

    # Center on Oregon
    kepler_map.config.mapState.latitude = OREGON_LAT
    kepler_map.config.mapState.longitude = OREGON_LNG
    kepler_map.config.mapState.zoom = 7
    kepler_map._update_map_config()

    print(f"Exporting to {output_html}...")
    kepler_map.export(output_html)
    print(f"Done! → {output_html}")


# ---------------------------------------------------------------------------
# BUILD BOTH MAPS
# ---------------------------------------------------------------------------

build_map(
    mode="drive",
    zone_config=DRIVE_ZONE_CONFIG,
    weight_col="drive_zone_weight",
    output_html="oregon_drive_access_map.html",
)

build_map(
    mode="transit",
    zone_config=TRANSIT_ZONE_CONFIG,
    weight_col="transit_zone_weight",
    output_html="oregon_transit_access_map.html",
)

print("\nAll done!")
print("  oregon_drive_access_map.html   — green zones by drive time")
print("  oregon_transit_access_map.html — blue zones by transit time")
