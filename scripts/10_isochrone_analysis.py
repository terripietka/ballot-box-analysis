import os
import pandas as pd
from dateutil import parser

from ballot_box_analysis.isochrone import IsochroneGenerator


# ---------------------------------------------------------------------------
# CONFIG
# ---------------------------------------------------------------------------

BOXES_CSV = "data/geocoded/PE26_Oregon_Drop_Sites_20260324_geocoded.csv"
OUTPUT_DIR = "isochrones"

TRAVEL_TYPE = "public_transport"  # "driving" or "public_transport"

ARRIVAL_WEEKDAY = "Monday"
ARRIVAL_TIME = "18:00"  # change per run (08:00, 13:00, 18:00, etc.)

# MINUTES = [10, 15, 20]  # adjust per mode if needed
MINUTES = [15, 30, 45]  # adjust per mode if needed

TIMEZONE = "America/Los_Angeles"


# ---------------------------------------------------------------------------
# MAIN
# ---------------------------------------------------------------------------

os.makedirs(OUTPUT_DIR, exist_ok=True)

# Load data
boxes_df = pd.read_csv(BOXES_CSV)

# Initialize generator
gen = IsochroneGenerator.from_pandas(
    boxes_df,
    lat_col="lat",
    lng_col="lng",
    name_or_id_col="Drop Site Name",  # adjust if needed
)

gen.set_travel_type(TRAVEL_TYPE)
gen.set_arrival_time(ARRIVAL_WEEKDAY, ARRIVAL_TIME, TIMEZONE)

# Parse time for filename
parsed_time = parser.parse(gen.arrival_time_iso)
weekday = parsed_time.strftime("%A")
time_str = parsed_time.strftime("%H-%M")

# Run isochrones
for minutes in MINUTES:
    print(f"{TRAVEL_TYPE} | {weekday} {time_str} | {minutes} min")

    isochrones = gen.generate_isochrones(travel_minutes=minutes)

    filename = f"{OUTPUT_DIR}/" f"{TRAVEL_TYPE}_" f"{weekday}_" f"{time_str}_" f"{minutes}min.geojson"

    isochrones.to_file(filename, driver="GeoJSON")

    print(f"Saved: {filename}")


print("Done.")
