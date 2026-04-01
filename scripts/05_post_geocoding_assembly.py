import os
import pandas as pd

original_df = pd.read_csv("data/processed/CD06-RegisteredVoters-2026-03-05-16619_validated.csv")
geocoded_df = pd.read_csv("data/geocoded/CD06-RegisteredVoters-2026-03-05-16619_geocoded.csv")

print(original_df.head())
print(geocoded_df.head())

addr_cols = ["street_address", "city", "state", "zip"]

print("Original duplicates on address key:", original_df.duplicated(subset=addr_cols).sum())

print("Geocoded duplicates on address key:", geocoded_df.duplicated(subset=addr_cols).sum())

orig_addr_counts = original_df.groupby(addr_cols).size().reset_index(name="n_original")
geo_addr_counts = geocoded_df.groupby(addr_cols).size().reset_index(name="n_geocoded")

problem_addresses = orig_addr_counts.merge(geo_addr_counts, on=addr_cols, how="inner")

problem_addresses = problem_addresses[(problem_addresses["n_original"] > 1) | (problem_addresses["n_geocoded"] > 1)]

print(problem_addresses.sort_values(["n_original", "n_geocoded"], ascending=False).head(20))

print("Missing lat:", geocoded_df["lat"].isna().sum())
print("Missing lng:", geocoded_df["lng"].isna().sum())

point_empty_count = (geocoded_df["geometry"] == "POINT EMPTY").sum()

print(f"POINT EMPTY count: {point_empty_count}")

non_or_count = (geocoded_df["state"] != "OR").sum()

print(f"Non-OR state count: {non_or_count}")

point_empty_df = geocoded_df[geocoded_df["geometry"].str.strip().eq("POINT EMPTY")].copy()

print("POINT EMPTY rows:", len(point_empty_df))

unique_buildings = point_empty_df["building_id"].nunique()

print("Unique building_ids (POINT EMPTY):", unique_buildings)

rows = len(point_empty_df)
unique_buildings = point_empty_df["building_id"].nunique()

print(f"Rows: {rows}")
print(f"Unique buildings: {unique_buildings}")
print(f"Avg rows per building: {rows / unique_buildings:.2f}")

# Merge on original index to preserve all rows
# merged_df = original_df.merge(
#     geocoded_df[
#         [
#             "street_address",
#             "city",
#             "state",
#             "zip",
#             "lat",
#             "lng",
#             "geocoding_source",
#             "geometry",
#             "address_id",
#             "building_id",
#         ]
#     ],
#     on=["street_address", "city", "state", "zip"],
#     how="left",
# )

# total_original = len(original_df)
# total_geocoded = len(geocoded_df)
# total_merged = len(merged_df)

# print(f"Total original rows: {total_original}")
# print(f"Total geocoded rows: {total_geocoded}")
# print(f"Total merged rows: {total_merged}")
