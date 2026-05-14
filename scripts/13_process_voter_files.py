import pandas as pd

# ---------------------------------------------------------------------------
# CONFIG
# ---------------------------------------------------------------------------

VOTERS_CSV = "voters_kepler_ready.csv"
OUTPUT_DIR = "data/kepler"

# ---------------------------------------------------------------------------
# LOAD + DEDUPE
# ---------------------------------------------------------------------------

print("Loading voters...")
df = pd.read_csv(VOTERS_CSV)
print(f"  Raw rows: {len(df)}")

df = df.drop_duplicates(subset=["address_id"])
print(f"  After dedupe: {len(df)}")

# ---------------------------------------------------------------------------
# ADD NUMERIC HEATMAP WEIGHTS
# Higher = further from ballot box = "hotter" on heatmap
# ---------------------------------------------------------------------------

drive_order = {"0-10min": 1, "10-15min": 2, "15-20min": 3, "Not covered": 4}
transit_order = {"0-15min": 1, "15-30min": 2, "30-45min": 3, "Not covered": 4}

df["drive_zone_weight"] = df["drive_zone"].map(drive_order)
df["transit_zone_weight"] = df["transit_zone"].map(transit_order)

# ---------------------------------------------------------------------------
# SPLIT INTO COVERAGE GROUPS
# ---------------------------------------------------------------------------

drive_covered = df[df["drive_zone"] != "Not covered"].copy()
drive_not_covered = df[df["drive_zone"] == "Not covered"].copy()

transit_covered = df[df["transit_zone"] != "Not covered"].copy()
transit_not_covered = df[df["transit_zone"] == "Not covered"].copy()

# ---------------------------------------------------------------------------
# CROSS-CHECK
# Voters not covered by drive but ARE covered by transit — and vice versa
# These are the most analytically interesting groups
# ---------------------------------------------------------------------------

# Not reachable by car but reachable by transit
drive_no_transit_yes = df[(df["drive_zone"] == "Not covered") & (df["transit_zone"] != "Not covered")].copy()

# Not reachable by transit but reachable by car
drive_yes_transit_no = df[(df["drive_zone"] != "Not covered") & (df["transit_zone"] == "Not covered")].copy()

# Not covered by either — most underserved voters
neither_covered = df[(df["drive_zone"] == "Not covered") & (df["transit_zone"] == "Not covered")].copy()

# Covered by both
both_covered = df[(df["drive_zone"] != "Not covered") & (df["transit_zone"] != "Not covered")].copy()

# ---------------------------------------------------------------------------
# PRINT SUMMARY
# ---------------------------------------------------------------------------

total = len(df)

print(f"\n{'=' * 55}")
print(f"COVERAGE SUMMARY (unique addresses)")
print(f"{'=' * 55}")
print(f"Total unique addresses:          {total:>10,}")
print(f"{'─' * 55}")
print(f"Drive covered:                   {len(drive_covered):>10,}  ({100 * len(drive_covered) / total:.1f}%)")
print(f"Drive NOT covered:               {len(drive_not_covered):>10,}  ({100 * len(drive_not_covered) / total:.1f}%)")
print(f"{'─' * 55}")
print(f"Transit covered:                 {len(transit_covered):>10,}  ({100 * len(transit_covered) / total:.1f}%)")
print(
    f"Transit NOT covered:             {len(transit_not_covered):>10,}  ({100 * len(transit_not_covered) / total:.1f}%)"
)
print(f"{'─' * 55}")
print(f"Covered by BOTH:                 {len(both_covered):>10,}  ({100 * len(both_covered) / total:.1f}%)")
print(
    f"Drive only (no transit):         {len(drive_yes_transit_no):>10,}  ({100 * len(drive_yes_transit_no) / total:.1f}%)"
)
print(
    f"Transit only (no drive):         {len(drive_no_transit_yes):>10,}  ({100 * len(drive_no_transit_yes) / total:.1f}%)"
)
print(f"NOT covered by either:           {len(neither_covered):>10,}  ({100 * len(neither_covered) / total:.1f}%)")
print(f"{'=' * 55}")

# ---------------------------------------------------------------------------
# SAVE FILES
# ---------------------------------------------------------------------------

import os

os.makedirs(OUTPUT_DIR, exist_ok=True)

files = {
    "voters_drive_covered.csv": drive_covered,
    "voters_drive_not_covered.csv": drive_not_covered,
    "voters_transit_covered.csv": transit_covered,
    "voters_transit_not_covered.csv": transit_not_covered,
    "voters_drive_no_transit_yes.csv": drive_no_transit_yes,
    "voters_drive_yes_transit_no.csv": drive_yes_transit_no,
    "voters_neither_covered.csv": neither_covered,
    "voters_both_covered.csv": both_covered,
}

print(f"\nSaving files to {OUTPUT_DIR}/")
for filename, data in files.items():
    path = os.path.join(OUTPUT_DIR, filename)
    data.to_csv(path, index=False)
    print(f"  {filename}: {len(data):,} rows")

print("\nDone!")
