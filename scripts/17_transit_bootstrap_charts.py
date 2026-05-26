import os

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
import numpy as np
import pandas as pd

# ---------------------------------------------------------------------------
# CONFIG
# ---------------------------------------------------------------------------

COVERAGE_CSV = "OR_transit_bootstrap_coverage.csv"
OUTPUT_DIR = "data/charts"

TRANSIT_COLOR = "#E3970A"
PLATEAU_COLOR = "#FF2D2D"
STATE_LINE_COLOR = "#ffffff"

# Transit thresholds in order — existing + new bootstrap points
TRANSIT_THRESHOLDS = [15, 30, 45, 60, 120, 180, 240]

# Plateau detection — flag when incremental growth drops below this %
PLATEAU_THRESHOLD_PCT = 1.0

TARGET_WEEKDAY = "Tuesday"
TARGET_TIME = "18:00"

os.makedirs(OUTPUT_DIR, exist_ok=True)

# ---------------------------------------------------------------------------
# LOAD + FILTER
# ---------------------------------------------------------------------------

print("Loading coverage summary...")
cov = pd.read_csv(COVERAGE_CSV)
cov = cov[cov["county"].str.upper() != "WALLA WALLA"]
cov["time_label"] = cov["weekday"] + " " + cov["time"]

# Filter to target time window, transit, statewide
transit_state = cov[
    (cov["mode"] == "public_transport")
    & (cov["weekday"] == TARGET_WEEKDAY)
    & (cov["time"] == TARGET_TIME)
    & (cov["group"] == "statewide")
].copy()

# Filter to available thresholds
transit_state = transit_state[transit_state["minutes"].isin(TRANSIT_THRESHOLDS)]
transit_state = transit_state.sort_values("minutes")

print(f"\nAvailable transit thresholds for {TARGET_WEEKDAY} {TARGET_TIME}:")
print(transit_state[["minutes", "covered", "total", "percent_covered"]].to_string(index=False))

# ---------------------------------------------------------------------------
# PLATEAU DETECTION
# ---------------------------------------------------------------------------

transit_state = transit_state.reset_index(drop=True)
transit_state["covered_prev"] = transit_state["covered"].shift(1)
transit_state["incremental_voters"] = transit_state["covered"] - transit_state["covered_prev"]
transit_state["incremental_pct"] = (100 * transit_state["incremental_voters"] / transit_state["total"]).round(2)

# Find plateau — first point where incremental growth drops below threshold
plateau_rows = transit_state[transit_state["incremental_pct"] < PLATEAU_THRESHOLD_PCT].dropna()

if len(plateau_rows) > 0:
    plateau_minutes = plateau_rows.iloc[0]["minutes"]
    plateau_covered = plateau_rows.iloc[0]["covered"]
    plateau_pct = plateau_rows.iloc[0]["percent_covered"]
    print(f"\nPlateau detected at: {plateau_minutes} min")
    print(f"  Transit-accessible voters: {int(plateau_covered):,} ({plateau_pct:.1f}%)")
else:
    plateau_minutes = None
    print(f"\nNo plateau detected — coverage still growing at {TRANSIT_THRESHOLDS[-1]} min")
    print("  Consider running longer isochrones")

# ---------------------------------------------------------------------------
# COUNTY LEVEL
# ---------------------------------------------------------------------------

transit_county = cov[
    (cov["mode"] == "public_transport")
    & (cov["weekday"] == TARGET_WEEKDAY)
    & (cov["time"] == TARGET_TIME)
    & (cov["group"] == "county")
].copy()
transit_county = transit_county[transit_county["minutes"].isin(TRANSIT_THRESHOLDS)]
transit_county = transit_county.sort_values(["county", "minutes"])

# Per-county plateau: max covered across all thresholds
county_max = (
    transit_county.groupby("county")
    .agg(
        max_covered=("covered", "max"),
        total=("total", "first"),
    )
    .reset_index()
)
county_max["transit_accessible_pct"] = (100 * county_max["max_covered"] / county_max["total"]).round(1)
county_max = county_max.sort_values("transit_accessible_pct", ascending=True)

# ---------------------------------------------------------------------------
# CHART 1: Statewide bootstrap curve — covered voters vs threshold
# ---------------------------------------------------------------------------

fig, ax1 = plt.subplots(figsize=(12, 6))
fig.patch.set_facecolor("#1a1a2e")
ax1.set_facecolor("#1a1a2e")

x = transit_state["minutes"].values
covered = transit_state["covered"].values
pcts = transit_state["percent_covered"].values

# Bar: incremental voters added at each step
incremental = transit_state["incremental_voters"].fillna(transit_state["covered"].iloc[0]).values
ax1.bar(range(len(x)), incremental, color=TRANSIT_COLOR, alpha=0.6, label="New voters added")

# Cumulative covered line on right axis
ax2 = ax1.twinx()
ax2.set_facecolor("#1a1a2e")
ax2.plot(
    range(len(x)),
    covered,
    marker="o",
    color=STATE_LINE_COLOR,
    linewidth=2.5,
    markersize=8,
    label="Cumulative covered",
    zorder=5,
)

for i, (c, p) in enumerate(zip(covered, pcts)):
    ax2.text(i, c + covered[-1] * 0.01, f"{int(c):,}\n({p:.1f}%)", ha="center", fontsize=7, color="white")

# Mark plateau
if plateau_minutes is not None:
    plateau_idx = list(x).index(plateau_minutes)
    ax2.axvline(x=plateau_idx, color=PLATEAU_COLOR, linewidth=2, linestyle="--", zorder=6)
    ax2.text(
        plateau_idx + 0.1,
        covered[0],
        f"Plateau\n{plateau_minutes} min\n{int(plateau_covered):,} voters",
        color=PLATEAU_COLOR,
        fontsize=8,
        va="bottom",
    )

ax1.set_xticks(range(len(x)))
ax1.set_xticklabels([f"{m} min" for m in x], color="white", fontsize=10)
ax1.set_ylabel("New Voters Added per Step", color=TRANSIT_COLOR, fontsize=11)
ax1.yaxis.set_major_formatter(mticker.FuncFormatter(lambda v, _: f"{int(v):,}"))
ax1.tick_params(axis="y", colors=TRANSIT_COLOR)
ax1.tick_params(axis="x", colors="white")
ax1.spines["bottom"].set_color("#444")
ax1.spines["left"].set_color(TRANSIT_COLOR)
ax1.spines["top"].set_visible(False)
ax1.spines["right"].set_color(STATE_LINE_COLOR)

ax2.set_ylabel("Cumulative Voters Covered", color=STATE_LINE_COLOR, fontsize=11)
ax2.yaxis.set_major_formatter(mticker.FuncFormatter(lambda v, _: f"{int(v):,}"))
ax2.tick_params(axis="y", colors=STATE_LINE_COLOR)
ax2.spines["right"].set_color(STATE_LINE_COLOR)
ax2.spines["top"].set_visible(False)

lines1, labels1 = ax1.get_legend_handles_labels()
lines2, labels2 = ax2.get_legend_handles_labels()
leg = ax1.legend(
    lines1 + lines2, labels1 + labels2, facecolor="#2a2a4e", labelcolor="white", fontsize=9, loc="upper left"
)
leg.get_frame().set_edgecolor("#444")

ax1.set_title(
    f"Statewide Transit Coverage Bootstrap — {TARGET_WEEKDAY} {TARGET_TIME}\n"
    f"(red line = plateau threshold: <{PLATEAU_THRESHOLD_PCT}% new voters per step)",
    color="white",
    fontsize=13,
    fontweight="bold",
    pad=15,
)

fig.subplots_adjust(bottom=0.12, top=0.88, left=0.1, right=0.9)
fig.savefig(f"{OUTPUT_DIR}/transit_bootstrap_statewide.png", dpi=150, facecolor=fig.get_facecolor())
plt.close(fig)
print("\nSaved: transit_bootstrap_statewide.png")

# ---------------------------------------------------------------------------
# CHART 2: All county lines muted + statewide avg prominent
# ---------------------------------------------------------------------------

pivot = transit_county.pivot_table(index="county", columns="minutes", values="percent_covered")[TRANSIT_THRESHOLDS]

fig, ax = plt.subplots(figsize=(13, 7))
fig.patch.set_facecolor("#1a1a2e")
ax.set_facecolor("#1a1a2e")

x_idx = list(range(len(TRANSIT_THRESHOLDS)))

# All counties muted grey
for county_name, row in pivot.iterrows():
    ax.plot(x_idx, row.values, color="#555577", linewidth=0.8, alpha=0.4)

# Statewide avg — prominent white line
ax.plot(
    x_idx,
    transit_state["percent_covered"].values,
    color="white",
    linewidth=3,
    linestyle="--",
    marker="D",
    markersize=8,
    label="Statewide avg",
    zorder=5,
)
for i, val in enumerate(transit_state["percent_covered"].values):
    ax.text(i, val + 2, f"{val:.1f}%", ha="center", fontsize=9, color="white", fontweight="bold")

if plateau_minutes is not None:
    plateau_idx = TRANSIT_THRESHOLDS.index(plateau_minutes)
    ax.axvline(x=plateau_idx, color=PLATEAU_COLOR, linewidth=2, linestyle="--", alpha=0.8)
    ax.text(plateau_idx + 0.05, 5, f"Plateau\n{plateau_minutes} min", color=PLATEAU_COLOR, fontsize=8)

ax.set_xticks(x_idx)
ax.set_xticklabels([f"{m} min" for m in TRANSIT_THRESHOLDS], color="white", fontsize=10)
ax.set_ylabel("% of Voters Covered", color="white", fontsize=11)
ax.yaxis.set_major_formatter(mticker.PercentFormatter())
ax.set_ylim(0, 110)
ax.set_xlim(-0.5, len(TRANSIT_THRESHOLDS) - 0.5)
ax.tick_params(colors="white")
ax.spines["bottom"].set_color("#444")
ax.spines["left"].set_color("#444")
ax.spines["top"].set_visible(False)
ax.spines["right"].set_visible(False)
ax.set_title(
    f"Transit Coverage Bootstrap — {TARGET_WEEKDAY} {TARGET_TIME}\n"
    f"(all counties shown, statewide average highlighted)",
    color="white",
    fontsize=13,
    fontweight="bold",
    pad=15,
)
leg = ax.legend(facecolor="#2a2a4e", labelcolor="white", fontsize=9, loc="upper left")
leg.get_frame().set_edgecolor("#444")

fig.subplots_adjust(bottom=0.12, top=0.88, left=0.09, right=0.95)
fig.savefig(f"{OUTPUT_DIR}/transit_bootstrap_county.png", dpi=150, facecolor=fig.get_facecolor())
plt.close(fig)
print("Saved: transit_bootstrap_county.png")

# ---------------------------------------------------------------------------
# CHART 3: County max transit accessible % — bar chart
# ---------------------------------------------------------------------------

n = len(county_max)
x_bar = np.arange(n)

fig, ax = plt.subplots(figsize=(18, 7))
fig.patch.set_facecolor("#1a1a2e")
ax.set_facecolor("#1a1a2e")

bars = ax.bar(x_bar, county_max["transit_accessible_pct"], color=TRANSIT_COLOR, alpha=0.85)

for bar in bars:
    h = bar.get_height()
    ax.text(
        bar.get_x() + bar.get_width() / 2, h + 0.3, f"{h:.0f}%", ha="center", va="bottom", fontsize=6, color="white"
    )

ax.set_xticks(x_bar)
ax.set_xticklabels(county_max["county"].str.title(), rotation=45, ha="right", fontsize=8, color="white")
ax.set_ylabel("Max % Voters Transit Accessible", color="white", fontsize=11)
ax.set_title(
    f"County Transit Accessible Population (Bootstrap Max) — {TARGET_WEEKDAY} {TARGET_TIME}",
    color="white",
    fontsize=14,
    fontweight="bold",
    pad=15,
)
ax.yaxis.set_major_formatter(mticker.PercentFormatter())
ax.set_ylim(0, 115)
ax.tick_params(colors="white")
ax.spines["bottom"].set_color("#444")
ax.spines["left"].set_color("#444")
ax.spines["top"].set_visible(False)
ax.spines["right"].set_visible(False)

plt.tight_layout()
fig.savefig(
    f"{OUTPUT_DIR}/transit_accessible_by_county.png", dpi=150, bbox_inches="tight", facecolor=fig.get_facecolor()
)
plt.close(fig)
print("Saved: transit_accessible_by_county.png")

# ---------------------------------------------------------------------------
# SAVE SUMMARY CSV
# ---------------------------------------------------------------------------

summary = county_max.rename(
    columns={
        "county": "County",
        "max_covered": "Transit Accessible Voters",
        "total": "Total Voters",
        "transit_accessible_pct": "Transit Accessible %",
    }
)
summary["County"] = summary["County"].str.title()
summary.to_csv(f"{OUTPUT_DIR}/transit_accessible_summary.csv", index=False)
print("Saved: transit_accessible_summary.csv")

# ---------------------------------------------------------------------------
# PRINT SUMMARY
# ---------------------------------------------------------------------------

print(f"\n{'=' * 60}")
print(f"TRANSIT ACCESSIBLE POPULATION SUMMARY — {TARGET_WEEKDAY} {TARGET_TIME}")
print(f"{'=' * 60}")
if plateau_minutes:
    total_voters = transit_state["total"].iloc[0]
    print(f"Plateau at:                {plateau_minutes} min")
    print(f"Transit accessible voters: {int(plateau_covered):,} of {int(total_voters):,}")
    print(f"Transit accessible %:      {plateau_pct:.1f}%")
else:
    print("No plateau detected — coverage still growing at max threshold")
print(f"{'=' * 60}")
print(f"\nAll charts saved to {OUTPUT_DIR}/")
print("  transit_bootstrap_statewide.png       — cumulative curve + incremental bars")
print("  transit_bootstrap_county.png          — county lines, statewide highlighted")
print("  transit_accessible_by_county.png      — max accessible % per county")
print("  transit_accessible_summary.csv        — county-level table")

# ---------------------------------------------------------------------------
# CHART 4: Top 15 counties — transit-accessible voters NOT covered within 45 min
# Mirrors the drive chart — actionable counties where transit exists but
# drop sites aren't well positioned
# ---------------------------------------------------------------------------

# Build transit accessible population per county from bootstrap data
county_transit_accessible = (
    transit_county.groupby("county")["covered"].max().reset_index().rename(columns={"covered": "transit_accessible"})
)

# Get transit coverage at 45 min for Tuesday 18:00, county level
transit_45 = cov[
    (cov["mode"] == "public_transport")
    & (cov["minutes"] == 45)
    & (cov["weekday"] == TARGET_WEEKDAY)
    & (cov["time"] == TARGET_TIME)
    & (cov["group"] == "county")
][["county", "covered", "total"]].copy()

# Merge with transit accessible population
transit_45 = transit_45.merge(county_transit_accessible[["county", "transit_accessible"]], on="county", how="left")

# Voters with transit access but NOT covered within 45 min
transit_45["transit_accessible_not_covered"] = (transit_45["transit_accessible"] - transit_45["covered"]).clip(lower=0)

# Coverage rate among transit-accessible voters
transit_45["transit_accessible_covered_pct"] = (
    (transit_45["covered"] / transit_45["transit_accessible"] * 100).clip(upper=100).round(1)
)

# Top 15 by uncovered transit-accessible voters
top15 = transit_45.nlargest(15, "transit_accessible_not_covered")

fig, ax1 = plt.subplots(figsize=(14, 7))
fig.patch.set_facecolor("#1a1a2e")
ax1.set_facecolor("#1a1a2e")

x = range(len(top15))
bars = ax1.bar(
    x,
    top15["transit_accessible_not_covered"],
    color=TRANSIT_COLOR,
    alpha=0.85,
    label="Transit-accessible voters not covered",
)

for bar, val in zip(bars, top15["transit_accessible_not_covered"]):
    ax1.text(
        bar.get_x() + bar.get_width() / 2,
        bar.get_height() + 30,
        f"{int(val):,}",
        ha="center",
        va="bottom",
        fontsize=8,
        color="white",
    )

ax1.set_xticks(x)
ax1.set_xticklabels(top15["county"].str.title(), rotation=30, ha="right", fontsize=9, color="white")
ax1.set_ylabel("Transit-Accessible Voters Not Covered", color=TRANSIT_COLOR, fontsize=11)
ax1.yaxis.set_major_formatter(mticker.FuncFormatter(lambda v, _: f"{int(v):,}"))
ax1.tick_params(axis="y", colors=TRANSIT_COLOR)
ax1.tick_params(axis="x", colors="white")
ax1.spines["bottom"].set_color("#444")
ax1.spines["left"].set_color(TRANSIT_COLOR)
ax1.spines["top"].set_visible(False)
ax1.spines["right"].set_color("#4B96D2")

# Right axis — % of transit-accessible voters covered
ax2 = ax1.twinx()
ax2.set_facecolor("#1a1a2e")
ax2.plot(
    x,
    top15["transit_accessible_covered_pct"],
    marker="o",
    color="#4B96D2",
    linewidth=2,
    markersize=7,
    label="% transit-accessible covered",
    zorder=5,
)

for i, val in enumerate(top15["transit_accessible_covered_pct"]):
    ax2.text(i, val - 4, f"{val:.1f}%", ha="center", fontsize=8, color="#4B96D2", fontweight="bold")

ax2.set_ylabel("% of Transit-Accessible Voters Covered", color="#4B96D2", fontsize=11)
ax2.yaxis.set_major_formatter(mticker.PercentFormatter())
ax2.tick_params(axis="y", colors="#4B96D2")
ax2.set_ylim(0, 110)
ax2.spines["right"].set_color("#4B96D2")
ax2.spines["top"].set_visible(False)

lines1, labels1 = ax1.get_legend_handles_labels()
lines2, labels2 = ax2.get_legend_handles_labels()
leg = ax1.legend(
    lines1 + lines2, labels1 + labels2, facecolor="#2a2a4e", labelcolor="white", fontsize=9, loc="upper right"
)
leg.get_frame().set_edgecolor("#444")

ax1.set_title(
    f"Counties Where Transit Exists But Drop Sites Are Underpositioned — Top 15\n"
    f"(transit-accessible voters not covered within 45 min, {TARGET_WEEKDAY} {TARGET_TIME})",
    color="white",
    fontsize=13,
    fontweight="bold",
    pad=15,
)

fig.subplots_adjust(bottom=0.18, top=0.88, left=0.1, right=0.92)
fig.savefig(f"{OUTPUT_DIR}/transit_underserved_top15.png", dpi=150, facecolor=fig.get_facecolor())
plt.close(fig)
print("  transit_underserved_top15.png         — top 15 actionable transit counties")
