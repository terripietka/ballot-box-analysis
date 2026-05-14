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

VOTERS_CSV = "voters_kepler_ready.csv"
COVERAGE_CSV = "OR_coverage_summary_v2.csv"
OUTPUT_DIR = "data/charts"

COVERED_COLOR = "#4BB543"
NOT_COVERED_COLOR = "#DC3C3C"
DRIVE_COLOR = "#4B96D2"
TRANSIT_COLOR = "#E3970A"

DRIVE_MINUTES = 20
TRANSIT_MINUTES = 45
COVERAGE_THRESHOLD = 95.0  # % — counties below this are highlighted

# Snapshot: Tuesday 18:00 used for all county bar charts and line charts
SNAPSHOT_WEEKDAY = "Tuesday"
SNAPSHOT_TIME = "18:00"

# All 4 time points for statewide line charts
TIME_ORDER_ALL = ["Monday 18:00", "Tuesday 8:00", "Tuesday 13:00", "Tuesday 18:00"]
TIME_LABELS_SHORT = ["Mon 18:00", "Tue 08:00", "Tue 13:00", "Tue 18:00"]

# Drive: 3 shades of blue
DRIVE_COLORS = {10: "#a8d4f5", 15: "#4B96D2", 20: "#1a5a96"}

# Transit: 3 shades of amber
TRANSIT_COLORS = {15: "#f5d08a", 30: "#E3970A", 45: "#8a5500"}

os.makedirs(OUTPUT_DIR, exist_ok=True)

# ---------------------------------------------------------------------------
# LOAD VOTER FILE
# ---------------------------------------------------------------------------

print("Loading voters...")
df = pd.read_csv(VOTERS_CSV)
df = df[df["county"].str.upper() != "WALLA WALLA"]
print(f"  Total voters: {len(df):,}")

# ---------------------------------------------------------------------------
# COVERAGE FLAGS
# ---------------------------------------------------------------------------

df["drive_covered"] = df["drive_zone"] != "Not covered"
df["transit_covered"] = df["transit_zone"] != "Not covered"
df["either_covered"] = df["drive_covered"] | df["transit_covered"]
df["neither_covered"] = ~df["either_covered"]
df["drive_only"] = df["drive_covered"] & ~df["transit_covered"]
df["transit_only"] = df["transit_covered"] & ~df["drive_covered"]
df["both_covered"] = df["drive_covered"] & df["transit_covered"]

total = len(df)

# ---------------------------------------------------------------------------
# STATEWIDE SUMMARY
# ---------------------------------------------------------------------------

statewide = pd.DataFrame([
    {"group": "Drive covered", "voters": df["drive_covered"].sum()},
    {"group": "Drive not covered", "voters": (~df["drive_covered"]).sum()},
    {"group": "Transit covered", "voters": df["transit_covered"].sum()},
    {"group": "Transit not covered", "voters": (~df["transit_covered"]).sum()},
    {"group": "Covered by both", "voters": df["both_covered"].sum()},
    {"group": "Drive only", "voters": df["drive_only"].sum()},
    {"group": "Transit only", "voters": df["transit_only"].sum()},
    {"group": "Neither covered", "voters": df["neither_covered"].sum()},
])
statewide["pct"] = (100 * statewide["voters"] / total).round(1)

print(f"\n{'=' * 55}")
print("STATEWIDE COVERAGE SUMMARY")
print(f"{'=' * 55}")
for _, row in statewide.iterrows():
    print(f"  {row['group']:<30} {row['voters']:>10,}  ({row['pct']}%)")
print(f"{'=' * 55}")

# ---------------------------------------------------------------------------
# LOAD COVERAGE CSV
# ---------------------------------------------------------------------------

print("\nLoading coverage summary...")
cov = pd.read_csv(COVERAGE_CSV)
cov["time_label"] = cov["weekday"] + " " + cov["time"]
cov = cov[cov["county"].str.upper() != "WALLA WALLA"] if "county" in cov.columns else cov

# Snapshot: Tuesday 18:00, county level
cov_snapshot = cov[
    (cov["weekday"] == SNAPSHOT_WEEKDAY) & (cov["time"] == SNAPSHOT_TIME) & (cov["group"] == "county")
].copy()

# ---------------------------------------------------------------------------
# COUNTY SUMMARY FROM VOTER FILE
# ---------------------------------------------------------------------------

county = (
    df.groupby("county")
    .agg(
        total_voters=("drive_covered", "count"),
        drive_covered=("drive_covered", "sum"),
        transit_covered=("transit_covered", "sum"),
        neither_covered=("neither_covered", "sum"),
        both_covered=("both_covered", "sum"),
    )
    .reset_index()
)
county["drive_pct"] = (100 * county["drive_covered"] / county["total_voters"]).round(1)
county["transit_pct"] = (100 * county["transit_covered"] / county["total_voters"]).round(1)
county["neither_pct"] = (100 * county["neither_covered"] / county["total_voters"]).round(1)
county["drive_not_covered"] = county["total_voters"] - county["drive_covered"]
county["transit_not_covered"] = county["total_voters"] - county["transit_covered"]

# Sort order: drive 20min coverage ascending (Tuesday 18:00 snapshot)
drive_20_snapshot = cov_snapshot[(cov_snapshot["mode"] == "driving") & (cov_snapshot["minutes"] == 20)][
    ["county", "percent_covered"]
].rename(columns={"percent_covered": "drive_20_pct"})

county_order = drive_20_snapshot.sort_values("drive_20_pct", ascending=True)["county"].tolist()

# Under-95% counties (from voter file summary, drive ≤20min)
under95_counties = county[county["drive_pct"] < COVERAGE_THRESHOLD]["county"].tolist()

print(f"\nCounties below {COVERAGE_THRESHOLD}% drive coverage: {len(under95_counties)}")
print(f"  {', '.join([c.title() for c in sorted(under95_counties)])}")

# ---------------------------------------------------------------------------
# SAVE SUMMARY CSVs
# ---------------------------------------------------------------------------

statewide.to_csv(f"{OUTPUT_DIR}/statewide_summary.csv", index=False)
county.to_csv(f"{OUTPUT_DIR}/county_summary.csv", index=False)
print(f"\nSaved summary CSVs to {OUTPUT_DIR}/")

# ---------------------------------------------------------------------------
# SHARED STYLING HELPERS
# ---------------------------------------------------------------------------


def style_ax_dark(ax):
    ax.set_facecolor("#1a1a2e")
    ax.tick_params(colors="white")
    ax.spines["bottom"].set_color("#444")
    ax.spines["left"].set_color("#444")
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)
    ax.yaxis.label.set_color("white")


def style_ax_time(ax):
    """Styling for time-of-day line charts with all 4 time points."""
    style_ax_dark(ax)
    ax.yaxis.set_major_formatter(mticker.PercentFormatter())
    ax.set_ylim(0, 110)
    ax.set_xlim(-0.5, len(TIME_ORDER_ALL) - 0.5)
    ax.set_xticks(range(len(TIME_ORDER_ALL)))
    ax.set_xticklabels(TIME_LABELS_SHORT, color="white", fontsize=10)
    ax.set_ylabel("% of Voters Covered", color="white", fontsize=11)
    ax.axvline(x=0.5, color="#555", linewidth=0.8, linestyle=":")
    ax.text(0.25, 107, "Mon", color="#aaa", fontsize=8, ha="center")
    ax.text(2.0, 107, "Tuesday", color="#aaa", fontsize=8, ha="center")


# ---------------------------------------------------------------------------
# CHART 1: County bar chart — drive thresholds 10/15/20 min (Tue 18:00)
# Under-95% counties highlighted with red x-tick labels
# ---------------------------------------------------------------------------


def county_threshold_bar_chart(mode, thresholds, color_map, title, filename, county_subset=None):
    order = county_subset if county_subset else county_order
    n_thresholds = len(thresholds)
    bar_width = 0.7 / n_thresholds
    x = np.arange(len(order))

    fig, ax = plt.subplots(figsize=(20, 8))
    fig.patch.set_facecolor("#1a1a2e")
    ax.set_facecolor("#1a1a2e")

    for i, minutes in enumerate(thresholds):
        data = (
            cov_snapshot[(cov_snapshot["mode"] == mode) & (cov_snapshot["minutes"] == minutes)]
            .set_index("county")["percent_covered"]
            .reindex(order)
            .fillna(0)
        )
        offset = (i - (n_thresholds - 1) / 2) * bar_width
        bars = ax.bar(
            x + offset, data.values, width=bar_width, color=color_map[minutes], label=f"≤{minutes} min", alpha=0.9
        )
        for bar in bars:
            h = bar.get_height()
            if h > 0:
                ax.text(
                    bar.get_x() + bar.get_width() / 2,
                    h + 0.5,
                    f"{h:.0f}%",
                    ha="center",
                    va="bottom",
                    fontsize=5,
                    color="white",
                )

    ax.set_xticks(x)
    xticklabels = ax.set_xticklabels([c.title() for c in order], rotation=45, ha="right", fontsize=8)
    for label, county_name in zip(xticklabels, order):
        label.set_color("#DC3C3C" if county_name.upper() in {c.upper() for c in under95_counties} else "white")

    ax.set_ylabel("% of Voters Covered", color="white", fontsize=11)
    title_suffix = "" if county_subset else f"\n(counties below {COVERAGE_THRESHOLD:.0f}% shown in red)"
    ax.set_title(f"{title}{title_suffix}", color="white", fontsize=14, fontweight="bold", pad=15)
    ax.yaxis.set_major_formatter(mticker.PercentFormatter())
    ax.set_ylim(0, 115)
    style_ax_dark(ax)
    ax.axhline(y=COVERAGE_THRESHOLD, color="#DC3C3C", linewidth=0.8, linestyle="--", alpha=0.6)
    ax.text(
        len(order) - 0.5,
        COVERAGE_THRESHOLD + 0.5,
        f"{COVERAGE_THRESHOLD:.0f}%",
        color="#DC3C3C",
        fontsize=8,
        ha="right",
    )
    leg = ax.legend(facecolor="#2a2a4e", labelcolor="white", fontsize=10)
    leg.get_frame().set_edgecolor("#444")

    plt.tight_layout()
    plt.savefig(f"{OUTPUT_DIR}/{filename}", dpi=150, bbox_inches="tight", facecolor=fig.get_facecolor())
    plt.close()
    print(f"Saved: {filename}")


county_threshold_bar_chart(
    mode="driving",
    thresholds=[10, 15, 20],
    color_map=DRIVE_COLORS,
    title="Voter Coverage by County — Drive Time (≤10 / ≤15 / ≤20 min) — Tuesday 18:00",
    filename="county_coverage_drive.png",
)

county_threshold_bar_chart(
    mode="public_transport",
    thresholds=[15, 30, 45],
    color_map=TRANSIT_COLORS,
    title="Voter Coverage by County — Transit Time (≤15 / ≤30 / ≤45 min) — Tuesday 18:00",
    filename="county_coverage_transit.png",
)

# ---------------------------------------------------------------------------
# CHART 1b: Under-95% counties only — drive thresholds
# ---------------------------------------------------------------------------

under95_order = [c for c in county_order if c in under95_counties]

county_threshold_bar_chart(
    mode="driving",
    thresholds=[10, 15, 20],
    color_map=DRIVE_COLORS,
    title=f"Counties Below {COVERAGE_THRESHOLD:.0f}% Drive Coverage — Drive Thresholds — Tuesday 18:00",
    filename="under95_county_coverage_drive.png",
    county_subset=under95_order,
)

# ---------------------------------------------------------------------------
# CHART 1c: Under-95% counties only — transit thresholds
# ---------------------------------------------------------------------------

county_threshold_bar_chart(
    mode="public_transport",
    thresholds=[15, 30, 45],
    color_map=TRANSIT_COLORS,
    title=f"Counties Below {COVERAGE_THRESHOLD:.0f}% Drive Coverage — Transit Thresholds — Tuesday 18:00",
    filename="under95_county_coverage_transit.png",
    county_subset=under95_order,
)

# ---------------------------------------------------------------------------
# CHART 3: Raw counts stacked — drive vs transit
# ---------------------------------------------------------------------------

county_merged = county.set_index("county").reindex(county_order).reset_index()
x = np.arange(len(county_order))
bar_width = 0.35

fig, ax = plt.subplots(figsize=(18, 8))
fig.patch.set_facecolor("#1a1a2e")
ax.set_facecolor("#1a1a2e")

ax.bar(
    x - bar_width / 2,
    county_merged["drive_covered"],
    width=bar_width,
    color=DRIVE_COLOR,
    label="Drive covered",
    alpha=0.9,
)
ax.bar(
    x - bar_width / 2,
    county_merged["drive_not_covered"],
    width=bar_width,
    bottom=county_merged["drive_covered"],
    color="#DC3C3C",
    label="Drive not covered",
    alpha=0.7,
)
ax.bar(
    x + bar_width / 2,
    county_merged["transit_covered"],
    width=bar_width,
    color=TRANSIT_COLOR,
    label="Transit covered",
    alpha=0.9,
)
ax.bar(
    x + bar_width / 2,
    county_merged["transit_not_covered"],
    width=bar_width,
    bottom=county_merged["transit_covered"],
    color="#b05000",
    label="Transit not covered",
    alpha=0.7,
)

ax.set_xticks(x)
xticklabels = ax.set_xticklabels([c.title() for c in county_order], rotation=45, ha="right", fontsize=8)
for label, county_name in zip(xticklabels, county_order):
    label.set_color("#DC3C3C" if county_name in under95_counties else "white")

ax.set_ylabel("Number of Voters", color="white", fontsize=11)
ax.set_title(
    "Voter Coverage by County — Raw Counts (Drive ≤20min vs Transit ≤45min)",
    color="white",
    fontsize=14,
    fontweight="bold",
    pad=15,
)
ax.yaxis.set_major_formatter(mticker.FuncFormatter(lambda v, _: f"{int(v):,}"))
style_ax_dark(ax)
leg = ax.legend(facecolor="#2a2a4e", labelcolor="white", fontsize=10)
leg.get_frame().set_edgecolor("#444")

plt.tight_layout()
plt.savefig(f"{OUTPUT_DIR}/county_coverage_counts.png", dpi=150, bbox_inches="tight", facecolor=fig.get_facecolor())
plt.close()
print("Saved: county_coverage_counts.png")

# ---------------------------------------------------------------------------
# CHART 4: Statewide horizontal bar
# ---------------------------------------------------------------------------

fig, ax = plt.subplots(figsize=(12, 5))
fig.patch.set_facecolor("#1a1a2e")
ax.set_facecolor("#1a1a2e")

categories = ["Drive", "Transit"]
covered_pcts = [100 * df["drive_covered"].sum() / total, 100 * df["transit_covered"].sum() / total]
not_covered_pcts = [100 - p for p in covered_pcts]
y = np.arange(len(categories))

ax.barh(y, covered_pcts, height=0.4, color=[DRIVE_COLOR, TRANSIT_COLOR], alpha=0.9, label="Covered")
ax.barh(y, not_covered_pcts, height=0.4, left=covered_pcts, color=NOT_COVERED_COLOR, alpha=0.7, label="Not Covered")

ax.set_yticks(y)
ax.set_yticklabels(categories, color="white", fontsize=12)
ax.set_xlabel("% of Voters", color="white", fontsize=11)
ax.set_title("Statewide Voter Coverage — Drive vs Transit", color="white", fontsize=14, fontweight="bold", pad=15)
ax.xaxis.set_major_formatter(mticker.PercentFormatter())
style_ax_dark(ax)
ax.spines["left"].set_color("#444")
leg = ax.legend(facecolor="#2a2a4e", labelcolor="white", fontsize=10)
leg.get_frame().set_edgecolor("#444")

for i, (cov_pct, ncov) in enumerate(zip(covered_pcts, not_covered_pcts)):
    ax.text(cov_pct / 2, i, f"{cov_pct:.1f}%", ha="center", va="center", color="white", fontsize=11, fontweight="bold")
    ax.text(
        cov_pct + ncov / 2, i, f"{ncov:.1f}%", ha="center", va="center", color="white", fontsize=11, fontweight="bold"
    )

plt.tight_layout()
plt.savefig(f"{OUTPUT_DIR}/statewide_coverage.png", dpi=150, bbox_inches="tight", facecolor=fig.get_facecolor())
plt.close()
print("Saved: statewide_coverage.png")

# ---------------------------------------------------------------------------
# CHART 5: Under-95% counties — dual axis (uncovered count + % line)
# ---------------------------------------------------------------------------

under95 = county[county["drive_pct"] < COVERAGE_THRESHOLD].sort_values("drive_not_covered", ascending=False)

fig, ax1 = plt.subplots(figsize=(14, 7))
fig.patch.set_facecolor("#1a1a2e")
ax1.set_facecolor("#1a1a2e")

bars = ax1.bar(
    range(len(under95)),
    under95["drive_not_covered"],
    color=NOT_COVERED_COLOR,
    alpha=0.85,
    label="Voters not covered (drive)",
)
for bar in bars:
    h = bar.get_height()
    ax1.text(
        bar.get_x() + bar.get_width() / 2, h + 30, f"{int(h):,}", ha="center", va="bottom", fontsize=8, color="white"
    )

ax1.set_xticks(range(len(under95)))
ax1.set_xticklabels(under95["county"].str.title(), rotation=35, ha="right", fontsize=9, color="white")
ax1.set_ylabel("Voters Not Covered (Drive)", color=NOT_COVERED_COLOR, fontsize=11)
ax1.yaxis.set_major_formatter(mticker.FuncFormatter(lambda v, _: f"{int(v):,}"))
ax1.tick_params(axis="y", colors=NOT_COVERED_COLOR)
ax1.tick_params(axis="x", colors="white")
ax1.spines["bottom"].set_color("#444")
ax1.spines["left"].set_color(NOT_COVERED_COLOR)
ax1.spines["top"].set_visible(False)
ax1.spines["right"].set_color(DRIVE_COLOR)

ax2 = ax1.twinx()
ax2.set_facecolor("#1a1a2e")
ax2.plot(
    range(len(under95)),
    under95["drive_pct"],
    marker="o",
    color=DRIVE_COLOR,
    linewidth=2,
    markersize=7,
    label="Drive % covered",
    zorder=5,
)
for i, val in enumerate(under95["drive_pct"]):
    ax2.text(i, val - 2.5, f"{val:.1f}%", ha="center", fontsize=8, color=DRIVE_COLOR, fontweight="bold")

ax2.set_ylabel("% of Voters Covered (Drive)", color=DRIVE_COLOR, fontsize=11)
ax2.yaxis.set_major_formatter(mticker.PercentFormatter())
ax2.tick_params(axis="y", colors=DRIVE_COLOR)
ax2.set_ylim(0, 100)
ax2.axhline(y=COVERAGE_THRESHOLD, color="#888", linewidth=1, linestyle="--")
ax2.text(
    len(under95) - 0.5,
    COVERAGE_THRESHOLD + 1,
    f"{COVERAGE_THRESHOLD:.0f}% threshold",
    color="#888",
    fontsize=8,
    ha="right",
)
ax2.spines["right"].set_color(DRIVE_COLOR)
ax2.spines["top"].set_visible(False)

lines1, labels1 = ax1.get_legend_handles_labels()
lines2, labels2 = ax2.get_legend_handles_labels()
leg = ax1.legend(
    lines1 + lines2, labels1 + labels2, facecolor="#2a2a4e", labelcolor="white", fontsize=9, loc="upper right"
)
leg.get_frame().set_edgecolor("#444")

ax1.set_title(
    f"Counties Below {COVERAGE_THRESHOLD:.0f}% Drive Coverage — Voters Affected vs % Covered",
    color="white",
    fontsize=14,
    fontweight="bold",
    pad=15,
)
fig.subplots_adjust(bottom=0.18, top=0.9, left=0.1, right=0.92)
fig.savefig(f"{OUTPUT_DIR}/under95_drive_dual_axis.png", dpi=150, facecolor=fig.get_facecolor())
plt.close(fig)
print("Saved: under95_drive_dual_axis.png")

# ---------------------------------------------------------------------------
# CHART 6: Under-95% stacked coverage breakdown
# ---------------------------------------------------------------------------

under95 = under95.copy()
under95["drive_only_seg"] = under95["drive_covered"] - under95["both_covered"]
under95["transit_only_seg"] = under95["transit_covered"] - under95["both_covered"]
under95_tv = under95.sort_values("total_voters", ascending=False)
x_u = np.arange(len(under95_tv))

fig, ax = plt.subplots(figsize=(14, 7))
fig.patch.set_facecolor("#1a1a2e")
ax.set_facecolor("#1a1a2e")

ax.bar(x_u, under95_tv["both_covered"], color="#4BB543", alpha=0.9, label="Covered by both")
ax.bar(
    x_u,
    under95_tv["drive_only_seg"],
    bottom=under95_tv["both_covered"],
    color=DRIVE_COLOR,
    alpha=0.9,
    label="Drive only",
)
ax.bar(
    x_u,
    under95_tv["transit_only_seg"],
    bottom=under95_tv["both_covered"] + under95_tv["drive_only_seg"],
    color=TRANSIT_COLOR,
    alpha=0.9,
    label="Transit only",
)
ax.bar(
    x_u,
    under95_tv["neither_covered"],
    bottom=under95_tv["both_covered"] + under95_tv["drive_only_seg"] + under95_tv["transit_only_seg"],
    color=NOT_COVERED_COLOR,
    alpha=0.9,
    label="Neither covered",
)

ax.set_xticks(x_u)
ax.set_xticklabels(under95_tv["county"].str.title(), rotation=35, ha="right", fontsize=9, color="white")
ax.set_ylabel("Number of Voters", color="white", fontsize=11)
ax.set_title(
    f"Coverage Breakdown — Counties Below {COVERAGE_THRESHOLD:.0f}% Drive Coverage",
    color="white",
    fontsize=14,
    fontweight="bold",
    pad=15,
)
ax.yaxis.set_major_formatter(mticker.FuncFormatter(lambda v, _: f"{int(v):,}"))
style_ax_dark(ax)
leg = ax.legend(facecolor="#2a2a4e", labelcolor="white", fontsize=9, loc="upper right")
leg.get_frame().set_edgecolor("#444")

fig.subplots_adjust(bottom=0.18, top=0.9, left=0.1, right=0.95)
fig.savefig(f"{OUTPUT_DIR}/under95_coverage_breakdown.png", dpi=150, facecolor=fig.get_facecolor())
plt.close(fig)
print("Saved: under95_coverage_breakdown.png")

# ---------------------------------------------------------------------------
# STATEWIDE TIME-OF-DAY CHARTS — all 4 time points
# ---------------------------------------------------------------------------

x_all = list(range(len(TIME_ORDER_ALL)))

# CHART 7: Statewide drive — all 3 thresholds
fig, ax = plt.subplots(figsize=(11, 6))
fig.patch.set_facecolor("#1a1a2e")

for minutes in [10, 15, 20]:
    series = (
        cov[(cov["mode"] == "driving") & (cov["minutes"] == minutes) & (cov["group"] == "statewide")]
        .set_index("time_label")["percent_covered"]
        .reindex(TIME_ORDER_ALL)
    )
    ax.plot(
        x_all,
        series.values,
        marker="o",
        linewidth=2.5,
        markersize=8,
        color=DRIVE_COLORS[minutes],
        label=f"Drive ≤{minutes} min",
    )
    for i, val in enumerate(series.values):
        ax.text(i, val + 1.5, f"{val:.1f}%", ha="center", fontsize=8, color=DRIVE_COLORS[minutes])

style_ax_time(ax)
ax.set_title(
    "Statewide Drive Coverage by Time of Day — All Thresholds", color="white", fontsize=14, fontweight="bold", pad=15
)
leg = ax.legend(facecolor="#2a2a4e", labelcolor="white", fontsize=10)
leg.get_frame().set_edgecolor("#444")

fig.subplots_adjust(bottom=0.12, top=0.9, left=0.1, right=0.95)
fig.savefig(f"{OUTPUT_DIR}/statewide_drive_all_thresholds.png", dpi=150, facecolor=fig.get_facecolor())
plt.close(fig)
print("Saved: statewide_drive_all_thresholds.png")

# CHART 8: Statewide transit — all 3 thresholds
fig, ax = plt.subplots(figsize=(11, 6))
fig.patch.set_facecolor("#1a1a2e")

for minutes in [15, 30, 45]:
    series = (
        cov[(cov["mode"] == "public_transport") & (cov["minutes"] == minutes) & (cov["group"] == "statewide")]
        .set_index("time_label")["percent_covered"]
        .reindex(TIME_ORDER_ALL)
    )
    ax.plot(
        x_all,
        series.values,
        marker="s",
        linewidth=2.5,
        markersize=8,
        color=TRANSIT_COLORS[minutes],
        label=f"Transit ≤{minutes} min",
    )
    for i, val in enumerate(series.values):
        ax.text(i, val + 1.5, f"{val:.1f}%", ha="center", fontsize=8, color=TRANSIT_COLORS[minutes])

style_ax_time(ax)
ax.set_title(
    "Statewide Transit Coverage by Time of Day — All Thresholds", color="white", fontsize=14, fontweight="bold", pad=15
)
leg = ax.legend(facecolor="#2a2a4e", labelcolor="white", fontsize=10)
leg.get_frame().set_edgecolor("#444")

fig.subplots_adjust(bottom=0.12, top=0.9, left=0.1, right=0.95)
fig.savefig(f"{OUTPUT_DIR}/statewide_transit_all_thresholds.png", dpi=150, facecolor=fig.get_facecolor())
plt.close(fig)
print("Saved: statewide_transit_all_thresholds.png")

# ---------------------------------------------------------------------------
# COUNTY TIME-OF-DAY CHARTS — under-95% highlighted, Tuesday 18:00 snapshot
# ---------------------------------------------------------------------------


def county_time_chart(mode, minutes, color, title, filename):
    mode_df = cov[(cov["mode"] == mode) & (cov["minutes"] == minutes)].copy()

    county_only = mode_df[mode_df["group"] == "county"]
    pivot = county_only.pivot_table(index="county", columns="time_label", values="percent_covered").reindex(
        columns=TIME_ORDER_ALL
    )

    # Sort by Tuesday 18:00 value ascending
    pivot["sort_val"] = pivot.get(f"{SNAPSHOT_WEEKDAY} {SNAPSHOT_TIME}", pivot.mean(axis=1))
    pivot = pivot.sort_values("sort_val").drop(columns="sort_val")

    # Normalise case for comparison — coverage CSV may differ from voter file
    under95_upper = {c.upper() for c in under95_counties}
    under95_idx = [c for c in pivot.index if c.upper() in under95_upper]
    rest_idx = [c for c in pivot.index if c.upper() not in under95_upper]

    state_line = (
        mode_df[mode_df["group"] == "statewide"].set_index("time_label")["percent_covered"].reindex(TIME_ORDER_ALL)
    )

    fig, ax = plt.subplots(figsize=(13, 7))
    fig.patch.set_facecolor("#1a1a2e")

    # All well-covered counties — muted
    for county_name in rest_idx:
        ax.plot(x_all, pivot.loc[county_name].values, color="#555577", linewidth=0.8, alpha=0.45)

    # Under-95% counties — highlighted with varied line styles to avoid color confusion
    cmap = plt.cm.get_cmap("tab10", 10)
    line_styles = ["-", "--", ":", "-.", "-", "--", ":", "-.", "-", "--", ":", "-.", "-", "--", ":"]

    for i, county_name in enumerate(under95_idx):
        row = pivot.loc[county_name]
        ax.plot(
            x_all,
            row.values,
            marker="o",
            linewidth=2,
            markersize=6,
            color=cmap(i % 10),
            linestyle=line_styles[i],
            label=county_name.title(),
        )

    # Statewide avg
    ax.plot(
        x_all,
        state_line.values,
        color="white",
        linewidth=2.5,
        linestyle="--",
        marker="D",
        markersize=7,
        label="Statewide avg",
        zorder=5,
    )
    for i, val in enumerate(state_line.values):
        ax.text(i, val + 1.8, f"{val:.1f}%", ha="center", fontsize=8, color="white", fontweight="bold")

    # 95% reference line
    ax.axhline(y=COVERAGE_THRESHOLD, color="#DC3C3C", linewidth=0.8, linestyle="--", alpha=0.6)
    ax.text(
        len(TIME_ORDER_ALL) - 0.5,
        COVERAGE_THRESHOLD + 1,
        f"{COVERAGE_THRESHOLD:.0f}%",
        color="#DC3C3C",
        fontsize=8,
        ha="right",
    )

    style_ax_time(ax)
    ax.set_xlim(-0.5, len(TIME_ORDER_ALL) - 0.5)  # no extra room needed
    ax.set_title(
        f"{title}\n(counties below {COVERAGE_THRESHOLD:.0f}% drive coverage highlighted)",
        color="white",
        fontsize=13,
        fontweight="bold",
        pad=15,
    )
    leg = ax.legend(
        facecolor="#2a2a4e",
        labelcolor="white",
        fontsize=8,
        loc="lower left",
        title=f"Under {COVERAGE_THRESHOLD:.0f}% + Statewide",
        title_fontsize=8,
    )
    leg.get_title().set_color("white")
    leg.get_frame().set_edgecolor("#444")

    fig.subplots_adjust(bottom=0.12, top=0.88, left=0.09, right=0.95)
    fig.savefig(f"{OUTPUT_DIR}/{filename}", dpi=150, facecolor=fig.get_facecolor())
    plt.close(fig)
    print(f"Saved: {filename}")


# CHART 9: County drive ≤20min by time
county_time_chart(
    mode="driving",
    minutes=20,
    color=DRIVE_COLOR,
    title="County Drive Coverage by Time of Day — ≤20 min",
    filename="county_drive_20min_by_time.png",
)

# CHART 10: County transit ≤45min by time
county_time_chart(
    mode="public_transport",
    minutes=45,
    color=TRANSIT_COLOR,
    title="County Transit Coverage by Time of Day — ≤45 min",
    filename="county_transit_45min_by_time.png",
)

# ---------------------------------------------------------------------------
# UNDER-95% SUMMARY TABLE
# ---------------------------------------------------------------------------

table = (
    county[county["drive_pct"] < COVERAGE_THRESHOLD]
    .sort_values("drive_not_covered", ascending=False)[
        ["county", "total_voters", "drive_pct", "drive_not_covered", "transit_pct", "neither_covered", "neither_pct"]
    ]
    .rename(
        columns={
            "county": "County",
            "total_voters": "Total Voters",
            "drive_pct": "Drive % Covered",
            "drive_not_covered": "Drive Uncovered",
            "transit_pct": "Transit % Covered",
            "neither_covered": "Neither Covered",
            "neither_pct": "Neither %",
        }
    )
)
table["County"] = table["County"].str.title()

print(f"\n{'=' * 75}")
print(f"COUNTIES BELOW {COVERAGE_THRESHOLD:.0f}% DRIVE COVERAGE")
print(f"{'=' * 75}")
print(table.to_string(index=False))
print(f"{'=' * 75}")
print(f"TOTAL drive uncovered: {table['Drive Uncovered'].sum():,}")
print(f"TOTAL neither covered: {table['Neither Covered'].sum():,}")

table.to_csv(f"{OUTPUT_DIR}/under95_county_focus.csv", index=False)
print(f"\nSaved: under95_county_focus.csv")

# ---------------------------------------------------------------------------
# DONE
# ---------------------------------------------------------------------------

print(f"\nAll charts saved to {OUTPUT_DIR}/")
print("\nSnapshot charts (Tuesday 18:00):")
print("  county_coverage_drive.png               — drive ≤10/15/20 min per county")
print("  county_coverage_transit.png             — transit ≤15/30/45 min per county")
print("  county_coverage_counts.png              — raw counts stacked")
print("  statewide_coverage.png                  — statewide overview")
print("  under95_drive_dual_axis.png             — uncovered count + % line")
print("  under95_coverage_breakdown.png          — drive/transit/neither breakdown")
print("\nTime-of-day charts (Mon 18:00 + Tue 08:00/13:00/18:00):")
print("  statewide_drive_all_thresholds.png      — statewide drive, 3 thresholds")
print("  statewide_transit_all_thresholds.png    — statewide transit, 3 thresholds")
print("  county_drive_20min_by_time.png          — county lines drive, under-95% highlighted")
print("  county_transit_45min_by_time.png        — county lines transit, under-95% highlighted")
print("\nTables:")
print("  statewide_summary.csv")
print("  county_summary.csv")
print("  under95_county_focus.csv")
