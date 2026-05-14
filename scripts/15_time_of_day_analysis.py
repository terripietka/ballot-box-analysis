import os

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
import pandas as pd

# ---------------------------------------------------------------------------
# CONFIG
# ---------------------------------------------------------------------------

COVERAGE_CSV = "OR_coverage_summary_v2.csv"
OUTPUT_DIR = "data/charts"

DRIVE_COLOR = "#4B96D2"
TRANSIT_COLOR = "#E3970A"

# All 4 time points including Monday
TIME_ORDER = ["Monday 18:00", "Tuesday 08:00", "Tuesday 13:00", "Tuesday 18:00"]
TIME_LABELS_SHORT = ["Mon 18:00", "Tue 08:00", "Tue 13:00", "Tue 18:00"]

# Drive: 3 shades of blue
DRIVE_COLORS = {10: "#a8d4f5", 15: "#4B96D2", 20: "#1a5a96"}

# Transit: 3 shades of amber
TRANSIT_COLORS = {15: "#f5d08a", 30: "#E3970A", 45: "#8a5500"}

os.makedirs(OUTPUT_DIR, exist_ok=True)

# ---------------------------------------------------------------------------
# LOAD DATA
# ---------------------------------------------------------------------------

print("Loading coverage summary...")
df = pd.read_csv(COVERAGE_CSV)
df["time_label"] = df["weekday"] + " " + df["time"]

x = list(range(len(TIME_ORDER)))


def style_ax(ax):
    """Apply consistent dark theme styling to an axis."""
    ax.set_facecolor("#1a1a2e")
    ax.yaxis.set_major_formatter(mticker.PercentFormatter())
    ax.set_ylim(0, 110)
    ax.set_xlim(-0.5, len(TIME_ORDER) - 0.5)
    ax.set_xticks(x)
    ax.set_xticklabels(TIME_LABELS_SHORT, color="white", fontsize=10)
    ax.set_ylabel("% of Voters Covered", color="white", fontsize=11)
    ax.tick_params(colors="white")
    ax.spines["bottom"].set_color("#444")
    ax.spines["left"].set_color("#444")
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)
    # Monday / Tuesday separator
    ax.axvline(x=0.5, color="#555", linewidth=0.8, linestyle=":")
    ax.text(0.25, 107, "Mon", color="#aaa", fontsize=8, ha="center")
    ax.text(2.0, 107, "Tuesday", color="#aaa", fontsize=8, ha="center")


# ---------------------------------------------------------------------------
# CHART 1: Statewide — drive, all 3 thresholds (10, 15, 20 min)
# ---------------------------------------------------------------------------

fig, ax = plt.subplots(figsize=(11, 6))
fig.patch.set_facecolor("#1a1a2e")

for minutes in [10, 15, 20]:
    series = (
        df[(df["mode"] == "driving") & (df["minutes"] == minutes) & (df["group"] == "statewide")]
        .set_index("time_label")["percent_covered"]
        .reindex(TIME_ORDER)
    )
    ax.plot(x, series.values, marker="o", linewidth=2.5, markersize=8,
            color=DRIVE_COLORS[minutes], label=f"Drive ≤{minutes} min")
    for i, val in enumerate(series.values):
        ax.text(i, val + 1.5, f"{val:.1f}%", ha="center", fontsize=8,
                color=DRIVE_COLORS[minutes])

style_ax(ax)
ax.set_title("Statewide Drive Coverage by Time of Day — All Thresholds",
             color="white", fontsize=14, fontweight="bold", pad=15)
ax.legend(facecolor="#2a2a4e", labelcolor="white", fontsize=10)

fig.subplots_adjust(bottom=0.12, top=0.9, left=0.1, right=0.95)
fig.savefig(f"{OUTPUT_DIR}/statewide_drive_all_thresholds.png", dpi=150,
            facecolor=fig.get_facecolor())
plt.close(fig)
print("Saved: statewide_drive_all_thresholds.png")

# ---------------------------------------------------------------------------
# CHART 2: Statewide — transit, all 3 thresholds (15, 30, 45 min)
# ---------------------------------------------------------------------------

fig, ax = plt.subplots(figsize=(11, 6))
fig.patch.set_facecolor("#1a1a2e")

for minutes in [15, 30, 45]:
    series = (
        df[(df["mode"] == "public_transport") & (df["minutes"] == minutes) & (df["group"] == "statewide")]
        .set_index("time_label")["percent_covered"]
        .reindex(TIME_ORDER)
    )
    ax.plot(x, series.values, marker="s", linewidth=2.5, markersize=8,
            color=TRANSIT_COLORS[minutes], label=f"Transit ≤{minutes} min")
    for i, val in enumerate(series.values):
        ax.text(i, val + 1.5, f"{val:.1f}%", ha="center", fontsize=8,
                color=TRANSIT_COLORS[minutes])

style_ax(ax)
ax.set_title("Statewide Transit Coverage by Time of Day — All Thresholds",
             color="white", fontsize=14, fontweight="bold", pad=15)
ax.legend(facecolor="#2a2a4e", labelcolor="white", fontsize=10)

fig.subplots_adjust(bottom=0.12, top=0.9, left=0.1, right=0.95)
fig.savefig(f"{OUTPUT_DIR}/statewide_transit_all_thresholds.png", dpi=150,
            facecolor=fig.get_facecolor())
plt.close(fig)
print("Saved: statewide_transit_all_thresholds.png")

# ---------------------------------------------------------------------------
# HELPER: county line chart with bottom N highlighted
# ---------------------------------------------------------------------------

def county_time_chart(mode, minutes, color, title, filename):
    mode_df = df[(df["mode"] == mode) & (df["minutes"] == minutes)].copy()

    # Pivot counties over time
    county_only = mode_df[mode_df["group"] == "county"]
    pivot = county_only.pivot_table(
        index="county", columns="time_label", values="percent_covered"
    ).reindex(columns=TIME_ORDER)

    pivot["mean_cov"] = pivot.mean(axis=1)
    pivot = pivot.sort_values("mean_cov").drop(columns="mean_cov")
    bottom10 = pivot.head(10)
    rest = pivot.iloc[10:]

    # Statewide line
    state_line = (
        mode_df[mode_df["group"] == "statewide"]
        .set_index("time_label")["percent_covered"]
        .reindex(TIME_ORDER)
    )

    fig, ax = plt.subplots(figsize=(13, 7))
    fig.patch.set_facecolor("#1a1a2e")

    # All other counties — muted grey
    for _, row in rest.iterrows():
        ax.plot(x, row.values, color="#555577", linewidth=0.8, alpha=0.45)

    # Bottom 10 — highlighted with distinct colors + end labels
    cmap = plt.cm.get_cmap("tab10", len(bottom10))
    for i, (county_name, row) in enumerate(bottom10.iterrows()):
        ax.plot(x, row.values, marker="o", linewidth=2, markersize=6,
                color=cmap(i), label=county_name.title())
        # Label at end of line
        ax.text(len(TIME_ORDER) - 0.85, row.values[-1], county_name.title(),
                fontsize=7, color=cmap(i), va="center")

    # Statewide avg — white dashed
    ax.plot(x, state_line.values, color="white", linewidth=2.5, linestyle="--",
            marker="D", markersize=7, label="Statewide avg", zorder=5)
    for i, val in enumerate(state_line.values):
        ax.text(i, val + 1.8, f"{val:.1f}%", ha="center", fontsize=8,
                color="white", fontweight="bold")

    style_ax(ax)
    ax.set_xlim(-0.5, len(TIME_ORDER) + 0.5)  # extra room for end labels
    ax.set_title(
        f"{title}\n(10 least covered counties highlighted)",
        color="white", fontsize=13, fontweight="bold", pad=15,
    )
    ax.legend(facecolor="#2a2a4e", labelcolor="white", fontsize=8,
              loc="lower left", title="Bottom 10 + Statewide", title_fontsize=8)

    fig.subplots_adjust(bottom=0.12, top=0.88, left=0.09, right=0.88)
    fig.savefig(f"{OUTPUT_DIR}/{filename}", dpi=150, facecolor=fig.get_facecolor())
    plt.close(fig)
    print(f"Saved: {filename}")


# ---------------------------------------------------------------------------
# CHART 3: County lines — drive ≤20 min
# ---------------------------------------------------------------------------

county_time_chart(
    mode="driving",
    minutes=20,
    color=DRIVE_COLOR,
    title="County Drive Coverage by Time of Day — ≤20 min",
    filename="county_drive_20min_by_time.png",
)

# ---------------------------------------------------------------------------
# CHART 4: County lines — transit ≤45 min
# ---------------------------------------------------------------------------

county_time_chart(
    mode="public_transport",
    minutes=45,
    color=TRANSIT_COLOR,
    title="County Transit Coverage by Time of Day — ≤45 min",
    filename="county_transit_45min_by_time.png",
)

print(f"\nAll time-of-day charts saved to {OUTPUT_DIR}/")
print("\nFiles:")
print("  statewide_drive_all_thresholds.png  — statewide drive, 3 threshold lines")
print("  statewide_transit_all_thresholds.png — statewide transit, 3 threshold lines")
print("  county_drive_20min_by_time.png      — county lines, drive ≤20min, bottom 10 highlighted")
print("  county_transit_45min_by_time.png    — county lines, transit ≤45min, bottom 10 highlighted")