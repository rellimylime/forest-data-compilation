# ==============================================================================
# docs/dashboard/scripts/build_static_figures.py
# Build GitHub-hostable static PNG figures AND compact JSON/CSV aggregates from
# local FIA summary parquets. The aggregates let the Streamlit dashboard render
# interactive Plotly charts even when the source parquets are not available
# (e.g. on a hosted deployment that only has the contents of this repo).
# ==============================================================================

from __future__ import annotations

import json
from pathlib import Path

import matplotlib

matplotlib.use("Agg")

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd


REPO_ROOT = Path(__file__).resolve().parents[3]
SUMM_DIR = REPO_ROOT / "05_fia" / "data" / "processed" / "summaries"
OUT_DIR = REPO_ROOT / "docs" / "dashboard" / "static" / "figures" / "fia"
DATA_DIR = REPO_ROOT / "docs" / "dashboard" / "static" / "data" / "fia"
MANIFEST_PATH = REPO_ROOT / "docs" / "dashboard" / "static" / "metadata" / "static_figure_manifest.json"
SOURCE_SCRIPT = "docs/dashboard/scripts/build_static_figures.py"

BG = "#0d1a12"
PANEL = "#111f17"
BORDER = "#2a4035"
GRID = "#1e3024"
TEXT = "#d4e8da"
MUTED = "#8aab94"
GREEN = "#7bbf92"
BLUE = "#90b8d0"
GOLD = "#d4aa64"
RED = "#e15759"
PURPLE = "#b07aa1"

DIST_COLORS = {
    "fire": RED,
    "insects": "#59a14f",
    "disease": "#f28e2b",
    "weather": "#4e79a7",
    "animal": "#9c755f",
    "vegetation": "#76b7b2",
    "geologic": "#bab0ac",
    "other": "#8c8c8c",
}
AGENT_COLORS = {
    "bark beetles": RED,
    "defoliators": "#59a14f",
    "sucking insects": "#76b7b2",
    "boring insects": "#edc948",
    "insects": "#b6d77a",
    "root/butt disease": "#f28e2b",
    "canker/rust": "#ff9da7",
    "foliage/wilt disease": "#9c755f",
    "disease": "#ffbf7f",
    "fire": RED,
    "complex": PURPLE,
    "abiotic": "#4e79a7",
    "human": "#bab0ac",
    "other": "#8c8c8c",
    "unknown": "#555555",
}

MANIFEST: list[dict[str, str]] = []
DATA_MANIFEST: list[dict[str, str]] = []


def configure_theme() -> None:
    plt.rcParams.update(
        {
            "figure.facecolor": BG,
            "axes.facecolor": PANEL,
            "axes.edgecolor": BORDER,
            "axes.labelcolor": MUTED,
            "axes.titlecolor": TEXT,
            "xtick.color": MUTED,
            "ytick.color": MUTED,
            "text.color": TEXT,
            "font.family": "DejaVu Sans",
            "font.size": 10,
            "axes.titleweight": "bold",
            "axes.grid": True,
            "grid.color": GRID,
            "grid.linewidth": 0.8,
        }
    )


def read_summary(file_name: str, columns: list[str] | None = None) -> pd.DataFrame | None:
    path = SUMM_DIR / file_name
    if not path.is_file():
        print(f"skip missing: {path}")
        return None
    return pd.read_parquet(path, columns=columns)


def save(fig: plt.Figure, file_name: str, title: str, description: str) -> None:
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    fig.savefig(
        OUT_DIR / file_name,
        dpi=180,
        bbox_inches="tight",
        facecolor=BG,
        edgecolor=BG,
    )
    plt.close(fig)
    MANIFEST.append(
        {
            "file": file_name,
            "title": title,
            "description": description,
            "path": f"docs/dashboard/static/figures/fia/{file_name}",
            "source_script": SOURCE_SCRIPT,
        }
    )
    print(f"wrote figure {file_name}")


def save_json(name: str, payload: dict | list, description: str) -> None:
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    path = DATA_DIR / f"{name}.json"
    path.write_text(json.dumps(payload, indent=2, allow_nan=False), encoding="utf-8")
    DATA_MANIFEST.append(
        {
            "name": name,
            "kind": "json",
            "description": description,
            "path": f"docs/dashboard/static/data/fia/{name}.json",
            "source_script": SOURCE_SCRIPT,
        }
    )
    print(f"wrote data {name}.json")


def save_csv(name: str, df: pd.DataFrame, description: str) -> None:
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    path = DATA_DIR / f"{name}.csv"
    df.to_csv(path, index=False, float_format="%.4f")
    DATA_MANIFEST.append(
        {
            "name": name,
            "kind": "csv",
            "description": description,
            "rows": int(len(df)),
            "path": f"docs/dashboard/static/data/fia/{name}.csv",
            "source_script": SOURCE_SCRIPT,
        }
    )
    print(f"wrote data {name}.csv ({len(df):,} rows)")


def sample_frame(df: pd.DataFrame, n: int = 60000) -> pd.DataFrame:
    if len(df) <= n:
        return df
    return df.sample(n, random_state=42)


def conus(df: pd.DataFrame) -> pd.DataFrame:
    return df[
        df["LON"].between(-125, -66)
        & df["LAT"].between(24, 50)
    ]


def style_map_axis(ax: plt.Axes, title: str) -> None:
    ax.set_title(title, loc="left", pad=10)
    ax.set_xlim(-125, -66)
    ax.set_ylim(24, 50)
    ax.set_xlabel("Longitude")
    ax.set_ylabel("Latitude")
    ax.grid(True, alpha=0.7)
    for spine in ax.spines.values():
        spine.set_color(BORDER)


def annotate_bars(ax: plt.Axes, values: pd.Series, suffix: str = "") -> None:
    xmax = max(float(values.max()), 1.0)
    for idx, value in enumerate(values):
        ax.text(
            float(value) + xmax * 0.015,
            idx,
            f"{value:,.0f}{suffix}",
            va="center",
            ha="left",
            color=MUTED,
            fontsize=9,
        )


def histogram_payload(values: pd.Series, bins: int) -> dict:
    counts, edges = np.histogram(values.to_numpy(), bins=bins)
    return {
        "bin_edges": edges.tolist(),
        "bin_centers": ((edges[:-1] + edges[1:]) / 2).tolist(),
        "counts": counts.tolist(),
        "n": int(values.shape[0]),
        "median": float(values.median()),
    }


# ---- per-figure builders ----------------------------------------------------


def plot_flag_rates() -> None:
    df = read_summary(
        "plot_exclusion_flags.parquet",
        columns=[
            "pct_forested",
            "exclude_nonforest",
            "exclude_human_dist",
            "exclude_harvest",
            "exclude_harvest_agent",
            "has_fire",
            "has_insect",
            "exclude_any",
        ],
    )
    if df is None:
        return
    fields = {
        "exclude_any": ("Any exclusion", "OR of all four", RED),
        "exclude_nonforest": ("Nonforest condition", "COND_STATUS_CD = 5", RED),
        "exclude_human_dist": ("Human disturbance", "DSTRBCD = 80", "#f28e2b"),
        "exclude_harvest": ("Harvest", "TRTCD = 10", "#4e79a7"),
        "exclude_harvest_agent": ("Harvest (agent code)", "AGENTCD 80-89", "#a371f7"),
        "has_fire": ("Fire present", "DSTRBCD 30/31/32", RED),
        "has_insect": ("Insect present", "DSTRBCD 10/11/12", "#59a14f"),
    }
    rows = []
    for key, (label, basis, color) in fields.items():
        series = df[key]
        if series.notna().sum() == 0:
            continue
        rows.append({
            "flag": key,
            "label": label,
            "basis": basis,
            "color": color,
            "rate_pct": float(series.mean() * 100),
        })
    save_json("flag_rates", rows, "Plot-filter and disturbance flag rates (percent of plot visits).")

    # PNG fallback
    rates = pd.Series({row["label"]: row["rate_pct"] for row in rows}).sort_values()
    colors = [row["color"] for row in sorted(rows, key=lambda row: row["rate_pct"])]
    fig, ax = plt.subplots(figsize=(9.5, 4.8))
    ax.barh(rates.index, rates.values, color=colors, height=0.68)
    annotate_bars(ax, rates, "%")
    ax.set_xlabel("Percent of plot visits")
    ax.set_title("FIA plot filters and disturbance flags", loc="left")
    ax.set_xlim(0, max(10, float(rates.max()) * 1.22))
    save(fig, "flag_rates.png", "FIA plot filters and disturbance flags",
         "Percent of plot visits flagged by exclusion and disturbance fields.")

    # pct_forested histogram aggregate
    pct = df["pct_forested"].dropna()
    if len(pct):
        save_json("pct_forested_hist", histogram_payload(pct, bins=30),
                  "pct_forested histogram (forested fraction per plot visit).")


def plot_plot_status_map() -> None:
    flags = read_summary("plot_exclusion_flags.parquet", columns=["PLT_CN", "exclude_any", "pct_forested"])
    trees = read_summary("plot_tree_metrics.parquet", columns=["PLT_CN", "LAT", "LON"])
    if flags is None or trees is None:
        return
    coords = trees.dropna(subset=["LAT", "LON"]).drop_duplicates("PLT_CN")
    df = flags.merge(coords, on="PLT_CN", how="left").dropna(subset=["LAT", "LON"])
    df = df[df["pct_forested"] >= 0.5]
    df = conus(df)
    df["status"] = np.where(df["exclude_any"], "Excluded", "Clean")

    sample = sample_frame(df[["LAT", "LON", "status"]], n=12000)
    save_csv("plot_status_points", sample.rename(columns={"LAT": "lat", "LON": "lon"}),
             "Sampled clean/excluded plot points for interactive plotting.")

    fig, ax = plt.subplots(figsize=(10, 5.6))
    for status, color in {"Clean": GREEN, "Excluded": RED}.items():
        part = sample[sample["status"] == status]
        ax.scatter(part["LON"], part["LAT"], s=4, c=color, alpha=0.45, linewidths=0, label=status)
    style_map_axis(ax, "Clean and excluded FIA plot locations")
    ax.legend(loc="lower left", frameon=True, facecolor=PANEL, edgecolor=BORDER, labelcolor=TEXT)
    save(fig, "plot_status_map.png", "Clean and excluded FIA plot locations",
         "Sampled FIA plot locations after joining plot filters to coordinates.")


def plot_locations_ba() -> None:
    df = read_summary("plot_tree_metrics.parquet",
                      columns=["LAT", "LON", "ba_live_total", "shannon_h_ba", "n_species_live", "state"])
    if df is None:
        return
    df = conus(df.dropna(subset=["LAT", "LON", "ba_live_total"]))
    df = df[df["ba_live_total"] > 0]
    sample = sample_frame(df, n=12000)
    out = sample[["LAT", "LON", "ba_live_total", "shannon_h_ba", "n_species_live"]].copy()
    out.columns = ["lat", "lon", "ba_live_total", "shannon_h_ba", "n_species_live"]
    save_csv("plot_tree_points", out,
             "Sampled FIA plot locations with live BA, Shannon H, and species richness for interactive maps.")

    vmax = sample["ba_live_total"].quantile(0.98)
    fig, ax = plt.subplots(figsize=(10, 5.6))
    pts = ax.scatter(
        sample["LON"], sample["LAT"],
        c=sample["ba_live_total"].clip(upper=vmax),
        cmap="viridis", s=4, alpha=0.55, linewidths=0,
    )
    style_map_axis(ax, "Live basal area at FIA plot locations")
    cbar = fig.colorbar(pts, ax=ax, fraction=0.035, pad=0.02)
    cbar.set_label("Live BA, clipped at 98th percentile")
    cbar.ax.yaxis.set_tick_params(color=MUTED)
    plt.setp(cbar.ax.get_yticklabels(), color=MUTED)
    save(fig, "plot_locations_ba.png", "Live basal area at FIA plot locations",
         "Sampled FIA plot locations colored by live basal area.")


def plot_tree_metric_distributions() -> None:
    df = read_summary("plot_tree_metrics.parquet",
                      columns=["ba_live_total", "shannon_h_ba",
                               "ba_live_softwood", "ba_live_hardwood",
                               "ba_live_sapling", "ba_live_intermediate", "ba_live_mature",
                               "state"])
    if df is None:
        return
    ba = df["ba_live_total"].dropna()
    ba = ba[(ba > 0) & (ba <= ba.quantile(0.99))]
    shannon = df["shannon_h_ba"].dropna()
    shannon = shannon[(shannon > 0) & (shannon <= shannon.quantile(0.99))]

    save_json("ba_live_hist", histogram_payload(ba, bins=42),
              "Live basal-area histogram (clipped at 99th percentile).")
    save_json("shannon_h_hist", histogram_payload(shannon, bins=38),
              "Shannon diversity histogram (BA-weighted, clipped at 99th percentile).")

    if "state" in df.columns:
        sw_hw = (df.dropna(subset=["state"])
                 .groupby("state")[["ba_live_softwood", "ba_live_hardwood"]]
                 .mean().reset_index()
                 .rename(columns={"ba_live_softwood": "softwood", "ba_live_hardwood": "hardwood"}))
        save_csv("state_softwood_hardwood_ba", sw_hw,
                 "Mean softwood/hardwood basal area per state.")

        size_cols = [c for c in ["ba_live_sapling", "ba_live_intermediate", "ba_live_mature"]
                     if c in df.columns]
        if size_cols:
            sz = (df.dropna(subset=["state"])
                  .groupby("state")[size_cols].mean().reset_index()
                  .rename(columns={"ba_live_sapling": "sapling",
                                   "ba_live_intermediate": "intermediate",
                                   "ba_live_mature": "mature"}))
            save_csv("state_size_class_ba", sz,
                     "Mean basal area per state by tree size class.")

    fig, axes = plt.subplots(1, 2, figsize=(11, 4.6))
    axes[0].hist(ba, bins=42, color=GREEN, edgecolor=BG, linewidth=0.5)
    axes[0].axvline(ba.median(), color=GOLD, linewidth=2)
    axes[0].set_title("Live basal area", loc="left")
    axes[0].set_xlabel("BA per acre")
    axes[0].set_ylabel("Plot visits")

    axes[1].hist(shannon, bins=38, color=BLUE, edgecolor=BG, linewidth=0.5)
    axes[1].axvline(shannon.median(), color=GOLD, linewidth=2)
    axes[1].set_title("BA-weighted Shannon diversity", loc="left")
    axes[1].set_xlabel("Shannon H")
    axes[1].set_ylabel("Plot visits")
    fig.suptitle("FIA forest structure distributions", x=0.08, y=1.02, ha="left",
                 fontsize=13, fontweight="bold")
    fig.tight_layout()
    save(fig, "tree_metric_distributions.png", "FIA forest structure distributions",
         "Live basal area and BA-weighted Shannon diversity distributions.")


def plot_disturbance_category_counts() -> None:
    df = read_summary("plot_disturbance_history.parquet", columns=["disturbance_category"])
    if df is None:
        return
    counts = df["disturbance_category"].fillna("unknown").value_counts().sort_values()
    rows = [{"category": str(cat), "count": int(cnt),
             "color": DIST_COLORS.get(str(cat), "#8c8c8c")}
            for cat, cnt in counts.items()]
    save_json("disturbance_category_counts", rows,
              "FIA disturbance-history record counts grouped by category.")

    fig, ax = plt.subplots(figsize=(8.5, 4.7))
    ax.barh(counts.index, counts.values,
            color=[DIST_COLORS.get(label, "#8c8c8c") for label in counts.index], height=0.68)
    annotate_bars(ax, counts)
    ax.set_xlabel("Records")
    ax.set_title("Disturbance events by category", loc="left")
    ax.set_xlim(0, float(counts.max()) * 1.22)
    save(fig, "disturbance_category_counts.png", "Disturbance events by category",
         "FIA disturbance-history records grouped into natural disturbance categories.")


def plot_fire_type_breakdown() -> None:
    df = read_summary("plot_disturbance_history.parquet",
                      columns=["disturbance_category", "disturbance_label"])
    if df is None:
        return
    fire = df[df["disturbance_category"] == "fire"]
    if fire.empty:
        return
    counts = fire["disturbance_label"].fillna("Fire").value_counts()
    rows = [{"label": str(label), "count": int(cnt)} for label, cnt in counts.items()]
    save_json("fire_type_breakdown", rows, "Fire-type record counts within FIA disturbance history.")

    fig, ax = plt.subplots(figsize=(5.8, 4.8))
    ax.pie(
        counts.values,
        labels=counts.index,
        startangle=90,
        counterclock=False,
        colors=[RED, "#ff9d9a", "#c85250", GOLD, PURPLE][: len(counts)],
        wedgeprops={"width": 0.42, "edgecolor": BG, "linewidth": 2},
        textprops={"color": TEXT, "fontsize": 9},
    )
    ax.text(0, 0, f"{counts.sum():,}\nrecords", ha="center", va="center", color=TEXT, fontsize=11)
    ax.set_title("Fire type breakdown", loc="left")
    save(fig, "fire_type_breakdown.png", "Fire type breakdown",
         "FIA fire disturbance records by disturbance label.")


def plot_disturbance_year_counts() -> None:
    df = read_summary("plot_disturbance_history.parquet",
                      columns=["DSTRBYR", "disturbance_category"])
    if df is None:
        return
    yr = pd.to_numeric(df["DSTRBYR"], errors="coerce")
    keep = yr.notna() & (yr >= 1950) & (yr <= 2025)
    sub = df.loc[keep].copy()
    sub["year"] = yr.loc[keep].astype(int)
    sub["category"] = sub["disturbance_category"].fillna("unknown")
    pivot = (sub.groupby(["year", "category"]).size().reset_index(name="count"))
    save_csv("disturbance_year_counts", pivot,
             "Year x category counts of FIA disturbance records (1950-2025).")


def plot_top_disturbance_types() -> None:
    df = read_summary("plot_disturbance_history.parquet",
                      columns=["disturbance_label", "disturbance_category"])
    if df is None:
        return
    counts = (
        df.groupby(["disturbance_label", "disturbance_category"]).size()
        .reset_index(name="count").sort_values("count", ascending=False).head(20)
    )
    save_json("top_disturbance_types",
              counts.to_dict(orient="records"),
              "Top 20 disturbance labels with category and record count.")

    counts_sorted = counts.sort_values("count")
    colors = [DIST_COLORS.get(label, "#8c8c8c") for label in counts_sorted["disturbance_category"]]

    fig, ax = plt.subplots(figsize=(9.5, 7.2))
    ax.barh(counts_sorted["disturbance_label"], counts_sorted["count"], color=colors, height=0.64)
    annotate_bars(ax, counts_sorted["count"])
    ax.set_xlabel("Records")
    ax.set_title("Top FIA disturbance types", loc="left")
    ax.set_xlim(0, float(counts_sorted["count"].max()) * 1.24)
    save(fig, "top_disturbance_types.png", "Top FIA disturbance types",
         "The 20 most common FIA disturbance labels.")


def plot_disturbance_event_locations() -> None:
    df = read_summary("plot_disturbance_history.parquet",
                      columns=["LAT", "LON", "disturbance_category", "disturbance_label"])
    if df is None:
        return
    df = conus(df.dropna(subset=["LAT", "LON", "disturbance_category"]))
    sample = sample_frame(df, n=15000)
    out = sample[["LAT", "LON", "disturbance_category", "disturbance_label"]].copy()
    out.columns = ["lat", "lon", "category", "label"]
    save_csv("disturbance_event_points", out,
             "Sampled disturbance event points (lat/lon/category/label).")

    fig, ax = plt.subplots(figsize=(10, 5.6))
    for category, color in DIST_COLORS.items():
        part = sample[sample["disturbance_category"] == category]
        if not part.empty:
            ax.scatter(part["LON"], part["LAT"], s=5, c=color, alpha=0.55, linewidths=0, label=category)
    style_map_axis(ax, "Disturbance event locations")
    ax.legend(loc="lower left", ncol=2, frameon=True, facecolor=PANEL, edgecolor=BORDER,
              labelcolor=TEXT, fontsize=8)
    save(fig, "disturbance_event_locations.png", "Disturbance event locations",
         "Sampled FIA disturbance events by category.")


def plot_damage_agent_top20() -> None:
    df = read_summary("plot_damage_agents.parquet",
                      columns=["agent_label", "agent_category", "n_trees_tpa"])
    if df is None:
        return
    counts = (
        df.dropna(subset=["agent_label"])
        .groupby(["agent_label", "agent_category"], dropna=False)["n_trees_tpa"]
        .sum().reset_index().sort_values("n_trees_tpa", ascending=False).head(20)
    )
    save_json("damage_agent_top20", counts.to_dict(orient="records"),
              "Top 20 damage agents ranked by summed affected TPA.")

    counts_sorted = counts.sort_values("n_trees_tpa")
    colors = [AGENT_COLORS.get(label, "#8c8c8c") for label in counts_sorted["agent_category"]]

    fig, ax = plt.subplots(figsize=(9.5, 7.4))
    ax.barh(counts_sorted["agent_label"], counts_sorted["n_trees_tpa"], color=colors, height=0.64)
    annotate_bars(ax, counts_sorted["n_trees_tpa"])
    ax.set_xlabel("Affected trees per acre, summed")
    ax.set_title("Top 20 damage agents", loc="left")
    ax.set_xlim(0, float(counts_sorted["n_trees_tpa"].max()) * 1.24)
    save(fig, "damage_agent_top20.png", "Top 20 damage agents",
         "Damage agents ranked by summed affected trees per acre.")


def plot_agent_category_ba() -> None:
    df = read_summary("plot_damage_agents.parquet", columns=["agent_category", "ba_per_acre"])
    if df is None:
        return
    counts = (df.dropna(subset=["agent_category"])
              .groupby("agent_category")["ba_per_acre"].sum().sort_values())
    rows = [{"category": str(cat), "ba_per_acre": float(val),
             "color": AGENT_COLORS.get(str(cat), "#8c8c8c")}
            for cat, val in counts.items()]
    save_json("agent_category_ba", rows,
              "Summed affected basal area per FIA damage-agent category.")

    colors = [AGENT_COLORS.get(label, "#8c8c8c") for label in counts.index]
    fig, ax = plt.subplots(figsize=(8.5, 5.2))
    ax.barh(counts.index, counts.values, color=colors, height=0.68)
    annotate_bars(ax, counts)
    ax.set_xlabel("Affected basal area, summed")
    ax.set_title("Basal area affected by damage category", loc="left")
    ax.set_xlim(0, float(counts.max()) * 1.24)
    save(fig, "agent_category_ba.png", "Basal area affected by damage category",
         "Summed affected basal area by FIA damage-agent category.")


def plot_agent_state_heatmap() -> None:
    df = read_summary("plot_damage_agents.parquet",
                      columns=["state", "agent_category", "n_trees_tpa"])
    if df is None:
        return
    heat = (df.dropna(subset=["state", "agent_category"])
            .groupby(["agent_category", "state"])["n_trees_tpa"].sum()
            .reset_index())
    save_csv("agent_state_heatmap", heat,
             "Long-format damage-agent category x state x summed TPA.")

    wide = heat.pivot(index="agent_category", columns="state", values="n_trees_tpa").fillna(0)
    if wide.empty:
        return
    wide = wide.loc[wide.sum(axis=1).sort_values(ascending=False).index]
    fig, ax = plt.subplots(figsize=(11.5, 5.8))
    im = ax.imshow(np.log1p(wide.values), cmap="YlOrRd", aspect="auto")
    ax.set_xticks(range(len(wide.columns)))
    ax.set_xticklabels(wide.columns, rotation=90, fontsize=7)
    ax.set_yticks(range(len(wide.index)))
    ax.set_yticklabels(wide.index, fontsize=8)
    ax.set_title("Damage category by state", loc="left")
    ax.grid(False)
    cbar = fig.colorbar(im, ax=ax, fraction=0.02, pad=0.015)
    cbar.set_label("log1p affected TPA")
    fig.tight_layout()
    save(fig, "agent_category_state_heatmap.png", "Damage category by state",
         "State by damage-agent-category heatmap using log1p affected TPA.")


def plot_damage_agent_locations() -> None:
    agents = read_summary("plot_damage_agents.parquet",
                          columns=["PLT_CN", "agent_category", "agent_label"])
    trees = read_summary("plot_tree_metrics.parquet", columns=["PLT_CN", "LAT", "LON"])
    if agents is None or trees is None:
        return
    coords = trees.dropna(subset=["LAT", "LON"]).drop_duplicates("PLT_CN")
    df = agents.dropna(subset=["agent_category"]).merge(coords, on="PLT_CN", how="left").dropna(subset=["LAT", "LON"])
    df = conus(df)
    sample = sample_frame(df, n=15000)
    out = sample[["LAT", "LON", "agent_category", "agent_label"]].copy()
    out.columns = ["lat", "lon", "category", "label"]
    save_csv("damage_agent_points", out, "Sampled FIA plots with damage-agent points by category.")

    fig, ax = plt.subplots(figsize=(10, 5.6))
    for category, color in AGENT_COLORS.items():
        part = sample[sample["agent_category"] == category]
        if not part.empty:
            ax.scatter(part["LON"], part["LAT"], s=5, c=color, alpha=0.5, linewidths=0, label=category)
    style_map_axis(ax, "Damage agent locations")
    ax.legend(loc="lower left", ncol=2, frameon=True, facecolor=PANEL, edgecolor=BORDER,
              labelcolor=TEXT, fontsize=7)
    save(fig, "damage_agent_locations.png", "Damage agent locations",
         "Sampled FIA plots with damage-agent records by category.")


def build_mortality_aggregates() -> None:
    df = read_summary("plot_mortality_metrics.parquet",
                      columns=["AGENTCD", "component_type", "tpamort_per_acre", "state"])
    if df is None:
        return
    AGENTCD_LABELS = {10: "Insect", 20: "Disease", 30: "Fire", 40: "Animal",
                      50: "Weather", 60: "Vegetation", 70: "Unknown", 80: "Harvest"}
    df = df.copy()
    df["agent_label"] = df["AGENTCD"].map(AGENTCD_LABELS).fillna("Other")
    agent_tpa = (df.groupby(["agent_label", "component_type"])["tpamort_per_acre"]
                 .sum().reset_index().sort_values("tpamort_per_acre", ascending=False))
    save_csv("mortality_by_agent", agent_tpa,
             "Mortality TPA by inferred agent label and component_type.")

    if "state" in df.columns:
        state_mort = (df.groupby(["state", "component_type"])["tpamort_per_acre"]
                      .sum().reset_index())
        save_csv("state_mortality", state_mort,
                 "Summed mortality TPA per state and component_type.")


def build_seedling_aggregates() -> None:
    df = read_summary("plot_seedling_metrics.parquet",
                      columns=["state", "count_softwood", "count_hardwood", "shannon_h_count"])
    if df is None:
        return
    sw_cols = [c for c in ["count_softwood", "count_hardwood"] if c in df.columns]
    if sw_cols:
        seed_state = (df.dropna(subset=["state"])
                      .groupby("state")[sw_cols].sum().reset_index()
                      .rename(columns={"count_softwood": "softwood",
                                       "count_hardwood": "hardwood"}))
        save_csv("state_seedlings", seed_state,
                 "Summed softwood/hardwood seedling counts per state.")
    if "shannon_h_count" in df.columns:
        seed_div = (df.dropna(subset=["state", "shannon_h_count"])
                    .groupby("state")["shannon_h_count"].mean().reset_index()
                    .rename(columns={"shannon_h_count": "mean_shannon_h"}))
        save_csv("state_seedling_diversity", seed_div,
                 "Mean seedling Shannon diversity per state.")


def build_treatment_aggregates() -> None:
    df = read_summary("plot_treatment_history.parquet",
                      columns=["treatment_label", "treatment_category", "TRTYR", "INVYR", "STATECD"])
    if df is None:
        return
    cat_counts = (df.dropna(subset=["treatment_label"])
                  .groupby(["treatment_label", "treatment_category"])
                  .size().reset_index(name="count")
                  .sort_values("count", ascending=False))
    save_csv("treatment_label_counts", cat_counts,
             "Record counts by treatment label and category.")

    state_col = "STATECD" if "STATECD" in df.columns else ("state" if "state" in df.columns else None)
    if state_col is not None:
        state_treat = (df.dropna(subset=[state_col])
                       .groupby([state_col, "treatment_category"]).size()
                       .reset_index(name="count")
                       .rename(columns={state_col: "state"}))
        save_csv("state_treatments", state_treat,
                 "Treatment record counts per state and category.")

    if "TRTYR" in df.columns:
        year_raw = pd.to_numeric(df["TRTYR"], errors="coerce")
        valid = year_raw.notna() & (year_raw >= 1900) & (year_raw < 9999)
        if "INVYR" in df.columns:
            invyr = pd.to_numeric(df["INVYR"], errors="coerce")
            valid &= invyr.notna() & (year_raw <= invyr)
        sub = df.loc[valid].copy()
        sub["treatment_year"] = year_raw.loc[valid].astype(int)
        year_counts = (sub.groupby(["treatment_year", "treatment_category"], dropna=False)
                       .size().reset_index(name="count"))
        save_csv("treatment_year_counts", year_counts,
                 "Treatment year x category counts.")


def write_manifest() -> None:
    MANIFEST_PATH.parent.mkdir(parents=True, exist_ok=True)
    MANIFEST_PATH.write_text(
        json.dumps(
            {
                "title": "Static FIA dashboard figures and aggregates",
                "description": "PNG figures and compact JSON/CSV aggregates generated from local FIA "
                               "summary parquets. The dashboard prefers the aggregates for interactive "
                               "Plotly rendering and falls back to PNGs when parquets are unavailable.",
                "figures": MANIFEST,
                "data": DATA_MANIFEST,
            },
            indent=2,
        ),
        encoding="utf-8",
    )
    print(f"wrote {MANIFEST_PATH.relative_to(REPO_ROOT).as_posix()}")


def main() -> None:
    configure_theme()
    plot_flag_rates()
    plot_plot_status_map()
    plot_locations_ba()
    plot_tree_metric_distributions()
    plot_disturbance_category_counts()
    plot_fire_type_breakdown()
    plot_disturbance_year_counts()
    plot_top_disturbance_types()
    plot_disturbance_event_locations()
    plot_damage_agent_top20()
    plot_agent_category_ba()
    plot_agent_state_heatmap()
    plot_damage_agent_locations()
    build_mortality_aggregates()
    build_seedling_aggregates()
    build_treatment_aggregates()
    write_manifest()


if __name__ == "__main__":
    main()
