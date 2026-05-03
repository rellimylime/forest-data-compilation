# ==============================================================================
# pages/3_FIA_Forest.py
# FIA Forest Inventory — data explorer
# Ported from 05_fia/docs/fia_data_dashboard.py
# ==============================================================================

import os
import sys
from pathlib import Path

import pandas as pd
import streamlit as st

sys.path.insert(0, str(Path(__file__).parent.parent))
from utils import (
    apply_dark_css, metric_card, dark_fig, scatter_geo_usa,
    load_parquet, parquet_meta, repo_path, color_status, PLOTLY_AVAILABLE,
    plot_source_link, render_top_nav,
)

st.set_page_config(page_title="FIA Forest", page_icon="🌲", layout="wide")
apply_dark_css()
render_top_nav()

if PLOTLY_AVAILABLE:
    import plotly.express as px
    import plotly.graph_objects as go

# ------------------------------------------------------------------------------
# Paths
# ------------------------------------------------------------------------------

SUMM_DIR   = repo_path("05_fia", "data", "processed", "summaries")
CLIM_DIR   = repo_path("05_fia", "data", "processed", "site_climate")

def sp(fname): return str(SUMM_DIR / fname)
def cp(fname): return str(CLIM_DIR / fname)


def source_plotly_chart(fig, source_line: int, **kwargs):
    st.plotly_chart(fig, **kwargs)
    plot_source_link("docs/dashboard/pages/3_FIA_Forest.py", line=source_line)



# ------------------------------------------------------------------------------
# Color maps
# ------------------------------------------------------------------------------

DIST_COLORS = {
    "fire": "#e15759", "insects": "#59a14f", "disease": "#f28e2b",
    "weather": "#4e79a7", "animal": "#9c755f", "vegetation": "#76b7b2",
    "geologic": "#bab0ac", "other": "#8c8c8c",
}
AGENT_COLORS = {
    "bark beetles": "#e15759", "defoliators": "#59a14f",
    "sucking insects": "#76b7b2", "boring insects": "#edc948",
    "insects": "#b6d77a", "root/butt disease": "#f28e2b",
    "canker/rust": "#ff9da7", "foliage/wilt disease": "#9c755f",
    "disease": "#ffbf7f", "fire": "#e15759", "complex": "#b07aa1",
    "abiotic": "#4e79a7", "human": "#bab0ac", "other": "#8c8c8c", "unknown": "#555555",
}

STATE_EAST_TO_WEST = [
    "ME", "NH", "VT", "MA", "RI", "CT", "NJ", "DE", "MD", "DC",
    "NY", "PA", "FL", "SC", "NC", "VA", "WV", "OH", "MI", "IN",
    "KY", "TN", "GA", "AL", "MS", "WI", "IL", "LA", "AR", "MO",
    "IA", "MN", "OK", "KS", "NE", "SD", "ND", "TX", "CO", "NM",
    "WY", "MT", "AZ", "UT", "ID", "NV", "WA", "OR", "CA", "AK", "HI",
]
STATE_CODE_BY_ABBR = {
    "AL": 1, "AK": 2, "AZ": 4, "AR": 5, "CA": 6, "CO": 8, "CT": 9,
    "DE": 10, "DC": 11, "FL": 12, "GA": 13, "HI": 15, "ID": 16,
    "IL": 17, "IN": 18, "IA": 19, "KS": 20, "KY": 21, "LA": 22,
    "ME": 23, "MD": 24, "MA": 25, "MI": 26, "MN": 27, "MS": 28,
    "MO": 29, "MT": 30, "NE": 31, "NV": 32, "NH": 33, "NJ": 34,
    "NM": 35, "NY": 36, "NC": 37, "ND": 38, "OH": 39, "OK": 40,
    "OR": 41, "PA": 42, "RI": 44, "SC": 45, "SD": 46, "TN": 47,
    "TX": 48, "UT": 49, "VT": 50, "VA": 51, "WA": 53, "WV": 54,
    "WI": 55, "WY": 56,
}
STATE_ABBR_BY_CODE = {value: key for key, value in STATE_CODE_BY_ABBR.items()}


def state_order(values) -> list:
    """Return present states in approximate east-to-west order."""
    present = [value for value in pd.Series(values).dropna().unique().tolist()]
    by_label = {str(value).upper(): value for value in present}
    by_code = {}
    for value in present:
        try:
            by_code[int(value)] = value
        except (TypeError, ValueError):
            pass

    ordered = []
    for abbr in STATE_EAST_TO_WEST:
        if abbr in by_label:
            ordered.append(by_label[abbr])
        elif STATE_CODE_BY_ABBR.get(abbr) in by_code:
            ordered.append(by_code[STATE_CODE_BY_ABBR[abbr]])

    ordered_keys = {str(value).upper() for value in ordered}
    leftovers = sorted(
        [value for value in present if str(value).upper() not in ordered_keys],
        key=lambda value: str(value),
    )
    return ordered + leftovers


def apply_state_order(df: pd.DataFrame, state_col: str) -> tuple[pd.DataFrame, list]:
    order = state_order(df[state_col])
    out = df.copy()
    out[state_col] = pd.Categorical(out[state_col], categories=order, ordered=True)
    return out, order


def state_plot_height(n_states: int, minimum: int = 500) -> int:
    return max(minimum, min(1150, 24 * max(n_states, 1) + 150))


def order_state_yaxis(fig, order: list) -> None:
    fig.update_yaxes(categoryorder="array", categoryarray=list(reversed(order)))


def finish_state_bar(fig, order: list):
    fig = dark_fig(fig)
    order_state_yaxis(fig, order)
    fig.update_layout(
        height=state_plot_height(len(order)),
        margin=dict(l=86, r=20, t=20, b=40),
    )
    return fig


# ------------------------------------------------------------------------------
# Title and load data
# ------------------------------------------------------------------------------

st.title("🌲 FIA Forest Inventory")
st.markdown(
    "USDA Forest Inventory and Analysis — processed summaries for all 50 US states. "
    "Source tables from `05_fia/scripts/05_build_fia_summaries.R`."
)

with st.spinner("Loading FIA summaries…"):
    tree_df,    tree_err    = load_parquet(sp("plot_tree_metrics.parquet"))
    disturb_df, disturb_err = load_parquet(sp("plot_disturbance_history.parquet"))
    agents_df,  agents_err  = load_parquet(sp("plot_damage_agents.parquet"))
    mort_df,    mort_err    = load_parquet(sp("plot_mortality_metrics.parquet"))
    seed_df,    seed_err    = load_parquet(sp("plot_seedling_metrics.parquet"))
    treat_df,   treat_err   = load_parquet(sp("plot_treatment_history.parquet"))
    flags_df,   flags_err   = load_parquet(sp("plot_exclusion_flags.parquet"))
# site_climate.parquet is 23.5M rows — not loaded eagerly; use metadata only
clim_df = None

# ------------------------------------------------------------------------------
# Tabs
# ------------------------------------------------------------------------------

(tab_overview, tab_filters, tab_forests, tab_disturb,
 tab_agents, tab_mort, tab_treatments, tab_climate) = st.tabs([
    "📂 Overview",
    "🚩 Plot Filters",
    "🌲 Tree Metrics",
    "🔥 Disturbance",
    "🪲 Damage Agents",
    "💀 Mortality & Regeneration",
    "🪚 Treatment History",
    "🌡️ Site Climate",
])

# ==============================================================================
# TAB 1 — OVERVIEW
# ==============================================================================
with tab_overview:
    if tree_df is not None:
        n_plots  = tree_df["PLT_CN"].nunique()
        n_states = tree_df["state"].nunique() if "state" in tree_df.columns else "—"
        yr_min   = int(tree_df["INVYR"].min())
        yr_max   = int(tree_df["INVYR"].max())
        n_visits = len(tree_df)
        c1, c2, c3, c4 = st.columns(4)
        c1.markdown(metric_card("Unique Plots", f"{n_plots:,}",  "distinct PLT_CN"),       unsafe_allow_html=True)
        c2.markdown(metric_card("States",        str(n_states),   "with tree data"),        unsafe_allow_html=True)
        c3.markdown(metric_card("Year Range",    f"{yr_min}–{yr_max}", "INVYR"),            unsafe_allow_html=True)
        c4.markdown(metric_card("Plot Visits",   f"{n_visits:,}", "PLT_CN × INVYR rows"),  unsafe_allow_html=True)
    else:
        st.info("Run `Rscript 05_fia/scripts/05_build_fia_summaries.R` to generate the summary parquets.")

    st.markdown("---")

    # File availability
    files_info = [
        ("plot_tree_metrics.parquet",        tree_df,    "Tree metrics (BA, diversity, size class)"),
        ("plot_exclusion_flags.parquet",      flags_df,   "Plot exclusion & disturbance flags"),
        ("plot_disturbance_history.parquet",  disturb_df, "Disturbance history (fire/insect/disease)"),
        ("plot_damage_agents.parquet",        agents_df,  "Damage agents (specific insect/disease codes)"),
        ("plot_mortality_metrics.parquet",    mort_df,    "Mortality (natural & harvest)"),
        ("plot_seedling_metrics.parquet",     seed_df,    "Seedling regeneration"),
        ("plot_treatment_history.parquet",    treat_df,   "Treatment history (cutting, regen, site prep)"),
        ("plot_cond_fortypcd.parquet",        None,       "Condition / forest type (not pre-loaded)"),
        ("site_climate.parquet",          None,    "Point climate — FIA plots + ITRDB sites (TerraClimate 1958–2024)"),
    ]
    rows = []
    for fname, df, desc in files_info:
        is_summ = fname != "site_climate.parquet"
        fpath = sp(fname) if is_summ else cp(fname)
        exists = os.path.isfile(fpath)
        size_mb = os.path.getsize(fpath) / 1e6 if exists else None
        if df is not None:
            n_rows = len(df)
        elif exists and fpath.endswith(".parquet"):
            m = parquet_meta(fpath)
            n_rows = m.get("rows") or "—"
        else:
            n_rows = "—"
        rows.append({
            "File": fname, "Description": desc,
            "Status": "✅" if exists else "❌",
            "Size": f"{size_mb:.1f} MB" if size_mb else "—",
            "Rows": f"{n_rows:,}" if isinstance(n_rows, int) else n_rows,
        })
    st.dataframe(
        pd.DataFrame(rows).style.map(color_status, subset=["Status"]),
        use_container_width=True, hide_index=True,
    )

    st.markdown("---")
    st.markdown("### How Datasets Connect")
    st.markdown(
        "All datasets share **`PLT_CN`** (plot control number) as the primary identifier.\n\n"
        "| Dataset | Grain | Join key |\n"
        "|---------|-------|----------|\n"
        "| `plot_tree_metrics` | 1 row per plot × year | `PLT_CN, INVYR` |\n"
        "| `plot_exclusion_flags` | 1 row per plot × year | `PLT_CN, INVYR` |\n"
        "| `plot_seedling_metrics` | 1 row per plot × year | `PLT_CN, INVYR` |\n"
        "| `plot_disturbance_history` | 1+ rows per plot × year | `PLT_CN, INVYR, CONDID` |\n"
        "| `plot_damage_agents` | 1+ rows per plot × year | `PLT_CN, INVYR, CONDID` |\n"
        "| `plot_mortality_metrics` | 1+ rows per plot × year | `PLT_CN, INVYR` |\n"
        "| `plot_treatment_history` | 1+ rows per condition × treatment slot | `PLT_CN, INVYR` |\n"
        "| `site_climate` | 1 row per site × year × month × variable | `site_id` (numeric = FIA, alphanumeric = ITRDB) |\n"
    )

# ==============================================================================
# TAB 2 — PLOT FILTERS
# ==============================================================================
with tab_filters:
    st.markdown(
        "**`plot_exclusion_flags.parquet`** — one row per plot × inventory year. "
        "Join on `PLT_CN + INVYR`. Produced by Step 7 of `05_build_fia_summaries.R`."
    )
    if flags_df is None or len(flags_df) == 0:
        st.info(
            "`plot_exclusion_flags.parquet` not found.\n\n"
            "Generate with Step 7 of `05_build_fia_summaries.R`. "
            "Requires COND parquets with TRTCD columns (re-run `03_extract_trees.R --force-cond`)."
        )
    else:
        n_total  = len(flags_df)
        pct_excl = 100.0 * flags_df["exclude_any"].mean()
        pct_nf   = 100.0 * flags_df["exclude_nonforest"].mean()
        pct_hd   = 100.0 * flags_df["exclude_human_dist"].mean()
        pct_harv = (100.0 * flags_df["exclude_harvest"].dropna().mean()
                    if "exclude_harvest" in flags_df.columns and flags_df["exclude_harvest"].notna().any() else None)
        pct_ha   = (100.0 * flags_df["exclude_harvest_agent"].dropna().mean()
                    if "exclude_harvest_agent" in flags_df.columns and flags_df["exclude_harvest_agent"].notna().any() else None)
        pct_fire = 100.0 * flags_df["has_fire"].mean()
        pct_ins  = 100.0 * flags_df["has_insect"].mean()

        c1, c2, c3, c4 = st.columns(4)
        c1.markdown(metric_card("Plot×Year Rows",  f"{n_total:,}",      "total"),               unsafe_allow_html=True)
        c2.markdown(metric_card("Excluded (any)",  f"{pct_excl:.1f}%",  "exclude_any = TRUE"),  unsafe_allow_html=True)
        c3.markdown(metric_card("Has Fire",        f"{pct_fire:.1f}%",  "DSTRBCD 30/31/32"),    unsafe_allow_html=True)
        c4.markdown(metric_card("Has Insects",     f"{pct_ins:.1f}%",   "DSTRBCD 10/11/12"),    unsafe_allow_html=True)

        st.markdown("---")
        col_l, col_r = st.columns(2)

        with col_l:
            st.subheader("Flag rates")
            flag_rows = [
                ("exclude_nonforest",     pct_nf,   "COND_STATUS_CD = 5", "#f85149"),
                ("exclude_human_dist",    pct_hd,   "DSTRBCD = 80",       "#f28e2b"),
                ("exclude_harvest",       pct_harv, "TRTCD = 10",         "#4e79a7"),
                ("exclude_harvest_agent", pct_ha,   "AGENTCD 80–89",      "#a371f7"),
                ("has_fire",              pct_fire, "DSTRBCD 30/31/32",   "#e15759"),
                ("has_insect",            pct_ins,  "DSTRBCD 10/11/12",   "#59a14f"),
            ]
            flag_df_plot = pd.DataFrame(
                [(name, rate, basis, color) for name, rate, basis, color in flag_rows if rate is not None],
                columns=["Flag", "Rate (%)", "Basis", "_color"],
            )
            if PLOTLY_AVAILABLE and len(flag_df_plot) > 0:
                fig = px.bar(
                    flag_df_plot, y="Flag", x="Rate (%)", orientation="h",
                    color="Flag",
                    color_discrete_map=dict(zip(flag_df_plot["Flag"], flag_df_plot["_color"])),
                    hover_data={"Basis": True, "Rate (%)": ":.2f", "_color": False},
                )
                fig.update_layout(showlegend=False, yaxis=dict(autorange="reversed"))
                source_plotly_chart(dark_fig(fig), source_line=295, use_container_width=True)

        with col_r:
            st.subheader("What each flag means")
            st.markdown(
                "| Flag | Source | Meaning |\n"
                "|------|--------|---------|\n"
                "| `exclude_nonforest` | `COND_STATUS_CD = 5` | Nonsampled portion of a forest land plot (denied access, hazard, etc.) — flag name is a misnomer; code 5 IS forest land |\n"
                "| `exclude_human_dist` | `DSTRBCD = 80` | Human-induced disturbance: logging, clearing, development |\n"
                "| `exclude_harvest` | `TRTCD = 10` | Cutting treatment recorded on condition |\n"
                "| `exclude_harvest_agent` | `AGENTCD 80–89` | Tree-level harvest cause-of-death (more sensitive) |\n"
                "| `exclude_any` | OR of all four | Convenience — standard clean-plot filter |\n"
                "| `has_fire` | `DSTRBCD 30/31/32` | Fire disturbance — **positive filter** |\n"
                "| `has_insect` | `DSTRBCD 10/11/12` | Insect damage — **positive filter** |\n"
                "| `pct_forested` | `CONDPROP_UNADJ` | Fraction of plot in forested conditions (0–1) |\n"
            )
            st.info(
                "**FIA samples ALL US land.** ~59% of plot×year rows have `pct_forested = 0`. "
                "Always filter `pct_forested >= 0.5` as the **primary gate** before using any flags."
            )

        st.markdown("---")
        st.subheader("How to apply these flags")
        r_tab, py_tab = st.tabs(["R", "Python"])
        with r_tab:
            st.code(
                'library(arrow); library(dplyr)\n'
                'flags <- read_parquet("05_fia/data/processed/summaries/plot_exclusion_flags.parquet")\n'
                'trees <- read_parquet("05_fia/data/processed/summaries/plot_tree_metrics.parquet")\n\n'
                '# Standard clean-plot filter\n'
                'clean <- flags |> filter(pct_forested >= 0.5, !exclude_any)\n'
                'trees_clean <- trees |> inner_join(clean, by = c("PLT_CN", "INVYR"))\n\n'
                '# Select only burned plots\n'
                'trees_fire <- trees |>\n'
                '  inner_join(flags |> filter(pct_forested >= 0.5, has_fire),\n'
                '             by = c("PLT_CN", "INVYR"))',
                language="r",
            )
        with py_tab:
            st.code(
                'import pandas as pd\n'
                'flags = pd.read_parquet("05_fia/data/processed/summaries/plot_exclusion_flags.parquet")\n'
                'trees = pd.read_parquet("05_fia/data/processed/summaries/plot_tree_metrics.parquet")\n\n'
                '# Standard clean-plot filter\n'
                'clean = flags[(flags["pct_forested"] >= 0.5) & ~flags["exclude_any"]]\n'
                'trees_clean = trees.merge(clean[["PLT_CN", "INVYR"]], on=["PLT_CN", "INVYR"])',
                language="python",
            )

        if "pct_forested" in flags_df.columns and PLOTLY_AVAILABLE:
            st.markdown("---")
            st.subheader("pct_forested distribution")
            fig = px.histogram(flags_df, x="pct_forested", nbins=30,
                               color_discrete_sequence=["#59a14f"])
            source_plotly_chart(dark_fig(fig), source_line=354, use_container_width=True)

        if tree_df is not None and "LAT" in tree_df.columns and PLOTLY_AVAILABLE:
            st.markdown("---")
            st.subheader("Excluded vs. clean plot locations")
            coord = tree_df[["PLT_CN", "LAT", "LON"]].drop_duplicates("PLT_CN")
            map_flags = flags_df.merge(coord, on="PLT_CN", how="left").dropna(subset=["LAT", "LON"])
            map_flags["Plot status"] = map_flags["exclude_any"].map({True: "Excluded", False: "Clean"})
            map_flags = map_flags[map_flags["pct_forested"] >= 0.5]
            if len(map_flags) > 50_000:
                map_flags = map_flags.sample(50_000, random_state=42)
            fig = scatter_geo_usa(map_flags, "LAT", "LON", "Plot status",
                                  color_map={"Excluded": "#f85149", "Clean": "#59a14f"})
            source_plotly_chart(fig, source_line=367, use_container_width=True)

# ==============================================================================
# TAB 3 — TREE METRICS
# ==============================================================================
with tab_forests:
    if tree_df is None:
        st.info("plot_tree_metrics.parquet not available.")
    else:
        if "LAT" in tree_df.columns and PLOTLY_AVAILABLE:
            st.subheader("Plot Locations")
            map_l, map_r = st.columns([2, 1])
            with map_l:
                map_metric = st.radio("Color by",
                    ["ba_live_total", "shannon_h_ba", "n_species_live"],
                    horizontal=True, key="forest_map_metric")
            with map_r:
                all_states = state_order(tree_df["state"]) if "state" in tree_df.columns else []
                map_state = st.selectbox("Zoom to state", ["All states"] + all_states, key="forest_map_state")

            map_df = tree_df.dropna(subset=["LAT", "LON", map_metric])
            if map_state != "All states":
                map_df = map_df[map_df["state"] == map_state]
            elif len(map_df) > 50_000:
                map_df = map_df.sample(50_000, random_state=42)

            labels = {"ba_live_total": "Live BA (ft²/acre)", "shannon_h_ba": "Shannon H",
                      "n_species_live": "Species richness"}
            fig = px.scatter_geo(map_df, lat="LAT", lon="LON", color=map_metric,
                                 color_continuous_scale="Viridis", opacity=0.6,
                                 labels={map_metric: labels.get(map_metric, map_metric)})
            fig.update_traces(marker_size=5 if map_state != "All states" else 3)
            geo_cfg = dict(bgcolor="#0e1117", landcolor="#1c2128", lakecolor="#0e1117",
                           coastlinecolor="#444", showland=True, showlakes=True, showcoastlines=True)
            if map_state != "All states":
                geo_cfg.update(fitbounds="locations", resolution=50)
            else:
                geo_cfg["scope"] = "usa"
            fig.update_layout(paper_bgcolor="#0e1117", geo=geo_cfg, font_color="#ddd",
                              coloraxis_colorbar=dict(bgcolor="#161b22", tickcolor="#ddd",
                                                      title_font_color="#ddd"),
                              margin=dict(l=0, r=0, t=10, b=0))
            source_plotly_chart(fig, source_line=397, use_container_width=True)

        col1, col2 = st.columns(2)
        with col1:
            st.subheader("Live BA distribution")
            if "ba_live_total" in tree_df.columns and PLOTLY_AVAILABLE:
                fig = px.histogram(tree_df[tree_df["ba_live_total"] > 0],
                                   x="ba_live_total", nbins=60,
                                   color_discrete_sequence=["#59a14f"])
                source_plotly_chart(dark_fig(fig), source_line=417, use_container_width=True)
        with col2:
            st.subheader("Shannon Diversity (BA-weighted)")
            if "shannon_h_ba" in tree_df.columns and PLOTLY_AVAILABLE:
                fig = px.histogram(tree_df[tree_df["shannon_h_ba"] > 0],
                                   x="shannon_h_ba", nbins=50,
                                   color_discrete_sequence=["#4e79a7"])
                source_plotly_chart(dark_fig(fig), source_line=424, use_container_width=True)

        if all(c in tree_df.columns for c in ["ba_live_softwood", "ba_live_hardwood", "state"]):
            st.subheader("Softwood vs. Hardwood BA by state")
            sw_hw = (tree_df.groupby("state")[["ba_live_softwood", "ba_live_hardwood"]]
                     .mean().reset_index()
                     .rename(columns={"ba_live_softwood": "Softwood", "ba_live_hardwood": "Hardwood"}))
            sw_hw, state_order_present = apply_state_order(sw_hw, "state")
            if PLOTLY_AVAILABLE:
                fig = px.bar(sw_hw.melt(id_vars="state", var_name="Type", value_name="Mean BA"),
                             x="Mean BA", y="state", color="Type", barmode="stack",
                             orientation="h",
                             color_discrete_map={"Softwood": "#f28e2b", "Hardwood": "#59a14f"},
                             labels={"state": "State"})
                fig = finish_state_bar(fig, state_order_present)
                source_plotly_chart(fig, source_line=436, use_container_width=True)

        size_cols = [c for c in ["ba_live_sapling", "ba_live_intermediate", "ba_live_mature"]
                     if c in tree_df.columns]
        if size_cols and "state" in tree_df.columns:
            st.subheader("Size class BA by state")
            sz = tree_df.groupby("state")[size_cols].mean().reset_index()
            rename_map = {"ba_live_sapling": "Sapling", "ba_live_intermediate": "Intermediate",
                          "ba_live_mature": "Mature"}
            sz = sz.rename(columns=rename_map)
            sz_melt = sz.melt(id_vars="state",
                              value_vars=[rename_map[c] for c in size_cols if c in rename_map],
                              var_name="Size class", value_name="Mean BA")
            sz_melt, state_order_present = apply_state_order(sz_melt, "state")
            if PLOTLY_AVAILABLE:
                fig = px.bar(sz_melt, x="Mean BA", y="state", color="Size class", barmode="stack",
                             orientation="h",
                             color_discrete_map={"Sapling": "#76b7b2", "Intermediate": "#59a14f",
                                                 "Mature": "#4e79a7"},
                             labels={"state": "State"})
                fig = finish_state_bar(fig, state_order_present)
                source_plotly_chart(fig, source_line=457, use_container_width=True)

# ==============================================================================
# TAB 4 — DISTURBANCE
# ==============================================================================
with tab_disturb:
    if disturb_df is None:
        st.info("plot_disturbance_history.parquet not found.")
    else:
        col1, col2 = st.columns(2)
        with col1:
            st.subheader("Events by category")
            cat_counts = (disturb_df.groupby("disturbance_category").size()
                          .reset_index(name="count").sort_values("count", ascending=False))
            if PLOTLY_AVAILABLE:
                fig = px.bar(cat_counts, x="disturbance_category", y="count",
                             color="disturbance_category", color_discrete_map=DIST_COLORS,
                             labels={"disturbance_category": "Category", "count": "Records"})
                fig.update_layout(showlegend=False)
                source_plotly_chart(dark_fig(fig), source_line=478, use_container_width=True)
        with col2:
            st.subheader("Fire type breakdown")
            fire_df = disturb_df[disturb_df["disturbance_category"] == "fire"]
            if len(fire_df) > 0 and PLOTLY_AVAILABLE:
                fire_counts = fire_df.groupby("disturbance_label").size().reset_index(name="count")
                fig = px.pie(fire_counts, names="disturbance_label", values="count",
                             color_discrete_sequence=["#e15759", "#ff9d9a", "#c85250"])
                fig.update_layout(paper_bgcolor="#0e1117", font_color="#ddd")
                source_plotly_chart(fig, source_line=488, use_container_width=True)

        if "DSTRBYR" in disturb_df.columns:
            st.subheader("Year of disturbance")
            yr_df = disturb_df[disturb_df["DSTRBYR"].notna() & (disturb_df["DSTRBYR"] != 9999)]
            if len(yr_df) > 0 and PLOTLY_AVAILABLE:
                fig = px.histogram(yr_df, x="DSTRBYR", color="disturbance_category",
                                   color_discrete_map=DIST_COLORS, nbins=50)
                source_plotly_chart(dark_fig(fig), source_line=497, use_container_width=True)

        st.subheader("Top disturbance types")
        label_counts = (disturb_df.groupby(["disturbance_label", "disturbance_category"])
                        .size().reset_index(name="count")
                        .sort_values("count", ascending=False).head(20))
        if PLOTLY_AVAILABLE:
            fig = px.bar(label_counts, x="count", y="disturbance_label", orientation="h",
                         color="disturbance_category", color_discrete_map=DIST_COLORS)
            fig.update_layout(yaxis=dict(autorange="reversed"))
            source_plotly_chart(dark_fig(fig), source_line=506, use_container_width=True)

        if all(c in disturb_df.columns for c in ["LAT", "LON"]) and PLOTLY_AVAILABLE:
            st.subheader("Disturbance event locations")
            map_df = disturb_df.dropna(subset=["LAT", "LON"])
            if len(map_df) > 50_000:
                map_df = map_df.sample(50_000, random_state=42)
            fig = scatter_geo_usa(map_df, "LAT", "LON", "disturbance_category",
                                  color_map=DIST_COLORS, hover_name="disturbance_label")
            source_plotly_chart(fig, source_line=516, use_container_width=True)

# ==============================================================================
# TAB 5 — DAMAGE AGENTS
# ==============================================================================
with tab_agents:
    if agents_df is None:
        st.info("plot_damage_agents.parquet not found.")
    else:
        col1, col2 = st.columns(2)
        with col1:
            st.subheader("Top 20 damage agents (by TPA)")
            has_label = agents_df[agents_df["agent_label"].notna()]
            if len(has_label) > 0 and PLOTLY_AVAILABLE:
                top_agents = (has_label.groupby(["agent_label", "agent_category"])
                              ["n_trees_tpa"].sum().reset_index()
                              .sort_values("n_trees_tpa", ascending=False).head(20))
                fig = px.bar(top_agents, x="n_trees_tpa", y="agent_label", orientation="h",
                             color="agent_category", color_discrete_map=AGENT_COLORS)
                fig.update_layout(yaxis=dict(autorange="reversed"))
                source_plotly_chart(dark_fig(fig), source_line=535, use_container_width=True)
        with col2:
            st.subheader("BA affected by agent category")
            cat_ba = (agents_df[agents_df["agent_category"].notna()]
                      .groupby("agent_category")["ba_per_acre"].sum().reset_index()
                      .sort_values("ba_per_acre", ascending=False))
            if PLOTLY_AVAILABLE:
                fig = px.bar(cat_ba, x="ba_per_acre", y="agent_category", orientation="h",
                             color="agent_category", color_discrete_map=AGENT_COLORS)
                fig.update_layout(showlegend=False, yaxis=dict(autorange="reversed"))
                source_plotly_chart(dark_fig(fig), source_line=545, use_container_width=True)

        if "state" in agents_df.columns and PLOTLY_AVAILABLE:
            st.subheader("Agent category × state heatmap")
            heat_df = (agents_df[agents_df["agent_category"].notna()]
                       .groupby(["state", "agent_category"])["n_trees_tpa"].sum().reset_index())
            if len(heat_df) > 0:
                state_order_present = state_order(heat_df["state"])
                heat_pivot = heat_df.pivot(index="agent_category", columns="state",
                                            values="n_trees_tpa").fillna(0)
                heat_pivot = heat_pivot.reindex(columns=state_order_present)
                fig = px.imshow(heat_pivot, color_continuous_scale="YlOrRd", aspect="auto",
                                 labels={"color": "Affected TPA (sum)"})
                fig.update_layout(paper_bgcolor="#0e1117", font_color="#ddd",
                                  margin=dict(l=140, r=20, t=20, b=60))
                source_plotly_chart(fig, source_line=559, use_container_width=True)

        if tree_df is not None and "LAT" in tree_df.columns and PLOTLY_AVAILABLE:
            st.subheader("Damage agent locations")
            coord = tree_df[["PLT_CN", "LAT", "LON"]].drop_duplicates("PLT_CN")
            map_df2 = (agents_df[agents_df["agent_category"].notna()]
                       .merge(coord, on="PLT_CN", how="left")
                       .dropna(subset=["LAT", "LON"]))
            if len(map_df2) > 50_000:
                map_df2 = map_df2.sample(50_000, random_state=42)
            fig = scatter_geo_usa(map_df2, "LAT", "LON", "agent_category",
                                  color_map=AGENT_COLORS, hover_name="agent_label")
            source_plotly_chart(fig, source_line=573, use_container_width=True)

# ==============================================================================
# TAB 6 — MORTALITY & REGENERATION
# ==============================================================================
with tab_mort:
    mort_col, seed_col = st.columns(2)
    AGENTCD_LABELS = {10: "Insect", 20: "Disease", 30: "Fire",
                      40: "Animal", 50: "Weather", 60: "Vegetation",
                      70: "Unknown", 80: "Harvest"}

    with mort_col:
        st.subheader("Mortality by agent")
        if mort_df is None:
            st.info("plot_mortality_metrics.parquet not found.")
        else:
            mort_plot = mort_df.copy()
            mort_plot["agent_label"] = mort_plot["AGENTCD"].map(AGENTCD_LABELS).fillna("Other")
            agent_tpa = (mort_plot.groupby(["agent_label", "component_type"])
                         ["tpamort_per_acre"].sum().reset_index()
                         .sort_values("tpamort_per_acre", ascending=False))
            if PLOTLY_AVAILABLE:
                fig = px.bar(agent_tpa, x="tpamort_per_acre", y="agent_label", orientation="h",
                             color="component_type",
                             color_discrete_map={"natural": "#e15759", "harvest": "#4e79a7"})
                fig.update_layout(yaxis=dict(autorange="reversed"))
                source_plotly_chart(dark_fig(fig), source_line=597, use_container_width=True)

            if "state" in mort_plot.columns and PLOTLY_AVAILABLE:
                nat_harv = (mort_plot.groupby(["state", "component_type"])
                             ["tpamort_per_acre"].sum().reset_index())
                nat_harv, state_order_present = apply_state_order(nat_harv, "state")
                fig2 = px.bar(nat_harv, x="tpamort_per_acre", y="state", color="component_type",
                               color_discrete_map={"natural": "#e15759", "harvest": "#4e79a7"},
                              barmode="stack", orientation="h", labels={"state": "State"})
                fig2 = finish_state_bar(fig2, state_order_present)
                source_plotly_chart(fig2, source_line=607, use_container_width=True)

    with seed_col:
        st.subheader("Seedling regeneration")
        if seed_df is None:
            st.info("plot_seedling_metrics.parquet not found.")
        else:
            if "state" in seed_df.columns:
                sw_cols = [c for c in ["count_softwood", "count_hardwood"] if c in seed_df.columns]
                if sw_cols and PLOTLY_AVAILABLE:
                    seed_state = (seed_df.groupby("state")[sw_cols].sum().reset_index()
                                  .rename(columns={"count_softwood": "Softwood",
                                                    "count_hardwood": "Hardwood"}))
                    sw_melt = seed_state.melt(id_vars="state",
                                               value_vars=[c for c in ["Softwood", "Hardwood"]
                                                           if c in seed_state.columns],
                                               var_name="Type", value_name="Seedling count")
                    sw_melt, state_order_present = apply_state_order(sw_melt, "state")
                    fig = px.bar(sw_melt, x="Seedling count", y="state", color="Type", barmode="stack",
                                 orientation="h",
                                 color_discrete_map={"Softwood": "#f28e2b", "Hardwood": "#59a14f"},
                                 labels={"state": "State"})
                    fig = finish_state_bar(fig, state_order_present)
                    source_plotly_chart(fig, source_line=629, use_container_width=True)

            if "shannon_h_count" in seed_df.columns and "state" in seed_df.columns and PLOTLY_AVAILABLE:
                seed_div = (seed_df.groupby("state")["shannon_h_count"].mean().reset_index()
                            .rename(columns={"shannon_h_count": "Mean Shannon H"}))
                seed_div, state_order_present = apply_state_order(seed_div, "state")
                fig = px.bar(seed_div, x="Mean Shannon H", y="state", orientation="h",
                             color_discrete_sequence=["#76b7b2"], labels={"state": "State"})
                fig = finish_state_bar(fig, state_order_present)
                source_plotly_chart(fig, source_line=640, use_container_width=True)

# ==============================================================================
# TAB 7 — TREATMENT HISTORY
# ==============================================================================
with tab_treatments:
    if treat_df is None:
        st.info("plot_treatment_history.parquet not found.")
    else:
        n_treat = len(treat_df)
        n_plots_treat = treat_df["PLT_CN"].nunique() if "PLT_CN" in treat_df.columns else "—"
        c1, c2, c3 = st.columns(3)
        c1.markdown(metric_card("Treatment Records", f"{n_treat:,}", "non-zero TRTCD rows"), unsafe_allow_html=True)
        c2.markdown(metric_card("Plots with Treatments", f"{n_plots_treat:,}", "distinct PLT_CN"), unsafe_allow_html=True)
        c3.markdown(metric_card("Treatment Types", "5", "TRTCD 10/20/30/40/50"), unsafe_allow_html=True)

        st.markdown("---")

        if "treatment_category" in treat_df.columns and PLOTLY_AVAILABLE:
            col1, col2 = st.columns(2)
            with col1:
                st.subheader("By treatment category")
                cat_counts = (treat_df.groupby(["treatment_label", "treatment_category"])
                              .size().reset_index(name="count")
                              .sort_values("count", ascending=False))
                CAT_COLORS = {"harvest": "#e15759", "site_prep": "#f28e2b",
                              "regeneration": "#59a14f", "other_silv": "#4e79a7"}
                fig = px.bar(cat_counts, x="count", y="treatment_label", orientation="h",
                             color="treatment_category", color_discrete_map=CAT_COLORS,
                             labels={"treatment_label": "", "count": "Records",
                                     "treatment_category": "Category"})
                fig.update_layout(yaxis=dict(autorange="reversed"))
                source_plotly_chart(dark_fig(fig), source_line=670, use_container_width=True)

            with col2:
                st.subheader("By state")
                if "STATECD" in treat_df.columns or "state" in treat_df.columns:
                    state_col = "state" if "state" in treat_df.columns else "STATECD"
                    state_treat = (treat_df.groupby([state_col, "treatment_category"])
                                    .size().reset_index(name="count"))
                    state_treat, state_order_present = apply_state_order(state_treat, state_col)
                    fig = px.bar(state_treat, x="count", y=state_col, color="treatment_category",
                                  color_discrete_map=CAT_COLORS, barmode="stack",
                                  orientation="h",
                                  labels={state_col: "State", "count": "Records"})
                    fig = finish_state_bar(fig, state_order_present)
                    source_plotly_chart(fig, source_line=684, use_container_width=True)

        if "TRTYR" in treat_df.columns and PLOTLY_AVAILABLE:
            st.subheader("Treatment year distribution")
            year_raw = pd.to_numeric(treat_df["TRTYR"], errors="coerce")
            valid_year = year_raw.notna() & (year_raw >= 1900) & (year_raw < 9999)

            if "INVYR" in treat_df.columns:
                invyr = pd.to_numeric(treat_df["INVYR"], errors="coerce")
                valid_year &= invyr.notna() & (year_raw <= invyr)

            tyr = treat_df.loc[valid_year].copy()
            invalid_tyr = treat_df.loc[year_raw.notna() & ~valid_year].copy()
            if len(tyr) > 0:
                tyr["treatment_year"] = year_raw.loc[valid_year].astype("int64")
                color_col = "treatment_category" if "treatment_category" in tyr.columns else None
                group_cols = ["treatment_year"] + ([color_col] if color_col else [])
                year_counts = tyr.groupby(group_cols, dropna=False).size().reset_index(name="records")

                fig = px.bar(
                    year_counts,
                    x="treatment_year",
                    y="records",
                    color=color_col,
                    color_discrete_map=CAT_COLORS if color_col else None,
                    labels={"treatment_year": "Treatment Year", "records": "Treatment records"},
                )
                fig.update_xaxes(tickformat="d", dtick=5)
                source_plotly_chart(dark_fig(fig), source_line=708, use_container_width=True)
                st.caption(
                    f"Plotted {len(tyr):,} records with calendar years from "
                    f"{tyr['treatment_year'].min()}-{tyr['treatment_year'].max()}. "
                    f"Filtered {len(invalid_tyr):,} non-calendar or post-inventory `TRTYR` values "
                    "such as `1`, `9999`, or years later than `INVYR`."
                )

                if len(invalid_tyr) > 0:
                    invalid_tyr = invalid_tyr.assign(
                        TRTYR_raw=year_raw.loc[invalid_tyr.index].astype("Int64")
                    )
                    invalid_cols = [c for c in [
                        "TRTYR_raw", "TRTCD", "treatment_label", "treatment_category", "INVYR"
                    ] if c in invalid_tyr.columns]
                    invalid_summary = (
                        invalid_tyr[invalid_cols]
                        .groupby(invalid_cols, dropna=False)
                        .size()
                        .reset_index(name="records")
                        .sort_values("records", ascending=False)
                        .head(12)
                    )
                    with st.expander("Filtered treatment-year values"):
                        st.dataframe(invalid_summary, use_container_width=True, hide_index=True)

        st.markdown("---")
        st.markdown(
            "**TRTCD lookup table:**\n\n"
            "| TRTCD | Label | Category |\n"
            "|-------|-------|----------|\n"
            "| 10 | Cutting | harvest |\n"
            "| 20 | Site preparation | site_prep |\n"
            "| 30 | Artificial regeneration | regeneration |\n"
            "| 40 | Natural regeneration | regeneration |\n"
            "| 50 | Other silvicultural treatment | other_silv |\n"
        )

# ==============================================================================
# TAB 8 — SITE CLIMATE
# ==============================================================================
with tab_climate:
    st.markdown(
        "Point-based TerraClimate extraction for **6,956 site locations** — "
        "2,070 FIA plots and 4,886 ITRDB chronology sites — via Google Earth Engine. "
        "6 variables, 1958–2024. This is a secondary/example product separate from "
        "the main FIA forest inventory analysis above."
    )

    clim_path = cp("site_climate.parquet")
    clim_exists = os.path.isfile(clim_path)

    if not clim_exists:
        st.info(
            "`site_climate.parquet` not found.  \n"
            "Generate with `Rscript 05_fia/scripts/06_extract_site_climate.R`."
        )
    else:
        clim_meta = parquet_meta(clim_path)
        n_rows = f"{clim_meta['rows']:,}" if clim_meta.get("rows") else "23,468,680"

        c1, c2, c3, c4 = st.columns(4)
        c1.markdown(metric_card("Rows",   n_rows,    "site×yr×month×variable"), unsafe_allow_html=True)
        c2.markdown(metric_card("Sites",  "6,956",   "2,070 FIA + 4,886 ITRDB"), unsafe_allow_html=True)
        c3.markdown(metric_card("Period", "1958–2024", "calendar years"),        unsafe_allow_html=True)
        c4.markdown(metric_card("Vars",   "6",       "tmmx tmmn pr def pet aet"), unsafe_allow_html=True)

        if clim_meta.get("columns"):
            st.markdown("**Schema:**")
            schema_df = pd.DataFrame({
                "Column": clim_meta["columns"],
                "Type":   clim_meta.get("dtypes", ["—"] * len(clim_meta["columns"])),
            })
            st.dataframe(schema_df, use_container_width=True, hide_index=True,
                         height=min(300, 35 * len(clim_meta["columns"]) + 40))

        st.info(
            "ℹ️ File is 23.5M rows — not loaded in the dashboard. "
            "Use `demo_03_site_climate.R` or the R/Python snippets below for analysis."
        )

        st.markdown("---")
        st.subheader("Load in R")
        st.code(
            'library(arrow); library(dplyr)\n'
            'clim <- read_parquet("05_fia/data/processed/site_climate/site_climate.parquet")\n\n'
            '# Annual water-year precipitation per site\n'
            'clim |> filter(variable == "pr") |>\n'
            '  group_by(site_id, water_year) |>\n'
            '  summarise(precip_mm = sum(value, na.rm = TRUE))',
            language="r",
        )
