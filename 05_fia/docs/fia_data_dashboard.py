# ==============================================================================
# fia_data_dashboard.py
# FIA Compiled Data Explorer — Streamlit dashboard
#
# Visualizes the processed parquets produced by the 05_fia pipeline:
#   plot_tree_metrics.parquet       — BA, diversity, size class, canopy layer
#   plot_disturbance_history.parquet — fire/insect/disease disturbance events
#   plot_damage_agents.parquet      — specific insect/disease agent codes
#   plot_mortality_metrics.parquet  — natural and harvest mortality by agent
#   plot_seedling_metrics.parquet   — seedling regeneration by species group
#
# Usage:
#   streamlit run 05_fia/docs/fia_data_dashboard.py
#
# Data directory is set via the sidebar or FIA_DATA_DIR environment variable.
# The dashboard degrades gracefully when parquets are not yet available.
# ==============================================================================

import os
import math
from pathlib import Path

import pandas as pd
import streamlit as st
import streamlit.components.v1 as components

try:
    import plotly.express as px
    import plotly.graph_objects as go
    PLOTLY_AVAILABLE = True
except ImportError:
    PLOTLY_AVAILABLE = False

# ------------------------------------------------------------------------------
# Page config
# ------------------------------------------------------------------------------

st.set_page_config(
    page_title="FIA Data Explorer",
    page_icon="🌲",
    layout="wide",
    initial_sidebar_state="expanded",
)

# Dark-theme CSS matching fiadb_dashboard.py
st.markdown("""
<style>
  .stApp { background-color: #0e1117; color: #ddd; }
  .block-container { padding-top: 1.2rem; }
  .metric-card {
    background: #161b22;
    border: 1px solid #30363d;
    border-radius: 8px;
    padding: 14px 18px 10px;
    margin-bottom: 8px;
  }
  .metric-card .label { font-size: 11px; color: #888; text-transform: uppercase; letter-spacing: .05em; }
  .metric-card .value { font-size: 26px; font-weight: 700; color: #fff; }
  .metric-card .sub   { font-size: 11px; color: #666; margin-top: 2px; }
  .file-ok   { color: #3fb950; font-weight: 600; }
  .file-miss { color: #f85149; }
  .cat-badge {
    display: inline-block;
    padding: 2px 9px;
    border-radius: 4px;
    font-size: 11px;
    font-weight: 600;
    color: #fff;
    margin-right: 4px;
  }
</style>
""", unsafe_allow_html=True)

# ------------------------------------------------------------------------------
# Category colours (disturbance / damage agent)
# ------------------------------------------------------------------------------

DIST_COLORS = {
    "fire":       "#e15759",
    "insects":    "#59a14f",
    "disease":    "#f28e2b",
    "weather":    "#4e79a7",
    "animal":     "#9c755f",
    "vegetation": "#76b7b2",
    "geologic":   "#bab0ac",
    "other":      "#8c8c8c",
}

AGENT_COLORS = {
    "bark beetles":          "#e15759",
    "defoliators":           "#59a14f",
    "sucking insects":       "#76b7b2",
    "boring insects":        "#edc948",
    "insects":               "#b6d77a",
    "root/butt disease":     "#f28e2b",
    "canker/rust":           "#ff9da7",
    "foliage/wilt disease":  "#9c755f",
    "disease":               "#ffbf7f",
    "fire":                  "#e15759",
    "complex":               "#b07aa1",
    "abiotic":               "#4e79a7",
    "human":                 "#bab0ac",
    "other":                 "#8c8c8c",
    "unknown":               "#555555",
}

# ------------------------------------------------------------------------------
# Expected output files
# ------------------------------------------------------------------------------

EXPECTED_FILES = {
    "plot_tree_metrics.parquet":        "Tree metrics (BA, diversity, size class)",
    "plot_disturbance_history.parquet": "Disturbance history (fire/insect/disease)",
    "plot_damage_agents.parquet":       "Damage agents (specific species/agents)",
    "plot_mortality_metrics.parquet":   "Mortality (natural & harvest)",
    "plot_seedling_metrics.parquet":    "Seedling regeneration",
    "plot_cond_fortypcd.parquet":       "Condition / forest type",
}

# US state abbreviations → FIPS numeric codes for plotly choropleth
STATE_FIPS = {
    "AL": "01","AK": "02","AZ": "04","AR": "05","CA": "06","CO": "08",
    "CT": "09","DE": "10","FL": "12","GA": "13","HI": "15","ID": "16",
    "IL": "17","IN": "18","IA": "19","KS": "20","KY": "21","LA": "22",
    "ME": "23","MD": "24","MA": "25","MI": "26","MN": "27","MS": "28",
    "MO": "29","MT": "30","NE": "31","NV": "32","NH": "33","NJ": "34",
    "NM": "35","NY": "36","NC": "37","ND": "38","OH": "39","OK": "40",
    "OR": "41","PA": "42","RI": "44","SC": "45","SD": "46","TN": "47",
    "TX": "48","UT": "49","VT": "50","VA": "51","WA": "53","WV": "54",
    "WI": "55","WY": "56",
}

# ------------------------------------------------------------------------------
# Data loaders
# ------------------------------------------------------------------------------

@st.cache_data(show_spinner=False)
def load_parquet(path: str):
    """Load a parquet file; return (df, None) or (None, error_msg)."""
    if not os.path.isfile(path):
        return None, f"Not found: {path}"
    try:
        return pd.read_parquet(path), None
    except Exception as e:
        return None, str(e)

def data_path(data_dir: str, filename: str) -> str:
    return str(Path(data_dir) / filename)

# ------------------------------------------------------------------------------
# Plotly helpers
# ------------------------------------------------------------------------------

PLOTLY_DARK = dict(
    plot_bgcolor="#0e1117",
    paper_bgcolor="#0e1117",
    font_color="#ddd",
    xaxis=dict(gridcolor="#222", linecolor="#444"),
    yaxis=dict(gridcolor="#222", linecolor="#444"),
)

def dark_fig(fig):
    fig.update_layout(**PLOTLY_DARK, margin=dict(l=40, r=20, t=30, b=40))
    return fig

def scatter_map(df, lat_col, lon_col, color_col, color_map=None,
                title="", hover_name=None, size=3):
    """plotly scatter_geo map limited to USA."""
    kwargs = dict(
        lat=lat_col, lon=lon_col, color=color_col,
        scope="usa", title=title, opacity=0.7,
        hover_name=hover_name,
    )
    if color_map:
        kwargs["color_discrete_map"] = color_map
    fig = px.scatter_geo(df.dropna(subset=[lat_col, lon_col, color_col]),
                         **kwargs)
    fig.update_traces(marker_size=size)
    fig.update_layout(
        paper_bgcolor="#0e1117",
        plot_bgcolor="#0e1117",
        geo=dict(bgcolor="#0e1117", landcolor="#1c2128",
                 lakecolor="#0e1117", coastlinecolor="#444",
                 showland=True, showlakes=True, showcoastlines=True),
        font_color="#ddd",
        margin=dict(l=0, r=0, t=30, b=0),
        legend=dict(bgcolor="#161b22", bordercolor="#30363d", borderwidth=1),
    )
    return fig

def metric_card(label, value, sub=""):
    return (f'<div class="metric-card">'
            f'<div class="label">{label}</div>'
            f'<div class="value">{value}</div>'
            f'<div class="sub">{sub}</div>'
            f'</div>')

# ------------------------------------------------------------------------------
# Main
# ------------------------------------------------------------------------------

def main():
    st.title("FIA Compiled Data — Explorer")
    st.caption("Visualizes processed parquets from the 05_fia pipeline.")

    # ── Sidebar ──────────────────────────────────────────────────────────────
    with st.sidebar:
        st.header("Data")
        default_dir = os.environ.get(
            "FIA_DATA_DIR",
            str(Path(__file__).parent.parent / "data" / "processed" / "summaries")
        )
        data_dir = st.text_input(
            "Summaries directory",
            value=default_dir,
            help="Path to 05_fia/data/processed/summaries/"
        )
        st.markdown("---")
        st.markdown("**Filters**")
        st.caption("Applied on Forests, Disturbance, and Mortality tabs.")

    # Pre-load all datasets (fast cache hits after first load)
    with st.spinner("Loading data…"):
        tree_df,    tree_err    = load_parquet(data_path(data_dir, "plot_tree_metrics.parquet"))
        disturb_df, disturb_err = load_parquet(data_path(data_dir, "plot_disturbance_history.parquet"))
        agents_df,  agents_err  = load_parquet(data_path(data_dir, "plot_damage_agents.parquet"))
        mort_df,    mort_err    = load_parquet(data_path(data_dir, "plot_mortality_metrics.parquet"))
        seed_df,    seed_err    = load_parquet(data_path(data_dir, "plot_seedling_metrics.parquet"))

    # Sidebar filters (populated after loading)
    with st.sidebar:
        all_states = sorted(tree_df["state"].dropna().unique().tolist()) if tree_df is not None else []
        sel_states = st.multiselect("States", all_states, placeholder="All states")

        if tree_df is not None and "INVYR" in tree_df.columns:
            yr_min = int(tree_df["INVYR"].min())
            yr_max = int(tree_df["INVYR"].max())
            sel_years = st.slider("Inventory years", yr_min, yr_max, (yr_min, yr_max))
        else:
            sel_years = (2000, 2024)

    def apply_filters(df):
        if df is None:
            return None
        out = df.copy()
        if sel_states and "state" in out.columns:
            out = out[out["state"].isin(sel_states)]
        if "INVYR" in out.columns:
            out = out[(out["INVYR"] >= sel_years[0]) & (out["INVYR"] <= sel_years[1])]
        return out

    tree_f    = apply_filters(tree_df)
    disturb_f = apply_filters(disturb_df)
    agents_f  = apply_filters(agents_df)
    mort_f    = apply_filters(mort_df)
    seed_f    = apply_filters(seed_df)

    # ── Tabs ─────────────────────────────────────────────────────────────────
    tab_overview, tab_forests, tab_disturb, tab_agents, tab_mort = st.tabs([
        "📋 Overview",
        "🌲 Forests",
        "🔥 Disturbance",
        "🪲 Damage Agents",
        "💀 Mortality & Regeneration",
    ])

    # ==========================================================================
    # TAB 1 — OVERVIEW
    # ==========================================================================
    with tab_overview:
        st.subheader("Data Availability")

        # File availability table
        rows = []
        for fname, desc in EXPECTED_FILES.items():
            fpath = data_path(data_dir, fname)
            exists = os.path.isfile(fpath)
            if exists:
                df_tmp, _ = load_parquet(fpath)
                size_mb = os.path.getsize(fpath) / 1e6
                n_rows = len(df_tmp) if df_tmp is not None else "—"
                n_states = (df_tmp["state"].nunique()
                            if df_tmp is not None and "state" in df_tmp.columns else "—")
                yr_range = (
                    f"{int(df_tmp['INVYR'].min())}–{int(df_tmp['INVYR'].max())}"
                    if df_tmp is not None and "INVYR" in df_tmp.columns else "—"
                )
            else:
                size_mb, n_rows, n_states, yr_range = None, "—", "—", "—"

            rows.append({
                "File":        fname,
                "Description": desc,
                "Status":      "✓" if exists else "✗",
                "Size (MB)":   f"{size_mb:.1f}" if size_mb is not None else "—",
                "Rows":        f"{n_rows:,}" if isinstance(n_rows, int) else n_rows,
                "States":      n_states,
                "Years":       yr_range,
            })

        avail_df = pd.DataFrame(rows)

        # Colour the Status column
        def color_status(val):
            return "color: #3fb950" if val == "✓" else "color: #f85149"

        st.dataframe(
            avail_df.style.applymap(color_status, subset=["Status"]),
            use_container_width=True,
            hide_index=True,
        )

        # Summary stat cards
        if tree_df is not None:
            n_plots  = tree_df["PLT_CN"].nunique()
            n_states = tree_df["state"].nunique() if "state" in tree_df.columns else "—"
            yr_min_v = int(tree_df["INVYR"].min())
            yr_max_v = int(tree_df["INVYR"].max())
            n_visits = len(tree_df["PLT_CN"])

            cols = st.columns(4)
            cols[0].markdown(metric_card("Unique Plots", f"{n_plots:,}",
                                         "distinct PLT_CN"), unsafe_allow_html=True)
            cols[1].markdown(metric_card("States", str(n_states),
                                         "with tree data"), unsafe_allow_html=True)
            cols[2].markdown(metric_card("Year Range",
                                         f"{yr_min_v}–{yr_max_v}",
                                         "INVYR"), unsafe_allow_html=True)
            cols[3].markdown(metric_card("Plot Visits", f"{n_visits:,}",
                                         "PLT_CN × INVYR rows"), unsafe_allow_html=True)

            # INVYR distribution
            st.markdown("**Plot visits by inventory year**")
            yr_counts = tree_df.groupby("INVYR")["PLT_CN"].nunique().reset_index()
            yr_counts.columns = ["INVYR", "n_plots"]
            if PLOTLY_AVAILABLE:
                fig = px.bar(yr_counts, x="INVYR", y="n_plots",
                             labels={"INVYR": "Inventory Year", "n_plots": "Unique Plots"},
                             color_discrete_sequence=["#4e79a7"])
                st.plotly_chart(dark_fig(fig), use_container_width=True)
            else:
                st.bar_chart(yr_counts.set_index("INVYR"))

            # State coverage
            if "state" in tree_df.columns:
                st.markdown("**Plots per state**")
                state_counts = (tree_df.groupby("state")["PLT_CN"]
                                .nunique().reset_index()
                                .rename(columns={"PLT_CN": "n_plots"})
                                .sort_values("n_plots", ascending=False))
                if PLOTLY_AVAILABLE:
                    fig2 = px.bar(state_counts, x="state", y="n_plots",
                                  labels={"state": "State", "n_plots": "Unique Plots"},
                                  color_discrete_sequence=["#59a14f"])
                    st.plotly_chart(dark_fig(fig2), use_container_width=True)
                else:
                    st.bar_chart(state_counts.set_index("state"))
        else:
            st.info("Run `Rscript 05_fia/scripts/05_build_fia_summaries.R` to generate the summary parquets.")

    # ==========================================================================
    # TAB 2 — FORESTS
    # ==========================================================================
    with tab_forests:
        if tree_f is None or len(tree_f) == 0:
            st.info("plot_tree_metrics.parquet not available or no rows match the current filter.")
        else:
            if not PLOTLY_AVAILABLE:
                st.warning("Install `plotly` for interactive charts.")

            # Map
            if "LAT" in tree_f.columns and "LON" in tree_f.columns and PLOTLY_AVAILABLE:
                st.subheader("Plot Locations")
                map_metric = st.radio(
                    "Color by",
                    ["ba_live_total", "shannon_h_ba", "n_species_live"],
                    horizontal=True,
                    key="forest_map_metric",
                )
                map_df = tree_f.dropna(subset=["LAT", "LON", map_metric])
                # Sample for performance if large
                if len(map_df) > 50_000:
                    map_df = map_df.sample(50_000, random_state=42)
                labels = {
                    "ba_live_total": "Live BA (ft²/acre)",
                    "shannon_h_ba":  "Shannon H (BA-weighted)",
                    "n_species_live": "Live species richness",
                }
                fig = px.scatter_geo(
                    map_df,
                    lat="LAT", lon="LON",
                    color=map_metric,
                    color_continuous_scale="Viridis",
                    scope="usa",
                    labels={map_metric: labels.get(map_metric, map_metric)},
                    opacity=0.6,
                )
                fig.update_traces(marker_size=3)
                fig.update_layout(
                    paper_bgcolor="#0e1117",
                    geo=dict(bgcolor="#0e1117", landcolor="#1c2128",
                             lakecolor="#0e1117", coastlinecolor="#444",
                             showland=True, showlakes=True, showcoastlines=True),
                    font_color="#ddd",
                    coloraxis_colorbar=dict(bgcolor="#161b22",
                                            tickcolor="#ddd", title_font_color="#ddd"),
                    margin=dict(l=0, r=0, t=10, b=0),
                )
                st.plotly_chart(fig, use_container_width=True)

            # BA distributions
            col1, col2 = st.columns(2)

            with col1:
                st.subheader("Live Basal Area distribution")
                if "ba_live_total" in tree_f.columns and PLOTLY_AVAILABLE:
                    fig = px.histogram(
                        tree_f[tree_f["ba_live_total"] > 0],
                        x="ba_live_total", nbins=60,
                        labels={"ba_live_total": "Live BA (ft²/acre)"},
                        color_discrete_sequence=["#59a14f"],
                    )
                    st.plotly_chart(dark_fig(fig), use_container_width=True)

            with col2:
                st.subheader("Shannon Diversity (BA-weighted)")
                if "shannon_h_ba" in tree_f.columns and PLOTLY_AVAILABLE:
                    fig = px.histogram(
                        tree_f[tree_f["shannon_h_ba"] > 0],
                        x="shannon_h_ba", nbins=50,
                        labels={"shannon_h_ba": "Shannon H"},
                        color_discrete_sequence=["#4e79a7"],
                    )
                    st.plotly_chart(dark_fig(fig), use_container_width=True)

            # Softwood vs hardwood by state
            if all(c in tree_f.columns for c in ["ba_live_softwood", "ba_live_hardwood", "state"]):
                st.subheader("Softwood vs. Hardwood BA by state")
                sw_hw = (tree_f.groupby("state")[["ba_live_softwood", "ba_live_hardwood"]]
                         .mean().reset_index()
                         .rename(columns={"ba_live_softwood": "Softwood", "ba_live_hardwood": "Hardwood"})
                         .sort_values("Softwood", ascending=False))
                if PLOTLY_AVAILABLE:
                    fig = px.bar(
                        sw_hw.melt(id_vars="state", var_name="Type", value_name="Mean BA"),
                        x="state", y="Mean BA", color="Type",
                        color_discrete_map={"Softwood": "#f28e2b", "Hardwood": "#59a14f"},
                        labels={"state": "State", "Mean BA": "Mean Live BA (ft²/acre)"},
                        barmode="stack",
                    )
                    st.plotly_chart(dark_fig(fig), use_container_width=True)

            # Size class by state
            size_cols = [c for c in ["ba_live_sapling", "ba_live_intermediate", "ba_live_mature"]
                         if c in tree_f.columns]
            if size_cols and "state" in tree_f.columns:
                st.subheader("Size class BA by state")
                sz = (tree_f.groupby("state")[size_cols].mean().reset_index())
                rename_map = {"ba_live_sapling": "Sapling",
                              "ba_live_intermediate": "Intermediate",
                              "ba_live_mature": "Mature"}
                sz = sz.rename(columns=rename_map)
                sz_melt = sz.melt(id_vars="state",
                                  value_vars=[rename_map[c] for c in size_cols if c in rename_map],
                                  var_name="Size class", value_name="Mean BA")
                if PLOTLY_AVAILABLE:
                    fig = px.bar(
                        sz_melt, x="state", y="Mean BA", color="Size class",
                        color_discrete_map={"Sapling": "#76b7b2",
                                            "Intermediate": "#59a14f",
                                            "Mature": "#4e79a7"},
                        barmode="stack",
                        labels={"Mean BA": "Mean Live BA (ft²/acre)", "state": "State"},
                    )
                    st.plotly_chart(dark_fig(fig), use_container_width=True)

    # ==========================================================================
    # TAB 3 — DISTURBANCE
    # ==========================================================================
    with tab_disturb:
        if disturb_f is None or len(disturb_f) == 0:
            st.info("plot_disturbance_history.parquet not available or no rows match the current filter.\n\n"
                    "This file is produced by Step 5 of `05_build_fia_summaries.R`, which requires "
                    "that `03_extract_trees.R` has been re-run to include DSTRBCD fields.")
        else:
            col1, col2 = st.columns([1, 1])

            with col1:
                st.subheader("Disturbance events by category")
                cat_counts = (disturb_f.groupby("disturbance_category")
                              .size().reset_index(name="count")
                              .sort_values("count", ascending=False))
                if PLOTLY_AVAILABLE:
                    fig = px.bar(
                        cat_counts, x="disturbance_category", y="count",
                        color="disturbance_category",
                        color_discrete_map=DIST_COLORS,
                        labels={"disturbance_category": "Category",
                                "count": "Condition-disturbance records"},
                    )
                    fig.update_layout(showlegend=False)
                    st.plotly_chart(dark_fig(fig), use_container_width=True)

            with col2:
                st.subheader("Fire type breakdown")
                fire_df = disturb_f[disturb_f["disturbance_category"] == "fire"]
                if len(fire_df) > 0:
                    fire_counts = (fire_df.groupby("disturbance_label")
                                   .size().reset_index(name="count"))
                    if PLOTLY_AVAILABLE:
                        fig = px.pie(
                            fire_counts, names="disturbance_label", values="count",
                            color_discrete_sequence=["#e15759", "#ff9d9a", "#c85250"],
                        )
                        fig.update_layout(paper_bgcolor="#0e1117", font_color="#ddd",
                                          legend=dict(bgcolor="#161b22"))
                        st.plotly_chart(fig, use_container_width=True)
                else:
                    st.caption("No fire disturbance records in current filter.")

            # Disturbance year histogram
            if "DSTRBYR" in disturb_f.columns:
                st.subheader("Year of disturbance")
                yr_df = disturb_f[disturb_f["DSTRBYR"].notna() & (disturb_f["DSTRBYR"] != 9999)]
                if len(yr_df) > 0 and PLOTLY_AVAILABLE:
                    fig = px.histogram(
                        yr_df, x="DSTRBYR", color="disturbance_category",
                        color_discrete_map=DIST_COLORS, nbins=50,
                        labels={"DSTRBYR": "Disturbance Year",
                                "disturbance_category": "Category"},
                    )
                    st.plotly_chart(dark_fig(fig), use_container_width=True)

            # Top labels
            st.subheader("Top disturbance types")
            label_counts = (disturb_f.groupby(["disturbance_label", "disturbance_category"])
                            .size().reset_index(name="count")
                            .sort_values("count", ascending=False)
                            .head(20))
            if PLOTLY_AVAILABLE:
                fig = px.bar(
                    label_counts,
                    x="count", y="disturbance_label", orientation="h",
                    color="disturbance_category",
                    color_discrete_map=DIST_COLORS,
                    labels={"disturbance_label": "", "count": "Records",
                            "disturbance_category": "Category"},
                )
                fig.update_layout(yaxis=dict(autorange="reversed"))
                st.plotly_chart(dark_fig(fig), use_container_width=True)

            # Map
            if (all(c in disturb_f.columns for c in ["LAT", "LON", "disturbance_category"])
                    and PLOTLY_AVAILABLE):
                st.subheader("Disturbance event locations")
                map_df = disturb_f.dropna(subset=["LAT", "LON"])
                if len(map_df) > 50_000:
                    map_df = map_df.sample(50_000, random_state=42)
                fig = scatter_map(
                    map_df, "LAT", "LON", "disturbance_category",
                    color_map=DIST_COLORS,
                    hover_name="disturbance_label",
                )
                st.plotly_chart(fig, use_container_width=True)

    # ==========================================================================
    # TAB 4 — DAMAGE AGENTS
    # ==========================================================================
    with tab_agents:
        if agents_f is None or len(agents_f) == 0:
            st.info("plot_damage_agents.parquet not available or no rows match the current filter.\n\n"
                    "This file requires re-running `03_extract_trees.R` (adds DAMAGE_AGENT_CD1/2/3) "
                    "and `05_build_fia_summaries.R` Step 6.")
        else:
            col1, col2 = st.columns([1, 1])

            with col1:
                st.subheader("Top 20 damage agents (by affected TPA)")
                has_label = agents_f[agents_f["agent_label"].notna()]
                if len(has_label) > 0:
                    top_agents = (has_label.groupby(["agent_label", "agent_category"])
                                  ["n_trees_tpa"].sum().reset_index()
                                  .sort_values("n_trees_tpa", ascending=False)
                                  .head(20))
                    if PLOTLY_AVAILABLE:
                        fig = px.bar(
                            top_agents, x="n_trees_tpa", y="agent_label",
                            orientation="h", color="agent_category",
                            color_discrete_map=AGENT_COLORS,
                            labels={"n_trees_tpa": "Affected TPA (sum)",
                                    "agent_label": "", "agent_category": "Category"},
                        )
                        fig.update_layout(yaxis=dict(autorange="reversed"))
                        st.plotly_chart(dark_fig(fig), use_container_width=True)

            with col2:
                st.subheader("BA affected by agent category")
                cat_ba = (agents_f[agents_f["agent_category"].notna()]
                          .groupby("agent_category")["ba_per_acre"].sum().reset_index()
                          .sort_values("ba_per_acre", ascending=False))
                if PLOTLY_AVAILABLE:
                    fig = px.bar(
                        cat_ba, x="ba_per_acre", y="agent_category",
                        orientation="h", color="agent_category",
                        color_discrete_map=AGENT_COLORS,
                        labels={"ba_per_acre": "Affected BA (ft²/acre, sum)",
                                "agent_category": ""},
                    )
                    fig.update_layout(showlegend=False,
                                      yaxis=dict(autorange="reversed"))
                    st.plotly_chart(dark_fig(fig), use_container_width=True)

            # Agent × state heatmap
            if "state" in agents_f.columns and PLOTLY_AVAILABLE:
                st.subheader("Damage agent category × state heatmap")
                heat_df = (agents_f[agents_f["agent_category"].notna()]
                           .groupby(["state", "agent_category"])
                           ["n_trees_tpa"].sum().reset_index())
                if len(heat_df) > 0:
                    heat_pivot = heat_df.pivot(
                        index="agent_category", columns="state", values="n_trees_tpa"
                    ).fillna(0)
                    fig = px.imshow(
                        heat_pivot,
                        color_continuous_scale="YlOrRd",
                        labels={"x": "State", "y": "Agent category",
                                "color": "Affected TPA (sum)"},
                        aspect="auto",
                    )
                    fig.update_layout(paper_bgcolor="#0e1117", font_color="#ddd",
                                      coloraxis_colorbar=dict(bgcolor="#161b22",
                                                              tickcolor="#ddd"),
                                      margin=dict(l=140, r=20, t=20, b=60))
                    st.plotly_chart(fig, use_container_width=True)

            # Map
            if all(c in agents_f.columns for c in ["LAT", "LON", "agent_category"]) and PLOTLY_AVAILABLE:
                st.subheader("Damage agent locations")
                map_df = agents_f.dropna(subset=["LAT", "LON", "agent_category"])
                if len(map_df) > 50_000:
                    map_df = map_df.sample(50_000, random_state=42)

                # DAMAGE_AGENT_CD is on trees; need to join LAT/LON from tree_df
                # (damage_agents parquet doesn't carry coordinates directly)
                if tree_df is not None and "LAT" in tree_df.columns:
                    coord = tree_df[["PLT_CN", "LAT", "LON"]].drop_duplicates("PLT_CN")
                    map_df2 = agents_f[agents_f["agent_category"].notna()].merge(
                        coord, on="PLT_CN", how="left"
                    ).dropna(subset=["LAT", "LON"])
                    if len(map_df2) > 50_000:
                        map_df2 = map_df2.sample(50_000, random_state=42)
                    fig = scatter_map(
                        map_df2, "LAT", "LON", "agent_category",
                        color_map=AGENT_COLORS,
                        hover_name="agent_label",
                    )
                    st.plotly_chart(fig, use_container_width=True)
                else:
                    st.caption("LAT/LON not yet available — re-run the pipeline to add coordinates.")

    # ==========================================================================
    # TAB 5 — MORTALITY & REGENERATION
    # ==========================================================================
    with tab_mort:
        mort_col, seed_col = st.columns(2)

        with mort_col:
            st.subheader("Mortality by agent")
            if mort_f is None or len(mort_f) == 0:
                st.info("plot_mortality_metrics.parquet not available.")
            else:
                AGENTCD_LABELS = {
                    10: "Insect", 20: "Disease", 30: "Fire",
                    40: "Animal", 50: "Weather", 60: "Vegetation",
                    70: "Unknown", 80: "Harvest",
                }
                mort_plot = mort_f.copy()
                mort_plot["agent_label"] = (mort_plot["AGENTCD"]
                                            .map(AGENTCD_LABELS)
                                            .fillna("Other"))
                agent_tpa = (mort_plot.groupby(["agent_label", "component_type"])
                             ["tpamort_per_acre"].sum().reset_index()
                             .sort_values("tpamort_per_acre", ascending=False))
                if PLOTLY_AVAILABLE:
                    fig = px.bar(
                        agent_tpa, x="tpamort_per_acre", y="agent_label",
                        color="component_type",
                        color_discrete_map={"natural": "#e15759", "harvest": "#4e79a7"},
                        orientation="h",
                        labels={"tpamort_per_acre": "TPA Mortality (sum)",
                                "agent_label": "", "component_type": "Type"},
                    )
                    fig.update_layout(yaxis=dict(autorange="reversed"))
                    st.plotly_chart(dark_fig(fig), use_container_width=True)

                if "state" in mort_plot.columns:
                    nat_harv = (mort_plot.groupby(["state", "component_type"])
                                ["tpamort_per_acre"].sum().reset_index())
                    if PLOTLY_AVAILABLE:
                        fig2 = px.bar(
                            nat_harv, x="state", y="tpamort_per_acre",
                            color="component_type",
                            color_discrete_map={"natural": "#e15759", "harvest": "#4e79a7"},
                            labels={"tpamort_per_acre": "TPA Mortality (sum)",
                                    "state": "State", "component_type": "Type"},
                            barmode="stack",
                        )
                        st.plotly_chart(dark_fig(fig2), use_container_width=True)

        with seed_col:
            st.subheader("Seedling regeneration")
            if seed_f is None or len(seed_f) == 0:
                st.info("plot_seedling_metrics.parquet not available.")
            else:
                if "state" in seed_f.columns:
                    sw_cols = [c for c in ["count_softwood", "count_hardwood"]
                               if c in seed_f.columns]
                    if sw_cols:
                        seed_state = (seed_f.groupby("state")[sw_cols]
                                      .sum().reset_index()
                                      .rename(columns={"count_softwood": "Softwood",
                                                        "count_hardwood": "Hardwood"})
                                      .sort_values("Softwood", ascending=False))
                        sw_melt = seed_state.melt(
                            id_vars="state",
                            value_vars=[c for c in ["Softwood", "Hardwood"]
                                        if c in seed_state.columns],
                            var_name="Type", value_name="Seedling count"
                        )
                        if PLOTLY_AVAILABLE:
                            fig = px.bar(
                                sw_melt, x="state", y="Seedling count",
                                color="Type",
                                color_discrete_map={"Softwood": "#f28e2b",
                                                    "Hardwood": "#59a14f"},
                                barmode="stack",
                                labels={"state": "State"},
                            )
                            st.plotly_chart(dark_fig(fig), use_container_width=True)

                if "shannon_h_count" in seed_f.columns and "state" in seed_f.columns and PLOTLY_AVAILABLE:
                    seed_div = (seed_f.groupby("state")["shannon_h_count"]
                                .mean().reset_index()
                                .rename(columns={"shannon_h_count": "Mean Shannon H"})
                                .sort_values("Mean Shannon H", ascending=False))
                    fig = px.bar(
                        seed_div, x="state", y="Mean Shannon H",
                        color_discrete_sequence=["#76b7b2"],
                        labels={"state": "State"},
                    )
                    st.plotly_chart(dark_fig(fig), use_container_width=True)


if __name__ == "__main__":
    main()
