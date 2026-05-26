# ==============================================================================
# pages/3_FIA_Forest.py
# FIA Forest Inventory — interactive data explorer.
#
# Rendering priority:
#   1. Compact JSON/CSV aggregates from docs/dashboard/static/data/fia/
#      (always interactive, no local parquets required)
#   2. Live parquets in 05_fia/data/processed/summaries/ (full data, interactive)
#   3. Static PNG fallback in docs/dashboard/static/figures/fia/
#
# Aggregates are produced by docs/dashboard/scripts/build_static_figures.py.
# ==============================================================================

import os
import sys
from pathlib import Path

import pandas as pd
import streamlit as st

sys.path.insert(0, str(Path(__file__).parent.parent))
from utils import (
    apply_dark_css, metric_card, dark_fig,
    load_parquet, load_static_data_csv, load_static_data_json,
    parquet_meta, repo_path, color_status, PLOTLY_AVAILABLE,
    plot_source_link, render_top_nav,
)

st.set_page_config(page_title="FIA Forest", page_icon="🌲", layout="wide")
apply_dark_css()
render_top_nav()

if PLOTLY_AVAILABLE:
    import plotly.express as px
    import plotly.graph_objects as go

# ------------------------------------------------------------------------------
# Paths and themes
# ------------------------------------------------------------------------------

SUMM_DIR   = repo_path("05_fia", "data", "processed", "summaries")
CLIM_DIR   = repo_path("05_fia", "data", "processed", "site_climate")
STATIC_FIA_FIG_DIR = repo_path("docs", "dashboard", "static", "figures", "fia")
STATIC_FIA_FIG_SCRIPT = "docs/dashboard/scripts/build_static_figures.py"

APP_BG = "#0d1a12"
APP_PANEL = "#111f17"
APP_LAND = "#162219"
APP_BORDER = "#2a4035"
APP_TEXT = "#d4e8da"
APP_MUTED = "#8aab94"

DIST_COLORS = {
    "fire": "#e15759", "insects": "#59a14f", "disease": "#f28e2b",
    "weather": "#4e79a7", "animal": "#9c755f", "vegetation": "#76b7b2",
    "geologic": "#bab0ac", "other": "#8c8c8c", "unknown": "#555555",
}
AGENT_COLORS = {
    "bark beetles": "#e15759", "defoliators": "#59a14f",
    "sucking insects": "#76b7b2", "boring insects": "#edc948",
    "insects": "#b6d77a", "root/butt disease": "#f28e2b",
    "canker/rust": "#ff9da7", "foliage/wilt disease": "#9c755f",
    "disease": "#ffbf7f", "fire": "#e15759", "complex": "#b07aa1",
    "abiotic": "#4e79a7", "human": "#bab0ac", "other": "#8c8c8c",
    "unknown": "#555555",
}


def sp(fname): return str(SUMM_DIR / fname)
def cp(fname): return str(CLIM_DIR / fname)


def chart_with_source(fig, *, source_line: int | None = None,
                      source_label: str = "Chart code",
                      source_path: str = "docs/dashboard/pages/3_FIA_Forest.py",
                      data_source: str | None = None,
                      **kwargs) -> None:
    st.plotly_chart(fig, use_container_width=True, **kwargs)
    plot_source_link(source_path, label=source_label, line=source_line)
    if data_source:
        plot_source_link(STATIC_FIA_FIG_SCRIPT, label=f"Aggregate ({data_source})")


def static_image_fallback(file_name: str, caption: str) -> bool:
    path = STATIC_FIA_FIG_DIR / file_name
    if not path.is_file():
        return False
    st.image(str(path), caption=caption, use_container_width=True)
    plot_source_link(STATIC_FIA_FIG_SCRIPT, label="Static figure script")
    return True


def style_choropleth(fig):
    fig.update_layout(
        height=520,
        paper_bgcolor=APP_BG,
        plot_bgcolor=APP_BG,
        font_color=APP_TEXT,
        margin=dict(l=0, r=0, t=45, b=0),
        coloraxis_colorbar=dict(bgcolor=APP_PANEL, tickcolor=APP_MUTED,
                                title_font_color=APP_MUTED),
    )
    fig.update_geos(
        bgcolor=APP_BG, lakecolor=APP_BG, landcolor=APP_LAND,
        coastlinecolor=APP_BORDER, subunitcolor=APP_BORDER,
        showlakes=True, showland=True, showcoastlines=True,
    )
    return fig


def style_geo_scatter(fig, height: int = 560):
    fig.update_layout(
        height=height,
        paper_bgcolor=APP_BG,
        plot_bgcolor=APP_BG,
        font_color=APP_TEXT,
        margin=dict(l=0, r=0, t=10, b=0),
        legend=dict(bgcolor=APP_PANEL, bordercolor=APP_BORDER, borderwidth=1),
    )
    fig.update_geos(
        bgcolor=APP_BG, lakecolor=APP_BG, landcolor=APP_LAND,
        coastlinecolor=APP_BORDER, subunitcolor=APP_BORDER,
        showlakes=True, showland=True, showcoastlines=True,
        scope="usa",
    )
    return fig


def style_bar(fig, height: int | None = None):
    layout = dict(
        bargap=0.18,
        paper_bgcolor=APP_BG,
        plot_bgcolor=APP_BG,
        font_color=APP_TEXT,
        margin=dict(l=10, r=20, t=30, b=40),
        legend=dict(bgcolor=APP_PANEL, bordercolor=APP_BORDER, borderwidth=1),
    )
    if height:
        layout["height"] = height
    fig.update_layout(**layout)
    fig.update_xaxes(gridcolor="#1e3024", linecolor=APP_BORDER, zerolinecolor=APP_BORDER)
    fig.update_yaxes(gridcolor="#1e3024", linecolor=APP_BORDER, zerolinecolor=APP_BORDER)
    return fig


# ------------------------------------------------------------------------------
# State helpers
# ------------------------------------------------------------------------------

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
STATE_NAME_BY_ABBR = {
    "AL": "Alabama", "AK": "Alaska", "AZ": "Arizona", "AR": "Arkansas",
    "CA": "California", "CO": "Colorado", "CT": "Connecticut",
    "DE": "Delaware", "DC": "District of Columbia", "FL": "Florida",
    "GA": "Georgia", "HI": "Hawaii", "ID": "Idaho", "IL": "Illinois",
    "IN": "Indiana", "IA": "Iowa", "KS": "Kansas", "KY": "Kentucky",
    "LA": "Louisiana", "ME": "Maine", "MD": "Maryland",
    "MA": "Massachusetts", "MI": "Michigan", "MN": "Minnesota",
    "MS": "Mississippi", "MO": "Missouri", "MT": "Montana",
    "NE": "Nebraska", "NV": "Nevada", "NH": "New Hampshire",
    "NJ": "New Jersey", "NM": "New Mexico", "NY": "New York",
    "NC": "North Carolina", "ND": "North Dakota", "OH": "Ohio",
    "OK": "Oklahoma", "OR": "Oregon", "PA": "Pennsylvania",
    "RI": "Rhode Island", "SC": "South Carolina", "SD": "South Dakota",
    "TN": "Tennessee", "TX": "Texas", "UT": "Utah", "VT": "Vermont",
    "VA": "Virginia", "WA": "Washington", "WV": "West Virginia",
    "WI": "Wisconsin", "WY": "Wyoming",
}
STATE_ABBR_BY_NAME = {name.upper(): abbr for abbr, name in STATE_NAME_BY_ABBR.items()}


def state_abbr(value) -> str | None:
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return None
    text = str(value).strip()
    upper = text.upper()
    if upper in STATE_CODE_BY_ABBR:
        return upper
    if upper in STATE_ABBR_BY_NAME:
        return STATE_ABBR_BY_NAME[upper]
    try:
        return STATE_ABBR_BY_CODE.get(int(float(text)))
    except (TypeError, ValueError):
        return None


def add_state_abbr(df: pd.DataFrame, state_col: str) -> pd.DataFrame:
    out = df.copy()
    out["_state_abbr"] = out[state_col].map(state_abbr)
    return out.dropna(subset=["_state_abbr"])


def state_choropleth(df: pd.DataFrame, state_col: str, value_col: str, title: str,
                     color_scale: str = "YlGn", value_label: str | None = None):
    map_df = add_state_abbr(df, state_col)
    map_df["_state_name"] = map_df["_state_abbr"].map(STATE_NAME_BY_ABBR).fillna(map_df["_state_abbr"])
    fig = px.choropleth(
        map_df,
        locations="_state_abbr",
        locationmode="USA-states",
        color=value_col,
        scope="usa",
        hover_name="_state_name",
        hover_data={value_col: ":,.2f", "_state_abbr": False, "_state_name": False},
        color_continuous_scale=color_scale,
        labels={value_col: value_label or value_col},
        title=title,
    )
    return style_choropleth(fig)


def state_order_present(values) -> list:
    present = [str(v).upper() for v in pd.Series(values).dropna().unique().tolist()]
    ordered = [abbr for abbr in STATE_EAST_TO_WEST if abbr in present]
    leftover = sorted(value for value in present if value not in set(ordered))
    return ordered + leftover


# ------------------------------------------------------------------------------
# Title
# ------------------------------------------------------------------------------

st.title("🌲 FIA Forest Inventory")
st.markdown(
    "USDA Forest Inventory and Analysis — processed summaries for all 50 US states. "
    "Source tables from `05_fia/scripts/05_build_fia_summaries.R`. "
    "Charts here are interactive: most are rendered from compact aggregates that ship with "
    "the dashboard repository, so they work even without the underlying parquet files."
)

with st.spinner("Loading FIA summaries…"):
    tree_df,    _ = load_parquet(sp("plot_tree_metrics.parquet"))
    disturb_df, _ = load_parquet(sp("plot_disturbance_history.parquet"))
    agents_df,  _ = load_parquet(sp("plot_damage_agents.parquet"))
    mort_df,    _ = load_parquet(sp("plot_mortality_metrics.parquet"))
    seed_df,    _ = load_parquet(sp("plot_seedling_metrics.parquet"))
    treat_df,   _ = load_parquet(sp("plot_treatment_history.parquet"))
    flags_df,   _ = load_parquet(sp("plot_exclusion_flags.parquet"))

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
        st.info(
            "Local FIA summary parquets not found. Charts on later tabs will still render "
            "from the GitHub-hosted aggregates in `docs/dashboard/static/data/fia/`."
        )

    st.markdown("---")

    files_info = [
        ("plot_tree_metrics.parquet",        tree_df,    "Tree metrics (BA, diversity, size class)"),
        ("plot_exclusion_flags.parquet",      flags_df,   "Plot exclusion & disturbance flags"),
        ("plot_disturbance_history.parquet",  disturb_df, "Disturbance history (fire/insect/disease)"),
        ("plot_damage_agents.parquet",        agents_df,  "Damage agents (specific insect/disease codes)"),
        ("plot_mortality_metrics.parquet",    mort_df,    "Mortality (natural & harvest)"),
        ("plot_seedling_metrics.parquet",     seed_df,    "Seedling regeneration"),
        ("plot_treatment_history.parquet",    treat_df,   "Treatment history (cutting, regen, site prep)"),
        ("plot_cond_fortypcd.parquet",        None,       "Condition / forest type (not pre-loaded)"),
        ("plot_condition_metadata.parquet",   None,       "Stable plot IDs, coordinates, forest type groups, condition area"),
        ("plot_seedling_species.parquet",     None,       "Species-level seedling counts for recruitment and CWM workflows"),
        ("plot_disturbance_classification.parquet", None, "Control/disturbed eligibility and natural disturbance classes"),
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
    st.markdown("### How datasets connect")
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

    st.markdown(
        "Thermophilization-facing FIA products add a condition-grain layer:\n\n"
        "| Dataset | Grain | Join key |\n"
        "|---------|-------|----------|\n"
        "| `plot_condition_metadata` | 1 row per condition visit | `PLT_CN, INVYR, CONDID`; `stable_plot_id` joins to site climate |\n"
        "| `plot_seedling_species` | 1+ rows per condition, subplot, and species | `PLT_CN, INVYR, CONDID, SPCD` |\n"
        "| `plot_disturbance_classification` | 1 row per condition visit | `PLT_CN, INVYR, CONDID` |\n"
    )

# ==============================================================================
# TAB 2 — PLOT FILTERS
# ==============================================================================
with tab_filters:
    st.markdown(
        "**`plot_exclusion_flags.parquet`** — one row per plot × inventory year. "
        "Join on `PLT_CN + INVYR`. Produced by Step 7 of `05_build_fia_summaries.R`."
    )

    flag_rows = load_static_data_json("flag_rates")
    if flag_rows and PLOTLY_AVAILABLE:
        flag_df = pd.DataFrame(flag_rows).sort_values("rate_pct")
        fire_rate = next((r["rate_pct"] for r in flag_rows if r["flag"] == "has_fire"), None)
        insect_rate = next((r["rate_pct"] for r in flag_rows if r["flag"] == "has_insect"), None)
        excl_rate = next((r["rate_pct"] for r in flag_rows if r["flag"] == "exclude_any"), None)

        c1, c2, c3, c4 = st.columns(4)
        if flags_df is not None:
            c1.markdown(metric_card("Plot×Year Rows", f"{len(flags_df):,}", "total"),
                        unsafe_allow_html=True)
        else:
            c1.markdown(metric_card("Plot×Year Rows", "—", "load parquet for total"),
                        unsafe_allow_html=True)
        c2.markdown(metric_card("Excluded (any)", f"{excl_rate:.1f}%" if excl_rate else "—",
                                "exclude_any = TRUE"), unsafe_allow_html=True)
        c3.markdown(metric_card("Has Fire", f"{fire_rate:.1f}%" if fire_rate else "—",
                                "DSTRBCD 30/31/32"), unsafe_allow_html=True)
        c4.markdown(metric_card("Has Insects", f"{insect_rate:.1f}%" if insect_rate else "—",
                                "DSTRBCD 10/11/12"), unsafe_allow_html=True)

        st.markdown("---")
        col_l, col_r = st.columns([1.05, 1])
        with col_l:
            st.subheader("Flag rates")
            fig = px.bar(
                flag_df, x="rate_pct", y="label", orientation="h",
                color="label",
                color_discrete_map=dict(zip(flag_df["label"], flag_df["color"])),
                hover_data={"basis": True, "rate_pct": ":.2f", "color": False, "label": False},
                labels={"rate_pct": "Percent of plot visits", "label": ""},
                text=flag_df["rate_pct"].round(1).astype(str) + "%",
            )
            fig.update_traces(textposition="outside", cliponaxis=False)
            fig.update_layout(showlegend=False,
                              xaxis_range=[0, max(10, flag_df["rate_pct"].max() * 1.2)])
            chart_with_source(style_bar(fig, height=420), source_line=320,
                              data_source="flag_rates.json")
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
                "**FIA samples ALL US land.** Roughly 59% of plot×year rows have `pct_forested = 0`. "
                "Always filter `pct_forested >= 0.5` as the **primary gate** before using any flags."
            )
    elif not PLOTLY_AVAILABLE:
        st.info("Install `plotly` for interactive charts.")
    else:
        st.info("Aggregates not found. Run `python docs/dashboard/scripts/build_static_figures.py`.")

    # pct_forested histogram
    st.markdown("---")
    st.subheader("`pct_forested` distribution")
    pct_data = load_static_data_json("pct_forested_hist")
    if pct_data and PLOTLY_AVAILABLE:
        bin_centers = pct_data["bin_centers"]
        counts = pct_data["counts"]
        bar_width = (pct_data["bin_edges"][1] - pct_data["bin_edges"][0]) * 0.95
        fig = go.Figure(go.Bar(x=bin_centers, y=counts, width=[bar_width] * len(bin_centers),
                               marker_color="#59a14f", marker_line_color=APP_BG, marker_line_width=0.5,
                               hovertemplate="pct_forested ≈ %{x:.2f}<br>%{y:,} plot visits<extra></extra>"))
        fig.update_layout(xaxis_title="pct_forested", yaxis_title="Plot visits")
        chart_with_source(style_bar(fig, height=320), source_line=361,
                          data_source="pct_forested_hist.json")
    else:
        static_image_fallback("flag_rates.png",
                              "Plot-filter flag rates (pct_forested distribution unavailable).")

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

    st.markdown("---")
    st.subheader("Excluded vs. clean plot locations")
    status_pts = load_static_data_csv("plot_status_points")
    if status_pts is not None and PLOTLY_AVAILABLE:
        fig = px.scatter_geo(
            status_pts, lat="lat", lon="lon", color="status",
            color_discrete_map={"Clean": "#59a14f", "Excluded": "#e15759"},
            opacity=0.55, hover_data={"lat": ":.2f", "lon": ":.2f"},
        )
        fig.update_traces(marker_size=4)
        chart_with_source(style_geo_scatter(fig), source_line=400,
                          data_source="plot_status_points.csv")
        st.caption("Sampled to ~12k plots so the map stays interactive on a static deployment.")
    else:
        static_image_fallback(
            "plot_status_map.png",
            "Clean and excluded FIA plot locations generated from plot filters and tree-coordinate summaries.",
        )

# ==============================================================================
# TAB 3 — TREE METRICS
# ==============================================================================
with tab_forests:
    st.subheader("Plot locations")
    tree_pts = load_static_data_csv("plot_tree_points")
    if tree_pts is not None and PLOTLY_AVAILABLE:
        metric_options = {
            "ba_live_total": "Live BA (ft²/acre)",
            "shannon_h_ba": "Shannon H (BA-weighted)",
            "n_species_live": "Species richness (live)",
        }
        chosen = st.radio("Color by", list(metric_options.keys()),
                          format_func=metric_options.get,
                          horizontal=True, key="tree_map_metric")
        sub = tree_pts.dropna(subset=["lat", "lon", chosen])
        fig = px.scatter_geo(
            sub, lat="lat", lon="lon", color=chosen,
            color_continuous_scale="Viridis", opacity=0.6,
            labels={chosen: metric_options[chosen]},
            hover_data={"lat": ":.2f", "lon": ":.2f", chosen: ":.2f"},
        )
        fig.update_traces(marker_size=4)
        fig.update_layout(coloraxis_colorbar=dict(bgcolor=APP_PANEL, tickcolor=APP_MUTED,
                                                  title_font_color=APP_MUTED))
        chart_with_source(style_geo_scatter(fig), source_line=438,
                          data_source="plot_tree_points.csv")
    else:
        static_image_fallback("plot_locations_ba.png",
                              "Sampled FIA plot locations colored by live basal area.")

    st.subheader("Forest structure distributions")
    ba_hist = load_static_data_json("ba_live_hist")
    sh_hist = load_static_data_json("shannon_h_hist")
    if ba_hist and sh_hist and PLOTLY_AVAILABLE:
        col1, col2 = st.columns(2)
        with col1:
            ba_w = (ba_hist["bin_edges"][1] - ba_hist["bin_edges"][0]) * 0.95
            fig = go.Figure(go.Bar(
                x=ba_hist["bin_centers"], y=ba_hist["counts"],
                width=[ba_w] * len(ba_hist["bin_centers"]),
                marker_color="#59a14f", marker_line_color=APP_BG, marker_line_width=0.4,
                hovertemplate="BA ≈ %{x:.0f}<br>%{y:,} plot visits<extra></extra>",
            ))
            fig.add_vline(x=ba_hist["median"], line_color="#d4aa64", line_width=2,
                          annotation_text=f"median {ba_hist['median']:.0f}",
                          annotation_position="top right",
                          annotation_font_color="#d4aa64")
            fig.update_layout(xaxis_title="Live BA per acre", yaxis_title="Plot visits",
                              title="Live basal area")
            chart_with_source(style_bar(fig, height=320), source_line=464,
                              data_source="ba_live_hist.json")
        with col2:
            sh_w = (sh_hist["bin_edges"][1] - sh_hist["bin_edges"][0]) * 0.95
            fig = go.Figure(go.Bar(
                x=sh_hist["bin_centers"], y=sh_hist["counts"],
                width=[sh_w] * len(sh_hist["bin_centers"]),
                marker_color="#4e79a7", marker_line_color=APP_BG, marker_line_width=0.4,
                hovertemplate="Shannon H ≈ %{x:.2f}<br>%{y:,} plot visits<extra></extra>",
            ))
            fig.add_vline(x=sh_hist["median"], line_color="#d4aa64", line_width=2,
                          annotation_text=f"median {sh_hist['median']:.2f}",
                          annotation_position="top right",
                          annotation_font_color="#d4aa64")
            fig.update_layout(xaxis_title="Shannon H (BA-weighted)", yaxis_title="Plot visits",
                              title="Forest diversity")
            chart_with_source(style_bar(fig, height=320), source_line=478,
                              data_source="shannon_h_hist.json")
    else:
        static_image_fallback("tree_metric_distributions.png",
                              "Live basal area and BA-weighted Shannon diversity distributions.")

    sw_hw = load_static_data_csv("state_softwood_hardwood_ba")
    if sw_hw is not None and PLOTLY_AVAILABLE:
        st.subheader("Softwood vs. hardwood BA by state")
        ba_metric = st.radio("Basal area metric", ["softwood", "hardwood"],
                             horizontal=True, key="state_ba_metric",
                             format_func=str.title)
        fig = state_choropleth(sw_hw, "state", ba_metric,
                               f"Mean {ba_metric.title()} basal area by state",
                               color_scale="YlGn", value_label=f"Mean {ba_metric} BA")
        chart_with_source(fig, source_line=494,
                          data_source="state_softwood_hardwood_ba.csv")

    sz = load_static_data_csv("state_size_class_ba")
    if sz is not None and PLOTLY_AVAILABLE:
        st.subheader("Size-class BA by state")
        size_class = st.radio("Size class", ["sapling", "intermediate", "mature"],
                              horizontal=True, key="state_size_class",
                              format_func=str.title)
        fig = state_choropleth(sz, "state", size_class,
                               f"Mean {size_class.title()} basal area by state",
                               color_scale="YlGnBu", value_label=f"Mean {size_class} BA")
        chart_with_source(fig, source_line=505,
                          data_source="state_size_class_ba.csv")

# ==============================================================================
# TAB 4 — DISTURBANCE
# ==============================================================================
with tab_disturb:
    cat_data = load_static_data_json("disturbance_category_counts")
    fire_data = load_static_data_json("fire_type_breakdown")

    col1, col2 = st.columns(2)
    with col1:
        st.subheader("Events by category")
        if cat_data and PLOTLY_AVAILABLE:
            cat_df = pd.DataFrame(cat_data).sort_values("count")
            fig = px.bar(
                cat_df, x="count", y="category", orientation="h",
                color="category",
                color_discrete_map=dict(zip(cat_df["category"], cat_df["color"])),
                labels={"count": "Records", "category": ""},
                text=cat_df["count"].apply(lambda v: f"{v:,}"),
            )
            fig.update_traces(textposition="outside", cliponaxis=False)
            fig.update_layout(showlegend=False,
                              xaxis_range=[0, cat_df["count"].max() * 1.18])
            chart_with_source(style_bar(fig, height=380), source_line=525,
                              data_source="disturbance_category_counts.json")
        else:
            static_image_fallback("disturbance_category_counts.png",
                                  "FIA disturbance-history records grouped by category.")
    with col2:
        st.subheader("Fire type breakdown")
        if fire_data and PLOTLY_AVAILABLE:
            fdf = pd.DataFrame(fire_data)
            fig = px.pie(fdf, names="label", values="count", hole=0.55,
                         color_discrete_sequence=["#e15759", "#ff9d9a", "#c85250", "#d4aa64", "#b07aa1"])
            fig.update_traces(marker=dict(line=dict(color=APP_BG, width=2)),
                              textinfo="percent+label", textfont_color=APP_TEXT)
            fig.update_layout(
                paper_bgcolor=APP_BG, plot_bgcolor=APP_BG, font_color=APP_TEXT,
                legend=dict(bgcolor=APP_PANEL, bordercolor=APP_BORDER, borderwidth=1),
                margin=dict(l=10, r=10, t=20, b=10), height=380,
            )
            chart_with_source(fig, source_line=545, data_source="fire_type_breakdown.json")
        else:
            static_image_fallback("fire_type_breakdown.png",
                                  "FIA fire disturbance records by disturbance label.")

    # Year of disturbance — properly grouped time series
    st.subheader("Year of disturbance")
    yr_df = load_static_data_csv("disturbance_year_counts")
    if yr_df is not None and PLOTLY_AVAILABLE and len(yr_df):
        view = st.radio(
            "View",
            ["Stacked by category", "Lines per category", "Total per year"],
            horizontal=True, key="disturb_year_view",
        )
        yr_df = yr_df.sort_values("year")
        if view == "Stacked by category":
            cats_ordered = (yr_df.groupby("category")["count"].sum()
                            .sort_values(ascending=False).index.tolist())
            yr_df["category"] = pd.Categorical(yr_df["category"], categories=cats_ordered, ordered=True)
            fig = px.bar(
                yr_df, x="year", y="count", color="category",
                color_discrete_map=DIST_COLORS,
                labels={"year": "Disturbance year (DSTRBYR)", "count": "Records",
                        "category": "Category"},
            )
            fig.update_layout(barmode="stack")
        elif view == "Lines per category":
            fig = px.line(
                yr_df, x="year", y="count", color="category",
                color_discrete_map=DIST_COLORS, markers=True,
                labels={"year": "Disturbance year (DSTRBYR)", "count": "Records",
                        "category": "Category"},
            )
        else:
            totals = yr_df.groupby("year", as_index=False)["count"].sum()
            fig = px.bar(
                totals, x="year", y="count",
                labels={"year": "Disturbance year (DSTRBYR)", "count": "Records"},
                color_discrete_sequence=["#7bbf92"],
            )
        fig.update_xaxes(dtick=5)
        chart_with_source(style_bar(fig, height=420), source_line=580,
                          data_source="disturbance_year_counts.csv")
        st.caption(
            "Records are FIA `DSTRBYR` values from 1950–2025 grouped by inferred disturbance "
            "category. Pre-1980 counts are sparse because most DSTRBYR coverage starts with the "
            "annualized inventory."
        )
    elif disturb_df is not None and PLOTLY_AVAILABLE:
        # fallback to live-data path
        sub = disturb_df[disturb_df["DSTRBYR"].notna() & (disturb_df["DSTRBYR"] != 9999)]
        if len(sub):
            fig = px.histogram(sub, x="DSTRBYR", color="disturbance_category",
                               color_discrete_map=DIST_COLORS, nbins=50)
            chart_with_source(dark_fig(fig), source_line=596)
    else:
        st.info("Run the build script to generate `disturbance_year_counts.csv`.")

    st.subheader("Top disturbance types")
    top_data = load_static_data_json("top_disturbance_types")
    if top_data and PLOTLY_AVAILABLE:
        tdf = pd.DataFrame(top_data).sort_values("count")
        fig = px.bar(
            tdf, x="count", y="disturbance_label", orientation="h",
            color="disturbance_category", color_discrete_map=DIST_COLORS,
            labels={"count": "Records", "disturbance_label": "",
                    "disturbance_category": "Category"},
        )
        fig.update_layout(yaxis=dict(autorange="reversed"))
        chart_with_source(style_bar(fig, height=620), source_line=611,
                          data_source="top_disturbance_types.json")
    else:
        static_image_fallback("top_disturbance_types.png",
                              "The 20 most common FIA disturbance labels.")

    st.subheader("Disturbance event locations")
    dist_pts = load_static_data_csv("disturbance_event_points")
    if dist_pts is not None and PLOTLY_AVAILABLE:
        cats = sorted(dist_pts["category"].dropna().unique().tolist())
        chosen = st.multiselect("Categories", cats, default=cats, key="disturb_map_cats")
        sub = dist_pts[dist_pts["category"].isin(chosen)]
        fig = px.scatter_geo(
            sub, lat="lat", lon="lon", color="category",
            color_discrete_map=DIST_COLORS,
            hover_name="label",
            hover_data={"lat": ":.2f", "lon": ":.2f"},
            opacity=0.6,
        )
        fig.update_traces(marker_size=4)
        chart_with_source(style_geo_scatter(fig), source_line=632,
                          data_source="disturbance_event_points.csv")
        st.caption("Sampled to ~15k events for fast interactive rendering.")
    else:
        static_image_fallback("disturbance_event_locations.png",
                              "Sampled FIA disturbance-event locations by category.")

# ==============================================================================
# TAB 5 — DAMAGE AGENTS
# ==============================================================================
with tab_agents:
    top_agents = load_static_data_json("damage_agent_top20")
    cat_ba = load_static_data_json("agent_category_ba")

    col1, col2 = st.columns(2)
    with col1:
        st.subheader("Top 20 damage agents (by TPA)")
        if top_agents and PLOTLY_AVAILABLE:
            tdf = pd.DataFrame(top_agents).sort_values("n_trees_tpa")
            fig = px.bar(
                tdf, x="n_trees_tpa", y="agent_label", orientation="h",
                color="agent_category", color_discrete_map=AGENT_COLORS,
                labels={"n_trees_tpa": "Affected TPA (sum)",
                        "agent_label": "", "agent_category": "Category"},
            )
            fig.update_layout(yaxis=dict(autorange="reversed"))
            chart_with_source(style_bar(fig, height=620), source_line=658,
                              data_source="damage_agent_top20.json")
        else:
            static_image_fallback("damage_agent_top20.png",
                                  "Damage agents ranked by summed affected trees per acre.")
    with col2:
        st.subheader("BA affected by category")
        if cat_ba and PLOTLY_AVAILABLE:
            cdf = pd.DataFrame(cat_ba).sort_values("ba_per_acre")
            fig = px.bar(
                cdf, x="ba_per_acre", y="category", orientation="h",
                color="category",
                color_discrete_map=dict(zip(cdf["category"], cdf["color"])),
                labels={"ba_per_acre": "Affected BA (sum)", "category": ""},
            )
            fig.update_layout(showlegend=False)
            chart_with_source(style_bar(fig, height=420), source_line=672,
                              data_source="agent_category_ba.json")
        else:
            static_image_fallback("agent_category_ba.png",
                                  "Summed affected basal area by FIA damage-agent category.")

    st.subheader("Agent category by state heatmap")
    heat_long = load_static_data_csv("agent_state_heatmap")
    if heat_long is not None and PLOTLY_AVAILABLE:
        wide = heat_long.pivot(index="agent_category", columns="state",
                               values="n_trees_tpa").fillna(0)
        ordered_states = state_order_present(wide.columns.tolist())
        wide = wide.reindex(columns=ordered_states)
        wide = wide.loc[wide.sum(axis=1).sort_values(ascending=False).index]
        fig = px.imshow(
            wide, color_continuous_scale="YlOrRd", aspect="auto",
            labels={"color": "Affected TPA (sum)", "x": "State", "y": "Agent category"},
        )
        fig.update_layout(
            paper_bgcolor=APP_BG, plot_bgcolor=APP_BG, font_color=APP_TEXT,
            margin=dict(l=140, r=20, t=20, b=60), height=480,
            coloraxis_colorbar=dict(bgcolor=APP_PANEL, tickcolor=APP_MUTED,
                                    title_font_color=APP_MUTED),
        )
        chart_with_source(fig, source_line=694, data_source="agent_state_heatmap.csv")
    else:
        static_image_fallback("agent_category_state_heatmap.png",
                              "State by damage-agent-category heatmap.")

    st.subheader("Damage agent locations")
    agent_pts = load_static_data_csv("damage_agent_points")
    if agent_pts is not None and PLOTLY_AVAILABLE:
        cats = sorted(agent_pts["category"].dropna().unique().tolist())
        chosen = st.multiselect("Categories", cats, default=cats, key="agent_map_cats")
        sub = agent_pts[agent_pts["category"].isin(chosen)]
        fig = px.scatter_geo(
            sub, lat="lat", lon="lon", color="category",
            color_discrete_map=AGENT_COLORS, hover_name="label",
            hover_data={"lat": ":.2f", "lon": ":.2f"}, opacity=0.55,
        )
        fig.update_traces(marker_size=4)
        chart_with_source(style_geo_scatter(fig), source_line=712,
                          data_source="damage_agent_points.csv")
        st.caption("Sampled to ~15k records.")
    else:
        static_image_fallback("damage_agent_locations.png",
                              "Sampled FIA plots with damage-agent records by category.")

# ==============================================================================
# TAB 6 — MORTALITY & REGENERATION
# ==============================================================================
with tab_mort:
    mort_col, seed_col = st.columns(2)
    with mort_col:
        st.subheader("Mortality by agent")
        mort_agent = load_static_data_csv("mortality_by_agent")
        if mort_agent is not None and PLOTLY_AVAILABLE:
            mort_agent = mort_agent.sort_values("tpamort_per_acre", ascending=True)
            fig = px.bar(
                mort_agent, x="tpamort_per_acre", y="agent_label", orientation="h",
                color="component_type",
                color_discrete_map={"natural": "#e15759", "harvest": "#4e79a7"},
                labels={"tpamort_per_acre": "TPA mortality (sum)",
                        "agent_label": "", "component_type": "Component"},
            )
            fig.update_layout(yaxis=dict(autorange="reversed"))
            chart_with_source(style_bar(fig, height=380), source_line=738,
                              data_source="mortality_by_agent.csv")

        state_mort = load_static_data_csv("state_mortality")
        if state_mort is not None and PLOTLY_AVAILABLE:
            comps = ["All mortality"] + sorted(state_mort["component_type"].dropna().unique().tolist())
            choice = st.selectbox("Mortality map", comps, key="state_mortality_component")
            if choice == "All mortality":
                sub = state_mort.groupby("state")["tpamort_per_acre"].sum().reset_index()
                title = "Mortality by State"
            else:
                sub = state_mort[state_mort["component_type"] == choice]
                title = f"{choice.title()} mortality by state"
            fig = state_choropleth(sub, "state", "tpamort_per_acre", title,
                                   color_scale="OrRd", value_label="TPA mortality")
            chart_with_source(fig, source_line=753, data_source="state_mortality.csv")

    with seed_col:
        st.subheader("Seedling regeneration")
        state_seed = load_static_data_csv("state_seedlings")
        if state_seed is not None and PLOTLY_AVAILABLE:
            sw_hw = state_seed.melt(id_vars="state",
                                    value_vars=[c for c in ["softwood", "hardwood"]
                                                if c in state_seed.columns],
                                    var_name="Type", value_name="Seedling count")
            seed_type = st.radio("Seedling type",
                                 sw_hw["Type"].unique().tolist(),
                                 horizontal=True, key="state_seed_type",
                                 format_func=str.title)
            sub = sw_hw[sw_hw["Type"] == seed_type]
            fig = state_choropleth(sub, "state", "Seedling count",
                                   f"{seed_type.title()} seedling count by state",
                                   color_scale="YlGn", value_label="Seedling count")
            chart_with_source(fig, source_line=771, data_source="state_seedlings.csv")

        seed_div = load_static_data_csv("state_seedling_diversity")
        if seed_div is not None and PLOTLY_AVAILABLE:
            fig = state_choropleth(seed_div, "state", "mean_shannon_h",
                                   "Mean seedling diversity by state",
                                   color_scale="BuGn", value_label="Mean Shannon H")
            chart_with_source(fig, source_line=781, data_source="state_seedling_diversity.csv")

# ==============================================================================
# TAB 7 — TREATMENT HISTORY
# ==============================================================================
with tab_treatments:
    treat_labels = load_static_data_csv("treatment_label_counts")
    state_treat = load_static_data_csv("state_treatments")
    year_treat = load_static_data_csv("treatment_year_counts")

    if treat_df is not None:
        n_treat = len(treat_df)
        n_plots_treat = treat_df["PLT_CN"].nunique() if "PLT_CN" in treat_df.columns else "—"
        c1, c2, c3 = st.columns(3)
        c1.markdown(metric_card("Treatment Records", f"{n_treat:,}", "non-zero TRTCD rows"),
                    unsafe_allow_html=True)
        c2.markdown(metric_card("Plots with Treatments", f"{n_plots_treat:,}", "distinct PLT_CN"),
                    unsafe_allow_html=True)
        c3.markdown(metric_card("Treatment Types", "5", "TRTCD 10/20/30/40/50"),
                    unsafe_allow_html=True)
        st.markdown("---")

    CAT_COLORS = {"harvest": "#e15759", "site_prep": "#f28e2b",
                  "regeneration": "#59a14f", "other_silv": "#4e79a7"}

    if treat_labels is not None and PLOTLY_AVAILABLE:
        col1, col2 = st.columns(2)
        with col1:
            st.subheader("By treatment category")
            tdf = treat_labels.sort_values("count", ascending=True)
            fig = px.bar(
                tdf, x="count", y="treatment_label", orientation="h",
                color="treatment_category", color_discrete_map=CAT_COLORS,
                labels={"treatment_label": "", "count": "Records",
                        "treatment_category": "Category"},
            )
            fig.update_layout(yaxis=dict(autorange="reversed"))
            chart_with_source(style_bar(fig, height=320), source_line=815,
                              data_source="treatment_label_counts.csv")
        with col2:
            st.subheader("By state")
            if state_treat is not None:
                opts = ["All categories"] + sorted(state_treat["treatment_category"].dropna().unique().tolist())
                pick = st.selectbox("Treatment map", opts, key="state_treatment_category")
                if pick == "All categories":
                    sub = state_treat.groupby("state")["count"].sum().reset_index()
                    title = "Treatment records by state"
                else:
                    sub = state_treat[state_treat["treatment_category"] == pick]
                    title = f"{pick.replace('_', ' ').title()} treatment records by state"
                fig = state_choropleth(sub, "state", "count", title,
                                       color_scale="YlOrBr", value_label="Treatment records")
                chart_with_source(fig, source_line=830, data_source="state_treatments.csv")

    if year_treat is not None and PLOTLY_AVAILABLE and len(year_treat):
        st.subheader("Treatment year distribution")
        view = st.radio("View",
                        ["Stacked by category", "Lines per category"],
                        horizontal=True, key="treat_year_view")
        ydf = year_treat.sort_values("treatment_year")
        if view == "Stacked by category":
            fig = px.bar(
                ydf, x="treatment_year", y="count", color="treatment_category",
                color_discrete_map=CAT_COLORS,
                labels={"treatment_year": "Treatment year",
                        "count": "Records", "treatment_category": "Category"},
            )
            fig.update_layout(barmode="stack")
        else:
            fig = px.line(
                ydf, x="treatment_year", y="count", color="treatment_category",
                color_discrete_map=CAT_COLORS, markers=True,
                labels={"treatment_year": "Treatment year",
                        "count": "Records", "treatment_category": "Category"},
            )
        fig.update_xaxes(dtick=5, tickformat="d")
        chart_with_source(style_bar(fig, height=400), source_line=854,
                          data_source="treatment_year_counts.csv")

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
