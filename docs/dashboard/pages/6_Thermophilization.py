# ==============================================================================
# pages/6_Thermophilization.py
# FIA recruitment thermophilization workflow
# ==============================================================================

import os
import sys
import html
from pathlib import Path

import pandas as pd
import streamlit as st

sys.path.insert(0, str(Path(__file__).parent.parent))
from utils import (
    apply_dark_css, color_status, load_parquet, metric_card, page_header,
    load_static_json, parquet_meta, plot_source_link, render_top_nav, repo_path, route_grid,
    workflow_grid, PLOTLY_AVAILABLE,
)


st.set_page_config(page_title="Thermophilization", layout="wide")
apply_dark_css()
render_top_nav()

if PLOTLY_AVAILABLE:
    import plotly.express as px


OUTPUTS = [
    {
        "section": "FIA foundation",
        "label": "plot_condition_metadata.parquet",
        "path": "05_fia/data/processed/summaries/plot_condition_metadata.parquet",
        "producer": "05_fia/scripts/summaries/build_condition_metadata.R",
        "grain": "PLT_CN x INVYR x CONDID",
        "role": "Stable plot IDs, condition geography, forest type group, and area fields.",
    },
    {
        "section": "FIA foundation",
        "label": "plot_seedling_species.parquet",
        "path": "05_fia/data/processed/summaries/plot_seedling_species.parquet",
        "producer": "05_fia/scripts/summaries/build_seedling_species.R",
        "grain": "PLT_CN x INVYR x CONDID x SUBP x SPCD",
        "role": "Species-level seedling counts used to build recruitment community metrics.",
    },
    {
        "section": "FIA foundation",
        "label": "plot_disturbance_classification.parquet",
        "path": "05_fia/data/processed/summaries/plot_disturbance_classification.parquet",
        "producer": "05_fia/scripts/summaries/build_disturbance_classification.R",
        "grain": "PLT_CN x INVYR x CONDID",
        "role": "Control/disturbed eligibility, natural disturbance class, timing, and forest type strata.",
    },
    {
        "section": "Species niches",
        "label": "species_climate_niches.parquet",
        "path": "06_species_niches/data/processed/species_climate_niches.parquet",
        "producer": "06_species_niches/WORKFLOW.md",
        "grain": "SPCD",
        "role": "External occurrence-based climate envelopes for FIA tree species.",
    },
    {
        "section": "Climate traits",
        "label": "plot_recruitment_cwm.parquet",
        "path": "07_thermophilization/data/processed/plot_recruitment_cwm.parquet",
        "producer": "07_thermophilization/scripts/01_build_plot_recruitment_cwm.R",
        "grain": "PLT_CN x INVYR x CONDID",
        "role": "Seedling community-weighted climate affinity: temp, precipitation, and CWD.",
    },
    {
        "section": "Matching",
        "label": "plot_matches.parquet",
        "path": "07_thermophilization/data/processed/plot_matches.parquet",
        "producer": "07_thermophilization/scripts/02_match_disturbed_to_controls.R",
        "grain": "disturbed condition x matched control x rank",
        "role": "Five nearest undisturbed controls per disturbed condition, with pairwise CWM deltas.",
    },
    {
        "section": "Headline results",
        "label": "thermophilization_by_class_region.parquet",
        "path": "07_thermophilization/data/processed/thermophilization_by_class_region.parquet",
        "producer": "07_thermophilization/scripts/03_stratified_thermophilization.R",
        "grain": "disturbance_class x East/West",
        "role": "Mean delta and bootstrap 95% CI for temperature, precipitation, and CWD.",
    },
    {
        "section": "Headline results",
        "label": "thermophilization_high_severity.parquet",
        "path": "07_thermophilization/data/processed/thermophilization_high_severity.parquet",
        "producer": "07_thermophilization/scripts/03_stratified_thermophilization.R",
        "grain": "East/West",
        "role": "High-severity proxy summary, currently crown fire.",
    },
    {
        "section": "Headline results",
        "label": "thermophilization_by_time_region.parquet",
        "path": "07_thermophilization/data/processed/thermophilization_by_time_region.parquet",
        "producer": "07_thermophilization/scripts/03_stratified_thermophilization.R",
        "grain": "time bin x East/West",
        "role": "Time since disturbance summary pooled across disturbance classes.",
    },
    {
        "section": "Headline results",
        "label": "thermophilization_by_class_time_region.parquet",
        "path": "07_thermophilization/data/processed/thermophilization_by_class_time_region.parquet",
        "producer": "07_thermophilization/scripts/04_thermophilization_by_class_time.R",
        "grain": "disturbance_class x East/West x time bin",
        "role": "Time since disturbance summary that keeps fire, insect, disease, and weather separate.",
    },
    {
        "section": "Diagnostics",
        "label": "disturbance_year_coverage.parquet",
        "path": "07_thermophilization/data/processed/disturbance_year_coverage.parquet",
        "producer": "07_thermophilization/scripts/04_thermophilization_by_class_time.R",
        "grain": "disturbance_class x East/West",
        "role": "How often FIA provides a usable disturbance year instead of an unknown/continuous code.",
    },
]


STATIC_METADATA = load_static_json("metadata", "thermophilization_outputs.json", default={}) or {}
if STATIC_METADATA.get("outputs"):
    OUTPUTS = STATIC_METADATA["outputs"]


def output_label(item: dict) -> str:
    return item.get("label") or item.get("file") or Path(item["path"]).name


def output_file(item: dict) -> str:
    return item.get("file") or Path(item["path"]).name


def output_inventory() -> pd.DataFrame:
    rows = []
    for item in OUTPUTS:
        full_path = repo_path(item["path"])
        exists = full_path.is_file()
        rows_val = "-"
        size_val = "-"
        if exists and item["path"].endswith(".parquet"):
            meta = parquet_meta(str(full_path))
            rows_val = f"{meta.get('rows'):,}" if meta.get("rows") else "-"
            size_val = f"{meta.get('size_mb'):.1f} MB" if meta.get("size_mb") else "-"
        rows.append(
            {
                "Section": item["section"],
                "Output": output_label(item),
                "File": output_file(item),
                "Status": "OK" if exists else "Missing",
                "Rows": rows_val,
                "Size": size_val,
                "Grain": item["grain"],
                "Role": item["role"],
                "Path": item["path"],
                "Producer": item["producer"],
            }
        )
    return pd.DataFrame(rows)


def status_card(item: dict) -> str:
    full_path = repo_path(item["path"])
    exists = full_path.is_file()
    meta = parquet_meta(str(full_path)) if exists and item["path"].endswith(".parquet") else {}
    rows = f"{meta.get('rows'):,} rows" if meta.get("rows") else "metadata pending"
    size = f"{meta.get('size_mb'):.1f} MB" if meta.get("size_mb") else ""
    status = "ready" if exists else "not found"
    status_class = "fd-pill-green" if exists else "fd-pill-amber"
    label = html.escape(output_label(item))
    role = html.escape(item["role"])
    grain = html.escape(item["grain"])
    path = html.escape(item["path"])
    return f"""
    <div class="fd-route-card">
      <div class="fd-route-title">{label}</div>
      <div class="fd-route-body">{role}</div>
      <span class="fd-pill {status_class}">{status}</span>
      <span class="fd-pill">{grain}</span>
      <div class="fd-file-path">{path}</div>
      <div class="fd-status-line">{rows}{' / ' + size if size else ''}</div>
    </div>
    """


def render_status_grid(section: str) -> None:
    cards = [status_card(item) for item in OUTPUTS if item["section"] == section]
    st.markdown('<div class="fd-grid">' + "".join(cards) + '</div>', unsafe_allow_html=True)


def plot_delta_table(path: str, title: str) -> None:
    full_path = repo_path(path)
    if not full_path.is_file():
        st.info(f"Run the producer script to create `{path}`.")
        return
    df, err = load_parquet(str(full_path))
    if err or df is None:
        st.warning(err or f"Could not load `{path}`.")
        return
    st.dataframe(df, use_container_width=True, hide_index=True)
    if not PLOTLY_AVAILABLE:
        return
    if {"delta_temp_mean", "delta_temp_lo", "delta_temp_hi"}.issubset(df.columns):
        label_cols = [c for c in ["disturbance_class", "region_east_west", "time_bin"] if c in df.columns]
        plot_df = df.copy()
        plot_df["group"] = plot_df[label_cols].astype(str).agg(" / ".join, axis=1) if label_cols else title
        fig = px.bar(
            plot_df,
            x="group",
            y="delta_temp_mean",
            error_y=plot_df["delta_temp_hi"] - plot_df["delta_temp_mean"],
            error_y_minus=plot_df["delta_temp_mean"] - plot_df["delta_temp_lo"],
            color="region_east_west" if "region_east_west" in plot_df.columns else None,
            color_discrete_sequence=["#7bbf92", "#90b8d0", "#d4aa64"],
            labels={"delta_temp_mean": "Delta CWM temperature", "group": ""},
            title=title,
        )
        fig.update_layout(
            paper_bgcolor="#0d1a12",
            plot_bgcolor="#0d1a12",
            font_color="#d4e8da",
            margin=dict(l=20, r=20, t=45, b=110),
            xaxis_tickangle=-35,
        )
        st.plotly_chart(fig, use_container_width=True)


st.markdown(
    page_header(
        "FIA recruitment analysis",
        "Thermophilization Workflow",
        "A guided view of how FIA plot summaries, site climate, species climate affinity, and matched controls combine to test whether post-disturbance seedlings favor warmer or drier climates.",
    ),
    unsafe_allow_html=True,
)

c1, c2, c3, c4 = st.columns(4)
inv = output_inventory()
c1.markdown(metric_card("Workflow stages", "7", "traits, CWM, matching, summaries"), unsafe_allow_html=True)
c2.markdown(metric_card("Core key", "condition visit", "PLT_CN + INVYR + CONDID"), unsafe_allow_html=True)
c3.markdown(metric_card("Matching", "5 controls", "same forest type + region"), unsafe_allow_html=True)
c4.markdown(metric_card("Sign", "delta > 0", "warmer / wetter / drier affinity"), unsafe_allow_html=True)

st.markdown(
    route_grid(
        [
            {
                "title": "What the page answers",
                "body": "Which outputs exist, what each one contains, and which script produces it.",
                "pills": ["outputs", "schemas", "scripts"],
            },
            {
                "title": "How the pieces connect",
                "body": "FIA seedlings get species climate traits, then disturbed conditions are matched to comparable controls.",
                "pills": ["SPCD", "CWM", "matches"],
            },
            {
                "title": "How to read the signal",
                "body": "Positive delta_temp means disturbed seedlings favor warmer-climate species than their matched controls.",
                "pills": ["delta_temp", "delta_cwd"],
            },
        ]
    ),
    unsafe_allow_html=True,
)

tab_guide, tab_outputs, tab_results, tab_use = st.tabs(
    ["Guide", "Outputs", "Results", "How to Use"]
)

with tab_guide:
    st.markdown('<div class="fd-section-label">Workflow map</div>', unsafe_allow_html=True)
    st.markdown(
        workflow_grid(
            [
                {
                    "label": "1",
                    "title": "Site climate baseline",
                    "body": "FIA stable plot IDs are joined to monthly TerraClimate and collapsed to 1981-2010 baseline climate.",
                },
                {
                    "label": "2",
                    "title": "Species niches",
                    "body": "The upstream species-niche module supplies temperature, precipitation, and moisture affinity for each FIA species code.",
                },
                {
                    "label": "3",
                    "title": "Seedling CWM",
                    "body": "Seedling counts weight those species traits to produce one recruitment climate score per condition visit.",
                },
                {
                    "label": "4",
                    "title": "Disturbance classification",
                    "body": "FIA disturbance and treatment codes label clean controls, natural disturbances, and excluded human/harvest cases.",
                },
                {
                    "label": "5",
                    "title": "Matched controls",
                    "body": "Each disturbed condition gets up to five clean controls in the same forest type, region, and inventory window.",
                },
                {
                    "label": "6",
                    "title": "Delta summaries",
                    "body": "Deltas are averaged by disturbance class, region, time since disturbance, and high-severity proxy.",
                },
                {
                    "label": "7",
                    "title": "Niche QA",
                    "body": "Species-name, occurrence, and climate-niche checks live upstream in the species-niche module before this analysis is rerun.",
                },
            ]
        ),
        unsafe_allow_html=True,
    )

    st.markdown('<div class="fd-section-label">Signal convention</div>', unsafe_allow_html=True)
    st.markdown(
        """
        | Field | Reading |
        |---|---|
        | `delta_cwm_temp > 0` | disturbed seedlings favor species associated with warmer baseline climates |
        | `delta_cwm_precip > 0` | disturbed seedlings favor species associated with wetter climates |
        | `delta_cwm_cwd > 0` | disturbed seedlings favor species associated with higher water deficit, a dry-affinity signal |
        | Beech-exclusion and fire species-shift outputs | guardrails against mistaking one dominant species response for broad community turnover |
        """
    )

with tab_outputs:
    st.markdown('<div class="fd-section-label">Status by stage</div>', unsafe_allow_html=True)
    for section in inv["Section"].drop_duplicates():
        st.markdown(f"#### {section}")
        render_status_grid(section)

    st.markdown('<div class="fd-section-label">Full inventory</div>', unsafe_allow_html=True)
    st.dataframe(
        inv.style.map(lambda v: "color: #6dba86" if v == "OK" else "color: #d4aa64", subset=["Status"]),
        use_container_width=True,
        hide_index=True,
    )

with tab_results:
    st.markdown('<div class="fd-section-label">Headline tables</div>', unsafe_allow_html=True)
    result_items = [
        item for item in OUTPUTS
        if item["section"] in {"Headline results", "Diagnostics"}
    ]
    result_labels = [
        f"{output_label(item)} ({output_file(item)})"
        for item in result_items
    ]
    result_choice = st.selectbox(
        "Choose result table",
        result_labels,
    )
    selected_result = result_items[result_labels.index(result_choice)]
    plot_delta_table(selected_result["path"], output_label(selected_result))
    plot_source_link(
        selected_result["producer"],
        label="Producer script",
    )

    st.markdown('<div class="fd-section-label">Interpretation guardrail</div>', unsafe_allow_html=True)
    st.markdown(
        """
        A statistically clean CWM shift is not automatically a climate-driven community shift.
        The first-pass species drilldowns were retired from the active run order until the external
        species niche table is built. Rebuild those checks after the GBIF/BIEN/CHELSA niche source
        is in place.
        """
    )

with tab_use:
    st.markdown('<div class="fd-section-label">R workflow</div>', unsafe_allow_html=True)
    st.code(
        'library(arrow)\n'
        'library(dplyr)\n\n'
        'matches <- read_parquet("07_thermophilization/data/processed/plot_matches.parquet")\n\n'
        '# Collapse five matched controls to one disturbed-plot delta.\n'
        'plot_delta <- matches |>\n'
        '  group_by(disturbed_id, disturbance_class, region_east_west) |>\n'
        '  summarise(\n'
        '    delta_temp = mean(delta_cwm_temp, na.rm = TRUE),\n'
        '    delta_cwd  = mean(delta_cwm_cwd, na.rm = TRUE),\n'
        '    n_controls = n(),\n'
        '    .groups = "drop"\n'
        '  )\n\n'
        '# Positive delta_temp = warmer-affinity recruitment after disturbance.\n'
        'plot_delta |>\n'
        '  group_by(disturbance_class, region_east_west) |>\n'
        '  summarise(mean_delta_temp = mean(delta_temp, na.rm = TRUE), .groups = "drop")',
        language="r",
    )

    st.markdown('<div class="fd-section-label">Python workflow</div>', unsafe_allow_html=True)
    st.code(
        'import pandas as pd\n\n'
        'matches = pd.read_parquet("07_thermophilization/data/processed/plot_matches.parquet")\n'
        'plot_delta = (\n'
        '    matches.groupby(["disturbed_id", "disturbance_class", "region_east_west"])\n'
        '    .agg(delta_temp=("delta_cwm_temp", "mean"),\n'
        '         delta_cwd=("delta_cwm_cwd", "mean"),\n'
        '         n_controls=("control_id", "size"))\n'
        '    .reset_index()\n'
        ')\n'
        'summary = (\n'
        '    plot_delta.groupby(["disturbance_class", "region_east_west"])\n'
        '    .agg(mean_delta_temp=("delta_temp", "mean"))\n'
        '    .reset_index()\n'
        ')',
        language="python",
    )

    st.markdown('<div class="fd-section-label">Run order</div>', unsafe_allow_html=True)
    st.code(
        "Rscript 07_thermophilization/scripts/01_build_plot_recruitment_cwm.R\n"
        "Rscript 07_thermophilization/scripts/02_match_disturbed_to_controls.R\n"
        "Rscript 07_thermophilization/scripts/03_stratified_thermophilization.R\n"
        "Rscript 07_thermophilization/scripts/04_thermophilization_by_class_time.R",
        language="bash",
    )
