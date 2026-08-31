# ==============================================================================
# pages/6_Thermophilization.py
# Forest community climate affinity and how it changes between FIA surveys
# ==============================================================================

import sys
import html
from pathlib import Path

import pandas as pd
import streamlit as st

sys.path.insert(0, str(Path(__file__).parent.parent))
from utils import (
    apply_dark_css, load_parquet, load_static_json, metric_card, page_header,
    parquet_meta, plot_source_link, render_top_nav, repo_path, route_grid,
    workflow_grid,
)


st.set_page_config(page_title="Thermophilization", layout="wide")
apply_dark_css()
render_top_nav()


# Fallback used when the static metadata file is unavailable. Keep in sync with
# docs/dashboard/static/metadata/thermophilization_outputs.json.
OUTPUTS = [
    {
        "section": "FIA foundation",
        "label": "Condition metadata",
        "path": "05_fia/data/processed/summaries/plot_condition_metadata.parquet",
        "producer": "05_fia/scripts/summaries/build_condition_metadata.R",
        "grain": "PLT_CN x INVYR x CONDID",
        "role": "Stable plot IDs, condition geography, forest type group, and area fields.",
    },
    {
        "section": "FIA foundation",
        "label": "Forested-condition foundation",
        "path": "05_fia/data/processed/summaries/forested_condition_foundation.parquet",
        "producer": "05_fia/scripts/foundations/02_build_forested_condition_foundation.R",
        "grain": "PLT_CN x INVYR x CONDID",
        "role": "Which conditions are forest, and each one's share of the visit's forested area.",
    },
    {
        "section": "FIA foundation",
        "label": "Disturbance classification",
        "path": "05_fia/data/processed/summaries/fia_condition_disturbance_flags.parquet",
        "producer": "05_fia/scripts/summaries/build_disturbance_classification.R",
        "grain": "PLT_CN x INVYR x CONDID",
        "role": "Control/disturbed eligibility, natural disturbance class, timing, and strata.",
    },
    {
        "section": "Species niches",
        "label": "Species climate niches",
        "path": "06_species_niches/data/processed/species_climate_niches_us_study_area.parquet",
        "producer": "06_species_niches/WORKFLOW.md",
        "grain": "species_key",
        "role": "The climate found across each species' mapped range. Eight indicators.",
    },
    {
        "section": "Community climate",
        "label": "Condition community climate",
        "path": "07_thermophilization/data/processed/plot_community_climate_trees.parquet",
        "producer": "07_thermophilization/scripts/01_build_condition_community_climate.R",
        "grain": "community_layer x PLT_CN x INVYR x CONDID",
        "role": "Weighted mean and median climate affinity per condition, all condition classes.",
    },
    {
        "section": "Community climate",
        "label": "Forest plot-visit CWM",
        "path": "07_thermophilization/data/processed/forest_plot_visit_cwm_trees.parquet",
        "producer": "07_thermophilization/scripts/02_build_forest_plot_visit_cwm.R",
        "grain": "community_layer x PLT_CN x INVYR",
        "role": "The analysis response. Forested conditions only, weighted by forested-area share.",
    },
    {
        "section": "Disturbance",
        "label": "Plot disturbance extent",
        "path": "07_thermophilization/data/processed/plot_disturbance_severity.parquet",
        "producer": "07_thermophilization/scripts/03_build_plot_disturbance_severity.R",
        "grain": "stable_plot_id x PLT_CN x INVYR",
        "role": "How much of the plot visit carried each disturbance type.",
    },
    {
        "section": "Change",
        "label": "Consecutive-survey change",
        "path": "07_thermophilization/data/processed/forest_visit_interval_change_trees.parquet",
        "producer": "07_thermophilization/scripts/04_build_visit_interval_change.R",
        "grain": "community_layer x stable_plot_id x previous_PLT_CN x current_PLT_CN",
        "role": "Change between each survey and the one before it, with annualized rates.",
    },
    {
        "section": "Change",
        "label": "First-to-last change",
        "path": "07_thermophilization/data/processed/forest_first_last_change.parquet",
        "producer": "07_thermophilization/scripts/05_build_first_last_change.R",
        "grain": "stable_plot_id",
        "role": "Change from a plot's earliest to latest survey, all three life stages.",
    },
    {
        "section": "Diagnostics",
        "label": "Before/after survey coverage",
        "path": "07_thermophilization/qa/outputs/disturbance_survey_coverage_by_plot.parquet",
        "producer": "07_thermophilization/qa/scripts/02_disturbance_survey_coverage.R",
        "grain": "stable_plot_id",
        "role": "Which plots have a survey before and after a disturbance, per query.",
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


st.markdown(
    page_header(
        "FIA community analysis",
        "Thermophilization Workflow",
        "How FIA species records and BIEN/TerraClimate species niches combine into forest "
        "community climate affinity, and how that affinity changes between repeated surveys "
        "of the same plot. These are analysis inputs; no models are fitted here.",
    ),
    unsafe_allow_html=True,
)

inv = output_inventory()
ready = int((inv["Status"] == "OK").sum())

c1, c2, c3, c4 = st.columns(4)
c1.markdown(metric_card("Pipeline stages", "5", "condition, visit, disturbance, 2 change designs"), unsafe_allow_html=True)
c2.markdown(metric_card("Response grain", "plot visit", "forested conditions only"), unsafe_allow_html=True)
c3.markdown(metric_card("Life stages", "3", "seedlings, saplings, trees"), unsafe_allow_html=True)
c4.markdown(metric_card("Products present", f"{ready}/{len(inv)}", "on this machine"), unsafe_allow_html=True)

st.markdown(
    route_grid(
        [
            {
                "title": "What the page answers",
                "body": "Which products exist, what each one contains, and which script produces it.",
                "pills": ["outputs", "grains", "scripts"],
            },
            {
                "title": "How the pieces connect",
                "body": "Species get climate niches, conditions get community means, forested "
                        "conditions combine into a plot-visit value, then surveys are compared.",
                "pills": ["niche", "condition", "visit", "change"],
            },
            {
                "title": "How to read the signal",
                "body": "Positive change in mean_temp means the community shifted toward species "
                        "associated with warmer parts of their ranges. It is not the plot's own climate.",
                "pills": ["mean_temp", "mean_cwd"],
            },
        ]
    ),
    unsafe_allow_html=True,
)

tab_guide, tab_outputs, tab_use = st.tabs(["Guide", "Outputs", "How to Use"])

with tab_guide:
    st.markdown('<div class="fd-section-label">Pipeline</div>', unsafe_allow_html=True)
    st.markdown(
        workflow_grid(
            [
                {
                    "label": "1",
                    "title": "Species niches",
                    "body": "The species-niche module summarizes TerraClimate across each species' "
                            "BIEN range map, giving eight climate indicators per species.",
                },
                {
                    "label": "2",
                    "title": "Condition community climate",
                    "body": "Within each FIA condition, species niche values are averaged weighted "
                            "by abundance. Because that is a ratio, condition size does not enter yet.",
                },
                {
                    "label": "3",
                    "title": "Forest plot-visit CWM",
                    "body": "Conditions combine into one value per visit. Only forested conditions "
                            "count, weighted by each one's share of the visit's forested area.",
                },
                {
                    "label": "4",
                    "title": "Disturbance extent",
                    "body": "FIA condition disturbance codes aggregate to the plot visit, giving the "
                            "share of the plot carrying fire, insects, disease, weather, or harvest.",
                },
                {
                    "label": "5",
                    "title": "Change between surveys",
                    "body": "Two designs are built: each survey against the one before it, and the "
                            "earliest survey against the latest. Choosing between them stays open.",
                },
            ]
        ),
        unsafe_allow_html=True,
    )

    st.markdown('<div class="fd-section-label">Reading the numbers</div>', unsafe_allow_html=True)
    st.markdown(
        """
        | Field | Reading |
        |---|---|
        | `mean_temp` | The species present are associated, on average, with this annual mean temperature. **Not the plot's own temperature.** |
        | `delta_mean_temp > 0` | The community shifted toward species associated with warmer parts of their ranges. |
        | `delta_mean_cwd > 0` | A shift toward species associated with higher climate water deficit, a dry-affinity signal. |
        | `forested_plot_proportion` | How much of the plot was forest. A low value means the number describes a small patch. |
        | `frac_weight_with_niche` | How much of the community had a niche value. A mean built from a third of the stems is not the same measurement as one built from nearly all. |
        """
    )

    st.markdown('<div class="fd-section-label">Two change designs</div>', unsafe_allow_html=True)
    st.markdown(
        """
        A plot surveyed in 2002, 2012, and 2022 can be summarized two ways, and both are built:

        - **Consecutive intervals** — 2002 to 2012, then 2012 to 2022. Two rows. More
          observations, and a change can be lined up against a disturbance dated to a particular
          interval. Repeat rows from one plot are correlated, which a model has to account for.
        - **First to last** — 2002 to 2022. One row. One clean long-run change per plot, but it
          cannot say when during the twenty years the change happened.

        Neither is designated primary. See `docs/METHOD_DECISIONS_NEEDED.md`.
        """
    )

    st.markdown('<div class="fd-section-label">Limits worth knowing</div>', unsafe_allow_html=True)
    st.markdown(
        """
        - FIA records a condition disturbance code only when an event killed or damaged at least
          25% of the trees in that condition, over at least 1 acre. **"No disturbance code" means
          "below FIA's threshold", not "undisturbed."**
        - `insect_*` fields cover all insects; FIA cannot isolate bark beetle. `fire_*` fields cover
          all fire; crown fire alone is `prop_crown_fire`.
        - `is_high_severity_fire` stays empty until a cutoff is set in `config.yaml`.
        - Species niches are *realized* niches: where a species grows now, not where it could grow.
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

    st.markdown('<div class="fd-section-label">Preview a product</div>', unsafe_allow_html=True)
    present = [item for item in OUTPUTS if repo_path(item["path"]).is_file()]
    if not present:
        st.info("No thermophilization products found locally. Run the pipeline first.")
    else:
        labels = [f"{output_label(item)} ({output_file(item)})" for item in present]
        choice = st.selectbox("Choose a product", labels)
        selected = present[labels.index(choice)]
        df, err = load_parquet(str(repo_path(selected["path"])))
        if err or df is None:
            st.warning(err or f"Could not load `{selected['path']}`.")
        else:
            st.caption(f"Grain: {selected['grain']}")
            st.dataframe(df.head(200), use_container_width=True, hide_index=True)
        plot_source_link(selected["producer"], label="Producer script")

with tab_use:
    st.markdown('<div class="fd-section-label">Run order</div>', unsafe_allow_html=True)
    st.code(
        "Rscript 05_fia/scripts/foundations/01_build_plot_visit_context.R\n"
        "Rscript 05_fia/scripts/foundations/02_build_forested_condition_foundation.R\n"
        "Rscript 07_thermophilization/scripts/01_build_condition_community_climate.R --layer=trees\n"
        "Rscript 07_thermophilization/scripts/02_build_forest_plot_visit_cwm.R\n"
        "Rscript 07_thermophilization/scripts/03_build_plot_disturbance_severity.R\n"
        "Rscript 07_thermophilization/scripts/04_build_visit_interval_change.R --layer=trees\n"
        "Rscript 07_thermophilization/scripts/05_build_first_last_change.R\n"
        "Rscript 07_thermophilization/qa/scripts/01_validate_thermophilization_products.R",
        language="bash",
    )
    st.caption(
        "Scripts 01 and 04 take --layer=seedlings|saplings|trees. "
        "Script 02 builds all three layers by default."
    )

    st.markdown('<div class="fd-section-label">R workflow</div>', unsafe_allow_html=True)
    st.code(
        'library(arrow)\n'
        'library(dplyr)\n\n'
        'change <- read_parquet(\n'
        '  "07_thermophilization/data/processed/forest_visit_interval_change_trees.parquet"\n'
        ')\n\n'
        '# Intervals containing a dated fire, on plots with good niche coverage.\n'
        'burned <- change |>\n'
        '  filter(fire_within_interval, meets_niche_coverage_threshold) |>\n'
        '  select(stable_plot_id, previous_INVYR, current_INVYR,\n'
        '         delta_mean_temp, rate_mean_temp_per_year, prop_fire)\n\n'
        '# Positive delta_mean_temp = shift toward warmer-affinity species.\n'
        'burned |>\n'
        '  summarise(\n'
        '    n_intervals = n(),\n'
        '    n_plots = n_distinct(stable_plot_id),\n'
        '    median_delta_temp = median(delta_mean_temp, na.rm = TRUE)\n'
        '  )',
        language="r",
    )

    st.markdown('<div class="fd-section-label">Python workflow</div>', unsafe_allow_html=True)
    st.code(
        'import pandas as pd\n\n'
        'change = pd.read_parquet(\n'
        '    "07_thermophilization/data/processed/forest_visit_interval_change_trees.parquet"\n'
        ')\n\n'
        'burned = change[\n'
        '    change["fire_within_interval"] & change["meets_niche_coverage_threshold"]\n'
        ']\n\n'
        'print(\n'
        '    burned["stable_plot_id"].nunique(),\n'
        '    burned["delta_mean_temp"].median(),\n'
        ')',
        language="python",
    )

    st.markdown('<div class="fd-section-label">Before comparing plots</div>', unsafe_allow_html=True)
    st.markdown(
        """
        Filter on `frac_weight_with_niche` (or use `meets_niche_coverage_threshold`), and check
        `forested_plot_proportion` if small forest patches would distort the comparison. Both are
        carried on every row precisely so they can be applied here, rather than being decided upstream.
        """
    )
