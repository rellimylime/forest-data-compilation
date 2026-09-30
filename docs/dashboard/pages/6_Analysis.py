# ==============================================================================
# pages/6_Analysis.py
# The core condition-level analysis, its committed results, and related work
# ==============================================================================

import sys
import html
from pathlib import Path

import pandas as pd
import streamlit as st

sys.path.insert(0, str(Path(__file__).parent.parent))
from utils import (
    apply_dark_css, latest_model_run, load_csv, load_parquet, load_product_catalog,
    load_static_json, metric_card, page_header, page_intro, parquet_meta,
    plot_source_link, repo_link, repo_path, route_grid, workflow_grid,
)


st.set_page_config(page_title="Analysis", layout="wide")
apply_dark_css()


# ------------------------------------------------------------------------------
# Core pipeline definition
# ------------------------------------------------------------------------------

CORE_STEPS = [
    {
        "label": "1",
        "title": "FIA inventory · 05_fia",
        "body": "Download and extract FIA tables, build the national summaries, the "
                "plot-visit context, and the stable-plot site list.",
    },
    {
        "label": "2",
        "title": "Species climate niches · 06_species_niches",
        "body": "Summarize TerraClimate across each species' BIEN range map into eight "
                "climate indicators per species.",
    },
    {
        "label": "3",
        "title": "Condition-level analysis · 09_analysis",
        "body": "Build stable-condition histories, climate-niche CWM change, agent-attributed "
                "mortality, cumulative site CWD, the model inputs, and the preliminary models.",
    },
]

CORE_COMMANDS = """\
# 1. FIA inventory (05_fia)
Rscript 05_fia/scripts/core/01_download_fia.R
Rscript 05_fia/scripts/core/02_inspect_fia.R
Rscript 05_fia/scripts/core/03_extract_trees.R
Rscript 05_fia/scripts/core/04_extract_seedlings_mortality.R
Rscript 05_fia/scripts/core/05_build_fia_summaries.R
Rscript 05_fia/scripts/foundations/01_build_plot_visit_context.R

# 2. Species climate niches (06_species_niches); script 04 needs Google Earth Engine
Rscript 06_species_niches/scripts/01_build_species_universe.R
Rscript 06_species_niches/scripts/02_check_bien_ranges.R
Rscript 06_species_niches/scripts/03_download_bien_ranges.R
Rscript 06_species_niches/qa/scripts/01_validate_species_niche_workflow.R
Rscript 06_species_niches/scripts/04_extract_terraclimate_from_ranges.R
Rscript 06_species_niches/scripts/05_build_species_climate_niches.R

# 3. Condition-level analysis (09_analysis): stages 00-10, models and reports included
Rscript 09_analysis/scripts/run_analysis_pipeline.R
"""

RUNNER_STAGES = [
    ("00", "00_build_remeasurement_components.R", "Official FIA PREV_PLT_CN visit histories"),
    ("01", "01_build_condition_histories_and_cwm.R", "Eligible stable conditions and individual-abundance CWM responses"),
    ("02", "02_build_interval_mortality.R", "Verified mortality for each consecutive-visit interval"),
    ("03", "03_select_complete_condition_histories.sql", "Complete first-to-last condition histories"),
    ("04", "04_build_cumulative_mortality.R", "Cumulative fire, insect, and disease mortality per history"),
    ("05", "05_prepare_site_cwd_inputs.sql", "Measurement dates and FIA site locations"),
    ("05_extract", "Analysis-specific TerraClimate point extraction", "Monthly CWD at the selected analysis sites"),
    ("05_validate", "05_validate_site_cwd_cache.R", "Check the cache against the declared analysis window"),
    ("06", "06_add_cumulative_site_cwd.sql", "Cumulative site CWD and the life-stage model input"),
    ("07", "07_build_pooled_community_cwm.sql", "Combined sapling-and-adult community response"),
    ("08", "08_fit_preliminary_models_and_report.R", "Nine preliminary models and the results report"),
    ("09", "09_run_preliminary_robustness_checks.R", "Focused robustness checks"),
    ("10", "qa/scripts/validate_qa_products.R", "Validate every QA product"),
]

def product_present(path: str) -> bool:
    full = repo_path(path)
    return full.is_file() or full.is_dir()


def catalog_products(catalog: dict, family: str) -> list[dict]:
    return [p for p in catalog.get("products", []) if p.get("family") == family]


def product_table(products: list[dict]) -> pd.DataFrame:
    return pd.DataFrame([
        {
            "Table": p["title"],
            "One row is": p["one_row_is"],
            "On this machine": "present" if product_present(p["path"]) else "not built",
            "Path": p["path"],
            "Made by": p.get("producer") or "",
        }
        for p in products
    ])


def run_summary(run_dir: Path) -> str:
    """First paragraph of the run README, which states what the run contains."""
    readme = run_dir / "README.md"
    if not readme.is_file():
        return ""
    paragraphs = [block.strip() for block in readme.read_text(encoding="utf-8").split("\n\n")]
    body = [block for block in paragraphs if block and not block.startswith("#")]
    return body[0] if body else ""


# ------------------------------------------------------------------------------
# Related work: 07 plot-visit workflow outputs
# ------------------------------------------------------------------------------

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
        "role": "The plot-visit response. Forested conditions only, weighted by forested-area share.",
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


def status_card(item: dict) -> str:
    full_path = repo_path(item["path"])
    exists = full_path.is_file()
    meta = parquet_meta(str(full_path)) if exists and item["path"].endswith(".parquet") else {}
    rows = f"{meta.get('rows'):,} rows" if meta.get("rows") else "metadata pending"
    size = f"{meta.get('size_mb'):.1f} MB" if meta.get("size_mb") else ""
    status = "ready" if exists else "not built"
    status_class = "fd-pill-green" if exists else "fd-pill-amber"
    return f"""
    <div class="fd-route-card">
      <div class="fd-route-title">{html.escape(output_label(item))}</div>
      <div class="fd-route-body">{html.escape(item["role"])}</div>
      <span class="fd-pill {status_class}">{status}</span>
      <span class="fd-pill">{html.escape(item["grain"])}</span>
      <div class="fd-file-path">{html.escape(item["path"])}</div>
      <div class="fd-status-line">{rows}{' / ' + size if size else ''}</div>
    </div>
    """


def render_status_grid(section: str) -> None:
    cards = [status_card(item) for item in OUTPUTS if item["section"] == section]
    st.markdown('<div class="fd-grid">' + "".join(cards) + '</div>', unsafe_allow_html=True)


# ------------------------------------------------------------------------------
# Page
# ------------------------------------------------------------------------------

catalog, _, catalog_error = load_product_catalog()
analysis_products = catalog_products(catalog or {}, "analysis")
run_dir = latest_model_run()
model_fit, _ = load_csv(str(run_dir / "model_fit.csv")) if run_dir else (None, None)

st.markdown(
    page_header(
        "Core analysis",
        "From raw FIA to model results",
        "The end-to-end path that produces the model data and the current results: FIA "
        "inventory, species climate niches, then the condition-level analysis. The "
        "plot-visit method (07) and the disturbance evidence (08) are kept under Related work; "
        "the models do not use them.",
    ),
    unsafe_allow_html=True,
)
page_intro(
    "The pipeline, the tables it builds, and the committed model results.",
    "core",
    [("pages/5_Data_Catalog.py", "Find data"), ("pages/8_Query_Builder.py", "Build a dataset"),
     ("pages/3_FIA_Forest.py", "Processed FIA data")],
)

present = sum(product_present(p["path"]) for p in analysis_products)
c1, c2, c3, c4 = st.columns(4)
c1.markdown(metric_card("Core steps", "3", "FIA → species niches → analysis"), unsafe_allow_html=True)
c2.markdown(metric_card("Unit of analysis", "condition", "stable FIA condition; never averaged to plots"), unsafe_allow_html=True)
c3.markdown(
    metric_card("Models", str(len(model_fit)) if model_fit is not None else "—",
                "3 responses × saplings, adults, combined"),
    unsafe_allow_html=True,
)
c4.markdown(
    metric_card("Analysis tables", f"{present}/{len(analysis_products)}", "present on this machine"),
    unsafe_allow_html=True,
)

st.markdown(
    route_grid(
        [
            {
                "title": "What the models ask",
                "body": "Does cumulative fire, insect, or disease mortality predict how a condition's "
                        "community climate affinity changed from its first to its last survey?",
                "pills": ["response: CWM change", "predictors: mortality, site CWD"],
            },
            {
                "title": "How to read the response",
                "body": "A positive temperature change means the species present shifted toward species "
                        "associated with warmer parts of their ranges. It is not the site's own climate.",
                "pills": ["temperature", "precipitation", "CWD"],
            },
            {
                "title": "Where the results live",
                "body": "One authoritative model run is committed under 09_analysis/results/model_runs/, "
                        "with its inputs recorded by SHA-256.",
                "pills": ["coefficients", "figures", "robustness"],
            },
        ]
    ),
    unsafe_allow_html=True,
)

tab_pipeline, tab_tables, tab_results, tab_related = st.tabs(
    ["Pipeline", "Tables", "Results", "Related work"]
)

with tab_pipeline:
    st.markdown('<div class="fd-section-label">Three steps, in order</div>', unsafe_allow_html=True)
    st.markdown(workflow_grid(CORE_STEPS), unsafe_allow_html=True)
    st.markdown('<div class="fd-section-label">Commands</div>', unsafe_allow_html=True)
    st.code(CORE_COMMANDS, language="bash")
    st.caption(
        "The analysis runner accepts `--dry-run`, `--from=<stage>`, `--through=<stage>`, "
        "`--skip-extraction`, and `--skip-models`. Full detail: "
        + repo_link("docs/REPRODUCE.md", "docs/REPRODUCE.md, Path 7",
                    anchor="path-7-condition-level-cumulative-mortality-analysis")
        + "."
    )
    st.markdown('<div class="fd-section-label">Analysis runner stages</div>', unsafe_allow_html=True)
    st.dataframe(
        pd.DataFrame(RUNNER_STAGES, columns=["Stage", "Script", "What it does"]),
        width="stretch", hide_index=True,
    )
    plot_source_link("09_analysis/scripts/run_analysis_pipeline.R", label="Runner")
    st.markdown('<div class="fd-section-label">Method definitions</div>', unsafe_allow_html=True)
    st.markdown(
        "- CWMs use individual-abundance weights at condition-visit grain; basal area is not used.\n"
        "- Saplings and adults are kept separate, plus one combined sapling-and-adult community. "
        "Seedlings are excluded because their microplot sampling does not align with condition-level disturbance.\n"
        "- Mortality is verified FIA deaths attributed to fire, insects, or disease by `AGENTCD`, "
        "summed over the complete history. It is not annualized.\n"
        "- Only histories inside the tracked 1997–2025 TerraClimate window enter the models.\n\n"
        "Full definitions: " + repo_link("09_analysis/docs/METHODS.md") + "."
    )

with tab_tables:
    st.markdown('<div class="fd-section-label">Tables the analysis builds</div>', unsafe_allow_html=True)
    if catalog_error or not analysis_products:
        st.warning(catalog_error or "The catalog snapshot lists no analysis tables.")
    else:
        st.dataframe(product_table(analysis_products), width="stretch", hide_index=True)
        st.caption(
            "Descriptions come from the committed catalog snapshot. Use Catalog for every "
            "column, or Build Data to join these tables into one export."
        )
        built = [p for p in analysis_products if repo_path(p["path"]).is_file()]
        if built:
            st.markdown('<div class="fd-section-label">Preview a table</div>', unsafe_allow_html=True)
            titles = [p["title"] for p in built]
            chosen = built[titles.index(st.selectbox("Table", titles))]
            df, err = load_parquet(str(repo_path(chosen["path"])))
            if err or df is None:
                st.warning(err or f"Could not load `{chosen['path']}`.")
            else:
                st.caption(f"One row is {chosen['one_row_is']}. Showing the first 200 rows.")
                st.dataframe(df.head(200), width="stretch", hide_index=True)
            if chosen.get("producer"):
                plot_source_link(chosen["producer"], label="Made by")

with tab_results:
    if run_dir is None:
        st.info("No committed model run was found under `09_analysis/results/model_runs/`.")
    else:
        rel_run = run_dir.relative_to(repo_path())
        st.markdown(f"#### `{run_dir.name}`")
        summary = run_summary(run_dir)
        if summary:
            st.markdown(summary)
        st.caption(
            f"Folder: {repo_link(rel_run.as_posix())} · run index: "
            + repo_link("09_analysis/results/model_runs/README.md")
        )

        st.markdown('<div class="fd-section-label">Model fit</div>', unsafe_allow_html=True)
        if model_fit is not None:
            st.dataframe(
                model_fit[["group_label", "response", "n", "stable_plots", "r_squared", "adjusted_r_squared"]]
                .rename(columns={
                    "group_label": "Group", "response": "Response", "n": "Conditions",
                    "stable_plots": "Stable plots", "r_squared": "R²", "adjusted_r_squared": "Adjusted R²",
                }),
                width="stretch", hide_index=True,
            )

        st.markdown('<div class="fd-section-label">Coefficients</div>', unsafe_allow_html=True)
        coefficients, err = load_csv(str(run_dir / "coefficients.csv"))
        if coefficients is None:
            st.warning(err)
        else:
            f1, f2, f3 = st.columns([1, 1, 1])
            responses = f1.multiselect("Response", sorted(coefficients["response"].unique()),
                                       default=sorted(coefficients["response"].unique()))
            groups = f2.multiselect("Group", list(dict.fromkeys(coefficients["group_label"])),
                                    default=list(dict.fromkeys(coefficients["group_label"])))
            show_intercept = f3.checkbox("Show intercepts", value=False)
            shown = coefficients[
                coefficients["response"].isin(responses) & coefficients["group_label"].isin(groups)
            ]
            if not show_intercept:
                shown = shown[shown["term"] != "(Intercept)"]
            st.dataframe(
                shown[["group_label", "response", "term", "estimate", "conf_low", "conf_high", "p_value"]]
                .rename(columns={
                    "group_label": "Group", "response": "Response", "term": "Term",
                    "estimate": "Estimate", "conf_low": "95% low", "conf_high": "95% high",
                    "p_value": "p",
                }),
                width="stretch", hide_index=True,
            )
            st.caption("Uncertainty is HC1, clustered by stable plot. Preliminary models; see the report for context.")

        manifest, _ = load_csv(str(run_dir / "figures" / "figure_manifest.csv"))
        if manifest is not None and not manifest.empty:
            st.markdown('<div class="fd-section-label">Figures</div>', unsafe_allow_html=True)
            g1, g2, g3 = st.columns(3)
            section = g1.selectbox("Figure type", list(dict.fromkeys(manifest["section"])),
                                   format_func=lambda s: s.replace("_", " ").capitalize())
            in_section = manifest[manifest["section"] == section]
            response = g2.selectbox("Response", list(dict.fromkeys(in_section["response_label"])))
            in_response = in_section[in_section["response_label"] == response]
            driver = g3.selectbox("Predictor", list(dict.fromkeys(in_response["driver_label"])))
            figure = in_response[in_response["driver_label"] == driver].iloc[0]
            figure_path = repo_path(figure["figure_path"])
            if figure_path.is_file():
                st.image(str(figure_path), caption=figure["caption"])
            else:
                st.warning(f"Figure not found: `{figure['figure_path']}`")

        st.markdown('<div class="fd-section-label">Full reports</div>', unsafe_allow_html=True)
        st.caption("Self-contained HTML reports. Download and open in a browser.")
        r1, r2 = st.columns(2)
        for column, rel, label in [
            (r1, "preliminary_results.html", "Preliminary results report"),
            (r2, "robustness/robustness_results.html", "Robustness report"),
        ]:
            report = run_dir / rel
            if report.is_file():
                column.download_button(
                    f"{label} ({report.stat().st_size / 1e6:.1f} MB)",
                    data=report.read_bytes(), file_name=report.name, mime="text/html",
                )

with tab_related:
    st.markdown(
        "These modules contain documented methods and prepared evidence that the current "
        "models do not use. They stay in the repository as resources."
    )

    st.markdown("### Plot-visit community climate · `07_thermophilization`")
    st.markdown(
        "An alternative way to measure community climate affinity: condition CWMs are combined "
        "into one value per **plot visit** (forested conditions only, area-weighted), trees are "
        "weighted by basal area, and surveys are compared both consecutively and first-to-last. "
        "The condition-level analysis replaced this for the current models; its outputs are not "
        "built on this machine."
    )
    with st.expander("How the plot-visit workflow works"):
        st.markdown(
            workflow_grid(
                [
                    {"label": "1", "title": "Condition community climate",
                     "body": "Species niche values averaged within each FIA condition, weighted by "
                             "abundance (basal area for trees)."},
                    {"label": "2", "title": "Forest plot-visit CWM",
                     "body": "Forested conditions combined into one value per visit, weighted by "
                             "each one's share of the forested area."},
                    {"label": "3", "title": "Disturbance extent",
                     "body": "FIA condition disturbance codes aggregated to the share of the plot "
                             "carrying fire, insects, disease, weather, or harvest."},
                    {"label": "4", "title": "Change between surveys",
                     "body": "Each survey against the one before it, and the earliest against the latest."},
                ]
            ),
            unsafe_allow_html=True,
        )
        st.markdown(
            """
            | Field | Reading |
            |---|---|
            | `mean_temp` | The species present are associated, on average, with this annual mean temperature. **Not the plot's own temperature.** |
            | `delta_mean_temp > 0` | The community shifted toward species associated with warmer parts of their ranges. |
            | `forested_plot_proportion` | How much of the plot was forest. A low value means the number describes a small patch. |
            | `frac_weight_with_niche` | How much of the community had a niche value. |

            - FIA records a condition disturbance code only when an event damaged at least 25% of
              the trees over at least 1 acre, so "no code" means "below threshold", not "undisturbed".
            - `is_high_severity_fire` stays empty until a cutoff is set in `config.yaml`.

            """
        )
        st.markdown("Details: " + repo_link("07_thermophilization/README.md") + ".")
    with st.expander("Plot-visit workflow outputs"):
        for section in dict.fromkeys(item["section"] for item in OUTPUTS):
            st.markdown(f"#### {section}")
            render_status_grid(section)

    st.markdown("### Disturbance evidence · `08_disturbance_linkage`")
    st.markdown(
        "Prepared, auditable disturbance evidence kept separate by source: FIA condition fire "
        "codes, live-tree damage agents, MTBS fire perimeters, and IDS aerial detections and "
        "survey coverage. None of it is required by the current mortality models, which measure "
        "disturbance as agent-attributed tree deaths instead."
    )
    linkage_products = catalog_products(catalog or {}, "disturbance_linkage")
    if linkage_products:
        st.dataframe(product_table(linkage_products), width="stretch", hide_index=True)
    st.caption("Details: " + repo_link("08_disturbance_linkage/README.md") + ".")
