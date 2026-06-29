# ==============================================================================
# pages/4_Architecture.py
# Repository architecture explorer
# ==============================================================================

import sys
from pathlib import Path

import pandas as pd
import streamlit as st
import streamlit.components.v1 as components

sys.path.insert(0, str(Path(__file__).parent.parent))
from utils import apply_dark_css, metric_card, render_top_nav


st.set_page_config(page_title="Architecture", page_icon="🧭", layout="wide")
apply_dark_css()
render_top_nav()


PAGE_CSS = """
<style>
  .arch-intro {
    background: linear-gradient(135deg, #161b22 0%, #1b2430 100%);
    border: 1px solid #30363d;
    border-radius: 14px;
    padding: 18px 20px 16px;
    margin-bottom: 18px;
  }
  .arch-intro p {
    margin: 0;
    color: #c9d1d9;
    line-height: 1.6;
  }
  .arch-note {
    background: #161b22;
    border: 1px solid #30363d;
    border-left: 4px solid #58a6ff;
    border-radius: 10px;
    padding: 12px 14px;
    color: #c9d1d9;
    margin: 10px 0 16px;
  }
  .arch-flow {
    display: flex;
    flex-wrap: wrap;
    align-items: stretch;
    gap: 10px;
    margin: 10px 0 18px;
  }
  .arch-card {
    flex: 1 1 200px;
    min-width: 180px;
    background: #161b22;
    border: 1px solid #30363d;
    border-radius: 12px;
    padding: 14px 14px 12px;
    overflow-wrap: anywhere;
  }
  .arch-card.blue { border-color: #1f6feb; background: #111c2f; }
  .arch-card.green { border-color: #2ea043; background: #10241b; }
  .arch-card.purple { border-color: #8957e5; background: #1b1430; }
  .arch-card.gold { border-color: #d29922; background: #2a2110; }
  .arch-card.gray { border-color: #57606a; background: #1b2128; }
  .arch-step {
    display: inline-block;
    font-size: 11px;
    letter-spacing: 0.05em;
    text-transform: uppercase;
    color: #8b949e;
    margin-bottom: 8px;
  }
  .arch-title {
    font-size: 15px;
    font-weight: 700;
    color: #f0f6fc;
    margin-bottom: 6px;
  }
  .arch-body {
    font-size: 13px;
    line-height: 1.55;
    color: #c9d1d9;
  }
  .arch-arrow {
    flex: 0 0 auto;
    align-self: center;
    color: #8b949e;
    font-size: 22px;
    padding: 0 2px;
  }
  .arch-pill {
    display: inline-block;
    margin-top: 8px;
    padding: 3px 9px;
    border-radius: 999px;
    background: #21262d;
    border: 1px solid #30363d;
    color: #8b949e;
    font-size: 11px;
  }
  .arch-mini-card {
    background: #161b22;
    border: 1px solid #30363d;
    border-radius: 12px;
    padding: 14px;
    height: 100%;
  }
  .arch-nav-grid {
    display: grid;
    grid-template-columns: repeat(4, minmax(0, 1fr));
    gap: 12px;
    margin: 10px 0 18px;
  }
  .arch-nav-card {
    background: #161b22;
    border: 1px solid #30363d;
    border-radius: 12px;
    padding: 14px;
  }
  .arch-nav-card h4 {
    margin: 0 0 8px;
    font-size: 15px;
    color: #f0f6fc;
  }
  .arch-nav-card p {
    margin: 0;
    color: #c9d1d9;
    font-size: 13px;
    line-height: 1.55;
  }
  .arch-mini-card h4 {
    margin: 0 0 8px;
    font-size: 15px;
    color: #f0f6fc;
  }
  .arch-mini-card p, .arch-mini-card li {
    color: #c9d1d9;
    font-size: 13px;
    line-height: 1.55;
  }
  .arch-mini-card ul {
    margin: 0;
    padding-left: 18px;
  }
  .arch-caption {
    color: #8b949e;
    font-size: 12px;
    margin-top: -6px;
    margin-bottom: 12px;
  }
  @media (max-width: 960px) {
    .arch-nav-grid {
      grid-template-columns: 1fr;
    }
  }
</style>
"""


st.markdown(PAGE_CSS, unsafe_allow_html=True)


def flow_block(cards):
    parts = ['<div class="arch-flow">']
    for i, card in enumerate(cards):
        parts.append(
            f"""
            <div class="arch-card {card['tone']}">
              <div class="arch-step">{card['step']}</div>
              <div class="arch-title">{card['title']}</div>
              <div class="arch-body">{card['body']}</div>
              {f"<div class='arch-pill'>{card['pill']}</div>" if card.get('pill') else ""}
            </div>
            """
        )
        if i < len(cards) - 1:
            parts.append('<div class="arch-arrow">&rarr;</div>')
    parts.append("</div>")
    return "".join(parts)


CLIMATE_OPTIONS = {
    "TerraClimate": {
        "tone": "purple",
        "why": "Best when you want global coverage and the broadest climate variable set.",
        "scripts": [
            "02_terraclimate/scripts/01_build_pixel_maps.R",
            "02_terraclimate/scripts/02_extract_terraclimate.R",
            "scripts/build_climate_summaries.R terraclimate",
        ],
        "outputs": [
            "02_terraclimate/data/processed/pixel_maps/",
            "02_terraclimate/data/processed/pixel_values/",
            "processed/climate/terraclimate/damage_areas_summaries/",
        ],
        "extraction": "Google Earth Engine extraction at unique TerraClimate pixels.",
    },
    "PRISM": {
        "tone": "gold",
        "why": "Best when you want high-resolution CONUS climate without using Google Earth Engine.",
        "scripts": [
            "03_prism/scripts/01_build_pixel_maps.R",
            "03_prism/scripts/02_extract_prism.R",
            "scripts/build_climate_summaries.R prism",
        ],
        "outputs": [
            "03_prism/data/processed/pixel_maps/",
            "03_prism/data/processed/pixel_values/",
            "processed/climate/prism/damage_areas_summaries/",
        ],
        "extraction": "Download, extract, and discard monthly PRISM files from the web service.",
    },
    "WorldClim": {
        "tone": "blue",
        "why": "Best when you want global coverage from local GeoTIFF files instead of GEE.",
        "scripts": [
            "04_worldclim/scripts/01_download_worldclim.R",
            "04_worldclim/scripts/02_build_pixel_maps.R",
            "04_worldclim/scripts/03_extract_worldclim.R",
            "scripts/build_climate_summaries.R worldclim",
        ],
        "outputs": [
            "04_worldclim/data/raw/",
            "04_worldclim/data/processed/pixel_maps/",
            "04_worldclim/data/processed/pixel_values/",
            "processed/climate/worldclim/damage_areas_summaries/",
        ],
        "extraction": "Read climate values from locally downloaded WorldClim GeoTIFFs.",
    },
}


st.title("Architecture Explorer")
st.markdown(
    """
    <div class="arch-intro">
      <p>
        This page is meant to make the repository feel easier to understand at a glance.
        Instead of one giant diagram, it breaks the architecture into a few calmer views:
        what the two main workstreams are, where the climate branches split, what is shared,
        and what the final outputs feed into.
      </p>
    </div>
    """,
    unsafe_allow_html=True,
)


c1, c2, c3, c4 = st.columns(4)
with c1:
    st.markdown(metric_card("Main workstreams", "2", "IDS + climate and FIA"), unsafe_allow_html=True)
with c2:
    st.markdown(metric_card("Climate options", "3", "TerraClimate, PRISM, WorldClim"), unsafe_allow_html=True)
with c3:
    st.markdown(metric_card("Shared climate step", "1", "build_climate_summaries.R"), unsafe_allow_html=True)
with c4:
    st.markdown(metric_card("Downstream layers", "3", "demos, dashboard, thermophilization"), unsafe_allow_html=True)

st.markdown(
    """
    <div class="arch-nav-grid">
      <div class="arch-nav-card">
        <h4>Start here</h4>
        <p>Use this page for the calmest visual explanation of how the repo fits together.</p>
      </div>
      <div class="arch-nav-card">
        <h4>Then choose a data page</h4>
        <p>Use the top navigation to open <code>IDS Survey</code>, <code>Climate</code>, <code>FIA Forest</code>, or <code>Thermophilization</code>.</p>
      </div>
      <div class="arch-nav-card">
        <h4>Need exact file paths</h4>
        <p>Open <code>Data Catalog</code> for output locations, schemas, and load examples.</p>
      </div>
      <div class="arch-nav-card">
        <h4>Need deeper docs</h4>
        <p>Use <code>README.md</code>, <code>docs/REPRODUCE.md</code>, and the workstream <code>WORKFLOW.md</code> files only when you want more detail.</p>
      </div>
    </div>
    """,
    unsafe_allow_html=True,
)


tab_overview, tab_ids, tab_fia, tab_thermo, tab_shared, tab_outputs = st.tabs(
    ["Overview", "IDS + Climate", "FIA", "Thermophilization", "Shared Pieces", "Outputs"]
)


with tab_overview:
    st.subheader("How the repo connects observations to climate")
    st.markdown(
        "The repository has two main production paths. **IDS** starts with survey polygons and "
        "joins them to gridded climate through a pixel map. **FIA** starts with inventory tables, "
        "builds plot and condition summaries, and optionally joins plot locations to the same "
        "climate layer for downstream thermophilization work."
    )
    st.markdown(
        flow_block(
            [
                {
                    "step": "IDS",
                    "title": "IDS foundation",
                    "body": "Download, inspect, clean, and organize the IDS layers that every climate workflow depends on.",
                    "tone": "blue",
                    "pill": "01_ids/",
                },
                {
                    "step": "Climate",
                    "title": "Grid climate to IDS polygons",
                    "body": "Run TerraClimate, PRISM, and/or WorldClim depending on coverage, period, and resolution needs.",
                    "tone": "purple",
                    "pill": "02_terraclimate/, 03_prism/, 04_worldclim/",
                },
                {
                    "step": "Shared",
                    "title": "Observation summaries",
                    "body": "All IDS climate branches end in the same summary builder for observation-level climate outputs.",
                    "tone": "gold",
                    "pill": "scripts/build_climate_summaries.R",
                },
                {
                    "step": "Use",
                    "title": "Use the outputs",
                    "body": "Processed IDS and climate outputs feed demos, documentation, and dashboard pages.",
                    "tone": "gray",
                    "pill": "output/ and docs/dashboard/",
                },
            ]
        ),
        unsafe_allow_html=True,
    )

    st.markdown(
        flow_block(
            [
                {
                    "step": "FIA",
                    "title": "FIA state tables",
                    "body": "Download FIA state tables and inspect their schema before heavy processing.",
                    "tone": "green",
                    "pill": "05_fia/scripts/01-02",
                },
                {
                    "step": "FIA",
                    "title": "Plot and condition extracts",
                    "body": "Build parquet partitions for trees, conditions, seedlings, mortality, disturbance, and damage agents.",
                    "tone": "green",
                    "pill": "05_fia/scripts/03-04",
                },
                {
                    "step": "FIA",
                    "title": "National FIA summaries",
                    "body": "Aggregate to plot, condition, and seedling outputs used for forest structure, disturbance, and recruitment analysis.",
                    "tone": "green",
                    "pill": "05_fia/scripts/05",
                },
                {
                    "step": "Climate",
                    "title": "Site climate bridge",
                    "body": "Map FIA plot locations to TerraClimate pixels and extract long-term monthly climate for trait and thermophilization workflows.",
                    "tone": "purple",
                    "pill": "05_fia/scripts/06",
                },
            ]
        ),
        unsafe_allow_html=True,
    )

    left, right = st.columns([1.1, 0.9])
    with left:
        st.markdown("#### What is shared across the repo")
        st.markdown(
            """
            - The climate workstreams all use the same pixel-decomposition idea.
            - The final climate summaries all come from one shared script.
            - The dashboard and demo scripts consume the finished outputs in the same way.
            """
        )
    with right:
        st.markdown("#### What changes between paths")
        st.markdown(
            """
            - IDS starts with polygon survey data; FIA starts with plot tables.
            - PRISM is CONUS-only, while TerraClimate and WorldClim are global.
            - FIA site climate is optional and uses point locations instead of polygons.
            """
        )

    st.markdown("---")
    st.markdown("#### Why pixel decomposition")
    st.markdown(
        "| Concern | Naive (clip rasters per obs) | Pixel decomposition |\n"
        "|---------|------------------------------|---------------------|\n"
        "| **Speed** | Clip once per obs × month | Extract once per unique pixel; 4.4M areas share a finite set of 4km cells |\n"
        "| **Accuracy** | Partial pixel coverage ignored | `coverage_fraction` captures how much of each pixel falls inside the polygon |\n"
        "| **Storage** | One raster per observation | Long-format parquets; load one variable at a time with Arrow |\n"
        "| **Adjacent polygons** | Same raster cell extracted repeatedly | Deduplication handles naturally |\n"
        "| **Flexibility** | Fixed spatial summary | Re-aggregate with different weights or spatial filters without re-extracting |\n"
    )

    st.markdown("---")
    st.subheader("Pixel map: the bridge between observations and climate")
    st.markdown(
        "The pixel map is the bridge between two otherwise separate datasets — "
        "observation records on one side, gridded climate on the other. "
        "Joining through it gives every observation a full climate history."
    )
    bridge_left, bridge_mid, bridge_right = st.columns([5, 2, 5])
    with bridge_left:
        st.markdown(
            """
            <div class="arch-mini-card" style="background:#0d1f33;border-color:#1f4080;">
              <div style="font-size:13px;font-weight:700;color:#58a6ff;margin-bottom:6px;">Observation data</div>
              <div style="font-size:11px;color:#8b949e;margin-bottom:8px;">IDS aerial surveys · FIA forest inventory</div>
              <div style="font-family:monospace;font-size:11px;color:#79c0ff;line-height:1.8;">
                DAMAGE_AREA_ID · site_id / PLT_CN<br>
                DCA_CODE · HOST_CODE · acres<br>
                survey_year · region<br>
                species · basal area<br>
                disturbance · mortality
              </div>
            </div>
            """,
            unsafe_allow_html=True,
        )
    with bridge_mid:
        st.markdown(
            """
            <div style="text-align:center;padding-top:30px;">
              <div style="font-size:24px;">🔑</div>
              <div style="font-family:monospace;font-size:11px;color:#8b949e;margin-top:6px;">pixel_map</div>
              <div style="font-family:monospace;font-size:10px;color:#8b949e;">obs_id ↔ pixel_id</div>
            </div>
            """,
            unsafe_allow_html=True,
        )
    with bridge_right:
        st.markdown(
            """
            <div class="arch-mini-card" style="background:#1a1030;border-color:#5a3fa0;">
              <div style="font-size:13px;font-weight:700;color:#a371f7;margin-bottom:6px;">Climate data</div>
              <div style="font-size:11px;color:#8b949e;margin-bottom:8px;">TerraClimate · GEE · 4km · 1958–2024</div>
              <div style="font-family:monospace;font-size:11px;color:#c9a9f7;line-height:1.8;">
                pixel_id · year · month<br>
                tmmx — max temp (°C)<br>
                tmmn — min temp (°C)<br>
                pr — precipitation (mm)<br>
                def — water deficit (mm)<br>
                pet · aet
              </div>
            </div>
            """,
            unsafe_allow_html=True,
        )

    st.markdown(
        '<div style="text-align:center;color:#8b949e;font-size:13px;margin:14px 0;">↓ &nbsp; join on obs_id + pixel_id → every observation now has a full climate history</div>',
        unsafe_allow_html=True,
    )

    st.markdown(
        """
        <div class="arch-mini-card" style="background:#0d2018;border-color:#1f5030;">
          <div style="font-size:13px;font-weight:700;color:#3fb950;margin-bottom:10px;">Analysis-ready questions</div>
          <ul style="margin:0;padding-left:20px;color:#c9d1d9;font-size:13px;line-height:1.7;">
            <li>Did MPB outbreaks (<code>DCA_CODE = 11006</code>) follow years of elevated water deficit?</li>
            <li>Which FIA plots experienced sustained heat stress before high tree mortality was recorded?</li>
            <li>How does pre-outbreak climate differ between infested and non-infested damage areas in the same region and year?</li>
            <li>Is there a drought lag — do deficit anomalies 1–3 years prior predict outbreak size (<code>acres</code>)?</li>
          </ul>
        </div>
        """,
        unsafe_allow_html=True,
    )

with tab_ids:
    st.subheader("IDS plus climate")
    st.caption("Pick one climate dataset to see the branch in a simpler form.")

    dataset = st.selectbox("Climate dataset", list(CLIMATE_OPTIONS.keys()))
    cfg = CLIMATE_OPTIONS[dataset]

    st.markdown(
        flow_block(
            [
                {
                    "step": "1",
                    "title": "IDS foundation",
                    "body": "Clean IDS and build the core layers that all climate branches start from.",
                    "tone": "blue",
                    "pill": "01_ids/",
                },
                {
                    "step": "2",
                    "title": f"{dataset} extraction",
                    "body": cfg["extraction"],
                    "tone": cfg["tone"],
                    "pill": dataset,
                },
                {
                    "step": "3",
                    "title": "Shared climate summaries",
                    "body": "Join climate values back to observations and write final summary parquets.",
                    "tone": "gold",
                    "pill": "build_climate_summaries.R",
                },
                {
                    "step": "4",
                    "title": "Use outputs",
                    "body": "Open the summaries in analysis code, demos, or the dashboard.",
                    "tone": "gray",
                    "pill": "processed/climate/",
                },
            ]
        ),
        unsafe_allow_html=True,
    )

    col1, col2 = st.columns([1.05, 0.95])
    with col1:
        st.markdown("#### When to choose this dataset")
        st.markdown(cfg["why"])

        st.markdown("#### Main scripts")
        st.code("\n".join(cfg["scripts"]), language="text")

    with col2:
        st.markdown("#### Main output locations")
        st.code("\n".join(cfg["outputs"]), language="text")

    st.markdown("#### What makes the climate architecture less repetitive")
    compare_df = pd.DataFrame(
        [
            ["Naive approach", "Clip or extract climate separately for every observation."],
            ["Pixel-decomposition approach", "Map observations to pixels first, then extract climate once per unique pixel."],
            ["Main benefit", "Far less repeated extraction work and cleaner, reusable summary outputs."],
        ],
        columns=["Idea", "Meaning"],
    )
    st.dataframe(compare_df, use_container_width=True, hide_index=True)


with tab_fia:
    st.subheader("FIA pipeline")
    st.markdown(
        '<div class="arch-note">The core FIA path is straightforward: download tables, build state-level parquet extracts, then aggregate to national plot summaries. The site-climate branch is optional.</div>',
        unsafe_allow_html=True,
    )

    st.markdown(
        flow_block(
            [
                {
                    "step": "1",
                    "title": "Download FIA tables",
                    "body": "Get state tables and national reference tables from FIA DataMart.",
                    "tone": "green",
                    "pill": "05_fia/scripts/01_download_fia.R",
                },
                {
                    "step": "2",
                    "title": "Inspect and build lookups",
                    "body": "Check schema and write the species and forest-type lookup parquets.",
                    "tone": "green",
                    "pill": "05_fia/scripts/02_inspect_fia.R",
                },
                {
                    "step": "3",
                    "title": "Extract state tables",
                    "body": "Build state-partitioned tree, condition, seedling, and mortality parquet outputs.",
                    "tone": "green",
                    "pill": "05_fia/scripts/03-04",
                },
                {
                    "step": "4",
                    "title": "Build national summaries",
                    "body": "Aggregate the state outputs into the tracked FIA summary files used downstream.",
                    "tone": "green",
                    "pill": "05_fia/scripts/05_build_fia_summaries.R",
                },
            ]
        ),
        unsafe_allow_html=True,
    )

    left, right = st.columns(2)
    with left:
        st.markdown("#### Core FIA outputs")
        st.code(
            "\n".join(
                [
                    "05_fia/data/processed/summaries/plot_tree_metrics.parquet",
                    "05_fia/data/processed/summaries/plot_seedling_metrics.parquet",
                    "05_fia/data/processed/summaries/plot_seedling_species.parquet",
                    "05_fia/data/processed/summaries/plot_mortality_metrics.parquet",
                    "05_fia/data/processed/summaries/plot_disturbance_history.parquet",
                    "05_fia/data/processed/summaries/plot_disturbance_classification.parquet",
                    "05_fia/data/processed/summaries/plot_condition_metadata.parquet",
                    "05_fia/data/processed/summaries/plot_treatment_history.parquet",
                    "05_fia/data/processed/summaries/plot_exclusion_flags.parquet",
                ]
            ),
            language="text",
        )
    with right:
        st.markdown("#### Optional site-climate branch")
        st.markdown(
            """
            If you need climate at FIA plot locations:

            - compile site coordinates
            - map each site to one TerraClimate pixel
            - extract monthly TerraClimate values
            - write `site_pixel_map.parquet` and `site_climate.parquet`
            """
        )
        st.code("05_fia/scripts/site_climate/02_extract_terraclimate.R", language="text")

    with st.expander("Site-climate branch — step-by-step detail"):
        fia_steps = [
            ("Step 1", "Compile FIA site locations",
             "408,040 stable FIA plot locations with valid public coordinates are compiled from "
             "the condition extracts. Each row is a plot location, not an inventory visit.",
             "05_fia/scripts/site_climate/01_build_site_list.R"),
            ("Step 2", "Build site pixel map",
             "Map each FIA plot to its containing TerraClimate grid cell. The 408,040 sites "
             "currently map to 338,219 unique climate pixels.",
             "05_fia/scripts/site_climate/02_extract_terraclimate.R"),
            ("Step 3", "Authenticate Google Earth Engine (one-time)",
             "Browser authentication writes credentials to `~/.config/earthengine/`; subsequent runs "
             "initialize automatically.",
             "Manual step, once per machine"),
            ("Step 4", "Extract TerraClimate 1958–2024 via GEE",
             "For each year, build a stacked GEE image (6 variables × 12 months) and sample at the "
             "unique site-pixel centroids. Annual outputs land in `_gee_annual/sites_{year}.parquet`.",
             "05_fia/scripts/site_climate/02_extract_terraclimate.R"),
            ("Step 5", "Consolidate to final parquet",
             "Join annual pixel values back to the site pixel map. Point locations mean "
             "`coverage_fraction = 1.0` everywhere, so a direct pixel join is enough. "
             "The current national output has about 1.95 billion rows and is approximately 5.85 GB.",
             "05_fia/scripts/site_climate/02_extract_terraclimate.R"),
        ]
        for step, title, description, script in fia_steps:
            st.markdown(f"**{step} · {title}**")
            st.markdown(description)
            st.caption(f"Script: `{script}`")


with tab_thermo:
    st.subheader("Thermophilization layer")
    st.markdown(
        '<div class="arch-note">This is the downstream analysis path that turns FIA seedlings and species climate niches into a matched disturbed-versus-control recruitment signal.</div>',
        unsafe_allow_html=True,
    )
    st.markdown(
        flow_block(
            [
                {
                    "step": "1",
                    "title": "FIA site climate",
                    "body": "TerraClimate histories at stable FIA plot locations provide the baseline climate space.",
                    "tone": "purple",
                    "pill": "site_climate.parquet",
                },
                {
                    "step": "2",
                    "title": "Species climate affinity",
                    "body": "The species-niche module supplies each FIA species' realized temperature, precipitation, and moisture envelope.",
                    "tone": "green",
                    "pill": "species_climate_niches.parquet",
                },
                {
                    "step": "3",
                    "title": "Seedling CWM",
                    "body": "Species-level seedling counts weight those traits into one recruitment climate score per condition.",
                    "tone": "green",
                    "pill": "plot_recruitment_cwm.parquet",
                },
                {
                    "step": "4",
                    "title": "Matched controls",
                    "body": "Disturbed conditions are compared to clean controls in the same forest type, region, and climate neighborhood.",
                    "tone": "gold",
                    "pill": "plot_matches.parquet",
                },
                {
                    "step": "5",
                    "title": "Deltas",
                    "body": "Summaries report positive or negative CWM shifts by disturbance class, region, and time since disturbance.",
                    "tone": "gray",
                    "pill": "07_thermophilization/scripts/03-04",
                },
            ]
        ),
        unsafe_allow_html=True,
    )

    st.markdown("#### Main products")
    st.markdown(
        "| Product | Why it matters |\n"
        "|---|---|\n"
        "| `plot_disturbance_classification.parquet` | defines control and disturbed candidate pools |\n"
        "| `species_climate_niches.parquet` | gives each species a realized climate fingerprint |\n"
        "| `plot_recruitment_cwm.parquet` | converts seedlings into plot-level climate-affinity scores |\n"
        "| `plot_matches.parquet` | stores disturbed-control pairs and CWM deltas |\n"
        "| `thermophilization_by_class_region.parquet` | headline disturbance-class x region summary |\n"
    )


with tab_shared:
    st.subheader("Shared pieces")
    st.caption("These are the parts that tie the repo together across workstreams.")

    a, b = st.columns(2)
    with a:
        st.markdown(
            """
            <div class="arch-mini-card">
              <h4>Shared climate code</h4>
              <ul>
                <li><code>scripts/utils/climate_extract.R</code> handles pixel maps and extraction helpers.</li>
                <li><code>scripts/utils/time_utils.R</code> keeps calendar and water-year handling consistent.</li>
                <li><code>scripts/utils/gee_utils.R</code> centralizes Google Earth Engine setup.</li>
                <li><code>scripts/build_climate_summaries.R</code> builds final observation summaries for all climate datasets.</li>
              </ul>
            </div>
            """,
            unsafe_allow_html=True,
        )
    with b:
        st.markdown(
            """
            <div class="arch-mini-card">
              <h4>Shared documentation spine</h4>
              <ul>
                <li><code>README.md</code> is the front door.</li>
                <li><code>docs/README.md</code> is the docs hub.</li>
                <li><code>docs/REPRODUCE.md</code> gives exact run order.</li>
                <li><code>docs/DATA_PRODUCTS.md</code> maps outputs to scripts.</li>
              </ul>
            </div>
            """,
            unsafe_allow_html=True,
        )

    st.markdown("#### If you want to read the repo in a calm order")
    read_order = pd.DataFrame(
        [
            ["1", "README.md", "Understand the two main workstreams and where to go next."],
            ["2", "docs/REPRODUCE.md", "See the production run order without digging into code."],
            ["3", "01_ids/README.md or 05_fia/README.md", "Choose the workstream you care about."],
            ["4", "WORKFLOW.md in that directory", "Read the technical detail only after the overview makes sense."],
            ["5", "scripts/ and docs/dashboard/", "Inspect implementation and outputs when you are ready."],
        ],
        columns=["Step", "Read", "Why"],
    )
    st.dataframe(read_order, use_container_width=True, hide_index=True)


with tab_outputs:
    st.subheader("Where the finished outputs go")

    st.markdown(
        flow_block(
            [
                {
                    "step": "Finished",
                    "title": "IDS outputs",
                    "body": "Cleaned layers, matches, and area metrics.",
                    "tone": "blue",
                    "pill": "01_ids/data/processed/ and processed/ids/",
                },
                {
                    "step": "Finished",
                    "title": "Climate outputs",
                    "body": "Final damage-area climate summaries for TerraClimate, PRISM, and/or WorldClim.",
                    "tone": "purple",
                    "pill": "processed/climate/",
                },
                {
                    "step": "Finished",
                    "title": "FIA outputs",
                    "body": "Tracked plot-level summary parquets and optional site climate.",
                    "tone": "green",
                    "pill": "05_fia/data/processed/",
                },
                {
                    "step": "Used by",
                    "title": "Demos and dashboard",
                    "body": "The repo's examples and the Streamlit dashboard read the finished outputs.",
                    "tone": "gray",
                    "pill": "output/ and docs/dashboard/",
                },
            ]
        ),
        unsafe_allow_html=True,
    )

    left, right = st.columns(2)
    with left:
        st.markdown("#### Consumers")
        st.markdown(
            """
            - `scripts/demos/` for example analyses
            - `docs/dashboard/` for interactive exploration
            - downstream notebooks or analysis scripts that read parquet outputs
            """
        )
    with right:
        st.markdown("#### Standalone guide")
        st.markdown(
            """
            If you still want a static architecture page outside Streamlit, the short HTML companion is at:

            `docs/pipeline_diagram.html`

            This dashboard page is the main built-in architecture view.
            """
        )
