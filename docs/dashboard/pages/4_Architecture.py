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
from utils import apply_dark_css, metric_card


st.set_page_config(page_title="Architecture", page_icon="🧭", layout="wide")
apply_dark_css()


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
    st.markdown(metric_card("Main consumers", "2", "demo scripts and dashboard"), unsafe_allow_html=True)

st.markdown(
    """
    <div class="arch-nav-grid">
      <div class="arch-nav-card">
        <h4>Start here</h4>
        <p>Use this page for the calmest visual explanation of how the repo fits together.</p>
      </div>
      <div class="arch-nav-card">
        <h4>Then choose a data page</h4>
        <p>Use the sidebar to open <code>IDS Survey</code>, <code>Climate</code>, or <code>FIA Forest</code>.</p>
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


tab_overview, tab_ids, tab_fia, tab_shared, tab_outputs = st.tabs(
    ["Overview", "IDS + Climate", "FIA", "Shared Pieces", "Outputs"]
)


with tab_overview:
    st.subheader("Start here")
    st.markdown(
        "| Concern | Naive (clip rasters per obs) | Pixel decomposition |\n"
        "|---------|------------------------------|---------------------|\n"
        "| **Speed** | Clip once per obs × month | Extract once per unique pixel; 4.4M areas share a finite set of 4km cells |\n"
        "| **Accuracy** | Partial pixel coverage ignored | `coverage_fraction` captures how much of each pixel falls inside the polygon |\n"
        "| **Storage** | One raster per observation | Long-format parquets; load one variable at a time with Arrow |\n"
        "| **Adjacent polygons** | Same raster cell extracted repeatedly | Deduplication handles naturally |\n"
        "| **Flexibility** | Fixed spatial summary | Re-aggregate with different weights or spatial filters without re-extracting |\n"
    )

# ==============================================================================
# FIA PIPELINE STEPS
# ==============================================================================
with tab_fia:
    st.subheader("FIA Site Climate Extraction — Step by Step")

    fia_steps = [
        ("Step 1", "Compile FIA site locations",
         "`05_fia/data/processed/site_climate/all_site_locations.csv` — 6,956 unique FIA plot locations (lat/lon + site_id) "
         "compiled from the FIA PLOT table across all 50 states. "
         "Each row is a unique geographic plot location (not a visit — INVYR not included here).",
         "`05_fia/scripts/03_extract_trees.R` (LAT/LON extracted from PLOT table)"),
        ("Step 2", "Build site pixel map",
         "For point locations, pixel assignment is much simpler than for polygons: "
         "find the TerraClimate 4km grid cell whose centroid is nearest to each plot location. "
         "Uses `build_pixel_map()` from `climate_extract.R` with a reference raster. "
         "Output: `site_pixel_map.parquet` (site_id · pixel_id · x · y, 6,956 rows).",
         "`05_fia/scripts/06_extract_site_climate.R`"),
        ("Step 3", "Authenticate GEE (one-time)",
         "Browser authentication: `ee$Authenticate(auth_mode = 'notebook')`. "
         "Credentials saved to `~/.config/earthengine/credentials` — "
         "subsequent runs call `ee$Initialize()` automatically.",
         "Manual step; run once per machine"),
        ("Step 4", "Extract TerraClimate 1958–2024 via GEE",
         "Loop over 1958–2024. For each year: build a stacked GEE Image "
         "(6 variables × 12 months = 72 bands), extract values at the unique site pixel centroids "
         "via `.sampleRegions()`. Save to `_gee_annual/yr_{year}.parquet`. "
         "Years with no GEE data (2025+) are skipped with a warning.",
         "`05_fia/scripts/06_extract_site_climate.R` (calls `climate_extract.R`)"),
        ("Step 5", "Consolidate to final parquet",
         "Join annual pixel values back to the site pixel map. "
         "Since all FIA sites are points, `coverage_fraction = 1.0` for all — "
         "no area-weighting needed, just a direct join on `pixel_id`. "
         "Add `water_year` and `water_year_month`. "
         "Output: `site_climate.parquet` (23.5M rows · 62 MB).",
         "`05_fia/scripts/06_extract_site_climate.R`"),
    ]

    for step, title, description, script in fia_steps:
        with st.expander(f"**{step}: {title}**", expanded=True):
            st.markdown(description)
            st.caption(f"Script: `{script}`")

    st.markdown("---")
    st.subheader("What the pixel map makes possible")
    st.markdown(
        "The pixel map is the bridge between two otherwise separate datasets — "
        "observation records on one side, gridded climate on the other. "
        "Joining through it gives every observation a full climate history."
    )
    components.html("""<!DOCTYPE html><html><head><meta charset="utf-8">
<style>
  body { margin: 0; padding: 0; background: transparent;
         font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif; }
  .cd-outer { margin: 16px 0 8px; }

  /* Top row: two worlds + bridge */
  .cd-top { display: flex; align-items: stretch; gap: 0; }

  .cd-world {
    flex: 1; border-radius: 12px; padding: 20px 22px;
    display: flex; flex-direction: column; gap: 10px;
  }
  .cd-world-obs {
    background: #0d1f33; border: 1px solid #1f4080;
  }
  .cd-world-clim {
    background: #1a1030; border: 1px solid #5a3fa0;
  }
  .cd-world-icon { font-size: 28px; margin-bottom: 2px; }
  .cd-world-label {
    font-size: 13px; font-weight: 700; letter-spacing: 0.3px;
    margin-bottom: 4px;
  }
  .cd-obs-label  { color: #58a6ff; }
  .cd-clim-label { color: #a371f7; }
  .cd-world-sub  { font-size: 11px; color: #6e7681; margin-bottom: 8px; }

  .cd-tag-row { display: flex; flex-wrap: wrap; gap: 5px; }
  .cd-tag {
    display: inline-block; padding: 3px 9px; border-radius: 20px;
    font-size: 11px; font-family: monospace; line-height: 1.4;
  }
  .cd-tag-obs  { background: #0d2d5e; color: #79c0ff; }
  .cd-tag-clim { background: #1a0d40; color: #c9a9f7; }
  .cd-tag-dim  { background: #1c2128; color: #8b949e; }

  /* Bridge */
  .cd-bridge {
    display: flex; flex-direction: column;
    align-items: center; justify-content: center;
    padding: 0 18px; flex-shrink: 0; gap: 6px;
  }
  .cd-bridge-line {
    width: 2px; height: 32px;
    background: linear-gradient(to bottom, #1f4080, #5a3fa0);
  }
  .cd-bridge-pill {
    background: #161b22; border: 1px solid #444d56;
    border-radius: 20px; padding: 6px 14px;
    text-align: center;
  }
  .cd-bridge-key { font-size: 18px; }
  .cd-bridge-label {
    font-size: 10px; color: #8b949e; font-family: monospace;
    margin-top: 2px; white-space: nowrap;
  }

  /* Down arrow + result */
  .cd-down {
    text-align: center; padding: 14px 0 10px;
    color: #444d56; font-size: 13px; letter-spacing: 0.5px;
  }
  .cd-down-arrow { font-size: 22px; color: #444d56; display: block; }

  .cd-result {
    background: #0d2018; border: 1px solid #1f5030;
    border-radius: 12px; padding: 18px 22px;
  }
  .cd-result-label {
    color: #3fb950; font-size: 13px; font-weight: 700; margin-bottom: 12px;
  }
  .cd-questions { display: flex; flex-direction: column; gap: 8px; }
  .cd-q {
    display: flex; align-items: baseline; gap: 10px;
    font-size: 12px; color: #c9d1d9; line-height: 1.5;
  }
  .cd-q-bullet {
    color: #3fb950; font-size: 14px; flex-shrink: 0; margin-top: 1px;
  }
  .cd-q-key {
    font-family: monospace; font-size: 11px;
    background: #0d3520; color: #56d364;
    padding: 1px 6px; border-radius: 4px; margin-left: 4px;
  }
</style>

<div class="cd-outer">

  <!-- Two worlds connected by the pixel map -->
  <div class="cd-top">

    <div class="cd-world cd-world-obs">
      <div class="cd-world-icon">🗺️🌲</div>
      <div class="cd-world-label cd-obs-label">Observation Data</div>
      <div class="cd-world-sub">IDS aerial surveys · FIA forest inventory</div>
      <div class="cd-tag-row">
        <span class="cd-tag cd-tag-obs">DAMAGE_AREA_ID</span>
        <span class="cd-tag cd-tag-obs">site_id / PLT_CN</span>
        <span class="cd-tag cd-tag-dim">DCA_CODE (e.g. MPB)</span>
        <span class="cd-tag cd-tag-dim">HOST_CODE · acres</span>
        <span class="cd-tag cd-tag-dim">survey_year · region</span>
        <span class="cd-tag cd-tag-dim">species · basal area</span>
        <span class="cd-tag cd-tag-dim">disturbance · mortality</span>
      </div>
    </div>

    <div class="cd-bridge">
      <div class="cd-bridge-line"></div>
      <div class="cd-bridge-pill">
        <div class="cd-bridge-key">🔑</div>
        <div class="cd-bridge-label">pixel_map</div>
        <div class="cd-bridge-label">obs_id ↔ pixel_id</div>
      </div>
      <div class="cd-bridge-line"></div>
    </div>

    <div class="cd-world cd-world-clim">
      <div class="cd-world-icon">🌐</div>
      <div class="cd-world-label cd-clim-label">Climate Data</div>
      <div class="cd-world-sub">TerraClimate · Google Earth Engine · 4km · 1958–2024</div>
      <div class="cd-tag-row">
        <span class="cd-tag cd-tag-clim">pixel_id · year · month</span>
        <span class="cd-tag cd-tag-dim">tmmx — max temp (°C)</span>
        <span class="cd-tag cd-tag-dim">tmmn — min temp (°C)</span>
        <span class="cd-tag cd-tag-dim">pr — precipitation (mm)</span>
        <span class="cd-tag cd-tag-dim">def — water deficit (mm)</span>
        <span class="cd-tag cd-tag-dim">pet · aet</span>
      </div>
    </div>

  </div>

  <!-- Arrow down -->
  <div class="cd-down">
    <span class="cd-down-arrow">↓</span>
    join on obs_id + pixel_id → every observation now has a full climate history
  </div>

  <!-- What you can ask -->
  <div class="cd-result">
    <div class="cd-result-label">✦ Analysis-ready questions</div>
    <div class="cd-questions">
      <div class="cd-q">
        <span class="cd-q-bullet">›</span>
        <span>Did MPB outbreaks <span class="cd-q-key">DCA_CODE = 11006</span> follow years of elevated water deficit?</span>
      </div>
      <div class="cd-q">
        <span class="cd-q-bullet">›</span>
        <span>Which FIA plots experienced sustained heat stress before high tree mortality was recorded?</span>
      </div>
      <div class="cd-q">
        <span class="cd-q-bullet">›</span>
        <span>How does pre-outbreak climate differ between infested and non-infested damage areas in the same region and year?</span>
      </div>
      <div class="cd-q">
        <span class="cd-q-bullet">›</span>
        <span>Is there a drought lag — do deficit anomalies 1–3 years prior predict outbreak size <span class="cd-q-key">acres</span>?</span>
      </div>
    </div>
  </div>

</div>
</body></html>""", height=420)

    st.markdown("---")
    st.subheader("Water year convention")
    st.markdown(
        flow_block(
            [
                {
                    "step": "Path A",
                    "title": "IDS foundation",
                    "body": "Download, inspect, clean, and organize the IDS layers that every climate workflow depends on.",
                    "tone": "blue",
                    "pill": "01_ids/",
                },
                {
                    "step": "Path A",
                    "title": "Choose climate dataset",
                    "body": "Run TerraClimate, PRISM, and/or WorldClim depending on coverage and resolution needs.",
                    "tone": "purple",
                    "pill": "02_terraclimate/, 03_prism/, 04_worldclim/",
                },
                {
                    "step": "Shared",
                    "title": "Climate summaries",
                    "body": "All climate branches end in the same summary builder for observation-level outputs.",
                    "tone": "gold",
                    "pill": "scripts/build_climate_summaries.R",
                },
                {
                    "step": "Use",
                    "title": "Analysis and exploration",
                    "body": "Processed outputs feed demo scripts, documentation, and the rest of the dashboard.",
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
                    "step": "Path B",
                    "title": "FIA downloads",
                    "body": "Download FIA state tables and inspect their schema before heavy processing.",
                    "tone": "green",
                    "pill": "05_fia/scripts/01-02",
                },
                {
                    "step": "Path B",
                    "title": "State-level extracts",
                    "body": "Build parquet partitions for trees, conditions, seedlings, and mortality.",
                    "tone": "green",
                    "pill": "05_fia/scripts/03-04",
                },
                {
                    "step": "Path B",
                    "title": "National summaries",
                    "body": "Aggregate to plot-level FIA outputs used for forest structure, disturbance, and treatment analysis.",
                    "tone": "green",
                    "pill": "05_fia/scripts/05",
                },
                {
                    "step": "Optional",
                    "title": "Site climate",
                    "body": "Optionally map FIA plot locations to TerraClimate pixels and extract long-term monthly climate.",
                    "tone": "purple",
                    "pill": "05_fia/scripts/06",
                },
            ]
        ),
        unsafe_allow_html=True,
    )

    left, right = st.columns([1.1, 0.9])
    with left:
        st.markdown("#### What is shared")
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
                    "05_fia/data/processed/summaries/plot_mortality_metrics.parquet",
                    "05_fia/data/processed/summaries/plot_disturbance_history.parquet",
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
        st.code("05_fia/scripts/06_extract_site_climate.R", language="text")


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
