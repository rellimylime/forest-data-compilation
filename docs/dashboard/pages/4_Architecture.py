# ==============================================================================
# pages/4_Architecture.py
# Pixel Decomposition Architecture — pipeline flow and comparison
# ==============================================================================

import sys
from pathlib import Path

import pandas as pd
import streamlit as st

sys.path.insert(0, str(Path(__file__).parent.parent))
from utils import apply_dark_css, metric_card

st.set_page_config(page_title="Architecture", page_icon="🔗", layout="wide")
apply_dark_css()

st.title("🔗 Pipeline Architecture")
st.markdown(
    "Both the **IDS aerial survey** and **FIA forest inventory** pipelines extract climate "
    "using the same core pattern: map observations to raster pixels, extract climate values "
    "once per unique pixel, then join back. The only difference is how observations map to pixels."
)

# ==============================================================================
# PIPELINE FLOW DIAGRAM — HTML table
# ==============================================================================

PIPELINE_HTML = """
<style>
  .pipe-wrap { padding: 4px 0 16px; }
  .pipe-table { width: 100%; border-collapse: separate; border-spacing: 10px 8px; table-layout: fixed; }
  .pipe-hdr {
    text-align: center; padding: 5px 8px;
    background: #21262d; border-radius: 6px;
    color: #8b949e; font-size: 10px; font-weight: 700;
    letter-spacing: 1.5px; text-transform: uppercase;
  }
  .pipe-arrow { text-align: center; color: #444d56; font-size: 22px; vertical-align: middle; width: 32px; }
  /* IDS track — blue */
  .box-ids {
    background: #0d1f33; border: 1px solid #1f4080; border-radius: 10px;
    padding: 14px 16px; color: #ddd; vertical-align: top;
  }
  .box-ids .track-label { color: #58a6ff; font-weight: 700; font-size: 13px; margin-bottom: 6px; }
  /* FIA track — green */
  .box-fia {
    background: #0d2018; border: 1px solid #1f5030; border-radius: 10px;
    padding: 14px 16px; color: #ddd; vertical-align: top;
  }
  .box-fia .track-label { color: #3fb950; font-weight: 700; font-size: 13px; margin-bottom: 6px; }
  /* Shared TerraClimate — purple, spans both tracks */
  .box-tc {
    background: #1a1030; border: 1px solid #5a3fa0; border-radius: 10px;
    padding: 14px 16px; color: #ddd; vertical-align: middle; text-align: center;
  }
  .box-tc .track-label { color: #a371f7; font-weight: 700; font-size: 13px; margin-bottom: 8px; }
  /* Detail text */
  .box-detail { font-size: 12px; line-height: 1.6; color: #c9d1d9; }
  .box-filename { margin-top: 8px; font-size: 11px; color: #8b949e; font-family: monospace; }
  .box-method {
    display: inline-block; margin-top: 6px;
    padding: 2px 8px; border-radius: 12px;
    font-size: 11px; font-family: monospace; font-weight: 600;
  }
  .method-ids { background: #0d2d5e; color: #79c0ff; }
  .method-fia { background: #0d3520; color: #56d364; }
  /* Separator row */
  .pipe-sep { height: 4px; background: transparent; }
  /* Insight box */
  .insight {
    margin-top: 8px; padding: 12px 16px;
    background: #161b22; border-left: 3px solid #a371f7;
    border-radius: 0 8px 8px 0; font-size: 13px; color: #c9d1d9; line-height: 1.6;
  }
  .insight strong { color: #a371f7; }
  /* Step number badge */
  .step-badge {
    display: inline-block; width: 20px; height: 20px; line-height: 20px;
    background: #30363d; border-radius: 50%; text-align: center;
    font-size: 11px; font-weight: 700; color: #8b949e;
    margin-right: 6px; vertical-align: middle;
  }
</style>

<div class="pipe-wrap">
<table class="pipe-table">
  <!-- Header row -->
  <tr>
    <th class="pipe-hdr">① Source Observations</th>
    <th class="pipe-arrow"></th>
    <th class="pipe-hdr">② Pixel Map</th>
    <th class="pipe-arrow"></th>
    <th class="pipe-hdr">③ Climate Extraction</th>
    <th class="pipe-arrow"></th>
    <th class="pipe-hdr">④ Analysis-Ready Output</th>
  </tr>

  <!-- IDS track -->
  <tr>
    <td class="box-ids">
      <div class="track-label">🗺️ IDS — Polygons</div>
      <div class="box-detail">
        <b>4.4M</b> damage areas<br>
        1997–2024 · 10 FS regions<br>
        DCA_CODE · HOST_CODE<br>
        Irregular polygon shapes
      </div>
      <div class="box-filename">ids_layers_cleaned.gpkg<br>layer: damage_areas</div>
    </td>
    <td class="pipe-arrow">→</td>
    <td class="box-ids">
      <div class="track-label">Area-Weighted Pixel Map</div>
      <div class="box-detail">
        Each polygon overlaps <b>1–N</b> raster pixels<br>
        <code style="background:#0d2d5e;padding:1px 4px;border-radius:3px;font-size:11px;">exact_extract()</code> computes<br>
        <b>coverage_fraction</b> per pixel<br>
        Deduplication across polygons
      </div>
      <span class="box-method method-ids">coverage_fraction: 0.0–1.0</span>
      <div class="box-filename">damage_areas_pixel_map.parquet<br>DAMAGE_AREA_ID · pixel_id · coverage_fraction</div>
    </td>
    <td class="pipe-arrow">→</td>
    <!-- TerraClimate spans IDS + FIA rows -->
    <td class="box-tc" rowspan="3">
      <div style="font-size:32px; margin-bottom:8px;">🌐</div>
      <div class="track-label">TerraClimate<br>(Google Earth Engine)</div>
      <div class="box-detail">
        IDAHO_EPSCOR/TERRACLIMATE<br>
        4 km global · monthly<br>
        1958–2024<br>
        14 variables<br><br>
        <div style="background:#0f0a1f;border-radius:6px;padding:8px;margin-top:4px;">
          <div style="color:#a371f7;font-size:11px;font-weight:700;margin-bottom:4px;">KEY EFFICIENCY</div>
          <div style="font-size:11px;line-height:1.5;">Extract climate once per<br>
          <b style="color:#e3c9ff;">unique pixel</b><br>
          — not per observation.<br>
          Many polygons/points<br>share the same 4km cell.</div>
        </div>
      </div>
    </td>
    <td class="pipe-arrow">→</td>
    <td class="box-ids">
      <div class="track-label">Climate Summaries</div>
      <div class="box-detail">
        <b>Area-weighted</b> mean:<br>
        <code style="background:#0d2d5e;padding:1px 4px;border-radius:3px;font-size:11px;">Σ(value × frac) / Σ(frac)</code><br>
        per damage area × month<br><br>
        value_min · value_max<br>
        n_pixels · sum_coverage_fraction
      </div>
      <div class="box-filename">processed/climate/terraclimate/<br>damage_areas_summaries/{var}.parquet<br>~10–140 GB per variable</div>
    </td>
  </tr>

  <!-- Separator row (inside the rowspan) -->
  <tr>
    <td class="pipe-sep"></td>
    <td></td>
    <td class="pipe-sep"></td>
    <td></td>
    <!-- col 5 (TerraClimate) is spanned -->
    <td></td>
    <td class="pipe-sep"></td>
  </tr>

  <!-- FIA track -->
  <tr>
    <td class="box-fia">
      <div class="track-label">🌲 FIA — Point Locations</div>
      <div class="box-detail">
        <b>6,956</b> FIA plot locations<br>
        all 50 US states<br>
        site_id · latitude · longitude<br>
        Single point per plot
      </div>
      <div class="box-filename">all_site_locations.csv<br>(from FIA PLOT table)</div>
    </td>
    <td class="pipe-arrow">→</td>
    <td class="box-fia">
      <div class="track-label">Nearest-Centroid Pixel Map</div>
      <div class="box-detail">
        Each point maps to<br>
        <b>exactly 1</b> raster pixel<br>
        <code style="background:#0d3520;padding:1px 4px;border-radius:3px;font-size:11px;">cellFromXY()</code> finds<br>
        nearest pixel centroid
      </div>
      <span class="box-method method-fia">coverage_fraction = 1.0 (always)</span>
      <div class="box-filename">fia_site_pixel_map.parquet<br>site_id · pixel_id · x · y</div>
    </td>
    <td class="pipe-arrow">→</td>
    <!-- TerraClimate spanned from IDS row -->
    <td class="pipe-arrow">→</td>
    <td class="box-fia">
      <div class="track-label">FIA Site Climate</div>
      <div class="box-detail">
        <b>Direct</b> pixel value<br>
        (no area-weighting needed)<br>
        per site × year × month<br><br>
        + water_year · water_year_month
      </div>
      <div class="box-filename">fia_site_climate.parquet<br>23.5M rows · 62 MB<br>6 vars · 1958–2024</div>
    </td>
  </tr>
</table>

<div class="insight">
  <strong>Why pixel decomposition?</strong>
  Rather than clipping rasters per observation (slow, and imprecise for large or irregular polygons),
  both pipelines first map observations → pixels, then extract climate values once per
  <em>unique pixel</em> and join back. With 4.4M IDS damage areas covering a finite set of
  4km TerraClimate grid cells, deduplication provides an enormous speed and storage advantage.
  The IDS and FIA pipelines use the <strong>same utility functions</strong>
  (<code style="background:#21262d;padding:1px 5px;border-radius:3px;">build_pixel_map()</code>,
  <code style="background:#21262d;padding:1px 5px;border-radius:3px;">extract_climate_from_gee()</code>
  from <code style="background:#21262d;padding:1px 5px;border-radius:3px;">scripts/utils/climate_extract.R</code>)
  — the only difference is the pixel-assignment method at Step ②.
</div>
</div>
"""

st.markdown(PIPELINE_HTML, unsafe_allow_html=True)

st.markdown("---")

# ==============================================================================
# STEP-BY-STEP SECTIONS
# ==============================================================================

tab_ids, tab_fia, tab_comparison, tab_code = st.tabs([
    "🗺️ IDS Pipeline Steps",
    "🌲 FIA Pipeline Steps",
    "⚖️ Side-by-Side Comparison",
    "💻 Code Reference",
])

# ==============================================================================
# IDS PIPELINE STEPS
# ==============================================================================
with tab_ids:
    st.subheader("IDS Climate Extraction — Step by Step")

    steps = [
        ("Step 0", "Download + merge IDS geodatabases",
         "10 regional `.gdb` files (one per FS region, 1997–2024) → merged + cleaned "
         "into a single `ids_layers_cleaned.gpkg` with three layers: "
         "`damage_areas` (4.4M polygons), `damage_points` (1.2M points), `surveyed_areas` (74.5K polygons).",
         "`01_ids/scripts/01_download_ids.R` + `02_merge_clean_ids.R`"),
        ("Step 1", "Build pixel maps",
         "For each layer, use `exact_extract()` to find every TerraClimate 4km pixel that overlaps "
         "each polygon, recording the `coverage_fraction` (0–1) for each polygon-pixel pair. "
         "Output: `{layer}_pixel_map.parquet` with columns "
         "`DAMAGE_AREA_ID · pixel_id · x · y · coverage_fraction`.",
         "`02_terraclimate/scripts/02_build_pixel_maps.R`"),
        ("Step 2", "Deduplicate pixels + extract climate via GEE",
         "Collect all unique `pixel_id` values across all observations. "
         "For each year, build a stacked GEE Image (6–14 variables × 12 months) and call "
         "`.sampleRegions()` at the unique pixel centroids — far fewer requests than "
         "one per observation. Output: `terraclimate_{year}.parquet` (wide, one row per pixel-month).",
         "`02_terraclimate/scripts/03_extract_terraclimate.R`"),
        ("Step 3", "Build area-weighted summaries",
         "Join pixel climate values back through the pixel map to observations. "
         "For each damage area × month, compute: "
         "`weighted_mean = Σ(value × coverage_fraction) / Σ(coverage_fraction)` "
         "plus `value_min`, `value_max`, `n_pixels`, `sum_coverage_fraction`. "
         "Output: one `{variable}.parquet` per climate variable (~10–140 GB each).",
         "`scripts/build_summaries.R` (shared utility, called per variable)"),
    ]

    for step, title, description, script in steps:
        with st.expander(f"**{step}: {title}**", expanded=True):
            st.markdown(description)
            st.caption(f"Script: `{script}`")

    st.markdown("---")
    st.subheader("Why pixel decomposition?")
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
         "`all_site_locations.csv` — 6,956 unique FIA plot locations (lat/lon + site_id) "
         "compiled from the FIA PLOT table across all 50 states. "
         "Each row is a unique geographic plot location (not a visit — INVYR not included here).",
         "`05_fia/scripts/03_extract_trees.R` (LAT/LON extracted from PLOT table)"),
        ("Step 2", "Build site pixel map",
         "For point locations, pixel assignment is much simpler than for polygons: "
         "find the TerraClimate 4km grid cell whose centroid is nearest to each plot location. "
         "Uses `build_pixel_map()` from `climate_extract.R` with a reference raster. "
         "Output: `fia_site_pixel_map.parquet` (site_id · pixel_id · x · y, 6,956 rows).",
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
         "Output: `fia_site_climate.parquet` (23.5M rows · 62 MB).",
         "`05_fia/scripts/06_extract_site_climate.R`"),
    ]

    for step, title, description, script in fia_steps:
        with st.expander(f"**{step}: {title}**", expanded=True):
            st.markdown(description)
            st.caption(f"Script: `{script}`")

    st.markdown("---")
    st.subheader("Water year convention")
    st.markdown(
        "All time series carry **both** calendar and water year columns:\n\n"
        "- Water year runs **October → September**\n"
        "- `water_year`: if `calendar_month ≥ 10` → `calendar_year + 1`, else `calendar_year`\n"
        "- `water_year_month`: 1 = October, 2 = November, …, 12 = September\n\n"
        "This allows analyses to use whichever time base is most appropriate "
        "(calendar year for temperature; water year for precipitation, drought, and SWE)."
    )

# ==============================================================================
# COMPARISON TABLE
# ==============================================================================
with tab_comparison:
    st.subheader("IDS vs. FIA — Side by Side")

    comp_data = [
        ["Observation type",          "Polygon (damage area)",                      "Point (plot location)"],
        ["n_observations",            "~4.4M damage areas",                         "6,956 FIA sites"],
        ["Pixel assignment method",   "`exact_extract()` + `coverage_fraction`",    "`cellFromXY()` nearest centroid"],
        ["coverage_fraction",         "Weighted, 0–1 per pixel",                    "Always 1.0 (single pixel)"],
        ["Area-weighted mean",        "Required — polygons span multiple pixels",   "Not needed — direct pixel value"],
        ["n_unique_pixels",           "Much smaller than n_obs × n_pixels_per_obs", "≤ 6,956 (one per site)"],
        ["Climate source",            "TerraClimate, PRISM, WorldClim",             "TerraClimate only"],
        ["Temporal range",            "1997–2024 (IDS survey era)",                 "1958–2024 (full TerraClimate record)"],
        ["Output per variable",       "~10–140 GB parquet",                         "62 MB total (all 6 variables combined)"],
        ["Output location",           "`processed/climate/{dataset}/damage_areas_summaries/`",
                                      "`05_fia/data/processed/site_climate/`"],
        ["Utility functions",         "`build_pixel_map()`, `extract_climate_from_gee()`",
                                      "Same functions from `climate_extract.R`"],
    ]
    comp_df = pd.DataFrame(comp_data, columns=["Aspect", "IDS Pathway", "FIA Pathway"])
    st.dataframe(comp_df, use_container_width=True, hide_index=True)

    st.markdown("---")
    st.subheader("Shared utility functions")
    utils_data = [
        ["`build_pixel_map()`",            "`scripts/utils/climate_extract.R`", "Both — maps observations to pixel_id"],
        ["`extract_climate_from_gee()`",   "`scripts/utils/climate_extract.R`", "Both — pulls TerraClimate via GEE"],
        ["`init_gee()`",                   "`scripts/utils/gee_utils.R`",       "Both — GEE auth + initialization"],
        ["`calendar_to_water_year()`",     "`scripts/utils/time_utils.R`",      "Both — adds water_year columns"],
        ["`load_config()`",                "`scripts/utils/load_config.R`",     "Both — reads config.yaml paths"],
    ]
    st.dataframe(
        pd.DataFrame(utils_data, columns=["Function", "File", "Used by"]),
        use_container_width=True, hide_index=True,
    )

# ==============================================================================
# CODE REFERENCE
# ==============================================================================
with tab_code:
    st.subheader("Reading large climate summaries efficiently (R)")
    st.code(
        'library(arrow); library(dplyr)\n\n'
        '# DON\'T: read_parquet() loads the full 10-140 GB into memory\n'
        '# DO:    open_dataset() creates a lazy Arrow reference\n'
        'tmmx <- open_dataset("processed/climate/terraclimate/damage_areas_summaries/tmmx.parquet")\n\n'
        '# Filter BEFORE collect() — only touched rows are scanned\n'
        'mpb_summer <- tmmx |>\n'
        '  filter(calendar_month %in% 6:8) |>\n'
        '  collect()\n\n'
        '# Join to IDS metadata via DAMAGE_AREA_ID\n'
        'library(sf)\n'
        'mpb <- st_read("01_ids/data/processed/ids_layers_cleaned.gpkg",\n'
        '               layer = "damage_areas",\n'
        '               query = "SELECT DAMAGE_AREA_ID FROM damage_areas WHERE DCA_CODE = 11006")\n'
        'st_geometry(mpb) <- NULL\n\n'
        'mpb_climate <- tmmx |>\n'
        '  semi_join(mpb, by = "DAMAGE_AREA_ID", copy = TRUE) |>\n'
        '  collect()',
        language="r",
    )

    st.markdown("---")
    st.subheader("Reading FIA site climate (R + Python)")
    col1, col2 = st.columns(2)
    with col1:
        st.code(
            'library(arrow); library(dplyr)\n\n'
            'clim <- read_parquet(\n'
            '  "05_fia/data/processed/site_climate/fia_site_climate.parquet"\n'
            ')\n\n'
            '# Annual water-year precipitation per site\n'
            'clim |>\n'
            '  filter(variable == "pr") |>\n'
            '  group_by(site_id, water_year) |>\n'
            '  summarise(precip_mm = sum(value, na.rm = TRUE))\n\n'
            '# Join to FIA tree metrics via site_id / PLT_CN\n'
            '# (site_id maps to PLT_CN — see 06_extract_site_climate.R)',
            language="r",
        )
    with col2:
        st.code(
            'import pandas as pd\n\n'
            'clim = pd.read_parquet(\n'
            '    "05_fia/data/processed/site_climate/fia_site_climate.parquet"\n'
            ')\n\n'
            '# Annual water-year precipitation per site\n'
            'pr = clim[clim["variable"] == "pr"]\n'
            'annual = (\n'
            '    pr.groupby(["site_id", "water_year"])["value"]\n'
            '    .sum()\n'
            '    .reset_index()\n'
            '    .rename(columns={"value": "precip_mm"})\n'
            ')',
            language="python",
        )

    st.markdown("---")
    st.subheader("Pixel map schemas")
    col1, col2 = st.columns(2)
    with col1:
        st.caption("IDS pixel map (one row per polygon-pixel pair)")
        st.code(
            '# damage_areas_pixel_map.parquet\n'
            'DAMAGE_AREA_ID      large_string  # links to IDS gpkg\n'
            'pixel_id            int64         # unique raster pixel\n'
            'x                   float64       # pixel centroid lon (WGS84)\n'
            'y                   float64       # pixel centroid lat (WGS84)\n'
            'coverage_fraction   float64       # fraction of pixel in polygon (0–1)',
            language="text",
        )
    with col2:
        st.caption("FIA site pixel map (one row per plot location)")
        st.code(
            '# fia_site_pixel_map.parquet\n'
            'site_id     str      # FIA plot identifier\n'
            'pixel_id    int64    # same grid as IDS pixel maps\n'
            'x           float64  # pixel centroid lon\n'
            'y           float64  # pixel centroid lat\n'
            '# coverage_fraction = 1.0 implicitly for all points',
            language="text",
        )
