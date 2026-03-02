# ==============================================================================
# pages/5_Data_Catalog.py
# Data Catalog — all repo outputs with schemas and load code
# ==============================================================================

import os
import sys
from pathlib import Path

import pandas as pd
import streamlit as st

sys.path.insert(0, str(Path(__file__).parent.parent))
from utils import apply_dark_css, parquet_meta, repo_path, color_status

st.set_page_config(page_title="Data Catalog", page_icon="📋", layout="wide")
apply_dark_css()

st.title("📋 Data Catalog")
st.markdown(
    "All processed outputs in the repository — paths, sizes, row counts, schemas, "
    "and load code snippets."
)

# ------------------------------------------------------------------------------
# Full catalog definition
# ------------------------------------------------------------------------------

CATALOG = {
    "IDS": [
        {
            "label":   "IDS cleaned geopackage",
            "path":    "01_ids/data/processed/ids_layers_cleaned.gpkg",
            "format":  "GeoPackage",
            "desc":    "Merged and cleaned IDS data from 10 USFS regions. "
                       "Three layers: damage_areas (4.4M), damage_points (1.2M), surveyed_areas (74.5K).",
            "r_code":  (
                'library(sf)\n'
                'gpkg <- "01_ids/data/processed/ids_layers_cleaned.gpkg"\n'
                'damage_areas <- st_read(gpkg, layer = "damage_areas",\n'
                '  query = "SELECT * FROM damage_areas WHERE DCA_CODE = 11006")\n'
                'surveyed_areas <- st_read(gpkg, layer = "surveyed_areas")'
            ),
            "py_code": (
                'import geopandas as gpd\n'
                'damage_areas = gpd.read_file(\n'
                '    "01_ids/data/processed/ids_layers_cleaned.gpkg",\n'
                '    layer="damage_areas")'
            ),
        },
        {
            "label":   "Damage area → surveyed area",
            "path":    "processed/ids/damage_area_to_surveyed_area.parquet",
            "format":  "Parquet",
            "desc":    "Spatial assignment of each damage area to its enclosing survey footprint. "
                       "Key columns: DAMAGE_AREA_ID, SURVEY_ID, overlap_fraction.",
            "r_code":  'library(arrow)\ndf <- read_parquet("processed/ids/damage_area_to_surveyed_area.parquet")',
            "py_code": 'import pandas as pd\ndf = pd.read_parquet("processed/ids/damage_area_to_surveyed_area.parquet")',
        },
        {
            "label":   "Damage area metrics",
            "path":    "processed/ids/damage_area_area_metrics.parquet",
            "format":  "Parquet",
            "desc":    "Area sizes (ha), survey coverage fractions per damage area.",
            "r_code":  'df <- arrow::read_parquet("processed/ids/damage_area_area_metrics.parquet")',
            "py_code": 'df = pd.read_parquet("processed/ids/damage_area_area_metrics.parquet")',
        },
    ],
    "TerraClimate": [
        {
            "label":   "IDS damage area pixel map",
            "path":    "02_terraclimate/data/processed/pixel_maps/damage_areas_pixel_map.parquet",
            "format":  "Parquet",
            "desc":    "Links each IDS damage area to its overlapping TerraClimate 4km pixels "
                       "with coverage_fraction weights.",
            "r_code":  'pm <- arrow::read_parquet(\n  "02_terraclimate/data/processed/pixel_maps/damage_areas_pixel_map.parquet")',
            "py_code": 'pm = pd.read_parquet("02_terraclimate/data/processed/pixel_maps/damage_areas_pixel_map.parquet")',
        },
        {
            "label":   "Summaries — tmmx (max temp)",
            "path":    "processed/climate/terraclimate/damage_areas_summaries/tmmx.parquet",
            "format":  "Parquet (~10 GB)",
            "desc":    "Monthly area-weighted max temperature per IDS damage area. "
                       "Columns: DAMAGE_AREA_ID, calendar_year, calendar_month, water_year, "
                       "water_year_month, variable, weighted_mean, value_min, value_max.",
            "r_code":  (
                'library(arrow); library(dplyr)\n'
                '# Lazy — no data loaded until collect()\n'
                'ds <- open_dataset("processed/climate/terraclimate/damage_areas_summaries/tmmx.parquet")\n'
                'df <- ds |> filter(calendar_year == 2020) |> collect()'
            ),
            "py_code": (
                'import pyarrow.parquet as pq, pyarrow.compute as pc\n'
                '# Schema only (instant)\n'
                'schema = pq.read_schema("processed/climate/terraclimate/damage_areas_summaries/tmmx.parquet")\n'
                '# Filter before loading\n'
                'import pyarrow.dataset as ds\n'
                'dataset = ds.dataset("processed/climate/terraclimate/damage_areas_summaries/tmmx.parquet")\n'
                'df = dataset.to_table(filter=pc.equal(pc.field("calendar_year"), 2020)).to_pandas()'
            ),
        },
        {
            "label":   "Summaries — other variables (12 more)",
            "path":    "processed/climate/terraclimate/damage_areas_summaries/",
            "format":  "Directory of parquets",
            "desc":    "One parquet per variable: tmmn, pr, srad, vs, vap, vpd, pet, aet, def, soil, swe, ro, pdsi. "
                       "Same schema as tmmx above. ~10–13 GB each.",
            "r_code":  (
                '# Load all variables lazily as a single multi-file dataset\n'
                'library(arrow)\n'
                'all_vars <- open_dataset(\n'
                '  "processed/climate/terraclimate/damage_areas_summaries/")'
            ),
            "py_code": (
                'import pyarrow.dataset as ds\n'
                'all_vars = ds.dataset("processed/climate/terraclimate/damage_areas_summaries/")'
            ),
        },
    ],
    "PRISM": [
        {
            "label":   "IDS damage area pixel map",
            "path":    "03_prism/data/processed/pixel_maps/damage_areas_pixel_map.parquet",
            "format":  "Parquet",
            "desc":    "Links each IDS damage area to PRISM 800m pixels (CONUS only).",
            "r_code":  'pm <- arrow::read_parquet("03_prism/data/processed/pixel_maps/damage_areas_pixel_map.parquet")',
            "py_code": 'pm = pd.read_parquet("03_prism/data/processed/pixel_maps/damage_areas_pixel_map.parquet")',
        },
        {
            "label":   "Summaries — all 7 variables",
            "path":    "processed/climate/prism/damage_areas_summaries/",
            "format":  "Directory of parquets",
            "desc":    "ppt, tmean, tmin, tmax, tdmean, vpdmin, vpdmax. ~19–23 GB each. "
                       "CONUS only — AK/HI rows have NaN values.",
            "r_code":  (
                'library(arrow)\n'
                'prism_ppt <- open_dataset(\n'
                '  "processed/climate/prism/damage_areas_summaries/ppt.parquet")'
            ),
            "py_code": (
                'import pyarrow.dataset as ds\n'
                'prism_ppt = ds.dataset("processed/climate/prism/damage_areas_summaries/ppt.parquet")'
            ),
        },
    ],
    "WorldClim": [
        {
            "label":   "IDS damage area pixel map",
            "path":    "04_worldclim/data/processed/pixel_maps/damage_areas_pixel_map.parquet",
            "format":  "Parquet",
            "desc":    "Links each IDS damage area to WorldClim 4.5km pixels (global).",
            "r_code":  'pm <- arrow::read_parquet("04_worldclim/data/processed/pixel_maps/damage_areas_pixel_map.parquet")',
            "py_code": 'pm = pd.read_parquet("04_worldclim/data/processed/pixel_maps/damage_areas_pixel_map.parquet")',
        },
        {
            "label":   "Summaries — tmin, tmax, prec",
            "path":    "processed/climate/worldclim/damage_areas_summaries/",
            "format":  "Directory of parquets",
            "desc":    "Three variables: tmin, tmax, prec. ~9–13 GB each. Global coverage 1950–2024.",
            "r_code":  (
                'library(arrow)\n'
                'wc_prec <- open_dataset(\n'
                '  "processed/climate/worldclim/damage_areas_summaries/prec.parquet")'
            ),
            "py_code": (
                'import pyarrow.dataset as ds\n'
                'wc_prec = ds.dataset("processed/climate/worldclim/damage_areas_summaries/prec.parquet")'
            ),
        },
    ],
    "FIA": [
        {
            "label":   "plot_tree_metrics.parquet",
            "path":    "05_fia/data/processed/summaries/plot_tree_metrics.parquet",
            "format":  "Parquet",
            "desc":    "BA, diversity (Shannon H), size class, and canopy layer per plot × year. "
                       "One row per PLT_CN × INVYR.",
            "r_code":  'library(arrow)\ntrees <- read_parquet("05_fia/data/processed/summaries/plot_tree_metrics.parquet")',
            "py_code": 'trees = pd.read_parquet("05_fia/data/processed/summaries/plot_tree_metrics.parquet")',
        },
        {
            "label":   "plot_exclusion_flags.parquet",
            "path":    "05_fia/data/processed/summaries/plot_exclusion_flags.parquet",
            "format":  "Parquet",
            "desc":    "Per-plot exclusion flags: exclude_nonforest, exclude_human_dist, "
                       "exclude_harvest, exclude_harvest_agent, exclude_any, has_fire, has_insect. "
                       "Also pct_forested (primary gate). Join on PLT_CN + INVYR.",
            "r_code":  (
                'library(arrow); library(dplyr)\n'
                'flags <- read_parquet("05_fia/data/processed/summaries/plot_exclusion_flags.parquet")\n'
                'clean_plots <- flags |> filter(pct_forested >= 0.5, !exclude_any)'
            ),
            "py_code": (
                'flags = pd.read_parquet("05_fia/data/processed/summaries/plot_exclusion_flags.parquet")\n'
                'clean = flags[(flags["pct_forested"] >= 0.5) & ~flags["exclude_any"]]'
            ),
        },
        {
            "label":   "plot_disturbance_history.parquet",
            "path":    "05_fia/data/processed/summaries/plot_disturbance_history.parquet",
            "format":  "Parquet",
            "desc":    "Long-format disturbance events from COND.DSTRBCD1/2/3. "
                       "One row per condition × disturbance slot. "
                       "Columns: PLT_CN, INVYR, CONDID, DSTRBCD, DSTRBYR, disturbance_label, disturbance_category.",
            "r_code":  'disturb <- arrow::read_parquet("05_fia/data/processed/summaries/plot_disturbance_history.parquet")',
            "py_code": 'disturb = pd.read_parquet("05_fia/data/processed/summaries/plot_disturbance_history.parquet")',
        },
        {
            "label":   "plot_damage_agents.parquet",
            "path":    "05_fia/data/processed/summaries/plot_damage_agents.parquet",
            "format":  "Parquet",
            "desc":    "Tree-level damage agent codes (FHAAST/PTIPS). "
                       "Long format: one row per PLT_CN × SPCD × DAMAGE_AGENT_CD. "
                       "Includes ba_per_acre, n_trees_tpa, agent_label, agent_category.",
            "r_code":  'agents <- arrow::read_parquet("05_fia/data/processed/summaries/plot_damage_agents.parquet")',
            "py_code": 'agents = pd.read_parquet("05_fia/data/processed/summaries/plot_damage_agents.parquet")',
        },
        {
            "label":   "plot_mortality_metrics.parquet",
            "path":    "05_fia/data/processed/summaries/plot_mortality_metrics.parquet",
            "format":  "Parquet",
            "desc":    "Between-measurement mortality from TREE_GRM_COMPONENT. "
                       "Columns: PLT_CN, INVYR, AGENTCD, component_type (natural/harvest), tpamort_per_acre.",
            "r_code":  'mort <- arrow::read_parquet("05_fia/data/processed/summaries/plot_mortality_metrics.parquet")',
            "py_code": 'mort = pd.read_parquet("05_fia/data/processed/summaries/plot_mortality_metrics.parquet")',
        },
        {
            "label":   "plot_seedling_metrics.parquet",
            "path":    "05_fia/data/processed/summaries/plot_seedling_metrics.parquet",
            "format":  "Parquet",
            "desc":    "Seedling regeneration counts per plot × year. "
                       "Columns: PLT_CN, INVYR, treecount_total, count_softwood, count_hardwood, "
                       "n_species_seedling, shannon_h_count.",
            "r_code":  'seed <- arrow::read_parquet("05_fia/data/processed/summaries/plot_seedling_metrics.parquet")',
            "py_code": 'seed = pd.read_parquet("05_fia/data/processed/summaries/plot_seedling_metrics.parquet")',
        },
        {
            "label":   "plot_treatment_history.parquet",
            "path":    "05_fia/data/processed/summaries/plot_treatment_history.parquet",
            "format":  "Parquet",
            "desc":    "All silvicultural treatments (TRTCD 10/20/30/40/50) with TRTYR. "
                       "Long format: one row per condition × treatment slot. "
                       "Columns: PLT_CN, INVYR, CONDID, TRTCD, TRTYR, treatment_label, treatment_category.",
            "r_code":  'treat <- arrow::read_parquet("05_fia/data/processed/summaries/plot_treatment_history.parquet")',
            "py_code": 'treat = pd.read_parquet("05_fia/data/processed/summaries/plot_treatment_history.parquet")',
        },
        {
            "label":   "plot_cond_fortypcd.parquet",
            "path":    "05_fia/data/processed/summaries/plot_cond_fortypcd.parquet",
            "format":  "Parquet",
            "desc":    "Condition-level forest type (FORTYPCD) and disturbance codes pass-through. "
                       "One row per PLT_CN × INVYR × CONDID.",
            "r_code":  'cond <- arrow::read_parquet("05_fia/data/processed/summaries/plot_cond_fortypcd.parquet")',
            "py_code": 'cond = pd.read_parquet("05_fia/data/processed/summaries/plot_cond_fortypcd.parquet")',
        },
        {
            "label":   "fia_site_pixel_map.parquet",
            "path":    "05_fia/data/processed/site_climate/fia_site_pixel_map.parquet",
            "format":  "Parquet",
            "desc":    "Maps each FIA plot location (site_id) to its TerraClimate 4km pixel. "
                       "Columns: site_id, pixel_id, x (lon), y (lat).",
            "r_code":  'pm <- arrow::read_parquet("05_fia/data/processed/site_climate/fia_site_pixel_map.parquet")',
            "py_code": 'pm = pd.read_parquet("05_fia/data/processed/site_climate/fia_site_pixel_map.parquet")',
        },
        {
            "label":   "fia_site_climate.parquet",
            "path":    "05_fia/data/processed/site_climate/fia_site_climate.parquet",
            "format":  "Parquet",
            "desc":    "Monthly TerraClimate at 6,956 FIA plot locations. 23.5M rows. "
                       "Columns: site_id, year, month, water_year, water_year_month, variable, value. "
                       "Variables: tmmx (°C), tmmn (°C), pr (mm), def (mm), pet (mm), aet (mm). "
                       "Period: 1958–2024.",
            "r_code":  (
                'library(arrow); library(dplyr)\n'
                'clim <- read_parquet("05_fia/data/processed/site_climate/fia_site_climate.parquet")\n'
                '# Annual water-year precip\n'
                'clim |> filter(variable == "pr") |>\n'
                '  group_by(site_id, water_year) |>\n'
                '  summarise(precip_mm = sum(value, na.rm = TRUE))'
            ),
            "py_code": (
                'clim = pd.read_parquet("05_fia/data/processed/site_climate/fia_site_climate.parquet")\n'
                '# Annual water-year precip\n'
                'precip = (clim[clim["variable"] == "pr"]\n'
                '          .groupby(["site_id", "water_year"])["value"].sum())'
            ),
        },
    ],
}

# ------------------------------------------------------------------------------
# Column search input
# ------------------------------------------------------------------------------

col_search = st.text_input("🔍 Search across all catalog entries (label, path, description)", key="catalog_search")

# ------------------------------------------------------------------------------
# Section navigation
# ------------------------------------------------------------------------------

section_tabs = st.tabs(list(CATALOG.keys()))

for tab, (section, entries) in zip(section_tabs, CATALOG.items()):
    with tab:
        for entry in entries:
            # Apply search filter
            if col_search:
                searchable = f"{entry['label']} {entry['path']} {entry['desc']}".lower()
                if col_search.lower() not in searchable:
                    continue

            full_path = str(repo_path(entry["path"]))
            exists = os.path.isfile(full_path) or os.path.isdir(full_path)
            status = "✅" if exists else "❌"

            # Get metadata for parquets
            if exists and entry["path"].endswith(".parquet"):
                m = parquet_meta(full_path)
                size_str  = f"{m['size_mb']:.1f} MB" if m.get("size_mb") else "—"
                rows_str  = f"{m['rows']:,} rows" if m.get("rows") else "—"
                cols_list = m.get("columns", [])
                dtypes    = m.get("dtypes", [])
            else:
                size_str, rows_str, cols_list, dtypes = "—", "—", [], []

            with st.expander(f"{status} **{entry['label']}**  ·  `{entry['path']}`  ·  {size_str}  ·  {entry['format']}"):
                st.markdown(entry["desc"])

                if rows_str != "—":
                    st.caption(f"{rows_str}  ·  {len(cols_list)} columns")

                if cols_list:
                    st.markdown("**Schema:**")
                    schema_df = pd.DataFrame({
                        "Column": cols_list,
                        "Type":   dtypes if dtypes else ["—"] * len(cols_list),
                    })
                    st.dataframe(schema_df, use_container_width=True, hide_index=True,
                                 height=min(400, 35 * len(cols_list) + 40))

                r_tab, py_tab = st.tabs(["R", "Python"])
                with r_tab:
                    st.code(entry["r_code"], language="r")
                with py_tab:
                    st.code(entry["py_code"], language="python")

# ------------------------------------------------------------------------------
# Summary inventory table
# ------------------------------------------------------------------------------

st.markdown("---")
st.subheader("Full Inventory Table")
all_rows = []
for section, entries in CATALOG.items():
    for entry in entries:
        full_path = str(repo_path(entry["path"]))
        exists = os.path.isfile(full_path) or os.path.isdir(full_path)
        if exists and entry["path"].endswith(".parquet"):
            m = parquet_meta(full_path)
            size_str = f"{m['size_mb']:.1f} MB" if m.get("size_mb") else "—"
            rows_str = f"{m['rows']:,}" if m.get("rows") else "—"
        else:
            size_str, rows_str = "—", "—"
        all_rows.append({
            "Section": section,
            "Label": entry["label"],
            "Status": "✅" if exists else "❌",
            "Format": entry["format"],
            "Size": size_str,
            "Rows": rows_str,
            "Path": entry["path"],
        })

inv_df = pd.DataFrame(all_rows)
st.dataframe(
    inv_df.style.applymap(color_status, subset=["Status"]),
    use_container_width=True,
    hide_index=True,
)
