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
from utils import apply_dark_css, parquet_meta, load_sample, repo_path, color_status

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
                       "Three layers: damage_areas (4.4M rows), damage_points (1.2M rows), "
                       "surveyed_areas (74.5K rows). Key columns: DAMAGE_AREA_ID, SURVEY_YEAR, "
                       "DCA_CODE, HOST_CODE, ACRES, geometry.",
            "r_code":  (
                'library(sf)\n'
                'gpkg <- "01_ids/data/processed/ids_layers_cleaned.gpkg"\n'
                '# Filter to MPB only at read time — avoids loading 4.4M rows\n'
                'damage_areas <- st_read(gpkg, layer = "damage_areas",\n'
                '  query = "SELECT * FROM damage_areas WHERE DCA_CODE = 11006")\n'
                'surveyed_areas <- st_read(gpkg, layer = "surveyed_areas")'
            ),
            "py_code": (
                'import geopandas as gpd\n'
                'damage_areas = gpd.read_file(\n'
                '    "01_ids/data/processed/ids_layers_cleaned.gpkg",\n'
                '    layer="damage_areas",\n'
                '    where="DCA_CODE = 11006")  # SQL filter at read time'
            ),
        },
    ],
    "TerraClimate": [
        {
            "label":   "IDS damage area pixel map",
            "path":    "02_terraclimate/data/processed/pixel_maps/damage_areas_pixel_map.parquet",
            "format":  "Parquet",
            "desc":    "Links each IDS damage area to its overlapping TerraClimate 4km pixels "
                       "with coverage_fraction weights (0–1). "
                       "Columns: DAMAGE_AREA_ID, pixel_id, x (lon), y (lat), coverage_fraction.",
            "r_code":  'pm <- arrow::read_parquet(\n  "02_terraclimate/data/processed/pixel_maps/damage_areas_pixel_map.parquet")',
            "py_code": 'pm = pd.read_parquet("02_terraclimate/data/processed/pixel_maps/damage_areas_pixel_map.parquet")',
        },
        {
            "label":   "Summaries — tmmx (monthly max temp)",
            "path":    "processed/climate/terraclimate/damage_areas_summaries/tmmx.parquet",
            "format":  "Parquet (~10–13 GB)",
            "desc":    "Monthly area-weighted maximum temperature (°C) per IDS damage area. "
                       "Columns: DAMAGE_AREA_ID, calendar_year, calendar_month, water_year, "
                       "water_year_month, variable, weighted_mean, value_min, value_max.",
            "r_code":  (
                'library(arrow); library(dplyr)\n'
                '# Lazy — no data loaded until collect()\n'
                'ds <- open_dataset("processed/climate/terraclimate/damage_areas_summaries/tmmx.parquet")\n'
                'df <- ds |> filter(calendar_year == 2020) |> collect()'
            ),
            "py_code": (
                'import pyarrow.dataset as ds, pyarrow.compute as pc\n'
                '# Filter before loading (avoids reading full ~10 GB)\n'
                'dataset = ds.dataset("processed/climate/terraclimate/damage_areas_summaries/tmmx.parquet")\n'
                'df = dataset.to_table(filter=pc.equal(pc.field("calendar_year"), 2020)).to_pandas()'
            ),
        },
        {
            "label":   "Summaries — tmmn (monthly min temp)",
            "path":    "processed/climate/terraclimate/damage_areas_summaries/tmmn.parquet",
            "format":  "Parquet (~10–13 GB)",
            "desc":    "Monthly area-weighted minimum temperature (°C) per IDS damage area. Same schema as tmmx.",
            "r_code":  'ds <- arrow::open_dataset("processed/climate/terraclimate/damage_areas_summaries/tmmn.parquet")',
            "py_code": 'import pyarrow.dataset as ds\ndataset = ds.dataset("processed/climate/terraclimate/damage_areas_summaries/tmmn.parquet")',
        },
        {
            "label":   "Summaries — pr (monthly precipitation)",
            "path":    "processed/climate/terraclimate/damage_areas_summaries/pr.parquet",
            "format":  "Parquet (~10–13 GB)",
            "desc":    "Monthly area-weighted precipitation (mm) per IDS damage area. Same schema as tmmx.",
            "r_code":  'ds <- arrow::open_dataset("processed/climate/terraclimate/damage_areas_summaries/pr.parquet")',
            "py_code": 'import pyarrow.dataset as ds\ndataset = ds.dataset("processed/climate/terraclimate/damage_areas_summaries/pr.parquet")',
        },
        {
            "label":   "Summaries — def (climate water deficit)",
            "path":    "processed/climate/terraclimate/damage_areas_summaries/def.parquet",
            "format":  "Parquet (~10–13 GB)",
            "desc":    "Monthly area-weighted climate water deficit / CWD (mm) per IDS damage area. "
                       "CWD = PET − AET; key drought stress predictor. Same schema as tmmx.",
            "r_code":  'ds <- arrow::open_dataset("processed/climate/terraclimate/damage_areas_summaries/def.parquet")',
            "py_code": 'import pyarrow.dataset as ds\ndataset = ds.dataset("processed/climate/terraclimate/damage_areas_summaries/def.parquet")',
        },
        {
            "label":   "Summaries — pet (reference ET)",
            "path":    "processed/climate/terraclimate/damage_areas_summaries/pet.parquet",
            "format":  "Parquet (~10–13 GB)",
            "desc":    "Monthly area-weighted reference evapotranspiration (mm). Same schema as tmmx.",
            "r_code":  'ds <- arrow::open_dataset("processed/climate/terraclimate/damage_areas_summaries/pet.parquet")',
            "py_code": 'import pyarrow.dataset as ds\ndataset = ds.dataset("processed/climate/terraclimate/damage_areas_summaries/pet.parquet")',
        },
        {
            "label":   "Summaries — aet (actual ET)",
            "path":    "processed/climate/terraclimate/damage_areas_summaries/aet.parquet",
            "format":  "Parquet (~10–13 GB)",
            "desc":    "Monthly area-weighted actual evapotranspiration (mm). Same schema as tmmx.",
            "r_code":  'ds <- arrow::open_dataset("processed/climate/terraclimate/damage_areas_summaries/aet.parquet")',
            "py_code": 'import pyarrow.dataset as ds\ndataset = ds.dataset("processed/climate/terraclimate/damage_areas_summaries/aet.parquet")',
        },
        {
            "label":   "Summaries — pdsi (Palmer Drought)",
            "path":    "processed/climate/terraclimate/damage_areas_summaries/pdsi.parquet",
            "format":  "Parquet (~10–13 GB)",
            "desc":    "Monthly area-weighted Palmer Drought Severity Index (unitless). Same schema as tmmx.",
            "r_code":  'ds <- arrow::open_dataset("processed/climate/terraclimate/damage_areas_summaries/pdsi.parquet")',
            "py_code": 'import pyarrow.dataset as ds\ndataset = ds.dataset("processed/climate/terraclimate/damage_areas_summaries/pdsi.parquet")',
        },
        {
            "label":   "Summaries — soil (soil moisture)",
            "path":    "processed/climate/terraclimate/damage_areas_summaries/soil.parquet",
            "format":  "Parquet (~10–13 GB)",
            "desc":    "Monthly area-weighted soil moisture (mm). Same schema as tmmx.",
            "r_code":  'ds <- arrow::open_dataset("processed/climate/terraclimate/damage_areas_summaries/soil.parquet")',
            "py_code": 'import pyarrow.dataset as ds\ndataset = ds.dataset("processed/climate/terraclimate/damage_areas_summaries/soil.parquet")',
        },
        {
            "label":   "Summaries — swe (snow water equiv)",
            "path":    "processed/climate/terraclimate/damage_areas_summaries/swe.parquet",
            "format":  "Parquet (~10–13 GB)",
            "desc":    "Monthly area-weighted snow water equivalent (mm). Same schema as tmmx.",
            "r_code":  'ds <- arrow::open_dataset("processed/climate/terraclimate/damage_areas_summaries/swe.parquet")',
            "py_code": 'import pyarrow.dataset as ds\ndataset = ds.dataset("processed/climate/terraclimate/damage_areas_summaries/swe.parquet")',
        },
        {
            "label":   "Summaries — ro (runoff)",
            "path":    "processed/climate/terraclimate/damage_areas_summaries/ro.parquet",
            "format":  "Parquet (~10–13 GB)",
            "desc":    "Monthly area-weighted runoff (mm). Same schema as tmmx.",
            "r_code":  'ds <- arrow::open_dataset("processed/climate/terraclimate/damage_areas_summaries/ro.parquet")',
            "py_code": 'import pyarrow.dataset as ds\ndataset = ds.dataset("processed/climate/terraclimate/damage_areas_summaries/ro.parquet")',
        },
        {
            "label":   "Summaries — srad (shortwave radiation)",
            "path":    "processed/climate/terraclimate/damage_areas_summaries/srad.parquet",
            "format":  "Parquet (~10–13 GB)",
            "desc":    "Monthly area-weighted downward shortwave radiation (W/m²). Same schema as tmmx.",
            "r_code":  'ds <- arrow::open_dataset("processed/climate/terraclimate/damage_areas_summaries/srad.parquet")',
            "py_code": 'import pyarrow.dataset as ds\ndataset = ds.dataset("processed/climate/terraclimate/damage_areas_summaries/srad.parquet")',
        },
        {
            "label":   "Summaries — vap (vapor pressure)",
            "path":    "processed/climate/terraclimate/damage_areas_summaries/vap.parquet",
            "format":  "Parquet (~10–13 GB)",
            "desc":    "Monthly area-weighted vapor pressure (kPa). Same schema as tmmx.",
            "r_code":  'ds <- arrow::open_dataset("processed/climate/terraclimate/damage_areas_summaries/vap.parquet")',
            "py_code": 'import pyarrow.dataset as ds\ndataset = ds.dataset("processed/climate/terraclimate/damage_areas_summaries/vap.parquet")',
        },
        {
            "label":   "Summaries — vpd (vapor pressure deficit)",
            "path":    "processed/climate/terraclimate/damage_areas_summaries/vpd.parquet",
            "format":  "Parquet (~10–13 GB)",
            "desc":    "Monthly area-weighted vapor pressure deficit (kPa). Same schema as tmmx.",
            "r_code":  'ds <- arrow::open_dataset("processed/climate/terraclimate/damage_areas_summaries/vpd.parquet")',
            "py_code": 'import pyarrow.dataset as ds\ndataset = ds.dataset("processed/climate/terraclimate/damage_areas_summaries/vpd.parquet")',
        },
        {
            "label":   "Summaries — vs (wind speed)",
            "path":    "processed/climate/terraclimate/damage_areas_summaries/vs.parquet",
            "format":  "Parquet (~10–13 GB)",
            "desc":    "Monthly area-weighted wind speed (m/s). Same schema as tmmx.",
            "r_code":  'ds <- arrow::open_dataset("processed/climate/terraclimate/damage_areas_summaries/vs.parquet")',
            "py_code": 'import pyarrow.dataset as ds\ndataset = ds.dataset("processed/climate/terraclimate/damage_areas_summaries/vs.parquet")',
        },
    ],
    "PRISM": [
        {
            "label":   "IDS damage area pixel map",
            "path":    "03_prism/data/processed/pixel_maps/damage_areas_pixel_map.parquet",
            "format":  "Parquet",
            "desc":    "Links each IDS damage area to PRISM 800m pixels (CONUS only). "
                       "Columns: DAMAGE_AREA_ID, pixel_id, x (lon), y (lat), coverage_fraction.",
            "r_code":  'pm <- arrow::read_parquet("03_prism/data/processed/pixel_maps/damage_areas_pixel_map.parquet")',
            "py_code": 'pm = pd.read_parquet("03_prism/data/processed/pixel_maps/damage_areas_pixel_map.parquet")',
        },
        {
            "label":   "Summaries — ppt (monthly precipitation)",
            "path":    "processed/climate/prism/damage_areas_summaries/ppt.parquet",
            "format":  "Parquet (~19–23 GB)",
            "desc":    "Monthly area-weighted precipitation (mm) per IDS damage area. "
                       "CONUS only — AK/HI damage areas have NaN values. "
                       "Columns: DAMAGE_AREA_ID, calendar_year, calendar_month, water_year, "
                       "water_year_month, variable, weighted_mean, value_min, value_max.",
            "r_code":  'ds <- arrow::open_dataset("processed/climate/prism/damage_areas_summaries/ppt.parquet")',
            "py_code": 'import pyarrow.dataset as ds\ndataset = ds.dataset("processed/climate/prism/damage_areas_summaries/ppt.parquet")',
        },
        {
            "label":   "Summaries — tmax (monthly max temp)",
            "path":    "processed/climate/prism/damage_areas_summaries/tmax.parquet",
            "format":  "Parquet (~19–23 GB)",
            "desc":    "Monthly area-weighted maximum temperature (°C). CONUS only. Same schema as ppt.",
            "r_code":  'ds <- arrow::open_dataset("processed/climate/prism/damage_areas_summaries/tmax.parquet")',
            "py_code": 'import pyarrow.dataset as ds\ndataset = ds.dataset("processed/climate/prism/damage_areas_summaries/tmax.parquet")',
        },
        {
            "label":   "Summaries — tmin (monthly min temp)",
            "path":    "processed/climate/prism/damage_areas_summaries/tmin.parquet",
            "format":  "Parquet (~19–23 GB)",
            "desc":    "Monthly area-weighted minimum temperature (°C). CONUS only. Same schema as ppt.",
            "r_code":  'ds <- arrow::open_dataset("processed/climate/prism/damage_areas_summaries/tmin.parquet")',
            "py_code": 'import pyarrow.dataset as ds\ndataset = ds.dataset("processed/climate/prism/damage_areas_summaries/tmin.parquet")',
        },
        {
            "label":   "Summaries — tmean (monthly mean temp)",
            "path":    "processed/climate/prism/damage_areas_summaries/tmean.parquet",
            "format":  "Parquet (~19–23 GB)",
            "desc":    "Monthly area-weighted mean temperature (°C). CONUS only. Same schema as ppt.",
            "r_code":  'ds <- arrow::open_dataset("processed/climate/prism/damage_areas_summaries/tmean.parquet")',
            "py_code": 'import pyarrow.dataset as ds\ndataset = ds.dataset("processed/climate/prism/damage_areas_summaries/tmean.parquet")',
        },
        {
            "label":   "Summaries — tdmean (mean dew point)",
            "path":    "processed/climate/prism/damage_areas_summaries/tdmean.parquet",
            "format":  "Parquet (~19–23 GB)",
            "desc":    "Monthly area-weighted mean dew point temperature (°C). CONUS only. Same schema as ppt.",
            "r_code":  'ds <- arrow::open_dataset("processed/climate/prism/damage_areas_summaries/tdmean.parquet")',
            "py_code": 'import pyarrow.dataset as ds\ndataset = ds.dataset("processed/climate/prism/damage_areas_summaries/tdmean.parquet")',
        },
        {
            "label":   "Summaries — vpdmax (max vapor pressure deficit)",
            "path":    "processed/climate/prism/damage_areas_summaries/vpdmax.parquet",
            "format":  "Parquet (~19–23 GB)",
            "desc":    "Monthly area-weighted maximum VPD (hPa). CONUS only. Same schema as ppt.",
            "r_code":  'ds <- arrow::open_dataset("processed/climate/prism/damage_areas_summaries/vpdmax.parquet")',
            "py_code": 'import pyarrow.dataset as ds\ndataset = ds.dataset("processed/climate/prism/damage_areas_summaries/vpdmax.parquet")',
        },
        {
            "label":   "Summaries — vpdmin (min vapor pressure deficit)",
            "path":    "processed/climate/prism/damage_areas_summaries/vpdmin.parquet",
            "format":  "Parquet (~19–23 GB)",
            "desc":    "Monthly area-weighted minimum VPD (hPa). CONUS only. Same schema as ppt.",
            "r_code":  'ds <- arrow::open_dataset("processed/climate/prism/damage_areas_summaries/vpdmin.parquet")',
            "py_code": 'import pyarrow.dataset as ds\ndataset = ds.dataset("processed/climate/prism/damage_areas_summaries/vpdmin.parquet")',
        },
    ],
    "WorldClim": [
        {
            "label":   "IDS damage area pixel map",
            "path":    "04_worldclim/data/processed/pixel_maps/damage_areas_pixel_map.parquet",
            "format":  "Parquet",
            "desc":    "Links each IDS damage area to WorldClim 4.5km pixels (global). "
                       "Columns: DAMAGE_AREA_ID, pixel_id, x (lon), y (lat), coverage_fraction.",
            "r_code":  'pm <- arrow::read_parquet("04_worldclim/data/processed/pixel_maps/damage_areas_pixel_map.parquet")',
            "py_code": 'pm = pd.read_parquet("04_worldclim/data/processed/pixel_maps/damage_areas_pixel_map.parquet")',
        },
        {
            "label":   "Summaries — prec (monthly precipitation)",
            "path":    "processed/climate/worldclim/damage_areas_summaries/prec.parquet",
            "format":  "Parquet (~9–13 GB)",
            "desc":    "Monthly area-weighted precipitation (mm) per IDS damage area. "
                       "Global coverage 1950–2024. "
                       "Columns: DAMAGE_AREA_ID, calendar_year, calendar_month, water_year, "
                       "water_year_month, variable, weighted_mean, value_min, value_max.",
            "r_code":  'ds <- arrow::open_dataset("processed/climate/worldclim/damage_areas_summaries/prec.parquet")',
            "py_code": 'import pyarrow.dataset as ds\ndataset = ds.dataset("processed/climate/worldclim/damage_areas_summaries/prec.parquet")',
        },
        {
            "label":   "Summaries — tmax (monthly max temp)",
            "path":    "processed/climate/worldclim/damage_areas_summaries/tmax.parquet",
            "format":  "Parquet (~9–13 GB)",
            "desc":    "Monthly area-weighted maximum temperature (°C). Global. Same schema as prec.",
            "r_code":  'ds <- arrow::open_dataset("processed/climate/worldclim/damage_areas_summaries/tmax.parquet")',
            "py_code": 'import pyarrow.dataset as ds\ndataset = ds.dataset("processed/climate/worldclim/damage_areas_summaries/tmax.parquet")',
        },
        {
            "label":   "Summaries — tmin (monthly min temp)",
            "path":    "processed/climate/worldclim/damage_areas_summaries/tmin.parquet",
            "format":  "Parquet (~9–13 GB)",
            "desc":    "Monthly area-weighted minimum temperature (°C). Global. Same schema as prec.",
            "r_code":  'ds <- arrow::open_dataset("processed/climate/worldclim/damage_areas_summaries/tmin.parquet")',
            "py_code": 'import pyarrow.dataset as ds\ndataset = ds.dataset("processed/climate/worldclim/damage_areas_summaries/tmin.parquet")',
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

                # Sample rows (parquets only; skipped for large files not present locally)
                if exists and entry["path"].endswith(".parquet"):
                    sample = load_sample(full_path)
                    if sample is not None:
                        st.markdown("**Sample rows (first 5):**")
                        st.dataframe(sample, use_container_width=True, hide_index=True)
                    elif size_str != "—":
                        st.caption(f"Sample not available for large file ({size_str})")

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
    inv_df.style.map(color_status, subset=["Status"]),
    use_container_width=True,
    hide_index=True,
)
