================================================================================
TerraClimate Pixel-Level Extraction for IDS Observations
================================================================================
Source: https://www.climatologylab.org/terraclimate.html
Description: High-resolution (~4km) global climate and water balance dataset
derived from WorldClim, CRU, and JRA-55 reanalysis. Monthly data from 1958 to
present. Pixel-level values extracted at IDS observation locations via GEE.

Citation: Abatzoglou, J.T., S.Z. Dobrowski, S.A. Parks, K.C. Hegewisch (2018).
TerraClimate, a high-resolution global dataset of monthly climate and climatic
water balance from 1958-2015. Scientific Data 5:170191.

GEE Asset: IDAHO_EPSCOR/TERRACLIMATE
Native resolution: ~4km (1/24th degree, approximately 4638m at equator)
Temporal resolution: Monthly (all 12 months preserved per year)
Temporal coverage: 1958-present (IDS extraction: 1997-2024)

================================================================================
DATA ARCHITECTURE (Two-Table Design)
================================================================================
This extraction preserves ALL individual pixel values within each IDS polygon,
rather than computing polygon means. This enables analysis of within-polygon
climate variation.

1. PIXEL MAPS (observation -> pixel mapping)
   Location: 02_terraclimate/data/processed/pixel_maps/
   Files:
     - damage_areas_pixel_map.parquet
     - damage_points_pixel_map.parquet
     - surveyed_areas_pixel_map.parquet
   Columns: OBSERVATION_ID (or SURVEY_FEATURE_ID), DAMAGE_AREA_ID, pixel_id,
            x, y, coverage_fraction
   Purpose: Links each IDS observation to the climate raster pixels it overlaps

2. PIXEL VALUES (climate data per unique pixel per month)
   Location: 02_terraclimate/data/processed/pixel_values/
   Files: terraclimate_{year}.parquet (one per year, 1997-2024)
   Columns: pixel_id, x, y, year, month, tmmx, tmmn, pr, ... (14 climate vars)
   Purpose: Climate values for each unique pixel across all time steps

To join: Use pixel_id to link pixel_values to observations via pixel_maps.
         For polygon summaries, use coverage_fraction as weights.

================================================================================
VARIABLES EXTRACTED (14 total)
================================================================================
See data_dictionary.csv for full details. All values have scale factors applied.

Temperature: tmmx (max), tmmn (min) - units: degrees C
Precipitation & Water: pr (precip), aet (actual ET), pet (reference ET),
                       def (water deficit), soil (moisture), swe (snow), ro (runoff)
Atmospheric: vap (vapor pressure), vpd (vapor pressure deficit),
             srad (solar radiation), vs (wind speed)
Drought Index: pdsi (Palmer Drought Severity Index)

================================================================================
SCRIPTS
================================================================================
  00_explore_terraclimate.R - Exploratory analysis of TerraClimate data
  01_build_pixel_maps.R     - Build pixel maps from GEE reference raster
  02_extract_terraclimate.R - Extract monthly pixel values from GEE

================================================================================
DOCUMENTATION
================================================================================
  - WORKFLOW.md: Script descriptions, data flow, processing decisions
  - cleaning_log.md: Data quality issues and resolutions
  - data_dictionary.csv: Field definitions and scale factors
  - docs/terraclim_ref.pdf: TerraClimate reference publication
