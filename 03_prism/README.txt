================================================================================
PRISM 800m Pixel-Level Extraction for IDS Observations
================================================================================
Source: https://prism.oregonstate.edu/
Description: High-resolution (800m) climate data for the contiguous United
States. Developed by Oregon State University's PRISM Climate Group. Monthly
data from 1981 to present. Pixel-level values extracted via direct web service
download (services.nacse.org); no Google Earth Engine account required.

Citation: PRISM Climate Group, Oregon State University, https://prism.oregonstate.edu

Product: AN81m (monthly 800m normals)
Access: https://services.nacse.org/prism/data/get/us/800m/{variable}/{YYYYMM}
Native resolution: 800m (~30 arc-seconds)
Temporal resolution: Monthly (all 12 months preserved per year)
Temporal coverage: 1981-present (IDS extraction: 1997-2024)
Geographic coverage: CONUS only (excludes Alaska and Hawaii)

================================================================================
RUNNING THE WORKFLOW
================================================================================
Run scripts in order. Steps 1-2 are PRISM-specific (in this directory).
Step 3 uses a shared script (in the top-level scripts/ directory) that works
identically for all climate datasets (TerraClimate, WorldClim).

  Step 1: Rscript 03_prism/scripts/01_build_pixel_maps.R
          Maps each CONUS IDS observation to the ~800m PRISM pixels it overlaps.
          Downloads one reference raster from the web service on first run.

  Step 2: Rscript 03_prism/scripts/02_extract_prism.R
          Downloads monthly PRISM grids from the web service, extracts values
          at pixel locations, then deletes the downloaded file. Raw data never
          accumulates on disk. Saves one parquet per year.

  Step 3: Rscript scripts/build_climate_summaries.R prism
          Computes area-weighted climate means per observation per month.
          Water year columns are added during this step.

================================================================================
USING THE OUTPUTS
================================================================================
The workflow produces two key output types:

  1. PIXEL MAPS  -- link observations to raster pixels
     Location: 03_prism/data/processed/pixel_maps/
     Files: damage_areas_pixel_map.parquet
            damage_points_pixel_map.parquet
            surveyed_areas_pixel_map.parquet
     Columns: OBSERVATION_ID, DAMAGE_AREA_ID, pixel_id, x, y, coverage_fraction
     Note: Alaska and Hawaii observations are absent (CONUS only)

  2. PIXEL VALUES -- monthly climate data per unique pixel
     Location: 03_prism/data/processed/pixel_values/
     Files: prism_{year}.parquet (one per year, 1997-2024)
     Columns: pixel_id, x, y, year, month, ppt, tmean, tmin, tmax, tdmean, vpdmin, vpdmax

After summaries (step 3):
     Location: processed/climate/prism/damage_areas_summaries/
     Format: one parquet per variable (read with open_dataset())
     Columns: DAMAGE_AREA_ID, calendar_year, calendar_month, water_year,
              water_year_month, variable, weighted_mean, value_min, value_max,
              n_pixels, n_pixels_with_data, sum_coverage_fraction

================================================================================
VARIABLES EXTRACTED (7 total)
================================================================================
All values are delivered in physical units (no scale factors needed).

Temperature:        tmean (mean), tmin (min), tmax (max), tdmean (dew point) -- °C
Precipitation:      ppt (total precipitation) -- mm
Vapor Pressure:     vpdmin (minimum VPD), vpdmax (maximum VPD) -- hPa

================================================================================
NOTES
================================================================================
- PRISM resolution is ~25x finer than TerraClimate (800m vs ~4km), meaning many
  more pixels per observation and longer extraction times
- Alaska (R10) and Hawaii observations are excluded (CONUS only). For those
  observations, use TerraClimate or WorldClim
- No download script needed: monthly grids are streamed and discarded on the fly
- The web service has a ~0.5s delay between requests; full extraction over
  1997-2024 for all 7 variables takes several hours

================================================================================
DOCUMENTATION
================================================================================
  WORKFLOW.md        Technical architecture, script details, design decisions
  docs/ARCHITECTURE.md (top-level)  Shared pixel decomposition pattern and schemas
