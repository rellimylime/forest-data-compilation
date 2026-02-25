================================================================================
WorldClim Monthly Weather Pixel-Level Extraction for IDS Observations
================================================================================
Source: https://www.worldclim.org/data/monthlywth.html
Description: Global historical monthly weather data at ~4.5km resolution.
Interpolated from weather station observations (CRU TS 4.09). Monthly 1950-2024.

Citation: Fick, S.E. and R.J. Hijmans (2017). WorldClim 2: new 1km spatial
resolution climate surfaces for global land areas. International Journal of
Climatology 37(12): 4302-4315.

Data source: https://geodata.ucdavis.edu/climate/worldclim/
Native resolution: ~4.5km (2.5 arc-minutes)
Temporal resolution: Monthly (all 12 months preserved per year)
Temporal coverage: 1950-2024 (IDS extraction: 1997-2024)
Geographic coverage: Global land areas

================================================================================
RUNNING THE WORKFLOW
================================================================================
Run scripts in order. Steps 1-3 are WorldClim-specific (in this directory).
Step 4 uses a shared script (in the top-level scripts/ directory) that works
identically for all climate datasets (TerraClimate, PRISM).

  Step 1: Rscript 04_worldclim/scripts/01_download_worldclim.R
          Downloads decade-based GeoTIFF archives from geodata.ucdavis.edu.
          (~600 MB total; one-time download, files kept locally).

  Step 2: Rscript 04_worldclim/scripts/02_build_pixel_maps.R
          Maps each IDS observation to the ~4.5km WorldClim pixels it overlaps.
          Uses a downloaded GeoTIFF as the reference raster.

  Step 3: Rscript 04_worldclim/scripts/03_extract_worldclim.R
          Extracts monthly pixel values from the local GeoTIFF files.
          Saves one parquet per year.

  Step 4: Rscript scripts/build_climate_summaries.R worldclim
          Computes area-weighted climate means per observation per month.
          Water year columns are added during this step.

================================================================================
USING THE OUTPUTS
================================================================================
The workflow produces two key output types:

  1. PIXEL MAPS  -- link observations to raster pixels
     Location: 04_worldclim/data/processed/pixel_maps/
     Files: damage_areas_pixel_map.parquet
            damage_points_pixel_map.parquet
            surveyed_areas_pixel_map.parquet
     Columns: OBSERVATION_ID, DAMAGE_AREA_ID, pixel_id, x, y, coverage_fraction

  2. PIXEL VALUES -- monthly climate data per unique pixel
     Location: 04_worldclim/data/processed/pixel_values/
     Files: worldclim_{year}.parquet (one per year, 1997-2024)
     Columns: pixel_id, x, y, year, month, tmin, tmax, prec

After summaries (step 4):
     Location: processed/climate/worldclim/damage_areas_summaries/
     Format: one parquet per variable (read with open_dataset())
     Columns: DAMAGE_AREA_ID, calendar_year, calendar_month, water_year,
              water_year_month, variable, weighted_mean, value_min, value_max,
              n_pixels, n_pixels_with_data, sum_coverage_fraction

================================================================================
VARIABLES EXTRACTED (3 total)
================================================================================
All values are delivered in physical units (no scale factors needed).

Temperature:    tmin (minimum), tmax (maximum) -- °C
Precipitation:  prec (total precipitation) -- mm

================================================================================
DATA ORGANIZATION
================================================================================
Raw data is organized by decade:

  1950-1959  1960-1969  1970-1979  1980-1989  1990-1999
  2000-2009  2010-2019  2020-2024

Each decade archive contains 120 monthly GeoTIFFs (10 years × 12 months × 1
file per variable), except 2020-2024 which has 60 files (5 years). Files are
organized into per-variable subdirectories after extraction.

================================================================================
NOTES
================================================================================
- WorldClim resolution (~4.5km) is similar to TerraClimate (~4km), so pixel
  counts per observation are comparable
- Fewer variables than TerraClimate (3 vs 14); no water balance or drought indices
- The one-time bulk download (~600 MB) makes the download a distinct script step,
  unlike PRISM where data is streamed and discarded per month

================================================================================
DOCUMENTATION
================================================================================
  WORKFLOW.md        Technical architecture, script details, design decisions
  docs/ARCHITECTURE.md (top-level)  Shared pixel decomposition pattern and schemas
