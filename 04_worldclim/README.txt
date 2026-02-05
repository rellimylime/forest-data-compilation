================================================================================
WorldClim Monthly Weather Pixel-Level Extraction for IDS Observations
================================================================================
Source: https://www.worldclim.org/data/monthlywth.html
Description: Global historical monthly weather data at ~4.5km resolution.
Interpolated from weather station observations. Monthly data from 1960-2021.

Citation: Fick, S.E. and R.J. Hijmans (2017). WorldClim 2: new 1km spatial
resolution climate surfaces for global land areas. International Journal of
Climatology 37(12): 4302-4315.

Data source: https://geodata.ucdavis.edu/climate/worldclim/
Native resolution: ~4.5km (2.5 arc-minutes)
Temporal resolution: Monthly (all 12 months preserved per year)
Temporal coverage: 1960-2021 (IDS extraction: 1997-2021)
Geographic coverage: Global land areas

================================================================================
DATA ARCHITECTURE (Two-Table Design)
================================================================================
Same architecture as TerraClimate - see 02_terraclimate/README.txt for details.

1. PIXEL MAPS (observation -> pixel mapping)
   Location: 04_worldclim/data/processed/pixel_maps/
   Files:
     - damage_areas_pixel_map.parquet
     - damage_points_pixel_map.parquet
     - surveyed_areas_pixel_map.parquet

2. PIXEL VALUES (climate data per unique pixel per month)
   Location: 04_worldclim/data/processed/pixel_values/
   Files: worldclim_{year}.parquet (one per year, 1997-2021)
   Columns: pixel_id, x, y, year, month, tmin, tmax, prec

================================================================================
VARIABLES EXTRACTED (3 total)
================================================================================
All values are in physical units (no scale factors needed).

Temperature: tmin (minimum), tmax (maximum) - units: °C
Precipitation: prec (total precipitation) - units: mm

================================================================================
SCRIPTS
================================================================================
  01_download_worldclim.R   - Download decade GeoTIFF archives from geodata.ucdavis.edu
  02_build_pixel_maps.R     - Build pixel maps from downloaded reference raster
  03_extract_worldclim.R    - Extract monthly pixel values from local GeoTIFFs

================================================================================
NOTES
================================================================================
- WorldClim data ends in 2021 (IDS years 2022-2024 have no WorldClim data)
- Data is organized by decade: 1960-1969, 1970-1979, ..., 2019-2021
- Resolution is similar to TerraClimate (~4.5km vs ~4km)
- Fewer variables than TerraClimate (3 vs 14)

================================================================================
DOCUMENTATION
================================================================================
  - WORKFLOW.md: Script descriptions, data flow, processing decisions
