================================================================================
PRISM 800m Pixel-Level Extraction for IDS Observations
================================================================================
Source: https://prism.oregonstate.edu/
Description: High-resolution (800m) climate data for the contiguous United States.
Developed by Oregon State University's PRISM Climate Group. Monthly data from
1981 to present. Pixel-level values extracted via Google Earth Engine.

Citation: PRISM Climate Group, Oregon State University, https://prism.oregonstate.edu

GEE Asset: OREGONSTATE/PRISM/AN81m
Native resolution: 800m (~30 arc-seconds)
Temporal resolution: Monthly (all 12 months preserved per year)
Temporal coverage: 1981-present (IDS extraction: 1997-2024)
Geographic coverage: CONUS only (excludes Alaska and Hawaii)

================================================================================
DATA ARCHITECTURE (Two-Table Design)
================================================================================
Same architecture as TerraClimate - see 02_terraclimate/README.txt for details.

1. PIXEL MAPS (observation -> pixel mapping)
   Location: 03_prism/data/processed/pixel_maps/
   Files:
     - damage_areas_pixel_map.parquet
     - damage_points_pixel_map.parquet
     - surveyed_areas_pixel_map.parquet
   Note: Only CONUS observations included (Alaska/Hawaii excluded)

2. PIXEL VALUES (climate data per unique pixel per month)
   Location: 03_prism/data/processed/pixel_values/
   Files: prism_{year}.parquet (one per year, 1997-2024)
   Columns: pixel_id, x, y, year, month, ppt, tmean, tmin, tmax, tdmean, vpdmin, vpdmax

================================================================================
VARIABLES EXTRACTED (7 total)
================================================================================
All values are in physical units (no scale factors needed).

Temperature: tmean (mean), tmin (min), tmax (max), tdmean (dew point) - units: °C
Precipitation: ppt (total precipitation) - units: mm
Vapor Pressure: vpdmin (min VPD), vpdmax (max VPD) - units: hPa

================================================================================
SCRIPTS
================================================================================
  01_build_pixel_maps.R - Build pixel maps from GEE reference raster (CONUS only)
  02_extract_prism.R    - Extract monthly pixel values from GEE

================================================================================
NOTES
================================================================================
- PRISM has ~25x more pixels than TerraClimate due to higher resolution
- Extraction may take significantly longer than TerraClimate
- Alaska (R10) and Hawaii observations are not covered
- For Alaska/Hawaii, use TerraClimate or ERA5 instead

================================================================================
DOCUMENTATION
================================================================================
  - WORKFLOW.md: Script descriptions, data flow, processing decisions
