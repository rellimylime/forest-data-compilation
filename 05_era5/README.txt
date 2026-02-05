================================================================================
ERA5 Daily Reanalysis Pixel-Level Extraction for IDS Observations
================================================================================
Source: https://cds.climate.copernicus.eu/datasets/reanalysis-era5-single-levels
Description: ECMWF's fifth generation atmospheric reanalysis. Hourly data
aggregated to daily, covering the global atmosphere from 1940 to present.
Coarser resolution (~28km) but comprehensive variable set and daily temporal.

Citation: Hersbach, H., et al. (2020). The ERA5 global reanalysis. Quarterly
Journal of the Royal Meteorological Society, 146(730), 1999-2049.

Data source: Copernicus Climate Data Store (CDS) API
Native resolution: ~28km (0.25 degree)
Temporal resolution: Daily (~365 time steps per year)
Temporal coverage: 1940-present (IDS extraction: 1997-2024)
Geographic coverage: Global

================================================================================
DATA ARCHITECTURE (Two-Table Design)
================================================================================
Same architecture as TerraClimate - see 02_terraclimate/README.txt for details.

1. PIXEL MAPS (observation -> pixel mapping)
   Location: 05_era5/data/processed/pixel_maps/
   Note: Due to coarse resolution, many IDS observations map to same pixel

2. PIXEL VALUES (climate data per unique pixel per day)
   Location: 05_era5/data/processed/pixel_values/
   Files: era5_{year}.parquet (one per year, 1997-2024)
   Columns: pixel_id, x, y, year, month, day, [20 climate variables]

================================================================================
VARIABLES EXTRACTED (20 total)
================================================================================
All values converted to standard units during extraction.

Temperature (°C): t2m (2m air), d2m (2m dewpoint), skt (skin),
                  stl1 (soil 0-7cm), stl2 (soil 7-28cm)
Precipitation (mm): tp (total), sf (snowfall)
Snow: sd (snow depth, m water equivalent)
Pressure: sp (surface pressure, hPa)
Wind (m/s): u10, v10 (10m u/v components)
Radiation (MJ/m²): ssrd (solar down), ssr (net solar), str (net thermal)
Soil water (m³/m³): swvl1 (0-7cm), swvl2 (7-28cm)
Evaporation (mm): e (total), pev (potential)
Vegetation: lai_hv (LAI high veg), lai_lv (LAI low veg)

================================================================================
SCRIPTS
================================================================================
  01_download_era5.R      - Download NetCDFs via CDS API (requires registration)
  02_build_pixel_maps.R   - Build pixel maps from downloaded reference raster
  03_extract_era5.R       - Extract daily pixel values from local NetCDFs

================================================================================
PREREQUISITES
================================================================================
1. Register at https://cds.climate.copernicus.eu/
2. Accept the ERA5 data license
3. Create ~/.cdsapirc with credentials:
   url: https://cds.climate.copernicus.eu/api
   key: <your-uid>:<your-api-key>
4. Install Python cdsapi: pip install cdsapi

================================================================================
NOTES
================================================================================
- Each variable-year download is 1-2 GB; full download is ~500+ GB
- Downloads may take hours to days depending on CDS queue
- Daily resolution produces large output files (~500 MB per year)
- Coarse resolution means less within-polygon variation than other datasets

================================================================================
DOCUMENTATION
================================================================================
  - WORKFLOW.md: Script descriptions, data flow, processing decisions
