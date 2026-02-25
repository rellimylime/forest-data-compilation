================================================================================
ERA5 Monthly Reanalysis Pixel-Level Extraction for IDS Observations
================================================================================
Source: https://cds.climate.copernicus.eu/datasets/reanalysis-era5-single-levels-monthly-means
Description: ECMWF's fifth generation atmospheric reanalysis. Monthly averaged
means, covering the global atmosphere from 1940 to present. Coarser resolution
(~28km) but comprehensive variable set.

Citation: Hersbach, H., et al. (2020). The ERA5 global reanalysis. Quarterly
Journal of the Royal Meteorological Society, 146(730), 1999-2049.

Data source: Copernicus Climate Data Store (CDS) API
Native resolution: ~28km (0.25 degree)
Temporal resolution: Monthly (12 time steps per year)
Temporal coverage: 1940-present (IDS extraction: 1997-2024)
Geographic coverage: Global

================================================================================
DATA ARCHITECTURE (Two-Table Design)
================================================================================
Same architecture as TerraClimate - see 02_terraclimate/README.txt for details.

1. PIXEL MAPS (observation -> pixel mapping)
   Location: 05_era5/data/processed/pixel_maps/
   Note: Due to coarse resolution, many IDS observations map to same pixel

2. PIXEL VALUES (climate data per unique pixel per month)
   Location: 05_era5/data/processed/pixel_values/
   Files: era5_{year}.parquet (one per year, 1997-2024)
   Columns: pixel_id, x, y, year, month, [48 climate variables]

================================================================================
VARIABLES EXTRACTED (48 total)
================================================================================
All values converted to standard units during extraction.

Temperature (°C): t2m (2m air), d2m (2m dewpoint), skt (skin),
                  mn2t (daily min), mx2t (daily max),
                  stl1 (soil 0-7cm), stl2 (7-28cm),
                  stl3 (28-100cm), stl4 (100-289cm)
Precipitation (mm): tp (total), cp (convective), sf (snowfall)
                    Note: large-scale precip = tp - cp (derivable)
Snow: sd (snow depth, m water equivalent), snowc (snow cover fraction),
      smlt (snowmelt, mm), rsn (snow density, kg/m³)
      Note: physical depth (m) = sd * 1000 / rsn (derivable)
Pressure: sp (surface pressure, hPa), msl (mean sea level pressure, hPa)
Wind (m/s): u10, v10 (10m u/v components), i10fg (10m wind gust),
            u100, v100 (100m u/v components)
            Note: wind speed and direction are derivable from u/v components
Radiation (MJ/m²): ssrd (solar down total), fdir (solar down direct-beam),
                   ssr (net solar), strd (thermal down), str (net thermal)
                   fal (surface albedo, fraction)
                   Note: diffuse solar = ssrd - fdir (derivable)
Soil water (m³/m³): swvl1 (0-7cm), swvl2 (7-28cm),
                    swvl3 (28-100cm), swvl4 (100-289cm)
Hydrology (mm): e (total evaporation, negative↑), pev (potential evaporation, negative↑),
                ro (total runoff), sro (surface runoff)
                Note: subsurface runoff = ro - sro (derivable)
Energy (MJ/m²): sshf (sensible heat flux), slhf (latent heat flux)
                Note: both negative when heat flows surface→atmosphere
Cloud Cover: tcc (total, fraction), lcc (low <800hPa), mcc (mid 400-800hPa),
             hcc (high >400hPa)
Atmosphere: tcwv (column water vapour, kg/m²), cape (CAPE, J/kg),
            blh (boundary layer height, m)
Vegetation: lai_hv (LAI high veg), lai_lv (LAI low veg)

================================================================================
SCRIPTS
================================================================================
  01_download_era5.R      - Download NetCDFs via CDS API (requires registration)
  02_build_pixel_maps.R   - Build pixel maps from downloaded reference raster
  03_extract_era5.R       - Extract monthly pixel values from local NetCDFs

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
- Monthly means: ~10-20 MB per variable-year; 48 vars × 28 years = ~1,344 files (~16 GB raw)
- Downloads are fast (~seconds each); total queue time will depend on CDS load
- Monthly pixel values: ~5-50 MB per year depending on pixel count
- Coarse resolution means less within-polygon variation than other datasets
- CAUTION: for accumulated variables (tp, ssrd, fluxes), ERA5 monthly means
  report mean daily rate — multiply by days_in_month for monthly totals

================================================================================
DOCUMENTATION
================================================================================
  - WORKFLOW.md: Script descriptions, data flow, processing decisions
      § "Pre-Download Decisions and Gotchas" — key decisions to confirm before
        starting the download and sign-convention gotchas for analysis
