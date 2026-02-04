================================================================================
TerraClimate Extraction for IDS Observations
================================================================================
Source: https://www.climatologylab.org/terraclimate.html
Description: High-resolution (~4km) global climate and water balance dataset 
derived from WorldClim, CRU, and JRA-55 reanalysis. Monthly data from 1958 to 
present. Data extracted at IDS observation centroids via Google Earth Engine.

Citation: Abatzoglou, J.T., S.Z. Dobrowski, S.A. Parks, K.C. Hegewisch (2018). 
TerraClimate, a high-resolution global dataset of monthly climate and climatic 
water balance from 1958-2015. Scientific Data 5:170191.

GEE Asset: IDAHO_EPSCOR/TERRACLIMATE
Native resolution: ~4km (1/24th degree, approximately 4638m at equator)
Temporal resolution: Monthly (aggregated to annual means for this extraction)
Temporal coverage: 1958-2024 (data availability may lag by several months)
Extraction date: 2025-01-31

================================================================================
EXTRACTION METHODOLOGY
================================================================================
Data was NOT downloaded as raw raster tiles. Instead, point values were 
extracted directly from Google Earth Engine using the following approach:

1. For each IDS damage polygon, compute centroid using st_point_on_surface()
2. Query TerraClimate ImageCollection for the observation's SURVEY_YEAR
3. Calculate annual mean across all 12 months for each variable
4. Extract pixel values at centroid coordinates using sampleRegions()
5. Scale = 4000m (approximately native resolution)

This yields one row per IDS observation with climate conditions at that 
location during that year.

================================================================================
OUTPUT FILES
================================================================================
Raw extraction CSVs:
  Location: 02_terraclimate/data/raw/
  Format: CSV (one file per region-year batch)
  Naming convention: tc_r{REGION_ID}_{SURVEY_YEAR}.csv
  Example: tc_r10_2020.csv (Alaska, year 2020)
  Total files: 251 (one per region × year combination)
  Total observations extracted: 4,475,817 (matches IDS cleaned data)
  Extraction time: ~25 minutes

Merged output (analysis-ready):
  Location: 02_terraclimate/data/processed/ids_terraclimate_merged.gpkg
  Format: GeoPackage (IDS geometries + scaled climate variables)
  Size: 4.2 GB
  Scale factors: Applied (values in physical units)
  Merge date: 2025-02-03

================================================================================
VARIABLES EXTRACTED (14 total)
================================================================================
All values are ANNUAL MEANS of monthly data. Raw integer values from GEE 
require scaling (see scale factors below).

Temperature:
  - tmmx: Maximum temperature (scale: 0.1, units: °C)
  - tmmn: Minimum temperature (scale: 0.1, units: °C)

Precipitation & Water:
  - pr: Precipitation accumulation (scale: 1, units: mm)
  - aet: Actual evapotranspiration (scale: 0.1, units: mm)
  - pet: Reference evapotranspiration, Penman-Monteith (scale: 0.1, units: mm)
  - def: Climate water deficit (scale: 0.1, units: mm)
  - soil: Soil moisture (scale: 0.1, units: mm)
  - swe: Snow water equivalent (scale: 1, units: mm)
  - ro: Runoff (scale: 1, units: mm)

Atmospheric:
  - vap: Vapor pressure (scale: 0.001, units: kPa)
  - vpd: Vapor pressure deficit (scale: 0.01, units: kPa)
  - srad: Downward surface shortwave radiation (scale: 0.1, units: W/m²)
  - vs: Wind speed at 10m (scale: 0.01, units: m/s)

Drought Index:
  - pdsi: Palmer Drought Severity Index (scale: 0.01, units: unitless)

================================================================================
KEY FIELDS IN OUTPUT CSVs
================================================================================
- OBSERVATION_ID: Links to IDS cleaned data (join key)
- REGION_ID: USFS region (added during extraction)
- SURVEY_YEAR: Year of observation (added during extraction)
- [14 climate variables]: Raw values from GEE (apply scale factors above)

================================================================================
KNOWN ISSUES
================================================================================
1. RAW CSVs REQUIRE SCALING: Raw CSV values in data/raw/ are integers from GEE.
   Scale factors are applied in the merged GeoPackage (data/processed/).

2. ANNUAL MEANS: Values represent mean of 12 monthly values. For variables 
   like precipitation (pr) where annual TOTAL is more meaningful, multiply 
   the mean by 12 in downstream analysis.

3. INVALID CENTROIDS: 10 IDS observations had geometries that produced NaN 
   centroids (0.0002% of data). These were excluded from extraction.

4. EDGE PIXELS: Points near coastlines or at edges of TerraClimate coverage 
   may have missing values if the centroid falls in a NoData pixel.

5. COORDINATE PRECISION: st_point_on_surface() may produce slightly different 
   results for complex multipolygons on geodetic (lat/lon) coordinates. This 
   is negligible at 4km resolution.

================================================================================
DATA QUALITY NOTES
================================================================================
1. DUPLICATES: Extraction produced 3,499 duplicate OBSERVATION_IDs due to 
   sub-batch boundary issue. All duplicates identical; removed during merge.

2. NA IDS: 15 observations from Region 9, 2024 lost OBSERVATION_ID during 
   GEE extraction. Excluded from merge.

3. MISSING CLIMATE DATA: 1,235 observations (0.03%) have no climate values.
   Centroids fall in TerraClimate NoData pixels, concentrated in:
   - Alaska (694)
   - Pacific Northwest (152)
   - Other coastal areas (389)

4. JOIN METHOD: Merge uses OBSERVATION_ID only (not REGION_ID/SURVEY_YEAR) 
   to avoid type mismatch issues.

================================================================================
USING THE MERGED DATA
================================================================================
The merged GeoPackage contains IDS observations with scaled climate variables:

```r
library(sf)

# Load merged data (IDS + TerraClimate, scale factors already applied)
data <- st_read("02_terraclimate/data/processed/ids_terraclimate_merged.gpkg")

# Climate variables are now in physical units (°C, mm, kPa, etc.)
```

To work with raw extraction CSVs directly (e.g., for debugging):

```r
library(dplyr)
library(readr)
library(purrr)

tc_files <- list.files("02_terraclimate/data/raw", pattern = "\\.csv$", 
                       full.names = TRUE)
terraclimate_raw <- map_dfr(tc_files, read_csv)
# Note: Raw values require scale factor application
```

================================================================================
PROCESSING NOTES
================================================================================
- Extraction performed in batches by REGION_ID × SURVEY_YEAR
- Sub-batches of 5000 features used to stay within GEE limits
- Progress is resumable: existing CSV files are skipped on re-run
- CRS: IDS centroids transformed to EPSG:4326 (WGS84) before extraction