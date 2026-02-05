================================================================================
TerraClimate Extraction for IDS Observations
================================================================================
Source: https://www.climatologylab.org/terraclimate.html
Description: High-resolution (~4km) global climate and water balance dataset
derived from WorldClim, CRU, and JRA-55 reanalysis. Monthly data from 1958 to
present. Data extracted at IDS observation locations via Google Earth Engine.

Citation: Abatzoglou, J.T., S.Z. Dobrowski, S.A. Parks, K.C. Hegewisch (2018).
TerraClimate, a high-resolution global dataset of monthly climate and climatic
water balance from 1958-2015. Scientific Data 5:170191.

GEE Asset: IDAHO_EPSCOR/TERRACLIMATE
Native resolution: ~4km (1/24th degree, approximately 4638m at equator)
Temporal resolution: Monthly (aggregated to annual means for this extraction)
Temporal coverage: 1958-2024 (data availability may lag by several months)
Extraction date: 2025-01-31

================================================================================
VARIABLES EXTRACTED (14 total)
================================================================================
All values are annual means of monthly data. See data_dictionary.csv for full
details including scale factors, units, and usage notes.

Temperature: tmmx, tmmn
Precipitation & Water: pr, aet, pet, def, soil, swe, ro
Atmospheric: vap, vpd, srad, vs
Drought Index: pdsi

================================================================================
OUTPUT FILES
================================================================================
Raw extraction CSVs (per layer, per region-year batch):
  Location: 02_terraclimate/data/raw/
  Format: CSV with raw integer values from GEE (require scale factor application)
  Naming convention:
    - tc_damage_areas_r{REGION_ID}_{SURVEY_YEAR}.csv
    - tc_damage_points_r{REGION_ID}_{SURVEY_YEAR}.csv
    - tc_surveyed_areas_r{REGION_ID}_{SURVEY_YEAR}.csv

Merged output (analysis-ready, damage_areas layer only):
  Location: 02_terraclimate/data/processed/ids_terraclimate_merged.gpkg
  Format: GeoPackage (IDS geometries + scaled climate variables)
  Scale factors: Applied (values in physical units)

================================================================================
DOCUMENTATION
================================================================================
  - WORKFLOW.md: Script descriptions, inputs/outputs, processing decisions
  - cleaning_log.md: Data quality issues found during extraction and merging
  - data_dictionary.csv: Field definitions, scale factors, and usage notes
  - docs/terraclim_ref.pdf: TerraClimate reference publication
