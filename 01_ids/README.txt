================================================================================
USDA Forest Service Insect and Disease Detection Survey (IDS)
================================================================================
Source: https://www.fs.usda.gov/science-technology/data-tools-products/fhp-mapping-reporting/detection-surveys
Description: Annual aerial and ground survey data detecting forest insect and 
disease damage across US Forest Service regions. Polygons represent areas of 
observed tree mortality, defoliation, and other damage.
Format: Geodatabase (.gdb)
Spatial coverage: All USFS regions (R1-R6, R8-R10; R7 merged into R6 in 1965)
  - CONUS, Alaska, Hawaii
Temporal coverage: 1997-2024
Download date: 2025-01-29
Files downloaded:
  - CONUS_Region1_AllYears.gdb.zip (Northern Region - MT, ND, ID panhandle)
  - CONUS_Region2_AllYears.gdb.zip (Rocky Mountain Region - CO, WY, SD, NE, KS)
  - CONUS_Region3_AllYears.gdb.zip (Southwestern Region - AZ, NM)
  - CONUS_Region4_AllYears.gdb.zip (Intermountain Region - UT, NV, ID, WY)
  - CONUS_Region5_AllYears.gdb.zip (Pacific Southwest - CA)
  - HI_Region5_AllYears.gdb.zip (Pacific Southwest - HI)
  - CONUS_Region6_AllYears.gdb.zip (Pacific Northwest - OR, WA)
  - CONUS_Region8_AllYears.gdb.zip (Southern Region - 13 SE states)
  - CONUS_Region9_AllYears.gdb.zip (Eastern Region - 20 NE/MW states)
  - AK_Region10_AllYears.gdb.zip (Alaska)

Original CRS (varies by region):
  - CONUS (R1-R6, R8-R9): USA_Contiguous_Albers_Equal_Area_Conic_USGS_version
  - Alaska (R10): NAD83 / Alaska Albers
  - Hawaii (R5-HI): Hawaii Albers Equal Area Conic
Cleaned output CRS: EPSG:4326 (WGS84)

Feature counts (raw):
  - DAMAGE_AREAS_FLAT: 4,475,827 polygons
  - DAMAGE_POINTS_FLAT: 1,243,890 points
  - SURVEYED_AREAS_FLAT: 74,505 polygons

Key attributes (cleaned data - 16 fields):
  - OBSERVATION_ID: Unique identifier per damage observation
  - DAMAGE_AREA_ID: Geometry identifier (shared by pancake features)
  - SURVEY_YEAR: Year of observation (1997-2024)
  - REGION_ID: USFS region (1-6, 8-10)
  - HOST_CODE: Tree species affected (see lookups/host_code_lookup.csv)
  - DCA_CODE: Damage causing agent (see lookups/dca_code_lookup.csv)
  - DAMAGE_TYPE_CODE: Mortality, defoliation, etc. (see lookups/damage_type_lookup.csv)
  - ACRES: Area affected
  - AREA_TYPE: POLYGON or GRID_240/480/960/1920
  - OBSERVATION_COUNT: SINGLE or MULTIPLE (pancake features)
  - PERCENT_AFFECTED_CODE: Canopy damage 1-5 scale (DMSM method, 2015+)
  - PERCENT_MID: Midpoint percentage
  - LEGACY_TPA: Trees per acre (Legacy method, pre-2015)
  - LEGACY_NO_TREES: Total tree count
  - LEGACY_SEVERITY_CODE: Severity rating (Legacy method)
  - SOURCE_FILE: Original .gdb filename

Known issues:
  1. Legacy vs DMSM methodology break (~2015): Pre-2015 uses trees per acre 
     (LEGACY_TPA), post-2015 uses percent canopy affected (PERCENT_AFFECTED_CODE).
     These measures are NOT directly comparable.
  2. Pancake features: 14.7% of records share geometry with other observations
     (same DAMAGE_AREA_ID, different OBSERVATION_ID). ACRES should not be summed
     naively - group by DAMAGE_AREA_ID first.
  3. PERCENT_AFFECTED_CODE = -1 in 2015 (transition year): Recoded to NA in 
     cleaned data. Use LEGACY_* fields for these records.
  4. Survey effort varies by year: More records in recent years reflects 
     increased survey effort, not necessarily more damage.

Lookup tables (01_ids/lookups/):
  - host_code_lookup.csv (76 species)
  - dca_code_lookup.csv (130 damage agents)
  - damage_type_lookup.csv (9 damage types)
  - percent_affected_lookup.csv (5 intensity levels)
  - legacy_severity_lookup.csv (4 severity levels)
  - region_lookup.csv (10 regions with state coverage)

Cleaned output: 01_ids/data/processed/ids_damage_areas_cleaned.gpkg (3.77 GB)

Citation: USDA Forest Service. Forest Health Protection Insect and Disease 
Detection Survey Data. https://www.fs.usda.gov/foresthealth/