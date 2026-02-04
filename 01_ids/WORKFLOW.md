# IDS Data Workflow

## Status
- [x] Download raw data (01_download_ids.R)
- [x] Inspect data structure (02_inspect_ids.R)
- [x] Clean data (03_clean_ids.R)
- [x] Verify output (04_verify_ids.R)

## Scripts

### 01_download_ids.R
Downloads all regional geodatabases from USFS.
- Input: URLs from config.yaml
- Output: 10 raw .gdb files in data/raw/ (~1.6 GB total)

### 02_inspect_ids.R
Explores data structure, checks field consistency, generates lookup tables.
- Input: Raw .gdb files
- Output: 
  - data_dictionary.csv
  - lookups/host_code_lookup.csv (76 species)
  - lookups/dca_code_lookup.csv (130 damage agents)
  - lookups/damage_type_lookup.csv (9 types)
  - lookups/percent_affected_lookup.csv (5 levels)
  - lookups/legacy_severity_lookup.csv (4 levels)
  - lookups/region_lookup.csv (10 regions)
- Key findings:
  - All 10 regions have identical field structure (44 fields)
  - 4.5M damage area polygons, 1997-2024
  - Three different CRS across regions (CONUS/Alaska/Hawaii Albers)
  - Legacy vs DMSM methodology break ~2015

### 03_clean_ids.R
Selects fields, transforms CRS, merges all regions for every IDS layer.
- Input: 10 raw .gdb files
- Output: data/processed/ids_layers_cleaned.gpkg (layers: damage_areas, damage_points, surveyed_areas)
- Actions:
  - Keep relevant fields per layer (codes only - lookups for names)
  - Transform all regions to EPSG:4326 (WGS84)
  - Standardize OBSERVATION_COUNT to uppercase (damage layers)
  - Recode PERCENT_AFFECTED_CODE -1 → NA (damage layers)
  - Add SOURCE_FILE column for traceability
  - Merge all 10 regions per layer into a single geopackage

### 04_verify_ids.R
Validates cleaned output before proceeding to climate merge.
- Input: ids_layers_cleaned.gpkg
- Checks (per layer):
  - Field structure matches expected fields per layer
  - CRS is EPSG:4326
  - Cleaning actions applied (uppercase, no -1 values)
  - No empty geometries
  - Summary stats by region/year (damage type for damage layers)
  - Legacy vs DMSM field population by year (damage layers)

## Decisions Log

| Decision | Rationale | Date |
|----------|-----------|------|
| Use DAMAGE_AREAS_FLAT only | Points and surveyed areas not needed for initial climate merge | 2025-01-30 |
| Keep codes only, create lookup tables | Reduces file size, codes are complete (text often missing) | 2025-01-30 |
| Keep both Legacy and DMSM intensity fields | Not directly comparable; let analysis scripts decide which to use | 2025-01-30 |
| Transform to EPSG:4326 | Matches TerraClimate CRS for downstream merge | 2025-01-30 |
| Drop US_AREA column | Redundant with REGION_ID; use region_lookup.csv instead | 2025-01-30 |
| Keep pancake features (14.7%) | Preserves multi-agent damage info; document ACRES summing caveat | 2025-01-30 |
| Recode PERCENT_AFFECTED_CODE -1 → NA | 2015 transition year placeholder; LEGACY_* fields available | 2025-01-30 |
| Add SOURCE_FILE column | Distinguishes CA vs HI (both REGION_ID=5) | 2025-01-30 |
