# IDS Data Workflow

## Status
- [x] Download raw data (01_download_ids.R)
- [x] Inspect data structure (02_inspect_ids.R)
- [x] Clean data (03_clean_ids.R)
- [x] Verify output (04_verify_ids.R)
- [x] Explore coverage (05_explore_ids_coverage.R)

## Scripts

### 01_download_ids.R
Downloads all regional geodatabases from USFS.
- **Input:** URLs from config.yaml
- **Output:** 10 raw .gdb files in `data/raw/` (~1.6 GB total)
- Skips files that already exist

### 02_inspect_ids.R
Explores raw data structure, checks field consistency across regions, and generates lookup tables.
- **Input:** Raw .gdb files in `data/raw/`
- **Output:**
  - `data_dictionary.csv` (field metadata from R5 sample)
  - `lookups/host_code_lookup.csv` (76 species)
  - `lookups/dca_code_lookup.csv` (130 damage agents)
  - `lookups/damage_type_lookup.csv` (9 types)
  - `lookups/percent_affected_lookup.csv` (5 levels)
  - `lookups/legacy_severity_lookup.csv` (4 levels)
  - `lookups/region_lookup.csv` (10 regions)
- **Key findings:** All 10 regions have identical field structure (44 fields); three different CRS across regions; legacy vs DMSM methodology break ~2015

### 03_clean_ids.R
Selects fields, transforms CRS, and merges all regions for every IDS layer.
- **Input:** 10 raw .gdb files in `data/raw/`
- **Output:** `data/processed/ids_layers_cleaned.gpkg` (layers: `damage_areas`, `damage_points`, `surveyed_areas`)
- **Actions:**
  - Select layer-specific fields (damage layers keep 15 fields; surveyed areas keep 4 fields). Codes only — use lookups for names.
  - Transform all regions to EPSG:4326 (WGS84)
  - Standardize OBSERVATION_COUNT to uppercase (damage layers)
  - Recode PERCENT_AFFECTED_CODE -1 → NA (damage layers)
  - Add SOURCE_FILE column for traceability
  - Generate SURVEY_FEATURE_ID for surveyed_areas
  - Merge all 10 regions per layer into a single geopackage

### 04_verify_ids.R
Validates cleaned output before proceeding to TerraClimate extraction.
- **Input:** `data/processed/ids_layers_cleaned.gpkg`
- **Checks (per layer):**
  - Field structure matches expected fields
  - CRS is EPSG:4326
  - Cleaning actions applied (uppercase OBSERVATION_COUNT, no -1 values)
  - No empty geometries
  - Summary stats by region/year
  - Legacy vs DMSM field population by year (damage layers)

### 05_explore_ids_coverage.R
Explores raw data for era-specific patterns, missingness, and regional temporal coverage.
- **Input:** Raw .gdb files in `data/raw/`
- **Output:** CSVs in `data/processed/ids_exploration_raw/`
  - `ids_columns_by_era.csv` — which columns have data pre/post 2015
  - `ids_missing_by_era.csv` — fraction NA per column per era
  - `ids_value_summary_pre_2015.csv` / `ids_value_summary_post_2015.csv` — distributions for era-specific columns
  - `ids_region_coverage.csv` — year range and gaps per region

## Decisions Log

| Decision | Rationale | Date |
|----------|-----------|------|
| Extract and clean all three IDS layers | Full IDS content available for downstream use (damage areas, damage points, surveyed areas) | 2025-01-30 |
| Keep codes only, create lookup tables | Reduces file size; codes are complete (text often missing) | 2025-01-30 |
| Keep both Legacy and DMSM intensity fields | Not directly comparable; let analysis scripts decide which to use | 2025-01-30 |
| Transform to EPSG:4326 | Matches TerraClimate CRS for downstream merge | 2025-01-30 |
| Drop US_AREA column | Redundant with REGION_ID; use region_lookup.csv instead | 2025-01-30 |
| Keep pancake features (14.7%) | Preserves multi-agent damage info; document ACRES summing caveat | 2025-01-30 |
| Recode PERCENT_AFFECTED_CODE -1 → NA | 2015 transition year placeholder; LEGACY_* fields available | 2025-01-30 |
| Add SOURCE_FILE column | Distinguishes CA vs HI (both REGION_ID=5) | 2025-01-30 |
