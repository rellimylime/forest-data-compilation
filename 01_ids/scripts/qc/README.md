# IDS QC Scripts

Two quality control scripts for the IDS pipeline. Neither is required for data reproduction — they generate diagnostic output to verify cleaning and explore raw data structure.

For the project-wide QC overview, see [`docs/TESTING.md`](../../../docs/TESTING.md).

---

## validate_ids.R

**Run after:** `03_clean_ids.R` (reads `01_ids/data/processed/ids_layers_cleaned.gpkg`)

**Output:** Console only — no files written.

**Usage:**
```bash
Rscript 01_ids/scripts/qc/validate_ids.R
```

### What it checks

Runs the same checks on all three layers (`damage_areas`, `damage_points`, `surveyed_areas`):

| Check | What it verifies |
|-------|-----------------|
| Layer presence | All 3 layers exist in the GeoPackage |
| Field structure | All expected fields are present; reports missing and unexpected extras |
| Field types | Prints R class of each field |
| Missing data | % NA per field, sorted descending |
| Cleaning actions | `OBSERVATION_COUNT` is uppercase; `PERCENT_AFFECTED_CODE` has no `-1` sentinel values; `SOURCE_FILE` distribution |
| Summary stats | Feature counts by `REGION_ID`, `SURVEY_YEAR`, `DAMAGE_TYPE_CODE`; `ACRES` distribution |
| Intensity fields | Population of `PERCENT_AFFECTED_CODE` (DMSM) and `LEGACY_TPA` by survey year — confirms pre/post-2015 era split |
| Geometry validity | Count of invalid and empty geometries |
| Merge readiness | 5-item checklist before proceeding to climate extraction: CRS = EPSG:4326, `SURVEY_YEAR` present, geometry present, n > 0, no empty geometries |

The final section prints either `✓ DATA READY FOR TERRACLIMATE MERGE` or lists which checks failed.

---

## explore_ids_coverage.R

**Run after:** `01_download_ids.R` (reads raw `.gdb` files in `01_ids/data/raw/`)

**Output:** 5 CSV files written to `01_ids/data/processed/ids_exploration_raw/` (gitignored).

**Usage:**
```bash
Rscript 01_ids/scripts/qc/explore_ids_coverage.R
```

### What it checks

| Step | Output file | Description |
|------|-------------|-------------|
| Column availability by era | `ids_columns_by_era.csv` | Whether each field has any non-NA values pre-2015 vs. post-2015 — identifies era-specific fields |
| Missingness by era | `ids_missing_by_era.csv` | Fraction NA per field per era |
| Value distributions (pre-2015) | `ids_value_summary_pre_2015.csv` | Min/max/median or top-10 values for fields that only appear pre-2015 |
| Value distributions (post-2015) | `ids_value_summary_post_2015.csv` | Same for fields that only appear post-2015 |
| Regional temporal coverage | `ids_region_coverage.csv` | Per region: year range, number of survey years, and any gaps in the sequence |

This script was used to identify the pre/post-2015 methodology break documented in `cleaning_log.md`.
