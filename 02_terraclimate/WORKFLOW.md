# TerraClimate Extraction Workflow

## Status
- [x] Explore extraction methods (00_explore_terraclimate.R)
- [x] Configure GEE access (scripts/utils/gee_utils.R)
- [x] Extract climate data at IDS locations (01_extract_terraclimate.R)
- [x] Process, scale, and merge with IDS data (02_merge_terraclimate.R)

## Overview

## Multi-source climate extraction (new)

The repository now includes a generic extraction script that preserves **all intersecting climate pixels** and **all available timesteps** (monthly or daily, depending on source), rather than polygon means.

- Script: `scripts/01_extract_climate_pixels.R`
- Utilities: `scripts/utils/climate_extraction_utils.R`
- Sources supported: TerraClimate, WorldClim, PRISM, ERA5 Daily
- Output: one row per IDS polygon x climate pixel x climate timestamp, written to `data/raw/climate_pixels/`

This new workflow can be run alongside the existing TerraClimate annual-mean pipeline documented below.

TerraClimate is a global gridded climate dataset at ~4km resolution. Rather than downloading raw raster tiles, we extract point values directly at IDS observation locations via Google Earth Engine. This is storage-efficient (~500 MB of CSVs vs ~500 GB of raw tiles) and directly joinable to IDS data via OBSERVATION_ID.

## Scripts

### scripts/utils/gee_utils.R
Utility functions for Google Earth Engine operations via reticulate.

**Key functions:**
- `init_gee()` — Initialize GEE with project credentials
- `sf_points_to_ee()` — Convert sf points to ee.FeatureCollection
- `get_terraclimate_annual()` — Get annual mean image for given year
- `extract_at_points()` — Sample image values at point locations
- `apply_terraclimate_scales()` — Apply scale factors to raw values

**Configuration:** Requires `local/user_config.yaml` with GEE project ID

---

### 00_explore_terraclimate.R
Tests extraction methods on a small sample before committing to full workflow.
- **Input:** `01_ids/data/processed/ids_layers_cleaned.gpkg` (100 features from R5, 2020)
- **Output:** Console output only (no files written)
- **Tests:** Polygon vs centroid extraction, scale factor application, timing estimates
- **Finding:** IDS polygons are much smaller than TerraClimate pixels (~16 km²), so centroid extraction is appropriate

---

### 01_extract_terraclimate.R
Extracts TerraClimate annual means for all three IDS layers.
- **Input:**
  - `01_ids/data/processed/ids_layers_cleaned.gpkg` (all layers)
  - `config.yaml` (TerraClimate variable definitions)
- **Output:** CSVs in `data/raw/` with layer-specific naming:
  - `tc_damage_areas_r{REGION}_{YEAR}.csv`
  - `tc_damage_points_r{REGION}_{YEAR}.csv`
  - `tc_surveyed_areas_r{REGION}_{YEAR}.csv`
- **Process:**
  1. Load unique REGION_ID x SURVEY_YEAR combinations per layer
  2. Check for existing output files (resumable)
  3. For each batch:
     - **damage_areas:** Extract polygon means on unique DAMAGE_AREA_ID geometries, then join back to all observations (handles pancake features)
     - **damage_points:** Extract at point locations
     - **surveyed_areas:** Extract polygon means using SURVEY_FEATURE_ID
  4. Sub-batches of 5000 features to stay within GEE limits
- **Runtime:** ~25 minutes for damage_areas layer (4,475,817 features; 10 excluded for invalid coordinates)

---

### 02_merge_terraclimate.R
Cleans and joins TerraClimate data to IDS damage_areas observations.
- **Input:**
  - `data/raw/tc_damage_areas_r*.csv` (region-year extraction CSVs)
  - `01_ids/data/processed/ids_layers_cleaned.gpkg` (damage_areas layer)
  - `config.yaml` (scale factors)
- **Output:** `data/processed/ids_terraclimate_merged.gpkg`
- **Process:**
  1. Load and combine all damage_areas TerraClimate CSVs
  2. Apply scale factors from config (e.g., tmmx x 0.1 → degrees C)
  3. Remove rows with NA OBSERVATION_ID (15 from Region 9, 2024 batch)
  4. Deduplicate on OBSERVATION_ID (3,499 duplicates from sub-batch boundary issue)
  5. Left join IDS data to TerraClimate on OBSERVATION_ID only (avoids type mismatch)
  6. Report missing climate data by region
  7. Save merged GeoPackage
- **Output size:** ~4.2 GB
- **Expected:** 4,475,827 rows; 1,235 with missing climate data (0.03%)

---

## Configuration

### config.yaml (TerraClimate section)
Variable definitions, scale factors, and GEE asset path. See `config.yaml` for full details.

### local/user_config.yaml
```yaml
gee_project: "your-gee-project-id"
```

---

## Decisions Log

| Decision | Rationale | Date |
|----------|-----------|------|
| Point extraction vs raster download | 500 MB output vs 500+ GB raw tiles; faster, more efficient | 2025-01-31 |
| Use st_point_on_surface() for centroids | Guarantees point inside polygon (unlike st_centroid for concave shapes) | 2025-01-31 |
| Extract annual means (not monthly) | Reduces data volume; annual climate sufficient for forest damage analysis | 2025-01-31 |
| Keep raw values in extraction CSVs | Simpler extraction; apply scales in processing step | 2025-01-31 |
| Scale = 4000m for extraction | Approximately native resolution; balances precision and performance | 2025-01-31 |
| Batch by REGION_ID x SURVEY_YEAR | Natural grouping; enables resumable extraction | 2025-01-31 |
| Sub-batch at 5000 features | GEE API limit for sampleRegions() | 2025-01-31 |
| Exclude observations with NaN centroids | Only 10 affected (0.0002%); not worth complex geometry fixes | 2025-01-31 |
| Merge damage_areas only (for now) | Primary analysis layer; points and surveyed_areas merge deferred | 2025-02-03 |

---

## Troubleshooting

### "Band pattern did not match any bands"
**Cause:** Variable name mismatch (e.g., using "ppt" instead of "pr")
**Solution:** Check `config.yaml` variable names match TerraClimate band names exactly

### NaN values in extraction
**Possible causes:**
1. Invalid centroid coordinates → Check geometry validity
2. Point in NoData pixel (ocean/edge) → Check if coastal location
3. Year not available in TerraClimate → Check temporal coverage (1958-2023+)

### GEE timeout errors
**Cause:** Too many features in single request
**Solution:** Reduce `batch_size` in extraction loop (default 5000)

### Python/reticulate issues
**Solution:** Ensure correct Python environment in `.Renviron`:
```
RETICULATE_PYTHON=/path/to/python
```

### Duplicate rows in TerraClimate CSVs
**Cause:** Sub-batch boundary issue — features at positions 5000, 10000, etc. may be extracted twice
**Solution:** Deduplicate with `distinct(OBSERVATION_ID, .keep_all = TRUE)` during merge. All duplicates have identical values.

### Many NAs after merge (type mismatch)
**Cause:** REGION_ID/SURVEY_YEAR stored as numeric in CSVs but integer in geopackage
**Solution:** Join on OBSERVATION_ID only; drop REGION_ID/SURVEY_YEAR from TerraClimate data before join

---

## Data Flow Diagram

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                           Google Earth Engine                              │
│                                                                           │
│  ┌─────────────────────────────────────────────────────────────────────┐  │
│  │  IDAHO_EPSCOR/TERRACLIMATE                                        │  │
│  │  Global ~4km monthly climate rasters (1958-present)               │  │
│  └─────────────────────────────────────────────────────────────────────┘  │
└─────────────────────────────────────────────────────────────────────────────┘
                                    │
                                    │ sampleRegions() at IDS locations
                                    │ (01_extract_terraclimate.R)
                                    ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│  02_terraclimate/data/raw/                                                │
│  tc_damage_areas_r1_1997.csv ... tc_damage_areas_r10_2024.csv            │
│  tc_damage_points_r1_1997.csv ... tc_surveyed_areas_r10_2024.csv         │
│  Raw integer values, one row per IDS observation                          │
└─────────────────────────────────────────────────────────────────────────────┘
                                    │
                                    │ 02_merge_terraclimate.R
                                    │ Apply scales, deduplicate, join to IDS
                                    ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│  02_terraclimate/data/processed/                                          │
│  ids_terraclimate_merged.gpkg (~4.2 GB)                                   │
│  IDS damage_areas observations + scaled climate variables                 │
└─────────────────────────────────────────────────────────────────────────────┘
```
