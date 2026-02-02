# TerraClimate Extraction Workflow

## Status
- [x] Configure GEE access (gee_utils.R)
- [x] Extract climate data at IDS centroids (01_extract_terraclimate.R)
- [ ] Process and scale extracted data (02_process_terraclimate.R)
- [ ] Merge with IDS data (03_merge_ids_terraclimate.R)

## Overview

TerraClimate is a global gridded climate dataset at ~4km resolution. Rather than downloading raw raster tiles, we extract point values directly at IDS observation locations via Google Earth Engine. This approach is:
- **Efficient:** Extract only what we need (~500 MB vs ~500 GB of raw tiles)
- **Fast:** ~25 minutes for 4.5M observations
- **Joinable:** Output includes OBSERVATION_ID for direct merge with IDS data

## Scripts

### scripts/utils/gee_utils.R
Utility functions for Google Earth Engine operations via reticulate.

**Key functions:**
- `init_gee()` - Initialize GEE with project credentials
- `sf_points_to_ee()` - Convert sf points to ee.FeatureCollection
- `get_terraclimate_annual()` - Get annual mean image for given year
- `extract_at_points()` - Sample image values at point locations
- `apply_terraclimate_scales()` - Apply scale factors to raw values

**Dependencies:** reticulate, yaml, here, sf, dplyr  
**Configuration:** Requires `local/user_config.yaml` with GEE project ID

---

### 01_extract_terraclimate.R
Extracts TerraClimate annual means at IDS polygon centroids.

**Input:**
- `01_ids/data/processed/ids_damage_areas_cleaned.gpkg`
- `config.yaml` (TerraClimate variable definitions)

**Output:**
- `02_terraclimate/data/raw/tc_r{REGION}_{YEAR}.csv` (251 files)

**Process:**
1. Load unique REGION_ID × SURVEY_YEAR combinations from IDS data
2. Check for existing output files (resumable)
3. For each batch:
   - Load IDS geometries for that region-year
   - Compute centroids using `st_point_on_surface()`
   - Filter invalid coordinates (NaN)
   - Query TerraClimate annual mean image
   - Extract in sub-batches of 5000 (GEE limit)
   - Save to CSV

**Runtime:** ~25 minutes for full dataset  
**Features extracted:** 4,475,817 (10 excluded for invalid coordinates)

---

### 02_process_terraclimate.R *(TODO)*
Processes raw extraction CSVs into analysis-ready format.

**Planned actions:**
1. Combine all 251 CSVs into single file
2. Apply scale factors (e.g., tmmx × 0.1 → °C)
3. Calculate annual totals for flux variables (pr, aet, pet, def, ro)
4. Add derived variables (e.g., mean annual temperature)
5. Check for missing values (coastal NoData)
6. Export to `02_terraclimate/data/processed/terraclimate_scaled.csv`

---

### 03_merge_ids_terraclimate.R *(TODO)*
Joins processed climate data to IDS observations.

**Planned actions:**
1. Load IDS cleaned data
2. Load processed TerraClimate data
3. Left join on OBSERVATION_ID
4. Verify join completeness (expect ~10 unmatched due to invalid centroids)
5. Export to `merged_data/ids_terraclimate_merged.csv`

---

## Configuration

### config.yaml (TerraClimate section)
```yaml
raw:
  terraclimate:
    gee_asset: "IDAHO_EPSCOR/TERRACLIMATE"
    spatial_resolution: "~4km (1/24th degree)"
    temporal_resolution: "monthly"
    variables:
      tmmx:
        description: "Maximum temperature"
        units: "°C"
        scale: 0.1
      pr:
        description: "Precipitation accumulation"
        units: "mm"
        scale: 1
      # ... (14 variables total)
```

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
| Batch by REGION_ID × SURVEY_YEAR | Natural grouping; enables resumable extraction | 2025-01-31 |
| Sub-batch at 5000 features | GEE API limit for sampleRegions() | 2025-01-31 |
| Exclude observations with NaN centroids | Only 10 affected (0.0002%); not worth complex geometry fixes | 2025-01-31 |

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

---

## Data Flow Diagram

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                           Google Earth Engine                                │
│                                                                             │
│  ┌─────────────────────────────────────────────────────────────────────┐   │
│  │  IDAHO_EPSCOR/TERRACLIMATE                                          │   │
│  │  Global ~4km monthly climate rasters (1958-present)                 │   │
│  └─────────────────────────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────────────────────┘
                                      │
                                      │ sampleRegions()
                                      │ at IDS centroids
                                      ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│  02_terraclimate/data/raw/                                                  │
│  tc_r1_1997.csv, tc_r1_1998.csv, ... tc_r10_2024.csv (251 files)           │
│  Raw integer values, one row per IDS observation                            │
└─────────────────────────────────────────────────────────────────────────────┘
                                      │
                                      │ 02_process_terraclimate.R
                                      │ Apply scales, combine files
                                      ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│  02_terraclimate/data/processed/                                            │
│  terraclimate_scaled.csv                                                    │
│  Physical units, derived variables, single file                             │
└─────────────────────────────────────────────────────────────────────────────┘
                                      │
                                      │ 03_merge_ids_terraclimate.R
                                      │ Join on OBSERVATION_ID
                                      ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│  merged_data/                                                               │
│  ids_terraclimate_merged.csv                                                │
│  IDS observations + climate variables, analysis-ready                       │
└─────────────────────────────────────────────────────────────────────────────┘
```