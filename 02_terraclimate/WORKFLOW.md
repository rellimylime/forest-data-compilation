# TerraClimate Extraction Workflow

## Status
- [x] Configure GEE access (gee_utils.R)
- [x] Extract climate data at IDS centroids (01_extract_terraclimate.R)
- [x] Process, scale, and merge with IDS data (02_merge_terraclimate.R)

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

### 02_merge_terraclimate.R
Combines all TerraClimate CSVs, applies scale factors, and joins with IDS data.

**Input:**
- `02_terraclimate/data/raw/tc_r*.csv` (251 files from extraction)
- `01_ids/data/processed/ids_damage_areas_cleaned.gpkg`
- `config.yaml` (scale factors)

**Output:**
- `02_terraclimate/data/processed/ids_terraclimate_merged.gpkg`

**Process:**
1. Load and combine all 251 TerraClimate CSVs
2. Apply scale factors from config (e.g., tmmx × 0.1 → °C)
3. Load IDS cleaned data
4. Left join on OBSERVATION_ID
5. Save merged GeoPackage

**Runtime:** ~5 minutes  
**Output size:** 4.2 GB

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
                                      │ 02_merge_terraclimate.R
                                      │ Apply scales, combine, join to IDS
                                      ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│  02_terraclimate/data/processed/                                            │
│  ids_terraclimate_merged.gpkg (4.2 GB)                                      │
│  IDS observations + scaled climate variables, analysis-ready                │
└─────────────────────────────────────────────────────────────────────────────┘
```