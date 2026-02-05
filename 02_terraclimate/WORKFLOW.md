# TerraClimate Pixel-Level Extraction Workflow

## Status
- [x] Explore TerraClimate data (00_explore_terraclimate.R)
- [ ] Build pixel maps (01_build_pixel_maps.R)
- [ ] Extract monthly pixel values (02_extract_terraclimate.R)

## Overview

TerraClimate is extracted via Google Earth Engine at ~4km resolution. This workflow preserves ALL individual pixel values within each IDS polygon (not polygon means) and ALL monthly values for each year. This enables analysis of within-polygon climate variation and seasonal patterns.

### Architecture: Two-Table Design

```
IDS Observations                 Pixel Maps                    Pixel Values
┌──────────────────┐        ┌──────────────────────┐       ┌─────────────────────┐
│ OBSERVATION_ID   │───────▶│ OBSERVATION_ID       │       │ pixel_id            │
│ geometry         │        │ pixel_id     ────────┼──────▶│ year, month         │
│ attributes...    │        │ x, y                 │       │ tmmx, tmmn, pr...   │
└──────────────────┘        │ coverage_fraction    │       │ (14 climate vars)   │
                            └──────────────────────┘       └─────────────────────┘
```

**Benefits:**
- Preserves within-polygon variation (important for large damage areas)
- Enables seasonal analysis (monthly data, not annual means)
- Efficient storage (unique pixels extracted once, not per observation)
- Handles "pancake features" (multiple observations sharing same geometry)

## Scripts

### scripts/utils/climate_extract.R
Core extraction framework shared by all climate datasets.

**Key functions:**
- `build_pixel_map()` — Map features to overlapping raster pixels
- `build_ids_pixel_maps()` — Build pixel maps for all IDS layers
- `extract_climate_from_gee()` — Extract values from GEE ImageCollection
- `get_unique_pixels()` — Get unique pixel coordinates from pixel map
- `join_to_observations()` — Join pixel values back to observations

---

### 00_explore_terraclimate.R
Exploratory analysis of TerraClimate data structure and values.
- **Purpose:** Understand data characteristics before extraction
- **Output:** Console output only

---

### 01_build_pixel_maps.R
Creates mapping from IDS observations to TerraClimate raster pixels.

- **Input:**
  - `01_ids/data/processed/ids_cleaned.gpkg` (all layers)
- **Output:**
  - `data/processed/pixel_maps/damage_areas_pixel_map.parquet`
  - `data/processed/pixel_maps/damage_points_pixel_map.parquet`
  - `data/processed/pixel_maps/surveyed_areas_pixel_map.parquet`
- **Process:**
  1. Download TerraClimate reference raster from GEE (defines pixel grid)
  2. For each IDS layer:
     - For polygons: use exactextractr to find all overlapping pixels
     - For points: use terra::cellFromXY to find containing pixel
  3. For damage_areas: deduplicate by DAMAGE_AREA_ID (pancake features)
  4. Save pixel maps as parquet files

---

### 02_extract_terraclimate.R
Extracts monthly climate values for all unique pixels via GEE.

- **Input:**
  - Pixel maps from step 1
  - GEE ImageCollection: IDAHO_EPSCOR/TERRACLIMATE
- **Output:** `data/processed/pixel_values/terraclimate_{year}.parquet`
  - One file per year (1997-2024)
  - Columns: pixel_id, x, y, year, month, [14 climate variables]
- **Process:**
  1. Load pixel maps, extract unique pixel coordinates
  2. For each year:
     - For each month: filter ImageCollection, extract at pixel coordinates
     - Apply scale factors from config.yaml
     - Save as parquet

---

## Configuration

### config.yaml (TerraClimate section)
```yaml
terraclimate:
  gee_asset: "IDAHO_EPSCOR/TERRACLIMATE"
  gee_scale: 4000
  variables:
    tmmx:
      scale: 0.1
      units: "°C"
    # ... (14 total variables)
```

### local/user_config.yaml
```yaml
gee_project: "your-gee-project-id"
```

---

## Joining Pixel Values to Observations

To get climate data for specific observations:

```r
library(arrow)
library(dplyr)
source("scripts/utils/climate_extract.R")

# Load pixel map and values
pixel_map <- load_pixel_map("02_terraclimate/data/processed/pixel_maps/damage_areas_pixel_map.parquet")
pixel_values <- load_pixel_values("02_terraclimate/data/processed/pixel_values", "terraclimate")

# Option 1: Weighted mean per observation (coverage_fraction as weights)
obs_climate <- join_to_observations(pixel_values, pixel_map, "OBSERVATION_ID")

# Option 2: Keep all pixels per observation (for variation analysis)
obs_pixels <- pixel_map %>%
  inner_join(pixel_values, by = "pixel_id")
```

---

## Decisions Log

| Decision | Rationale | Date |
|----------|-----------|------|
| Pixel-level extraction (not polygon means) | Preserves within-polygon climate variation | 2026-02-05 |
| Monthly values (not annual means) | Enables seasonal analysis | 2026-02-05 |
| Two-table architecture | Efficient storage; handles pancake features | 2026-02-05 |
| GEE extraction (not NetCDF download) | No local storage needed; direct pixel sampling | 2026-02-05 |
| Parquet format | Efficient columnar storage; fast filtering by year/month | 2026-02-05 |
| Scale factors applied during extraction | Values immediately usable in physical units | 2026-02-05 |
| exactextractr for polygon-pixel mapping | Provides coverage_fraction for proper weighting | 2026-02-05 |

---

## Data Flow Diagram

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                           Google Earth Engine                              │
│                                                                           │
│  IDAHO_EPSCOR/TERRACLIMATE                                                │
│  Global ~4km monthly climate rasters (1958-present)                       │
└─────────────────────────────────────────────────────────────────────────────┘
          │                                    │
          │ reference raster                   │ sampleRegions()
          │ (01_build_pixel_maps.R)            │ (02_extract_terraclimate.R)
          ▼                                    ▼
┌──────────────────────────┐        ┌──────────────────────────────────────────┐
│  data/raw/               │        │  data/processed/pixel_values/            │
│  terraclimate_ref.tif    │        │  terraclimate_1997.parquet               │
│  (pixel grid reference)  │        │  terraclimate_1998.parquet               │
└──────────────────────────┘        │  ...                                     │
          │                         │  terraclimate_2024.parquet               │
          │                         │  (monthly pixel values, scaled)          │
          ▼                         └──────────────────────────────────────────┘
┌────────────────────────────────────────────┐
│  data/processed/pixel_maps/                │
│  damage_areas_pixel_map.parquet            │
│  damage_points_pixel_map.parquet           │
│  surveyed_areas_pixel_map.parquet          │
│  (observation -> pixel mapping)            │
└────────────────────────────────────────────┘
```

---

## Troubleshooting

### GEE timeout errors
**Cause:** Too many pixels in single request
**Solution:** Reduce `batch_size` parameter (default 5000)

### Missing pixel values
**Cause:** Pixel in NoData area (ocean, data edge)
**Solution:** Check coastal/edge observations; accept as missing

### Python/reticulate issues
**Solution:** Set correct Python path in `.Renviron`:
```
RETICULATE_PYTHON=/path/to/python
```

### Large parquet files
**Note:** Monthly pixel-level data generates significant volume.
Typical size: ~50-100 MB per year depending on unique pixel count.
