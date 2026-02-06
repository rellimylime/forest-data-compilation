# TerraClimate Pixel-Level Extraction Workflow

## Status
- [x] Explore TerraClimate data (00_explore_terraclimate.R)
- [ ] Build pixel maps (01_build_pixel_maps.R)
- [ ] Extract monthly pixel values (02_extract_terraclimate.R)
- [ ] Reshape to long format (scripts/03_reshape_pixel_values.R terraclimate)
- [ ] Build observation summaries (scripts/04_build_climate_summaries.R terraclimate)

## Overview

TerraClimate is extracted via Google Earth Engine at ~4km resolution. This workflow preserves ALL individual pixel values within each IDS polygon (not polygon means) and ALL monthly values for each year. This enables analysis of within-polygon climate variation and seasonal patterns.

### Architecture: Pixel Decomposition (No Per-Observation Rasters)

Climate–IDS integration uses a shared **pixel decomposition** pattern that avoids clipping
rasters per observation. The same pattern is reused identically for PRISM, WorldClim, and ERA5.

```
IDS Observations    Pixel Maps                     Pixel Values (long)
┌──────────────┐   ┌──────────────────────────┐   ┌──────────────────────────────────────┐
│OBSERVATION_ID│──▶│OBSERVATION_ID            │   │ pixel_id                             │
│DAMAGE_AREA_ID│   │DAMAGE_AREA_ID            │   │ calendar_year, calendar_month        │
│geometry      │   │pixel_id ─────────────────┼──▶│ water_year, water_year_month         │
└──────────────┘   │x, y, coverage_fraction   │   │ variable, value                      │
                   └──────────────────────────┘   └──────────────────────────────────────┘
```

**coverage_fraction** = area(observation intersect pixel) / area(pixel). NOT normalized.

**Benefits:**
- Preserves within-polygon variation (important for large damage areas)
- Enables seasonal analysis (monthly data, not annual means)
- Efficient storage (unique pixels extracted once, not per observation)
- Handles "pancake features" (multiple observations sharing same geometry)
- Reusable pattern across all climate datasets

### Time Conventions

- **Calendar year/month** and **water year/month** are both stored.
- Water year: Oct-Sep. If month >= 10: water_year = cal_year + 1, water_year_month = month - 9.
- IDS data keeps its original `SURVEY_YEAR`; NOT forced into water year.
- Shared helper: `scripts/utils/time_utils.R`.

## Scripts

### scripts/utils/climate_extract.R
Core extraction framework shared by all climate datasets.

**Key functions:**
- `build_pixel_map()` -- Map features to overlapping raster pixels
- `build_ids_pixel_maps()` -- Build pixel maps for all IDS layers
- `extract_climate_from_gee()` -- Extract values from GEE ImageCollection
- `get_unique_pixels()` -- Get unique pixel coordinates from pixel map
- `join_to_observations()` -- Join pixel values back with weighted means and diagnostics

### scripts/utils/time_utils.R
Shared water year conversion helper.

**Key functions:**
- `calendar_to_water_year()` -- Convert calendar year/month to water year/month
- `add_water_year()` -- Append water_year and water_year_month to a data frame

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
- **Schema (damage areas):** OBSERVATION_ID, DAMAGE_AREA_ID, pixel_id, x, y, coverage_fraction
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

### scripts/03_reshape_pixel_values.R (shared)
Reshapes wide-format yearly parquet files into a single long-format parquet
with standardized columns. Run as: `Rscript scripts/03_reshape_pixel_values.R terraclimate`

- **Input:** `data/processed/pixel_values/terraclimate_{year}.parquet` (wide format)
- **Output:** `processed/climate/terraclimate/pixel_values.parquet`
  - Columns: pixel_id, calendar_year, calendar_month, water_year, water_year_month, variable, value
- **Process:**
  1. Load pixel maps to get set of valid pixel_ids
  2. Load yearly wide files, filter to valid pixels, pivot to long
  3. Append water_year and water_year_month via `time_utils.R`
  4. Write single parquet

---

### scripts/04_build_climate_summaries.R (shared)
Computes observation-level area-weighted climate means. Run as:
`Rscript scripts/04_build_climate_summaries.R terraclimate`

- **Input:**
  - `data/processed/pixel_maps/damage_areas_pixel_map.parquet`
  - `processed/climate/terraclimate/pixel_values.parquet`
- **Output:** `processed/climate/terraclimate/damage_areas_summaries_long.parquet`
  - Columns: DAMAGE_AREA_ID, calendar_year, calendar_month, water_year, water_year_month,
    variable, weighted_mean, n_pixels, n_pixels_with_data, sum_coverage_fraction
- **Weighted mean formula:**
  `sum(value * coverage_fraction) / sum(coverage_fraction)`
  (only over pixels with non-NA values)

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
      units: "deg C"
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
source("scripts/utils/time_utils.R")

# Option 1: Use pre-built summaries (recommended)
summaries <- read_parquet("processed/climate/terraclimate/damage_areas_summaries_long.parquet")

# Option 2: Build from scratch with join_to_observations()
pixel_map <- load_pixel_map("02_terraclimate/data/processed/pixel_maps/damage_areas_pixel_map.parquet")
pixel_values <- load_pixel_values("02_terraclimate/data/processed/pixel_values", "terraclimate")
obs_climate <- join_to_observations(pixel_values, pixel_map, "OBSERVATION_ID",
                                    add_water_year = TRUE)

# Option 3: Keep all pixels per observation (for variation analysis)
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
| Both calendar and water year retained | Different analyses need different time bases | 2026-02-06 |
| IDS stays on SURVEY_YEAR (no water year) | Survey timing is administrative, not hydrological | 2026-02-06 |
| Long format for standardized outputs | Dataset-agnostic; enables uniform joins | 2026-02-06 |
| Shared reshape/summary scripts | Same pattern for TerraClimate, PRISM, WorldClim, ERA5 | 2026-02-06 |

---

## Data Flow Diagram

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                           Google Earth Engine                              │
│  IDAHO_EPSCOR/TERRACLIMATE                                                │
│  Global ~4km monthly climate rasters (1958-present)                       │
└─────────────────────────────────────────────────────────────────────────────┘
          │                                    │
          │ reference raster                   │ sampleRegions()
          │ (01_build_pixel_maps.R)            │ (02_extract_terraclimate.R)
          ▼                                    ▼
┌──────────────────────────┐        ┌──────────────────────────────────────────┐
│  data/raw/               │        │  data/processed/pixel_values/            │
│  terraclimate_ref.tif    │        │  terraclimate_1997.parquet (wide)        │
│  (pixel grid reference)  │        │  ...                                     │
└──────────────────────────┘        │  terraclimate_2024.parquet (wide)        │
          │                         └──────────────────────────────────────────┘
          ▼                                    │
┌──────────────────────────────────┐           │ 03_reshape_pixel_values.R
│  data/processed/pixel_maps/      │           ▼
│  damage_areas_pixel_map.parquet  │  ┌────────────────────────────────────────┐
│  damage_points_pixel_map.parquet │  │  processed/climate/terraclimate/       │
│  surveyed_areas_pixel_map.parquet│  │  pixel_values.parquet (long format)    │
└──────────────────────────────────┘  │  calendar + water year, variable, value│
          │                           └────────────────────────────────────────┘
          │                                    │
          └─────────────────┬──────────────────┘
                            │ 04_build_climate_summaries.R
                            ▼
                  ┌─────────────────────────────────────────┐
                  │  processed/climate/terraclimate/         │
                  │  damage_areas_summaries_long.parquet     │
                  │  (weighted_mean per obs per var per time)│
                  └─────────────────────────────────────────┘
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
