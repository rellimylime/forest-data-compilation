# TerraClimate Extraction: Technical Reference

For a quick-start guide, workflow steps, and usage examples (including how to
filter by species or survey area), see **README.txt**.

This document covers the technical architecture, detailed script internals,
configuration, and design decisions.

## Status
- [x] Explore TerraClimate data (00_explore_terraclimate.R)
- [ ] Build pixel maps (01_build_pixel_maps.R)
- [ ] Extract monthly pixel values (02_extract_terraclimate.R)
- [ ] Reshape to long format (scripts/03_reshape_pixel_values.R terraclimate)
- [ ] Build observation summaries (scripts/04_build_climate_summaries.R terraclimate)

---

## Architecture: Pixel Decomposition

Climate-IDS integration uses a **pixel decomposition** pattern shared identically
across TerraClimate, PRISM, WorldClim, and ERA5. Instead of clipping rasters per
observation, each observation is mapped to the raster pixels it overlaps, and
climate values are extracted once per unique pixel.

```
IDS Observations    Pixel Maps                     Pixel Values (long)
+--------------+   +--------------------------+   +--------------------------------------+
|OBSERVATION_ID|-->|OBSERVATION_ID            |   | pixel_id                             |
|DAMAGE_AREA_ID|   |DAMAGE_AREA_ID            |   | calendar_year, calendar_month        |
|geometry      |   |pixel_id -----------------+-->| water_year, water_year_month         |
+--------------+   |x, y, coverage_fraction   |   | variable, value                      |
                   +--------------------------+   +--------------------------------------+
```

**coverage_fraction** = area(observation intersect pixel) / area(pixel). NOT normalized.
Used as weight when computing area-weighted means per observation.

**Why this design:**
- Preserves within-polygon variation (important for large damage areas)
- Enables seasonal analysis (monthly data, not annual means)
- Efficient storage (unique pixels extracted once, not per observation)
- Handles "pancake features" (multiple observations sharing same geometry)
- Reusable pattern across all climate datasets

### Time Conventions

- **Calendar year/month** and **water year/month** are both stored in long-format outputs.
- Water year: Oct-Sep. If month >= 10: water_year = cal_year + 1, water_year_month = month - 9.
- IDS data keeps its original `SURVEY_YEAR`; NOT forced into water year.
- Shared helper: `scripts/utils/time_utils.R`.

---

## Script Details

### Shared Utility Scripts

#### scripts/utils/climate_extract.R
Core extraction framework shared by all climate datasets.

| Function | Purpose |
|----------|---------|
| `build_pixel_map()` | Map features to overlapping raster pixels (exactextractr for polygons, cellFromXY for points) |
| `build_ids_pixel_maps()` | Build pixel maps for all IDS layers; handles pancake deduplication |
| `extract_climate_from_gee()` | Extract values from GEE ImageCollection in batches |
| `get_unique_pixels()` | Get unique pixel coordinates across all pixel maps |
| `get_reference_raster_from_gee()` | Download a reference raster from GEE to define pixel grid |
| `join_to_observations()` | Join pixel values back to observations with weighted means |
| `load_pixel_map()` | Read pixel map from parquet |
| `load_pixel_values()` | Load all yearly parquet files for a dataset |

#### scripts/utils/time_utils.R
Water year conversion helper.

| Function | Purpose |
|----------|---------|
| `calendar_to_water_year()` | Convert calendar year/month to water year/month |
| `water_to_calendar_year()` | Reverse conversion |
| `add_water_year()` | Append water_year and water_year_month columns |
| `water_year_month_label()` | Get month abbreviation for water year month number |

---

### 00_explore_terraclimate.R
Exploratory analysis of TerraClimate data structure and values before
committing to the full extraction workflow. Console output only, no files written.

- Initializes GEE and prints TerraClimate config
- Loads 100 sample IDS features (2020, Region 5)
- Tests centroid extraction on 5 variables
- Compares raw vs scaled values with sanity checks
- Estimates full extraction time across all ~4.5M features

---

### 01_build_pixel_maps.R
Creates the mapping from IDS observations to TerraClimate raster pixels.

**Input:**
- `01_ids/data/processed/ids_cleaned.gpkg` (all layers)

**Output:**
- `data/processed/pixel_maps/damage_areas_pixel_map.parquet`
- `data/processed/pixel_maps/damage_points_pixel_map.parquet`
- `data/processed/pixel_maps/surveyed_areas_pixel_map.parquet`

**Process:**
1. Download TerraClimate reference raster from GEE (cached after first run)
2. For each IDS layer:
   - Polygons (damage_areas, surveyed_areas): `exactextractr::exact_extract()` finds
     all overlapping pixels and computes coverage_fraction
   - Points (damage_points): `terra::cellFromXY()` finds containing pixel;
     coverage_fraction set to 1.0
3. For damage_areas: build map on unique DAMAGE_AREA_ID geometries first
   (handles pancake features), then join back to OBSERVATION_IDs
4. Save as parquet

**Pixel map schema (damage_areas):**

| Column | Type | Description |
|--------|------|-------------|
| OBSERVATION_ID | integer | IDS observation identifier |
| DAMAGE_AREA_ID | integer | Geometry identifier (shared by pancake features) |
| pixel_id | integer | Unique pixel identifier (raster cell number) |
| x | numeric | Pixel center longitude |
| y | numeric | Pixel center latitude |
| coverage_fraction | numeric | area(observation intersect pixel) / area(pixel) |

---

### 02_extract_terraclimate.R
Extracts monthly climate values for all unique pixels via GEE.

**Input:**
- Pixel maps from step 1
- GEE ImageCollection: IDAHO_EPSCOR/TERRACLIMATE

**Output:**
- `data/processed/pixel_values/terraclimate_{year}.parquet` (1997-2024)

**Process:**
1. Load pixel maps from all three IDS layers
2. Extract unique pixel coordinates (deduplicated across layers)
3. For each year (1997-2024):
   - Stack all 12 monthly images into a single 168-band image
     (14 variables x 12 months, bands named `{variable}_{month:02d}`)
   - Extract stacked image at pixel coordinates in batches of 2,500
   - Unstack result back to per-month rows
   - Apply scale factors from config.yaml
4. Save as yearly parquet files (wide format)

**Performance:** Stacking all months into one image reduces GEE round-trips
by ~12x (one `sampleRegions()` call per batch instead of 12). The
FeatureCollection is built from a GeoJSON dict in a single Python call
rather than per-point `ee.Feature()` construction. If GEE timeouts occur,
reduce `batch_size` (default 2,500).

**Pixel values schema (wide, per-year files):**

| Column | Type | Description |
|--------|------|-------------|
| pixel_id | integer | Matches pixel_id in pixel maps |
| x | numeric | Pixel center longitude |
| y | numeric | Pixel center latitude |
| year | integer | Calendar year |
| month | integer | Calendar month (1-12) |
| tmmx, tmmn, pr, ... | numeric | 14 climate variables (scale factors applied) |

---

### scripts/03_reshape_pixel_values.R (shared)
Reshapes wide-format yearly parquet files into a single long-format parquet.
Run as: `Rscript scripts/03_reshape_pixel_values.R terraclimate`

**Input:** `data/processed/pixel_values/terraclimate_{year}.parquet`
**Output:** `processed/climate/terraclimate/pixel_values.parquet`

**Process:**
1. Load pixel maps to get set of valid pixel_ids
2. Load yearly wide files, filter to valid pixels, pivot to long
3. Append water_year and water_year_month via `time_utils.R`
4. Write single parquet

---

### scripts/04_build_climate_summaries.R (shared)
Computes observation-level area-weighted climate means.
Run as: `Rscript scripts/04_build_climate_summaries.R terraclimate`

**Input:**
- `data/processed/pixel_maps/damage_areas_pixel_map.parquet`
- `processed/climate/terraclimate/pixel_values.parquet`

**Output:** `processed/climate/terraclimate/damage_areas_summaries_long.parquet`

**Weighted mean formula:**
```
weighted_mean = sum(value * coverage_fraction, na.rm = TRUE) /
                sum(coverage_fraction[!is.na(value)])
```

Only pixels with non-NA values contribute to the denominator.

---

## Data Flow

```
+-----------------------------------------------------------------------------+
|                           Google Earth Engine                                |
|  IDAHO_EPSCOR/TERRACLIMATE                                                  |
|  Global ~4km monthly climate rasters (1958-present)                         |
+-----------------------------------------------------------------------------+
          |                                    |
          | reference raster                   | sampleRegions()
          | (01_build_pixel_maps.R)            | (02_extract_terraclimate.R)
          v                                    v
+--------------------------+        +------------------------------------------+
|  data/raw/               |        |  data/processed/pixel_values/            |
|  terraclimate_ref.tif    |        |  terraclimate_1997.parquet (wide)        |
|  (pixel grid reference)  |        |  ...                                     |
+--------------------------+        |  terraclimate_2024.parquet (wide)        |
          |                         +------------------------------------------+
          v                                    |
+----------------------------------+           | 03_reshape_pixel_values.R
|  data/processed/pixel_maps/      |           v
|  damage_areas_pixel_map.parquet  |  +----------------------------------------+
|  damage_points_pixel_map.parquet |  |  processed/climate/terraclimate/       |
|  surveyed_areas_pixel_map.parquet|  |  pixel_values.parquet (long format)    |
+----------------------------------+  |  calendar + water year, variable, value|
          |                           +----------------------------------------+
          |                                    |
          +-----------------+------------------+
                            | 04_build_climate_summaries.R
                            v
                  +-----------------------------------------+
                  |  processed/climate/terraclimate/         |
                  |  damage_areas_summaries_long.parquet     |
                  |  (weighted_mean per obs per var per time)|
                  +-----------------------------------------+
```

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
    tmmn:
      scale: 0.1
      units: "deg C"
    pr:
      scale: 1
      units: "mm"
    # ... (14 total variables; see config.yaml for complete list)
```

### local/user_config.yaml (gitignored)
```yaml
gee_project: "your-gee-project-id"
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

## Troubleshooting

### GEE timeout errors
**Cause:** Too many pixels in single request.
**Solution:** Reduce `batch_size` parameter (default 2500 for monthly stacking).
With 168 bands per image, batch sizes above 3000 may exceed GEE limits.

### Missing pixel values
**Cause:** Pixel in NoData area (ocean, data edge).
**Solution:** Check coastal/edge observations; accept as missing.
See cleaning_log.md Issue #010 for affected regions.

### Python/reticulate issues
**Solution:** Set correct Python path in `.Renviron`:
```
RETICULATE_PYTHON=/path/to/python
```

### Large parquet files
**Note:** Monthly pixel-level data generates significant volume.
Typical size: ~50-100 MB per year depending on unique pixel count.
