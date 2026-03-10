# TerraClimate Extraction: Technical Reference

For a quick-start guide and directory overview, see **README.md**.

This document covers the technical architecture, per-script details, usage examples, and troubleshooting.

## Status
- [x] Explore TerraClimate data (00_explore_terraclimate.R)
- [x] Build pixel maps (01_build_pixel_maps.R)
- [x] Extract monthly pixel values (02_extract_terraclimate.R)
- [x] Build observation summaries (scripts/build_climate_summaries.R terraclimate)

---

## Architecture: Pixel Decomposition

Climate-IDS integration uses a **pixel decomposition** pattern shared identically
across TerraClimate, PRISM, and WorldClim. Instead of clipping rasters per
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

### scripts/build_climate_summaries.R (shared)
Computes observation-level area-weighted climate summaries.
Run as: `Rscript scripts/build_climate_summaries.R terraclimate`

**Input:**
- `data/processed/pixel_maps/damage_areas_pixel_map.parquet`
- `data/processed/pixel_values/terraclimate_{year}.parquet` (reads wide-format source files directly)

**Output:** `processed/climate/terraclimate/damage_areas_summaries/` (per-variable parquet files)

The script reads directly from wide-format yearly source files - no intermediate reshape step required. For each variable × year chunk, it reads the relevant column, joins to the pixel map, and computes area-weighted aggregations. Output is one parquet file per variable, readable as a unified dataset via `open_dataset()`.

**Weighted mean formula:**
```
weighted_mean = sum(value * coverage_fraction, na.rm = TRUE) /
                sum(coverage_fraction[!is.na(value)])
```

Only pixels with non-NA values contribute to the denominator.

**Summary columns:** weighted_mean, value_min, value_max, n_pixels, n_pixels_with_data, sum_coverage_fraction

Water year columns (water_year, water_year_month) are computed inside this script during chunk processing - no separate reshape step is needed.

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
+----------------------------------+           |
|  data/processed/pixel_maps/      |           |
|  damage_areas_pixel_map.parquet  |           |
|  damage_points_pixel_map.parquet |           |
|  surveyed_areas_pixel_map.parquet|           |
+----------------------------------+           |
          |                                    |
          +-----------------+------------------+
                            | build_climate_summaries.R
                            | (reads source files directly)
                            v
                  +-----------------------------------------+
                  |  processed/climate/terraclimate/         |
                  |  damage_areas_summaries/                 |
                  |    tmmx.parquet, tmmn.parquet, ...       |
                  |  (weighted_mean, value_min, value_max    |
                  |   per obs per var per time)              |
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

## Usage Examples

### Get Climate Data for a Specific Species

```r
library(sf)
library(dplyr)
library(arrow)

# 1. Load IDS data and species lookup
damage_areas <- st_read(
  "01_ids/data/processed/ids_layers_cleaned.gpkg",
  layer = "damage_areas"
)
species_lookup <- read.csv("01_ids/lookups/host_code_lookup.csv")
# Example codes: 122 = ponderosa pine, 202 = Douglas-fir,
#                746 = quaking aspen, 108 = lodgepole pine

# 2. Filter IDS observations
my_obs <- damage_areas %>%
  filter(HOST_CODE == 122)  # ponderosa pine

# 3. OPTION A: Use pre-built weighted summaries (one mean per observation per month)
summaries <- open_dataset("processed/climate/terraclimate/damage_areas_summaries")
my_climate <- summaries %>%
  filter(DAMAGE_AREA_ID %in% unique(my_obs$DAMAGE_AREA_ID)) %>%
  collect()

# 4. OPTION B: Keep individual pixel values (for within-polygon variation)
pixel_map <- read_parquet(
  "02_terraclimate/data/processed/pixel_maps/damage_areas_pixel_map.parquet"
)
pixel_values <- open_dataset("02_terraclimate/data/processed/pixel_values")
my_pixel_climate <- pixel_values %>%
  filter(pixel_id %in% (pixel_map %>% filter(OBSERVATION_ID %in% my_obs$OBSERVATION_ID) %>% pull(pixel_id))) %>%
  collect()

# 5. Join climate back to IDS attributes
result <- my_obs %>%
  st_drop_geometry() %>%
  inner_join(
    my_climate %>% filter(variable == "tmmx"),
    by = "DAMAGE_AREA_ID"
  )
```

> **Pancake features:** Multiple OBSERVATION_IDs can share the same DAMAGE_AREA_ID (same geometry, different damage agents). When computing total affected area, group by DAMAGE_AREA_ID first to avoid double-counting.

---

### Get Climate Data for a Survey Area

```r
# 1. Load surveyed areas
survey_areas <- st_read(
  "01_ids/data/processed/ids_layers_cleaned.gpkg",
  layer = "surveyed_areas"
)

# 2. Get the TerraClimate pixels for those survey polygons
pixel_map <- read_parquet(
  "02_terraclimate/data/processed/pixel_maps/surveyed_areas_pixel_map.parquet"
)
my_pixels <- pixel_map %>%
  filter(SURVEY_FEATURE_ID %in% survey_areas$SURVEY_FEATURE_ID)

# 3. Load pixel values and compute area-weighted mean per survey polygon
pixel_values <- open_dataset("02_terraclimate/data/processed/pixel_values")
survey_summaries <- pixel_values %>%
  filter(pixel_id %in% my_pixels$pixel_id) %>%
  collect() %>%
  inner_join(my_pixels, by = "pixel_id") %>%
  group_by(SURVEY_FEATURE_ID, calendar_year, calendar_month,
           water_year, water_year_month, variable) %>%
  summarize(
    weighted_mean = sum(value * coverage_fraction, na.rm = TRUE) /
                    sum(coverage_fraction[!is.na(value)]),
    n_pixels = n(),
    .groups = "drop"
  )
```

> **Note:** Surveyed areas use `SURVEY_FEATURE_ID` as their primary key. Pre-built summaries are generated for `damage_areas` only; for `surveyed_areas`, compute from pixel values directly as shown above.

---

### Time Window Filtering

IDS has `SURVEY_YEAR` (integer, no month). TerraClimate pixel values store monthly data with both `calendar_year`/`calendar_month` and `water_year`/`water_year_month` on every row. Choose your time window at join time:

```r
# Calendar year match: Jan–Dec of SURVEY_YEAR
my_climate %>% filter(calendar_year == 2020)

# Water year match: Oct 2019 – Sep 2020
my_climate %>% filter(water_year == 2020)

# Prior water year (lagged climate)
my_climate %>% filter(water_year == 2020 - 1)

# Growing season only (Apr–Sep = water year months 7–12)
my_climate %>% filter(water_year == 2020, water_year_month >= 7)
```

Water year runs Oct–Sep. If `month >= 10`: `water_year = cal_year + 1`, `water_year_month = month - 9`. Implemented in `scripts/utils/time_utils.R`.

> Because IDS surveys are typically flown in summer/fall, damage may reflect climate from the preceding winter/spring — better captured by water year. Without month-of-survey, this choice is inherently ambiguous; the pipeline stores both so you decide at analysis time.

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
