# Climate Extraction Architecture

This document describes the **pixel decomposition** pattern used across all climate datasets (TerraClimate, PRISM, WorldClim). This architecture is shared and reusable.

For dataset-specific details (GEE assets, variables, scale factors), see each dataset's WORKFLOW.md.

---

## Pixel Decomposition Pattern

Climate-IDS integration uses a **pixel decomposition** approach instead of clipping rasters per observation. Each IDS observation is mapped to the climate raster pixels it overlaps, and climate values are extracted once per unique pixel.

```
IDS Observations    Pixel Maps                     Pixel Values (long)
+--------------+   +--------------------------+   +--------------------------------------+
|OBSERVATION_ID|-->|OBSERVATION_ID            |   | pixel_id                             |
|DAMAGE_AREA_ID|   |DAMAGE_AREA_ID            |   | calendar_year, calendar_month        |
|geometry      |   |pixel_id -----------------+-->| water_year, water_year_month         |
+--------------+   |x, y, coverage_fraction   |   | variable, value                      |
                   +--------------------------+   +--------------------------------------+
```

**coverage_fraction** = area(observation ∩ pixel) / area(pixel). NOT normalized to sum to 1.
Used as weight when computing area-weighted means per observation.

### Why This Design

- **Preserves within-polygon variation** - Important for large damage areas that span multiple climate pixels
- **Enables seasonal analysis** - Monthly data retained, not collapsed to annual means
- **Efficient storage** - Unique pixels extracted once, not duplicated per observation
- **Handles pancake features** - Multiple observations sharing the same geometry map to the same pixel set
- **Reusable pattern** - Same workflow for all climate datasets

---

## Time Conventions

### Calendar Year vs Water Year

Both **calendar year/month** and **water year/month** are stored in long-format outputs.

- **Water year definition:** October-September cycle
  - If month >= 10: water_year = calendar_year + 1, water_year_month = month - 9
  - If month < 10: water_year = calendar_year, water_year_month = month + 3
- **IDS data:** Retains original `SURVEY_YEAR` (not forced into water year). Survey timing is administrative, not hydrological.
- **Shared helper:** `scripts/utils/time_utils.R` provides conversion functions

### Rationale

Different analyses require different time bases:
- Hydrological analyses (drought, runoff) → water year
- Phenological analyses (growing season) → calendar year
- Survey timing analyses → SURVEY_YEAR

By storing both, users can choose the appropriate time base for their research question.

---

## How Dataset Workflows Differ

The 4-step pattern is the same for all datasets, but **how the raw climate data is accessed** differs. This drives practical differences in the extraction scripts.

| | TerraClimate | PRISM | WorldClim |
|---|---|---|---|
| **Data source** | Google Earth Engine (cloud) | Web service, per-month | Bulk download, local TIFs |
| **Raw data on disk** | No | No | Yes (~600 MB) |
| **Separate download script** | No | No | Yes (`01_download_worldclim.R`) |
| **Geographic scope** | Global | CONUS only | Global |
| **Variables** | 14 | 7 | 3 (tmin, tmax, prec) |
| **Scale factors** | Yes (GEE integers) | No | No |

**TerraClimate - GEE cloud extraction:** All computation runs on Google Earth Engine servers. The extraction script sends pixel coordinates to GEE and receives values in batches. No raw raster files are stored locally. Requires GEE authentication.

**PRISM - Streaming download:** Each monthly zip is downloaded from `services.nacse.org`, values are extracted at pixel coordinates immediately, and the zip is deleted. Raw data never accumulates on disk. Download and extraction happen in the same script (`02_extract_prism.R`).

**WorldClim - Bulk download then extract:** All decade zips (~600 MB total) are downloaded and kept locally as individual monthly GeoTIFFs. A separate script (`01_download_worldclim.R`) handles this one-time download. The extraction script then reads from local files. This is why WorldClim has 3 scripts while PRISM has 2 - the download is a distinct, reusable step rather than inline per-month.

---

## Workflow Steps

All climate datasets follow this standard 3-step workflow:

### 1. Build Pixel Maps
**Script:** `<dataset>/scripts/01_build_pixel_maps.R`

**Input:**
- `01_ids/data/processed/ids_layers_cleaned.gpkg` (all 3 layers)
- Reference raster from climate dataset (defines pixel grid)

**Output:**
- `<dataset>/data/processed/pixel_maps/damage_areas_pixel_map.parquet`
- `<dataset>/data/processed/pixel_maps/damage_points_pixel_map.parquet`
- `<dataset>/data/processed/pixel_maps/surveyed_areas_pixel_map.parquet`

**Process:**
1. Download reference raster (cached after first run)
2. For polygons (damage_areas, surveyed_areas): `exactextractr::exact_extract()` finds all overlapping pixels and computes coverage_fraction
3. For points (damage_points or FIA site coordinates): `terra::cellFromXY()` finds containing pixel; coverage_fraction = 1.0
4. For damage_areas: build map on unique DAMAGE_AREA_ID geometries first (handles pancake features), then join back to OBSERVATION_IDs
5. Save as parquet

**Pixel map schema:**

| Column | Type | Description |
|--------|------|-------------|
| OBSERVATION_ID | integer | IDS observation identifier |
| DAMAGE_AREA_ID | integer | Geometry identifier (shared by pancake features) |
| pixel_id | integer | Unique pixel identifier (raster cell number) |
| x | numeric | Pixel center longitude |
| y | numeric | Pixel center latitude |
| coverage_fraction | numeric | area(observation ∩ pixel) / area(pixel) |

---

### 2. Extract Climate Values
**Script:** `<dataset>/scripts/02_extract_<dataset>.R`

**Input:**
- Pixel maps from step 1
- Climate data source (GEE ImageCollection, NetCDF files, or CDS API)

**Output:**
- `<dataset>/data/processed/pixel_values/<dataset>_{year}.parquet` (wide format)

**Process:**
1. Load pixel maps from all three IDS layers
2. Extract unique pixel coordinates (deduplicated across layers)
3. For each year in temporal range:
   - Extract climate values at pixel coordinates
   - Apply scale factors from config.yaml (if applicable)
   - Save as yearly parquet file (wide format: one row per pixel-month, one column per variable)

**Pixel values schema (wide, per-year files):**

| Column | Type | Description |
|--------|------|-------------|
| pixel_id | integer | Matches pixel_id in pixel maps |
| x | numeric | Pixel center longitude |
| y | numeric | Pixel center latitude |
| year | integer | Calendar year |
| month | integer | Calendar month (1-12) |
| day | integer | Day of month (if applicable) |
| var1, var2, ... | numeric | Climate variables (scale factors applied) |

---

### 3. Build Observation Summaries
**Script:** `scripts/build_climate_summaries.R <dataset>`

**Input:**
- `<dataset>/data/processed/pixel_maps/damage_areas_pixel_map.parquet`
- `<dataset>/data/processed/pixel_values/<dataset>_{year}.parquet` (wide-format yearly files)

**Output:** `processed/climate/<dataset>/damage_areas_summaries/` (per-variable parquet files)

The summaries script reads directly from the wide-format yearly source files - no intermediate reshape step is required. For each variable × year chunk, it reads the relevant column from the yearly file, joins to the pixel map, and computes area-weighted aggregations. Output is one parquet file per variable, readable as a unified dataset via `open_dataset()`.

**Weighted mean formula:**
```
weighted_mean = sum(value × coverage_fraction, na.rm = TRUE) /
                sum(coverage_fraction[!is.na(value)])
```

Only pixels with non-NA values contribute to the denominator.

**Summary schema:**

| Column | Type | Description |
|--------|------|-------------|
| OBSERVATION_ID | integer | IDS observation identifier |
| DAMAGE_AREA_ID | integer | Geometry identifier |
| calendar_year | integer | Calendar year |
| calendar_month | integer | Calendar month (1-12) |
| water_year | integer | Water year |
| water_year_month | integer | Month within water year |
| variable | character | Climate variable name |
| weighted_mean | numeric | Area-weighted mean across pixels |
| value_min | numeric | Minimum pixel value |
| value_max | numeric | Maximum pixel value |
| n_pixels | integer | Total number of pixels |
| n_pixels_with_data | integer | Number of non-NA pixels |
| sum_coverage_fraction | numeric | Sum of coverage_fractions |

---

## Shared Utility Scripts

### scripts/utils/climate_extract.R
Core extraction framework shared by all climate datasets.

| Function | Purpose |
|----------|---------|
| `build_pixel_map()` | Map features to overlapping raster pixels |
| `build_ids_pixel_maps()` | Build pixel maps for all IDS layers; handles pancake deduplication |
| `extract_climate_from_gee()` | Extract values from GEE ImageCollection in batches |
| `get_unique_pixels()` | Get unique pixel coordinates across all pixel maps |
| `get_reference_raster_from_gee()` | Download reference raster from GEE to define pixel grid |
| `join_to_observations()` | Join pixel values back to observations with weighted means |
| `load_pixel_map()` | Read pixel map from parquet |
| `load_pixel_values()` | Load all yearly parquet files for a dataset |

### scripts/utils/time_utils.R
Water year conversion helpers.

| Function | Purpose |
|----------|---------|
| `calendar_to_water_year()` | Convert calendar year/month to water year/month |
| `water_to_calendar_year()` | Reverse conversion |
| `add_water_year()` | Append water_year and water_year_month columns to data frame |
| `water_year_month_label()` | Get month abbreviation for water year month number |

### scripts/utils/gee_utils.R
Google Earth Engine utilities (for GEE-based datasets).

| Function | Purpose |
|----------|---------|
| `init_gee()` | Initialize GEE with project ID from local/user_config.yaml |
| `sf_to_ee()` | Convert sf object to ee.FeatureCollection |
| `ee_to_sf()` | Convert ee.FeatureCollection to sf object |
| `gee_batch_extract()` | Extract values from GEE ImageCollection in batches |

---

## Data Format Decisions

| Decision | Rationale |
|----------|-----------|
| **Pixel-level extraction** (not polygon means) | Preserves within-polygon climate variation |
| **Monthly values** (not annual means) | Enables seasonal analysis |
| **Two-table architecture** (pixel maps + pixel values) | Efficient storage; handles pancake features |
| **Parquet format** | Efficient columnar storage; fast filtering by year/month/variable |
| **Scale factors applied during extraction** | Values immediately usable in physical units |
| **exactextractr for polygon-pixel mapping** | Provides coverage_fraction for proper area weighting |
| **Both calendar and water year retained** | Different analyses need different time bases |
| **Long format for standardized outputs** | Dataset-agnostic; enables uniform joins across datasets |
| **Shared reshape/summary scripts** | Same pattern for all climate datasets reduces code duplication |

---

## Point-Based Extraction at Arbitrary Locations

The `build_pixel_map()` function in `scripts/utils/climate_extract.R` handles both polygon and point geometries. For **point observations** (e.g., FIA site coordinates from `05_fia/data/processed/site_climate/all_site_locations.csv`), it uses `terra::cellFromXY()` to find the containing pixel, and sets `coverage_fraction = 1.0`.

This means the full pixel decomposition pattern works for any set of lat/lon points — not just IDS polygons. The FIA site climate extraction (`05_fia/scripts/06_extract_site_climate.R`) uses exactly this pattern:

```
05_fia/data/processed/site_climate/all_site_locations.csv    TerraClimate GEE (1958-present)
  (site_id, lat, lon)                    |
          |                              |
          v build_pixel_map()            |
  site_pixel_map.parquet             |
  (site_id, pixel_id, x, y,             |
   coverage_fraction=1.0)               |
          |                             |
          v join on pixel_id            v
  site_climate.parquet     extract_climate_from_gee()
  (site_id, year, month,       annual parquets per variable
   water_year, variable, value)
```

For point-based analyses, `join_to_observations()` is bypassed — a simple `inner_join(pm_slim, by = "pixel_id")` suffices since there is no area weighting. The long-format output schema matches what IDS summaries use, enabling consistent downstream analysis.

**Key difference from IDS workflow:** IDS damage areas produce area-weighted means over potentially hundreds of pixels per polygon. FIA sites are points → each site gets exactly the value from its containing pixel (no averaging needed).

---

## Implementation Checklist

When adding a new climate dataset:

- [ ] Add dataset configuration to `config.yaml` (GEE asset/CDS parameters, variables, scale factors)
- [ ] Create dataset directory: `<NN>_<dataset>/`
- [ ] Copy template scripts from existing dataset
- [ ] Update `01_build_pixel_maps.R` with dataset-specific reference raster
- [ ] Update `02_extract_<dataset>.R` with data source (GEE or CDS)
- [ ] Create `WORKFLOW.md` with dataset-specific details (link to this doc for architecture)
- [ ] Create `cleaning_log.md` for dataset-specific issues and decisions
- [ ] Test workflow on small sample before full extraction
- [ ] Run shared `scripts/build_climate_summaries.R <dataset>`
- [ ] Verify outputs match expected schema

---

## File Organization

```
<NN>_<dataset>/
├── README.txt                    (Quick-start guide, usage examples)
├── WORKFLOW.md                   (Dataset-specific technical details → links to this doc)
├── cleaning_log.md               (Dataset-specific issues and decisions)
├── data_dictionary.csv           (Variable definitions)
├── docs/                         (Dataset-specific reference docs)
├── scripts/
│   ├── 00_explore_<dataset>.R    (Optional: exploratory analysis)
│   ├── 01_build_pixel_maps.R     (Required: polygon-pixel mapping)
│   └── 02_extract_<dataset>.R    (Required: climate value extraction)
└── data/
    ├── raw/                      (Reference rasters, downloaded files)
    └── processed/
        ├── pixel_maps/           (Parquet: observation → pixels)
        └── pixel_values/         (Parquet: yearly wide-format files)
```

**Shared outputs:**
```
processed/climate/<dataset>/
└── damage_areas_summaries/               (Per-variable parquet files, read with open_dataset())
    ├── tmmx.parquet
    ├── tmmn.parquet
    └── ...
```
