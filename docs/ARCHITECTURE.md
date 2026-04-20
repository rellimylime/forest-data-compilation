# Climate Extraction Architecture

**Navigation:** [Repo Home](../README.md) | [Docs Hub](README.md) | [Setup](../scripts/SETUP.md) | [Reproduce](REPRODUCE.md) | [Pipeline Map](PIPELINE_MAP.md) | [Data Products](DATA_PRODUCTS.md)

This document explains the shared climate extraction pattern used by TerraClimate, PRISM, and WorldClim. Dataset-specific details stay in each dataset's `WORKFLOW.md`; this page covers the common mental model.

## Core Pattern

The climate workflows use a pixel decomposition approach:

1. Map each IDS observation to the raster pixels it overlaps.
2. Extract climate values once per unique pixel.
3. Join pixel values back to observations and compute area-weighted summaries.

This is more efficient than clipping rasters separately for every observation and keeps within-polygon climate variation available until the final summary step.

## Data Model

```text
IDS observations -> pixel maps -> pixel values -> observation summaries
```

### 1. IDS observations

Source features come from the cleaned IDS layers in `01_ids/data/processed/ids_layers_cleaned.gpkg`.

### 2. Pixel maps

Pixel maps record which raster cells overlap which observations.

Typical columns:

| Column | Meaning |
|---|---|
| `OBSERVATION_ID` | IDS observation identifier when relevant |
| `DAMAGE_AREA_ID` | Shared geometry identifier for damage-area polygons |
| `pixel_id` | Raster cell identifier |
| `x`, `y` | Pixel centroid coordinates |
| `coverage_fraction` | Area overlap fraction for polygons, or `1.0` for points |

### 3. Pixel values

Each dataset writes yearly parquet files with one row per pixel-month and one column per climate variable.

Typical columns:

| Column | Meaning |
|---|---|
| `pixel_id` | Key used to join back to pixel maps |
| `x`, `y` | Pixel centroid coordinates |
| `year`, `month` | Calendar time |
| climate variables | Dataset-specific monthly values in physical units |

### 4. Observation summaries

The shared summary step writes one parquet per variable under `processed/climate/<dataset>/damage_areas_summaries/`.

Typical columns:

| Column | Meaning |
|---|---|
| `DAMAGE_AREA_ID` | Observation geometry key |
| `calendar_year`, `calendar_month` | Calendar time |
| `water_year`, `water_year_month` | Water-year time |
| `weighted_mean` | Area-weighted mean over intersecting pixels |
| `value_min`, `value_max` | Range across contributing pixels |
| `n_pixels`, `n_pixels_with_data` | Pixel counts |
| `sum_coverage_fraction` | Total contributing coverage fraction |

## Why This Design

- Preserves within-polygon variation for large IDS features.
- Avoids repeated extraction for duplicate or overlapping geometries.
- Keeps monthly time resolution for seasonal analysis.
- Supports multiple climate datasets with the same downstream pattern.
- Works for both polygon-based IDS data and point-based FIA site climate.

## Workflow Steps

### 1. Build pixel maps

Dataset-specific scripts:

- [02_terraclimate/scripts/01_build_pixel_maps.R](../02_terraclimate/scripts/01_build_pixel_maps.R)
- [03_prism/scripts/01_build_pixel_maps.R](../03_prism/scripts/01_build_pixel_maps.R)
- [04_worldclim/scripts/02_build_pixel_maps.R](../04_worldclim/scripts/02_build_pixel_maps.R)

Behavior:

- Polygons use overlap-based extraction so `coverage_fraction` is preserved.
- Points use the containing pixel and get `coverage_fraction = 1.0`.
- Damage areas are handled carefully so shared geometries do not trigger redundant work.

### 2. Extract climate values

Dataset-specific scripts:

- [02_terraclimate/scripts/02_extract_terraclimate.R](../02_terraclimate/scripts/02_extract_terraclimate.R)
- [03_prism/scripts/02_extract_prism.R](../03_prism/scripts/02_extract_prism.R)
- [04_worldclim/scripts/03_extract_worldclim.R](../04_worldclim/scripts/03_extract_worldclim.R)

Behavior:

- TerraClimate extracts through Google Earth Engine.
- PRISM downloads monthly files from the web service, extracts values, and deletes raw files immediately.
- WorldClim reads locally downloaded GeoTIFFs.

### 3. Build observation summaries

Shared script:

- [scripts/build_climate_summaries.R](../scripts/build_climate_summaries.R)

Behavior:

- Reads yearly wide-format source files.
- Selects one variable at a time.
- Joins pixel values to the damage-area pixel map.
- Computes observation-level summaries.
- Writes one output parquet per variable.

## Time Conventions

The climate outputs store both calendar and water-year time fields.

- Calendar year: January through December.
- Water year: October through September.
- IDS keeps `SURVEY_YEAR` as provided by the source data.

This allows downstream analysis to choose the time basis that matches the question instead of locking that choice in during extraction.

## Dataset Differences

| Dataset | Access method | Coverage | Resolution | Notes |
|---|---|---|---|---|
| TerraClimate | Google Earth Engine | Global | About 4 km | Broadest variable set |
| PRISM | Direct web-service download | CONUS only | About 800 m | Highest resolution |
| WorldClim | Local GeoTIFF download | Global land areas | About 4.5 km | Local-file workflow |

## FIA Point-Based Variant

The same architecture also supports point-based site climate extraction for FIA:

- FIA site locations are converted to points.
- Each site is mapped to a single containing TerraClimate pixel.
- Pixel values are joined back directly to site IDs.
- No area-weighted averaging is needed because each site has one containing pixel.

See [05_fia/scripts/06_extract_site_climate.R](../05_fia/scripts/06_extract_site_climate.R) and [05_fia/WORKFLOW.md](../05_fia/WORKFLOW.md) for the FIA-specific implementation.

## Shared Code

| Script | Role |
|---|---|
| [scripts/utils/climate_extract.R](../scripts/utils/climate_extract.R) | Pixel mapping and extraction helpers |
| [scripts/utils/time_utils.R](../scripts/utils/time_utils.R) | Calendar and water-year conversion helpers |
| [scripts/utils/gee_utils.R](../scripts/utils/gee_utils.R) | Google Earth Engine setup and helper functions |
| [scripts/build_climate_summaries.R](../scripts/build_climate_summaries.R) | Shared observation summary builder |

## See also

- [Docs Hub](README.md)
- [Reproduce](REPRODUCE.md)
- [TerraClimate README](../02_terraclimate/README.md)
- [FIA technical workflow](../05_fia/WORKFLOW.md)
