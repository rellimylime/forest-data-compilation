# PRISM 800m Pixel-Level Extraction Workflow

## Status
- [ ] Build pixel maps (01_build_pixel_maps.R)
- [ ] Extract monthly pixel values (02_extract_prism.R)

## Overview

PRISM provides the highest resolution (~800m) climate data available for CONUS. This workflow uses the same two-table architecture as TerraClimate but produces significantly more pixel mappings due to the finer resolution.

**Important:** PRISM only covers the contiguous United States. Alaska (R10) and Hawaii observations are automatically excluded during pixel map construction.

## Scripts

### 01_build_pixel_maps.R
Creates mapping from CONUS IDS observations to PRISM raster pixels.

- **Input:** `01_ids/data/processed/ids_cleaned.gpkg`
- **Output:**
  - `data/processed/pixel_maps/damage_areas_pixel_map.parquet`
  - `data/processed/pixel_maps/damage_points_pixel_map.parquet`
  - `data/processed/pixel_maps/surveyed_areas_pixel_map.parquet`
- **Process:**
  1. Download PRISM reference raster from GEE (defines 800m pixel grid)
  2. Filter IDS observations to CONUS only
  3. Build pixel maps using exactextractr

---

### 02_extract_prism.R
Extracts monthly climate values for all unique PRISM pixels via GEE.

- **Input:** Pixel maps from step 1
- **Output:** `data/processed/pixel_values/prism_{year}.parquet`
- **Process:** Same as TerraClimate but at 800m scale

---

## Resolution Comparison

| Dataset | Resolution | Typical pixels per IDS polygon |
|---------|------------|-------------------------------|
| PRISM | 800m | 25-100+ |
| TerraClimate | 4km | 1-4 |
| WorldClim | 4.5km | 1-4 |
| ERA5 | 28km | 1 |

PRISM's high resolution means more within-polygon variation is captured, but extraction takes longer.

---

## Decisions Log

| Decision | Rationale | Date |
|----------|-----------|------|
| GEE extraction (not direct download) | Direct download requires paid subscription; GEE provides free access | 2026-02-05 |
| Exclude Alaska/Hawaii | PRISM coverage is CONUS only | 2026-02-05 |
| 800m scale parameter | Native PRISM resolution | 2026-02-05 |
