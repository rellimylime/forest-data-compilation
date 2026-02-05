# ERA5 Daily Reanalysis Extraction Workflow

## Status
- [ ] Download NetCDFs via CDS API (01_download_era5.R)
- [ ] Build pixel maps (02_build_pixel_maps.R)
- [ ] Extract daily pixel values (03_extract_era5.R)

## Overview

ERA5 provides the most comprehensive variable set (20 variables) and daily temporal resolution. However, it has coarser spatial resolution (~28km) than other climate datasets.

**Use case:** Best for analysis requiring daily climate data or variables not available in other datasets (e.g., soil temperature, LAI, radiation components).

## Prerequisites

### CDS API Setup

1. Register at https://cds.climate.copernicus.eu/
2. Accept the ERA5 terms of use
3. Get your API credentials from your user profile
4. Create `~/.cdsapirc`:
   ```
   url: https://cds.climate.copernicus.eu/api
   key: <uid>:<api-key>
   ```
5. Install Python cdsapi: `pip install cdsapi`

## Scripts

### 01_download_era5.R
Downloads ERA5 NetCDF files via CDS API.

- **Output:** `data/raw/{variable}/{variable}_{year}.nc`
- **Files:** 20 variables × 28 years = 560 files
- **Size:** ~1-2 GB per file; ~500+ GB total
- **Process:**
  1. Initialize CDS API client
  2. For each variable, for each year:
     - Submit download request
     - Wait for processing
     - Download NetCDF
- **Note:** CDS queue times can be hours. Consider running overnight.

---

### 02_build_pixel_maps.R
Creates mapping from IDS observations to ERA5 raster pixels.

- **Input:**
  - `01_ids/data/processed/ids_cleaned.gpkg`
  - Any downloaded NetCDF (for reference grid)
- **Output:** `data/processed/pixel_maps/*_pixel_map.parquet`
- **Note:** ERA5's coarse resolution means many observations share pixels

---

### 03_extract_era5.R
Extracts daily climate values from local NetCDF files.

- **Input:**
  - Pixel maps from step 2
  - Downloaded NetCDFs
- **Output:** `data/processed/pixel_values/era5_{year}.parquet`
  - Columns: pixel_id, x, y, year, month, day, [20 variables]
  - Rows per year: n_pixels × 365 (or 366 for leap years)
- **Process:**
  1. For each year:
     - For each day:
       - Extract band values at pixel coordinates
       - Apply scale factors
       - Convert Kelvin to Celsius for temperature variables
     - Save as parquet

---

## Unit Conversions

Conversions applied during extraction:

| Variable | Raw Unit | Converted Unit | Conversion |
|----------|----------|----------------|------------|
| t2m, d2m, skt, stl1, stl2 | Kelvin | °C | -273.15 |
| tp, sf, e, pev | m | mm | ×1000 |
| sp | Pa | hPa | ×0.01 |
| ssrd, ssr, str | J/m² | MJ/m² | ×0.000001 |

---

## Data Volume

| Component | Size Estimate |
|-----------|---------------|
| Raw NetCDFs | ~500+ GB |
| Pixel values (daily) | ~500 MB/year |
| Total pixel values | ~14 GB |

Consider disk space requirements before starting download.

---

## Decisions Log

| Decision | Rationale | Date |
|----------|-----------|------|
| CDS API (not GEE) | Full ERA5 single levels not on GEE; ERA5-Land has fewer variables | 2026-02-05 |
| Daily resolution | User requirement; enables seasonal/event analysis | 2026-02-05 |
| 20-variable subset | Comprehensive coverage while avoiding redundant/specialized variables | 2026-02-05 |
| Area subsetting [72, -180, 17, -64] | Reduces download size to US + buffer | 2026-02-05 |

---

## Troubleshooting

### CDS API timeout
**Cause:** Large request or busy CDS servers
**Solution:** Requests are queued; check status at CDS website

### "Request failed" errors
**Cause:** Invalid variable name or date range
**Solution:** Verify era5_name in config.yaml matches CDS variable names

### Missing ~/.cdsapirc
**Error:** "Missing/incomplete CDS credentials"
**Solution:** Create file with url and key as shown in Prerequisites
