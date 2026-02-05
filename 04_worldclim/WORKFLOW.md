# WorldClim Monthly Weather Extraction Workflow

## Status
- [ ] Download decade GeoTIFFs (01_download_worldclim.R)
- [ ] Build pixel maps (02_build_pixel_maps.R)
- [ ] Extract monthly pixel values (03_extract_worldclim.R)

## Overview

WorldClim provides an alternative to TerraClimate with similar resolution (~4.5km). The main differences:
- Fewer variables (3 vs 14)
- Different interpolation methodology (station-based vs reanalysis-derived)
- Data ends in 2021 (no coverage for IDS years 2022-2024)

Unlike TerraClimate and PRISM, WorldClim is downloaded as local GeoTIFF files rather than extracted via GEE.

## Scripts

### 01_download_worldclim.R
Downloads decade archives from geodata.ucdavis.edu.

- **Output:** `data/raw/{variable}/{variable}_{decade}.tif`
- **Files:** 3 variables × 7 decades = 21 archives
- **Size:** ~500 MB total after extraction
- **Process:**
  1. For each variable (tmin, tmax, prec):
  2. For each decade (1960-1969 through 2019-2021):
     - Download ZIP archive
     - Extract GeoTIFF files
     - Delete ZIP to save space

---

### 02_build_pixel_maps.R
Creates mapping from IDS observations to WorldClim raster pixels.

- **Input:**
  - `01_ids/data/processed/ids_cleaned.gpkg`
  - Any downloaded GeoTIFF (for reference grid)
- **Output:** `data/processed/pixel_maps/*_pixel_map.parquet`

---

### 03_extract_worldclim.R
Extracts monthly climate values from local GeoTIFF files.

- **Input:**
  - Pixel maps from step 2
  - Downloaded GeoTIFFs
- **Output:** `data/processed/pixel_values/worldclim_{year}.parquet`
- **Process:**
  1. For each year (1997-2021):
     - Determine which decade file contains this year
     - Calculate band index: (year_offset × 12) + month
     - Extract values at pixel coordinates using terra::extract()

---

## File Structure

WorldClim GeoTIFFs contain multiple bands per file:

```
wc2.1_2.5m_tmin_1990_1999.tif
├── Band 1:  Jan 1990
├── Band 2:  Feb 1990
├── ...
├── Band 12: Dec 1990
├── Band 13: Jan 1991
├── ...
└── Band 120: Dec 1999
```

Band index formula: `(year - decade_start) × 12 + month`

---

## Temporal Coverage

| Decade File | Years | Bands |
|-------------|-------|-------|
| 1960-1969 | 1960-1969 | 120 |
| 1970-1979 | 1970-1979 | 120 |
| 1980-1989 | 1980-1989 | 120 |
| 1990-1999 | 1990-1999 | 120 |
| 2000-2009 | 2000-2009 | 120 |
| 2010-2018 | 2010-2018 | 108 |
| 2019-2021 | 2019-2021 | 36 |

**IDS years with no WorldClim data:** 2022, 2023, 2024

---

## Decisions Log

| Decision | Rationale | Date |
|----------|-----------|------|
| Local download (not GEE) | WorldClim monthly weather not available on GEE | 2026-02-05 |
| 2.5 arc-minute resolution | Matches available monthly weather data | 2026-02-05 |
| Skip years 2022-2024 | WorldClim data ends in 2021 | 2026-02-05 |
