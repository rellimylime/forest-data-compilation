# WorldClim Monthly Weather: Technical Reference

**For quick-start guide and usage examples, see README.txt**

This document covers WorldClim-specific technical details. For the shared pixel decomposition architecture, workflow steps, and data schemas, see **[`docs/ARCHITECTURE.md`](../docs/ARCHITECTURE.md)**.

---

## Status
- [ ] Download decade GeoTIFFs (01_download_worldclim.R)
- [ ] Build pixel maps (02_build_pixel_maps.R)
- [ ] Extract monthly pixel values (03_extract_worldclim.R)
- [ ] Build observation summaries (scripts/build_climate_summaries.R worldclim)

---

## Dataset Overview

**Source:** [WorldClim Version 2.1](https://www.worldclim.org/)
**Resolution:** ~4.5km (2.5 arc-minutes / 0.04166°)
**Coverage:** Global
**Temporal Resolution:** Monthly, 1960-2021 (ends before IDS 2022-2024)
**Variables:** 3 climate variables (tmin, tmax, prec)
**Access Method:** Direct download (local GeoTIFF files)

**Key Differences from TerraClimate:**
- Similar resolution (~4.5km vs 4km)
- Fewer variables (3 vs 14) — temperature and precipitation only
- Different interpolation methodology (station-based)
- **Data ends in 2021** — no coverage for IDS years 2022-2024
- Local download required (not available via GEE)

---

## WorldClim-Specific Parameters

### config.yaml Settings

```yaml
worldclim:
  download_url: "https://geodata.ucdavis.edu/climate/worldclim/2_1/base/"
  resolution: "2.5m"  # 2.5 arc-minutes (~4.5km)
  variables:
    tmin:
      units: "deg C"
      scale: 0.1  # WorldClim stores as integer × 10
    tmax:
      units: "deg C"
      scale: 0.1
    prec:
      units: "mm"
      scale: 1
```

---

## Script Details

### 01_download_worldclim.R
**WorldClim-Specific Behavior:**
- Downloads decade-based GeoTIFF archives from geodata.ucdavis.edu
- Each file contains 120 bands (10 years × 12 months), except:
  - 2010-2018: 108 bands (9 years × 12 months)
  - 2019-2021: 36 bands (3 years × 12 months)
- Downloads 3 variables × 7 decade files = 21 archives
- Total download size: ~500 MB

**Decade Files:**
```
wc2.1_2.5m_tmin_1960-1969.tif    # 120 bands
wc2.1_2.5m_tmin_1970-1979.tif    # 120 bands
wc2.1_2.5m_tmin_1980-1989.tif    # 120 bands
wc2.1_2.5m_tmin_1990-1999.tif    # 120 bands
wc2.1_2.5m_tmin_2000-2009.tif    # 120 bands
wc2.1_2.5m_tmin_2010-2018.tif    # 108 bands
wc2.1_2.5m_tmin_2019-2021.tif    # 36 bands
```

---

### 02_build_pixel_maps.R
**WorldClim-Specific Behavior:**
- Uses any downloaded GeoTIFF as reference raster (same grid for all variables)
- Resolution similar to TerraClimate (~4.5km)
- Includes all IDS observations (global coverage)

---

### 03_extract_worldclim.R
**WorldClim-Specific Behavior:**
- Extracts from local GeoTIFF files (not GEE)
- **Band index calculation:**
  ```r
  decade_start <- floor(year / 10) * 10
  year_offset <- year - decade_start
  band_index <- (year_offset * 12) + month
  ```
- Applies scale factors during extraction (×0.1 for temperatures)
- **Skips years 2022-2024** (no WorldClim data available)

---

## File Structure

WorldClim GeoTIFFs store multiple years in a single file:

```
wc2.1_2.5m_tmin_1990-1999.tif
├── Band 1:  Jan 1990
├── Band 2:  Feb 1990
├── ...
├── Band 12: Dec 1990
├── Band 13: Jan 1991
├── ...
└── Band 120: Dec 1999
```

**Band Index Formula:**
```
band = (year - decade_start) × 12 + month
```

**Example:** February 1995:
- decade_start = 1990
- year_offset = 1995 - 1990 = 5
- band = (5 × 12) + 2 = 62

---

## Temporal Coverage

| Decade File | Years | Bands | IDS Coverage |
|-------------|-------|-------|--------------|
| 1960-1969 | 1960-1969 | 120 | None (pre-IDS) |
| 1970-1979 | 1970-1979 | 120 | None (pre-IDS) |
| 1980-1989 | 1980-1989 | 120 | None (pre-IDS) |
| 1990-1999 | 1990-1999 | 120 | Partial (1997-1999) |
| 2000-2009 | 2000-2009 | 120 | Full |
| 2010-2018 | 2010-2018 | 108 | Full |
| 2019-2021 | 2019-2021 | 36 | Partial (2019-2021) |

**IDS years with NO WorldClim data:** 2022, 2023, 2024

**Affected observations:** ~500k IDS observations from 2022-2024 will have no WorldClim summaries

---

## Resolution Comparison

| Dataset | Resolution | Pixels per 10 km² IDS Polygon |
|---------|------------|-------------------------------|
| PRISM | 800m | ~156 pixels |
| TerraClimate | 4km | ~6 pixels |
| **WorldClim** | **4.5km** | **~5 pixels** |
| ERA5 | 28km | 1 pixel |

WorldClim and TerraClimate have similar spatial granularity.

---

## Decisions Log

| Decision | Rationale | Date |
|----------|-----------|------|
| Local download (not GEE) | WorldClim monthly weather not available on GEE (only bioclim) | TBD |
| 2.5 arc-minute resolution | Matches available monthly weather data | TBD |
| Skip years 2022-2024 | WorldClim data ends in 2021 | TBD |
| Decade-based files | WorldClim distribution format | TBD |
| Same pixel decomposition as TerraClimate | Proven efficient pattern | TBD |

---

## Workflow Execution

```r
# 1. Download GeoTIFFs (one-time, ~500 MB)
source("04_worldclim/scripts/01_download_worldclim.R")

# 2. Build pixel maps
source("04_worldclim/scripts/02_build_pixel_maps.R")

# 3. Extract climate from local files
source("04_worldclim/scripts/03_extract_worldclim.R")

# 4. Build observation summaries (shared script, reads source files directly)
Rscript scripts/build_climate_summaries.R worldclim
```

For detailed workflow architecture, see [`docs/ARCHITECTURE.md`](../docs/ARCHITECTURE.md).

---

## Troubleshooting

### Missing data for 2022-2024
**Expected:** WorldClim ends in 2021. IDS observations from 2022-2024 will have NA summaries.
**Solution:** Use TerraClimate or ERA5 for these years.

### Incorrect band index
**Symptom:** Climate values don't match expected month/year.
**Solution:** Verify band calculation: `(year - decade_start) × 12 + month`

### Download failures
**Cause:** geodata.ucdavis.edu server issues or network interruption.
**Solution:** Script includes retry logic. Check server status if persistent.

### Large raw file sizes
**Note:** Decade files are ~70-100 MB each. 21 total files = ~500 MB. This is manageable compared to GEE-based datasets.
