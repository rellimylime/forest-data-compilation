# WorldClim Monthly Weather: Technical Reference

**For quick-start guide and usage examples, see README.txt**

This document covers WorldClim-specific technical details. For the shared pixel decomposition architecture, workflow steps, and data schemas, see **[`docs/ARCHITECTURE.md`](../docs/ARCHITECTURE.md)**.

---

## Status
- [x] Download decade GeoTIFFs (01_download_worldclim.R)
- [x] Build pixel maps (02_build_pixel_maps.R)
- [x] Extract monthly pixel values (03_extract_worldclim.R)
- [ ] Build observation summaries (scripts/build_climate_summaries.R worldclim)

---

## Dataset Overview

**Source:** [WorldClim Version 2.1](https://www.worldclim.org/)
**Resolution:** ~4.5km (2.5 arc-minutes / 0.04166°)
**Coverage:** Global
**Temporal Resolution:** Monthly, 1950-2024 (CRU TS 4.09 interpolation)
**Variables:** 3 climate variables (tmin, tmax, prec)
**Access Method:** Direct download (local GeoTIFF files)

**Key Differences from TerraClimate:**
- Similar resolution (~4.5km vs 4km)
- Fewer variables (3 vs 14) - temperature and precipitation only
- Different interpolation methodology (station-based, CRU TS 4.09)
- Full IDS coverage (1997-2024)
- Local download required (not available via GEE)

---

## WorldClim-Specific Parameters

### config.yaml Settings

```yaml
worldclim:
  download_url_pattern: "https://geodata.ucdavis.edu/climate/worldclim/2_1/hist/cts4.09/wc2.1_cruts4.09_2.5m_{variable}_{decade}.zip"
  coverage: "Global land areas, 1950-2024"
  variables:
    tmin:
      units: "°C"
      scale: 1
    tmax:
      units: "°C"
      scale: 1
    prec:
      units: "mm"
      scale: 1
```

---

## Script Details

### 01_download_worldclim.R
**WorldClim-Specific Behavior:**
- Downloads decade-based zip archives from geodata.ucdavis.edu (CRU TS 4.09)
- Each zip contains 120 individual monthly GeoTIFFs (10 years × 12 months),
  except the final 2020-2024 zip which has 60 files (5 years × 12 months)
- Downloads 3 variables × 8 decade files = 24 archives
- Total download size: ~600 MB (estimate)

**Decade Archives (example: tmin):**
```
wc2.1_cruts4.09_2.5m_tmin_1950-1959.zip    # 120 monthly TIFs
wc2.1_cruts4.09_2.5m_tmin_1960-1969.zip    # 120 monthly TIFs
...
wc2.1_cruts4.09_2.5m_tmin_2010-2019.zip    # 120 monthly TIFs
wc2.1_cruts4.09_2.5m_tmin_2020-2024.zip    # 60 monthly TIFs
```

**Extracted file naming convention:**
```
wc2.1_cruts4.09_2.5m_tmin_1990-01.tif   (Jan 1990)
wc2.1_cruts4.09_2.5m_tmin_1990-02.tif   (Feb 1990)
...
wc2.1_cruts4.09_2.5m_tmin_1999-12.tif   (Dec 1999)
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
- Each month is a separate single-band TIF; no band index math needed
- Looks up monthly TIF by name: `wc2.1_cruts4.09_2.5m_{var}_{YYYY}-{MM}.tif`
- Values are in native units (°C, mm) - no scale factors applied
- Covers full IDS range 1997-2024

---

## File Structure

Each zip extracts to 120 individual monthly GeoTIFFs (single-band each):

```
04_worldclim/data/raw/tmin/
├── wc2.1_cruts4.09_2.5m_tmin_1990-01.tif   (Jan 1990)
├── wc2.1_cruts4.09_2.5m_tmin_1990-02.tif   (Feb 1990)
├── ...
└── wc2.1_cruts4.09_2.5m_tmin_1999-12.tif   (Dec 1999)
```

To extract February 1995: just load `tmin_1995-02.tif` directly (band 1).

---

## Temporal Coverage

| Decade Archive | Monthly TIFs | IDS Coverage |
|----------------|-------------|--------------|
| 1950-1959 | 120 | None (pre-IDS) |
| 1960-1969 | 120 | None (pre-IDS) |
| 1970-1979 | 120 | None (pre-IDS) |
| 1980-1989 | 120 | None (pre-IDS) |
| 1990-1999 | 120 | Partial (1997-1999) |
| 2000-2009 | 120 | Full |
| 2010-2019 | 120 | Full |
| 2020-2024 | 60 | Full (2020-2024) |

**Full IDS coverage:** all years 1997-2024 have WorldClim data.

---

## Resolution Comparison

| Dataset | Resolution | Pixels per 10 km² IDS Polygon |
|---------|------------|-------------------------------|
| PRISM | 800m | ~156 pixels |
| TerraClimate | 4km | ~6 pixels |
| **WorldClim** | **4.5km** | **~5 pixels** |

WorldClim and TerraClimate have similar spatial granularity.

---

## Decisions Log

| Decision | Rationale | Date |
|----------|-----------|------|
| Local download (not GEE) | WorldClim monthly weather not available on GEE (only bioclim) | 2026-02 |
| 2.5 arc-minute resolution | Matches available monthly weather data | 2026-02 |
| Extract 1997-2024 | Full IDS range; WorldClim CRU TS 4.09 covers through 2024 | 2026-02 |
| Decade-based files | WorldClim distribution format | 2026-02 |
| Same pixel decomposition as TerraClimate | Proven efficient pattern | 2026-02 |

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

### Unexpected "missing" messages during extraction
**Symptom:** Script prints `missing tmin/2020-01` etc.
**Cause:** Assumed filename convention (`wc2.1_cruts4.09_2.5m_{var}_{YYYY}-{MM}.tif`)
doesn't match actual filenames extracted from zip.
**Solution:** Check actual filenames: `list.files("04_worldclim/data/raw/tmin")`.
Update the `sprintf()` pattern in `03_extract_worldclim.R` line ~95 accordingly.

### Download failures
**Cause:** geodata.ucdavis.edu server issues or network interruption.
**Solution:** Script includes retry logic. Check server status if persistent.

### Large raw file sizes
**Note:** Decade files are ~70-100 MB each. 24 total files = ~600 MB. This is manageable compared to GEE-based datasets.
