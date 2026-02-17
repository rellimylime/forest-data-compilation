# ERA5 Reanalysis Data: Technical Reference

**For quick-start guide and usage examples, see README.txt**

This document covers ERA5-specific technical details. For the shared pixel decomposition architecture, workflow steps, and data schemas, see **[`docs/ARCHITECTURE.md`](../docs/ARCHITECTURE.md)**.

---

## Status
- [ ] Download NetCDFs via CDS API (01_download_era5.R)
- [ ] Build pixel maps (02_build_pixel_maps.R)
- [ ] Extract daily pixel values (03_extract_era5.R)
- [ ] Build observation summaries (scripts/build_climate_summaries.R era5)

---

## Dataset Overview

**Source:** [Copernicus Climate Data Store (CDS)](https://cds.climate.copernicus.eu/)
**Product:** ERA5 hourly data on single levels from 1940 to present (hourly aggregated to daily)
**Resolution:** ~28km (0.25°)
**Coverage:** Global
**Temporal Resolution:** Daily, 1940-present
**Variables:** 20 atmospheric and surface variables
**Access Method:** CDS API (local NetCDF download)

**Key Differences from Other Datasets:**
- **Coarsest resolution** (28km vs 4-8km) — often 1 pixel per observation
- **Finest temporal resolution** (daily vs monthly)
- **Most variables** (20 vs 3-14) — includes radiation, soil, LAI
- **Largest data volume** (~500 GB raw NetCDFs)
- **CDS API required** (registration + credentials)

---

## Prerequisites

### CDS API Setup (Required)

ERA5 data is accessed via the Copernicus Climate Data Store API, not GEE.

**Steps:**
1. **Register** at https://cds.climate.copernicus.eu/
2. **Accept ERA5 terms of use** in your profile
3. **Get API credentials** from your user profile page
4. **Create `~/.cdsapirc` file:**
   ```
   url: https://cds.climate.copernicus.eu/api
   key: <your-uid>:<your-api-key>
   ```
5. **Install Python cdsapi package:**
   ```bash
   pip install cdsapi
   ```

**Verification:**
```python
import cdsapi
c = cdsapi.Client()
print("CDS API configured successfully!")
```

---

## ERA5-Specific Parameters

### config.yaml Settings

```yaml
era5:
  cds_dataset: "reanalysis-era5-single-levels"
  resolution: 0.25  # degrees (~28km)
  area: [72, -180, 17, -64]  # [N, W, S, E] — US + buffer
  variables:
    t2m:
      era5_name: "2m_temperature"
      units: "deg C"
      conversion: "K_to_C"  # Subtract 273.15
    tp:
      era5_name: "total_precipitation"
      units: "mm"
      conversion: "m_to_mm"  # Multiply by 1000
    # ... (20 total variables; see config.yaml)
```

**Note:** ERA5 variable names differ from internal names. `era5_name` specifies the CDS API parameter.

---

## Script Details

### 01_download_era5.R
**ERA5-Specific Behavior:**
- Downloads NetCDF files via CDS API
- **One file per variable per year** (20 variables × 28 years = 560 files)
- Daily aggregation from hourly data (mean, min, max, or sum depending on variable)
- Geographic subsetting to US bounding box (`area` parameter) reduces download size by ~80%
- **Download time:** Hours to days (CDS queue times vary)
- **Storage:** ~500+ GB total

**Process:**
```r
for (variable in variables) {
  for (year in 1997:2024) {
    cds_api$retrieve(
      dataset = "reanalysis-era5-single-levels",
      request = list(
        variable = variable$era5_name,
        product_type = "reanalysis",
        year = year,
        month = 1:12,
        day = 1:31,
        time = "00:00",  # daily aggregation
        area = c(72, -180, 17, -64),  # US bounding box
        format = "netcdf"
      ),
      target = glue("{data_raw}/{variable}/{variable}_{year}.nc")
    )
  }
}
```

---

### 02_build_pixel_maps.R
**ERA5-Specific Behavior:**
- Uses any downloaded NetCDF as reference grid (same grid for all variables)
- **Coarse resolution** (28km) means:
  - Small IDS observations (< 1000 ha) often map to single pixel
  - Large observations map to 1-5 pixels
  - Less within-polygon variation captured than finer-resolution datasets

---

### 03_extract_era5.R
**ERA5-Specific Behavior:**
- Extracts from local NetCDF files (not GEE)
- **Daily data:** 365-366 rows per pixel per year
- Applies unit conversions during extraction:
  - Temperature: Kelvin → Celsius (−273.15)
  - Precipitation: meters → millimeters (×1000)
  - Pressure: Pascals → hectopascals (×0.01)
  - Radiation: J/m² → MJ/m² (×0.000001)

**Output schema (daily):**
```
pixel_id, x, y, year, month, day, [20 variables]
```

---

## Unit Conversions

ERA5 stores variables in SI units. Conversions applied during extraction for usability:

| Variable | Raw Unit | Converted Unit | Conversion |
|----------|----------|----------------|------------|
| t2m, d2m, skt, stl1, stl2 | Kelvin | °C | −273.15 |
| tp, sf, e, pev | m | mm | ×1000 |
| sp | Pa | hPa | ×0.01 |
| ssrd, ssr, str, strd | J/m² | MJ/m² | ×0.000001 |
| lai_lv, lai_hv | m²/m² | m²/m² | No conversion |
| u10, v10 | m/s | m/s | No conversion |

---

## Data Volume Estimates

| Component | Size |
|-----------|------|
| Raw NetCDFs (full downloads) | ~1-2 GB per variable-year |
| Raw NetCDFs (US subset) | ~300-500 MB per variable-year |
| **Total raw (US subset)** | **~500 GB** |
| Pixel values (daily, parquet) | ~500 MB per year |
| **Total pixel values** | **~14 GB** |

**Disk space recommendation:** 600+ GB available before starting download.

---

## Temporal Resolution Comparison

| Dataset | Resolution | Rows per Pixel per Year |
|---------|------------|-------------------------|
| **ERA5** | **Daily** | **365-366** |
| TerraClimate | Monthly | 12 |
| PRISM | Monthly | 12 |
| WorldClim | Monthly | 12 |

ERA5 produces 30x more rows per pixel than monthly datasets.

---

## Geographic Coverage

### Area Subsetting

Downloads are subset to US bounding box to reduce size:
```
area: [72°N, -180°W, 17°N, -64°W]
```

This covers:
- All 50 US states
- Generous buffer for coastal observations
- ~20% of global area

**Size reduction:** 80% smaller downloads than global coverage

---

## Variable Reference

ERA5 provides 20 variables across 5 categories:

### Atmospheric (6 variables)
- `t2m`: 2-meter temperature
- `d2m`: 2-meter dewpoint temperature
- `sp`: Surface pressure
- `u10`: 10m U wind component
- `v10`: 10m V wind component
- `skt`: Skin temperature

### Precipitation/Moisture (4 variables)
- `tp`: Total precipitation
- `sf`: Snowfall
- `e`: Evaporation
- `pev`: Potential evaporation

### Radiation (4 variables)
- `ssrd`: Surface solar radiation downwards
- `ssr`: Surface net solar radiation
- `str`: Surface net thermal radiation
- `strd`: Surface thermal radiation downwards

### Soil (2 variables)
- `stl1`: Soil temperature level 1 (0-7cm)
- `stl2`: Soil temperature level 2 (7-28cm)

### Vegetation (2 variables)
- `lai_lv`: Leaf area index (low vegetation)
- `lai_hv`: Leaf area index (high vegetation)

### Derived (2 variables)
- `vpd`: Vapor pressure deficit (calculated from t2m and d2m)
- `windspeed`: Wind speed (calculated from u10 and v10)

---

## Decisions Log

| Decision | Rationale | Date |
|----------|-----------|------|
| CDS API download (not GEE) | Full ERA5 single levels not on GEE; ERA5-Land has fewer variables | TBD |
| Daily resolution | Enables event-based analysis; user requirement | TBD |
| 20-variable subset | Comprehensive coverage without redundant/specialized variables | TBD |
| Area subsetting [72, -180, 17, -64] | 80% download size reduction; covers all US observations | TBD |
| Daily aggregation from hourly | Manageable data volume; sufficient for most analyses | TBD |
| Same pixel decomposition as other datasets | Proven efficient pattern | TBD |

---

## Workflow Execution

```r
# 1. Download NetCDFs via CDS API (one-time, ~500 GB, can take days)
source("05_era5/scripts/01_download_era5.R")

# 2. Build pixel maps
source("05_era5/scripts/02_build_pixel_maps.R")

# 3. Extract climate from local NetCDF files
source("05_era5/scripts/03_extract_era5.R")

# 4. Build observation summaries (shared script, reads source files directly)
Rscript scripts/build_climate_summaries.R era5
```

For detailed workflow architecture, see [`docs/ARCHITECTURE.md`](../docs/ARCHITECTURE.md).

---

## Troubleshooting

### CDS API authentication failure
**Error:** "Missing/incomplete CDS credentials"
**Solution:** Verify `~/.cdsapirc` exists with correct `url` and `key` lines

### CDS API timeout / long queue times
**Cause:** High CDS server load; requests are queued
**Solution:**
- Queue times can be hours; run overnight or over weekend
- Check request status at https://cds.climate.copernicus.eu/cdsapp#!/yourrequests
- Requests remain in queue even if R script times out

### Download failures mid-process
**Cause:** Network interruption or CDS server error
**Solution:**
- Script automatically skips already-downloaded files on restart
- Re-run to resume from last successful download

### Disk space exhausted
**Cause:** ~500 GB raw data + ~14 GB processed data
**Solution:**
- Check available space before starting
- Consider processing and deleting raw NetCDFs year-by-year
- Use external storage if needed

### Large output files
**Note:** Daily data produces 30x more rows than monthly datasets. This is expected.
**Parquet compression** keeps file sizes manageable (~500 MB/year).

### Missing variables in NetCDF
**Cause:** Variable name mismatch
**Solution:** Verify `era5_name` in config.yaml matches CDS API parameter names (see [CDS documentation](https://cds.climate.copernicus.eu/cdsapp#!/dataset/reanalysis-era5-single-levels))

### Coarse resolution concerns
**Expected:** 28km pixels are large relative to small IDS observations.
**Implication:** Limited within-polygon variation. Use PRISM or TerraClimate for finer spatial detail.
