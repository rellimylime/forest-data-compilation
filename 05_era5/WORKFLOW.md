# ERA5 Reanalysis Data: Technical Reference

**For quick-start guide and usage examples, see README.txt**

This document covers ERA5-specific technical details. For the shared pixel decomposition architecture, workflow steps, and data schemas, see **[`docs/ARCHITECTURE.md`](../docs/ARCHITECTURE.md)**.

---

## Status
- [ ] Download NetCDFs via CDS API (01_download_era5.R)
- [ ] Build pixel maps (02_build_pixel_maps.R)
- [ ] Extract monthly pixel values (03_extract_era5.R)
- [ ] Build observation summaries (scripts/build_climate_summaries.R era5)

---

## Pre-Download Decisions and Gotchas

Key decisions to confirm before starting the download, and analysis gotchas to flag to collaborators.

### Decisions to Confirm Before Downloading

**1. Monthly temporal resolution (decided)**
Current plan: monthly extraction (~16 GB raw, 12 rows/pixel/year) using the CDS dataset `reanalysis-era5-single-levels-monthly-means`. ERA5's primary value-add over TerraClimate/PRISM/WorldClim at monthly resolution is its much larger variable set (48 variables vs. 3-14) — radiation, soil layers, snow, hydrology, cloud cover, boundary layer, etc. Daily ERA5 would enable event-based analysis (GDD, drought streaks) but at ~1.3 TB raw and 365 rows/pixel/year — a significant storage and compute increase.

**2. ERA5 Single Levels vs. ERA5-Land**
Current plan: ERA5 Single Levels (~28km). ERA5-Land is a related product at ~9km resolution focused on land-surface variables (soil moisture, soil temperature, snow, evaporation, runoff, 2m temperature, precipitation) — roughly 3× finer spatial resolution for those variables. ERA5-Land lacks atmospheric variables (CAPE, boundary layer height, cloud cover, pressure). If fine-resolution soil and hydrology are the main priority over atmospheric/fire-weather variables, ERA5-Land may be the better choice. The two products cannot be simply substituted — different CDS datasets, different download scripts.

**3. CEMS-Fire FWI dataset (separate from ERA5)**
If Canadian Fire Weather Index components are needed (DC, DMC, FFMC, BUI, ISI, FWI, DSR), these are available pre-computed as daily values from the CDS dataset `cems-fire-historical` — derived from ERA5, no manual computation required. This is far more reliable than computing FWI components by hand. Would require a separate download script but uses the same CDS API infrastructure.

**4. Variables excluded as derivable — confirm acceptable**
The following were intentionally not downloaded because they can be computed from downloaded variables:
- Wind speed at 10m = sqrt(u10² + v10²), at 100m = sqrt(u100² + v100²)
- Wind direction = atan2(−u, −v) × 180/π (degrees from north, direction wind is FROM)
- VPD = f(t2m, d2m) using Magnus formula
- Relative humidity = f(t2m, d2m)
- Large-scale precipitation = tp − cp
- Subsurface runoff = ro − sro
- Diffuse solar radiation = ssrd − fdir
- Physical snow depth (m) = sd × 1000 / rsn
- Net radiation = ssr + str

If pre-computed versions of any of these are needed in the output parquets, flag now — easier to add before downloading than after.

---

### Gotchas for Analysis (flag to collaborators)

**5. Evaporation sign convention**
`e` (total evaporation) is negative when water evaporates from the surface to the atmosphere — the opposite of the intuitive "positive = water leaving the surface" convention. Multiply by −1 for positive-upward. `pev` (potential evaporation) is also negative.

**6. Sensible heat flux sign convention**
`sshf` is positive downward (atmosphere heating the surface). During the day when the surface is warmer than the air, values are typically negative (surface heating the atmosphere). This is the ECMWF convention and differs from some other reanalyses.

**7. Monthly mn2t and mx2t are means of daily extremes**
In ERA5 monthly means, `mn2t` and `mx2t` are the **mean** of each day's min/max temperature over the month — not the absolute monthly minimum or maximum. They are warmer/cooler than the absolute monthly extreme. For most ecological applications (e.g., diurnal range, frost-free season estimation) this is the right quantity, but be explicit in methods if comparing to station monthly absolute extremes.

**8. Accumulated variables in monthly means are mean daily rates**
`ssrd`, `ssr`, `str`, `strd`, `sshf`, `slhf`, `tp`, `cp`, `sf`, `e`, `pev`, `smlt`, `ro`, `sro` are expressed as **mean daily totals** in the monthly means product — ERA5 divides the monthly accumulation by the number of days in the month. To recover monthly totals, multiply by `days_in_month`:
```r
# Monthly total precipitation (mm)
tp_monthly_total <- tp * lubridate::days_in_month(as.Date(paste(year, month, "01", sep="-")))
```
Do not sum these variables naively across months using the monthly-mean values.

**9. Alaska and Hawaii coverage**
PRISM is CONUS-only (no Alaska, no Hawaii). ERA5 covers all IDS regions. For Region 10 (Alaska) and Region 5-HI (Hawaii), ERA5 is the only dataset in this compilation with climate data.

**10. Latent heat flux sign convention**
`slhf` is negative when evaporation is occurring — same ECMWF sign convention as `sshf` (positive = downward, toward surface) and `e` (negative when evaporating upward). Multiply by −1 for intuitive positive-upward. The Bowen ratio (sensible/latent) can be computed as `sshf / slhf`.

---

## Dataset Overview

**Source:** [Copernicus Climate Data Store (CDS)](https://cds.climate.copernicus.eu/)
**Product:** ERA5 monthly averaged reanalysis on single levels
**Resolution:** ~28km (0.25°)
**Coverage:** Global
**Temporal Resolution:** Monthly, 1940-present
**Variables:** 48 atmospheric and surface variables
**Access Method:** CDS API (local NetCDF download)

**Key Differences from Other Datasets:**
- **Coarsest resolution** (28km vs 4-8km) — often 1 pixel per observation
- **Same temporal resolution** as TerraClimate/PRISM/WorldClim (monthly)
- **Most variables** (48 vs 3-14) — includes radiation, soil, snow, hydrology, cloud cover, albedo
- **Raw download size** (~16 GB; 48 vars × 28 years × ~10-20 MB each)
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
- **One file per variable per year** (48 variables × 28 years = 1,344 files)
- Monthly means; each NetCDF contains 12 bands (one per month)
- Geographic subsetting to US bounding box (`area` parameter) reduces download size by ~80%
- **Download time:** Minutes to hours (CDS queue times vary; monthly files are fast)
- **Storage:** ~16 GB total (vs ~1.3 TB for daily)

**Process:**
```r
for (variable in variables) {
  for (year in 1997:2024) {
    cds_api$retrieve(
      dataset = "reanalysis-era5-single-levels-monthly-means",
      request = list(
        variable = variable$era5_name,
        product_type = "monthly_averaged_reanalysis",
        year = year,
        month = sprintf("%02d", 1:12),
        time = "00:00",
        area = c(72, -180, 17, -64),  # US bounding box
        data_format = "netcdf"
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
- **Monthly data:** 12 rows per pixel per year (one per month)
- Loads each variable's NetCDF once per year; extracts all 12 months simultaneously for efficiency
- Applies unit conversions during extraction:
  - Temperature: Kelvin → Celsius (−273.15)
  - Precipitation: meters → millimeters (×1000)
  - Pressure: Pascals → hectopascals (×0.01)
  - Radiation: J/m² → MJ/m² (×0.000001)
- Additive extraction: checks existing parquet columns; re-extracts only if new variables were added to config

**Output schema (monthly):**
```
pixel_id, x, y, year, month, [48 variables]
```

---

## Unit Conversions

ERA5 stores variables in SI units. Conversions applied during extraction for usability:

| Variable | Raw Unit | Converted Unit | Conversion |
|----------|----------|----------------|------------|
| t2m, d2m, skt, mn2t, mx2t, stl1, stl2, stl3, stl4 | Kelvin | °C | −273.15 |
| tp, cp, sf, smlt, ro, sro, e, pev | m | mm | ×1000 |
| sp, msl | Pa | hPa | ×0.01 |
| ssrd, ssr, str, strd, sshf, slhf, fdir | J/m² | MJ/m² | ×0.000001 |
| snowc | % (0–100) | fraction (0–1) | ×0.01 |
| swvl1–4, lai_lv, lai_hv, tcc, lcc, mcc, hcc, fal, u10, v10, i10fg, u100, v100, sd, rsn, tcwv, cape, blh | native | native | No conversion |

---

## Data Volume Estimates

| Component | Size |
|-----------|------|
| Raw NetCDFs (monthly means, US subset) | ~10-20 MB per variable-year |
| **Total raw (US subset, 48 vars × 28 years)** | **~16 GB** |
| Pixel values (monthly, parquet) | ~5-50 MB per year |
| **Total pixel values** | **~1 GB** |

**Disk space recommendation:** 25 GB available before starting download.

---

## Temporal Resolution Comparison

| Dataset | Resolution | Rows per Pixel per Year |
|---------|------------|-------------------------|
| TerraClimate | Monthly | 12 |
| PRISM | Monthly | 12 |
| WorldClim | Monthly | 12 |
| **ERA5** | **Monthly** | **12** |

All datasets are monthly at the same temporal granularity; ERA5's value-add is its broader variable set.

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

ERA5 provides 48 variables across 8 categories.

### Derivable from downloaded variables (do not re-download)

```r
# Wind speed (m/s) — from U/V components
wind_speed_10m  <- sqrt(u10^2  + v10^2)
wind_speed_100m <- sqrt(u100^2 + v100^2)

# Wind direction (degrees from north, meteorological: direction the wind is FROM)
wind_dir_10m  <- (atan2(-u10,  -v10)  * 180/pi + 360) %% 360
wind_dir_100m <- (atan2(-u100, -v100) * 180/pi + 360) %% 360

# Vapor pressure deficit (hPa) — Magnus formula; multiply by 0.1 for kPa
es <- function(T) 6.112 * exp(17.67 * T / (T + 243.5))  # sat. VP at temp T (°C)
vpd <- es(t2m) - es(d2m)   # es(d2m) = actual vapor pressure

# Relative humidity (%)
rh <- 100 * es(d2m) / es(t2m)

# Large-scale (stratiform) precipitation (mm)
lsp <- tp - cp

# Subsurface (drainage) runoff (mm)
ssro <- ro - sro

# Diffuse solar radiation at surface (MJ/m²)
diffuse_rad <- ssrd - fdir

# Physical snow depth (m) — sd is water equivalent, rsn is density
physical_snow_depth <- sd * 1000 / rsn

# Net radiation (MJ/m²)
net_rad <- ssr + str
```

### Temperature (9 variables)
- `t2m`: 2-meter air temperature (°C) — screen-level air temp; most-used surface temperature
- `d2m`: 2-meter dewpoint (°C) — air must cool to this to reach saturation; basis for VPD and RH
- `skt`: Skin temperature (°C) — radiative surface temperature (top of soil or snow); differs from t2m under calm/clear conditions
- `mn2t`: Minimum 2m temperature (°C) — minimum over the preceding 12-hour post-processing window; approximates daily low
- `mx2t`: Maximum 2m temperature (°C) — maximum over the preceding 12-hour window; approximates daily high
- `stl1`: Soil temperature layer 1 (°C) — mean temp of 0–7cm layer
- `stl2`: Soil temperature layer 2 (°C) — mean temp of 7–28cm layer
- `stl3`: Soil temperature layer 3 (°C) — mean temp of 28–100cm layer
- `stl4`: Soil temperature layer 4 (°C) — mean temp of 100–289cm layer; reflects deep geothermal baseline

### Precipitation (3 variables) — daily accumulations, sum across days
- `tp`: Total precipitation (mm) — all precip (rain + snow) reaching the surface
- `cp`: Convective precipitation (mm) — from convective events; large-scale = tp − cp
- `sf`: Snowfall (mm water equivalent) — solid precipitation

### Snow (4 variables)
- `sd`: Snow depth (m water equivalent) — total snowpack as liquid water equivalent; NOT physical depth
- `snowc`: Snow cover fraction (0–1) — fraction of grid cell covered by snow
- `smlt`: Snowmelt (mm) — water released by melting snowpack; daily accumulation
- `rsn`: Snow density (kg/m³) — use to convert sd to physical depth: `sd * 1000 / rsn`

### Soil Moisture (4 variables)
- `swvl1`: Volumetric soil water layer 1 (m³/m³) — water content of 0–7cm layer
- `swvl2`: Volumetric soil water layer 2 (m³/m³) — water content of 7–28cm layer
- `swvl3`: Volumetric soil water layer 3 (m³/m³) — water content of 28–100cm layer; root zone
- `swvl4`: Volumetric soil water layer 4 (m³/m³) — water content of 100–289cm layer; deep drainage zone

### Hydrology (4 variables) — daily accumulations, sum across days
- `e`: Total evaporation (mm) — water evaporated from surface; **negative when evaporating** (ECMWF convention; ×−1 for positive-upward)
- `pev`: Potential evaporation (mm) — max evaporation given available energy; **negative** (same convention as e)
- `ro`: Total runoff (mm) — water flowing off the grid cell (surface + subsurface)
- `sro`: Surface runoff (mm) — overland flow only; subsurface = ro − sro

### Radiation and Energy (8 variables) — daily accumulations, sum across days
- `ssrd`: Surface solar radiation downwards (MJ/m²) — total (direct + diffuse) downward shortwave
- `fdir`: Total sky direct solar radiation at surface (MJ/m²) — direct-beam only; diffuse = ssrd − fdir
- `ssr`: Surface net solar radiation (MJ/m²) — net shortwave (ssrd minus reflected)
- `strd`: Surface thermal radiation downwards (MJ/m²) — downward longwave from atmosphere
- `str`: Surface net thermal radiation (MJ/m²) — net longwave (downward minus upward emission)
- `sshf`: Surface sensible heat flux (MJ/m²) — turbulent heat exchange; **positive = atmosphere heating surface** (downward); typically negative during daytime
- `slhf`: Surface latent heat flux (MJ/m²) — energy used for evaporation; **negative when evaporating** (same ECMWF sign convention as sshf and e)
- `fal`: Forecast albedo (fraction 0–1) — surface shortwave reflectance; rises under snow cover

### Wind and Pressure (7 variables)
- `u10`: 10m U-component of wind (m/s) — east-west; positive = eastward (westerly wind)
- `v10`: 10m V-component of wind (m/s) — north-south; positive = northward (southerly wind)
- `i10fg`: 10m wind gust (m/s) — peak gust since previous post-processing step
- `u100`: 100m U-component of wind (m/s) — east-west at 100m; relevant for windthrow and fire spotting
- `v100`: 100m V-component of wind (m/s) — north-south at 100m
- `sp`: Surface pressure (hPa) — pressure at surface elevation; varies with terrain
- `msl`: Mean sea level pressure (hPa) — pressure corrected to sea level; use for synoptic patterns

### Cloud Cover (4 variables)
- `tcc`: Total cloud cover (fraction 0–1) — fraction of sky covered by cloud at any level
- `lcc`: Low cloud cover (fraction 0–1) — below ~800 hPa (stratus, fog); most affects surface radiation
- `mcc`: Medium cloud cover (fraction 0–1) — ~400–800 hPa
- `hcc`: High cloud cover (fraction 0–1) — above ~400 hPa (cirrus)

### Atmosphere and Vegetation (9 variables)
- `tcwv`: Total column water vapour (kg/m²) — precipitable water in the atmospheric column
- `cape`: Convective available potential energy (J/kg) — atmospheric instability; high = thunderstorm potential
- `blh`: Boundary layer height (m) — depth of the turbulently-mixed surface layer; affects fire behavior and dispersal
- `lai_hv`: Leaf area index, high vegetation (m²/m²) — one-sided leaf area per ground area for trees/shrubs
- `lai_lv`: Leaf area index, low vegetation (m²/m²) — leaf area for grasses/crops

---

## Decisions Log

| Decision | Rationale | Date |
|----------|-----------|------|
| CDS API download (not GEE) | Full ERA5 single levels not on GEE; ERA5-Land has fewer variables | 2026-02 |
| Monthly resolution | Consistent with other datasets; 48-variable set is ERA5's differentiator vs. daily granularity | 2026-02 |
| 48-variable set | Maximize coverage; exclude only variables derivable from downloaded vars (wind speed/direction, VPD, RH, lsp, ssro, diffuse rad, physical snow depth) | 2026-02 |
| Area subsetting [72, -180, 17, -64] | 80% download size reduction; covers all US observations | 2026-02 |
| Additive extraction (column check) | 03_extract_era5.R checks existing parquet columns; re-extracts only if new variables added to config | 2026-02 |
| Same pixel decomposition as other datasets | Proven efficient pattern | 2026-02 |

---

## Workflow Execution

```r
# 1. Download NetCDFs via CDS API (one-time, ~16 GB, hours)
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
**Cause:** ~16 GB raw NetCDFs + ~1 GB pixel values
**Solution:**
- Check 25 GB free before starting download
- Raw NetCDFs can be deleted after pixel values are written if storage is tight

### Missing variables in NetCDF
**Cause:** Variable name mismatch
**Solution:** Verify `era5_name` in config.yaml matches CDS API parameter names (see [CDS documentation](https://cds.climate.copernicus.eu/cdsapp#!/dataset/reanalysis-era5-single-levels))

### Coarse resolution concerns
**Expected:** 28km pixels are large relative to small IDS observations.
**Implication:** Limited within-polygon variation. Use PRISM or TerraClimate for finer spatial detail.
