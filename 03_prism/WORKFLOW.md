# PRISM Climate Data: Technical Reference

For a quick-start guide and directory overview, see **README.md**.

This document covers PRISM-specific technical details. For the shared pixel decomposition architecture, workflow steps, and data schemas, see **[`docs/ARCHITECTURE.md`](../docs/ARCHITECTURE.md)**.

---

## Status
- [x] Build pixel maps (01_build_pixel_maps.R)
- [x] Extract monthly pixel values (02_extract_prism.R)
- [ ] Build observation summaries (scripts/build_climate_summaries.R prism)

---

## Dataset Overview

**Source:** [PRISM Climate Group, Oregon State University](https://prism.oregonstate.edu/)
**Resolution:** ~800m (0.04166° / 1/24°)
**Coverage:** Contiguous United States only (CONUS)
**Temporal Resolution:** Monthly, 1981-present
**Variables:** 7 climate variables (ppt, tmean, tmin, tmax, tdmean, vpdmin, vpdmax)
**Access Method:** Direct web service (services.nacse.org) - no GEE account required

**Key Differences from TerraClimate:**
- Much higher spatial resolution (800m vs 4km) - more pixels per observation
- CONUS only - Alaska (R10) and Hawaii observations excluded
- Fewer variables (6 vs 14)
- Different interpolation methodology (station-based)

---

## PRISM-Specific Parameters

### config.yaml Settings

```yaml
prism:
  access_method: "Direct web service (services.nacse.org)"
  product: "AN81m (monthly 800m normals)"
  spatial_resolution: "800m (~30 arc-seconds)"
  output_prefix: "prism"
  variables:
    ppt:      { units: "mm" }
    tmean:    { units: "°C" }
    tmin:     { units: "°C" }
    tmax:     { units: "°C" }
    tdmean:   { units: "°C" }
    vpdmin:   { units: "hPa" }
    vpdmax:   { units: "hPa" }
```

All values are delivered in physical units by the web service - no scale factors needed.

---

## Script Details

### 01_build_pixel_maps.R
**PRISM-Specific Behavior:**
- Filters IDS observations to CONUS only (regions 1-6, 8, 9)
- Excludes Region 10 (Alaska) and Hawaii observations
- Downloads one reference raster from web service (ppt Jan 2000) to define the 800m pixel grid
- Higher resolution produces 5-25x more pixels per observation than TerraClimate

**Expected Output:**
- CONUS damage_areas: ~4.2M observations → ~50-100M pixel mappings
- File sizes are larger than TerraClimate due to higher resolution (expected)

---

### 02_extract_prism.R
**PRISM-Specific Behavior:**
- Downloads each month's zip from services.nacse.org, extracts with terra::extract(), deletes immediately
- One parquet per year saved to pixel_values/; safe to interrupt and resume (completed years skipped)
- Temporal range: 1981-present (earlier than TerraClimate); IDS extraction covers 1997-2024
- All values in physical units - no scale factors needed
- 0.5s courtesy delay between downloads; each file downloaded only once (within PRISM's 2/day limit)

**Performance Notes:**
- More unique pixels than TerraClimate due to finer resolution (~25x)
- Actual runtime ~4 hours for 1997-2024 (well under the 20-40h estimate)

---

## Resolution Comparison

| Dataset | Resolution | Pixels per 10 km² IDS Polygon |
|---------|------------|-------------------------------|
| **PRISM** | **800m** | **~156 pixels** |
| TerraClimate | 4km | ~6 pixels |
| WorldClim | 4.5km | ~5 pixels |

**Implication:** PRISM captures much more within-polygon climate variation, but extraction and storage requirements are proportionally larger.

---

## Geographic Coverage

### Included Regions (CONUS)
- Region 1 (Northern)
- Region 2 (Rocky Mountain)
- Region 3 (Southwestern)
- Region 4 (Intermountain)
- Region 5 (Pacific Southwest, California only)
- Region 6 (Pacific Northwest)
- Region 8 (Southern)
- Region 9 (Eastern)

### Excluded Regions (Outside CONUS)
- Region 10 (Alaska) - not covered by PRISM
- Region 5-HI (Hawaii) - not covered by PRISM

**Observations affected:** ~100k observations in Alaska + Hawaii excluded from PRISM extraction

---

## Temporal Coverage

| Period | Coverage | IDS Years Matched |
|--------|----------|-------------------|
| PRISM data | 1981-present | All IDS years (1997-2024) |
| TerraClimate | 1958-present | All IDS years |
| WorldClim | 1950-2024 | All IDS years (1997-2024) |

PRISM has complete coverage for all IDS survey years.

---

## Decisions Log

| Decision | Rationale | Date |
|----------|-----------|------|
| Direct web service (not GEE) | services.nacse.org is freely accessible; download-extract-delete avoids large local storage of raw zips | 2025 |
| Exclude Alaska/Hawaii | PRISM coverage is CONUS only | 2025 |
| Same pixel decomposition as TerraClimate | Proven efficient pattern | 2025 |

---

## Workflow Execution

```r
# 1. Build pixel maps (CONUS only)
source("03_prism/scripts/01_build_pixel_maps.R")

# 2. Extract climate via web service download
source("03_prism/scripts/02_extract_prism.R")

# 3. Build observation summaries (shared script, reads source files directly)
Rscript scripts/build_climate_summaries.R prism
```

For detailed workflow architecture, see [`docs/ARCHITECTURE.md`](../docs/ARCHITECTURE.md).

---

## Troubleshooting

### Missing Alaska/Hawaii data
**Expected:** PRISM does not cover these regions. Observations outside CONUS will have no PRISM summaries.

### Large pixel map file sizes
**Cause:** 800m resolution produces 5-25x more pixel mappings than TerraClimate.
**Solution:** This is expected. Parquet compression keeps file sizes manageable.

### Download failures
**Cause:** Network interruption or PRISM server error.
**Solution:** The script catches errors per-variable and stores NA; failed downloads are logged as WARNings. Re-run the script - completed years are skipped automatically.
