# PRISM Climate Data: Technical Reference

**For quick-start guide and usage examples, see README.txt**

This document covers PRISM-specific technical details. For the shared pixel decomposition architecture, workflow steps, and data schemas, see **[`docs/ARCHITECTURE.md`](../docs/ARCHITECTURE.md)**.

---

## Status
- [ ] Build pixel maps (01_build_pixel_maps.R)
- [ ] Extract monthly pixel values (02_extract_prism.R)
- [ ] Build observation summaries (scripts/build_climate_summaries.R prism)

---

## Dataset Overview

**Source:** [PRISM Climate Group, Oregon State University](https://prism.oregonstate.edu/)
**Resolution:** ~800m (0.04166° / 1/24°)
**Coverage:** Contiguous United States only (CONUS)
**Temporal Resolution:** Monthly, 1981-present
**Variables:** 6 climate variables (tmin, tmean, tmax, ppt, tdmean, vpdmin)
**Access Method:** Google Earth Engine (OREGONSTATE/PRISM/AN81m)

**Key Differences from TerraClimate:**
- Much higher spatial resolution (800m vs 4km) — more pixels per observation
- CONUS only — Alaska (R10) and Hawaii observations excluded
- Fewer variables (6 vs 14)
- Different interpolation methodology (station-based)

---

## PRISM-Specific Parameters

### config.yaml Settings

```yaml
prism:
  gee_asset: "OREGONSTATE/PRISM/AN81m"
  gee_scale: 800  # meters
  variables:
    ppt:
      scale: 1
      units: "mm"
    tmean:
      scale: 1
      units: "deg C"
    tmin:
      scale: 1
      units: "deg C"
    tmax:
      scale: 1
      units: "deg C"
    tdmean:
      scale: 1
      units: "deg C"
    vpdmin:
      scale: 0.01
      units: "hPa"
```

---

## Script Details

### 01_build_pixel_maps.R
**PRISM-Specific Behavior:**
- Filters IDS observations to CONUS only (regions 1-6, 8, 9)
- Excludes Region 10 (Alaska) and Hawaii observations
- Uses 800m reference raster from GEE
- Higher resolution produces 5-25x more pixels per observation than TerraClimate

**Expected Output:**
- CONUS damage_areas: ~4.2M observations → ~50-100M pixel mappings
- Extraction time will be significantly longer than TerraClimate

---

### 02_extract_prism.R
**PRISM-Specific Behavior:**
- Extracts from GEE ImageCollection: OREGONSTATE/PRISM/AN81m
- Temporal range: 1981-present (earlier than TerraClimate)
- No scale factors needed for tmin/tmax/tmean/ppt (already in physical units)
- vpdmin requires 0.01 scale factor

**Performance Notes:**
- More unique pixels than TerraClimate due to finer resolution
- Consider larger batch sizes (e.g., 5000) for GEE efficiency
- Monthly stacking approach same as TerraClimate (12x speedup)

---

## Resolution Comparison

| Dataset | Resolution | Pixels per 10 km² IDS Polygon |
|---------|------------|-------------------------------|
| **PRISM** | **800m** | **~156 pixels** |
| TerraClimate | 4km | ~6 pixels |
| WorldClim | 4.5km | ~5 pixels |
| ERA5 | 28km | 1 pixel |

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
- Region 10 (Alaska) — not covered by PRISM
- Region 5-HI (Hawaii) — not covered by PRISM

**Observations affected:** ~100k observations in Alaska + Hawaii excluded from PRISM extraction

---

## Temporal Coverage

| Period | Coverage | IDS Years Matched |
|--------|----------|-------------------|
| PRISM data | 1981-present | All IDS years (1997-2024) |
| TerraClimate | 1958-present | All IDS years |
| WorldClim | 1960-2021 | 1997-2021 (missing 2022-2024) |

PRISM has complete coverage for all IDS survey years.

---

## Decisions Log

| Decision | Rationale | Date |
|----------|-----------|------|
| GEE extraction (not direct download) | Free access via GEE; PRISM direct download requires paid subscription | TBD |
| Exclude Alaska/Hawaii | PRISM coverage is CONUS only | TBD |
| 800m scale parameter | Native PRISM resolution | TBD |
| Same pixel decomposition as TerraClimate | Proven efficient pattern | TBD |

---

## Workflow Execution

```r
# 1. Build pixel maps (CONUS only)
source("03_prism/scripts/01_build_pixel_maps.R")

# 2. Extract climate from GEE
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

### GEE timeout at 800m scale
**Cause:** High resolution increases computation load.
**Solution:** Reduce batch size from default 2500 to 1500-2000 pixels per request.
