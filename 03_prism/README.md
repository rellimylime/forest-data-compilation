# PRISM — 800m Pixel-Level Climate Extraction

**Source:** PRISM Climate Group, Oregon State University
**URL:** https://prism.oregonstate.edu/
**Access:** https://services.nacse.org/prism/data/get/us/800m/{variable}/{YYYYMM}

High-resolution (800m) gridded climate data for the contiguous United States. Downloaded and extracted at IDS observation locations via direct web service — no Google Earth Engine account required.

| | |
|---|---|
| **Product** | AN81m (monthly 800m normals) |
| **Spatial resolution** | ~800m (~30 arc-seconds) |
| **Temporal resolution** | Monthly (all 12 months per year) |
| **Temporal coverage** | 1981–present (IDS extraction: 1997–2024) |
| **Spatial coverage** | **CONUS only** — Alaska and Hawaii excluded |
| **Variables** | 7 (temperature, precipitation, vapor pressure deficit) |

**Citation:** PRISM Climate Group, Oregon State University. https://prism.oregonstate.edu

---

## Directory Structure

```
03_prism/
├── README.md               ← This file: overview and quick-start
├── WORKFLOW.md             ← Technical reference: architecture, script details, design decisions
└── data/
    ├── raw/            ← gitignored; reference raster (cached after step 1)
    └── processed/
        ├── pixel_maps/     ← gitignored; one parquet per IDS layer
        └── pixel_values/   ← gitignored; one parquet per year (1997–2024)
```

> No `lookups/` or `docs/` — PRISM has no dataset-specific lookup tables, and the shared architecture is documented in `docs/ARCHITECTURE.md`.

---

## Quick Start

Run steps in order from the repo root. Steps 1–2 are PRISM-specific; step 3 uses a shared script.

```bash
Rscript 03_prism/scripts/01_build_pixel_maps.R      # Map CONUS IDS obs to ~800m pixels
Rscript 03_prism/scripts/02_extract_prism.R         # Stream-download, extract, discard monthly grids
Rscript scripts/build_climate_summaries.R prism     # Area-weighted summaries per observation
```

**Note:** Step 2 streams data from the web service and deletes each downloaded file immediately after extraction — raw grids never accumulate on disk.

**Prerequisite:** `01_ids/` must be processed first.

---

## Key Outputs

| Output | Location | Description |
|--------|----------|-------------|
| `damage_areas_pixel_map.parquet` | `data/processed/pixel_maps/` | CONUS IDS observations → overlapping 800m PRISM pixels |
| `damage_points_pixel_map.parquet` | `data/processed/pixel_maps/` | Same for damage points |
| `surveyed_areas_pixel_map.parquet` | `data/processed/pixel_maps/` | Same for surveyed areas |
| `prism_{year}.parquet` | `data/processed/pixel_values/` | Monthly climate values per unique pixel (1997–2024) |
| `{variable}.parquet` | `processed/climate/prism/damage_areas_summaries/` | Area-weighted observation-level summaries, one file per variable |

**Summary columns:** `DAMAGE_AREA_ID`, `calendar_year`, `calendar_month`, `water_year`, `water_year_month`, `variable`, `weighted_mean`, `value_min`, `value_max`, `n_pixels`, `n_pixels_with_data`, `sum_coverage_fraction`

---

## Variables Extracted (7 total)

All values are delivered by the web service in physical units — no scale factors needed.

| Group | Variables |
|-------|-----------|
| Temperature | `tmean` (mean °C), `tmin` (min °C), `tmax` (max °C), `tdmean` (dew point °C) |
| Precipitation | `ppt` (total mm) |
| Vapor pressure | `vpdmin` (min hPa), `vpdmax` (max hPa) |

---

## CONUS-Only Coverage

Alaska (Region 10) and Hawaii observations are excluded from PRISM extraction (~100k observations). For those locations, use TerraClimate or WorldClim instead.

| PRISM-Included Regions | PRISM-Excluded Regions |
|------------------------|------------------------|
| R1 Northern, R2 Rocky Mountain | R10 Alaska |
| R3 Southwestern, R4 Intermountain | R5-HI Hawaii |
| R5 Pacific SW (CA), R6 Pacific NW | |
| R8 Southern, R9 Eastern | |
