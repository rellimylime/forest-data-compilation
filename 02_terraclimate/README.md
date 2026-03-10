# TerraClimate — Pixel-Level Climate Extraction

**Source:** TerraClimate / Climatology Lab
**URL:** https://www.climatologylab.org/terraclimate.html
**GEE Asset:** IDAHO_EPSCOR/TERRACLIMATE

High-resolution global climate and water balance dataset derived from WorldClim, CRU, and JRA-55 reanalysis. Extracted at IDS observation locations via Google Earth Engine using a pixel decomposition approach.

| | |
|---|---|
| **Spatial resolution** | ~4 km (1/24° ≈ 4,638 m at equator) |
| **Temporal resolution** | Monthly (all 12 months per year) |
| **Temporal coverage** | 1958–present (IDS extraction: 1997–2024) |
| **Spatial coverage** | Global |
| **Variables** | 14 (temperature, precipitation, ET, drought indices) |

**Citation:** Abatzoglou, J.T., S.Z. Dobrowski, S.A. Parks, K.C. Hegewisch (2018). TerraClimate, a high-resolution global dataset of monthly climate and climatic water balance from 1958–2015. *Scientific Data* 5:170191.

---

## Directory Structure

```
02_terraclimate/
├── README.md               ← This file: overview and quick-start
├── WORKFLOW.md             ← Technical reference: architecture, script details, usage examples
├── cleaning_log.md         ← Data quality issues and processing decisions
├── data_dictionary.csv     ← Field definitions for all output tables
├── scripts/
│   ├── 00_explore_terraclimate.R   ← Optional exploratory script (not in pipeline)
│   ├── 01_build_pixel_maps.R       ← Map IDS observations to ~4km TerraClimate pixels
│   └── 02_extract_terraclimate.R   ← Extract monthly values for all unique pixels via GEE
├── lookups/
│   └── damage_areas_pixel_centroids.parquet  ← git-tracked; used by dashboard
└── data/
    ├── raw/            ← gitignored; TerraClimate reference raster (cached by step 1)
    └── processed/
        ├── pixel_maps/     ← gitignored; ~156 MB per layer
        └── pixel_values/   ← gitignored; ~50–100 MB per year (1997–2024)
```

---

## Quick Start

Run steps in order from the repo root. Steps 1–2 are TerraClimate-specific; step 3 uses a shared script.

```bash
Rscript 02_terraclimate/scripts/01_build_pixel_maps.R          # Map IDS obs to ~4km pixels
Rscript 02_terraclimate/scripts/02_extract_terraclimate.R      # Extract monthly values via GEE
Rscript scripts/build_climate_summaries.R terraclimate         # Area-weighted summaries per obs
```

`00_explore_terraclimate.R` is an optional exploratory script run before the pipeline was built — not required for data reproduction.

**Prerequisite:** `01_ids/` must be processed first (steps 1–3 at minimum).

---

## Key Outputs

| Output | Location | Description |
|--------|----------|-------------|
| `damage_areas_pixel_map.parquet` | `data/processed/pixel_maps/` | Each IDS observation → overlapping TerraClimate pixels with coverage fractions |
| `damage_points_pixel_map.parquet` | `data/processed/pixel_maps/` | Same for damage points |
| `surveyed_areas_pixel_map.parquet` | `data/processed/pixel_maps/` | Same for surveyed areas |
| `terraclimate_{year}.parquet` | `data/processed/pixel_values/` | Monthly climate values per unique pixel, wide format (1997–2024) |
| `{variable}.parquet` | `processed/climate/terraclimate/damage_areas_summaries/` | Area-weighted observation-level summaries, one file per variable |

**Summary columns:** `DAMAGE_AREA_ID`, `calendar_year`, `calendar_month`, `water_year`, `water_year_month`, `variable`, `weighted_mean`, `value_min`, `value_max`, `n_pixels`, `n_pixels_with_data`, `sum_coverage_fraction`

---

## Variables Extracted (14 total)

| Group | Variables |
|-------|-----------|
| Temperature | `tmmx` (max °C), `tmmn` (min °C) |
| Precipitation / Water | `pr` (mm), `aet` (mm), `pet` (mm), `def` (mm), `soil` (mm), `swe` (mm), `ro` (mm) |
| Atmospheric | `vap` (kPa), `vpd` (kPa), `srad` (W/m²), `vs` (m/s) |
| Drought index | `pdsi` (unitless) |

See `data_dictionary.csv` for complete field definitions and scale factors.

---

## Lookup Files (git-tracked)

`lookups/damage_areas_pixel_centroids.parquet` — one row per unique TerraClimate pixel overlapping at least one IDS damage area. Used by the project dashboard to display a coverage density map.

| Column | Description |
|--------|-------------|
| `pixel_id` | Unique TerraClimate pixel identifier |
| `x` | Pixel centroid longitude (WGS84) |
| `y` | Pixel centroid latitude (WGS84) |
| `n_damage_areas` | Number of distinct damage areas overlapping this pixel |

Size: ~263,871 rows, 1.7 MB.
