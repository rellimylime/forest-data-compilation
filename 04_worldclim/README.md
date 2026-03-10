# WorldClim — Global Monthly Climate Extraction

**Source:** WorldClim Version 2.1 / UC Davis Geospatial Data
**URL:** https://www.worldclim.org/data/monthlywth.html
**Download:** https://geodata.ucdavis.edu/climate/worldclim/

Global historical monthly weather data interpolated from station observations (CRU TS 4.09). Extracted at IDS observation locations from locally-downloaded GeoTIFF files.

| | |
|---|---|
| **Spatial resolution** | ~4.5 km (2.5 arc-minutes) |
| **Temporal resolution** | Monthly (all 12 months per year) |
| **Temporal coverage** | 1950–2024 (IDS extraction: 1997–2024) |
| **Spatial coverage** | Global land areas |
| **Variables** | 3 (temperature min/max, precipitation) |

**Citation:** Fick, S.E. and R.J. Hijmans (2017). WorldClim 2: new 1km spatial resolution climate surfaces for global land areas. *International Journal of Climatology* 37(12): 4302–4315.

---

## Directory Structure

```
04_worldclim/
├── README.md               ← This file: overview and quick-start
├── WORKFLOW.md             ← Technical reference: architecture, script details, design decisions
└── data/
    ├── raw/            ← gitignored; ~600 MB decade GeoTIFF archives (kept locally)
    │   ├── tmin/       ← monthly .tif files organized by variable
    │   ├── tmax/
    │   └── prec/
    └── processed/
        ├── pixel_maps/     ← gitignored
        └── pixel_values/   ← gitignored; one parquet per year (1997–2024)
```

> No `lookups/` or `docs/` — WorldClim has no dataset-specific lookup tables.

---

## Quick Start

Run steps in order from the repo root. Steps 1–3 are WorldClim-specific; step 4 uses a shared script.

```bash
Rscript 04_worldclim/scripts/01_download_worldclim.R   # Download decade GeoTIFF archives (~600 MB, one-time)
Rscript 04_worldclim/scripts/02_build_pixel_maps.R     # Map IDS observations to ~4.5km pixels
Rscript 04_worldclim/scripts/03_extract_worldclim.R    # Extract monthly values from local GeoTIFFs
Rscript scripts/build_climate_summaries.R worldclim    # Area-weighted summaries per observation
```

Step 1 is a one-time download (~600 MB). Raw GeoTIFFs are kept locally for repeated extraction.

**Prerequisite:** `01_ids/` must be processed first.

---

## Key Outputs

| Output | Location | Description |
|--------|----------|-------------|
| `damage_areas_pixel_map.parquet` | `data/processed/pixel_maps/` | IDS observations → overlapping ~4.5km WorldClim pixels |
| `damage_points_pixel_map.parquet` | `data/processed/pixel_maps/` | Same for damage points |
| `surveyed_areas_pixel_map.parquet` | `data/processed/pixel_maps/` | Same for surveyed areas |
| `worldclim_{year}.parquet` | `data/processed/pixel_values/` | Monthly climate values per unique pixel (1997–2024) |
| `{variable}.parquet` | `processed/climate/worldclim/damage_areas_summaries/` | Area-weighted observation-level summaries, one file per variable |

**Summary columns:** `DAMAGE_AREA_ID`, `calendar_year`, `calendar_month`, `water_year`, `water_year_month`, `variable`, `weighted_mean`, `value_min`, `value_max`, `n_pixels`, `n_pixels_with_data`, `sum_coverage_fraction`

---

## Variables Extracted (3 total)

All values are in physical units — no scale factors needed.

| Variable | Description | Units |
|----------|-------------|-------|
| `tmin` | Monthly minimum temperature | °C |
| `tmax` | Monthly maximum temperature | °C |
| `prec` | Monthly total precipitation | mm |

---

## Data Organization

Raw data is downloaded as decade-based zip archives, each containing 120 individual monthly GeoTIFFs (one band per file):

| Decade | Files | IDS years covered |
|--------|-------|-------------------|
| 1950–1959 through 1980–1989 | 120 each | None (pre-IDS) |
| 1990–1999 | 120 | 1997–1999 |
| 2000–2009, 2010–2019 | 120 each | Full |
| 2020–2024 | 60 | Full |

Files are organized into per-variable subdirectories after extraction (e.g., `data/raw/tmin/`).
