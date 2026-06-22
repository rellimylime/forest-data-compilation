# Shared Scripts

**Navigation:** [Repo Home](../README.md) | [Docs Hub](../docs/README.md) | [Setup](SETUP.md) | [Reproduce](../docs/REPRODUCE.md)

This directory contains repo-level helpers that are shared across workstreams. It is not a separate analysis module. Production scripts that belong to a single dataset live inside that dataset's folder, such as `05_fia/scripts/` or `06_species_niches/scripts/`.

## What Lives Here

| Path | Purpose |
| --- | --- |
| `00_setup.R` | One-time local setup helper for `renv`, Python/reticulate, and Google Earth Engine configuration. |
| `SETUP.md` | Human-readable environment setup instructions. Start here on a new machine. |
| `build_climate_summaries.R` | Shared IDS climate post-processing script for TerraClimate, PRISM, and WorldClim. |
| `run_tests.R` | Runs the R test suite by module. |
| `utils/` | Shared R helper functions used by multiple workstreams. |
| `demos/` | Optional example analyses that write local demo outputs. |

## `build_climate_summaries.R`

This script does not combine FIA, species niches, or thermophilization data. It only summarizes climate values for IDS damage areas after a climate workstream has already extracted pixel-level values.

Conceptually:

```text
IDS damage-area pixel map + yearly climate pixel values
  -> area-weighted monthly climate summaries by IDS observation
```

Usage:

```bash
Rscript scripts/build_climate_summaries.R terraclimate
Rscript scripts/build_climate_summaries.R prism
Rscript scripts/build_climate_summaries.R worldclim
```

Inputs:

- `02_terraclimate/data/processed/pixel_maps/damage_areas_pixel_map.parquet`
- `03_prism/data/processed/pixel_maps/damage_areas_pixel_map.parquet`
- `04_worldclim/data/processed/pixel_maps/damage_areas_pixel_map.parquet`
- matching yearly pixel-value parquets from the selected climate workstream.

Outputs:

```text
processed/climate/<dataset>/damage_areas_summaries/
```

The root `processed/` directory is local-only and ignored by Git.

## Utilities

| Utility | Purpose |
| --- | --- |
| `utils/load_config.R` | Loads `config.yaml`. |
| `utils/time_utils.R` | Calendar/water-year helpers. |
| `utils/climate_extract.R` | Shared climate extraction helpers. |
| `utils/gee_utils.R` | Google Earth Engine initialization and helper functions. |
| `utils/metadata_utils.R` | Lightweight metadata output helpers. |
| `utils/cds_utils.R` | Climate Data Store helper code retained for workflows that need it. |

## Demos

Scripts in `demos/` are optional examples for checking outputs visually or building quick presentation figures. Their generated files go under `output/`, which is local-only and ignored by Git.

## Tests

Use `run_tests.R` for module-level R tests:

```bash
Rscript scripts/run_tests.R
Rscript scripts/run_tests.R 05_fia
Rscript scripts/run_tests.R --strict
```

See [docs/TESTING.md](../docs/TESTING.md) for the current QA/testing notes.
