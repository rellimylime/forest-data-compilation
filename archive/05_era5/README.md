# ERA5 - Archived Monthly Climate Extraction Reference

**Navigation:** [Repo Home](../../README.md) | [Docs Hub](../../docs/README.md) | [Data Products](../../docs/DATA_PRODUCTS.md) | [Archive Workflow](WORKFLOW.md) | [Scripts](scripts/) | [Legacy Notes](README.txt)

## Status

`archive/05_era5/` is an archived reference workflow. It documents how ERA5 monthly reanalysis data were downloaded, mapped to IDS pixels, and extracted into yearly parquet files, but it is not part of the active production pipeline described in [docs/REPRODUCE.md](../../docs/REPRODUCE.md).

## What this archive contains

| Item | Value |
|---|---|
| Source | Copernicus Climate Data Store ERA5 monthly means |
| Coverage | Global |
| Resolution | About 28 km |
| IDS extraction years | 1997-2024 |
| Variable count | 48 |
| Current role | Archived reference only |

## Key Paths

| Path | What belongs here |
|---|---|
| `archive/05_era5/data/metadata/` | Variable metadata exports and review tables |
| `archive/05_era5/data/raw/_batch_tmp/` | Temporary download chunks |
| `archive/05_era5/data/raw/<variable>/` | Monthly NetCDF files named `<variable>_{year}.nc` |
| `archive/05_era5/data/processed/pixel_maps/` | IDS feature-to-pixel crosswalks |
| `archive/05_era5/data/processed/pixel_values/` | Yearly ERA5 parquet extracts named `era5_{year}.parquet` |

## Script Order

| Step | Script | Output |
|---|---|---|
| 1 | [01_download_era5.R](scripts/01_download_era5.R) | Raw ERA5 NetCDF cache under `data/raw/` |
| 2 | [02_build_pixel_maps.R](scripts/02_build_pixel_maps.R) | Pixel maps under `data/processed/pixel_maps/` |
| 3 | [03_extract_era5.R](scripts/03_extract_era5.R) | Yearly parquet files under `data/processed/pixel_values/` |

## Important Notes

- The archive stops at yearly pixel-value extracts. There is no maintained `processed/climate/era5/` summary tree in the active repo.
- Use [WORKFLOW.md](WORKFLOW.md) and [README.txt](README.txt) if you need the original download decisions, variable notes, or CDS setup details.
- A minimal raw/processed skeleton is preserved in git with `.gitkeep` placeholders; the per-variable raw cache directories are created by the download script when needed.

## See also

- [Archive Workflow](WORKFLOW.md)
- [Legacy Notes](README.txt)
- [Data Products](../../docs/DATA_PRODUCTS.md)
