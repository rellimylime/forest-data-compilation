# Point TerraClimate Extraction

This folder contains a standalone point-based TerraClimate extraction that is separate from the numbered FIA, species-niche, and thermophilization workflows.

The input table is [input/all_site_locations.csv](input/all_site_locations.csv). It has one row per site with:

| Column | Meaning |
| --- | --- |
| `site_id` | Site identifier from the source dataset |
| `latitude` | Site latitude in decimal degrees |
| `longitude` | Site longitude in decimal degrees |
| `source` | Source label for the point, currently `FIA` or `ITRDB` |

Run:

```bash
Rscript site_climate/scripts/extract_terraclimate_points.R
```

Main outputs are written to `site_climate/data/processed/`:

| Output | Row Meaning |
| --- | --- |
| `site_pixel_map.parquet` | One row per input site, recording the TerraClimate pixel used for extraction |
| `site_climate.parquet` | One row per site x year x month x variable |

The extraction uses TerraClimate through Google Earth Engine and snaps points to the global 1/24 degree TerraClimate grid before sampling. It extracts monthly `tmmx`, `tmmn`, `pr`, `def`, `pet`, and `aet` from 1958 through the configured end year.

Some points can snap to TerraClimate pixels that return no land climate values, usually because the coordinate falls on an ocean or otherwise masked pixel. Those sites are listed in `site_climate/qa/outputs/site_climate_missing_sites.csv` when present.
