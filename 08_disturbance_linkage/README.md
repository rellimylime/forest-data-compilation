# 08 Disturbance Linkage

Links FIA plots to external, dated observations and writes per-plot association
tables keyed on `stable_plot_id`:

- **MTBS** — annual thematic burn-severity mosaics derived from satellite imagery.
- **IDS** — aerial insect/disease survey polygons with retained damage-causal-agent (`DCA_CODE`) observations.

These can feed a future disturbance-aware pass of
[07_thermophilization](../07_thermophilization/). Spatial overlap is an
association with an approximate public FIA coordinate, not proof that the mapped
event affected the measured plot.

## Why this module exists

FIA condition disturbance, MTBS spectral-change classes, and IDS aerial
detections are different observations. This module retains those distinctions
while allowing researchers to ask whether an MTBS pixel class or IDS `DCA_CODE`
polygon occurs within the configured public-coordinate buffer and year.

## Pipeline

| Script | Does | Output |
| --- | --- | --- |
| [`scripts/01_build_plot_footprints.R`](scripts/01_build_plot_footprints.R) | Buffer each unambiguous FIA public coordinate by `buffer_m` in `EPSG:5070`; exclude `stable_plot_id` groups containing multiple coordinate pairs. | `plot_footprints.gpkg` |
| [`scripts/02_extract_mtbs_severity.R`](scripts/02_extract_mtbs_severity.R) | Calculate class proportions from MTBS annual severity mosaics; exclude and report non-processing masks. | `plot_mtbs_fire_events.parquet` |
| [`scripts/03_extract_ids_agents.R`](scripts/03_extract_ids_agents.R) | Clip IDS polygons to footprints, union overlaps by plot/year/`DCA_CODE`, and measure actual overlap. | `plot_ids_agent_events.parquet` |
| [`scripts/04_build_plot_disturbance_linkage.R`](scripts/04_build_plot_disturbance_linkage.R) | Stack MTBS and IDS events while retaining source-specific codes and measurements. | `plot_disturbance_linkage_events.parquet` |
| [`qa/01_validate_disturbance_linkage.R`](qa/01_validate_disturbance_linkage.R) | Validate grains, keys, source-code preservation, masks, and overlap ranges. | `disturbance_linkage_validation_checks.csv` |

The final product has one row per
`stable_plot_id × source × year × source_event_code`. Raw source codes and labels
remain separate, and source-specific measures are not collapsed into a
misleading generic magnitude.

For IDS, `overlap_acres` is the clipped and de-duplicated area inside the FIA
buffer. `source_polygon_acres_sum` preserves the sum of the source polygons'
reported whole-polygon acreage for auditing; it is not plot overlap acreage.
`DCA_CODE` is retained in both the source-specific product and the integrated
event contract.

## Coordinate handling plan

The current stable-plot-grain product excludes groups with multiple public
coordinate pairs instead of silently selecting the first pair. Before
visit-specific or repeated-interval spatial products are exposed, the next
implementation should validate official `PREV_PLT_CN` linkages, retain previous-
and current-visit footprints separately, and report whether an external
observation intersects both visits or only one. One-visit matches are sensitivity
cases, not equivalent evidence.

## Config

Paths, the 800 m buffer, the CRS, and the MTBS class map live under
`processed.disturbance_linkage` in `config.yaml`.

## Inputs

- FIA public plot coordinates: `05_fia/.../plot_condition_metadata.parquet`.
- IDS: `01_ids/data/processed/ids_layers_cleaned.gpkg`, layer `damage_areas`;
  labels come from `01_ids/lookups/dca_code_lookup.csv`.
- MTBS: user-provided annual mosaics under `data/raw/mtbs/`. They are not stored
  in this repository.

## Run

```bash
Rscript 08_disturbance_linkage/scripts/01_build_plot_footprints.R
Rscript 08_disturbance_linkage/scripts/02_extract_mtbs_severity.R   # needs MTBS rasters
Rscript 08_disturbance_linkage/scripts/03_extract_ids_agents.R
Rscript 08_disturbance_linkage/scripts/04_build_plot_disturbance_linkage.R
Rscript 08_disturbance_linkage/qa/01_validate_disturbance_linkage.R
```
