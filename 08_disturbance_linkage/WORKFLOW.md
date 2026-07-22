# 08 Disturbance Linkage — Workflow

Technical reference for the linkage pipeline. Overview: [README](README.md).
Configuration: `processed.disturbance_linkage` in `config.yaml`.

## Purpose

Associate external, dated observations with approximate public FIA plot
coordinates while preserving their different meanings. Spatial overlap supports
screening and sensitivity analysis; by itself it does not establish that an
external event affected an FIA condition or caused a measured tree response.

## Inputs

| Input | Source | Provides |
| --- | --- | --- |
| FIA public plot coordinates | `05_fia/.../plot_condition_metadata.parquet` | `stable_plot_id`, `LAT`, `LON` |
| IDS cleaned layers | `01_ids/data/processed/ids_layers_cleaned.gpkg`, layer `damage_areas` | `DCA_CODE`, `SURVEY_YEAR`, `ACRES` |
| IDS agent lookup | `01_ids/lookups/dca_code_lookup.csv` | `DCA_CODE` to common-name label |
| MTBS mosaics | `data/raw/mtbs/`, user-provided | annual thematic burn-severity rasters |

## Steps

1. **Footprints** (`01`): build one 800 m buffer in `EPSG:5070` for each
   `stable_plot_id` with exactly one observed public coordinate pair. Exclude
   groups with multiple pairs rather than assigning an arbitrary coordinate.
2. **MTBS** (`02`): summarize every annual severity mosaic within each footprint.
   Write one row per `stable_plot_id × fire_year` with the dominant class, high-
   severity fraction, and valid/masked pixel counts. Class 6 is a non-processing
   mask and is excluded from severity denominators. Categorical codes are never
   averaged.
3. **IDS** (`03`): read one `SURVEY_YEAR` at a time; clip `damage_areas` to
   footprints; union overlapping polygons by plot/year/`DCA_CODE`; calculate
   actual overlap area and footprint fraction; retain the raw code and label.
   Write one row per `stable_plot_id × survey_year × DCA_CODE`.
4. **Integrate** (`04`): stack MTBS and IDS without converting unlike
   measurements into a generic magnitude. Write one row per
   `stable_plot_id × source × year × source_event_code`, retaining source codes,
   labels, linkage methods, and source-specific measurements.

## Output grains

```text
plot_footprints.gpkg                     stable_plot_id
plot_mtbs_fire_events.parquet            stable_plot_id x fire_year
plot_ids_agent_events.parquet            stable_plot_id x survey_year x DCA_CODE
plot_disturbance_linkage_events.parquet  stable_plot_id x source x year x source_event_code
```

The final table remains long and unmerged so consumers can define explicit survey
intervals and source-specific inclusion rules.

## IDS acreage contract

`ACRES` in the cleaned IDS source describes the entire source polygon. It must not
be summed and called plot-intersecting acreage after a yes/no spatial join.
Script `03` therefore records:

- `overlap_acres`: area of the clipped union inside the plot footprint;
- `footprint_overlap_fraction`: that union divided by footprint area;
- `source_polygon_acres_sum`: the original whole-polygon acreage sum, retained
  only for auditability;
- `n_source_polygons`: number of contributing source polygon records.

## Remaining spatial-method validation

Before a repeated-visit linkage product is used, validate chronological pairs
against official `PREV_PLT_CN`. A future visit-grain contract should keep the
previous and current footprints separately and distinguish matches to both from
one-visit-only matches. Public-coordinate fuzzing/swapping remains an inherent
uncertainty even when a stable public pair is observed.

## Run order

```bash
Rscript 08_disturbance_linkage/scripts/01_build_plot_footprints.R
Rscript 08_disturbance_linkage/scripts/02_extract_mtbs_severity.R   # needs MTBS rasters
Rscript 08_disturbance_linkage/scripts/03_extract_ids_agents.R
Rscript 08_disturbance_linkage/scripts/04_build_plot_disturbance_linkage.R
Rscript 08_disturbance_linkage/qa/01_validate_disturbance_linkage.R
```

## Scale notes

- IDS `damage_areas` contains about 4.4 million polygons; script `03` processes
  one survey year at a time.
- MTBS mosaics are large rasters and remain git-ignored under `data/raw/mtbs/`.
