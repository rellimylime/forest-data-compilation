# Analysis products

## Canonical data

| Product | Grain | Producer |
|---|---|---|
| `data/processed/fia_remeasurement_components.parquet` | FIA plot visit | 00 |
| `data/processed/condition_visit_cwm.parquet` | condition x visit x life stage | 01 |
| `data/processed/stable_condition_intervals.parquet` | condition x official PREV interval | 01 |
| `data/processed/stable_condition_cwm_change.parquet` | interval x life stage | 01 |
| `data/processed/interval_agent_mortality.parquet` | condition x official PREV interval | 02 |
| `data/processed/history_cumulative_mortality.parquet` | complete condition history | 04 |
| `data/processed/lifestage_model_base.parquet` | history x life stage | 04 |
| `data/processed/history_site_cwd.parquet` | complete condition history | 06 |
| `data/processed/lifestage_model_data.parquet` | history x life stage | 06 |
| `data/processed/pooled_condition_visit_cwm.parquet` | condition x visit | 07 |
| `data/processed/pooled_model_data.parquet` | complete condition history | 07 |

`data/intermediate/` contains compact handoffs between adjacent scripts.
`data/cache/terraclimate_site_cwd/` is the reproducible extraction cache used by
script 06.

## QA

QA summaries live in `qa/outputs/<producer>/`, where `<producer>` begins with
the numbered script that creates the result. `qa/qa_products.csv` records every
QA output, its producer, and whether it is required. Run
`qa/scripts/validate_qa_products.R --require-outputs` after rebuilding the
analysis to verify the complete QA chain. Large row-level copies are not
retained in QA.

## Model runs

Each run has one directory under `results/model_runs/`. A run directory contains
its manifest, formulas, coefficients, fit statistics, sample flow, sjPlot model
tables, figures, and self-contained HTML report. The run index explains the
model count and variables without requiring the directory name to be decoded.
