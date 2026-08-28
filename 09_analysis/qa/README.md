# Analysis QA

This directory contains compact checks emitted by the active numbered analysis
scripts. It is organized by the product being checked:

- `intervals/` - official PREV component flow
- `cwm/` - condition eligibility, weighting, pooled coverage, and sampling
- `mortality/` - interval and cumulative risk-set flow and bounds
- `predictors/` - TerraClimate CWD coverage, range, and duration relationship

QA products are summaries, not alternative analysis datasets. Row-level model
inputs live in `../data/processed/`; model tables and figures live inside their
run directory under `../results/model_runs/`.
