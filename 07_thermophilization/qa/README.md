# Thermophilization QA

Production scripts write compact diagnostics to `qa/outputs/`. Validation and
plotting scripts live in `qa/scripts/`; diagnostic figures live in
`qa/figures/`. Smoke-test artifacts remain isolated in `qa/smoke/`.

[`qa_products.csv`](qa_products.csv) maps each generated result family to its
tracked producer. Generated outputs are not committed.

```bash
Rscript scripts/run_tests.R 07_thermophilization
Rscript 07_thermophilization/qa/scripts/01_validate_thermophilization_products.R
```
