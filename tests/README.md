# Repository Test Suites

This repository now has a **per-processing-directory test suite**:

- `01_ids/tests/testthat/`
- `02_terraclimate/tests/testthat/`
- `03_prism/tests/testthat/`
- `04_worldclim/tests/testthat/`
- `05_fia/tests/testthat/`

Each suite validates the outputs produced by that directory, including:

- expected output files
- required schemas/columns
- key constraints and join sanity checks
- map/extraction integrity checks
- time key consistency (`calendar` vs `water year`)

## How to run

Run all suites (non-strict mode):

```bash
Rscript scripts/run_tests.R
```

Run a subset:

```bash
Rscript scripts/run_tests.R 01_ids 05_fia
```

Run strict mode (missing outputs are failures, not skips):

```bash
Rscript scripts/run_tests.R --strict
```

## Modes

- **Non-strict (default):** missing outputs are `skip` (good for development on partial data).
- **Strict:** missing outputs are `fail` (good for release gates and pre-delivery QA).

Set strict behavior directly via env var if needed:

```bash
STRICT_OUTPUT_CHECKS=true Rscript scripts/run_tests.R
```
