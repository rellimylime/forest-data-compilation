# QC and Validation

**Navigation:** [Repo Home](../README.md) | [Docs Hub](README.md) | [Setup](../scripts/SETUP.md) | [Reproduce](REPRODUCE.md) | [Pipeline Map](PIPELINE_MAP.md) | [Data Products](DATA_PRODUCTS.md)

This repository relies on manual diagnostics and inspection scripts rather than a formal automated test suite. This page shows what is covered, what is optional, and where the main validation gaps still are.

## IDS

IDS has the strongest QC coverage in the repo.

| Script | When to run | What it checks |
|---|---|---|
| [validate_ids.R](../01_ids/scripts/qc/validate_ids.R) | After `03_clean_ids.R` | Field structure, missing data, cleaning actions, geometry validity, and merge readiness |
| [explore_ids_coverage.R](../01_ids/scripts/qc/explore_ids_coverage.R) | After `01_download_ids.R` | Column availability by era, value distributions, and regional temporal coverage |

Related docs:

- [01_ids/scripts/qc/README.md](../01_ids/scripts/qc/README.md)
- [01_ids/WORKFLOW.md](../01_ids/WORKFLOW.md)

## Climate workstreams

There is no dedicated QC script for PRISM or WorldClim.

Current checks are mainly documented in cleaning logs and exploratory notes:

| Location | What it covers |
|---|---|
| [02_terraclimate/cleaning_log.md](../02_terraclimate/cleaning_log.md) | Scale factors, coastal NoData, degenerate geometries |
| [01_ids/cleaning_log.md](../01_ids/cleaning_log.md) | IDS-side issues that affect downstream climate joins |
| [02_terraclimate/scripts/explore/00_explore_terraclimate.R](../02_terraclimate/scripts/explore/00_explore_terraclimate.R) | Small-sample GEE sanity check before full extraction |

## FIA

FIA does not currently have a standalone QC script. The closest validation step is:

- [02_inspect_fia.R](../05_fia/scripts/02_inspect_fia.R), which checks schema availability and builds lookup parquets before the heavy extraction steps.

See also:

- [05_fia/README.md](../05_fia/README.md)
- [05_fia/WORKFLOW.md](../05_fia/WORKFLOW.md)

## What Is Not Covered

| Gap | Notes |
|---|---|
| Climate summary outputs | No automated checks for completeness, value ranges, or expected row counts in final summary parquets |
| PRISM and WorldClim QC | No dedicated validation or exploratory scripts beyond workflow notes |
| FIA summary calculations | No automated checks for plot-level metric correctness |
| Cross-dataset consistency | No automated comparison of TerraClimate, PRISM, and WorldClim outputs |

## Practical Review Notes

If you want a quick manual validation pass before a review:

1. Run the IDS QC scripts after the IDS foundation pipeline.
2. Spot-check one climate workstream against its `WORKFLOW.md` output descriptions.
3. Confirm expected output files using [Data Products](DATA_PRODUCTS.md).
4. Use the demo scripts in [scripts/demos/](../scripts/demos/) for sanity checks on completed outputs.

## See also

- [Docs Hub](README.md)
- [Reproduce](REPRODUCE.md)
- [IDS technical workflow](../01_ids/WORKFLOW.md)
- [Data products](DATA_PRODUCTS.md)
