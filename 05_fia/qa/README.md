# FIA QA

Validation code lives in `qa/scripts/`; generated checks live in `qa/outputs/`.
The output directory is ignored except for `.gitkeep` because every result is
reproducible from a tracked producer.

| Producer | Generated QA results |
|---|---|
| `scripts/foundations/02_build_forested_condition_foundation.R` | `forested_condition_*.csv` |
| `scripts/reference/02_audit_tree_cn.py` | `fia_tree_cn_*.csv` |
| `qa/scripts/validate_disturbance_classification.R` | Console validation |
| `qa/scripts/validate_seedling_products.R` | Console validation |
| `qa/scripts/validate_tree_life_stage_products.R` | Console validation |

Run the tracked test suite after rebuilding FIA products:

```bash
Rscript scripts/run_tests.R 05_fia
```
