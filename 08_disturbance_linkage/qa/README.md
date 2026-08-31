# Disturbance-Linkage QA

The `scripts/` tree is organized by evidence source. Generated QA results are
kept in `qa/outputs/`, while validation code is grouped the same way under
`qa/scripts/`. Smoke-test artifacts remain isolated in `qa/smoke/`.

[`qa_products.csv`](qa_products.csv) is the output-to-producer map. The two old
endpoint-linkage and MTBS-severity development products are not part of the
supported workflow.

```bash
Rscript --vanilla 08_disturbance_linkage/qa/scripts/fia/01_validate_survey_intervals.R
Rscript --vanilla 08_disturbance_linkage/qa/scripts/fia/03_validate_damage_agent_preparation.R
Rscript --vanilla scripts/run_tests.R 08_disturbance_linkage
```
