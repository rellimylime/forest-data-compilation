# CWM sensitivity products

These products answer targeted method questions without replacing the canonical condition-level analysis in `09_analysis/data/processed/`. The adult-tree weighting sensitivity includes a paired model comparison; the understory diagnostic does not fit models.

## Adult-tree weighting

Producer:

```powershell
Rscript 09_analysis/scripts/sensitivities/01_build_tree_weighting_cwm.R
Rscript 09_analysis/scripts/sensitivities/03_fit_tree_weighting_models.R
```

Output directory: `09_analysis/data/sensitivity/tree_weighting/`

The producer holds the eligible condition visits, stable-condition intervals, adult-tree cohort, and species niches fixed. It compares:

- individual abundance, using the repository aggregation `n_trees_tpa` from FIA expansion factors; and
- basal area, using repository-calculated `ba_per_acre = 0.005454 * DIA^2 * TPA_UNADJ`.

The principal handoff files are:

- `tree_condition_visit_cwm_abundance.parquet`
- `tree_condition_visit_cwm_basal_area.parquet`
- `tree_stable_condition_cwm_change_abundance.parquet`
- `tree_stable_condition_cwm_change_basal_area.parquet`
- `tree_condition_visit_weighting_comparison.parquet`
- `tree_stable_condition_weighting_comparison.parquet`
- `tree_weighting_summary.csv`
- `tree_weighting_history_model_data.parquet`
- `tree_weighting_model_coefficients.csv`
- `tree_weighting_model_fit.csv`
- `tree_weighting_model_sample_flow.csv`
- `tree_weighting_model_comparison.csv`

Every product states its weighting basis, input column, and units. The paired comparison files calculate basal-area minus abundance differences; use these for the direct comparison requested by Joan. QA is written to `09_analysis/qa/outputs/tree_weighting/`.

The model comparison is limited to the adult-tree group, because basal area is the requested alternative weight for trees. For each climate response, both weighting models use the same complete-case histories, predictors, and HC1 standard errors clustered by stable plot. This isolates the effect of changing the CWM weight from the effect of changing the analytical sample.

## Understory diagnostic pilot

Producer:

```powershell
Rscript 09_analysis/scripts/sensitivities/02_build_understory_cwm_diagnostic.R
```

Output directory: `09_analysis/data/sensitivity/understory_cwm_diagnostic/`

The pilot uses P2VEG cover and reports separate combined, shrub, forb, graminoid, and tree-layer summaries. For each climate indicator it retains the cover-weighted numerator, total recorded cover, species-level cover, niche-covered cover, CWM, niche-cover fraction, and survey support.

Unavailable values receive explicit `NU_*` statuses rather than numeric zero. The pilot distinguishes no structure survey, no group cover, no matching species records, no positive or species-level cover, and no usable niche cover. It reports 50%, 70%, 80%, and 90% coverage scenarios but deliberately does not select a final cutoff.

The principal handoff files are:

- `understory_condition_cwm_diagnostic.parquet`
- `understory_stable_condition_change_diagnostic.parquet`
- `understory_status_summary.csv`
- `coverage_threshold_summary.csv`

The stable-condition file is diagnostic: an interval CWM difference is calculated only when both endpoint values are available. It should not enter the active models until protocol comparability and a coverage rule are scientifically approved. QA is written to `09_analysis/qa/outputs/understory_cwm_diagnostic/`.
