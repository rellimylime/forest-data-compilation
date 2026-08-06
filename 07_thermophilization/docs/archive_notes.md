# Thermophilization archive notes

A running record of fields, columns, and approaches that were **removed or replaced** in the thermophilization work, and why. This exists so the live documentation (`README.md`, the internal meeting brief) can describe only what the products contain *now*, without carrying "we used to have X, it was removed" notes inline. When you remove or replace something, move the explanation here and leave the live docs describing only the current state.

Each entry: what it was, why it went away, and what to use instead.

---

## Removed derived disturbance-severity categories

**Removed:** `fire_severity_class`, `plot_disturbance_extent_class` (previously written by `07_thermophilization/scripts/03_build_plot_disturbance_severity.R`).

**What they were:** categorical labels that bucketed continuous FIA crown-fire area coverage into named severity/extent classes (e.g. a "high severity" label applied above some fixed share of the plot).

**Why removed:** the class boundaries were an analyst-chosen label layered on top of the data, not something FIA reports. That hid the decision (what counts as "high severity") inside the build script, where it was easy to miss and hard to change.

**Use instead:** grade high-severity fire directly from the FIA crown-fire area coverage that the class was derived from — `prop_crown_fire` (share of the whole plot) or `forested_prop_crown_fire` (share of forested area), both from `COND.DSTRBCD == 32`. The single cutoff lives in `config.yaml -> processed.thermophilization.high_severity_fire` (`column` + `threshold`). Script 03 writes `is_high_severity_fire` by thresholding that column; until a threshold is set, `is_high_severity_fire` is `NA` for every row. This keeps the raw continuous coverage in the product and moves the one judgement call (the cutoff) to a single, visible, reviewable place.

**Note — not removed:** the condition-level `is_high_severity_proxy` (= `has_crown_fire_condition`) in `fia_condition_disturbance_flags.parquet` is a plain alias of the raw crown-fire code, not a threshold, and remains. It is a different field from the plot-level, threshold-based `is_high_severity_fire`.

---

## Removed pooled all-condition plot CWMs

**Removed:** `plot_year_community_cwm_<layer>.parquet` and `plot_year_climate_change_<layer>.parquet`, with their producers `05_build_plot_year_community_cwm.R` and the original `06_build_plot_year_climate_change.R`.

**What they were:** plot-visit community-weighted means that pooled species abundance across *every* FIA condition class in a visit, and the consecutive-survey change table built on top of them. They set `forest_conditions_only = FALSE`.

**Why removed:** a forest community metric that includes pasture, water, and developed conditions is measuring something other than the forest community. The agreed method is to compute a CWM per condition and weight those over the visit's forested area only.

**Use instead:** `forest_plot_visit_cwm_<layer>.parquet` from `02_build_forest_plot_visit_cwm.R`. The consecutive-survey design was not discarded with its input — it was rebuilt on the corrected CWM as `04_build_visit_interval_change.R`, keeping the `PREV_PLT_CN` remeasurement-link enforcement unchanged. Forest-only values differ from the pooled values even on fully forested visits, because the estimand changed: area-weighting the condition means is not the same as pooling abundance across conditions.

---

## Removed duplicate seedling recruitment CWM

**Removed:** `plot_recruitment_cwm.parquet` and its producer `01_build_plot_recruitment_cwm.R`, plus the QA file `plot_recruitment_cwm_missing_species.csv`.

**What it was:** a condition-level community-weighted mean for the seedling layer only, written before the per-layer condition products existed.

**Why removed:** it was a duplicate. Its output was bit-for-bit identical to the seedling rows of `plot_community_climate_seedlings.parquet` on the shared climate columns — same 489,782 rows, same keys, zero difference in `cwm_temp`, `cwm_cwd`, or `frac_weight_with_niche`. Only the column names differed (`cwm_*` versus `mean_*`, `cwm_weight_total` versus `community_weight_total`). Two scripts computing the same numbers is two things to keep in sync.

**Use instead:** `plot_community_climate_seedlings.parquet`, which additionally carries weighted medians. Column mapping for anything that read the old file: `cwm_<x>` → `mean_<x>`, `cwm_weight_total` → `community_weight_total`, `n_seedling_species_total` → `n_species_total`. The old `seedlings_tpa` column was numerically identical to `cwm_weight_total` for this layer, since seedling TPA *is* the seedling weight; it was dropped rather than duplicated.

---

## Removed shadowed condition-status column in forest aggregation

**Removed:** the `suffixes = c("", "_foundation")` merge in `aggregate_forested_condition_cwm()` (`scripts/utils/forest_analysis.R`).

**What it was:** the forest aggregation joined condition metrics to the FIA condition foundation with a suffix rule. Because the metrics table carries its own `is_forested_condition`, that copy kept the unsuffixed name and won the merge, so the filter deciding which conditions count as forest read the metrics table's flag while the *weights* came from the foundation.

**Why removed:** the two flags agreed on every row in practice, so no shipped value was wrong. But the function asserted the foundation column and then didn't use it, and a stale flag in an input would have silently mislabeled nonforest conditions as forest — a constructed test case returned `n_forested_conditions_with_layer = 2` where the correct answer was 1.

**Use instead:** the function now drops any foundation-owned column from the metrics table before the join, so the foundation is unambiguously authoritative for condition status, area weight, and plot identity. It also counts conditions absent from the foundation and errors if they exceed a tolerance, instead of letting a left join drop them silently.
