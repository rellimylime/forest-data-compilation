# Thermophilization Analysis

This directory builds the FIA tables needed to ask whether plant communities are shifting toward species associated with warmer or drier climates, and whether those shifts are related to disturbance. The main inputs are:

- FIA species composition tables for seedlings, saplings, and trees from `05_fia/`.
- BIEN/TerraClimate species climate niches from `06_species_niches/`.
- FIA disturbance, treatment, and condition metadata from `05_fia/`.

In this workflow, a **community climate-affinity metric** means: take the species present in a FIA plot visit, join each species to its climate niche, and summarize those species niche values as a community-weighted mean or weighted median. A warmer community-weighted mean does not mean the plot climate itself is warmer; it means the species present are associated with warmer parts of their realized ranges.

The products in this directory answer three connected questions:

1. What is the community-weighted climate affinity of each plot survey year?
2. How does that value change between repeated FIA surveys of the same plot?
3. How much of the plot visit was affected by fire, insects, disease, weather, treatment, or human/harvest disturbance?

## Script Map

| Script | Main purpose | Main output |
| --- | --- | --- |
| `01_build_plot_recruitment_cwm.R` | Build the original condition-level seedling recruitment CWM table. This is useful for recruitment-specific diagnostics and earlier condition-level analyses. | `plot_recruitment_cwm.parquet` |
| `02_build_analysis_cohort.R` | Join seedling recruitment CWMs to condition-level disturbance/control labels and record which FIA conditions pass the first eligibility filters. | `plot_recruitment_analysis_cohort.parquet` |
| `03_build_plot_disturbance_severity.R` | Aggregate condition-level disturbance information to one row per plot visit using `CONDPROP_UNADJ`. This estimates disturbance proportions such as `prop_fire`, `prop_insect`, and `prop_disease`. | `plot_disturbance_severity.parquet` |
| `04_build_plot_community_climate_metrics.R` | Build condition-level weighted mean and weighted median climate-affinity metrics for seedlings, saplings, or trees while keeping FIA conditions separate. | `plot_community_climate_<layer>.parquet` |
| `05_build_plot_year_community_cwm.R` | Build the main plot survey-year community climate-affinity table for seedlings, saplings, or trees. This is the product used to estimate change through time. | `plot_year_community_cwm_<layer>.parquet` |
| `06_build_plot_year_climate_change.R` | Compare consecutive FIA surveys of the same stable plot and calculate absolute and annualized change in community climate affinity. | `plot_year_climate_change_<layer>.parquet` |
| `qa/01_validate_thermophilization_products.R` | Validate file presence, documented row grains, required columns, proportions, niche coverage fields, and rate calculations. | `thermophilization_validation_*.csv` |

For the cross-repo list of products and links to detailed output descriptions, see [Data Products](../docs/DATA_PRODUCTS.md#thermophilization-outputs).

## Current Status

Scripts `01`, `04`, and `05` can use study-area species niches with a flagged global fallback. In default mode, the workflow uses the study-area niche when one exists and uses the global BIEN range niche only when the species is observed in FIA but has no study-area-clipped niche. The fallback fraction is recorded in each CWM product so those rows can be reviewed, filtered, or handled in sensitivity checks.

Current completed products:

- Seedling recruitment climate-affinity CWMs.
- Condition-level disturbance/control analysis cohort.
- Plot-visit disturbance proportions and first-pass severity classes.
- Layer-specific weighted mean and weighted median climate-affinity metrics for seedlings, saplings, and trees.
- Plot survey-year community-weighted climate metrics for seedlings, saplings, and trees.
- Repeated-survey climate-affinity change metrics for seedlings, saplings, and trees.

Remaining work:

- Add P2 vegetation climate-affinity metrics after the P2 vegetation products are finalized.
- Fit and summarize models using repeated-survey climate change metrics and disturbance proportions.

## Current Workflow

1. Build species climate niches in `06_species_niches/`. The preferred primary input for this workflow is:

   ```text
   06_species_niches/data/processed/species_climate_niches_us_study_area.parquet
   ```

   Fallback mode also uses:

   ```text
   06_species_niches/data/processed/species_climate_niches.parquet
   ```

2. Build FIA seedling community-weighted means:

   ```bash
   Rscript 07_thermophilization/scripts/01_build_plot_recruitment_cwm.R
   ```

   This writes:

   ```text
   07_thermophilization/data/processed/plot_recruitment_cwm.parquet
   ```

3. Build the condition-level analysis cohort:

   ```bash
   Rscript 07_thermophilization/scripts/02_build_analysis_cohort.R
   ```

   This writes:

   ```text
   07_thermophilization/data/processed/plot_recruitment_analysis_cohort.parquet
   ```

   The cohort keeps FIA-forested natural-disturbance and control candidates with usable CWMs. Forest status, disturbance, and treatment eligibility are evaluated at the condition level. Whole-plot exclusion flags remain attached as sensitivity warnings rather than removing a clean condition because another condition on the same plot differs. Conditions below 95% niche coverage are retained with `meets_niche_coverage_threshold = FALSE` so the threshold can be tested rather than silently imposed.

4. Build plot-level disturbance proportions:

   ```bash
   Rscript 07_thermophilization/scripts/03_build_plot_disturbance_severity.R
   ```

   This writes:

   ```text
   07_thermophilization/data/processed/plot_disturbance_severity.parquet
   ```

   This product aggregates condition-level FIA disturbance classes to the plot-visit level using `CONDPROP_UNADJ`. It is the first-pass disturbance-intensity table for questions such as what proportion of a plot visit is affected by fire, insects, disease, or weather.

5. Build layer-specific community climate metrics:

   ```bash
   Rscript 07_thermophilization/scripts/04_build_plot_community_climate_metrics.R --layer=seedlings
   Rscript 07_thermophilization/scripts/04_build_plot_community_climate_metrics.R --layer=saplings
   Rscript 07_thermophilization/scripts/04_build_plot_community_climate_metrics.R --layer=trees
   ```

   These write:

   ```text
   07_thermophilization/data/processed/plot_community_climate_seedlings.parquet
   07_thermophilization/data/processed/plot_community_climate_saplings.parquet
   07_thermophilization/data/processed/plot_community_climate_trees.parquet
   ```

   These products keep life stages separate. Seedlings are the direct recruitment layer, saplings represent established recent regeneration, and trees represent the longer-lived overstory or live-tree community.

6. Build plot survey-year community-weighted climate metrics:

   ```bash
   Rscript 07_thermophilization/scripts/05_build_plot_year_community_cwm.R --layer=seedlings
   Rscript 07_thermophilization/scripts/05_build_plot_year_community_cwm.R --layer=saplings
   Rscript 07_thermophilization/scripts/05_build_plot_year_community_cwm.R --layer=trees
   ```

   These write:

   ```text
   07_thermophilization/data/processed/plot_year_community_cwm_seedlings.parquet
   07_thermophilization/data/processed/plot_year_community_cwm_saplings.parquet
   07_thermophilization/data/processed/plot_year_community_cwm_trees.parquet
   ```

   These are the plot-level survey-year products. They collapse all selected FIA conditions and subplots within a plot visit to one community-weighted climate-affinity row per layer and inventory year. By default, condition weights are multiplied by `CONDPROP_UNADJ` so smaller mapped conditions contribute less than larger mapped conditions within the same plot visit.

7. Build repeated-survey climate-affinity change metrics:

   ```bash
   Rscript 07_thermophilization/scripts/06_build_plot_year_climate_change.R --layer=seedlings
   Rscript 07_thermophilization/scripts/06_build_plot_year_climate_change.R --layer=saplings
   Rscript 07_thermophilization/scripts/06_build_plot_year_climate_change.R --layer=trees
   ```

   These write:

   ```text
   07_thermophilization/data/processed/plot_year_climate_change_seedlings.parquet
   07_thermophilization/data/processed/plot_year_climate_change_saplings.parquet
   07_thermophilization/data/processed/plot_year_climate_change_trees.parquet
   ```

   These compare consecutive survey years within the same stable FIA plot. Each row contains the previous survey value, current survey value, absolute change, and annualized rate of change for each climate-affinity metric. Current-survey disturbance proportions from script `03` are joined to each interval.

8. Validate thermophilization products:

   ```bash
   Rscript 07_thermophilization/qa/01_validate_thermophilization_products.R
   ```

   This writes:

   ```text
   07_thermophilization/qa/outputs/thermophilization_validation_checks.csv
   07_thermophilization/qa/outputs/thermophilization_validation_summary.csv
   ```

   This validator checks file presence, documented row grains, required columns, valid proportion ranges, niche-coverage ranges, and repeated-survey rate calculations. Treat this as the structural QA gate before fitting models.

## Script 01 Inputs

- `05_fia/data/processed/summaries/plot_seedling_species.parquet`
- `06_species_niches/data/processed/species_climate_niches_us_study_area.parquet`

The join uses FIA species codes through `species_key`, for example:

```text
SPCD 802 -> fia_spcd:802
```

## Script 01 Outputs

The output grain is:

```text
stable_plot_id x PLT_CN x INVYR x CONDID
```

In FIA terms:

- `stable_plot_id` is the same plot location followed through time.
- `PLT_CN` is one FIA plot visit record.
- `INVYR` is the inventory year for that visit.
- `CONDID` is a mapped forest condition within that plot visit.
- `SUBP` is a subplot/microplot location. Script `01` aggregates subplot-level seedling species records up to the condition level before calculating CWMs.

Main climate-affinity columns:

- `cwm_temp`: recruitment community-weighted annual mean temperature niche.
- `cwm_heat`: recruitment community-weighted warmest-month temperature niche.
- `cwm_cold`: recruitment community-weighted coldest-month temperature niche.
- `cwm_temp_seasonality`: recruitment community-weighted temperature seasonality.
- `cwm_cwd`: recruitment community-weighted annual climate water deficit niche.
- `cwm_peak_cwd`: recruitment community-weighted peak monthly climate water deficit niche.
- `cwm_pr`: recruitment community-weighted annual precipitation niche.
- `cwm_dry_month_pr`: recruitment community-weighted driest-month precipitation niche.

Coverage columns such as `frac_weight_with_niche` should be used to filter or flag plot conditions where many seedlings lack species niche values. Fallback coverage columns, such as `frac_weight_with_global_fallback_niche`, identify how much of the CWM comes from global rather than study-area niche values.

## CWM Formula

For each FIA condition, the script calculates:

```text
CWM_indicator = sum(seedling_weight_i * species_indicator_i) /
                sum(seedling_weight_i)
```

where `i` indexes species in the seedling community after aggregating across subplots within the same `PLT_CN x INVYR x CONDID`.

The default `seedling_weight_i` is `seedlings_tpa`, the expanded seedlings-per-acre value from the FIA seedling species product. Sensitivity checks can use raw counts or presence/absence through the `--weight` argument.

Rows with incomplete niche coverage are retained, not silently dropped. Use:

- `frac_weight_with_niche`
- `frac_weight_with_study_area_niche`
- `frac_weight_with_global_fallback_niche`
- `frac_seedling_species_with_niche`
- `n_seedling_species_total`
- `n_seedling_species_with_niche`
- `niche_scopes_used`

to decide whether a condition is safe for modeling or should be flagged.

Known species-level gaps are documented in:

```text
06_species_niches/qa/outputs/species_niche_gap_ledger.csv
07_thermophilization/qa/outputs/plot_recruitment_cwm_missing_species.csv
```

## Script 02 Inputs And Outputs

Inputs:

- `plot_recruitment_cwm.parquet`: condition-level recruitment CWMs.
- `plot_disturbance_classification.parquet`: natural-disturbance and control definitions at the FIA condition level.
- `plot_exclusion_flags.parquet`: nonforest, human-disturbance, and harvest exclusions at the whole plot-visit level.

Output grain:

```text
stable_plot_id x PLT_CN x INVYR x CONDID
```

The production cohort contains only rows where `analysis_eligible = TRUE`. In script `02`, a condition is eligible when it:

- has a matching condition-level disturbance classification;
- has a usable seedling recruitment CWM;
- is a FIA forested analysis condition;
- belongs to either the natural-disturbance group or the untreated/undisturbed control group.

The script records sequential row counts for those filters in `07_thermophilization/qa/outputs/analysis_cohort_attrition.csv`. It does **not** silently remove rows below the niche-coverage threshold; it retains them with `meets_niche_coverage_threshold = FALSE`.

Important fields retained for later matching and sensitivity analysis include:

- `disturbed_vs_control`
- `disturbance_class` and `disturbance_class_primary`
- `region_east_west`
- `forest_type_group` and `FORTYPCD`
- `is_forest_dominated_plot` for the optional 50%-forested sensitivity filter
- `time_since_disturbance`
- all eight recruitment CWM indicators
- `frac_weight_with_niche`
- `meets_niche_coverage_threshold`
- global-fallback coverage fields
- plot-level exclusion and harvest-agent warning fields

The script writes two compact QA files:

- `analysis_cohort_attrition.csv`: sequential row counts for every eligibility filter.
- `analysis_cohort_summary.csv`: final counts by analysis group, disturbance class, region, and niche coverage.

## Script 03 Inputs And Outputs

Input:

- `plot_disturbance_classification.parquet`: condition-level FIA disturbance/control definitions and condition proportions.

Output grain:

```text
stable_plot_id x PLT_CN x INVYR
```

The output contains plot-visit disturbance proportions:

- `prop_fire`
- `prop_crown_fire`
- `prop_insect`
- `prop_disease`
- `prop_weather`
- `prop_human_or_harvest`
- `dominant_disturbance_class`
- `dominant_disturbance_prop`
- `fire_severity_class`
- `plot_disturbance_extent_class`

Columns beginning with `prop_` are proportions of the mapped plot visit. Columns beginning with `forested_prop_` are proportions of the forested condition area only. Fire severity is an FIA-only proxy: crown fire is treated as the strongest available severity signal, while other fire codes are labeled as non-crown or unspecified fire.

The script writes two compact QA files:

- `plot_disturbance_severity_summary.csv`: overall counts and disturbance prevalence.
- `plot_disturbance_severity_by_class.csv`: counts by dominant disturbance class, fire severity class, extent class, and condition-proportion QA flag.

## Script 04 Inputs And Outputs

Inputs:

- `plot_seedling_species.parquet`, `plot_sapling_species.parquet`, or `plot_tree_species.parquet`, depending on `--layer`.
- `species_climate_niches_us_study_area.parquet`, with optional global fallback from `species_climate_niches.parquet`.

Output grain:

```text
community_layer x stable_plot_id x PLT_CN x INVYR x CONDID
```

The output contains both weighted means and weighted medians:

- `mean_temp` and `median_temp`
- `mean_heat` and `median_heat`
- `mean_cold` and `median_cold`
- `mean_temp_seasonality` and `median_temp_seasonality`
- `mean_cwd` and `median_cwd`
- `mean_peak_cwd` and `median_peak_cwd`
- `mean_pr` and `median_pr`
- `mean_dry_month_pr` and `median_dry_month_pr`

Coverage fields such as `frac_weight_with_niche`, `frac_species_with_niche`, and `frac_weight_with_global_fallback_niche` are retained so lower-coverage communities can be flagged or filtered during modeling.

Default weights:

- Seedlings: `seedlings_tpa`.
- Saplings: `abundance_for_cwm`, currently based on trees per acre.
- Trees: `abundance_for_cwm`, currently based on basal area per acre.

The script writes three compact QA files per layer:

- `plot_community_climate_summary_<layer>.csv`
- `plot_community_climate_coverage_by_state_<layer>.csv`
- `plot_community_climate_missing_species_<layer>.csv`

## Script 05 Inputs And Outputs

Inputs:

- `plot_seedling_species.parquet`, `plot_sapling_species.parquet`, or `plot_tree_species.parquet`, depending on `--layer`.
- `species_climate_niches_us_study_area.parquet`, with optional global fallback from `species_climate_niches.parquet`.

Output grain:

```text
community_layer x stable_plot_id x PLT_CN x INVYR
```

The output contains plot survey-year community-weighted means and weighted medians:

- `cwm_temp` and `median_temp`
- `cwm_heat` and `median_heat`
- `cwm_cold` and `median_cold`
- `cwm_temp_seasonality` and `median_temp_seasonality`
- `cwm_cwd` and `median_cwd`
- `cwm_peak_cwd` and `median_peak_cwd`
- `cwm_pr` and `median_pr`
- `cwm_dry_month_pr` and `median_dry_month_pr`

The default plot-level weight is:

```text
species abundance weight x CONDPROP_UNADJ
```

This keeps mixed-condition plots from giving the same influence to a small mapped condition and a large mapped condition. Use `--condition-prop-weight=FALSE` only as a sensitivity check.

The script writes three compact QA files per layer:

- `plot_year_community_cwm_summary_<layer>.csv`
- `plot_year_community_cwm_coverage_by_state_<layer>.csv`
- `plot_year_community_cwm_missing_species_<layer>.csv`

## Script 06 Inputs And Outputs

Inputs:

- `plot_year_community_cwm_seedlings.parquet`, `plot_year_community_cwm_saplings.parquet`, or `plot_year_community_cwm_trees.parquet`, depending on `--layer`.
- `plot_disturbance_severity.parquet`.

Output grain:

```text
community_layer x stable_plot_id x previous_PLT_CN x current_PLT_CN
```

The output represents consecutive FIA survey intervals. In plain language, each row asks: how did the plot-level community climate affinity change between the previous survey year and the current survey year?

Main column groups:

- Previous/current survey identity: previous and current `PLT_CN`, previous and current `INVYR`, and `years_between_surveys`.
- Previous/current community context: species richness, abundance weight, and niche coverage.
- Climate-affinity change: previous value, current value, absolute delta, and annualized rate for each CWM and median indicator.
- Current-survey disturbance: fire, crown fire, insect, disease, weather, human/harvest proportions and dominant disturbance class.
- Review flags: whether both surveys meet the niche coverage threshold and whether the recorded disturbance year falls inside the survey interval.

The script writes two compact QA files per layer:

- `plot_year_climate_change_summary_<layer>.csv`
- `plot_year_climate_change_by_disturbance_<layer>.csv`

## Output Reference

This section describes the main thermophilization products in plain language. For the cross-repo index of where products live and whether they are local or tracked, see [Data Products](../docs/DATA_PRODUCTS.md#thermophilization-outputs).

<a id="plot_recruitment_cwmparquet"></a>

### `plot_recruitment_cwm.parquet`

One row represents one FIA condition in one plot visit:

```text
stable_plot_id x PLT_CN x INVYR x CONDID
```

This is the original seedling recruitment climate-affinity table. It uses the tree seedling species present in a FIA condition and summarizes their BIEN/TerraClimate species niche values as community-weighted means. In plain language, it asks: for this condition in this survey year, are the recruiting tree seedlings associated with warmer, colder, wetter, or drier species ranges?

Main column groups:

- Plot and condition identity: `stable_plot_id`, `PLT_CN`, `INVYR`, `CONDID`, state/county/plot fields.
- FIA context: coordinates, forest type, condition proportion, forested status, disturbance flags.
- Community totals: seedling species counts, seedling abundance, and CWM weights.
- Climate-affinity means: `cwm_temp`, `cwm_heat`, `cwm_cold`, `cwm_cwd`, `cwm_pr`, and related indicators.
- Niche coverage: fraction of seedling abundance and species with usable niche values.

Use this table for condition-level seedling recruitment checks. Use the script `05` plot-year products for the confirmed plot survey-year analysis.

<a id="plot_recruitment_analysis_cohortparquet"></a>

### `plot_recruitment_analysis_cohort.parquet`

One row represents one FIA condition that passed the script `02` eligibility filters:

```text
stable_plot_id x PLT_CN x INVYR x CONDID
```

This table joins seedling recruitment CWM values, condition-level disturbance/control labels, and plot-level exclusion flags. It is the condition-level pool of disturbed and control candidates for disturbance-versus-control comparisons. The exact row filters are documented in [Script 02 Inputs And Outputs](#script-02-inputs-and-outputs), and their row-count effects are written to `analysis_cohort_attrition.csv`.

Main column groups:

- Recruitment CWM values from `plot_recruitment_cwm.parquet`.
- Disturbance/control fields: `disturbed_vs_control`, `disturbance_class`, `disturbance_class_primary`.
- Timing fields: disturbance year and time since disturbance when FIA records a usable year.
- Matching/modeling context: region, forest type, plot identifiers, and niche coverage.
- Sensitivity flags for plot-level exclusions and harvest-agent warnings.

Use this table when the analysis should stay at the FIA condition level.

<a id="plot_disturbance_severityparquet"></a>

### `plot_disturbance_severity.parquet`

One row represents one FIA plot visit:

```text
stable_plot_id x PLT_CN x INVYR
```

This table collapses FIA condition-level disturbance classifications to the plot survey-year level using `CONDPROP_UNADJ`. In plain language, it estimates how much of the mapped plot visit was affected by each disturbance type.

Main column groups:

- Plot visit identity: `stable_plot_id`, `PLT_CN`, `INVYR`.
- Disturbance proportions: `prop_fire`, `prop_crown_fire`, `prop_insect`, `prop_disease`, `prop_weather`, `prop_human_or_harvest`.
- Forested-area versions: `forested_prop_fire`, `forested_prop_insect`, `forested_prop_disease`, and related fields.
- Summary labels: `dominant_disturbance_class`, `dominant_disturbance_prop`, `fire_severity_class`, `plot_disturbance_extent_class`.
- QA: condition-proportion quality flag.

Use this table to attach plot-level disturbance amount and first-pass severity classes to plot-year climate-affinity metrics.

<a id="plot_community_climate_layerparquet"></a>

### `plot_community_climate_<layer>.parquet`

Files:

```text
plot_community_climate_seedlings.parquet
plot_community_climate_saplings.parquet
plot_community_climate_trees.parquet
```

One row represents one FIA condition and one community layer:

```text
community_layer x stable_plot_id x PLT_CN x INVYR x CONDID
```

These tables calculate both weighted means and weighted medians of species climate niches for seedlings, saplings, or trees while keeping FIA conditions separate. They are useful for condition-level diagnostics and for checking how much results depend on mixed-condition plots.

Main column groups:

- Community layer: `seedlings`, `saplings`, or `trees`.
- Condition identity and FIA context.
- Community abundance and species counts.
- Weighted means: `mean_temp`, `mean_cwd`, `mean_pr`, and related indicators.
- Weighted medians: `median_temp`, `median_cwd`, `median_pr`, and related indicators.
- Niche coverage and global-fallback usage.

Use these tables when the condition is the analysis unit.

<a id="plot_year_community_cwm_layerparquet"></a>

### `plot_year_community_cwm_<layer>.parquet`

Files:

```text
plot_year_community_cwm_seedlings.parquet
plot_year_community_cwm_saplings.parquet
plot_year_community_cwm_trees.parquet
```

One row represents one FIA plot visit/survey year and one community layer:

```text
community_layer x stable_plot_id x PLT_CN x INVYR
```

These are the main products for the confirmed "community weighted mean for each year of survey" analysis. They collapse all selected FIA conditions and subplots within a plot visit into one plot-level community climate-affinity value per layer and survey year. By default, abundance weights are multiplied by `CONDPROP_UNADJ`, so small mapped conditions contribute less than large mapped conditions.

Main column groups:

- Community layer and plot visit identity.
- Community abundance, species richness, and source-row counts.
- Community-weighted means: `cwm_temp`, `cwm_cwd`, `cwm_pr`, and related indicators.
- Weighted medians: `median_temp`, `median_cwd`, `median_pr`, and related indicators.
- Niche coverage and global-fallback usage.

Use these tables to estimate rates of change through repeated FIA survey years.

<a id="plot_year_climate_change_layerparquet"></a>

### `plot_year_climate_change_<layer>.parquet`

Files:

```text
plot_year_climate_change_seedlings.parquet
plot_year_climate_change_saplings.parquet
plot_year_climate_change_trees.parquet
```

One row represents one repeated-survey interval for one stable FIA plot and one community layer:

```text
community_layer x stable_plot_id x previous_PLT_CN x current_PLT_CN
```

These are the first rate-of-change products. They compare each plot survey year to the immediately previous survey year for the same stable plot. The rate columns divide the change by `years_between_surveys`, so they can be compared across FIA panels with different remeasurement intervals.

Main column groups:

- Previous/current survey identity and survey interval length.
- Previous/current community richness, abundance weight, and niche coverage.
- Previous/current climate-affinity values, absolute deltas, and per-year rates.
- Current-survey disturbance proportions and first-pass severity classes.
- Flags for niche coverage and whether a recorded disturbance year falls inside the survey interval.

Use these tables for the first repeated-plot analysis of whether community climate affinity is changing faster after disturbance.

<a id="qa-csvs"></a>

### QA CSVs

The QA CSVs in `07_thermophilization/qa/outputs/` are compact summaries of the production products. They are intended for quick review rather than modeling.

Main families:

- `plot_recruitment_cwm_*`: coverage summaries and missing seedling niche species.
- `analysis_cohort_*`: row attrition and final cohort counts.
- `plot_disturbance_severity_*`: disturbance prevalence, dominant class counts, and condition-proportion checks.
- `plot_community_climate_*`: condition-level layer coverage and missing species.
- `plot_year_community_cwm_*`: plot survey-year layer coverage and missing species.
- `plot_year_climate_change_*`: repeated-survey interval counts and disturbance summaries.
- `thermophilization_validation_*`: structural validation checks for files, row grains, required columns, proportions, coverage fields, and rate calculations.

Run the validator after rebuilding scripts `01` through `06`:

```bash
Rscript 07_thermophilization/qa/01_validate_thermophilization_products.R
```

## Smoke Tests

Limited runs write to ignored smoke folders:

```bash
Rscript 07_thermophilization/scripts/01_build_plot_recruitment_cwm.R --limit=100
Rscript 07_thermophilization/scripts/02_build_analysis_cohort.R --limit=1000
Rscript 07_thermophilization/scripts/03_build_plot_disturbance_severity.R --limit=1000
Rscript 07_thermophilization/scripts/04_build_plot_community_climate_metrics.R --layer=seedlings --limit=100
Rscript 07_thermophilization/scripts/04_build_plot_community_climate_metrics.R --layer=saplings --limit=100
Rscript 07_thermophilization/scripts/04_build_plot_community_climate_metrics.R --layer=trees --limit=100
Rscript 07_thermophilization/scripts/05_build_plot_year_community_cwm.R --layer=seedlings --limit=100
Rscript 07_thermophilization/scripts/05_build_plot_year_community_cwm.R --layer=saplings --limit=100
Rscript 07_thermophilization/scripts/05_build_plot_year_community_cwm.R --layer=trees --limit=100
Rscript 07_thermophilization/scripts/06_build_plot_year_climate_change.R --layer=seedlings --limit=1000
Rscript 07_thermophilization/scripts/06_build_plot_year_climate_change.R --layer=saplings --limit=1000
Rscript 07_thermophilization/scripts/06_build_plot_year_climate_change.R --layer=trees --limit=1000
```

Smoke outputs go to:

```text
07_thermophilization/data/smoke/
07_thermophilization/qa/smoke/
```

## Optional Arguments

- `--limit=N`: smoke test on the first `N` FIA condition rows.
- `--weight=seedlings_tpa`: default CWM weighting.
- `--weight=treecount_total`: raw seedling-count sensitivity check.
- `--weight=treecount_calc_total`: FIA calculated count sensitivity check.
- `--weight=presence`: species presence/absence sensitivity check.
- `--range-scope=us_study_area_with_global_fallback`: default. Use study-area niches first and global niches only for species without a study-area niche.
- `--range-scope=us_study_area`: use only study-area clipped species niches.
- `--range-scope=global`: use global BIEN range climate niches instead.

Script `02` also accepts:

- `--limit=N`: smoke test on the first `N` CWM condition rows.
- `--min-niche-coverage=0.95`: threshold used to flag lower-coverage conditions. It does not remove them.

Script `03` also accepts:

- `--limit=N`: smoke test on the first `N` disturbance-classification condition rows.

Script `04` also accepts:

- `--layer=seedlings|saplings|trees`: community layer to summarize.
- `--weight=<column>`: optional weight override. For example, `--weight=ba_per_acre` for tree basal-area weighting or `--weight=n_trees_tpa` for tree density weighting.
- `--range-scope=us_study_area_with_global_fallback`: default. Same interpretation as script `01`.
- `--limit=N`: smoke test on the first `N` complete FIA condition communities.

Script `05` also accepts:

- `--layer=seedlings|saplings|trees`: community layer to summarize.
- `--weight=<column>`: optional weight override.
- `--condition-prop-weight=TRUE`: default. Multiplies species abundance weights by `CONDPROP_UNADJ` before aggregating to plot visit.
- `--range-scope=us_study_area_with_global_fallback`: default. Same interpretation as script `01`.
- `--limit=N`: smoke test on the first `N` complete FIA plot visits.

Script `06` also accepts:

- `--layer=seedlings|saplings|trees`: community layer to summarize.
- `--min-niche-coverage=0.95`: threshold used to flag repeated-survey intervals where either the previous or current survey has lower niche coverage.
- `--limit=N`: smoke test on the first `N` stable plots in the selected layer.
