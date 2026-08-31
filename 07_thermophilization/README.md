# Thermophilization Analysis

**Navigation:** [Repo Home](../README.md) | [Docs Hub](../docs/README.md) | [FIA](../05_fia/README.md) | [Species Niches](../06_species_niches/README.md) | [Disturbance Linkage](../08_disturbance_linkage/README.md) | [Data Products](../docs/DATA_PRODUCTS.md)

This directory builds the FIA tables needed to ask whether forest plant communities are shifting toward species associated with warmer or drier climates, and whether those shifts are related to disturbance.

> **Current status:** The downstream CWM and change products in this directory
> were rebuilt on 2026-08-06 from the refreshed forested-condition foundation,
> so they now reflect the August 2026 Florida, Kentucky, and Texas FIA raw
> refresh. The weighting and cohort questions described below are still open,
> so treat these numbers as current but not final.

In this workflow, a **community climate-affinity metric** means: take the species present in a FIA plot visit, join each species to its climate niche, and summarize those niche values as a weighted mean or weighted median. A warmer community-weighted mean does not mean the plot's climate is warmer; it means the species present are associated with warmer parts of their ranges.

The products here answer three connected questions:

1. What is the community climate affinity of each forested plot visit?
2. How does that value change between repeated FIA surveys of the same plot?
3. How much of the plot visit was affected by fire, insects, disease, weather, treatment, or harvest?

Main inputs:

- FIA species composition tables for seedlings, saplings, and trees from [`05_fia/`](../05_fia/README.md).
- BIEN/TerraClimate species climate niches from [`06_species_niches/`](../06_species_niches/README.md).
- The FIA forested-condition foundation from `05_fia/scripts/foundations/02_build_forested_condition_foundation.R`.

Open weighting, cohort, and severity choices are tracked in [`docs/METHOD_DECISIONS_NEEDED.md`](../docs/METHOD_DECISIONS_NEEDED.md). Producer scripts preserve the information needed to make those choices; they do not make them.

## Pipeline

```text
05_fia species tables ──┐
06_species_niches      ─┴─→ 01 condition community climate
                                    │
       forested_condition_foundation┴─→ 02 forest plot-visit CWM
                                              ├──────────────→ 04 consecutive-survey change
       03 plot disturbance severity ──────────┘                 (one row per interval)
                                              └──────────────→ 05 first-to-last change
                                                                (one row per plot)
```

| Script | Purpose | Main output |
| --- | --- | --- |
| `01_build_condition_community_climate.R` | Weighted mean and weighted median climate affinity for one community layer, keeping FIA conditions separate. | `plot_community_climate_<layer>.parquet` |
| `02_build_forest_plot_visit_cwm.R` | Collapse conditions to one row per plot visit using **forested conditions only**, weighted by forested-area share. | `forest_plot_visit_cwm_<layer>.parquet` |
| `03_build_plot_disturbance_severity.R` | Aggregate condition-level disturbance to the plot visit using `CONDPROP_UNADJ`. | `plot_disturbance_severity.parquet` |
| `04_build_visit_interval_change.R` | Change between each survey and the one before it. | `forest_visit_interval_change_<layer>.parquet` |
| `05_build_first_last_change.R` | Change between a plot's earliest and latest survey. | `forest_first_last_change.parquet` |
| `qa/01_validate_thermophilization_products.R` | Structural validation: files, row grains, required columns, proportion ranges, rate arithmetic. | `thermophilization_validation_*.csv` |
| `qa/02_disturbance_survey_coverage.R` | How many stable plots have a survey before and after a disturbance, for types you designate in config. | `disturbance_survey_coverage_*` |
| `qa/03_plot_forest_analysis_qa.R` | Distribution figures for forested proportion, visit pairing, fire extent, and insect impact. | `qa/figures/*.png` |

### Two change designs, both built

A plot surveyed in 2002, 2012, and 2022 can be summarized two ways:

- **Consecutive intervals** (script `04`): 2002→2012 and 2012→2022. Two rows. More data points, and a change can be lined up against a disturbance dated to a specific interval. Repeat rows from one plot are correlated, which any model has to account for.
- **First to last** (script `05`): 2002→2022. One row. One long, clean change per plot, but it cannot say when during the twenty years the change happened.

Neither is the designated primary design. Choosing between them is open decision #2 in [`METHOD_DECISIONS_NEEDED.md`](../docs/METHOD_DECISIONS_NEEDED.md), so both are built from the same forest-only CWM and ordered by the same measurement dates.

## Run Order

Species niches and the FIA forested-condition foundation must be current first:

```bash
Rscript 05_fia/scripts/foundations/01_build_plot_visit_context.R
Rscript 05_fia/scripts/foundations/02_build_forested_condition_foundation.R
```

Then:

```bash
Rscript 07_thermophilization/scripts/01_build_condition_community_climate.R --layer=seedlings
Rscript 07_thermophilization/scripts/01_build_condition_community_climate.R --layer=saplings
Rscript 07_thermophilization/scripts/01_build_condition_community_climate.R --layer=trees
Rscript 07_thermophilization/scripts/02_build_forest_plot_visit_cwm.R
Rscript 07_thermophilization/scripts/03_build_plot_disturbance_severity.R
Rscript 07_thermophilization/scripts/04_build_visit_interval_change.R --layer=seedlings
Rscript 07_thermophilization/scripts/04_build_visit_interval_change.R --layer=saplings
Rscript 07_thermophilization/scripts/04_build_visit_interval_change.R --layer=trees
Rscript 07_thermophilization/scripts/05_build_first_last_change.R
Rscript 07_thermophilization/qa/scripts/01_validate_thermophilization_products.R
Rscript 07_thermophilization/qa/scripts/02_disturbance_survey_coverage.R
```

Script `02` builds all three layers by default; pass `--layer=trees` to restrict it.

### Rebuilding And Skipping

Every stage above checks whether its output is already current before doing any
work, so the block can be rerun end to end and only the stale products are
rebuilt. A product is rebuilt when:

- a rebuild was forced with `--force`,
- the output file is missing,
- any declared input is newer than the output, or
- the existing output is missing a column the product is supposed to have.

Otherwise the stage prints `skip (up to date)` and exits. A full rerun with
nothing stale takes seconds instead of roughly 45 minutes.

The important half of this is the third rule. Rebuilding
`forested_condition_foundation.parquet` makes every CWM and change product
downstream of it stale, and the check notices that on the next run. Skipping
merely because the output file exists would leave the old numbers silently in
place, which is how the August 2026 refresh produced products that disagreed
with their own inputs.

To rebuild anyway:

```bash
# Force one product.
Rscript 07_thermophilization/scripts/04_build_visit_interval_change.R --layer=trees --force

# Force a named product from a stage that builds several.
Rscript 07_thermophilization/scripts/02_build_forest_plot_visit_cwm.R --force=forest_plot_visit_cwm_trees
```

Modification time is not a content hash: cloning the repo, copying the data
directory, or restoring it from cloud sync all rewrite timestamps and can make
a stale product look current. The required-column check catches products built
before a schema change; `--force` covers everything else. Smoke runs
(`--limit=N`) always rebuild.

## How A Plot Visit Becomes One Number

Script `01` computes, for each FIA condition:

```text
CWM_indicator = sum(abundance_i * species_indicator_i) / sum(abundance_i)
```

Because that is a ratio, it is unaffected by how large the condition is — it is a per-condition average.

Script `02` then combines a visit's conditions:

```text
visit_CWM = sum(forest_condition_CWM_c * w_c) / sum(w_c)
      w_c = CONDPROP_UNADJ_c / forested_plot_proportion
```

Only conditions with `COND_STATUS_CD == 1` contribute, and their weights are renormalized to sum to 1 across the visit's forested area. Two consequences worth stating plainly:

- A plot that is 40% forest and 60% pasture reports the climate affinity of its forested 40%. `forested_plot_proportion` records that it was 0.4, so small forest patches can be filtered or flagged.
- A plot visit with no forested condition has no community to measure and does not appear in the product.

Condition area is applied exactly once, here. Script `01` does not apply it, because a ratio divides it out.

## Weighting Basis Per Layer

| Layer | Abundance weight | Meaning |
| --- | --- | --- |
| seedlings | `seedlings_tpa` | seedlings per acre |
| saplings | `n_trees_tpa` | sapling stems per acre |
| trees | `ba_per_acre` | basal area per acre |

Trees are weighted by basal area so a large-diameter tree counts for more than a sapling-sized stem. Override with `--weight=<column>` on script `01` for sensitivity checks.

## Niche Coverage

Rows are kept even when some species lack a climate niche, with coverage reported rather than silently dropped:

- `frac_weight_with_niche` — share of the community weight that had a niche value.
- `frac_species_with_niche` — share of species that had one.
- `frac_weight_with_study_area_niche` / `frac_weight_with_global_fallback_niche` — how much came from the study-area niche versus the global BIEN fallback.

Default range scope is `us_study_area_with_global_fallback`: use the study-area niche when one exists, fall back to the global range niche only for species observed in FIA that have no study-area-clipped niche. Filter on `frac_weight_with_niche` before comparing values across plots; a mean built from a third of the stems is not the same measurement as one built from nearly all.

## Output Reference

<a id="plot_community_climate_layerparquet"></a>

### `plot_community_climate_<layer>.parquet`

Grain: `community_layer x stable_plot_id x PLT_CN x INVYR x CONDID`.

Weighted means and weighted medians of species climate niches, with FIA conditions kept separate. Contains **every** FIA condition class, not just forest — it is the shared input to the forest-only product, and a forest-specific workflow must filter `COND_STATUS_CD == 1` (or use script `02`'s output instead).

Column groups: condition identity and FIA context; species counts and community weight; weighted means (`mean_temp`, `mean_cwd`, `mean_pr`, ...); weighted medians (`median_temp`, ...); niche coverage and fallback usage.

<a id="forest_plot_visit_cwm_layerparquet"></a>

### `forest_plot_visit_cwm_<layer>.parquet`

Grain: `community_layer x stable_plot_id x PLT_CN x INVYR`.

The analysis response. Forested conditions only, weighted by forested-area share.

Column groups:

- Plot visit identity and coordinates.
- `forested_plot_proportion` — how much of the plot was forest.
- `n_forested_conditions_with_layer`, `forested_condition_weight_with_layer` — how many forested conditions carried this layer, and how much of the forested area they covered. A value below 1 means part of the forested area had no stems of this layer.
- Weighted means and medians for the eight climate indicators.
- Niche coverage and fallback usage.
- `condition_proportion_quality_flag` — carried from the FIA condition foundation.

<a id="plot_disturbance_severityparquet"></a>

### `plot_disturbance_severity.parquet`

Grain: `stable_plot_id x PLT_CN x INVYR`.

How much of the mapped plot visit carried each disturbance type, from FIA condition records aggregated by `CONDPROP_UNADJ`.

Column groups: disturbance proportions (`prop_fire`, `prop_crown_fire`, `prop_insect`, `prop_disease`, `prop_weather`, `prop_human_or_harvest`); forested-area versions (`forested_prop_fire`, ...); summary labels (`dominant_disturbance_class`, `is_high_severity_fire`); fire and insect timing.

> **`fire_*` is all fire; `insect_*` is all insects.** `fire_disturbance_year_*` covers `DSTRBCD` 30/31/32, not crown fire alone. `insect_disturbance_year_*` covers 10/11/12, not bark beetle alone. For crown fire specifically use `prop_crown_fire` (`DSTRBCD` 32). FIA's condition disturbance code cannot isolate bark beetle.

High-severity fire is graded directly from `prop_crown_fire` (or `forested_prop_crown_fire`) via one cutoff in `config.yaml -> processed.thermophilization.high_severity_fire`. `is_high_severity_fire` is `NA` on every row until a threshold is set there.

<a id="forest_visit_interval_change_layerparquet"></a>

### `forest_visit_interval_change_<layer>.parquet`

Grain: `community_layer x stable_plot_id x previous_PLT_CN x current_PLT_CN`.

Change between each survey and the one before it. Rate columns divide by `years_between_surveys`, so panels with different remeasurement intervals are comparable.

Column groups: previous/current identity and interval length (`years_between_surveys`, `days_between_measurements`); previous/current forest extent and niche coverage; previous/current values, `delta_*`, and `rate_*_per_year` for all sixteen metrics; current-visit disturbance proportions; interval flags.

**Every interval uses FIA's own `PREV_PLT_CN` remeasurement link.** Chronological adjacency alone is not a link: plots replaced or re-established at a reused location carry a null or different `PREV_PLT_CN` and are excluded rather than paired into a change that never happened. `link_status` is `official_link_match` on every retained row, and the excluded counts are in `forest_visit_interval_change_linkage_<layer>.csv`.

Interval flags: `disturbance_within_interval` (any type), plus type-specific `fire_within_interval` and `insect_within_interval`, TRUE when a dated event of that type falls inside the interval.

<a id="forest_first_last_changeparquet"></a>

### `forest_first_last_change.parquet`

Grain: `stable_plot_id`.

Change from a plot's earliest to its latest survey, with all three life stages in one row. Restricted to visits where seedlings, saplings, and trees all have a usable value, so the three stages span the same interval and can be compared with each other.

Columns are named `<stage>_<endpoint>_<metric>` and `<stage>_change_<metric>`, for example `trees_first_mean_temp`, `trees_last_mean_temp`, `trees_change_mean_temp`. Endpoint context carries `first_PLT_CN`, `last_PLT_CN`, both inventory years, and both measurement-date bounds with their precision.

`survey_pairing_classes_observed`, `n_structural_matches`, `n_chronological_fallbacks`, and `all_resolved_links_structural` describe how well the plot's visits could be linked. Rows carrying a chronological fallback rest on a weaker link than a structural match.

### `forest_first_last_change_by_stage.parquet`

Grain: `stable_plot_id x life_stage`.

The same first-to-last comparison, but each life stage uses its own earliest and latest usable visit. This retains plots that the primary product drops because one stage is missing at an endpoint. Since stages can span different intervals here, do not difference one stage against another in this table — use `forest_first_last_change.parquet` for that.

<a id="qa-csvs"></a>

### QA CSVs

Compact summaries in `07_thermophilization/qa/outputs/`, for review rather than modeling:

- `plot_community_climate_*` — condition-level coverage by state and missing species.
- `forest_plot_visit_cwm_summary_<layer>.csv` — how many condition rows went in, how many aggregated as forest, how many were excluded as nonforest, and how many were absent from the FIA condition foundation.
- `plot_disturbance_severity_*` — disturbance prevalence, dominant class counts, condition-proportion checks.
- `forest_visit_interval_change_*` — interval counts, disturbance summaries, and linkage diagnostics.
- `forest_first_last_change_summary.csv` — plot counts at each cohort restriction.
- `disturbance_survey_coverage_*` — before/after survey counts (see below).
- `thermophilization_validation_*` — structural validation checks.

<a id="pre-post-disturbance-survey-coverage"></a>

## Pre/Post-Disturbance Survey Coverage

A recurring question is "how many plots have a survey before **and** after a disturbance?" Two tools answer it, and they are not interchangeable.

**Inside the change tables.** `forest_visit_interval_change_<layer>.parquet` carries `disturbance_within_interval`, TRUE when any recorded disturbance year falls inside an interval, plus type-specific `fire_within_interval` and `insect_within_interval`. Because the any-type field is any-type, a disease or weather event makes it TRUE even when nothing burned.

**The configurable coverage QA — `qa/02_disturbance_survey_coverage.R`.** This reads condition-level disturbance directly and counts, per **stable plot**, how many have a survey before and after a disturbance, for any type and condition you designate in `config.yaml -> processed.thermophilization.disturbance_survey_coverage`. It also supports a clean-baseline subset where the studied event is the plot's **first** recorded disturbance.

Add a named query in config; no code change is needed:

```yaml
        - name: crown_fire_2before_2after
          types: [crown_fire]
          min_surveys_before: 2
          min_surveys_after: 2
          first_disturbance_only: false
```

- `types`: one or more names from the `disturbance_types` registry (`any`, `fire`, `crown_fire`, `insect`, ...). Code sets are unioned.
- `min_surveys_before` / `min_surveys_after`: survey years required on each side (default 1 each).
- `first_disturbance_only`: require the studied event to be the plot's first dated disturbance of any type. Plots with an undated or continuous disturbance are excluded, because the absence of an earlier event cannot be established. Later events are allowed, but qualifying post-event surveys must precede the next dated disturbance.

Definitions:

- **survey year** — one distinct `INVYR` for a stable plot.
- **disturbance event** — a distinct (disturbance class group, disturbance year) pair, so one event recorded on several conditions counts once.
- **dated event** — an event with a real calendar year. `DSTRBYR` 0 and 9999 (continuous/unknown) cannot be ordered, so they are excluded from before/after timing and from the first-disturbance cohort.
- **before/after** — enough survey years strictly before, and enough on/after, the event. The visit that records the disturbance counts as an "after" survey.

Outputs:

- `disturbance_survey_coverage_summary.csv` — one row per configured query, with the headline `n_plots_match`.
- `disturbance_survey_coverage_by_plot.parquet` — per-plot TRUE/FALSE per query, for building cohorts downstream.
- `disturbance_survey_coverage_checks.csv` — internal-consistency checks: matched plots have enough survey years; no continuous/zero years leak in; code-subset nesting (crown_fire ≤ fire ≤ any) and first-disturbance nesting (first ≤ plain) hold; and, informationally, whether `DSTRBYR` is later than the visit that recorded it.

**Why "no code" is not "undisturbed":** FIA records a condition disturbance code only when the event killed or damaged at least 25% of the trees in that condition (and ≥ 1 acre), per the [FIADB Database Description](https://research.fs.usda.gov/sites/default/files/2024-05/wo-v9-2_apr2024_ug_fiadb_database_description_nfi.pdf). Plots below that threshold look undisturbed in these counts. The MTBS and IDS linkage in [`08_disturbance_linkage/`](../08_disturbance_linkage/README.md) is what catches sub-threshold and better-dated events.

## Smoke Tests

Limited runs write to ignored smoke folders (`data/smoke/`, `qa/smoke/`):

```bash
Rscript 07_thermophilization/scripts/01_build_condition_community_climate.R --layer=seedlings --limit=100
Rscript 07_thermophilization/scripts/03_build_plot_disturbance_severity.R --limit=1000
Rscript 07_thermophilization/scripts/04_build_visit_interval_change.R --layer=seedlings --limit=1000
```

## Optional Arguments

Every script:

- `--force` — rebuild regardless of the freshness check.
- `--force=<product>[,<product>]` — force only the named products, for stages that build more than one.

Script `01`:

- `--layer=seedlings|saplings|trees` — community layer to summarize.
- `--limit=N` — smoke test on the first `N` complete FIA condition communities.
- `--weight=<column>` — override the abundance weight, e.g. `--weight=n_trees_tpa` for tree density instead of basal area, or `--weight=presence` for presence/absence.
- `--range-scope=us_study_area_with_global_fallback` — default. Also accepts `us_study_area` or `global`.

Script `02`:

- `--layer=<layer>` — restrict to one layer; builds all three by default.

Script `03`:

- `--limit=N` — smoke test on the first `N` condition rows.

Script `04`:

- `--layer=seedlings|saplings|trees` — defaults to `seedlings`.
- `--limit=N` — smoke test on the first `N` stable plots.
- `--min-niche-coverage=0.95` — threshold for `meets_niche_coverage_threshold`. It flags rows; it does not remove them.

## Testing

```bash
Rscript scripts/run_tests.R 07_thermophilization
Rscript scripts/run_tests.R 07_thermophilization --strict
```

Non-strict skips checks whose products are absent; strict fails on them.

## See Also

- [Data Products](../docs/DATA_PRODUCTS.md#thermophilization-outputs) — cross-repo output inventory.
- [Method decisions still needed](../docs/METHOD_DECISIONS_NEEDED.md) — open choices these products deliberately leave open.
- [Archive notes](docs/archive_notes.md) — fields and approaches removed from this module, and why.
