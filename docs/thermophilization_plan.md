# Thermophilization Analysis Plan

**Navigation:** [Repo Home](../README.md) | [Docs Hub](README.md) | [Thermophilization Module](../07_thermophilization/README.md) | [Species Niches](../06_species_niches/README.md) | [Disturbance Linkage](../08_disturbance_linkage/README.md) | [Method Decisions](METHOD_DECISIONS_NEEDED.md)

This is the analysis plan: the question, the cohort rules, and the decisions still open. For how the tables are built, column by column, see the [thermophilization module README](../07_thermophilization/README.md). Pipeline mechanics are documented once, there.

## The Question

> Do forest plots recruit species with warmer or drier climate niches after disturbance than they did before, and more so than comparable undisturbed plots?

The repository currently builds the **inputs** to that question. It does not contain fitted models or inferential results. Findings belong here only once they are produced by a committed script and accompanied by QA outputs.

## What Exists

| Component | Status |
| --- | --- |
| Species climate niches | Built, with documented coverage warnings |
| Condition-level community climate affinity | Built for seedlings, saplings, trees |
| Forest-only plot-visit climate affinity | Built for seedlings, saplings, trees |
| Plot-visit disturbance extent | Built |
| Change between consecutive surveys | Built for seedlings, saplings, trees |
| Change from first survey to last | Built |
| Before/after disturbance survey coverage | Built as configurable QA |
| Cohort selection for a specific analysis | Not built |
| Matched-control comparison | Not built |
| Models and figures | Not built |

Current row counts, coverage, and validation status live in `07_thermophilization/qa/outputs/`. **This document does not restate counts or percentages.** They change whenever FIA summaries, species niches, or eligibility rules are rebuilt, and a hardcoded number that silently goes stale is worse than no number. Cite the generating QA file instead.

## Response Variables

The response is community climate affinity at the forested plot visit, for three life stages:

- **Seedlings** — the direct recruitment layer, and the most responsive to recent conditions.
- **Saplings** — established recent regeneration.
- **Trees** — the standing live-tree community, slowest to shift.

Eight climate indicators are available per layer, as both a weighted mean and a weighted median. The primary candidates are annual mean temperature (`mean_temp`), annual climate water deficit (`mean_cwd`), and annual precipitation (`mean_pr`).

Shrub, forb, and graminoid layers require finalized P2VEG summaries and are not in scope yet.

## Sign Convention

Every reported effect must state its direction explicitly. For the before/after designs:

```text
change = later survey - earlier survey
```

Positive `change_mean_temp` means the community shifted toward species associated with warmer parts of their ranges. Positive `change_mean_cwd` means a shift toward species associated with drier conditions.

For any future matched comparison:

```text
delta = disturbed plot - matched control plot
```

## Cohort Restrictions To Decide

These are candidate restrictions on which plots enter an analysis. None is applied inside a producer script; each is a filter an analyst applies, and each has a stated cost.

### Forested at the post-disturbance survey

The response is a tree/sapling/seedling community mean, so a plot with no forested condition after the disturbance has no community to measure. Such plots are already absent from `forest_plot_visit_cwm_<layer>.parquet` by construction.

Requiring it makes the estimand explicit: *change in community climate affinity among plots that remained forested*. The trade-off is that the estimate cannot speak to disturbances severe enough to convert forest to nonforest.

**How much this censors has not been measured.** `qa/02_disturbance_survey_coverage.R` counts before/after survey availability but contains no forest-status logic, so no committed QA output answers it. Measuring it means intersecting the before/after cohort in `disturbance_survey_coverage_by_plot.parquet` with forest status at the post-disturbance visit in `forested_condition_foundation.parquet`. Until that is written, do not quote a censoring rate.

### The disturbance of interest is the plot's first

The pre-disturbance survey is the baseline. If an earlier disturbance preceded it, that baseline is already a recovering stand — different fuels, structure, seed sources, and successional stage — so the measured change would mix recovery from the earlier event with response to the studied one. Requiring the studied disturbance to be first keeps the baseline a genuine pre-disturbance reference.

The estimand becomes: *change from an undisturbed baseline to the first post-disturbance survey*. A plot may have later disturbances, as long as the post-disturbance survey used precedes them.

This is implemented as `first_disturbance_only: true` in the coverage queries. Plots whose disturbance year is undated or continuous (`DSTRBYR` 0 or 9999) are excluded, because "no earlier disturbance" cannot be established when event order is unknowable — which is why this restriction costs more plots than a naive reading suggests.

**Current retention:** compare `n_plots_before_after` for `first_fire_before_after` against `fire_before_after` in `disturbance_survey_coverage_summary.csv`. Do not quote a remembered figure; read the file.

### Niche coverage threshold

Every CWM row reports `frac_weight_with_niche`. The primary analysis should either filter to a stated threshold (0.95 is the default flag in the change products) or carry coverage as a documented sensitivity axis.

### Control definition

Where a control group is needed, `fia_condition_disturbance_flags.parquet` supplies the per-condition flags:

```text
control   = is_forested_analysis_condition & !has_any_recorded_disturbance & !has_any_treatment
disturbed = is_forested_analysis_condition & is_natural_disturbance & !is_human_or_harvest & !has_any_treatment
```

Harvest and human-disturbance flags are retained rather than dropped, so they can be excluded or used in sensitivity analysis.

**"No disturbance code" is not "undisturbed."** FIA records a condition disturbance code only when the event killed or damaged at least 25% of the trees in that condition, over at least 1 acre. Sub-threshold events are invisible here. The MTBS and IDS linkage in [`08_disturbance_linkage/`](../08_disturbance_linkage/README.md) exists to catch them.

## Open Decisions

The numbered decisions in [METHOD_DECISIONS_NEEDED.md](METHOD_DECISIONS_NEEDED.md) are the authoritative list. The two that most shape this analysis:

1. **Consecutive intervals or first-to-last?** Both products are built. Consecutive intervals give more observations and let a change be lined up against a dated event, at the cost of correlated repeat rows from one plot. First-to-last gives one clean long-run change per plot but cannot locate when the change happened.
2. **Which insect severity measure?** The preparation product retains `tree_record_fraction`, `tpa_unadj_fraction`, and `basal_area_fraction` side by side; none is designated primary.

## Planned Matched-Control Design

Not implemented. If pursued, disturbed plots should be matched to control plots on forest type (`FORTYPCD` or `forest_type_group`), region or ecoregion, inventory period, baseline climate, and niche coverage. The matching script should write one row per disturbed-control pair with explicit match diagnostics, so the quality of the match can be reviewed rather than assumed.

## Sensitivity Checks

- Require `frac_weight_with_niche >= 0.95`.
- Compare study-area-only niches against study-area niches with global fallback.
- Compare abundance weighting against presence/absence.
- Compare `FORTYPCD` against broader forest-type grouping.
- Compare East/West grouping against ecoregion or climate-region grouping.
- Compare the two change designs against each other on the plots that appear in both.

## Known Data Limitations

These are documented QA considerations, not blockers:

- Some FIA source codes are pseudo taxa, genus-level records, unknowns, or infraspecific taxa. They are excluded or flagged in the species-niche workflow.
- Some accepted names have no BIEN range map. Documented in species-niche QA outputs.
- A small number of FIA-observed species need a global fallback niche because their BIEN range does not overlap the study-area extraction. Reported per row as `frac_weight_with_global_fallback_niche`.
- Some plot visits have incomplete niche coverage because a species present lacks a usable niche. Reported per row, not dropped.
- A small number of FIA tree records reference a `CONDID` with no matching COND row. They cannot be area-weighted and are excluded; the count is reported in `forest_plot_visit_cwm_summary_<layer>.csv`.

Authoritative status files: [Species Niches QA Guide](../06_species_niches/qa/README.md) and the [thermophilization module README](../07_thermophilization/README.md).

## Reporting Standards

A result belongs in this document only when:

1. It is generated by a committed script.
2. Its input and output paths are documented.
3. A QA file reports the sample size and coverage behind it.
4. Its sign convention is stated explicitly.
5. Its limitations sit next to it, not in separate informal notes.
6. Any count or percentage cites the generating QA file rather than being written inline.

Until then this document stays a plan.
