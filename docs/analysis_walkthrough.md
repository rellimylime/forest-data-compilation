# Thermophilization Analysis Walkthrough

**Navigation:** [Repo Home](../README.md) | [Docs Hub](README.md) | [Analysis Plan](thermophilization_plan.md) | [Species Niches](../06_species_niches/README.md) | [Thermophilization Module](../07_thermophilization/README.md)

## Overview

This walkthrough explains *why* each step of the thermophilization pipeline exists and what its numbers mean. For column lists, grains, and run commands, see the [thermophilization module README](../07_thermophilization/README.md) — those are documented once, there.

The question behind the whole pipeline:

> Are forest communities shifting toward species associated with warmer or drier climates, and is disturbance part of why?

## Conceptual Flow

```text
FIA species records by layer          BIEN range maps + TerraClimate
        |                                        |
        |                                        v
        |                             species climate niches
        v                                        |
   join species to niches  <---------------------+
        |
        v
   condition-level community climate affinity
        |
        +-- keep forested conditions only, weight by forested area
        v
   forest plot-visit climate affinity  <--- the analysis response
        |
        +-- compare surveys of the same plot
        v
   change: consecutive intervals, or first survey to last
        |
        +-- attach FIA disturbance extent
        v
   modeling (not in this repository)
```

## Key FIA Grains

FIA data are hierarchical, and the workflow keeps that structure explicit rather than flattening it early.

| Grain | Meaning | Identifying fields |
|---|---|---|
| Stable plot | A physical location followed through time | `stable_plot_id` |
| Plot visit | One inventory record for that location | `PLT_CN`, `INVYR` |
| Condition | A mapped land/forest condition inside a plot visit | `PLT_CN`, `INVYR`, `CONDID` |
| Subplot or microplot | Within-plot sampling unit | `SUBP` |
| Species record | A species observed within a sampling unit | `SPCD` |

The condition grain matters most. **One FIA plot can contain several conditions**, and they can differ in forest type, disturbance history, and whether they are forest at all. A plot that is half old-growth and half pasture is two conditions, not one average. That is why the pipeline computes climate affinity per condition first and only then combines.

## Step 1: Species Climate Niches

Built in [`06_species_niches/`](../06_species_niches/README.md).

Each resolved species is represented by the climate across its BIEN range map: TerraClimate is overlaid on the range polygon and summarized for the 1981–2010 baseline. Eight indicators result:

| Short name | Indicator |
|---|---|
| `temp` | annual mean temperature |
| `heat` | warmest-month temperature |
| `cold` | coldest-month temperature |
| `temp_seasonality` | temperature seasonality |
| `cwd` | annual climate water deficit |
| `peak_cwd` | peak monthly climate water deficit |
| `pr` | annual precipitation |
| `dry_month_pr` | driest-month precipitation |

This is a species' *realized* niche — where it actually grows today, not where it could grow. A species restricted by past land use or dispersal limits will look narrower than its physiological tolerance.

Details: [Species Niche Workflow](../06_species_niches/WORKFLOW.md), [Methods](../06_species_niches/docs/methods_species_niches.md), [QA Guide](../06_species_niches/qa/README.md).

## Step 2: Condition-Level Community Climate Affinity

Producer: `01_build_condition_community_climate.R`.

For each FIA condition and community layer, take the species present, look up each one's niche value, and average them weighted by abundance:

```text
CWM_indicator = sum(abundance_i * niche_indicator_i) / sum(abundance_i)
```

**What this number is not.** A CWM of 12 °C does not say the plot is 12 °C. It says the species growing there are, on average, ones whose ranges centre on about 12 °C. A plot can sit in a cold place and still have a warm-affinity community if warm-associated species have moved in.

Abundance weight differs by layer: seedlings and saplings are weighted by stems per acre, trees by basal area, so a mature tree counts for more than a sapling-sized stem.

Because a CWM is a ratio, it is unaffected by how large the condition is. Condition size is applied in the next step, exactly once.

## Step 3: Forest Plot-Visit Climate Affinity

Producer: `02_build_forest_plot_visit_cwm.R`. **This is the analysis response.**

Conditions are combined into one value per plot visit:

```text
visit_CWM = sum(condition_CWM_c * w_c) / sum(w_c)
      w_c = CONDPROP_UNADJ_c / forested_plot_proportion
```

Two rules are enforced here:

1. **Forested conditions only** (`COND_STATUS_CD == 1`). Pasture, water, and developed conditions are not part of a forest community, so they do not contribute. A plot that is 40% forest reports the climate affinity of that 40%, and `forested_plot_proportion` records that it was 0.4 so small patches can be filtered.
2. **Weighted by forested-area share.** A condition covering most of the forested area counts for more than a sliver.

A plot visit with no forested condition has no community to measure and simply does not appear.

## Step 4: Disturbance Extent

Producer: `03_build_plot_disturbance_severity.R`.

FIA records disturbance at the condition level. This step aggregates it to the plot visit using `CONDPROP_UNADJ`, giving proportions such as `prop_fire`, `prop_insect`, `prop_disease`, and `prop_crown_fire`.

**The big caveat.** FIA assigns a condition disturbance code only when the event killed or damaged at least 25% of the trees in that condition, over at least 1 acre. "No disturbance code" therefore means "not severe enough to cross FIA's threshold" — **not** "definitely undisturbed." A low-severity surface fire can leave no trace here. Catching those is the job of the MTBS and IDS linkage in [`08_disturbance_linkage/`](../08_disturbance_linkage/README.md).

Two further limits worth stating before anyone reads a result:

- `insect_*` covers all insects (`DSTRBCD` 10/11/12). FIA cannot isolate bark beetle.
- `fire_*` covers all fire (30/31/32). Crown fire alone is `prop_crown_fire` (32), the closest available proxy for high severity.

## Step 5: Change Between Surveys

Two producers, two designs, both built:

| Producer | Compares | Rows per plot |
|---|---|---|
| `04_build_visit_interval_change.R` | Each survey with the one before it | one per interval |
| `05_build_first_last_change.R` | Earliest survey with latest | one |

**Why both.** Consecutive intervals give more observations and let a change be lined up against a disturbance dated to a particular interval — but repeat rows from one plot are correlated, and a model has to account for that. First-to-last gives one clean long-run change per plot but cannot say when in twenty years the change happened. Which is right depends on the question being asked, so the choice is left to the analyst (open decision #2 in [METHOD_DECISIONS_NEEDED.md](METHOD_DECISIONS_NEEDED.md)).

**How visits are paired.** A change value is only computed when FIA's own `PREV_PLT_CN` remeasurement link agrees with the chronologically previous visit. This matters more than it sounds: plots that were replaced or re-established at a reused location carry a null or different `PREV_PLT_CN`, and treating them as remeasurements would invent change that never happened. Excluded counts are reported in the linkage QA rather than dropped silently.

## Downstream Analysis (Out of Scope Here)

How to define disturbed versus control, whether to match controls, how to group results, which responses to estimate — these depend on the specific analysis and belong to whoever runs it. This repository's job is to provide clean, documented, flagged inputs so those choices are easy to apply. It does not make them.

Open questions: [METHOD_DECISIONS_NEEDED.md](METHOD_DECISIONS_NEEDED.md). Cohort rules and their costs: [thermophilization_plan.md](thermophilization_plan.md).

## QA Files To Read First

Species niches:

- `06_species_niches/qa/outputs/species_niche_validation_decision.csv`
- `06_species_niches/qa/outputs/species_niche_validation_summary.csv`
- `06_species_niches/qa/outputs/species_taxon_resolution_summary.csv`
- `06_species_niches/qa/outputs/study_area_climate_gap_summary.csv`

Thermophilization:

- `07_thermophilization/qa/outputs/thermophilization_validation_summary.csv` — start here; the structural gate.
- `07_thermophilization/qa/outputs/forest_plot_visit_cwm_summary_<layer>.csv` — how many conditions went in, aggregated as forest, or were excluded.
- `07_thermophilization/qa/outputs/plot_community_climate_missing_species_<layer>.csv` — which species lack a niche.
- `07_thermophilization/qa/outputs/plot_disturbance_severity_summary.csv`
- `07_thermophilization/qa/outputs/forest_visit_interval_change_summary_<layer>.csv`
- `07_thermophilization/qa/outputs/forest_first_last_change_summary.csv`
- `07_thermophilization/qa/outputs/disturbance_survey_coverage_summary.csv` — before/after survey availability.

## Interpretation Boundaries

The repository currently supports statements about:

- Community climate affinity of forested plot visits, for seedlings, saplings, and trees.
- How that affinity changed between surveys of the same plot, by either change design.
- How much of a plot visit carried each FIA-recorded disturbance type.
- Which species niche gaps affect coverage, and by how much.
- Per-condition disturbance and treatment flags, from which any control/disturbed grouping can be built.

It does not yet support statements about:

- Whether disturbance increases or decreases thermophilization after formal modeling.
- Whether one region shows a stronger effect than another.
- Whether a specific disturbance class has a statistically reliable effect.

Those need modeling scripts and their QA outputs, neither of which exists yet.

## Schema Reference

Grains, columns, and caveats for every product are in the [module output reference](../07_thermophilization/README.md#output-reference). Machine-readable definitions, including keys and access rules, are in [`forest_explorer/registry/products.yaml`](../forest_explorer/registry/products.yaml) and rendered in the [Master Product Inventory](MASTER_PRODUCT_INVENTORY.md).

Two inputs from other modules that this analysis leans on:

- `05_fia/data/processed/summaries/fia_condition_disturbance_flags.parquet` — condition-level disturbance and treatment flags plus raw codes; the basis for any control/disturbed grouping.
- `06_species_niches/data/processed/species_climate_niches*.parquet` — one row per resolved species; the niche values every CWM is built from.
