# Dataset Query Guide

**Snapshot generated:** 2026-10-04 23:51:20 UTC
**Join registry:** 1.1.0
**Preset registry:** 1.1.0

**Navigation:** [Repository home](../README.md) | [Documentation hub](README.md) | [Searchable data catalog](DATA_CATALOG.md) | [Dashboard guide](dashboard/README.md)

This guide shows how existing products connect. It does not replace the product catalog or materialize a second giant database. The dashboard reads the same committed snapshots and generates paste-ready DuckDB SQL without opening the data files.

## Quick start

```bash
streamlit run docs/dashboard/app.py
```

Open **Data → Build a dataset** in the dashboard menu, choose a recipe or anchor product, select fields, review row-expansion warnings, and copy or download the generated SQL. See the [dashboard guide](dashboard/README.md) for running an export.

## Curated joins

| Anchor product | Add product | Join keys | Cardinality | Review | Important warning |
|---|---|---|---|---|---|
| FIA condition metadata | FIA condition disturbance and treatment flags | `PLT_CN, INVYR, CONDID = PLT_CN, INVYR, CONDID` | 1:1 — preserves rows | Constrained | Absence of a recorded disturbance is not proof that no event occurred. |
| FIA condition metadata | FIA plot tree structure | `PLT_CN, INVYR = PLT_CN, INVYR` | many:1 — preserves left rows | Constrained | Plot values repeat across conditions and must never be summed across joined rows. |
| FIA condition metadata | FIA plot seedling totals | `PLT_CN, INVYR = PLT_CN, INVYR` | many:1 — preserves left rows | Constrained | Plot values repeat across conditions and must never be summed across joined rows. |
| FIA condition metadata | FIA condition slope, aspect, and physiographic class | `PLT_CN, INVYR, CONDID = PLT_CN, INVYR, CONDID` | 1:1 — preserves rows | Constrained | ASPECT 0 means no aspect (slope under 5 percent), not north; filter on has_aspect first. |
| FIA condition metadata | FIA understory structure (state extracts) | `PLT_CN, INVYR, CONDID = PLT_CN, INVYR, CONDID` | 1:many — expands rows | Scientific review required | This expands each condition to subplot, growth-habit, and canopy-layer rows; protocol comparability through time still needs review. |
| FIA condition metadata | FIA understory species cover (state extracts) | `PLT_CN, INVYR, CONDID = PLT_CN, INVYR, CONDID` | 1:many — expands rows | Scientific review required | This expands each condition to individual plant records; PLANTS symbols do not directly join to FIA SPCD. |
| FIA condition metadata | FIA live-tree damage agents (labelled) | `PLT_CN, INVYR, CONDID = PLT_CN, INVYR, CONDID` | 1:many — expands rows | Constrained | This expands conditions to species-agent rows; damage on a living tree is not the same as disturbance or cause of death. |
| FIA live-tree damage agents (labelled) | FIA damage-agent lookup | `DAMAGE_AGENT_CD = DAMAGE_AGENT_CD` | many:1 — preserves left rows | Constrained | The lookup names agents but does not yet classify native versus non-native status. |
| FIA remeasurement components | FIA plot tree structure | `PLT_CN, INVYR = PLT_CN, INVYR` | 1:1 — preserves rows | Scientific review required | The remeasurement component product remains under scientific review. |
| FIA remeasurement components | FIA plot seedling totals | `PLT_CN, INVYR = PLT_CN, INVYR` | 1:1 — preserves rows | Scientific review required | The remeasurement component product remains under scientific review. |
| FIA remeasurement components | FIA condition metadata | `PLT_CN, INVYR = PLT_CN, INVYR` | 1:many — expands rows | Scientific review required | This expands each plot visit to its mapped conditions; CONDID is not stable through time. |
| FIA remeasurement components | Condition-visit climate affinity by life stage | `PLT_CN, INVYR = PLT_CN, INVYR` | 1:many — expands rows | Scientific review required | This expands plot visits by condition and life stage; CWM meaning still needs scientific review. |
| Condition-visit climate affinity by life stage | FIA condition disturbance and treatment flags | `PLT_CN, INVYR, CONDID = PLT_CN, INVYR, CONDID` | many:1 — preserves left rows | Scientific review required | Flags repeat across life stages; absence of a recorded disturbance is not proof of absence. |
| Condition-visit climate affinity by life stage | Interval agent-attributed mortality | `PLT_CN, INVYR, CONDID = T2_PLT_CN, T2_INVYR, CONDID` | many:1 — preserves left rows | Scientific review required | Mortality describes the interval that ends at this visit and repeats across life stages; a condition's first visit has no preceding interval, so its mortality is empty. |
| Condition-visit climate affinity by life stage | FIA condition slope, aspect, and physiographic class | `PLT_CN, INVYR, CONDID = PLT_CN, INVYR, CONDID` | many:1 — preserves left rows | Constrained | Topography repeats across life stages. ASPECT 0 means no aspect, not north; filter on has_aspect first. |
| Condition-visit climate affinity by life stage | FIA plot tree structure | `PLT_CN, INVYR = PLT_CN, INVYR` | many:1 — preserves left rows | Scientific review required | Plot metrics repeat across conditions and life stages and must never be summed. |
| Condition-visit climate affinity by life stage | FIA plot seedling totals | `PLT_CN, INVYR = PLT_CN, INVYR` | many:1 — preserves left rows | Scientific review required | Plot metrics repeat across conditions and life stages and must never be summed. |
| Stable-condition survey intervals | Stable-condition climate-affinity change | `stable_condition_interval_key = stable_condition_interval_key` | 1:many — expands rows | Scientific review required | This expands each interval to one row per available community life stage. |
| Stable-condition survey intervals | Interval agent-attributed mortality | `stable_condition_interval_key = stable_condition_interval_key` | 1:1 — preserves rows | Scientific review required | Mortality is an interval outcome, not an instantaneous visit value. |
| Stable-condition climate-affinity change | Interval agent-attributed mortality | `stable_condition_interval_key = stable_condition_interval_key` | many:1 — preserves left rows | Scientific review required | Mortality repeats across life-stage rows and must never be summed across layers. |
| Cumulative mortality by condition history | Site CWD by condition history | `history_id = history_id` | 1:1 — preserves rows | Scientific review required | Filter or explicitly retain cumulative_site_CWD_complete before modeling. |

## Research recipes

The baseline-table recipe is deliberately a query, not a new canonical product. This keeps one authoritative copy of every value while still giving collaborators a single export when they need one.

### Baseline research table

A condition-by-visit-by-life-stage panel: community climate affinity at every survey, fire/insect/disease mortality over the interval ending at that survey, condition disturbance, elevation, slope, aspect, and plot structure. It is assembled from the canonical products, not a copy of them.

- **Anchor:** Condition-visit climate affinity by life stage
- **Joins:** 5
- **Search terms:** baseline, giant file, panel, time series, mortality, CWM, density, diversity, topography, slope, aspect, elevation
- **Cautions:**
  - Plot structure, mortality, and topography repeat across condition and life-stage rows and must not be summed.
  - Mortality describes the interval ending at each visit; a condition's first visit has no preceding interval, so its mortality is empty.
  - ASPECT 0 means no aspect (slope under 5 percent), not north; use has_aspect.
  - Native/non-native agent status is not yet classified.
- **Still missing:**
  - Condition-matched understory change through time
  - Reviewed native/non-native insect and pathogen classification

### Condition CWM with disturbance context

Community-weighted temperature, precipitation, and CWD affinity at every condition visit and life stage, with condition-specific disturbance flags.

- **Anchor:** Condition-visit climate affinity by life stage
- **Joins:** 1
- **Search terms:** CWM, survey, time series, disturbance, condition, life stage
- **Cautions:**
  - Disturbance flags repeat across life stages.
  - For the panel version with mortality over each preceding interval, use the Baseline research table recipe.

### Condition-matched understory records

Join mapped condition metadata to understory structure observations as a starting point for inspecting repeat support and defining change metrics.

- **Anchor:** FIA condition metadata
- **Joins:** 2
- **Search terms:** understory, P2VEG, cover, vegetation, disturbance, condition
- **Cautions:**
  - This expands one condition to multiple subplot, growth-habit, and canopy-layer records.
  - It exposes existing observations only; comparable change metrics and protocol rules remain a methods decision.
- **Still missing:**
  - Reviewed condition-interval understory change outcomes

### Plot structure and diversity visits

Attach tree and seedling density, richness, and diversity summaries to the official remeasurement components used to order plot visits through time.

- **Anchor:** FIA remeasurement components
- **Joins:** 2
- **Search terms:** density, richness, diversity, Shannon, time series, panel, tree, seedling
- **Cautions:**
  - Values are visit summaries; interval deltas and annualized rates are not calculated by this recipe.
  - The remeasurement component product is reproducible but still marked not reviewed.
- **Still missing:**
  - Reviewed interval change and annualized change fields

### Exact FIA damage-agent names

Live-tree damage observations with official/common agent labels and any available scientific name retained from the FIA lookup.

- **Anchor:** FIA live-tree damage agents (labelled)
- **Joins:** 1
- **Search terms:** insect, pathogen, disease, damage, agent, scientific name, non-native, invasive
- **Cautions:**
  - Damage on a living tree is not the same observation as a condition disturbance or a cause of death.
  - The lookup does not currently include a reviewed native/non-native classification.
- **Still missing:**
  - Cited, geography-aware native/non-native status

### Life-stage CWM change with interval mortality

Stable-condition change in climate affinity by life stage, with cumulative interval mortality predictors attached at the matching interval.

- **Anchor:** Stable-condition climate-affinity change
- **Joins:** 1
- **Search terms:** panel, interval, mortality, CWM, change, sapling, adult
- **Cautions:**
  - Mortality values repeat across life-stage rows and must not be summed across layers.
  - These analysis products remain marked not reviewed in the product registry.

### Condition-history mortality with site CWD

One row per complete stable-condition history, combining cumulative agent mortality with cumulative site climatic water deficit.

- **Anchor:** Cumulative mortality by condition history
- **Joins:** 1
- **Search terms:** history, mortality, CWD, drought, model input
- **Cautions:**
  - Keep the CWD completeness flag or explicitly filter it before modeling.
  - This is first-to-last history grain, not a visit-level panel.

## Safety rules

- A `many:1` join preserves anchor rows but repeats the right-side value across the finer anchor grain.
- A `1:many` join expands anchor rows. The dashboard permits only one expanding join per query, preventing accidental cross-products between unrelated detail tables.
- Products marked for scientific review remain visible because the goal is navigation. The dashboard shows their warnings beside the plan; the generated SQL itself does not repeat them.
- Generated paths are repository-relative. Run queries from the repository root or edit the paths explicitly. Exports default to the gitignored `scratch_output/` directory, which must exist before DuckDB writes to it.
- Generated SQL uses `LEFT JOIN`, so an unmatched anchor row remains visible for QA.

## Refreshing the snapshots

Edit the curated YAML registries, then run:

```bash
Rscript forest_explorer/catalog/build_snapshot.R
```

The command refreshes the product catalog, join map, recipes, and this page together.
