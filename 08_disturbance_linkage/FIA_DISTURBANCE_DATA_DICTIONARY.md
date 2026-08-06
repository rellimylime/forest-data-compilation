# FIA Disturbance Preparation Data Dictionary

This page defines the calculated fields in the FIA condition, fire, and tree-damage products. It distinguishes raw FIA columns from values calculated by this repository.

## Forest condition fields

Source table: raw FIA `COND`.

| Output column | Source or calculation |
| --- | --- |
| `COND_STATUS_CD` | Raw `COND.COND_STATUS_CD`. Code `1` means accessible forest land. |
| `CONDPROP_UNADJ` | Raw `COND.CONDPROP_UNADJ`, the unadjusted share of the plot assigned to that condition. |
| `is_forested_condition` | `COND_STATUS_CD == 1`. |
| `forested_plot_proportion` | Sum of `CONDPROP_UNADJ` over forest conditions within the same `PLT_CN + INVYR` visit. |
| `forested_condition_weight` | For a forest condition, `CONDPROP_UNADJ / forested_plot_proportion`. It is `NA` for nonforest conditions. |
| `nonsampled_zero_proportion` | All conditions have `COND_STATUS_CD == 5` and their proportions sum to zero. |
| `condition_proportions_not_one` | A non-nonsampled visit's condition proportions differ from one by more than 0.02. |

The forested-condition weight is therefore calculated, not supplied directly by FIA. It renormalizes condition shares across only the forested portion of a visit. Nonforest conditions do not dilute a forest-community metric.

Example: if a visit is 0.60 forest A, 0.20 forest B, and 0.20 nonforest, then `forested_plot_proportion = 0.80`. Forest A has weight `0.60 / 0.80 = 0.75`; forest B has weight `0.20 / 0.80 = 0.25`; the nonforest condition has no forest weight.

The raw source columns remain in the foundation alongside the calculated values, while this page is the authoritative formula and provenance reference.

### Wyoming nonsampled zero-proportion visits

The condition-proportion QA includes 154 Wyoming visits from `INVYR = 2000` with one
`COND_STATUS_CD = 5` condition and `CONDPROP_UNADJ = 0`. Their plot records are
nonsampled. They are therefore not sampled condition-area errors and do not enter
forest-community calculations. The code definitions are documented in the USDA
Forest Service [FIADB Database Description and Users Manual, version 4.0](https://research.fs.usda.gov/download/treesearch/37446.pdf).

The USDA Forest Service report [*Wyoming's Forests, 2002*](https://research.fs.usda.gov/treesearch/22228)
cites the applicable source as “Forest Service field procedures,” a 1999
unpublished Interior West FIA field guide on file in Ogden, Utah. The report does
not give that guide a version number. Consequently, FIADB `MANUAL = 0.9` should
be read as the database's pre-national/regional protocol label, not as evidence
that the unpublished guide itself was titled version 0.9. The original 1999
field guide has not been located in the public FIA user-guide archive.

## What “eligible” means

“Eligible” describes the denominator used for the three candidate tree-damage fractions. It does not mean that the condition is forested or that the record belongs in a final model.

A raw `TREE` record is eligible when all of these are true:

```text
TREE.STATUSCD == 1
TREE.DIA >= 1 inch
TREE.TPA_UNADJ > 0
```

`STATUSCD == 1` means a live tree. The one-inch threshold comes from `raw.fia.tree_filters.dia_min_inches` in `config.yaml`. Records are also limited to the configured inventory-year window before denominators are calculated. Mortality-agent records are not included.

The condition-denominator product retains the configurable components in:

```text
eligible_status_codes
minimum_diameter_inches
requires_positive_tpa
denominator_definition_id
mortality_agents_included
```

## Tree-record counts

| Output column | Calculation |
| --- | --- |
| `eligible_tree_record_count` | Number of distinct raw `TREE.CN` records meeting the eligibility rule in the condition. |
| `affected_tree_record_count` | Number of those eligible records carrying the exact `DAMAGE_AGENT_CD`. |
| `tree_record_fraction` | `affected_tree_record_count / eligible_tree_record_count`. |

These are observed FIA tree-record counts, not estimated numbers of stems on the landscape.

## TPA columns

`TREE.TPA_UNADJ` is a raw FIA column. It is an unadjusted trees-per-acre expansion factor attached to each sampled tree record.

| Output column | Source or calculation |
| --- | --- |
| `TPA_UNADJ` | Raw `TREE.TPA_UNADJ`, retained in tree evidence. |
| `eligible_tpa_unadj_sum` | Sum of raw `TREE.TPA_UNADJ` over eligible records in the condition. |
| `affected_tpa_unadj_sum` | Sum of raw `TREE.TPA_UNADJ` over eligible records carrying the exact agent. |
| `tpa_unadj_fraction` | `affected_tpa_unadj_sum / eligible_tpa_unadj_sum`. |

The `_unadj_` text is intentional: these are descriptive per-acre values based on the raw FIA expansion factor, not FIA population estimates.

## Basal-area columns

Basal area is calculated; it is not read directly from the raw FIA tree table. For each tree record:

```text
basal_area_sqft_per_acre_from_tpa_unadj =
    0.005454 * TREE.DIA^2 * TREE.TPA_UNADJ
```

`TREE.DIA` is the raw diameter in inches. `0.005454 * DIA^2` converts diameter to tree cross-sectional area in square feet, and multiplying by `TREE.TPA_UNADJ` expresses that record's contribution in square feet per acre.

| Output column | Calculation |
| --- | --- |
| `basal_area_sqft_per_acre_from_tpa_unadj` | Per-tree-record formula above. |
| `eligible_basal_area_sqft_per_acre` | Sum over all eligible records in the condition. |
| `affected_basal_area_sqft_per_acre` | Sum over eligible records carrying the exact agent. |
| `basal_area_fraction` | Affected divided by eligible basal area. |

The output column names include `_tpa_unadj_` and `_basal_area_sqft_per_acre` so their source or units remain apparent without repeating constant documentation strings on every row.

## What counts as affected

Damage agents come directly from:

```text
TREE.DAMAGE_AGENT_CD1
TREE.DAMAGE_AGENT_CD2
TREE.DAMAGE_AGENT_CD3
```

A tree is affected for an exact agent when that code appears in at least one of those slots. If the same exact code is repeated in multiple slots on one tree, the tree and its TPA/basal-area contribution are counted once for that code, while the contributing slot numbers remain in the evidence table.

A tree carrying two different exact agents contributes separately to each agent's condition-level candidate. The workflow does not add different agents together into one combined severity score.

## Which product to use

- `fia_tree_damage_agent_evidence/`: inspect individual tree-agent evidence and raw source fields.
- `fia_condition_damage_denominators/`: inspect eligible denominator ingredients for every condition.
- `fia_condition_damage_agent_candidates/`: compare all exact damage agents.
- `fia_insect_severity_candidates/`: the same candidates filtered to official insect codes.

For forest-only work, filter `COND_STATUS_CD == 1`. No candidate fraction is designated as the primary insect-severity measure.
