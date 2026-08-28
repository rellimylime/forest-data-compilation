# Current analysis definitions

## Grain and condition histories

`PREV_PLT_CN` is the only authority for connecting visits. Within each linked
visit pair, the analysis retains the same numeric `CONDID`. `CONDID` never
creates a visit link. A condition must be forest (`COND_STATUS_CD == 1`) and
have `CONDPROP_UNADJ >= 0.30` at both endpoints of every retained interval.

The response observation is one complete first-to-last condition history. All
official intervals between those endpoints inform cumulative mortality.

## Community response

The three responses are last minus first community-weighted mean (CWM) niche
values for temperature, precipitation, and climatic water deficit. CWMs are
calculated at condition visit grain with individual-abundance weights:

- trees and saplings use expanded live-tree abundance;
- seedlings use expanded seedling tallies;
- species niche values use the US study-area estimate with global fallback.

The active tree CWM does not use basal area.

Life-stage responses are retained separately. A diagnostic pooled response
combines live seedlings, saplings, and adults after applying their appropriate
FIA sampling-element expansions. Conditions are not aggregated back to plots.

## Cumulative mortality

P2A GRM rows are excluded from both mortality numerators and denominators. A
tree lineage enters a condition history's denominator when it is first observed
alive in a risk visit. This includes the first visit and new live lineages first
observed at intermediate visits. Trees first observed only at the final visit
are excluded because they have no later mortality observation window.

Verified interval deaths come from FIA TREE/GRM mortality components. Fire,
insect, and disease attribution comes from the current death record's AGENTCD.
Each death uses the abundance weight assigned when its lineage entered the risk
set. The model predictor is cumulative mortality percentage over the complete
history; it is not annualized.

Sampling-element condition proportions are used with TPA expansion: microplot
for 1–4.9 inch stems, subplot for ordinary stems at least 5 inches, and macroplot
where applicable, with generic `CONDPROP_UNADJ` only as a documented fallback.

## Site CWD predictor

Site CWD is TerraClimate `def`, in millimetres per month. Monthly values whose
month timestamp falls within the actual first-to-last FIA measurement period
are summed. This is cumulative climatic exposure, not the community CWM-CWD
response.

## Preliminary models

For each of three responses and four vegetation groups, the current model is:

```text
delta CWM ~ cumulative fire mortality
          + cumulative insect mortality
          + cumulative disease mortality
          + cumulative site CWD
          + full survey period years
```

The four groups are seedlings, saplings, adults, and pooled live community.
Models are ordinary linear models with HC1 uncertainty clustered by stable FIA
plot. These are preliminary association models, not a finalized causal model.

Definitions that are constant for every current row are documented here rather
than repeated as columns: PREV-linked stable numeric CONDID histories; the
lineage-entry denominator above; verified agent deaths as numerator; P2A
excluded; cumulative rather than annual mortality.
