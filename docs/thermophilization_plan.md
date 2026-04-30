# Thermophilization Analysis Plan

**Navigation:** [Repo Home](../README.md) | [Docs Hub](README.md) | [FIA Workflow](../05_fia/WORKFLOW.md) | [Data Products](DATA_PRODUCTS.md)

## Summary

This plan describes a FIA-first analysis for testing whether extreme disturbances
shift post-disturbance regeneration toward warmer- or drier-climate-affinity
species. The main ecological question is:

> After high-severity disturbance, are the species recruiting into forested FIA
> plots more thermally tolerant or drought adapted than the species recruiting
> into comparable undisturbed plots of the same forest type?

The first-pass analysis should use FIA condition disturbance and treatment fields as the backbone, then use IDS as optional enrichment for disturbance severity, spatial context, and named pest/damage agents. FIA has the plot-level repeated inventory, seedlings, saplings, forest type, disturbance history, and climate coordinates needed for a defensible first version. IDS is powerful, but a spatial overlay with fuzzed FIA coordinates adds enough uncertainty that it should not be the first classification backbone.

The current repository already has most of the FIA summary infrastructure. The main gap is that the current plot-level seedling summary intentionally collapses species identity. That is reasonable for dashboard-level plot metrics, but it is not sufficient for this recruitment and thermophilization analysis.

## Why The Seedling Summary Drops Species Identity

The seedling extraction step does **not** drop species identity.

`05_fia/scripts/04_extract_seedlings_mortality.R` reads the FIA `SEEDLING` table and writes per-state seedling parquets aggregated by:

```text
PLT_CN, INVYR, SPCD, SFTWD_HRDWD
```

This means species identity is preserved in the intermediate seedling product. The output is documented as:

```text
05_fia/data/processed/seedlings/state={ST}/seedlings_{ST}.parquet
```

The species code is dropped later, in `05_fia/scripts/05_build_fia_summaries.R`, when the pipeline builds:

```text
05_fia/data/processed/summaries/plot_seedling_metrics.parquet
```

That summary has one row per `PLT_CN x INVYR`. It keeps total seedling counts, softwood/hardwood counts, seedling species richness, and count-weighted Shannon diversity. It drops `SPCD` because the purpose of that product is a compact plot-year summary that joins easily to other plot-year metrics.

That design makes sense for broad reporting, dashboards, and quick summaries. It does **not** make sense as the only seedling product for this project, because the core response is species composition: which species recruit after disturbance, and whether those species have warmer or drier climate affinities.

Decision for this analysis:

- Keep `plot_seedling_metrics.parquet` unchanged.
- Add or preserve a species-level seedling product for recruitment composition.
- Use the plot-level summary only for total density, richness, and diversity
  response variables.

## Current Data Reality

### FIA Sampling Hierarchy

FIA data are hierarchical, and each level answers a different part of the question.

| Level | FIA tables | What it contributes |
|---|---|---|
| Plot visit | `PLOT` | Inventory year, coordinates, plot status, state/county/unit/plot identifiers |
| Condition | `COND` | Forest type, forested status, condition area proportion, disturbance and treatment history |
| Subplot | `SUBPLOT`, `SUBP_COND` | Subplot-level condition mapping and local site attributes |
| Trees | `TREE` | Live/dead trees, saplings, basal area, diameter classes, crown class, damage agents |
| Seedlings | `SEEDLING` | Tree regeneration below 1 inch diameter by species on microplots |
| P2 vegetation | `P2VEG_SUBPLOT_SPP`, `P2VEG_SUBP_STRUCTURE` | Shrubs, forbs, grasses, and broader understory cover on intensified plots |
| Invasives | `INVASIVE_SUBPLOT_SPP` | Invasive plant species presence/cover on sampled plots |

The ecological interpretation depends on keeping these sampling frames separate. Seedlings are tree regeneration, not the full understory community. Shrubs, forbs, grasses, and invasives require the P2 vegetation and invasive tables, which are not yet part of the production FIA pipeline.

### Plot Identifiers

`PLT_CN` is the key used to join FIA child tables within a plot visit. It is the correct join key for `PLOT`, `COND`, `TREE`, `SEEDLING`, and related records from the same inventory record.

Longitudinal analysis needs a stable plot identity across inventory visits. Use one of these approaches:

1. Preferred first-pass stable id:

   ```text
   stable_plot_id = paste(STATECD, UNITCD, COUNTYCD, PLOT, sep = "_")
   ```

2. More rigorous repeated-plot chain:

   ```text
   PLOT.PREV_PLT_CN
   ```

The first-pass stable id is simple and transparent. The `PREV_PLT_CN` chain is better for edge cases where plot records are rekeyed across cycles, but it adds implementation complexity. The plan should start with the stable state/unit/county/plot id and validate repeated-visit linkage before modeling.

### Forested Plot Gate

FIA samples all land, not only forest. The analysis population must be restricted to forested FIA plot-visits before asking about forest recovery.

Use `plot_exclusion_flags.parquet` and condition metadata to define:

```text
pct_forested = sum(CONDPROP_UNADJ where COND_STATUS_CD == 1)
```

Recommended v1 population:

```text
pct_forested >= 0.5
```

Sensitivity checks should repeat the analysis with stricter thresholds, such as `pct_forested >= 0.75` and `pct_forested == 1`, because mixed-condition plots can create ambiguous responses.

### Current Repository Strengths

The repo already has:

- FIA condition disturbance history in `plot_disturbance_history.parquet`.
- FIA treatment history in `plot_treatment_history.parquet`.
- Plot-level exclusion flags in `plot_exclusion_flags.parquet`.
- Plot-level tree metrics in `plot_tree_metrics.parquet`.
- Plot-level seedling richness/diversity totals in `plot_seedling_metrics.parquet`.
- Species lookup data in `05_fia/lookups/ref_species.parquet`.
- FIA site climate products in `05_fia/data/processed/site_climate/`.

The repo still needs:

- Stable repeated-plot identifiers in FIA outputs.
- Species-level seedling composition product carried into summaries.
- Seedling `TPA_UNADJ` or another explicit density field retained from the raw
  `SEEDLING` table.
- P2 vegetation and invasive plant products for shrubs, forbs, grasses, and
  invasives.
- Species climate-affinity traits.

## Required Data Products

### 1. Keep Existing Plot-Level Seedling Summary

Keep:

```text
05_fia/data/processed/summaries/plot_seedling_metrics.parquet
```

Purpose:

- Quick plot-year seedling totals.
- Seedling richness and Shannon diversity.
- Joining to plot-level tree, disturbance, and climate summaries.

Do not use this product to identify which species are recruiting.

### 2. Add Species-Level Seedling Composition

Add a new analysis-ready product, for example:

```text
05_fia/data/processed/summaries/plot_seedling_species.parquet
```

Recommended grain:

```text
stable_plot_id x PLT_CN x INVYR x STATECD x CONDID x SUBP x SPCD
```

Recommended columns:

| Column | Reason |
|---|---|
| `stable_plot_id` | Links repeated visits for before/after and control matching |
| `PLT_CN` | Joins to current FIA visit-level products |
| `INVYR` | Inventory year |
| `STATECD`, `UNITCD`, `COUNTYCD`, `PLOT` | Transparent stable id components |
| `CONDID` | Links seedlings to forest type and disturbance condition |
| `SUBP` | Preserves microplot/subplot sampling location |
| `SPCD` | FIA species code, required for recruitment composition |
| `COMMON_NAME`, `SCIENTIFIC_NAME`, `GENUS`, `SPECIES` | Human-readable species identity |
| `SFTWD_HRDWD`, `WOODLAND` | Functional group and measurement context |
| `TREECOUNT` | Raw seedling count |
| `TREECOUNT_CALC` | FIA count used in calculations, where available |
| `TPA_UNADJ` | Seedlings-per-acre expansion factor, where available |
| `seedlings_tpa` | Expanded seedling density, preferably `sum(TPA_UNADJ)` if valid |

Reasoning:

Keeping `CONDID` and `SUBP` avoids prematurely mixing seedlings from different conditions within a plot. For some analyses, aggregation to `stable_plot_id x INVYR x SPCD` will be useful, but that should be a downstream step, not the only stored product.

### 3. Add Plot/Condition Metadata With Stable Id

Add or extend a metadata product, for example:

```text
05_fia/data/processed/summaries/plot_condition_metadata.parquet
```

Recommended grain:

```text
stable_plot_id x PLT_CN x INVYR x CONDID
```

Recommended columns:

- Stable id components: `STATECD`, `UNITCD`, `COUNTYCD`, `PLOT`,
  `stable_plot_id`.
- Visit identifiers: `PLT_CN`, `INVYR`.
- Coordinates: `LAT`, `LON`, with a note that public FIA coordinates are fuzzed.
- Forest attributes: `COND_STATUS_CD`, `CONDPROP_UNADJ`, `FORTYPCD`,
  forest type label.
- Disturbance fields: `DSTRBCD1-3`, `DSTRBYR1-3`.
- Treatment fields: `TRTCD1-3`, `TRTYR1-3`.
- Derived flags: forested, fire, crown fire, insect, disease, wind, drought,
  human disturbance, cutting/harvest.

Reasoning:

Most errors in this analysis would come from joining the right biological data to the wrong condition, visit, or disturbance record. A single documented condition metadata table lowers that risk.

### 4. Add P2 Vegetation And Invasive Products

To capture the full community, add raw and summary products for:

```text
P2VEG_SUBPLOT_SPP
P2VEG_SUBP_STRUCTURE
INVASIVE_SUBPLOT_SPP
REF_PLANT_DICTIONARY
REF_INVASIVE_SPECIES
```

Recommended first products:

- `p2veg_subplot_species.parquet`: species cover by subplot, condition, layer, growth habit, and plant code.
- `p2veg_subplot_structure.parquet`: cover by layer and growth habit.
- `invasive_subplot_species.parquet`: invasive species cover by subplot and condition.

Reasoning:

Seedlings answer tree recruitment. They do not answer whether shrub, forb, or grass diversity changes after disturbance. P2VEG and invasive tables should be added as a second phase because they are sampled on a subset of plots and need their own missingness/sampling-status checks.

### 5. Add Species Climate-Affinity Traits

Add a trait table, for example:

```text
processed/traits/species_climate_affinity.parquet
```

Recommended grain:

```text
species_source x species_code
```

For FIA tree seedlings:

```text
species_source = "FIA_SPCD"
species_code = SPCD
```

Recommended fields:

- `temp_mean`, `temp_p10`, `temp_p90`, `temp_min`, `temp_max`.
- `precip_mean`, `precip_p10`, `precip_p90`, `precip_min`, `precip_max`.
- `cwd_mean`, `cwd_p10`, `cwd_p90`, `cwd_min`, `cwd_max`.
- `n_occurrences`, `n_states`, `occurrence_period`.
- `trait_method`, such as `fia_occurrence_climate_1981_2010`.

Reasoning:

The boss's idea is to use each species' range and climate envelope as a proxy for thermal tolerance and drought adaptation. The simplest defensible v1 is to estimate those envelopes from FIA occurrence locations joined to baseline climate. Later versions can compare against external range maps or TRY/BIEN/USDA trait data.

## Analysis Workflow And Reasoning

### Step 1. Define The Analysis Population

Filter to FIA plot-visits that are meaningfully forested:

```text
pct_forested >= 0.5
```

Exclude or flag:

- Human-induced disturbance, `DSTRBCD == 80`.
- Cutting/harvest treatment, `TRTCD == 10`.
- Incidental harvest mortality, `AGENTCD 80-89`.
- Nonsampled forest conditions, `COND_STATUS_CD == 5`, depending on analysis sensitivity.

Reasoning:

The goal is ecological recovery after natural or extreme disturbance, not land conversion, access problems, or harvest treatment. Nonforest and partially sampled plots can look like low regeneration or high turnover for reasons that are not biological recovery.

### Step 2. Build A Stable Repeated-Plot Panel

Create `stable_plot_id` and summarize visits per stable plot:

```text
stable_plot_id = STATECD + "_" + UNITCD + "_" + COUNTYCD + "_" + PLOT
```

For each stable plot, determine:

- First inventory year.
- Last inventory year.
- Number of visits.
- Whether there is a pre-disturbance visit.
- Whether there is a post-disturbance visit.

Reasoning:

Thermophilization is a change question. A pure cross-sectional comparison can be useful, but the strongest evidence comes from comparing the same plot or matched plots before and after disturbance.

### Step 3. Classify Disturbance From FIA

Create disturbance classes from `COND.DSTRBCD1-3` and treatment fields.

Recommended v1 classes:

| Class | FIA codes or fields | Notes |
|---|---|---|
| fire | `30`, `31`, `32` | Crown fire `32` is the strongest FIA high-severity fire proxy |
| insect | `10`, `11`, `12` | Use damage-agent tables to identify bark beetle subsets where possible |
| disease | `20`, `21`, `22` | Pair with tree damage-agent categories where possible |
| wind | `52` | Wind, hurricane, tornado |
| drought/weather | `50`, `54` | Keep drought separate when sample size allows |
| human/deforestation | `80`, `TRTCD == 10`, low post-forest proportion | Usually exclude from natural-disturbance analysis |
| control | no disturbance codes, no harvest/treatment, forested | Matched within forest type/geography/climate |

Reasoning:

FIA disturbance codes are condition-level field observations. They are more directly tied to the plot than IDS polygons, but they are coarse. Use them to build the first analysis, then enrich with IDS where severity or named agents are needed.

### Step 4. Treat High Severity Carefully

Do not assume every FIA disturbance code is high severity.

Recommended high-severity proxies:

- Fire: prioritize crown fire `DSTRBCD == 32`; compare against all fire `30-32` as a sensitivity check.
- Bark beetle/insect: combine `DSTRBCD 10-12` with tree mortality, live-tree damage agents, or IDS bark beetle polygons when available.
- Disease: combine `DSTRBCD 20-22` with disease damage-agent categories and mortality where available.
- Windfall: use `DSTRBCD == 52`; consider overstory basal-area loss if repeated tree data are available.
- Deforestation: use `TRTCD == 10`, `DSTRBCD == 80`, and forested proportion changes. Treat this as a separate human-disturbance endpoint, not a natural disturbance class.

Reasoning:

The phrase "high severity" is ecologically important, but FIA codes alone do not always encode severity. A conservative high-severity proxy prevents the analysis from overclaiming.

### Step 5. Define Controls

Controls should be untreated, unimpacted plot-visits matched to disturbed plots.

Recommended matching variables:

- Same or similar `FORTYPCD`.
- Same broad region: West vs East, or EPA ecoregion if added.
- Similar baseline climate: temperature, precipitation, and CWD.
- Similar pre-disturbance basal area and species richness where available.
- Similar inventory period to avoid temporal sampling confounding.

Reasoning:

Forest type and climate strongly control which species can recruit. Without matching, a "disturbance effect" could simply be a ponderosa pine forest being compared to a maple-beech-birch forest.

### Step 6. Measure Recruitment Composition

Use species-level seedlings as the main recruitment response.

Core metrics by plot visit:

- Total seedling density.
- Seedling species richness.
- Seedling Shannon diversity.
- Species composition: abundance or density by `SPCD`.
- Share of seedlings that are softwoods/hardwoods.
- Share of seedlings from warm-affinity species.
- Share of seedlings from drought-affinity species.

Use saplings from `TREE` as a complementary response:

- Sapling basal area or trees-per-acre by species.
- Sapling community-weighted climate affinity.
- Comparison of seedling vs sapling signals.

Reasoning:

Seedlings capture recent recruitment. Saplings integrate a longer establishment window. If both layers shift toward warmer or drier species after disturbance, the thermophilization signal is stronger.

### Step 7. Build Species Climate-Affinity Traits

For each tree species, join occurrence locations to baseline climate and compute species-level climate envelopes.

Recommended baseline:

```text
1981-2010 climate normals
```

Recommended variables:

- Annual mean maximum temperature or mean temperature.
- Annual precipitation.
- Annual climate water deficit (`def`, PET minus AET).

Recommended species metrics:

- Mean climate across occurrences.
- 10th and 90th percentile climate.
- Min and max climate, used cautiously because outliers and fuzzed coordinates can be influential.
- Occurrence count and geographic coverage.

Reasoning:

Mean climate affinity is easy to interpret as a species' realized climate center. Quantiles are more robust than raw min/max. CWD is especially useful because "drought adapted" is not just low precipitation; it also reflects evaporative demand.

### Step 8. Calculate Community-Weighted Climate Affinity

For each plot visit, calculate seedling community-weighted means:

```text
CWM_temp = sum(seedling_weight_i * species_temp_mean_i)
CWM_cwd  = sum(seedling_weight_i * species_cwd_mean_i)
CWM_pr   = sum(seedling_weight_i * species_precip_mean_i)
```

Where `seedling_weight_i` is based on:

- Expanded density (`TPA_UNADJ`) when available and validated.
- Raw `TREECOUNT` as a sensitivity check.
- Presence/absence as a second sensitivity check for composition-only signals.

Reasoning:

Community-weighted means translate species identity into a plot-level ecological signal. If post-disturbance recruitment shifts toward species with higher temperature affinity or higher CWD affinity, that is direct evidence of thermophilization or xerophilization in recruitment.

### Step 9. Estimate Disturbance Effects

Compare disturbed plots to controls using:

- Before/after change where pre-disturbance visits exist.
- Matched post-disturbance comparisons where before/after is not available.
- Time-since-disturbance gradients where disturbance year is known.

Recommended effect metrics:

```text
delta_CWM_temp = post_CWM_temp - pre_CWM_temp
delta_CWM_cwd  = post_CWM_cwd  - pre_CWM_cwd
```

Reasoning:

The cleanest test is whether disturbed plots move farther toward warm/dry affinity than comparable controls over the same time period.

### Step 10. Compare West And East

Create a region variable:

- First-pass: West vs East using state groups.
- Better later version: ecological regions or climate regions.

Test:

```text
disturbance_class x region
```

Reasoning:

The working hypothesis is spatially explicit: warm- and drought-adapted trees should be recruiting more in the West after disturbance, while the East may show weaker or different shifts. The model should represent that expectation directly rather than only comparing national averages.

### Step 11. Add IDS Enrichment After FIA V1

Use IDS after the FIA-first workflow is validated.

Potential IDS additions:

- Bark beetle polygons near FIA plots.
- Percent affected / severity class where available.
- Damage agent names from IDS `DCA_CODE`.
- Surveyed-area context to distinguish absence of disturbance from absence of survey.

Reasoning:

IDS can improve severity and agent attribution, but FIA coordinates are fuzzed and IDS polygons vary in precision and methodology through time. The overlay should be treated as approximate enrichment, not as the first-pass truth.

## Modeling Plan

### Primary Response

Primary response:

```text
post-disturbance seedling CWM temperature affinity
post-disturbance seedling CWM CWD affinity
```

Core interpretation:

- Higher CWM temperature after disturbance suggests thermophilization.
- Higher CWM CWD affinity suggests recruitment by species associated with drier or higher water-deficit environments.
- Lower precipitation affinity can be interpreted as a related dry-affinity signal, but CWD is usually more mechanistic.

### Secondary Responses

Secondary responses:

- Seedling species richness.
- Seedling Shannon diversity.
- Total seedling density.
- Warm-affinity species share.
- Drought-affinity species share.
- Sapling CWM temperature and CWD affinity.
- Invasive cover or invasive richness where `INVASIVE_SUBPLOT_SPP` is available.
- Shrub, forb, and graminoid richness/cover where P2VEG is available.

### Recommended Model Families

Use mixed models or matched comparisons, depending on data support.

Recommended repeated-plot model:

```text
response ~ disturbance_class * region + time_since_disturbance +
           baseline_climate + baseline_structure + forest_type +
           (1 | stable_plot_id)
```

Recommended matched-control model:

```text
response ~ disturbed_vs_control * region + disturbance_class +
           baseline_climate + forest_type + inventory_year
```

Recommended before/after difference model:

```text
delta_response ~ disturbance_class * region +
                 delta_years + baseline_climate + forest_type
```

Reasoning:

Mixed models use repeated measurements where available. Matching controls reduces confounding when true before/after records are sparse. Difference models are easiest to explain to collaborators and can be the headline if sample sizes are adequate.

### Covariates

Include:

- Forest type (`FORTYPCD`).
- Region.
- Baseline climate.
- Inventory year or period.
- Time since disturbance.
- Pre-disturbance basal area or canopy structure where available.
- Disturbance class.

Avoid overfitting:

- Do not include highly correlated climate variables in the same small model without checking collinearity.
- Do not split into too many disturbance classes if sample sizes are thin.
- Keep human/harvest/deforestation separate from natural disturbances.

## Implementation Roadmap

### Phase 1. Document And Validate Existing Seedling Products

Tasks:

- Confirm that per-state `seedlings_{ST}.parquet` retains `SPCD`.
- Confirm that `plot_seedling_metrics.parquet` reproduces totals from species-level seedling products.
- Update workflow documentation to distinguish species-level seedling products from plot-level seedling summaries.

Expected outcome:

- The current summary is understood as a plot metric, not the recruitment composition table.

### Phase 2. Add Stable Plot And Condition Metadata

Tasks:

- Extend the FIA condition extraction or summary builder to retain `UNITCD`, `COUNTYCD`, and `PLOT` from the raw `PLOT` table.
- Derive `stable_plot_id`.
- Write a condition metadata summary that preserves `PLT_CN x INVYR x CONDID`.
- Carry forest type labels from `ref_forest_type.parquet`.

Expected outcome:

- Every response table can join to stable plot id, condition, forest type, and disturbance history.

### Phase 3. Add Species-Level Seedling Summary

Tasks:

- Extend seedling extraction to read `STATECD`, `UNITCD`, `COUNTYCD`, `PLOT`, `TREECOUNT_CALC`, and `TPA_UNADJ` when available.
- Join species names from `ref_species.parquet`.
- Write `plot_seedling_species.parquet`.
- Keep current `plot_seedling_metrics.parquet`.

Expected outcome:

- Species recruitment can be analyzed directly without breaking existing demos.

### Phase 4. Build Disturbance Classification

Tasks:

- Create a small reusable disturbance lookup/categorization helper.
- Classify condition-level disturbance and treatment fields.
- Flag fire, crown fire, insect, disease, wind, drought, human disturbance, and harvest.
- Create control eligibility flags.

Expected outcome:

- The same disturbance definitions are used in all analyses and figures.

### Phase 5. Build Species Climate-Affinity Traits

Tasks:

- Build annual or baseline climate summaries for FIA plot locations.
- Join species occurrence records to baseline climate.
- Calculate species climate envelopes for temperature, precipitation, and CWD.
- Write a trait table with occurrence counts and method metadata.

Expected outcome:

- Each FIA tree species has a transparent climate-affinity estimate.

### Phase 6. Build Analysis Tables

Tasks:

- Join seedling species composition to condition metadata.
- Join species climate-affinity traits.
- Aggregate to plot-visit community metrics.
- Build disturbed and matched-control datasets.
- Build pre/post datasets where repeated visits support them.

Expected outcome:

- Analysis-ready tables exist before modeling starts.

### Phase 7. Model And Summarize

Tasks:

- Fit primary models for seedling CWM temperature and CWD affinity.
- Fit secondary models for richness, diversity, density, and warm/dry species shares.
- Run West vs East interaction tests.
- Run sensitivity checks.
- Produce summary figures and tables.

Expected outcome:

- The analysis can answer whether disturbance is associated with warmer/drier recruitment, where that signal is strongest, and how robust it is.

## Test And Validation Plan

### Data Product Tests

- Validate that the species-level seedling product has the documented grain.
- Check that `SPCD` is never dropped from species-level products.
- Check that plot-year totals from species-level seedlings reproduce `plot_seedling_metrics.parquet`.
- Confirm `stable_plot_id` links multiple visits for repeated plots.
- Confirm condition metadata has one row per `PLT_CN x INVYR x CONDID`.
- Confirm disturbance labels reproduce the documented FIA code groups.

### Ecological Sanity Checks

- Map sample sizes by disturbance class, forest type, state, and region.
- Check that fire-heavy western states show plausible fire sample counts.
- Check that eastern states have plausible lower fire and higher disease/insect representation.
- Inspect top recruiting species by disturbance class and region.
- Confirm that species climate-affinity rankings are ecologically plausible.

### Modeling Sensitivity Checks

- Repeat with `pct_forested >= 0.75` and `pct_forested == 1`.
- Repeat with and without condition status 5 exclusions.
- Compare raw `TREECOUNT`, expanded `TPA_UNADJ`, and presence/absence weights.
- Compare crown fire only vs all fire.
- Compare FIA-only insect disturbance vs FIA plus IDS bark beetle enrichment.
- Compare before/after results to matched-control cross-sectional results.

## Assumptions And Defaults

- The v1 disturbance backbone is FIA condition disturbance/treatment history.
- IDS is optional enrichment for severity and named agents.
- Current plot-level summary products stay in place.
- Species-level seedling composition is added as a new product.
- Public FIA coordinates are treated as approximate because they are fuzzed.
- West vs East is a first-pass hypothesis grouping; later versions should use ecological or climate regions.
- Climate affinity is based on realized occurrence climate, not physiological tolerance.
- CWD is treated as the primary drought-affinity variable because it combines water supply and evaporative demand.

## Expected Interpretation

Evidence for thermophilization would look like:

- Post-disturbance seedling communities have higher CWM temperature affinity than matched controls.
- The increase is stronger in the West than in the East.
- The signal is strongest after severe fire, bark beetle mortality, disease, or drought/weather disturbance.
- Saplings show a similar but potentially weaker or lagged pattern.

Evidence against the hypothesis would look like:

- Disturbed and control plots show similar CWM temperature and CWD affinity.
- Richness or density changes occur without directional climate-affinity shifts.
- Apparent shifts disappear after matching by forest type and baseline climate.
- Eastern and western forests show similar responses once forest type and climate are controlled.

The goal is not only to test whether diversity changes after disturbance. The central goal is to test whether the identity of recruiting species shifts toward species whose realized ranges are warmer or drier.
