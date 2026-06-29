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

## Current Status

Tracks the Implementation Roadmap (below). Update this section as phases complete.

| Phase | Status | Notes |
| --- | --- | --- |
| 1. Document and validate existing seedling products | Done | `plot_seedling_metrics.parquet` keeps plot-level totals; species identity preserved upstream in `seedlings/state={ST}/seedlings_{ST}.parquet`. |
| 2. Stable plot and condition metadata | Done | `cond` extract carries `STATECD/UNITCD/COUNTYCD/PLOT/stable_plot_id` plus `LAT/LON/ELEV`. `plot_condition_metadata.parquet` produced by [build_condition_metadata.R](../05_fia/scripts/summaries/build_condition_metadata.R). |
| 3. Species-level seedling summary | Done | `plot_seedling_species.parquet` produced by [build_seedling_species.R](../05_fia/scripts/summaries/build_seedling_species.R). |
| 4. Disturbance classification | Done | `plot_disturbance_classification.parquet` and `plot_treatment_history.parquet` produced by the corresponding builder modules under `05_fia/scripts/summaries/`. |
| 5. Species climate-affinity traits | Done | `species_climate_affinity.parquet` written by [01_build_species_climate_affinity.R](../06_traits/scripts/01_build_species_climate_affinity.R): 603 species, 925,947 unique (stable_plot, SPCD) occurrences, 1981-2010 baseline. Spot-checks for Douglas-fir, ponderosa, sugar maple, loblolly all landed in expected ecological zones. |
| 6. Build analysis tables | In progress | Done: per-plot CWM ([02_build_plot_recruitment_cwm.R](../06_traits/scripts/02_build_plot_recruitment_cwm.R)) -> `plot_recruitment_cwm.parquet` (475,055 conditions). Done: disturbed-to-control matching ([03_match_disturbed_to_controls.R](../06_traits/scripts/03_match_disturbed_to_controls.R)) -> `plot_matches.parquet` (57,055 / 57,062 disturbed plots matched, K=5, median match distance 0.013 SD). |
| 7. Model and summarize | In progress | First stratified summaries ([04_stratified_thermophilization.R](../06_traits/scripts/04_stratified_thermophilization.R)) written. See "Findings So Far" below. |

## Findings So Far

First-pass national results from [04_stratified_thermophilization.R](../06_traits/scripts/04_stratified_thermophilization.R). For every disturbed FIA plot we found the 5 most climate-similar undisturbed plots in the same forest type and region, then compared the recruiting seedlings on the disturbed plot to those on its matched controls. 57,055 of 57,062 disturbed plots had usable matches (drawn from a pool of 330,437 control plots). Results are then averaged within each combination of disturbance class and region (East / West), with 95% confidence intervals from 1,000 bootstrap resamples.

How to read the numbers:

- A positive **delta_temp** means seedlings on disturbed plots favor warmer-climate species than seedlings on the matched undisturbed plots -- the "thermophilization" signal.
- A positive **delta_cwd** means seedlings favor more drought-tolerant species (higher climate water deficit) than the matched controls.
- 95% confidence intervals (`_lo` / `_hi`) tell you whether the effect is reliably different from zero. If both bounds are above zero, the warming/drying signal is real for that group.

### What the data say about the West > East prediction

The original hypothesis (more thermophilization in the West, especially after severe disturbance) is **partly supported on drought tolerance but not on temperature**, and two unexpected patterns turned up that are worth investigating in their own right.

| Signal | Reading |
| --- | --- |
| Fire, temperature | East +0.18 deg C [0.14, 0.22], West +0.13 deg C [0.07, 0.18]. Both regions show significant warm-affinity recruitment after fire, but the East signal is *larger* than West. The "less change in East" prediction does not hold for temperature. |
| Fire, CWD | East +4.8 mm [3.7, 5.9], West +6.6 mm [2.3, 11.5]. Both regions show drought-affinity recruitment, with West larger as predicted. **The boss's hypothesis is alive on the CWD axis.** |
| Crown fire (high-severity), West | n=680, delta_temp +0.14 [0.03, 0.26], delta_cwd +6.3 [-3.2, 14.7]. Severe fire in the West does drive modest thermophilization, with a marginal drier-affinity signal. |
| Crown fire (high-severity), East | n=35. Too small to read despite large point estimates. |
| Insect disturbance | East -0.14 [-0.18, -0.11], West -0.10 [-0.13, -0.06]. Both regions recruit *cooler*-affinity species after insect mortality. Real signal (CIs exclude zero), opposite direction from hypothesis. |
| Disease, East | +0.42 deg C [0.38, 0.46]. The largest delta_temp signal in any cell, larger than any fire signal. |
| Disease, West | Null. |
| Weather (wind/drought/other) | Null in both regions for temperature and inconsistent for CWD. |

### How the signals change over time

From [05_thermophilization_by_class_time.R](../06_traits/scripts/05_thermophilization_by_class_time.R) and `thermophilization_by_class_time_region.parquet`:

- **Fire's effect on recruitment lasts for years.** Plots burned 0-5 years ago and plots burned 5-10 years ago show roughly the same level of warm-affinity recruitment in both regions (East: +0.17 -> +0.20 deg C; West: +0.15 -> +0.11 deg C). The drier-affinity (CWD) signal is also steady or slightly larger at 5-10 years (East: +4.7 -> +7.0 mm; West: +4.9 -> +7.3 mm). So fire is not just a one-time pulse that fades quickly; recruitment keeps trending warmer/drier for at least a decade.
- **Insect cooling in the West holds steady.** West insect plots show the same -0.10 deg C delta at 0-5 years and 5-10 years -- not a short-lived blip after the outbreak.
- **Disease East shows a growing signal.** Disease-East plots are +0.15 deg C at 0-5 years but +0.34 deg C at 5-10 years, with a large drier-affinity jump at 5-10 years (+18.7 mm CWD). This looks like a real and possibly intensifying pattern, but only a small fraction of disease plots have a usable disturbance year (see next caveat), so this is a partial picture.
- **The 10-20 year bins should be ignored.** Cell sample sizes drop to n = 2-79 and the bootstrap confidence intervals get very wide. The -1.66 deg C fire-East 10-20 yr cell (n=13) is almost certainly noise from the small sample, not a real signal.

### What's driving the disease-East signal: northern hardwoods

From [06_disease_east_drilldown.R](../06_traits/scripts/06_disease_east_drilldown.R), `disease_east_by_forest_type.parquet`, and `disease_east_top_recruits.parquet`:

The +0.42 deg C cross-sectional disease-East signal is not spread evenly across the forest types in that group -- it is overwhelmingly concentrated in **maple-beech-birch (northern hardwood) forests**. 3,115 of the 5,883 disease-East plots (53%) sit in this single forest type, and they show **delta_temp = +0.74 [+0.69, +0.80]** with a tight CI. This is the forest type where beech bark disease and beech leaf disease are most active. The introduced-pest explanation looks well-supported.

Three smaller positive signals also show up:

- Oak-gum-cypress (bottomland hardwoods, n=151): +0.68 deg C [+0.42, +0.93]. Possibly laurel wilt killing southern Lauraceae (redbay, sassafras).
- Aspen-birch (n=179): +0.53 deg C [+0.20, +0.85].
- Loblolly-shortleaf pine (n=373): +0.18 deg C [+0.02, +0.35].

Three forest types show *cooling* (negative delta_temp) under disease, which the previous pooled analysis hid:

- Spruce-fir (n=166): -0.53 deg C [-0.76, -0.30]. Likely balsam woolly adelgid killing Fraser/balsam fir, with red spruce and other boreal recruits filling in (even cooler-affinity than the firs being lost).
- Longleaf-slash pine (n=211): -0.31 deg C [-0.64, +0.02], marginal.
- Elm-ash-cottonwood (n=187): -0.26 deg C [-0.58, +0.03], marginal. Worth investigating given emerald ash borer should produce *warmer*-affinity recruits, not cooler.

The top 20 most-recruiting species at disease-East plots are dominated by cool-affinity northern conifers (balsam fir, red spruce, white pine). This is not a contradiction of the warming signal: the list ranks total counts across all forest types, while the CWM signal is the per-plot mean *relative to climatically-matched undisturbed plots in the same forest type*. The warming signal doesn't require warm-affinity species to dominate the recruits in absolute terms; it only requires that the disease plots have slightly more warm-affinity recruits than the matched controls.

The next step is to identify *which species* are over-represented at disease plots within the maple-beech-birch forest type, compared to matched control plots in the same forest type. That directly identifies the species driving the +0.74 deg C signal.

### The maple-beech-birch warming signal is a beech sprout response, not classic thermophilization

From [07_disease_mbb_species_shift.R](../06_traits/scripts/07_disease_mbb_species_shift.R) and `disease_mbb_species_comparison.parquet`:

The single biggest species shift on maple-beech-birch disease plots is American beech *increasing*, not decreasing. Beech is **54% of the seedling community at disease plots vs 21% at matched controls** -- a delta_share of +0.33, and more than twenty times larger than the next biggest positive shift (striped maple at +0.013). Every other species shift is small by comparison.

This is the well-known **beech sprout response to beech bark disease**. When mature beech are damaged by the BBD scale-insect-plus-fungus complex (and increasingly by beech leaf disease), the dying trees produce prolific root suckers, creating dense beech thickets that dominate the seedling layer. The FIA disease code is correctly tagging BBD-affected plots, and what we're measuring is the well-documented ecological aftermath.

How this produces a "warming" signal:

- Beech temp_mean = 10.23 deg C, which is the warmest of the species commonly recruiting in northern hardwood forests.
- Species losing share at disease plots are mostly cool-affinity: balsam fir (4.84 deg C, -0.064), sugar maple (8.4 deg C, -0.047), eastern white pine (7.9 deg C, -0.013), eastern hemlock (7.8 deg C, -0.009). Red maple also loses share (-0.027) -- beech sprouts apparently outcompete even the classic gap-filler.
- Share-weighted mean climate affinity of species gaining ground: 9.96 deg C. Of species losing ground: 8.52 deg C. Difference: +1.44 deg C, consistent with the +0.74 deg C plot-level CWM shift.

Implication: the +0.74 deg C MBB disease signal is mathematically real but ecologically **not "thermophilization"** in the classic sense (warm-climate species advancing into newly opened gaps). It is closer to **disease-driven monodominance**: BBD damages mature beech -> beech root sprouts dominate -> community-weighted mean shifts toward beech's climate affinity, which happens to be warmer than the diverse cool-conifer / sugar-maple mix at undisturbed plots. Whether to count this as a thermophilization finding or to separate it out is a judgment call for the boss; both readings are defensible. The honest framing is that the disease signal in northern hardwoods is dominated by beech bark disease's sprout response, with the warmer-affinity recruiting community as a byproduct.

Notable side findings:

- **White ash is under-represented at MBB disease plots (-0.048)**. Consistent with emerald ash borer killing both adult ash (the seed source) and ash regeneration. Ash temp_mean (10.4) is similar to beech, so the loss does not strongly push CWM in either direction, but it is a real EAB fingerprint visible in the data.
- **Red maple is under-represented (-0.027)** in spite of its usual gap-fill behavior in eastern forests. Beech sprouting is dense enough to suppress even red maple regeneration.

Active follow-up: re-run the MBB disease CWM with American beech excluded. If the +0.74 deg C signal disappears entirely, it is purely the beech sprout story. If a smaller residual signal remains (e.g. +0.2 deg C), there is a true climate-driven thermophilization layer underneath the beech response that deserves its own writeup.

### Caveat: many disturbed plots have no usable disturbance year

How often FIA records a real year (versus the "continuous/unknown" 9999 code) varies a lot by disturbance type. From `disturbance_year_coverage.parquet`:

| Class | East: fraction with known year | West: fraction with known year |
| --- | --- | --- |
| fire | 0.93 | 0.98 |
| weather | 0.76 | 0.56 |
| insect | 0.27 | 0.47 |
| other | 0.18 | 0.13 |
| disease | 0.12 | 0.09 |

The missing years almost all come from FIA's 9999 code, which is what FIA crews enter when a disturbance is gradual or ongoing rather than tied to a single year (slow forest decline, multi-year insect outbreaks, chronic disease). This makes biological sense.

What it means for the analysis:

- **Fire and weather:** time-since-disturbance breakdowns are trustworthy. Almost every plot has a real year.
- **Insect:** time bins are partially trustworthy (about half of plots have a real year).
- **Disease and "other":** time bins should be treated as suggestive only. Fewer than 1 in 5 plots have a real year, so the time-binned subset is small and possibly not representative of the full set of disease plots.
- **The cross-sectional `disturbance_class x region` summaries are unaffected.** They don't use disturbance year, so they remain valid for all classes including disease.

## Next Steps

Things to work on next, in order. Each finished item should add a row to "Findings So Far" or open a follow-up.

1. **Done.** Break the time-since-disturbance results out by disturbance class ([05_thermophilization_by_class_time.R](../06_traits/scripts/05_thermophilization_by_class_time.R)). The first version pooled all classes together, which made the fire (warming) and insect (cooling) signals cancel each other out. Results are written up in "How the signals change over time" above.
2. **Drill into the disease-East signal.** Break the +0.42 deg C disease-East result down by `forest_type_group` and look at which species are actually recruiting on those plots. Working idea: the signal is mostly coming from ash, hemlock, or beech forests being killed by introduced pests (emerald ash borer, hemlock woolly adelgid, beech bark disease) and replaced by warmer-affinity species. If true, a few forest types should carry most of the signal and the top recruits should be classic gap-fillers like red maple, yellow-poplar, or sweetgum.
3. **Figure out why insect plots show cooling.** Two possible explanations:
   (a) **Different mix of recruits.** The big trees that died were warm-affinity (e.g. southern pine being killed by southern pine beetle), and what's coming up underneath is the cooler-affinity understory that was already there.
   (b) **Insect-disturbed plots already sit in cooler places.** Maybe the matching didn't fully balance baseline climate between disturbed and control plots. Easy check: compare the `dist_temp_z` distributions for insect-disturbed plots vs their matched controls. If they overlap closely (which they should, given the median match distance was only 0.013 SD), then explanation (a) is the right one.
4. **Tighten the matching distance limit** (the "caliper" — the maximum allowed climate distance between a disturbed plot and its match) from 2 SD to 0.5 SD, and re-run. The current 2 SD limit is so loose that it almost never rejects a match. A stricter limit is a free double-check: if the same headline signals show up, they are not just an effect of including some so-so matches.
5. **Use a finer forest-type grouping** (e.g. raw `FORTYPCD` instead of `forest_type_group`) for the largest-signal cells (disease-East, fire). Confirms the signal is not just an effect of mixing many different forest types into one bucket.

Backlog (deferred until the active list above clarifies the headline picture). Each is its own analysis or data product:

1. **Within-plot pre/post analysis using `PREV_PLT_CN`** (Step 9 of the workflow). The strongest causal evidence available from FIA's panel design. Requires a stable repeated-plot panel where the same plot is observed both before and after a recorded disturbance.
2. **Mortality-derived severity** (originally Step 4 "option 2"). Replaces the presence-as-severity proxy for beetle/disease/wind by deriving severity from `TREE` mortality. Higher fidelity but more code; only needed if presence-based results disagree with mortality-based results.
3. **Saplings as a complementary recruitment signal**. CWM over `TREE` records below the merch-size threshold gives a longer-window companion to the seedling CWM. If saplings show the same pattern, the signal is robust across recruitment time scales.
4. **P2VEG and invasive plant products** (Phase 4 of the data plan). The boss explicitly emphasized shrubs, forbs, grasses. Required to claim "understory" in the manuscript rather than only "tree regeneration."
5. **Ecoregion stratification**. Replace the East/West split with EPA Level III ecoregions for the second-pass analysis.
6. **Effect-modification by elevation, baseline climate quintile, and forest age** as covariates. Where the climate signal is strongest within a class.

## Open Questions And Things To Double-Check

Checks and loose ends, not new analyses. Each should be answered (or set aside with a written reason) before the writeup stage.

### Are the species traits (climate envelopes) reasonable?

- Look at the top and bottom 20 species ranked by `temp_mean`. The top should include well-known warm-climate trees like sweetgum, longleaf pine, live oak, slash pine. The bottom should be cold-climate species like white spruce, subalpine fir, balsam fir. If they don't, the trait calculation has a problem.
- Re-run [02_build_plot_recruitment_cwm.R](../06_traits/scripts/02_build_plot_recruitment_cwm.R) with the rare-species cutoff raised from 30 to 50 and 100 occurrences. The headline plot-level CWM numbers should not change much. If they do, our results depend on which rare-species cutoff we picked.
- Confirm that the median fraction of seedlings with usable traits stays at 1.0 (essentially all seedlings get a trait) when the cutoff is raised. If it drops, the cutoff is throwing out ecologically meaningful species in some forest types.

### Is the matching working as intended?

- The 7 unmatched disturbed plots: check whether they cluster in one forest-type-and-region cell. If so, that cell has a coverage hole worth flagging; if they're scattered, they're just edge cases.
- For each forest-type-and-region cell, check that the disturbed and control plots ended up with very similar baseline climate after matching. Both groups' average `temp_z` and `prec_z` should be close to identical (within 0.1 SD). Given the very small median match distance (0.013 SD), this should pass easily.
- Re-run with K=3 and K=10 matched controls per disturbed plot (current K=5). Headline numbers should be similar. If they swing a lot, K matters more than expected and we should investigate.
- Re-run without allowing the same control to be reused for multiple disturbed plots. Bootstrap CIs already account for the reuse, so this is a sanity check, not a substantive design change.

### Is the disturbance classification clean?

- Why do ~32K (~56%) of disturbed plots have no `time_since_disturbance`? Working hypothesis: most are FIA's 9999 ("continuous/unknown") year codes. Quick check: count how many NA-year plots have `has_continuous_disturbance_year == TRUE`.
- Confirm that the "disturbed" group excludes anything tagged as harvest or treated. This *should* be true based on [build_disturbance_classification.R](../05_fia/scripts/summaries/build_disturbance_classification.R) line 178-186, but worth checking that no disturbance class is accidentally polluted with harvested plots.

### Are the confidence intervals stable?

- Re-run [04_stratified_thermophilization.R](../06_traits/scripts/04_stratified_thermophilization.R) with a different random seed. The CI bounds for the disease-East cell should shift by less than 0.005 deg C. If they shift more, the bootstrap count (currently 1000 resamples) is too low and should be raised to 5000.

### Are the means representative?

- For each of the largest-signal cells (disease-East, fire-West, insect-East), plot the distribution of per-plot deltas. The mean should sit roughly in the middle of the distribution. If a few extreme plots are pulling the mean far from the center, also report the median and middle 50% range alongside the mean and CI.

### Does the species composition match the climate signal?

- For the disease-East cell, list the top 20 most-recruiting species. Their average climate affinity should clearly be warmer than the species being killed in those forests.
- For the insect cell, list the top recruiting species. If they're subcanopy hardwoods that were already growing in the understory before the disturbance, that supports explanation (3a) in the active-items list above.

### Reporting consistency

- Pick one sign convention for the writeup and stick to it: present `delta_temp` as deg C, `delta_cwd` as mm, both labeled "disturbed minus matched control." Don't flip signs across figures.
- Decide how to report the East-fire +0.18 deg C signal, since it doesn't fit the original "more change in West" prediction. The honest reading: fire drives warmer-affinity recruitment in *both* regions on temperature, with the West showing a somewhat stronger drier-affinity (CWD) signal. The East/West story is more nuanced than the original framing.

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
- Write `05_fia/data/processed/summaries/plot_disturbance_classification.parquet`.

Expected outcome:

- The same disturbance definitions are used in all analyses and figures.
- Controls are explicitly defined as forested, untreated, and unimpacted plot conditions.

### Phase 5. Build Species Climate-Affinity Traits

Tasks:

- Build annual or baseline climate summaries for FIA plot locations.
- Join species occurrence records to baseline climate.
- Calculate species climate envelopes for temperature, precipitation, and CWD.
- Write a trait table with occurrence counts and method metadata.

Expected outcome:

- Each FIA tree species has a transparent climate-affinity estimate.

Phase 5 implementation notes:

- The committed `all_site_locations.csv` originally held 6,956 sites from a prior pilot extraction and only matched 198 of the ~410K national FIA plot locations. It was regenerated from the national `cond` extract by [01_build_site_list.R](../05_fia/scripts/site_climate/01_build_site_list.R), producing 408,040 distinct stable plot locations. `site_id` is set to `stable_plot_id` (e.g. `8_2_109_63473`) so downstream products can join climate without an intermediate coordinate lookup.
- Stale GEE checkpoints under `_gee_annual/` were removed before re-running [02_extract_terraclimate.R](../05_fia/scripts/site_climate/02_extract_terraclimate.R), since `pixel_id` is tied to the input site set.
- Re-extraction snapped 408,040 sites to 338,219 unique TerraClimate pixels and produced a 1.95B-row `site_climate.parquet` (5.85 GB) covering 1958-2024 monthly values for `tmmx`, `tmmn`, `pr`, `def`, `pet`, `aet`. Years 2025-2026 were skipped because TerraClimate has no data yet for those years.
- The trait builder [01_build_species_climate_affinity.R](../06_traits/scripts/01_build_species_climate_affinity.R) reads this climate file plus `cond`, `trees` (filtered to `STATUSCD == 1`), and `ref_species`, deduplicates occurrences to one `(stable_plot_id, SPCD)` row so revisited plots do not double-count, and writes mean / p10 / p90 / min / max for annual mean temperature, annual precipitation, and annual CWD per species. Output: `06_traits/data/processed/species_climate_affinity.parquet`.

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
