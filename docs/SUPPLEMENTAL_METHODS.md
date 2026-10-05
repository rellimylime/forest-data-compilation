# Supplemental methods working draft

This document consolidates the manuscript-facing methods that are otherwise distributed across the repository. It describes the implemented condition-level analysis in `09_analysis/` and identifies sensitivity analyses and unresolved extensions explicitly. Module READMEs and workflow documents remain the technical authority for product schemas, commands, and QA.

Status: working draft for Joan and coauthor review. Numerical sample counts and results should be inserted from generated QA tables after the current BIEN clipping sensitivity and tree-weighting sensitivity are complete.

## How method provenance is labeled

This supplement distinguishes source definitions from analytical choices:

- **FIA-defined or FIA-supplied** identifies raw FIADB fields, official classifications and codes, the national sampling design, and official remeasurement links.
- **Repository-derived from FIA fields or standards** identifies quantities calculated from FIA inputs, such as expanded basal area, life-stage labels applied from FIA diameter thresholds, and bounded measurement dates.
- **Analysis decision** identifies choices made for this study, including the 30% condition threshold, CWM weights, complete-history requirements, mortality risk-set construction, response definitions, and model form.

| Element | FIA contribution | Repository or analysis contribution |
|---|---|---|
| Tree diameter and expansion | FIA supplies `DIA` and `TPA_UNADJ`. | The repository calculates basal area per acre with the standard circular-stem formula. Choosing basal area versus stem density as a CWM weight is an analysis decision. |
| Seedling, sapling, and adult sampling classes | FIA defines the diameter thresholds and samples the classes on subplots or microplots. | The repository applies those thresholds to create explicit life-stage products; excluding seedlings from the active model is an analysis decision. |
| Forest condition | FIA supplies `COND_STATUS_CD` and `CONDPROP_UNADJ`; code 1 is FIA's accessible-forest classification. | Requiring `CONDPROP_UNADJ >= 0.30` at every endpoint is an analysis decision. |
| Remeasurement linkage | FIA supplies `PREV_PLT_CN`. | Following only valid official links, matching the same numeric `CONDID`, and requiring complete histories are analysis decisions. |
| Species and mortality agents | FIA supplies `SPCD`, mortality `AGENTCD`, and damage-agent codes. | Taxonomic joins, climate niches, risk-set construction, agent grouping, and cumulative percentages are repository-derived or analysis decisions. |
| Climate affinity and models | FIA supplies community observations and public plot coordinates. | BIEN/TerraClimate niche construction, CWM formulas, site-CWD accumulation, and statistical models are analysis methods. |

## 1. Data source and FIA sampling design

We used the USDA Forest Service Forest Inventory and Analysis Database (FIADB), version 9.4 (August 2025), acquired as official state-table archives from FIA DataMart. Raw acquisitions and checksums are recorded in `05_fia/data/raw/download_manifest.csv`. The analysis uses FIA plot, condition, tree, seedling, and tree growth-removal-mortality tables together with national species and forest-type reference tables.

**FIA-defined sampling design.** The national FIA design consists of four 24-ft-radius subplots per plot. FIA defines and tallies trees at least 5 inches in diameter on subplots; saplings 1.0–4.9 inches in diameter and seedlings below 1 inch are tallied on associated 6.8-ft-radius microplots. FIA supplies `DIA` and the per-acre expansion factor `TPA_UNADJ`; it does not supply the `ba_per_acre` field used by this repository. **Repository-derived quantity.** For a tree of diameter `DIA` inches, basal area was calculated with the standard forestry formula

```text
basal area (ft2 acre-1) = 0.005454 × DIA^2 × TPA_UNADJ.
```

Raw state tables were processed separately and then combined into national products. Tree records used for composition retained FIA-supplied species identity, plot visit, mapped condition, subplot, live/dead status, diameter, and expansion fields. **Repository-derived classifications.** We applied FIA's diameter thresholds to label adult trees (at least 5 inches) and saplings (1.0–4.9 inches), and calculated expanded stem density and basal area. **Analysis decision.** The active community response used live stems only.

Source details: `05_fia/WORKFLOW.md`; `05_fia/scripts/core/03_extract_trees.R`; `05_fia/scripts/summaries/build_tree_species.R`.

## 2. Plot visits, mapped conditions, and remeasurement histories

A FIA plot visit can contain multiple mapped land conditions. **FIA-supplied classification.** We retained conditions classified by FIA as accessible forest land (`COND_STATUS_CD = 1`), using FIA's unadjusted condition share (`CONDPROP_UNADJ`). **Analysis decisions.** For the active condition-level response, a condition also had to account for at least 30% of the plot (`CONDPROP_UNADJ >= 0.30`) at every retained endpoint, and conditions were not aggregated back to whole plots.

**FIA-supplied linkage.** Remeasurements were linked using FIA's official `PREV_PLT_CN` field. **Analysis decisions.** The stable plot identifier was used to validate identity, but a shared stable plot identifier or numeric condition identifier was never used to invent a missing visit link. Within an official linked visit pair, conditions were matched by the same numeric `CONDID`. Thus, `CONDID` identified the mapped condition within linked visits but did not establish temporal linkage by itself.

Official links to available endpoints on the same stable plot were followed even when the linked visit was not the immediately preceding record in chronological order. Missing, unavailable, or cross-stable-plot links remained explicit in linkage QA. Endpoint dates used recorded measurement day when available, then month or year bounds when precision was lower. Interval duration was based on the midpoint of the defensible measurement-date bounds and divided by 365.2425 days per year.

The response unit was a complete first-to-last eligible condition history. All official intervals between the first and last endpoints contributed to cumulative mortality predictors.

Source details: `08_disturbance_linkage/INTERVAL_FOUNDATION.md`; `09_analysis/scripts/00_build_remeasurement_components.R`; `09_analysis/scripts/01_build_condition_histories_and_cwm.R`.

## 3. Species climate-niche estimates

We estimated fixed species climate-affinity traits by overlaying Botanical Information and Ecology Network (BIEN) range polygons with TerraClimate. FIA and Phase 2 Vegetation records defined the target species universe; FIA plot observations were not used to estimate species niche centers.

For FIA-facing products, BIEN polygons were clipped to the configured all-US study-area bounding box, which includes Alaska and Hawaii. The clipping geometry is a bounding box rather than a political boundary. The primary niche policy used the study-area estimate when available and a global BIEN-range estimate when an FIA-observed species lacked a usable study-area estimate. Every downstream CWM retained coverage fields describing the share of its community weight with a usable niche and the share relying on global fallback values.

The compact niche table contains eight fixed species indicators:

- annual mean temperature;
- mean temperature of the warmest and coldest months;
- temperature seasonality, calculated as warmest minus coldest monthly mean;
- annual climatic water deficit (CWD), calculated as the sum of monthly TerraClimate `def`;
- maximum monthly CWD;
- annual precipitation, calculated as the sum of monthly precipitation; and
- precipitation in the driest month.

The active response uses annual mean temperature, annual precipitation, and annual CWD. These species traits are static and are not recalculated by FIA inventory year.

Scientific names were reconciled before BIEN lookup. Taxonomic Name Resolution Service results were treated as review evidence rather than applied automatically. Genus-level `sp.` and `spp.` records were excluded from species-level niche assignment, and infraspecific names were not broadened to parent species without an explicit reviewed decision. Missing ranges, invalid geometry, climate-extraction gaps, global fallbacks, and high-weight missing taxa were recorded in the species-niche QA outputs.

Source details: `06_species_niches/docs/methods_species_niches.md`; `06_species_niches/WORKFLOW.md`; `06_species_niches/qa/README.md`.

## 4. Community-weighted climate affinity

For condition visit `c`, climate indicator `k`, species `i`, species niche value `z_ik`, and community weight `w_ic`, the community-weighted mean was

```text
CWM_ck = sum_i(w_ic × z_ik) / sum_i(w_ic)
```

using only positive, finite weights and nonmissing niche values for indicator `k`. The products retain the total community weight and the weight represented by a usable niche so incomplete niche coverage is not silently treated as complete.

### 4.1 Active individual-abundance response

**Analysis decision.** The active condition-level analysis weights adult-tree and sapling species by expanded live-stem abundance (`n_trees_tpa`), a repository aggregation of FIA's record-level expansion factors. Adult and sapling CWMs are retained separately. For each complete condition history, the response is the final minus initial CWM for annual mean temperature, annual precipitation, or annual CWD.

A combined forest-community response pools live adult and sapling abundance before calculating the CWM. Because adults and saplings are sampled on different FIA elements, this pooled response applies the appropriate subplot, microplot, or macroplot condition proportions before combining expanded abundance. The pooled response is therefore not an unadjusted average of the separately calculated adult and sapling CWMs.

### 4.2 Basal-area weighting sensitivity

Joan requested a parallel adult-tree analysis weighted by basal area as well as individual abundance. This sensitivity should use the same eligible conditions, visit links, species niches, endpoints, and response definitions as the active abundance product, changing only the adult-tree weight from `n_trees_tpa` to `ba_per_acre`. Keeping row membership identical permits direct attribution of differences to weighting rather than cohort construction.

The older `07_thermophilization/` workflow is not an adequate substitute for this comparison: it uses a different plot-visit grain, and its generic adult `abundance_for_cwm` field is primarily basal area with a stem-density fallback when basal area is unavailable. The sensitivity producer uses explicit `n_trees_tpa` and explicit `ba_per_acre` fields and never overwrites active products. See Section 10 for the product family.

### 4.3 Seedlings

Seedlings were excluded from the active response. FIA seedlings are sampled on small microplots, often with relatively few individuals and species, and their sampling support does not align reliably with condition-level disturbance. Restricting preliminary models to conditions containing all life stages and matching mortality more closely to seedling subplots did not eliminate the principal discrepancies. The pooled response consequently includes adults and saplings only.

## 5. Mortality and disturbance attribution

**FIA-supplied evidence.** Mortality status and agents came from FIA tree growth-removal-mortality components and the death record's `AGENTCD`. **Analysis decisions.** P2A records were excluded from mortality numerators and denominators, and the lineage risk set was constructed as described below. A tree lineage entered a condition history's risk set when it was first observed alive at a visit followed by an opportunity to observe mortality. This included live lineages present at the initial visit and lineages first observed alive at intermediate visits. Trees first observed only at the final visit were excluded because they had no subsequent mortality observation window.

Verified deaths were attributed to fire, insects, or disease using the death record's FIA `AGENTCD`. Each death retained the abundance weight assigned when its lineage entered the risk set. Sampling-element-specific condition proportions were used with FIA trees-per-acre expansion: microplot proportions for 1.0–4.9-inch stems, subplot proportions for ordinary stems at least 5 inches, and macroplot proportions where applicable. Generic condition proportion was used only as a documented fallback.

For each complete history, agent-specific mortality was the cumulative percentage of the lineage risk set that died from the specified agent between the first and last visits. It was not annualized. These FIA mortality predictors are distinct from live-tree damage-agent codes and from external MTBS fire or IDS aerial-survey evidence.

The repository separately prepares FIA condition disturbance slots, live-tree damage-agent evidence, MTBS fire perimeters, and IDS insect/disease detections. Those products support future triangulation and severity analyses but are not predictors in the active `09_analysis` models.

Source details: `09_analysis/docs/METHODS.md`; `09_analysis/scripts/02_build_interval_mortality.R`; `09_analysis/scripts/04_build_cumulative_mortality.R`; `08_disturbance_linkage/README.md`; `08_disturbance_linkage/FIA_DISTURBANCE_DATA_DICTIONARY.md`.

## 6. Site climatic water deficit

**Analysis-derived climate predictor.** The site-level climatic predictor was TerraClimate `def`, expressed in millimetres per month and extracted at FIA-supplied public plot coordinates. Monthly values whose timestamps fell within the actual first-to-last FIA measurement period were summed for each condition history. This cumulative site CWD is a climatic exposure predictor and is distinct from the CWM-CWD response, which describes the drought affinity of the species community.

The tracked extraction uses the University of Idaho TerraClimate THREDDS NCSS source and the 1997–2025 analysis window. Histories ending after December 2025 were marked outside the analysis window and excluded from models. Preflight validation rejects a cache with a different site-list hash, backend, source, variable, year window, missing year, partially represented site, or duplicate key.

Source details: `09_analysis/scripts/05_prepare_site_cwd_inputs.sql`; `09_analysis/scripts/05_validate_site_cwd_cache.R`; `09_analysis/scripts/06_add_cumulative_site_cwd.sql`.

## 7. Preliminary statistical models

Separate models were fitted for three community groups—saplings, adults, and pooled saplings plus adults—and three responses: change in temperature CWM, precipitation CWM, and CWD CWM. The implemented model form was

```text
delta CWM ~ cumulative fire mortality
          + cumulative insect mortality
          + cumulative disease mortality
          + cumulative site CWD
          + first-to-last survey duration
```

Models were ordinary linear models with HC1 standard errors clustered by stable FIA plot. They are preliminary association models, not finalized causal models. The authoritative no-seedling model run contains formulas, coefficient tables, fit statistics, sample-flow tables, input hashes, figures, robustness checks, and a self-contained HTML report.

Source details: `09_analysis/scripts/08_fit_preliminary_models_and_report.R`; `09_analysis/results/model_runs/20260905_cumulative_mortality_site_cwd_no_seedlings_v01/`.

## 8. Missingness, exclusions, and interpretation

A missing species niche does not imply a zero-valued niche. CWM products report species and weight coverage, and final analyses should apply a documented coverage rule or sensitivity rather than silently equating incomplete and complete communities.

A missing disturbance record does not prove that a plot was undisturbed. FIA disturbance slots, mortality agents, live-tree damage codes, MTBS, and IDS represent different detection processes and spatial or temporal supports. Conditions without recorded evidence are therefore described as having no recorded disturbance under the specified source, not as proven disturbance-free.

Whole-plot harvest, treatment, coordinate, and condition-proportion flags are retained for QA and sensitivity analysis. They do not automatically remove an otherwise eligible forest condition unless the active cohort definition says so.

## 9. Understory extension: exploratory decision stage

FIA Phase 2 Vegetation species data are available primarily for 13 western states and are much sparser than tree data. Records use USDA PLANTS symbols rather than FIA tree species codes, and species identification, niche coverage, subplot coverage, and protocol comparability vary. Absence of a P2VEG species row is not evidence of zero understory cover.

A noncanonical diagnostic condition-level pilot is implemented as the next step rather than being promoted immediately to the active analysis. For each condition visit and climate indicator, the pilot retains

```text
weighted_sum       = sum_i(cover_i × niche_i)
niche_covered_cover = sum_i(cover_i for taxa with a usable niche)
total_recorded_cover = sum_i(cover_i)
cover_weighted_CWM = weighted_sum / niche_covered_cover
niche_cover_fraction = niche_covered_cover / total_recorded_cover
```

where cover is species cover scaled by the subplot-condition share. Results are reported separately for combined understory and major growth habits. The pilot distinguishes no vegetation survey, no recorded understory, no species-level identification, no usable niche coverage, low niche-cover coverage, and usable CWM; unusable observations should receive explicit status codes such as `NU_*`, never numeric zeros. Multiple candidate coverage thresholds are summarized before Joan and the team choose a final inclusion rule. Interval change should be promoted to a canonical product only after both endpoints can be shown to have comparable protocol and sampling support.

Source details: `05_fia/WORKFLOW.md` (understory products); `05_fia/scripts/understory/01_extract_understory.R`; `06_species_niches/qa/README.md`. The diagnostic producer is `09_analysis/scripts/sensitivities/02_build_understory_cwm_diagnostic.R`; outputs are written under `09_analysis/data/sensitivity/understory_cwm_diagnostic/`.

## 10. Parallel tree-weighting sensitivity products

To keep the sensitivity discoverable without changing the active analysis, its location is

```text
09_analysis/data/sensitivity/tree_weighting/
```

with a tracked producer under

```text
09_analysis/scripts/sensitivities/01_build_tree_weighting_cwm.R
```

The product family should include

```text
tree_condition_visit_cwm_abundance.parquet
tree_condition_visit_cwm_basal_area.parquet
tree_stable_condition_cwm_change_abundance.parquet
tree_stable_condition_cwm_change_basal_area.parquet
tree_condition_visit_weighting_comparison.parquet
tree_stable_condition_weighting_comparison.parquet
tree_weighting_summary.csv
```

The comparison products pair identical condition visits and histories, report both weights and niche-coverage fractions, and calculate basal-area minus abundance differences for every CWM and CWM change. Filenames and columns must state the weight explicitly; the generic `abundance_for_cwm` alias should not be used in this sensitivity.

## 11. BIEN range-scope sensitivity

The primary niche policy prefers study-area-clipped BIEN/TerraClimate values and falls back globally only when a study-area value is unavailable. A separate sensitivity uses global niche values whenever clipping removes a meaningful portion of a BIEN polygon. The sensitivity reuses existing polygons and climate summaries, rebuilds parallel CWM products outside the canonical directories, and does not refit models or overwrite active data. Its report quantifies missing-niche importance by community weight and the resulting changes in species and community climate-affinity values.

Final numerical thresholds, counts, and effect sizes will be inserted from `/home/tippingPoint/ermiller/bien_clipping_supplement/` after the manifest and current-policy reproduction checks pass.

## 12. Reproducibility and QA

Every major product has a declared row grain, primary key, producer, and source path in the repository documentation and product registry. Structural validators check identifier uniqueness, endpoint integrity, forest-condition eligibility, date ordering, species-niche availability, polygon validity, CWM coverage, mortality bounds, and model-sample flow. Large data products remain local, while source code, compact QA tables, methods, and product contracts are version controlled.

Persisted floating-point aggregations in the active analysis use deterministic ordering and compensated summation in R or ordered single-thread aggregation in DuckDB. Model runs retain input SHA-256 hashes and a manifest so results can be tied to exact inputs. The queryable research bundle is an export assembled from canonical products; its `bundle_tables` metadata records the authoritative source and producer for each table.

## 13. Product and provenance map

| Methods component | Primary implementation | Main products |
|---|---|---|
| FIA acquisition and cleaning | `05_fia/scripts/core/` and `05_fia/scripts/summaries/` | state partitions and national FIA summaries |
| Plot-visit identity and dates | `05_fia/scripts/foundations/01_build_plot_visit_context.R` | `plot_visit_context.parquet` |
| Species niches | `06_species_niches/scripts/` | BIEN polygons, range climate, compact niche tables |
| Condition histories and active CWMs | `09_analysis/scripts/00_*` and `01_*` | remeasurement components, condition CWMs, stable-condition changes |
| Interval and cumulative mortality | `09_analysis/scripts/02_*` through `04_*` | interval mortality, cumulative mortality, model base |
| Site CWD | `09_analysis/scripts/05_*` and `06_*` | site cache contract, cumulative CWD, model data |
| Pooled adult–sapling CWM | `09_analysis/scripts/07_build_pooled_community_cwm.sql` | pooled condition CWM and pooled model data |
| CWM sensitivity products | `09_analysis/scripts/sensitivities/` | paired abundance/basal-area tree CWMs and an exploratory understory diagnostic |
| Models and robustness | `09_analysis/scripts/08_*` and `09_*` | authoritative model-run directory |
| External disturbance evidence | `08_disturbance_linkage/scripts/` | FIA, MTBS, and IDS preparation products; not active model inputs |
| Research handoff database | `forest_explorer/export/build_research_bundle.R` | `forest_research_bundle.duckdb` |

## 14. Items requiring scientific approval before final manuscript use

- the minimum acceptable species-niche weight coverage for community CWMs;
- whether basal-area results are presented as a co-primary analysis or sensitivity;
- the final understory protocol-comparability and coverage rules;
- whether any future `undisturbed` comparison group must lack evidence across FIA mortality and disturbance fields, MTBS fire, and IDS insect/disease records, and how source-specific non-detections are labeled;
- the final climate-novelty definition and thresholds;
- whether native/non-native damage-agent classification is included and which reviewed authority defines origin status; and
- whether density, richness, and diversity changes are supplied as derived products or calculated downstream from visit-level data.
