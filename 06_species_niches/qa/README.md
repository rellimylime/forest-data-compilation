# Species Niche QA

**Navigation:** [Module README](../README.md) | [Technical Workflow](../WORKFLOW.md) | [Methods](../docs/methods_species_niches.md) | [Thermophilization](../../07_thermophilization/README.md)

This folder contains validation scripts for the species climate niche workflow. The goal is to clearly document the variable parts of the workflow before running thermophilization analyses.

## Navigation

Start with the module overview in [../README.md](../README.md). Use this QA README when you need to interpret warnings or decide which output file to open. For methods rationale, formulas, and range-scope definitions, see [../docs/methods_species_niches.md](../docs/methods_species_niches.md).

## Minimal Outputs To Read First

Most people do not need to open every CSV in `qa/outputs/`. Start with this small set:

| File | Why it exists |
| --- | --- |
| `species_niche_validation_decision.csv` | One-line gate: are there structural blockers, and are warnings still unresolved? |
| `species_niche_validation_checks.csv` | Full checklist behind the decision file. Open this when the decision file reports warnings or errors. |
| `species_niche_gap_summary.csv` | Plain count of how many species have usable niches and why the others are missing. |
| `species_niche_gap_action_summary.csv` | Prioritized missing-data action types, grouped by importance. |
| `species_taxon_resolution_summary.csv` | Separates source-code counts from deduplicated biological-taxon counts. |
| `species_taxon_duplicate_source_codes.csv` | Lists taxa represented by more than one source code, such as FIA SPCD plus P2VEG symbol. |
| `study_area_climate_gap_summary.csv` | Summary of BIEN ranges that exist globally but do not produce study-area climate rows. |
| `tnrs_candidate_bien_range_summary.csv` | Counts BIEN-missing names whose TNRS candidate names do or do not have BIEN ranges. |
| `tnrs_candidate_name_pairs_available.csv` | Minimal side-by-side original-name vs candidate-name list for review. |
| `global_fallback_species_location_summary.csv` | Counts global-fallback species by source system and observed states. |

The other output files are supporting diagnostics. They are useful when a specific warning fails, but they are local regenerated artifacts rather than commit material.

## Main Validation Command

Run this after scripts `01` through `03`, and again after scripts `04` and `05`:

```bash
Rscript 06_species_niches/qa/01_validate_species_niche_workflow.R
```

This script checks the handoffs among:

1. the species universe,
2. BIEN range availability,
3. downloaded BIEN polygons,
4. TerraClimate range extraction,
5. compact species climate niches, and
6. the downstream thermophilization CWM table.

## How To Read The Result

Start with:

```text
06_species_niches/qa/outputs/species_niche_validation_decision.csv
```

Key fields:

- `proceed_to_script_04`: `TRUE` means the species universe, BIEN availability, and polygon products are structurally consistent enough to run the long TerraClimate extraction.
- `proceed_to_modeling`: `TRUE` means no validator warnings remain. This should be `TRUE` before using the niche table for final thermophilization modeling.
- `failed_required_checks`: any value above zero blocks the workflow.
- `failed_warning_checks`: warnings do not always block the next extraction step, but they must be reviewed, fixed, or explicitly justified before modeling.

Then inspect:

```text
06_species_niches/qa/outputs/species_niche_validation_checks.csv
```

Each row is one check. `severity = error` means a structural failure. `severity = warning` means a scientific or freshness issue that needs review.

Think of the QA outputs in three groups:

| Group | Meaning | Modeling implication |
| --- | --- | --- |
| `error` checks | The handoff between products is broken, such as missing files, duplicate keys, invalid geometries, or BIEN-available species missing polygons. | Stop and fix before continuing. |
| `warning` checks | The product exists, but something needs scientific review, documentation, filtering, or a sensitivity check. | Do not use for final modeling until reviewed. |
| Gap/action outputs | Species-level explanations for missing niches, prioritized by downstream CWM weight. | Use these to decide which gaps need taxonomic fixes and which can be documented exclusions. |

Warnings generally fall into a few recurring kinds:

| Warning kind | Main output to inspect | Plain-language interpretation |
| --- | --- | --- |
| BIEN missing fraction | `bien_range_missing_species.csv` and `species_niche_gap_summary.csv` | BIEN does not cover every target name; high-weight missing species matter most. |
| Polygon geometry or area QA | `species_niche_validation_checks.csv` and `bien_polygons_*.csv` | Empty or invalid geometry checks are blocking; unusual areas or extents require review. |
| Study-area climate gaps | `study_area_climate_gap_summary.csv` | BIEN ranges exist, but the study-area clipped range has no TerraClimate rows. |
| CWM coverage | `plot_recruitment_cwm_missing_species.csv` and CWM `frac_weight_with_niche` fields | Some FIA seedling communities have incomplete or zero niche coverage. |

## Coverage And Variability Outputs

The workflow intentionally covers several very different biological layers: tree seedlings, saplings, trees, shrubs, forbs, grasses, and P2VEG tree-layer records. Those groups do not have identical taxonomic cleanliness or BIEN range coverage.

Use these tables to quantify that variability:

```text
06_species_niches/qa/outputs/species_niche_validation_summary.csv
06_species_niches/qa/outputs/species_niche_gap_summary.csv
06_species_niches/qa/outputs/bien_range_missing_species.csv
```

Missing BIEN ranges are not all equally important: a rare forb with no BIEN range is a smaller problem than a common seedling species with high downstream CWM weight. The long ranked and action-queue files are generated locally when needed, but they are intentionally not tracked in Git.

## Taxonomic Name Review

Name-review rules are documented in
[Methods: Taxonomic Name Review](../docs/methods_species_niches.md#taxonomic-name-review).

The review workflow starts from BIEN-missing names, uses TNRS as evidence, and then records human-reviewed decisions in:

```text
06_species_niches/qa/outputs/tnrs_bien_missing_name_review_candidates.csv
06_species_niches/lookups/manual_bien_name_overrides_reviewed.csv
06_species_niches/qa/outputs/manual_bien_overrides_applied.csv
```

Only rows in the reviewed override table with:

```text
review_status = ready_for_pipeline
```

should be applied automatically. TNRS accepted names are not applied blindly, because some accepted-name mappings are taxonomically correct but ecologically wrong for the FIA common name or code.

## Polygon QA

Use these outputs to review the BIEN polygon product:

```text
06_species_niches/qa/outputs/bien_polygons_area_summary.csv
06_species_niches/qa/outputs/bien_polygons_nonpositive_area.csv
06_species_niches/qa/outputs/bien_polygons_missing_available_species.csv
06_species_niches/qa/outputs/bien_polygons_species_not_available.csv
```

The validator treats empty or invalid geometries as blocking errors. Nonpositive projected areas are warnings because they can come from projection or geometry edge cases, but they still need inspection before final modeling.

## Gap Ledger

After scripts `04` and `05` have been rerun from current inputs, document final niche gaps with:

```bash
Rscript 06_species_niches/qa/04_document_species_niche_gaps.R
Rscript 06_species_niches/qa/05_prioritize_species_niche_gap_actions.R
Rscript 06_species_niches/qa/06_validate_study_area_climate_gaps.R
Rscript 06_species_niches/qa/07_check_tnrs_candidate_bien_ranges.R
Rscript 06_species_niches/qa/08_describe_global_fallback_species.R
Rscript 06_species_niches/qa/09_build_species_taxon_crosswalk.R
```

Main outputs:

```text
06_species_niches/qa/outputs/species_niche_gap_summary.csv
06_species_niches/qa/outputs/species_niche_gap_action_summary.csv
06_species_niches/qa/outputs/species_taxon_resolution_summary.csv
06_species_niches/qa/outputs/species_taxon_duplicate_source_codes.csv
06_species_niches/qa/outputs/study_area_climate_gap_summary.csv
06_species_niches/qa/outputs/tnrs_candidate_bien_range_summary.csv
06_species_niches/qa/outputs/tnrs_candidate_name_pairs_available.csv
06_species_niches/qa/outputs/global_fallback_species_location_summary.csv
```

These files explain why each species does or does not have a usable climate niche. They should be regenerated any time the species universe, BIEN range availability, range climate extraction, compact niche table, or CWM table is rebuilt.

Detailed ledgers, action queues, rankings, and species-level diagnostic files are still written in `qa/outputs/` for local review, but they are ignored by Git. This keeps the repository readable while preserving the ability to inspect individual problem species during development.

The taxon crosswalk is the preferred way to count biological species rather than source codes. `species_key` remains the join key for FIA/P2VEG products, while `taxon_count_key` deduplicates multiple source codes that represent the same resolved taxon. Use `species_taxon_resolution_summary.csv` when reporting how many taxa are covered, missing, or recoverable through reviewed name work.

The action queue separates:

- true species that may need synonym/name-resolution work,
- genus-level `sp.` / `spp.` observations that should be excluded from the main species-level CWM and documented,
- infraspecific `var.` / `ssp.` / `subsp.` taxa that should stay unresolved unless a broader parent-species fallback is explicitly approved, and
- study-area climate gaps.

The study-area climate diagnostics separate species whose BIEN polygons are outside the configured study-area bounding box from species that intersect the study area but still failed to produce climate rows.

The TNRS candidate range diagnostic is a review aid, not an automatic override. It tests whether TNRS candidate names for BIEN-missing species return BIEN ranges. Candidate names with ranges still need ecological and taxonomic review before they are added to `lookups/manual_bien_name_overrides_reviewed.csv`. Infraspecific source names (`var.`, `ssp.`, and `subsp.`) are listed but not counted as candidate rescues, because parent-species fallback broadens the taxon and requires a separate decision.

The global-fallback location diagnostic compares the BIEN global range extent to the observed states and coordinate summaries from FIA seedling/tree/sapling products where coordinates are available. This helps separate Hawaii/non-native range-map issues from true study-area clipping or extraction problems.

## Output Index

The files below are the official CSV outputs to read or cite during review. Each can be regenerated from the scripts listed in [Gap Ledger](#gap-ledger).

| File | Row meaning | Main attributes |
| --- | --- | --- |
| `species_niche_validation_decision.csv` | One workflow gate decision | Whether required checks pass and whether warnings remain |
| `species_niche_validation_summary.csv` | One headline workflow count | Species-universe, BIEN availability, and polygon counts |
| `species_niche_validation_checks.csv` | One validation check | Check name, severity, pass/fail status, observed value, expected value, explanation |
| `species_niche_gap_summary.csv` | One missing-data stage | Counts source codes and CWM missing weight by gap category |
| `species_niche_gap_action_summary.csv` | One priority/action/gap group | Recommended action, priority, source-code count, CWM missing weight |
| `species_taxon_resolution_summary.csv` | One taxon-resolution status | Source-code counts, deduplicated source taxa, reporting taxa, abundance, CWM missing weight |
| `species_taxon_missing_summary.csv` | One taxon-resolution status by TNRS class | Deduplicated missing/recoverable taxon counts by review type |
| `species_taxon_duplicate_source_codes.csv` | One biological taxon with multiple source codes | Source keys, source systems, community layers, range availability, abundance |
| `study_area_climate_gap_summary.csv` | One study-area climate gap type | Species counts and whether global range/niche products exist |
| `global_fallback_species_location_summary.csv` | One source-system/state group for global-fallback species | Species counts, source rows, plot visits, conditions, coordinate-summary availability |
| `tnrs_candidate_bien_range_summary.csv` | One TNRS class / candidate range status | Source-code counts for candidate names with or without BIEN ranges |
| `tnrs_candidate_name_pairs_available.csv` | One non-infraspecific source code whose TNRS candidate has a BIEN range | Original name, candidate name, taxonomic status, review class, source key |
| `species_universe_metrics.csv` | One species-universe metric | Total source codes, niche targets, pseudo/aggregate taxa |
| `species_universe_layer_counts.csv` | One source-code system | Species-universe counts by source system |
| `bien_range_availability_summary.csv` | One BIEN availability summary | Available, missing, API-error, and review counts |
| `bien_range_availability_by_layer.csv` | One community-layer combination | BIEN availability by layer membership |
| `species_range_climate_summary*.csv` | One variable/metric/range-scope summary | TerraClimate extraction row counts and species counts |
| `species_climate_niches_summary*.csv` | One range-scope summary | Compact niche indicator completeness |

Detailed artifacts such as full ledgers, full TNRS candidate checks, polygon diagnostics, stale-species lists, rankings, and failure logs are generated in
`qa/outputs/` for debugging. They are intentionally ignored by Git unless they are promoted into the official list above.

Temporary review aids, such as one-off Markdown or CSV files prepared for a meeting, can also live in `qa/outputs/` while they are being discussed. They are not part of the reproducible workflow unless a kept script regenerates them and they are added to the output index.

## Current Rule Of Thumb

Do not move to final thermophilization modeling until:

1. `failed_required_checks == 0`,
2. `proceed_to_modeling == TRUE`, or every warning has a written justification,
3. `species_niche_gap_ledger.csv` has been regenerated from current products,
4. high-weight missing species have been reviewed, and
5. downstream CWM coverage is acceptable for the plots being modeled.
