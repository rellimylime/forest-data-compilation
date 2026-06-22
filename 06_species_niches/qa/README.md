# Species Niche QA

This folder contains validation scripts for the species climate niche workflow. The goal is to clearly document the variable parts of the workflow before running thermophilization analyses.

## Navigation

Start with the module overview in [../README.md](../README.md). Use this QA
README when you need to interpret warnings or decide which output file to open.
For methods rationale, formulas, and range-scope definitions, see
[../docs/methods_species_niches.md](../docs/methods_species_niches.md).

## Minimal Outputs To Read First

Most people do not need to open every CSV in `qa/outputs/`. Start with this
small set:

| File | Why it exists |
| --- | --- |
| `species_niche_validation_decision.csv` | One-line gate: are there structural blockers, and are warnings still unresolved? |
| `species_niche_validation_checks.csv` | Full checklist behind the decision file. Open this when the decision file reports warnings or errors. |
| `species_niche_gap_summary.csv` | Plain count of how many species have usable niches and why the others are missing. |
| `species_niche_gap_action_summary.csv` | Prioritized missing-data action types, grouped by importance. |
| `study_area_climate_gap_summary.csv` | Summary of BIEN ranges that exist globally but do not produce study-area climate rows. |

The other output files are supporting diagnostics. They are useful when a
specific warning fails, but they are local regenerated artifacts rather than
commit material.

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

Current warnings fall into a few recurring kinds:

| Warning kind | Main output to inspect | Plain-language interpretation |
| --- | --- | --- |
| Sapling layer representation | `species_niche_validation_checks.csv` | Saplings are not a separate species-universe layer yet, so do not claim sapling-specific niche coverage. |
| BIEN missing fraction | `bien_range_missing_species.csv` and `species_niche_gap_summary.csv` | BIEN does not cover every target name; high-weight missing species matter most. |
| Polygon area QA | `species_niche_validation_checks.csv` | Empty or invalid geometry checks are blocking; area edge cases are reviewed as warnings. |
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

Missing BIEN ranges are not all equally important: a rare forb with no BIEN
range is a smaller problem than a common seedling species with high downstream
CWM weight. The long ranked and action-queue files are generated locally when
needed, but they are intentionally not tracked in Git.

## Taxonomic Name Review

Name review is summarized in the module README under "Missing Data And Name Review":

```text
06_species_niches/README.md
```

The review workflow starts from BIEN-missing names, uses TNRS as evidence, and then records human-reviewed decisions in:

```text
06_species_niches/qa/outputs/tnrs_bien_missing_name_review_candidates.csv
06_species_niches/qa/outputs/tnrs_bien_missing_name_review_summary.csv
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
```

Main outputs:

```text
06_species_niches/qa/outputs/species_niche_gap_summary.csv
06_species_niches/qa/outputs/species_niche_gap_action_summary.csv
06_species_niches/qa/outputs/study_area_climate_gap_summary.csv
```

These files explain why each species does or does not have a usable climate niche. They should be regenerated any time the species universe, BIEN range availability, range climate extraction, compact niche table, or CWM table is rebuilt.

Detailed ledgers, action queues, rankings, and species-level diagnostic files
are still written in `qa/outputs/` for local review, but they are ignored by
Git. This keeps the repository readable while preserving the ability to inspect
individual problem species during development.

The action queue separates:

- true species that may need synonym/name-resolution work,
- genus-level `sp.` / `spp.` observations that should be excluded from the main species-level CWM and documented,
- infraspecific `var.` / `ssp.` taxa that need review before using a broader parent-species fallback, and
- study-area climate gaps.

The study-area climate diagnostics separate species whose BIEN polygons are outside the configured study-area bounding box from species that intersect the study area but still failed to produce climate rows.

## Supporting Outputs

These files are generated for traceability, but they are usually opened only
after a warning points to them. Most are ignored by Git and can be regenerated
from the QA scripts.

| File pattern | When to open it |
| --- | --- |
| `species_universe_*.csv` | Check species-universe composition, pseudo taxa, and layer counts from script `01`. |
| `bien_range_availability_*.csv` | Check BIEN lookup counts by layer from script `02`. |
| `bien_range_missing_species.csv` | Review all BIEN-missing species, not only those important in CWM. |
| `manual_bien_overrides_applied.csv` | Audit which reviewed manual name overrides script `02` actually applied. |
| `bien_range_polygon_*.csv` | Check polygon download summaries and failures from script `03`. |
| `bien_polygons_*.csv` | Inspect polygon handoff/area diagnostics from the validator. |
| `range_climate_*` and `compact_niche_*` | Inspect stale or missing species between polygons, TerraClimate extraction, and compact niches. |
| `species_range_climate_*` | QA summaries/failure tables from script `04`. |
| `species_climate_niches_*` | QA summaries/rankings/missing-indicator tables from script `05`. |
| `species_niche_coverage_by_*.csv` | Coverage by source system or community layer. |
| `species_niche_product_manifest.csv` | File sizes, timestamps, and MD5 hashes for major products. |
| `species_niche_top_cwm_gaps.csv` | Highest-weight missing species in downstream CWM. |
| `tnrs_bien_missing_name_review_*.csv` | TNRS-assisted review candidates and summaries. |

## Current Rule Of Thumb

Do not move to final thermophilization modeling until:

1. `failed_required_checks == 0`,
2. `proceed_to_modeling == TRUE`, or every warning has a written justification,
3. `species_niche_gap_ledger.csv` has been regenerated from current products,
4. high-weight missing species have been reviewed, and
5. downstream CWM coverage is acceptable for the plots being modeled.
