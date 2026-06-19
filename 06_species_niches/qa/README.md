# Species Niche QA

This folder contains validation scripts for the species climate niche workflow.
The goal is to make the variable parts of the workflow visible before they reach
the thermophilization analysis.

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

- `proceed_to_script_04`: `TRUE` means the species universe, BIEN availability,
  and polygon products are structurally consistent enough to run the long
  TerraClimate extraction.
- `proceed_to_modeling`: `TRUE` means no validator warnings remain. This should
  be `TRUE` before using the niche table for final thermophilization modeling.
- `failed_required_checks`: any value above zero blocks the workflow.
- `failed_warning_checks`: warnings do not always block the next extraction step,
  but they must be reviewed, fixed, or explicitly justified before modeling.

Then inspect:

```text
06_species_niches/qa/outputs/species_niche_validation_checks.csv
```

Each row is one check. `severity = error` means a structural failure. `severity =
warning` means a scientific or freshness issue that needs review.

## Coverage And Variability Outputs

The workflow intentionally covers several very different biological layers:
tree seedlings, saplings, trees, shrubs, forbs, grasses, and P2VEG tree-layer
records. Those groups do not have identical taxonomic cleanliness or BIEN range
coverage.

Use these tables to quantify that variability:

```text
06_species_niches/qa/outputs/species_niche_coverage_by_source.csv
06_species_niches/qa/outputs/species_niche_coverage_by_layer.csv
06_species_niches/qa/outputs/bien_missing_species_ranked_by_abundance.csv
```

The ranked missing-species file is especially important. Missing BIEN ranges are
not all equally important: a rare forb with no BIEN range is a smaller problem
than a common seedling species with high downstream CWM weight.

## Polygon QA

Use these outputs to review the BIEN polygon product:

```text
06_species_niches/qa/outputs/bien_polygons_area_summary.csv
06_species_niches/qa/outputs/bien_polygons_nonpositive_area.csv
06_species_niches/qa/outputs/bien_polygons_missing_available_species.csv
06_species_niches/qa/outputs/bien_polygons_species_not_available.csv
```

The validator treats empty or invalid geometries as blocking errors. Nonpositive
projected areas are warnings because they can come from projection or geometry
edge cases, but they still need inspection before final modeling.

## Gap Ledger

After scripts `04` and `05` have been rerun from current inputs, document final
niche gaps with:

```bash
Rscript 06_species_niches/qa/04_document_species_niche_gaps.R
```

Main outputs:

```text
06_species_niches/qa/outputs/species_niche_gap_ledger.csv
06_species_niches/qa/outputs/species_niche_gap_summary.csv
06_species_niches/qa/outputs/species_niche_top_cwm_gaps.csv
```

These files explain why each species does or does not have a usable climate
niche. They should be regenerated any time the species universe, BIEN range
availability, range climate extraction, compact niche table, or CWM table is
rebuilt.

## Current Rule Of Thumb

Do not move to final thermophilization modeling until:

1. `failed_required_checks == 0`,
2. `proceed_to_modeling == TRUE`, or every warning has a written justification,
3. `species_niche_gap_ledger.csv` has been regenerated from current products,
4. high-weight missing species have been reviewed, and
5. downstream CWM coverage is acceptable for the plots being modeled.
