# Species Climate Niches

This module builds species-level climate niche values for the thermophilization analysis. In plain language: it asks what climate each species' mapped BIEN range occupies, then turns that into a small set of climate indicators that can be joined to FIA species communities.

## Where To Start

Use this README as the entry point for the species niche workflow. More detailed references are linked where they matter:

- For the ecological/methodological rationale, formulas, and range-scope logic, see [Methods Notes](docs/methods_species_niches.md).
- For how to read validation outputs, see [Species Niche QA](qa/README.md).
- For the downstream CWM consumer, see [Thermophilization README](../07_thermophilization/README.md).

The split is intentional:

- `README.md` tells you what to run, what the main products are, and what the current status is.
- `docs/methods_species_niches.md` explains why the method is set up this way.
- `qa/README.md` explains how to interpret QA warnings and which output files to inspect.

The preferred product for the current FIA analysis is:

```text
06_species_niches/data/processed/species_climate_niches_us_study_area.parquet
```

This is the BIEN range-map niche table clipped to the configured all-U.S. study area bounding box.

## Current Rerun Status

Scripts `01` through `05` have been rerun from the current species universe and reviewed BIEN name overrides. The downstream thermophilization CWM consumer has also been rerun from the current compact niche table.

The BIEN availability table currently includes two reviewed overrides that were added after the last consolidated polygon build:

- `Chamaecyparis nootkatensis` -> `Callitropsis nootkatensis`
- `Quercus prinus` -> `Quercus montana`

Those two reviewed overrides now have usable study-area niche values.

Current regenerated counts:

| Product | Current count |
| --- | ---: |
| Species universe | 6,554 species-like records |
| Species targeted for BIEN niche lookup | 5,894 records |
| BIEN ranges available | 4,216 records |
| BIEN ranges missing | 1,678 records |
| Consolidated BIEN range polygons | 4,216 species |
| Species with study-area TerraClimate rows | 4,186 species |
| Species with compact study-area niches | 4,186 species |

The 30 BIEN-available species without study-area TerraClimate rows are documented in QA as study-area climate gaps. The diagnostic output classifies them as BIEN polygons outside the configured study-area bounding box, not stale pipeline products. Downstream CWM construction can now use the global BIEN niche table as a flagged fallback for FIA-observed species that lack a study-area niche.

Script `04` uses cached batch outputs so reruns can continue without restarting completed batches. The batch cache now includes a hash of the ordered species set, so batches from an older species universe cannot be accidentally reused.

## Workflow

Run scripts in order:

```bash
Rscript 06_species_niches/scripts/01_build_species_universe.R
Rscript 06_species_niches/scripts/02_check_bien_ranges.R
Rscript 06_species_niches/scripts/03_download_bien_ranges.R
Rscript 06_species_niches/qa/01_validate_species_niche_workflow.R
Rscript 06_species_niches/scripts/04_extract_terraclimate_from_ranges.R --range-scope=us_study_area
Rscript 06_species_niches/scripts/05_build_species_climate_niches.R --range-scope=us_study_area
Rscript 07_thermophilization/scripts/01_build_plot_recruitment_cwm.R
Rscript 06_species_niches/qa/01_validate_species_niche_workflow.R
Rscript 06_species_niches/qa/04_document_species_niche_gaps.R
Rscript 06_species_niches/qa/05_prioritize_species_niche_gap_actions.R
Rscript 06_species_niches/qa/06_validate_study_area_climate_gaps.R
```

When only reviewed name overrides change, the clean rerun can start at script `03`. Scripts `01` and `02` do not need to be rerun unless the species universe or manual override table changes again.

Smoke tests use `--limit=N` and write to ignored smoke folders:

```text
06_species_niches/data/smoke/
06_species_niches/qa/smoke/
```

## Script Outputs And Grains

| Script | Main output | Grain | Plain-language meaning |
| --- | --- | --- | --- |
| `01_build_species_universe.R` | `species_universe.parquet` | One row per source species code | Master list of FIA/P2VEG species that could need a climate niche. Includes trees, seedlings/saplings, shrubs, forbs, grasses, and P2VEG tree layers. |
| `02_check_bien_ranges.R` | `bien_range_availability.parquet` | One row per target species | Whether BIEN reports a range map for the species name we queried. Uses reviewed manual overrides only when `review_status = ready_for_pipeline`. |
| `03_download_bien_ranges.R` | `species_range_polygons.gpkg` | One polygon feature set per BIEN-available species, consolidated | BIEN range polygons downloaded locally and standardized enough for climate extraction. Reuses per-species cached downloads when available. |
| `qa/01_validate_species_niche_workflow.R` | `species_niche_validation_checks.csv` | One row per validation check | Structural and freshness checks across the species universe, BIEN availability, polygons, climate extraction, compact niches, and CWM consumers. |
| `04_extract_terraclimate_from_ranges.R` | `species_range_climate_us_study_area.parquet` | Species x month x variable x range metric | Monthly TerraClimate summaries over each BIEN range polygon after study-area clipping. |
| `05_build_species_climate_niches.R` | `species_climate_niches_us_study_area.parquet` | One row per species | Compact species climate indicators used as traits in downstream CWM analysis. |
| `qa/04_document_species_niche_gaps.R` | `species_niche_gap_ledger.csv` | One row per species in the universe | Human-auditable list of whether each species has a usable niche and, if not, why not. |

For deeper definitions of grain and range scope, see [Methods Notes: Grain Definitions](docs/methods_species_niches.md#grain-definitions) and [Methods Notes: Range Scope](docs/methods_species_niches.md#range-scope).

## What The Product Columns Mean

### `species_universe.parquet`

This table defines the species universe before BIEN is involved.

Important columns:

- `species_key`: stable project join key, such as `fia_spcd:802` or `p2veg:ACRU`.
- `source_code_system`: where the source code comes from, such as `fia_spcd` or `p2veg`.
- `source_species_code`: FIA `SPCD` or P2VEG plant code.
- `scientific_name`, `common_name`: names used for human review and BIEN lookup.
- `community_layers`: where the species appears in the FIA/P2VEG data, such as `seedling`, `tree`, `shrub`, `forb`, or `graminoid`.
- `is_pseudo_taxon`: `TRUE` for records like `Amelanchier spp.` or `Tree unknown` that are not a clean binomial species.
- `needs_niche`: `TRUE` when the record is specific enough to attempt a species-level niche.

### `bien_range_availability.parquet`

This table records whether BIEN had a range map for each targeted species.

Important columns:

- `bien_query_name`: binomial name submitted to BIEN, with spaces converted to underscores.
- `bien_range_available`: whether BIEN reported a range map.
- `range_lookup_status`: `available` or `no_range`.
- `range_match_status`: matching status used by downstream scripts.
- `needs_range_review`: `TRUE` when the species could not be matched to a BIEN range.
- `range_review_reason`: plain-language reason for review, usually that BIEN did not report a range map.

### `species_range_climate_us_study_area.parquet`

This is the range-climate overlay output.

Grain:

```text
species_key x month x variable x metric
```

Important columns:

- `month`: calendar month, 1-12.
- `variable`: TerraClimate variable, such as `tmean`, `pr`, or `def`.
- `metric`: spatial summary over the BIEN range polygon. Current compact niches use `mean`; p10/p50/p90 remain available for sensitivity checks.
- `value`: climate value for that species, month, variable, and metric.
- `climate_period`: currently `1981-2010`.
- `range_scope`: currently `us_study_area` for the recommended analysis product.

### `species_climate_niches_us_study_area.parquet`

This is the compact species trait table used by thermophilization scripts.

Grain:

```text
one row per species_key
```

Indicators:

- `tmean_annual_mean`: mean of monthly mean temperature values across the year.
- `tmean_warmest_month_mean`: warmest monthly mean temperature.
- `tmean_coldest_month_mean`: coldest monthly mean temperature.
- `temp_seasonality_mean`: `tmean_warmest_month_mean - tmean_coldest_month_mean`.
- `cwd_annual_sum`: sum of monthly climate water deficit.
- `cwd_max_month_mean`: maximum monthly climate water deficit.
- `pr_annual_sum`: sum of monthly precipitation.
- `pr_driest_month_mean`: minimum monthly precipitation.

These are species traits. They do not change by FIA plot or inventory year. They describe the realized climate envelope of the species' BIEN range during the 1981-2010 baseline climate period.

## Current Gap Summary

The current gap ledger is produced by:

```bash
Rscript 06_species_niches/qa/04_document_species_niche_gaps.R
```

Current summary after the full rerun:

| Gap stage | Species count | Meaning |
| --- | ---: | --- |
| BIEN ranges available | 4,216 | These species can proceed to polygon download and TerraClimate extraction. |
| BIEN ranges missing | 1,678 | BIEN did not report a range map for the queried name. |
| Not targeted for BIEN niche lookup | 660 | Pseudo taxa, genus-only records, unknown categories, or other records that are not clean species-level niche targets. |
| Study-area niches available | 4,186 | These species have compact niche values for downstream CWM analysis. |
| Outside/empty after study-area clip | 30 | BIEN ranges exist, but no study-area TerraClimate rows were produced. |

The gap summaries, action summaries, study-area climate-gap summaries, and CWM
coverage QA have been regenerated from the current products.

Tracked summary files:

- `06_species_niches/qa/outputs/species_niche_gap_summary.csv`
- `06_species_niches/qa/outputs/species_niche_gap_action_summary.csv`
- `06_species_niches/qa/outputs/study_area_climate_gap_summary.csv`
- `06_species_niches/qa/outputs/tnrs_bien_missing_name_review_summary.csv`
- `06_species_niches/lookups/manual_bien_name_overrides_reviewed.csv`

The full ledger, action queue, TNRS candidate table, and species-level diagnostic files are generated locally in `qa/outputs/` but are intentionally ignored by Git to keep commits readable. Regenerate the gap and TNRS review outputs any time scripts `01` through `05` are rerun or manual name overrides change.

## Missing Data And Name Review

Several different issues can prevent a source species from getting a usable climate niche. They need different responses, so the workflow keeps them separate instead of treating all missing values as the same problem.

| Issue | Plain-language meaning | How we handle it |
| --- | --- | --- |
| Genus-level or unknown records | FIA/P2VEG identifies the plant only as `sp.`, `spp.`, or unknown, such as `Amelanchier spp.` or `Tree unknown`. | Exclude from the main species-level CWM, document the lost weight, and consider a later genus-level sensitivity analysis if these records dominate. |
| Infraspecific records | FIA/NRCS identifies the plant below species level using `var.` or `ssp.`. These are legitimate botanical ranks, not the same as `spp.`. | First try the exact infraspecific name. If BIEN has no exact range, review whether the parent species is an acceptable fallback. Any parent-species fallback must be flagged because it broadens the niche from variety/subspecies to species. |
| FIA/NRCS name not found by BIEN | The source has a species name, but BIEN does not return a range for that exact name. | Use TNRS and supporting sources to check whether the name is an older synonym or accepted under another name. Only reviewed, high-confidence overrides are applied. |
| TNRS accepted name is ecologically ambiguous | TNRS gives an accepted name, but the accepted name may not match the FIA common name or ecological meaning. | Flag for expert review instead of applying automatically. `Quercus prinus` is the example: TNRS returns `Quercus michauxii`, but FIA `REF_SPECIES` confirms the code is chestnut oak, which supports `Quercus montana` as the likely modern name. |
| BIEN range exists, but no study-area climate rows | BIEN has a polygon, but the study-area-clipped TerraClimate extraction produces no rows. | Diagnose whether the BIEN polygon is outside the configured study-area bounding box or whether there is an extraction/geometry issue. Use global fallback only with an explicit flag. |
| Valid species with no BIEN range | The name appears valid, but BIEN does not provide a range map. | Keep missing, document the impact, or add an alternate range source if the species has high CWM weight. |

TNRS is used as a first-pass taxonomic name-resolution tool. Important TNRS fields are the submitted name, match score, taxonomic status, accepted name, accepted species, source, warnings, and unmatched terms. TNRS results are evidence, not automatic replacements. A reviewed name override should only be used by the pipeline when it appears in:

```text
06_species_niches/lookups/manual_bien_name_overrides_reviewed.csv
```

with:

```text
review_status = ready_for_pipeline
```

Script `02_check_bien_ranges.R` applies only those ready overrides and writes an audit table:

```text
06_species_niches/qa/outputs/manual_bien_overrides_applied.csv
```

Current reviewed examples:

| Source name | Current decision |
| --- | --- |
| `Chamaecyparis nootkatensis` | Ready to use `Callitropsis nootkatensis`. |
| `Quercus prinus` | Ready to use `Quercus montana` because FIA `REF_SPECIES` confirms this code is chestnut oak (`QUPR2`), while TNRS's `Quercus michauxii` refers to swamp chestnut oak. |
| `Nyssa biflora` | Documented exclusion from the main analysis; do not substitute broader `Nyssa sylvatica` or `Nyssa aquatica`. |
| `Metrosideros polymorpha` | Documented exclusion from the main analysis; valid name but no BIEN range, and alternate Hawaii range source would be needed for a future targeted analysis. |
| `Acer leucoderme` | Documented exclusion from the main analysis; do not substitute broad `Acer saccharum` because it would over-broaden chalk maple's niche. |

The main CWM models should keep `frac_weight_with_niche` and related coverage columns. Low coverage means the community-weighted mean is based on only part of the observed seedling community, not that the BIEN range itself is small.

For QA file details behind these categories, see [Species Niche QA: Gap Ledger](qa/README.md#gap-ledger).

## Required Validation Before Modeling

Run:

```bash
Rscript 06_species_niches/qa/01_validate_species_niche_workflow.R
```

Detailed QA instructions live in [Species Niche QA](qa/README.md).

Core outputs:

- `06_species_niches/qa/outputs/species_niche_validation_checks.csv`
- `06_species_niches/qa/outputs/species_niche_validation_summary.csv`
- `06_species_niches/qa/outputs/species_niche_validation_decision.csv`
- `06_species_niches/qa/outputs/species_niche_gap_summary.csv`
- `06_species_niches/qa/outputs/species_niche_gap_action_summary.csv`
- `06_species_niches/qa/outputs/study_area_climate_gap_summary.csv`
- `06_species_niches/qa/outputs/bien_range_missing_species.csv`

Validation rules:

- Any `severity = error` and `status = fail` means the workflow is structurally broken and should not proceed.
- `severity = warning` means the product can exist, but the issue must be reviewed or resolved before final modeling.
- `proceed_to_script_04 = TRUE` means scripts `01` through `03` are consistent enough to begin or resume TerraClimate extraction.
- `proceed_to_modeling = TRUE` means no unresolved validator warnings remain.
- Current validation has no failed `error` checks. It still has warning checks that must be reviewed before final modeling.

Current warning topics to inspect:

- Saplings are not represented as a distinct species-universe layer.
- BIEN missing fraction is above the default warning threshold.
- 18 BIEN polygons have non-positive planar area in QA.
- 30 BIEN-available species have no study-area TerraClimate rows after clipping.
- 2,923 plot-condition rows have seedlings but zero usable species niche weight.

### How The QA Issues Are Organized

The validator uses `error` for broken handoffs and `warning` for issues that need scientific review. A warning is not automatically a bad dataset; it means the issue must be fixed, filtered, or documented before final modeling.

| QA category | What it means | Current state | What to do |
| --- | --- | --- | --- |
| Structural blockers | Required files, unique keys, valid joins, and one polygon per BIEN-available species. | Passing. | No action unless a future rerun fails an `error` check. |
| Sampling-frame warning | Saplings are present in FIA `TREE`, but not labeled as a separate `community_layers = sapling` species-universe layer. | Warning. | Do not claim sapling-specific niche coverage until saplings are explicitly separated; seedling CWM is still usable. |
| BIEN coverage warning | BIEN has no range for 1,678 of 5,894 targeted records. | Warning. | Review high-weight missing species first; low-weight records can remain documented gaps. |
| Polygon geometry warning | 18 polygons have non-positive projected area in QA. | Warning. | Inspect as geometry/projection edge cases; empty and invalid geometries already pass blocking checks. |
| Study-area climate gap | BIEN range exists, but the study-area-clipped TerraClimate output has no rows. | 30 species. | Diagnostics classify these as outside the configured study-area bounding box; CWM can use global fallback values while flagging their weight. |
| CWM coverage warning | Some FIA plot conditions have seedlings, but none of those seedling species have usable niche values. | 2,923 condition rows. | Filter or flag zero-coverage rows before modeling; use `frac_weight_with_niche` for partial-coverage rows. |
| Taxonomic action queue | Missing species are prioritized by downstream seedling weight and action type. | High/medium records remain. | Genus-level records are exclusions; infraspecific records need parent-species fallback review; true species may need synonym or alternate range review. |

The action queue labels are deliberately different:

- `exclude_sp_spp_from_main_cwm`: genus-level `sp.` / `spp.` records; exclude from species-level CWM.
- `review_infraspecific_parent_species_fallback`: real `var.` / `ssp.` taxa with no exact BIEN range; decide whether using the parent species is acceptable.
- `try_synonym_or_alternate_range_source`: true species with no BIEN range; look for a synonym or a different range source.
- `document_study_area_or_climate_gap`: BIEN range exists, but no study-area clipped climate niche was produced. Use global fallback only with explicit `niche_scope_used` / fallback coverage flags.

## How To Interpret Gaps

Not every gap has the same importance.

- Pseudo taxa such as `Amelanchier spp.` do not have a species-level climate niche because the taxonomic unit is broader than a species. These should be excluded from species-niche CWMs or handled with a documented genus-level sensitivity analysis.
- Infraspecific taxa such as varieties (`var.`) and subspecies (`ssp.`) are real taxa in FIA/NRCS documentation. They should not be lumped with `sp.` / `spp.` records. If a parent-species niche is used, the output should flag that taxonomic broadening.
- Name mismatches or synonym issues, such as `Quercus prinus` vs accepted alternatives, should be reviewed manually if they carry high seedling weight.
- Species without BIEN range maps can be left missing if they are rare in the response data, but high-weight species should either get a manual name fix, a different range source, or a documented exclusion.
- Study-area clipped gaps can use global fallback niches when the species occurs in FIA. These rows should remain flagged because the niche may represent the broader global/native range rather than the U.S. realized range.

Downstream scripts must keep coverage columns, such as `frac_weight_with_niche`, so models can filter or flag plot conditions whose community-weighted means rely on incomplete species niche coverage.

The current default CWM mode is `us_study_area_with_global_fallback`: study-area niches are used first, and global niches fill only species without study-area niches. The CWM product tracks fallback influence with columns such as `niche_scopes_used` and `frac_weight_with_global_fallback_niche`.

## Remote Server Handoff

Before moving this workflow to a remote server, aim for one of two clearly labeled states:

1. **Code handoff only:** scripts, configuration templates, lookups, and documentation are current, but large data products will be regenerated on the server.
2. **Code plus data handoff:** scripts are current and the large generated products have been transferred outside Git.

Git intentionally does not track most generated data under `data/`, large spatial files, or local configuration. A remote run needs either regenerated or transferred copies of:

- `06_species_niches/data/processed/species_universe.parquet`
- `06_species_niches/data/processed/bien_range_availability.parquet`
- `06_species_niches/data/processed/species_range_polygons.gpkg`
- `06_species_niches/data/processed/species_range_climate_us_study_area.parquet`
- `06_species_niches/data/processed/species_climate_niches_us_study_area.parquet`
- `07_thermophilization/data/processed/plot_recruitment_cwm.parquet`

The clean-slate rerun after the current name-override update is:

```bash
Rscript 06_species_niches/scripts/03_download_bien_ranges.R
Rscript 06_species_niches/qa/01_validate_species_niche_workflow.R
Rscript 06_species_niches/scripts/04_extract_terraclimate_from_ranges.R --range-scope=us_study_area
Rscript 06_species_niches/scripts/05_build_species_climate_niches.R --range-scope=us_study_area
Rscript 07_thermophilization/scripts/01_build_plot_recruitment_cwm.R
Rscript 06_species_niches/qa/01_validate_species_niche_workflow.R
Rscript 06_species_niches/qa/04_document_species_niche_gaps.R
Rscript 06_species_niches/qa/05_prioritize_species_niche_gap_actions.R
Rscript 06_species_niches/qa/06_validate_study_area_climate_gaps.R
```

Script `04` is the long Google Earth Engine step. It uses batch caches, but the cache is keyed to the ordered species set so a changed BIEN species set cannot silently reuse incompatible old batches.

Keep local credentials and machine-specific paths out of Git. Use `local/user_config.yaml` or `.Renviron` on each machine for Earth Engine, Python, and other local settings.
