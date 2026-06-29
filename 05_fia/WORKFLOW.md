# FIA Data Pipeline - Technical Workflow

**Navigation:** [Repo Home](../README.md) | [Docs Hub](../docs/README.md) | [Setup](../scripts/SETUP.md) | [Reproduce](../docs/REPRODUCE.md) | [Pipeline Map](../docs/PIPELINE_MAP.md) | [Data Products](../docs/DATA_PRODUCTS.md) | [FIA Visual Explainer](../docs/fia-explorer.html) | [FIA README](README.md) | [Scripts](scripts/)

For a quick-start guide and directory overview, see **README.md**. This document covers per-script technical details, data flow, usage examples, and field references.

---

## Status

- [ ] Download FIA CSVs by state (`01_download_fia.R`)
- [ ] Inspect schema, generate lookup parquets (`02_inspect_fia.R`)
- [ ] Extract tree/BA/condition metrics (`03_extract_trees.R`)
- [ ] Extract seedlings and mortality (`04_extract_seedlings_mortality.R`)
- [ ] Build plot-level summaries + exclusion flags (`05_build_fia_summaries.R`)

Optional site-climate extension:

- [ ] Build distinct FIA site list (`site_climate/01_build_site_list.R`)
- [ ] Extract TerraClimate at FIA site locations (`site_climate/02_extract_terraclimate.R`)

---

## Data Source

**FIADB v9.4** (August 2025) - Forest Inventory and Analysis Database

- Distributor: FIA DataMart (`https://apps.fs.usda.gov/fia/datamart/CSV/`)
- Access method: `rFIA::getFIA()` downloads state-level CSVs from DataMart
- Reference tables (REF_SPECIES, REF_FOREST_TYPE) downloaded directly via `download.file()`
- User Guide: `wo-v9-4_Aug2025_UG_FIADB_database_description_NFI.pdf` (repo root)

## Plot Design

FIA uses a nationally consistent design: four 24-ft radius subplots per plot, each 1/24 acre. Each tree >=5" DBH is tallied on the subplot. Smaller trees (1-4.9") are tallied on a 6.8-ft radius microplot (1/300 acre). Seedlings (<1" DBH) are counted on the microplot.

For a visual version of this layout and the related FIADB table grain, see the static [FIA visual explainer](../docs/fia-explorer.html).

The `TPA_UNADJ` field on each TREE record is the unadjusted per-acre expansion factor derived from the subplot sampling design. Summing `TPA_UNADJ * BA` across trees gives per-acre basal area without needing to know subplot areas directly.

---

## Script Details

### [01_download_fia.R](scripts/01_download_fia.R)

**Inputs:** `config.raw.fia.states`, `config.raw.fia.tables_required`

**Outputs:** `05_fia/data/raw/{STATE}/{STATE}_{TABLE}.csv`

**Processing:**
- Downloads national REF_SPECIES and REF_FOREST_TYPE CSVs once to `data/raw/REF/`
- Loops over 50 states; uses `rFIA::getFIA(states=st, dir=state_dir, load=FALSE)`
- Skip-if-exists guard: checks all expected CSVs before calling `getFIA()`
- `tryCatch` per state so one failure does not abort the loop
- Accepts optional command-line state list for partial reruns

**New package required:** `rFIA` (not in renv.lock; install with `renv::install("rFIA")`)

---

### [02_inspect_fia.R](scripts/02_inspect_fia.R)

**Inputs:** `data/raw/REF/REF_SPECIES.csv`, `data/raw/REF/REF_FOREST_TYPE.csv`, sample of state CSVs

**Outputs:**
- `lookups/ref_species.parquet` - SPCD, COMMON_NAME, GENUS, SPECIES, SCIENTIFIC_NAME, SFTWD_HRDWD, WOODLAND, MAJOR_SPGRPCD, JENKINS_SPGRPCD
- `lookups/ref_forest_type.parquet` - FORTYPCD lookup

**Processing:**
- `SFTWD_HRDWD` values: `"S"` = softwood, `"H"` = hardwood (text, not 0/1)
- `WOODLAND` values: `"Y"` = woodland species (root collar measurement), `"N"` = standard
- Schema check reads just the header row (`fread(f, nrows=0)`) for efficiency
- Required columns per table are hardcoded inline (see script)

---

### [03_extract_trees.R](scripts/03_extract_trees.R)

**Inputs:** `data/raw/{ST}/{ST}_TREE.csv`, `data/raw/{ST}/{ST}_PLOT.csv`, `data/raw/{ST}/{ST}_COND.csv`, `lookups/ref_species.parquet`

**Outputs:**
- `data/processed/trees/state={ST}/trees_{ST}.parquet` - per-species BA by stratum
- `data/processed/cond/state={ST}/cond_{ST}.parquet` - FORTYPCD + condition attributes
- `data/processed/damage_agents/state={ST}/damage_agents_{ST}.parquet` - live-tree damage-agent abundance by condition and species
- `data/processed/harvest_flags/state={ST}/harvest_flags_{ST}.parquet` - plot visits with incidental harvest mortality codes

**Processing (per state):**

1. Load TREE, PLOT, COND with `fread(select=required_cols)` for speed
2. Filter TREE: `STATUSCD %in% c(1,2)`, `DIA >= 1.0`, `TPA_UNADJ > 0`, `INVYR` range (`DIA` - diameter)
3. Derive `ba_sqft_tree = 0.005454 * DIA^2`
4. Derive `size_class` from DIA using `fcase()`:
   - sapling: DIA < 5.0
   - intermediate: 5.0 <= DIA < 12.0
   - mature: DIA >= 12.0
5. Derive `canopy_layer` from CCLCD using `fcase()`:
   - overstory: CCLCD in {1, 2, 3}
   - understory: CCLCD in {4, 5}
   - CCLCD NA fallback: DIA >= 5.0 = overstory, else understory
6. Join `ref_sp[, .(SPCD, SFTWD_HRDWD, WOODLAND)]` on SPCD via data.table key join
7. Aggregate by `[PLT_CN, INVYR, SPCD, SFTWD_HRDWD, WOODLAND, STATUSCD, size_class, canopy_layer]`:
   - `ba_sqft = sum(ba_sqft_tree)`
   - `ba_per_acre = sum(TPA_UNADJ * ba_sqft_tree)`
   - `n_trees_tpa = sum(TPA_UNADJ)`
   - `n_trees_raw = .N`
8. For COND: filter to INVYR range, add STATECD via PLOT join, write parquet

**Memory:** Each state processes its TREE CSV in isolation. `rm()` + `gc()` after each state.

---

### [04_extract_seedlings_mortality.R](scripts/04_extract_seedlings_mortality.R)

**Inputs:**
- `data/raw/{ST}/{ST}_SEEDLING.csv`
- `data/raw/{ST}/{ST}_TREE_GRM_COMPONENT.csv`
- `data/raw/{ST}/{ST}_TREE.csv` (slim read for TRE_CN -> INVYR/SPCD/AGENTCD join)
- `lookups/ref_species.parquet`

**Outputs:**
- `data/processed/seedlings/state={ST}/seedlings_{ST}.parquet`
- `data/processed/mortality/state={ST}/mortality_{ST}.parquet`

**SEEDLING processing:**
- Filter: `TREECOUNT > 0`, INVYR range
- Seedlings are counted on the microplot (1/300 acre) per species per condition
- Aggregated: `treecount_total = sum(TREECOUNT)` by `[stable_plot_id, PLT_CN, INVYR, CONDID, SUBP, SPCD]`
- Retains optional FIA density fields as `treecount_calc_total` and `seedlings_tpa` when available
- Use `--force-seedlings` when refreshing older seedling parquets to the condition/subplot grain

**Important for thermophilization work:** this per-state seedling product preserves species identity through `SPCD` and keeps `CONDID`/`SUBP` so seedlings can be joined to exact condition metadata. Species identity is dropped only later when `05_build_fia_summaries.R` creates the compact plot-year summary `plot_seedling_metrics.parquet`. Use `plot_seedling_species.parquet` for recruitment composition and the plot summary only for total seedling count, richness, density, and Shannon diversity.

**TREE_GRM_COMPONENT processing:**
- Filter: `MICR_COMPONENT_AL_FOREST IN ('MORTALITY1','MORTALITY2','CUT1','CUT2')`
- Filter: `MICR_TPAMORT_UNADJ_AL_FOREST > 0` and not NA
- `component_type = "natural"` for MORTALITY1/2, `"harvest"` for CUT1/2
- Join to TREE (slim: CN, SPCD, AGENTCD, INVYR) via `TRE_CN` to get INVYR, SPCD, AGENTCD (TREE_GRM_COMPONENT does not carry INVYR directly)
- Filter INVYR range; join REF_SPECIES for SFTWD_HRDWD
- Aggregate: `tpamort_per_acre = sum(MICR_TPAMORT_UNADJ_AL_FOREST)` by `[PLT_CN, INVYR, SPCD, SFTWD_HRDWD, AGENTCD, component_type]`

---

### [05_build_fia_summaries.R](scripts/05_build_fia_summaries.R)

**Inputs:** Per-state partitioned parquets from scripts 03 and 04

**Outputs:** `data/processed/summaries/` (13 national parquet files)

**Processing:**
- Uses `open_dataset(..., partitioning="state")` to read partitioned parquets lazily
- State-loop collect pattern: one state at a time to control memory
- Shannon H computed with custom `compute_shannon_h()` in data.table (Arrow cannot compute `log()` lazily)

#### Detailed Output Provenance

Script `05` is an orchestrator. Each output is implemented by one focused
builder under `05_fia/scripts/summaries/`.

| Builder | Output | Grain | Main inputs | Purpose |
|---|---|---|---|---|
| `build_tree_metrics.R` | `plot_tree_metrics.parquet` | Plot visit | State tree partitions and condition coordinates | Structure, basal area, stem density, richness, and diversity |
| `build_seedling_metrics.R` | `plot_seedling_metrics.parquet` | Plot visit | State seedling partitions | Compact seedling totals, richness, and diversity |
| `build_mortality_metrics.R` | `plot_mortality_metrics.parquet` | Plot visit/species/agent/component | State mortality partitions | Natural mortality and harvest-removal summaries |
| `build_condition_forest_type.R` | `plot_cond_fortypcd.parquet` | Plot visit/condition | State condition partitions | Compact forest type and raw disturbance fields |
| `build_condition_metadata.R` | `plot_condition_metadata.parquet` | Plot visit/condition | State condition partitions and forest-type lookup | Stable plot identity and reusable condition-level join backbone |
| `build_tree_species.R` | `plot_tree_species.parquet` | Condition/subplot/species | State tree partitions and condition metadata | Live-tree composition for stems at least 5 inches diameter |
| `build_tree_species.R` | `plot_sapling_species.parquet` | Condition/subplot/species | State tree partitions and condition metadata | Live-sapling composition for stems 1.0-4.9 inches diameter |
| `build_seedling_species.R` | `plot_seedling_species.parquet` | Condition/subplot/species | State seedling partitions and condition metadata | Tree-regeneration composition below 1 inch diameter |
| `build_disturbance_classification.R` | `plot_disturbance_classification.parquet` | Plot visit/condition | Condition metadata | Natural-disturbance/control eligibility, timing, severity proxy, and forest-status fields |
| `build_disturbance_history.R` | `plot_disturbance_history.parquet` | Condition/disturbance slot | State condition partitions | Long-form disturbance codes and years |
| `build_treatment_history.R` | `plot_treatment_history.parquet` | Condition/treatment slot | State condition partitions | Long-form treatment codes and years |
| `build_damage_agents.R` | `plot_damage_agents.parquet` | Condition/species/agent | State damage-agent partitions | Live-tree damage-agent abundance |
| `build_exclusion_flags.R` | `plot_exclusion_flags.parquet` | Plot visit | State condition and harvest-flag partitions | Whole-plot review and sensitivity flags |

The dependency order matters:

```text
condition partitions
  -> plot_condition_metadata
  -> tree/sapling/seedling species products
  -> plot_disturbance_classification
```

The life-stage products use one shared FIA species identity but preserve separate observed communities. Species climate niches are calculated once per taxon downstream, then joined separately to seedling, sapling, and tree composition.

#### Output Data Dictionary

This section is the human-readable schema reference for FIA products. It describes what one row means and what information the columns carry. The parquet schema remains the exact machine-readable authority and can be checked with:

```r
names(arrow::read_parquet("path/to/product.parquet"))
```

Common column families used below:

| Family | Columns | Meaning |
|---|---|---|
| Plot visit | `PLT_CN`, `INVYR`, `state`/`STATECD` | One FIA inventory visit to a plot |
| Stable plot | `stable_plot_id`, `UNITCD`, `COUNTYCD`, `PLOT`, `PREV_PLT_CN` | Identity used to follow a plot location through time |
| Condition | `CONDID`, `COND_STATUS_CD`, `CONDPROP_UNADJ`, `pct_forested` | A mapped land/forest condition within a plot visit |
| Subplot | `SUBP` | One subplot or associated microplot sampling location |
| Species identity | `SPCD`, `COMMON_NAME`, `SCIENTIFIC_NAME`, `GENUS`, `SPECIES`, `SFTWD_HRDWD`, `WOODLAND`, `MAJOR_SPGRPCD`, `JENKINS_SPGRPCD` | FIA species code, names, and broad species groups |
| Location | `LAT`, `LON`, `ELEV` | Public FIA plot coordinates and elevation |
| Forest type | `FORTYPCD`, `forest_type_label`, `forest_type_group` | FIA forest type code and lookup labels |
| Disturbance slots | `DSTRBCD1-3`, `DSTRBYR1-3` | Up to three condition disturbance codes and years |
| Treatment slots | `TRTCD1-3`, `TRTYR1-3` | Up to three silvicultural treatment codes and years |

##### State-Partitioned Products

##### Tree State Partitions

One row represents a species within a plot visit, condition, subplot, live/dead status, size class, and canopy layer. It contains plot/condition/subplot and species identity fields plus:

- `STATUSCD`: live or dead status.
- `size_class`: sapling, intermediate, or mature diameter class.
- `canopy_layer`: overstory or understory crown position.
- `ba_sqft`: unexpanded basal area represented by the source records.
- `ba_per_acre`: expanded basal area per acre.
- `n_trees_tpa`: expanded trees per acre.
- `n_trees_raw`: number of source tree records.

##### Condition State Partitions

One row represents one mapped condition within a plot visit. It contains the plot visit, stable plot, condition, location, forest type, disturbance-slot, and treatment-slot column families. This is the source for condition metadata and disturbance/control classification.

##### Damage-Agent State Partitions

One row represents one tree species and damage agent within a plot visit and condition. Columns:

```text
PLT_CN, INVYR, CONDID, SPCD, SFTWD_HRDWD, DAMAGE_AGENT_CD, ba_per_acre, n_trees_tpa
```

It reports the basal area and trees per acre carrying each live-tree damage agent code.

##### Harvest-Flag State Partitions

One row identifies a plot visit where at least one tree has an incidental harvest/removal agent code (`AGENTCD` 80-89). Columns:

```text
PLT_CN, STATECD, INVYR
```

Only positive flags are stored. Absence from this table means no such code was found in the processed TREE records.

##### Seedling State Partitions

One row represents one tree species counted on a subplot microplot within a plot visit and condition. It contains stable plot, condition, subplot, and species identity fields plus:

- `treecount_total`: sum of raw FIA `TREECOUNT`.
- `treecount_calc_total`: sum of FIA calculated counts when available.
- `seedlings_tpa`: expanded seedlings per acre when available.
- `n_seedling_records`: number of source SEEDLING records summarized.

##### Mortality State Partitions

One row represents mortality or harvest removal for one species and agent within a plot visit. In plain language, it contains an entry per species with the plot, year, softwood/hardwood group, mortality agent, mortality type, and trees lost per acre for each state. Columns:

```text
PLT_CN, INVYR, SPCD, SFTWD_HRDWD, AGENTCD,
component_type, tpamort_per_acre
```

- `component_type`: `natural` for mortality or `harvest` for removals.
- `tpamort_per_acre`: expanded trees per acre that died or were removed.

##### National Summary Products

##### Plot Tree Metrics

One row per plot visit containing broad tree-community structure. Identifiers and location are followed by:

- Live/dead abundance: `ba_live_total`, `n_trees_live`, `ba_dead_total`, `n_trees_dead`.
- Species groups: `ba_live_softwood`, `ba_live_hardwood`.
- Size classes: `ba_live_sapling`, `ba_live_intermediate`, `ba_live_mature`.
- Canopy layers: `ba_live_overstory`, `ba_live_understory`.
- Diversity: `n_species_live`, `shannon_h_ba`.
- `species_temp_optima_mean`: legacy placeholder; current niche analysis uses the dedicated species-niche workflow instead.

##### Plot Seedling Metrics

One row per plot visit containing compact seedling totals and diversity:

```text
PLT_CN, INVYR, treecount_total, count_softwood, count_hardwood, n_species_seedling, shannon_h_count, state
```

This table does not preserve individual species. Use `plot_seedling_species.parquet` for composition and CWM analysis.

##### Plot Mortality Metrics

One row per plot visit, species, mortality agent, and component type. It has the same biological fields as the state mortality partitions plus `state`:

```text
PLT_CN, INVYR, SPCD, SFTWD_HRDWD, AGENTCD, component_type, tpamort_per_acre, state
```

##### Plot Condition Forest Type

One compact row per plot visit and condition. It contains plot visit, condition, location, forest type, condition proportion/status, and the three disturbance code/year slots. Use it for lightweight raw-code inspection; use `plot_condition_metadata.parquet` for downstream joins.

##### Plot Condition Metadata

One row per plot visit and condition. It contains the stable plot, condition, location, forest type, disturbance-slot, and treatment-slot families plus:

- `n_conditions`, `pct_forested`: whole-plot condition context.
- `is_forested_condition`: whether FIA classifies this condition as forest.
- `has_fire_condition`, `has_crown_fire_condition`,
  `has_insect_condition`, `has_disease_condition`,
  `has_wind_condition`, `has_drought_condition`,
  `has_human_dist_condition`, `has_cutting_treatment`: convenient
  condition-level flags.

This is the primary metadata join table for species-composition products.

##### Plot Tree Species

One row per condition, subplot, and species for live trees at least 5 inches diameter. It combines condition, subplot, species, location, forest type, disturbance, and treatment fields with:

- `ba_per_acre`, `n_trees_tpa`, `n_trees_raw`: species abundance.
- `n_tree_strata`, `size_classes_present`, `canopy_layers_present`: source strata represented by the row.
- `source_table`, `source_species_code`, `species_key`,
  `community_layer`: provenance and downstream join fields.
- `abundance_for_cwm`: default adult-tree community weight, primarily basal area per acre.

##### Plot Sapling Species

One row per condition, subplot, and species for live saplings 1.0-4.9 inches diameter. It has the same schema as `plot_tree_species.parquet`, but:

- `community_layer = "sapling"`.
- `size_classes_present = "sapling"`.
- `abundance_for_cwm` uses sapling trees per acre.

##### Plot Seedling Species

One row per condition, subplot, and species for tree regeneration below 1 inch diameter. It combines condition, subplot, species, stable plot, location, forest type, disturbance, and treatment fields with:

```text
treecount_total, treecount_calc_total, seedlings_tpa, n_seedling_records
```

This is the primary recruitment-composition input to thermophilization CWMs.

##### Plot Disturbance Classification

One row per plot visit and condition. It contains condition metadata plus:

- Region: `region_east_west`, `region_source`.
- Forest eligibility: `is_forested_condition`, `is_forest_dominated_plot`, `is_forested_analysis_condition`.
- Disturbance/treatment presence flags.
- Classification: `natural_disturbance_primary`, `disturbance_class_primary`, `disturbance_class`.
- Complexity/severity: `n_natural_disturbance_classes`, `is_multiple_natural_disturbance`, `is_high_severity_proxy`,   `high_severity_proxy_type`.
- Timing: earliest/latest disturbance, treatment, and cutting years plus time-since fields and continuous-year flags.
- Eligibility: `is_control_candidate`, `is_natural_disturbance_candidate`, `disturbed_vs_control`, `control_eligibility_reason`.

This is the primary disturbance/control analysis table.

##### Plot Disturbance History

One row per condition and populated disturbance slot:

```text
DSTRBCD, disturbance_label, disturbance_category, PLT_CN, INVYR, STATECD, CONDID, CONDPROP_UNADJ, LAT, LON, disturbance_slot, DSTRBYR
```

It preserves all recorded disturbance events instead of selecting one primary class.

##### Plot Treatment History

One row per condition and populated treatment slot:

```text
TRTCD, treatment_label, treatment_category, PLT_CN, INVYR, STATECD, CONDID, CONDPROP_UNADJ, LAT, LON, treatment_slot, TRTYR
```

It preserves cutting, site preparation, regeneration, and other silvicultural treatments.

##### Plot Damage Agents

One row per condition, tree species, and damage agent. Columns:

```text
DAMAGE_AGENT_CD, agent_label, agent_category, PLT_CN, INVYR, CONDID, SPCD, SFTWD_HRDWD, ba_per_acre, n_trees_tpa, state
```

It adds readable labels/categories to the state damage-agent extracts.

##### Plot Exclusion Flags

One row per plot visit containing whole-plot review and sensitivity flags:

```text
PLT_CN, INVYR, STATECD, n_conditions, pct_forested, exclude_nonforest, exclude_human_dist, exclude_harvest, exclude_harvest_agent, exclude_any, has_fire, has_insect
```

These flags describe the whole plot visit. They should not automatically remove an otherwise eligible condition; current condition-level analysis retains them as warnings and sensitivity filters.

#### Interpretation And FIA Code References

**Current control definition:** a control candidate must be an FIA-forested condition with no recorded disturbance code and no recorded treatment code. The primary condition-level analysis uses FIA's own condition classification, `COND_STATUS_CD == 1`. The separate field `is_forest_dominated_plot` records whether at least 50% of the whole plot visit is forest. That plot-level threshold is retained for sensitivity analyses but does not disqualify an FIA-forested condition merely because another mapped condition on the same plot is nonforest.

**plot_damage_agents columns:**

- `PLT_CN`, `INVYR`, `CONDID`, `SPCD`, `SFTWD_HRDWD`, `state`
- `DAMAGE_AGENT_CD` (5-digit PTIPS/FHAAST code from Appendix H)
- `agent_label` (e.g. "Mountain pine beetle", "Spruce budworm") — NA for unlabelled codes
- `agent_category` (bark beetles / defoliators / sucking insects / boring insects / root/butt disease / canker/rust / foliage/wilt disease / fire / other)
- `ba_per_acre`, `n_trees_tpa` (BA and TPA of live trees carrying that damage code)

**plot_treatment_history columns (Step 5b):**

One row per **condition × treatment slot** where TRTCD ≠ 0. Mirrors `plot_disturbance_history` but for active management treatments recorded in COND.TRTCD1/2/3. Requires TRTCD columns in cond parquets (run `03_extract_trees.R --force-cond` if missing).

- `PLT_CN`, `INVYR`, `STATECD`, `CONDID`, `CONDPROP_UNADJ`, `LAT`, `LON`
- `treatment_slot` (1, 2, or 3 — TRTCD1/2/3)
- `TRTCD` (raw FIA code), `TRTYR` (year of treatment; 9999 = continuous)
- `treatment_label` — human-readable: "Cutting", "Site preparation", "Artificial regeneration", "Natural regeneration", "Other silvicultural treatment"
- `treatment_category` — broad class: harvest / site_prep / regeneration / other_silv

| TRTCD | Label | Category |
|-------|-------|----------|
| 10 | Cutting | harvest |
| 20 | Site preparation | site_prep |
| 30 | Artificial regeneration | regeneration |
| 40 | Natural regeneration | regeneration |
| 50 | Other silvicultural treatment | other_silv |

**plot_exclusion_flags columns (Step 7):**

- `PLT_CN`, `INVYR`, `STATECD`, `n_conditions` (number of conditions in plot)
- `pct_forested` (sum of CONDPROP_UNADJ where COND_STATUS_CD == 1; 0–1)
  - FIA samples **all US land**; ~59% of plot×year rows have `pct_forested == 0`.
    Use `pct_forested >= 0.5` only as an optional whole-plot sensitivity    restriction. Condition-level analyses should use `COND_STATUS_CD == 1` for the condition being analyzed.
- `exclude_nonforest` (logical: any condition has COND_STATUS_CD == 5 — non-forest land with trees, meaning converted/deforested plots that still carry some trees)
- `exclude_human_dist` (logical: any DSTRBCD1/2/3 == 80 "Human-induced")
- `exclude_harvest` (logical: any TRTCD1/2/3 == 10 "Cutting", condition-level)
- `exclude_harvest_agent` (logical: any TREE record has AGENTCD 80–89, tree-level incidental harvest mortality; more sensitive than the condition-level TRTCD flag. Requires harvest_flags parquets from `03_extract_trees.R`.)
- `exclude_any` (logical: OR of all four `exclude_*` flags above — use for standard clean-plot filter)
- `has_fire` (logical: any DSTRBCD1/2/3 in {30,31,32} — **positive filter**, not an exclusion)
- `has_insect` (logical: any DSTRBCD1/2/3 in {10,11,12} — **positive filter**)

**Key disturbance codes (FIADB v9.4 COND.DSTRBCD):**

| Code | Meaning |
|------|---------|
| 10   | Insect damage (general) |
| 11   | Insect damage — understory |
| 12   | Insect damage — trees |
| 30   | Fire (general) |
| 31   | Ground fire |
| 32   | Crown fire |
| 80   | **Human-induced** (logging, development, clearing — exclude most analyses) |
| COND_STATUS_CD = 1 | Forested condition (keep); != 1 means deforested/non-forest |
| TRTCD = 10 | Cutting treatment (deforestation/harvest) |

---

## Optional Site-Climate Extension

The core FIA pipeline ends with `05_build_fia_summaries.R`. The following
scripts build a separate TerraClimate-at-FIA-plots dataset. They are not inputs
to the current BIEN-range species-niche workflow.

### [01_build_site_list.R](scripts/site_climate/01_build_site_list.R)

**Input:** State condition partitions from `03_extract_trees.R`

**Output:**

- `data/processed/site_climate/all_site_locations.csv`

**Grain:** One row per `stable_plot_id`

**Columns:**

```text
site_id, latitude, longitude, source
```

In plain language, this table contains one representative public FIA coordinate for each stable plot location. `site_id` is the FIA `stable_plot_id`, and `source` is `"FIA"`. It is the location input to the TerraClimate extraction.

**Processing:**

1. Reads distinct `stable_plot_id`, `LAT`, and `LON` combinations from the condition dataset.
2. Removes missing and zero coordinates.
3. Selects one representative public FIA coordinate per stable plot because fuzzed coordinates can vary slightly across visits.
4. Writes `site_id = stable_plot_id` so site climate joins directly to FIA biological products.

Regenerating the site list changes the set of GEE input points. Remove or
separate stale files under `site_climate/_gee_annual/` before rerunning
`02_extract_terraclimate.R`.

---

### [02_extract_terraclimate.R](scripts/site_climate/02_extract_terraclimate.R)

**Inputs:**
- `data/processed/site_climate/all_site_locations.csv`: site_id, latitude, longitude, source
- GEE credentials (`local/user_config.yaml`)

**Outputs:**
- `data/processed/site_climate/site_pixel_map.parquet` — site_id → pixel_id
- `data/processed/site_climate/site_climate.parquet` — long-format monthly climate

**Schema (`site_pixel_map.parquet`):**

```text
site_id, pixel_id, x, y, coverage_fraction
```

One row maps one FIA site to the TerraClimate grid cell used for extraction. Multiple FIA sites can map to the same climate pixel.

**Schema (`site_climate.parquet`):**

| Column | Type | Description |
|--------|------|-------------|
| site_id | character | From all_site_locations.csv |
| year | int | Calendar year |
| month | int | Calendar month (1-12) |
| water_year | int | Oct–Sep water year (month≥10: year+1) |
| water_year_month | int | Month within water year (Oct=1 … Sep=12) |
| variable | character | tmmx / tmmn / pr / def / pet / aet |
| value | double | Scale factors already applied (°C for temp, mm for water) |

**Variable scale factors:** tmmx=0.1, tmmn=0.1, pr=1.0, def=0.1, pet=0.1, aet=0.1

**Processing:**
1. `st_as_sf()` converts lat/lon to sf POINT object
2. A global TerraClimate raster (`rast(-180, 180, -90, 90, res=1/24°)`) is constructed in memory; `terra::cellFromXY()` snaps each site to its containing pixel. `pixel_id` is the global cell number — identical to the ID `extract_climate_from_gee()` embeds in its output — so the consolidation join is unambiguous. `coverage_fraction = 1.0` for all points.
3. `extract_climate_from_gee()` extracts 1958–present from `IDAHO_EPSCOR/TERRACLIMATE` GEE asset
4. Annual parquets consolidated, joined to site_id, pivoted to long format
5. Water year added via `calendar_to_water_year()` from `scripts/utils/time_utils.R`

**Note on coordinate fuzz:** FIA coordinates are fuzzed ~1 mile for privacy, which is well within TerraClimate's ~4km pixel. Multiple nearby FIA plots may map to the same pixel; this is expected and documented in `site_pixel_map.parquet`.

**Note on year range:** TerraClimate begins in 1958. The GEE extraction uses 1958–`config.raw.terraclimate.end_year`. This is a ~66-year range, much larger than the IDS-driven extraction (1997–present), but since FIA site pixels are small in number (~a few thousand unique pixels), GEE processing is fast.

---

## Data Flow

```text
FIA DataMart (50 state CSVs)          data/processed/site_climate/all_site_locations.csv
         |                                      |
         v 01_download_fia.R                    |
05_fia/data/raw/{STATE}/*.csv                   |
         |                                      |
         v 02_inspect_fia.R                     |
lookups/ref_species.parquet + schema checks     |
         |                                      |
         +------------+------------------+      |
         |            |                  |      |
         v            v                  v      |
03_extract_trees.R  04_extract_...     04_extract_...
trees/{state=ST}/   seedlings/{state=ST}/ mortality/{state=ST}/
cond/{state=ST}/    (DSTRBCD/TRTCD + LAT/LON)
damage_agents/{state=ST}/
         |                                      |
         v 05_build_fia_summaries.R             |
summaries/plot_tree_metrics.parquet             |
summaries/plot_seedling_metrics.parquet         |
summaries/plot_mortality_metrics.parquet        |
summaries/plot_cond_fortypcd.parquet            |
summaries/plot_condition_metadata.parquet       |
summaries/plot_tree_species.parquet              |
summaries/plot_sapling_species.parquet           |
summaries/plot_seedling_species.parquet         |
summaries/plot_disturbance_history.parquet      |
summaries/plot_disturbance_classification.parquet |
summaries/plot_treatment_history.parquet        |
summaries/plot_damage_agents.parquet            |
summaries/plot_exclusion_flags.parquet  <-------+---GEE (TerraClimate)
                                                |       |
                                                v site_climate/02_extract_terraclimate.R
                                        site_climate/site_pixel_map.parquet
                                        site_climate/site_climate.parquet
                                          (tmmx, tmmn, pr, def, pet, aet
                                           1958-present, monthly, long format)
```

---

## Decisions Log

| Decision | Rationale | Date |
|----------|-----------|------|
| rFIA for download only; `fread()` for processing | rFIA handles DataMart URL construction; `fread()` gives full column selection control needed for CCLCD/size-class stratification | 2026-02 |
| 5-script structure | Each script has a single responsibility; matches IDS precedent | 2026-02 |
| State-partitioned parquet output | Natural loop unit; resume-safe; `open_dataset()` unifies nationally | 2026-02 |
| CCLCD NA fallback: DIA >= 5.0 = overstory | Many older and woodland plots lack CCLCD; DIA threshold is a defensible proxy for canopy position | 2026-02 |
| Shannon H in data.table after collect() | Arrow 23.0.0 cannot compute `log()` in lazy evaluation mode | 2026-02 |
| Store `ba_sqft` (raw) and `ba_per_acre` separately | Raw for subplot-level aggregation and area weighting; per-acre for cross-plot comparison | 2026-02 |
| INVYR filter: 2000-2024 | FIA annual inventory began ~2000; pre-2000 data is periodic and structurally inconsistent with modern tables | 2026-02 |
| Join TREE_GRM_COMPONENT to TREE for INVYR | GRM table records measurement periods, not calendar years directly; TREE.INVYR is the T2 year | 2026-02 |
| species_temp_optima_mean placeholder column | Schema designed to accept thermophilization join when boss provides species temperature optima dataset | 2026-02 |
| STATUSCD 1 and 2 in same extraction pass | Live and standing dead trees are in the same TREE table; filtering both in one pass avoids reading the file twice | 2026-02 |
| Added TRTCD1/2/3 to cond_cols | Needed for `exclude_harvest` flag in Step 7; requires re-run of 03_extract_trees.R to populate cond parquets | 2026-03 |
| plot_exclusion_flags as separate parquet | Downstream analyses join this once per analysis rather than re-deriving filters from raw DSTRBCD/TRTCD; mirrors R4 staff recommendation to remove human-disturbed plots upfront | 2026-03 |
| FIA site climate: 1958-present | TerraClimate begins in 1958 and FIA sites need the full historical record; IDS pixel_values (1997–) not reused because site pixel set is much smaller and a fresh GEE query is faster | 2026-03 |
| def for CWD | TerraClimate's `def` band = PET − AET = climate water deficit (CWD); same concept as annual CWD used in disturbance risk models | 2026-03 |
| Global TC raster for pixel snapping | `build_pixel_map()` with a region-limited `ref_rast` (reconstructed from IDS pixel_values extent) is unsafe for points outside that region — `cellFromXY()` returns garbage cell numbers that can coincidentally match unrelated pixel IDs, silently extracting data for the wrong locations. A global raster guarantees correct snapping for sites anywhere. See `cleaning_log.md` Issue #001. | 2026-03 |

---

## FIADB Field Reference

Key fields confirmed from User Guide v9.4 (see `docs/FIADB_field_reference.md` for full notes):

| Field | Table | Description |
|-------|-------|-------------|
| DIA | TREE | Diameter in inches at breast height (4.5 ft uphill) for timber; root collar for woodland species |
| STATUSCD | TREE | 1=live, 2=standing dead |
| CCLCD | TREE | Crown class: 1=open grown, 2=dominant, 3=codominant, 4=intermediate, 5=overtopped |
| TPA_UNADJ | TREE | Trees per acre (unadjusted) - per-acre expansion factor based on subplot area |
| TREECLCD | TREE | 2=growing stock, 3=rough cull, 4=rotten cull |
| AGENTCD | TREE | Mortality agent code (what killed the tree) |
| SPCD | TREE/SEEDLING | FIA species code (join to REF_SPECIES) |
| TREECOUNT | SEEDLING | Count on microplot per species/condition; conifers >=6" tall, hardwoods >=12" tall |
| MICR_COMPONENT_AL_FOREST | TREE_GRM_COMPONENT | MORTALITY1/2=natural, CUT1/2=harvest, SURVIVOR, INGROWTH |
| MICR_TPAMORT_UNADJ_AL_FOREST | TREE_GRM_COMPONENT | Per-acre mortality expansion factor |
| FORTYPCD | COND | Forest type code (join to REF_FOREST_TYPE) |
| CONDPROP_UNADJ | COND | Proportion of plot area in this condition |
| SFTWD_HRDWD | REF_SPECIES | "S"=softwood, "H"=hardwood |
| WOODLAND | REF_SPECIES | "Y"=woodland species (root collar measurement), "N"=standard |
| DSTRBCD1 | COND | Disturbance code 1 (most important); **80=human-induced**, 10-12=insects, 20-22=disease, 30-32=fire (31=ground, 32=crown), 50-54=weather; codes 2 & 3 (DSTRBCD2/3) for additional disturbances |
| DSTRBYR1 | COND | Year of disturbance 1; 9999 = continuous; DSTRBYR2/3 parallel DSTRBCD2/3 |
| TRTCD1 | COND | Treatment code 1; **10=Cutting** (deforestation/harvest), 20=Site prep, 30=Artificial regen, 40=Natural regen, 50=Other silvicultural; TRTCD2/3 for additional treatments |
| TRTYR1 | COND | Year of treatment 1; TRTYR2/3 parallel TRTCD2/3 |
| COND_STATUS_CD | COND | **1=Forested** (keep for most analyses), 2=Non-forested, 3=Water, 4=Non-census water, 5=Non-forest land with trees |
| DAMAGE_AGENT_CD1 | TREE | 5-digit PTIPS/FHAAST damage agent code on live trees (Appendix H); up to 3 per tree; 11000s=bark beetles, 12000s=defoliators, 14000s=sucking insects, 15000s=borers, 21000s=root disease, 22000s=cankers |

---

## Usage Examples

### Load Plot Summaries

```r
library(arrow)
library(dplyr)

# All states at once (lazy, partitioned by state)
trees_ds <- open_dataset("05_fia/data/processed/trees", partitioning = "state")

# Filter to one state
co_trees <- trees_ds |> filter(state == "CO") |> collect()

# Final plot-level metrics
metrics <- read_parquet("05_fia/data/processed/summaries/plot_tree_metrics.parquet")

# Species lookup
ref_sp <- read_parquet("05_fia/lookups/ref_species.parquet")
```

---

### Filter to Forested, Undisturbed Plots

FIA samples all US land — ~59% of plot×year rows have `pct_forested == 0`. Always filter to forested plots first before applying disturbance exclusions.

```r
classes <- read_parquet(
  "05_fia/data/processed/summaries/plot_disturbance_classification.parquet"
)

# Primary condition-level gate.
forested_clean <- classes |>
  filter(is_forested_condition, disturbed_vs_control %in% c("control", "disturbed"))

# Optional whole-plot sensitivity restriction.
forest_dominated <- forested_clean |> filter(is_forest_dominated_plot)

# Positive filter: plots that burned
metrics_fire   <- metrics |> inner_join(flags |> filter(has_fire), by = c("PLT_CN", "INVYR"))

# Positive filter: plots with insect damage
metrics_insect <- metrics |> inner_join(flags |> filter(has_insect), by = c("PLT_CN", "INVYR"))
```

**Key disturbance codes (COND.DSTRBCD):**

| Code | Meaning |
|------|---------|
| 10–12 | Insect damage (general, understory, trees) |
| 30–32 | Fire (general, ground, crown) |
| 80 | Human-induced (logging, development, clearing) |
| COND_STATUS_CD = 1 | Forested condition (keep) |

---

### Validate Seedling Products

```r
# From repo root
Rscript 05_fia/scripts/qc/validate_seedling_products.R
```

**3. Run It**
From repo root:

```powershell
& 'C:\Program Files\R\R-4.5.1\bin\Rscript.exe' 05_fia/scripts/qc/validate_seedling_products.R
```

---

### Get Site-Level Climate Data

```r
library(arrow); library(dplyr)
clim <- read_parquet("05_fia/data/processed/site_climate/site_climate.parquet")

# Annual water-year precipitation per site
clim |> filter(variable == "pr") |>
  group_by(site_id, water_year) |>
  summarise(precip_mm = sum(value, na.rm = TRUE))

# All 6 variables: tmmx, tmmn, pr, def (CWD), pet, aet
```

---

## Connection to IDS+Climate Workstream

FIA plots and IDS damage areas share geographic space but use different spatial identifiers. Potential linkage approaches:

1. **County-level join**: Both FIA (STATECD/COUNTYCD) and IDS (REGION_ID) have county codes. Most reliable but coarsest spatial resolution.

2. **Coordinate overlay**: FIA coordinates are fuzzed ~1 mile; IDS polygons are georeferenced. Overlay is approximate but feasible for large IDS polygons.

3. **AGENTCD - DCA_CODE crosswalk** (planned): FIA TREE.AGENTCD (mortality cause) may align with IDS DCA_CODE (damage cause) for bark beetle and other agents. A formal crosswalk table would enable linking FIA mortality records to IDS disturbance polygons. This is a priority future task.

---

## Troubleshooting

**`rFIA::getFIA()` fails for a state**
- Re-run `01_download_fia.R CO` (pass state abbreviation as command-line arg)
- Or download manually: `https://apps.fs.usda.gov/fia/datamart/CSV/{ST}_TREE.csv`

**Missing CCLCD causes unexpected NA canopy_layer**
- Verify DIA fallback is being applied: check `is.na(canopy_layer)` count
- CCLCD is genuinely missing for some woodland species and older periodic plots

**TREE_GRM_COMPONENT has no rows for a state**
- Some states with only periodic (pre-2000) inventory have no GRM records
- The script handles this gracefully; mortality parquet will be empty

**Memory errors on large states (TX, CA)**
- `fread(select=cols)` minimizes memory by only reading needed columns
- `gc()` after each state should release memory
- If still failing, process large states separately: `Rscript 03_extract_trees.R TX`

---

## See also

- [FIA README](README.md)
- [Repo reproduction guide](../docs/REPRODUCE.md)
- [Data products](../docs/DATA_PRODUCTS.md)
- [Shared architecture](../docs/ARCHITECTURE.md)
