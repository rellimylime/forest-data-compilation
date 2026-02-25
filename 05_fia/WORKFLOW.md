# FIA Data Pipeline - Technical Workflow

## Status

- [ ] Download FIA CSVs by state (`01_download_fia.R`)
- [ ] Inspect schema, generate lookup parquets (`02_inspect_fia.R`)
- [ ] Extract tree/BA/condition metrics (`03_extract_trees.R`)
- [ ] Extract seedlings and mortality (`04_extract_seedlings_mortality.R`)
- [ ] Build plot-level summaries (`05_build_fia_summaries.R`)

---

## Data Source

**FIADB v9.4** (August 2025) - Forest Inventory and Analysis Database

- Distributor: FIA DataMart (`https://apps.fs.usda.gov/fia/datamart/CSV/`)
- Access method: `rFIA::getFIA()` downloads state-level CSVs from DataMart
- Reference tables (REF_SPECIES, REF_FOREST_TYPE) downloaded directly via `download.file()`
- User Guide: `wo-v9-4_Aug2025_UG_FIADB_database_description_NFI.pdf` (repo root)

## Plot Design

FIA uses a nationally consistent design: four 24-ft radius subplots per plot,
each 1/24 acre. Each tree >=5" DBH is tallied on the subplot. Smaller trees
(1-4.9") are tallied on a 6.8-ft radius microplot (1/300 acre). Seedlings (<1"
DBH) are counted on the microplot.

The `TPA_UNADJ` field on each TREE record is the unadjusted per-acre expansion
factor derived from the subplot sampling design. Summing `TPA_UNADJ * BA` across
trees gives per-acre basal area without needing to know subplot areas directly.

---

## Script Details

### 01_download_fia.R

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

### 02_inspect_fia.R

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

### 03_extract_trees.R

**Inputs:** `data/raw/{ST}/{ST}_TREE.csv`, `data/raw/{ST}/{ST}_PLOT.csv`, `data/raw/{ST}/{ST}_COND.csv`, `lookups/ref_species.parquet`

**Outputs:**
- `data/processed/trees/state={ST}/trees_{ST}.parquet` - per-species BA by stratum
- `data/processed/cond/state={ST}/cond_{ST}.parquet` - FORTYPCD + condition attributes

**Processing (per state):**

1. Load TREE, PLOT, COND with `fread(select=required_cols)` for speed
2. Filter TREE: `STATUSCD %in% c(1,2)`, `DIA >= 1.0`, `TPA_UNADJ > 0`, `INVYR` range
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

### 04_extract_seedlings_mortality.R

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
- Aggregated: `treecount_total = sum(TREECOUNT)` by `[PLT_CN, INVYR, SPCD, SFTWD_HRDWD]`
- No per-acre expansion stored (microplot-to-acre conversion can be applied at analysis time)

**TREE_GRM_COMPONENT processing:**
- Filter: `MICR_COMPONENT_AL_FOREST IN ('MORTALITY1','MORTALITY2','CUT1','CUT2')`
- Filter: `MICR_TPAMORT_UNADJ_AL_FOREST > 0` and not NA
- `component_type = "natural"` for MORTALITY1/2, `"harvest"` for CUT1/2
- Join to TREE (slim: CN, SPCD, AGENTCD, INVYR) via `TRE_CN` to get INVYR, SPCD, AGENTCD
  (TREE_GRM_COMPONENT does not carry INVYR directly)
- Filter INVYR range; join REF_SPECIES for SFTWD_HRDWD
- Aggregate: `tpamort_per_acre = sum(MICR_TPAMORT_UNADJ_AL_FOREST)` by `[PLT_CN, INVYR, SPCD, SFTWD_HRDWD, AGENTCD, component_type]`

---

### 05_build_fia_summaries.R

**Inputs:** Per-state partitioned parquets from scripts 03 and 04

**Outputs:** `data/processed/summaries/` (4 national parquet files)

**Processing:**
- Uses `open_dataset(..., partitioning="state")` to read partitioned parquets lazily
- State-loop collect pattern: one state at a time to control memory
- Shannon H computed with custom `compute_shannon_h()` in data.table (Arrow cannot compute `log()` lazily)

**plot_tree_metrics columns:**
- `PLT_CN`, `INVYR`, `state`
- `ba_live_total`, `ba_dead_total` (sum of ba_per_acre by STATUSCD)
- `ba_live_softwood`, `ba_live_hardwood` (by SFTWD_HRDWD)
- `ba_live_sapling`, `ba_live_intermediate`, `ba_live_mature` (by size_class)
- `ba_live_overstory`, `ba_live_understory` (by canopy_layer)
- `n_trees_live`, `n_trees_dead` (sum of n_trees_tpa)
- `n_species_live` (unique SPCD with ba_per_acre > 0)
- `shannon_h_ba` (BA-weighted Shannon H, live trees only)
- `species_temp_optima_mean` (placeholder NA - thermophilization join TBD)

---

## Data Flow

```
FIA DataMart (50 state CSVs)
         |
         v 01_download_fia.R
05_fia/data/raw/{STATE}/*.csv
         |
         v 02_inspect_fia.R
lookups/ref_species.parquet + schema checks
         |
         +------------+------------------+
         |            |                  |
         v            v                  v
03_extract_trees.R  04_extract_...     04_extract_...
trees/{state=ST}/   seedlings/{state=ST}/ mortality/{state=ST}/
cond/{state=ST}/
         |
         v 05_build_fia_summaries.R
summaries/plot_tree_metrics.parquet
summaries/plot_seedling_metrics.parquet
summaries/plot_mortality_metrics.parquet
summaries/plot_cond_fortypcd.parquet
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

---

## Connection to IDS+Climate Workstream

FIA plots and IDS damage areas share geographic space but use different spatial
identifiers. Potential linkage approaches:

1. **County-level join**: Both FIA (STATECD/COUNTYCD) and IDS (REGION_ID) have
   county codes. Most reliable but coarsest spatial resolution.

2. **Coordinate overlay**: FIA coordinates are fuzzed ~1 mile; IDS polygons are
   georeferenced. Overlay is approximate but feasible for large IDS polygons.

3. **AGENTCD - DCA_CODE crosswalk** (planned): FIA TREE.AGENTCD (mortality cause)
   may align with IDS DCA_CODE (damage cause) for bark beetle and other agents.
   A formal crosswalk table would enable linking FIA mortality records to IDS
   disturbance polygons. This is a priority future task.

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
