================================================================================
USDA Forest Service Forest Inventory and Analysis (FIADB)
================================================================================
Source:   https://apps.fs.usda.gov/fia/datamart/CSV/
Citation: Forest Inventory and Analysis National Program, USDA Forest Service
Format:   CSV (downloaded via rFIA package), processed to parquet
Coverage: All 50 US states, inventory years 2000-2024
User Guide: wo-v9-4_Aug2025_UG_FIADB_database_description_NFI.pdf (see repo root)

Description
-----------
FIA establishes and remeasures permanent plots (~1 acre each) across all US
forest land on a rotating cycle (roughly 1/5 of plots per year). Plot-level
tree measurements include diameter, crown class, condition, and species.
Regeneration (seedlings) and growth/removal/mortality between visits are also
recorded. The annual panel design means each plot has multiple visits over time
(tracked via INVYR), enabling longitudinal analysis of forest change.

Coordinates are "fuzzed and swapped" up to ~1 mile to protect landowner privacy.
State and county codes are exact.

Research objective
------------------
Compile a species-resolved, longitudinal dataset of forest structure and
diversity for thermophilization analysis: detecting shifts toward warmer-adapted
species and transitions to different vegetation types over time.

Scripts (run in order)
----------------------
01_download_fia.R               Download table CSVs for all 50 states via rFIA.
                                Also downloads national REF_SPECIES and
                                REF_FOREST_TYPE to data/raw/REF/.

02_inspect_fia.R                Verify required columns are present across
                                sampled states. Generates lookup parquets in
                                lookups/ from the national REF tables.

03_extract_trees.R              Extract TREE records, compute basal area, assign
                                size class and canopy layer. One parquet per state
                                to data/processed/trees/. Also extracts COND
                                (forest type + condition attributes) to
                                data/processed/cond/.

04_extract_seedlings_mortality.R  Extract SEEDLING counts and TREE_GRM_COMPONENT
                                  mortality events. One parquet per state to
                                  data/processed/seedlings/ and
                                  data/processed/mortality/.

05_build_fia_summaries.R        Aggregate all per-state parquets to plot-level
                                metrics: BA totals by stratum, Shannon diversity
                                index, species richness. Outputs to
                                data/processed/summaries/.

QC scripts (optional)
---------------------
qc/validate_fia.R               Spot-checks on processed outputs.

Key metrics
-----------
Basal area per tree (sq ft): BA = 0.005454 * DIA^2
Per-acre BA:                  TPA_UNADJ * BA  (TPA_UNADJ is on each TREE record)

Size classes (TREE.DIA in inches):
  sapling:       1.0 - 4.9
  intermediate:  5.0 - 11.9
  mature:       >= 12.0

Canopy layer (TREE.CCLCD):
  overstory:    CCLCD 1 (open grown), 2 (dominant), 3 (codominant)
  understory:   CCLCD 4 (intermediate), 5 (overtopped)
  fallback if CCLCD is NA: DIA >= 5.0 = overstory, < 5.0 = understory

Shannon diversity index (H): BA-weighted per species (live trees only)
  H = -sum(p_i * log(p_i)) where p_i = species_i BA / total BA

Key outputs
-----------
data/processed/trees/                Per-state tree records (partitioned by state)
  Columns: PLT_CN, INVYR, SPCD, SFTWD_HRDWD, WOODLAND, STATUSCD,
           size_class, canopy_layer, ba_sqft, ba_per_acre,
           n_trees_tpa, n_trees_raw

data/processed/seedlings/            Per-state seedling counts (partitioned by state)
  Columns: PLT_CN, INVYR, SPCD, SFTWD_HRDWD, treecount_total

data/processed/mortality/            Between-measurement mortality (partitioned by state)
  Columns: PLT_CN, INVYR, SPCD, SFTWD_HRDWD, AGENTCD, component_type,
           tpamort_per_acre

data/processed/summaries/            National plot-level aggregated metrics
  plot_tree_metrics.parquet
  plot_seedling_metrics.parquet
  plot_mortality_metrics.parquet
  plot_cond_fortypcd.parquet

lookups/                             Reference tables (small, fast)
  ref_species.parquet
  ref_forest_type.parquet

Read national data
------------------
library(arrow)
library(dplyr)

# All states at once
trees_ds <- open_dataset("05_fia/data/processed/trees", partitioning = "state")

# Filter to specific state
co_trees <- trees_ds |> filter(state == "CO") |> collect()

# Final plot metrics
metrics <- read_parquet("05_fia/data/processed/summaries/plot_tree_metrics.parquet")

# Species lookup
ref_sp <- read_parquet("05_fia/lookups/ref_species.parquet")

Join to IDS data
----------------
FIA plots have fuzzed coordinates (LAT, LON in PLOT table). STATECD and
COUNTYCD are exact and can be used for county-level joins. Direct spatial
overlay with IDS polygons is approximate (~1 mile uncertainty). See
WORKFLOW.md for notes on this.

Note: TREE.AGENTCD (mortality cause code) may align with IDS DCA_CODE for
bark beetle and other disturbance types. A crosswalk between these code
systems is a planned future task.

Prerequisites
-------------
R packages: rFIA (must install before 01_download_fia.R)
  renv::install("rFIA")
  renv::snapshot()

All other packages (data.table, arrow, fs, glue, here) are already in renv.lock.

================================================================================
