# FIA — Forest Inventory and Analysis

**Source:** USDA Forest Service, Forest Inventory and Analysis National Program
**URL:** https://apps.fs.usda.gov/fia/datamart/CSV/
**Database version:** FIADB v9.4 (August 2025)
**User Guide:** `wo-v9-4_Aug2025_UG_FIADB_database_description_NFI.pdf` (repo root)

FIA establishes and remeasures permanent plots (~1 acre each) across all US forest land on a rotating cycle. Plot-level tree measurements include diameter, crown class, condition, and species. Regeneration and mortality between visits are also recorded. The annual panel design enables longitudinal analysis of forest change.

| | |
|---|---|
| **Format** | CSV (downloaded via rFIA), processed to parquet |
| **Spatial coverage** | All 50 US states |
| **Inventory years** | 2000–2024 |
| **Coordinates** | Fuzzed ~1 mile for privacy; state and county codes are exact |

**Research objective:** Compile a species-resolved, longitudinal dataset of forest structure and diversity for thermophilization analysis — detecting shifts toward warmer-adapted species over time.

---

## Directory Structure

```
05_fia/
├── README.md               ← This file: overview and quick-start
├── WORKFLOW.md             ← Technical reference: script details, data flow, usage examples
├── scripts/
│   ├── 01_download_fia.R
│   ├── 02_inspect_fia.R
│   ├── 03_extract_trees.R
│   ├── 04_extract_seedlings_mortality.R
│   ├── 05_build_fia_summaries.R
│   ├── 06_extract_site_climate.R
│   └── qc/
│       └── validate_fia.R
├── lookups/                ← git-tracked reference parquets
│   ├── ref_species.parquet
│   └── ref_forest_type.parquet
├── docs/
│   └── FIADB_field_reference.md
└── data/
    ├── raw/            ← git-tracked; state CSV tables downloaded by 01_download_fia.R
    │   └── REF/        ← REF_SPECIES.csv, REF_FOREST_TYPE.csv
    └── processed/
        ├── trees/          ← gitignored; per-state parquets (partitioned by state)
        ├── cond/           ← gitignored; condition + forest type (per-state)
        ├── seedlings/      ← gitignored; per-state seedling counts
        ├── mortality/      ← gitignored; per-state GRM mortality records
        ├── summaries/      ← git-tracked; national plot-level summaries (8 parquets)
        └── site_climate/   ← git-tracked; TerraClimate at FIA site locations
```

---

## Quick Start

Run scripts in order from the repo root:

```bash
Rscript 05_fia/scripts/01_download_fia.R              # Download CSVs for all 50 states
Rscript 05_fia/scripts/02_inspect_fia.R               # Verify schema, generate lookup parquets
Rscript 05_fia/scripts/03_extract_trees.R             # Extract tree/BA/condition records
Rscript 05_fia/scripts/04_extract_seedlings_mortality.R  # Extract seedlings + GRM mortality
Rscript 05_fia/scripts/05_build_fia_summaries.R       # Aggregate to national plot-level summaries
Rscript 05_fia/scripts/06_extract_site_climate.R      # TerraClimate at all FIA sites via GEE (optional — requires GEE account)
```

**Prerequisite:** Install `rFIA` before step 1: `renv::install("rFIA"); renv::snapshot()`.

Step 6 requires a GEE account and produces the site-level climate data. All other steps are independent of GEE.

---

## Key Outputs

### Plot-Level Summaries (`data/processed/summaries/`) — git-tracked

| File | Description |
|------|-------------|
| `plot_tree_metrics.parquet` | BA totals, species richness, Shannon diversity by stratum (live/dead, softwood/hardwood, size class, canopy layer) |
| `plot_seedling_metrics.parquet` | Seedling counts per species per plot per inventory year |
| `plot_mortality_metrics.parquet` | Between-measurement mortality (TPA/acre) by agent and species |
| `plot_disturbance_history.parquet` | Condition-level disturbance events (DSTRBCD1/2/3) with labels and categories |
| `plot_damage_agents.parquet` | Live-tree damage agent codes (DAMAGE_AGENT_CD) with labels |
| `plot_treatment_history.parquet` | Silvicultural treatments (TRTCD1/2/3) with labels and categories |
| `plot_cond_fortypcd.parquet` | Condition-level forest type codes |
| `plot_exclusion_flags.parquet` | Per-plot flags for filtering non-forested, disturbed, and harvested plots |

### Site Climate (`data/processed/site_climate/`) — git-tracked

| File | Description |
|------|-------------|
| `site_pixel_map.parquet` | FIA site → TerraClimate pixel mapping |
| `site_climate.parquet` | Monthly TerraClimate (1958–present) for all FIA plot locations. 6 variables: `tmmx`, `tmmn`, `pr`, `def`, `pet`, `aet` |

### Lookup Tables (`lookups/`) — git-tracked

| File | Description |
|------|-------------|
| `ref_species.parquet` | SPCD → common name, genus, species, SFTWD_HRDWD, WOODLAND |
| `ref_forest_type.parquet` | FORTYPCD → forest type name |

---

## Key Metrics Defined

| Metric | Definition |
|--------|------------|
| Basal area per tree (sq ft) | `0.005454 * DIA²` |
| Per-acre BA | `TPA_UNADJ × ba_sqft_tree` |
| Shannon diversity (H) | `−Σ(pᵢ × log(pᵢ))` where pᵢ = species BA / total BA (live trees only) |
| Size: sapling | DIA 1.0–4.9 in |
| Size: intermediate | DIA 5.0–11.9 in |
| Size: mature | DIA ≥ 12.0 in |
| Canopy: overstory | CCLCD 1–3 (open grown, dominant, codominant) |
| Canopy: understory | CCLCD 4–5 (intermediate, overtopped) |
