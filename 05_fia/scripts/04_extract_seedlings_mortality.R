# ==============================================================================
# 04_extract_seedlings_mortality.R
# Extract SEEDLING counts and TREE_GRM_COMPONENT mortality from FIADB
#
# SEEDLINGS:
#   Counted on the microplot (1/300 acre) by species and condition class.
#   Minimum height: conifers >= 6 inches, hardwoods >= 12 inches.
#   TREECOUNT is the raw count of seedlings on the microplot.
#   Aggregated to plot x INVYR x condition x subplot x species level.
#
# MORTALITY (between-measurement):
#   TREE_GRM_COMPONENT records changes between consecutive visits (T1 -> T2).
#   Natural mortality: MICR_COMPONENT_AL_FOREST IN ('MORTALITY1','MORTALITY2')
#   Harvest removals:  MICR_COMPONENT_AL_FOREST IN ('CUT1','CUT2')
#   MICR_TPAMORT_UNADJ_AL_FOREST is the per-acre mortality expansion factor.
#   Joined to TREE via TRE_CN to get SPCD, AGENTCD, and INVYR.
#   Note: TREE_GRM_COMPONENT does not carry INVYR directly.
#
# Usage:
#   Rscript 05_fia/scripts/04_extract_seedlings_mortality.R
#   Rscript 05_fia/scripts/04_extract_seedlings_mortality.R CO WY MT
#   Rscript 05_fia/scripts/04_extract_seedlings_mortality.R --force-seedlings
#
# Output:
#   05_fia/data/processed/seedlings/state={ST}/seedlings_{ST}.parquet
#     Columns: stable_plot_id, PLT_CN, INVYR, STATECD, UNITCD, COUNTYCD, PLOT,
#              CONDID, SUBP, SPCD, species names/groups, treecount_total,
#              treecount_calc_total, seedlings_tpa, n_seedling_records
#   05_fia/data/processed/mortality/state={ST}/mortality_{ST}.parquet
#     Columns: PLT_CN, INVYR, SPCD, SFTWD_HRDWD, AGENTCD, component_type,
#              tpamort_per_acre
# ==============================================================================

source("scripts/utils/load_config.R")
source("scripts/utils/parquet_atomic.R")
source("scripts/utils/fia_seedling.R")
config <- load_config()

library(here)
library(fs)
library(glue)
library(data.table)
library(arrow)
library(tibble)

# ------------------------------------------------------------------------------
# Setup
# ------------------------------------------------------------------------------

# Pull FIA source and destination paths from the shared config.
fia_config   <- config$raw$fia
raw_dir      <- here(fia_config$local_dir)
out_seedling <- here(config$processed$fia$seedlings$output_dir)
out_mort     <- here(config$processed$fia$mortality$output_dir)
lookup_dir   <- here("05_fia/lookups")

# Allow state-specific reruns and seedling-only refreshes for schema upgrades.
args <- commandArgs(trailingOnly = TRUE)
force_seedlings <- "--force-seedlings" %in% args

# Treat command-line flags separately from state abbreviations.
state_args <- toupper(args[!args %in% c("--force-seedlings")])
states <- if (length(state_args) > 0) state_args else fia_config$states

# Use the same modern annual-inventory window as the tree extraction.
invyr_min <- fia_config$invyr_min
invyr_max <- fia_config$invyr_max

# Read mortality component labels from config so the script matches project settings.
natural_codes <- as.character(fia_config$mortality_codes$natural)
harvest_codes <- as.character(fia_config$mortality_codes$harvest)
all_codes     <- c(natural_codes, harvest_codes)

# Echo run settings so logs are self-documenting.
cat("FIA Seedling and Mortality Extraction\n")
cat("======================================\n\n")
cat(glue("Output (seedlings):  {out_seedling}\n"))
cat(glue("Output (mortality):  {out_mort}\n"))
cat(glue("States: {length(states)}\n"))
cat(glue("INVYR range: {invyr_min}-{invyr_max}\n"))
cat(glue("Mortality codes: {paste(all_codes, collapse=', ')}\n"))
if (force_seedlings) cat("Mode: --force-seedlings (seedling parquets will be rebuilt)\n")
cat("\n")

# Load REF_SPECIES once so seedlings and mortality share the same species groups.
ref_sp_path <- file.path(lookup_dir, "ref_species.parquet")
if (!file_exists(ref_sp_path)) {
  stop("ref_species.parquet not found. Run 02_inspect_fia.R first.")
}
# Keep species names and groups so seedling products are analysis-ready.
ref_sp <- as.data.table(read_parquet(ref_sp_path))
ref_sp_cols <- intersect(
  c("SPCD", "COMMON_NAME", "SCIENTIFIC_NAME", "GENUS", "SPECIES",
    "SFTWD_HRDWD", "WOODLAND", "MAJOR_SPGRPCD", "JENKINS_SPGRPCD"),
  names(ref_sp)
)
ref_sp <- ref_sp[, ..ref_sp_cols]
setkey(ref_sp, SPCD)

# Sum optional FIA fields as NA when a source table does not provide them.
sum_or_na <- function(x) {
  if (all(is.na(x))) NA_real_ else sum(x, na.rm = TRUE)
}

# Required columns define seedling identity; optional columns add stable IDs and density.
seedling_cols_required <- c("PLT_CN", "CONDID", "SUBP", "SPCD", "TREECOUNT", "INVYR")
seedling_cols_optional <- c("STATECD", "UNITCD", "COUNTYCD", "PLOT",
                            "TREECOUNT_CALC", "TPA_UNADJ")
seedling_cols <- c(seedling_cols_required, seedling_cols_optional)
grm_cols      <- c("TRE_CN", "PLT_CN", "MICR_COMPONENT_AL_FOREST",
                   "MICR_TPAMORT_UNADJ_AL_FOREST")

# TREE supplies the species, mortality agent, and inventory year for GRM records.
tree_slim_cols <- c("CN", "SPCD", "AGENTCD", "INVYR")

# ------------------------------------------------------------------------------
# Per-state extraction loop
# ------------------------------------------------------------------------------

t_total <- Sys.time()
n_done <- 0; n_skipped <- 0; n_failed <- 0

for (i in seq_along(states)) {
  # Build state-specific paths so interrupted runs can resume one state at a time.
  st           <- states[i]
  state_dir    <- file.path(raw_dir, st)
  seed_out     <- file.path(out_seedling, glue("state={st}/seedlings_{st}.parquet"))
  mort_out     <- file.path(out_mort,     glue("state={st}/mortality_{st}.parquet"))

  # Skip states whose products already exist unless we are refreshing seedlings.
  if (!force_seedlings && file_exists(seed_out) && file_exists(mort_out)) {
    cat(glue("[{i}/{length(states)}] {st}: output exists - skipping\n"))
    n_skipped <- n_skipped + 1
    next
  }

  # Raw FIA CSV filenames follow the state-table naming convention.
  seed_file <- file.path(state_dir, glue("{st}_SEEDLING.csv"))
  grm_file  <- file.path(state_dir, glue("{st}_TREE_GRM_COMPONENT.csv"))
  tree_file <- file.path(state_dir, glue("{st}_TREE.csv"))

  # A state with neither source table cannot contribute to this extraction step.
  if (!file_exists(seed_file) && !file_exists(grm_file)) {
    cat(glue("[{i}/{length(states)}] {st}: SEEDLING and GRM files not found - skipping\n"))
    n_failed <- n_failed + 1
    next
  }

  cat(glue("[{i}/{length(states)}] {st}:\n"))

  # Time each state so long-running reruns show where time is spent.
  t_st <- Sys.time()

  tryCatch({

    # ------ SEEDLINGS ----------------------------------------------------------

    if (force_seedlings || !file_exists(seed_out)) {
      # Seedling extraction can run even if mortality sources are missing.
      if (!file_exists(seed_file)) {
        cat("  SEEDLING.csv not found - skipping seedling extraction\n")
      } else {
        # Inspect the header first so optional fields do not break older state files.
        avail_seed <- names(fread(seed_file, nrows = 0L, showProgress = FALSE))
        missing_seed <- setdiff(seedling_cols_required, avail_seed)

        if (length(missing_seed) > 0) {
          cat(glue("  SEEDLING missing required columns: {paste(missing_seed, collapse=', ')} - skipping\n"))
        } else {
          # Read required fields plus whichever optional density/identity fields exist.
          select_seed <- intersect(seedling_cols, avail_seed)
          seed_dt <- fread(seed_file, select = select_seed, showProgress = FALSE)
          cat(glue("  Loaded: SEEDLING={format(nrow(seed_dt), big.mark=',')}\n"))

          # Add absent optional ID fields as NA so all state parquets share one schema.
          for (id_col in c("STATECD", "UNITCD", "COUNTYCD", "PLOT")) {
            if (!id_col %in% names(seed_dt)) seed_dt[, (id_col) := NA_integer_]
          }

          # Add absent optional count fields as NA so density-aware code can still run.
          for (count_col in c("TREECOUNT_CALC", "TPA_UNADJ")) {
            if (!count_col %in% names(seed_dt)) seed_dt[, (count_col) := NA_real_]
          }

          # Eligibility follows FIA's calculated abundance (TREECOUNT_CALC / valid
          # TPA_UNADJ), not the raw field count TREECOUNT which is null in many
          # states (ME ~21%, OR/CA ~20%). See scripts/utils/fia_seedling.R.
          # TREECOUNT is retained as a descriptive column but its missingness never
          # discards a record FIA counts.
          seed_dt <- seed_dt[
            INVYR >= invyr_min & INVYR <= invyr_max &
              seedling_eligible(TREECOUNT, TREECOUNT_CALC, TPA_UNADJ)
          ]

          # Build the same stable plot id used in condition metadata.
          seed_dt[, stable_plot_id := NA_character_]
          seed_dt[
            !is.na(STATECD) & !is.na(UNITCD) & !is.na(COUNTYCD) & !is.na(PLOT),
            stable_plot_id := paste(STATECD, UNITCD, COUNTYCD, PLOT, sep = "_")
          ]

          # Attach species names and broad functional groups for composition analyses.
          setkey(seed_dt, SPCD)
          seed_dt <- ref_sp[seed_dt, on = "SPCD"]

          # Group at condition/subplot/species grain so downstream joins stay honest.
          species_cols <- intersect(
            c("COMMON_NAME", "SCIENTIFIC_NAME", "GENUS", "SPECIES",
              "SFTWD_HRDWD", "WOODLAND", "MAJOR_SPGRPCD", "JENKINS_SPGRPCD"),
            names(seed_dt)
          )
          seed_group_cols <- c(
            "stable_plot_id", "PLT_CN", "INVYR", "STATECD", "UNITCD", "COUNTYCD", "PLOT",
            "CONDID", "SUBP", "SPCD", species_cols
          )

          # Sum raw counts and expanded density while preserving the sampling grain.
          seed_agg <- seed_dt[, .(
            treecount_total = sum(TREECOUNT, na.rm = TRUE),
            treecount_calc_total = sum_or_na(TREECOUNT_CALC),
            seedlings_tpa = sum_or_na(TPA_UNADJ),
            n_seedling_records = .N
          ), by = seed_group_cols]

          # Write the species-level seedling product used by recruitment composition work.
          dir_create(dirname(seed_out))
          write_parquet_atomic(as_tibble(seed_agg), seed_out, compression = "snappy")
          cat(glue("  Seedlings: {format(nrow(seed_agg), big.mark=',')} rows -> {file_size(seed_out)}\n"))
          rm(seed_dt, seed_agg)
        }
      }
    } else {
      cat("  Seedlings: output already exists\n")
    }

    # ------ MORTALITY (TREE_GRM_COMPONENT) ------------------------------------

    if (!file_exists(mort_out)) {
      # Mortality requires GRM plus TREE because GRM lacks species and inventory year.
      if (!file_exists(grm_file)) {
        cat("  TREE_GRM_COMPONENT.csv not found - skipping mortality extraction\n")
      } else if (!file_exists(tree_file)) {
        cat("  TREE.csv not found - cannot join GRM to get INVYR - skipping mortality\n")
      } else {
        # Read growth/removal/mortality records plus a slim TREE table for species and year.
        grm_dt  <- fread(grm_file,  select = grm_cols,       showProgress = FALSE)
        tree_slim <- fread(tree_file, select = tree_slim_cols, showProgress = FALSE)
        cat(glue("  Loaded: GRM={format(nrow(grm_dt), big.mark=',')}, ",
                 "TREE_slim={format(nrow(tree_slim), big.mark=',')}\n"))

        # Keep only natural mortality and harvest removal components with expansion factors.
        grm_dt <- grm_dt[
          MICR_COMPONENT_AL_FOREST %in% all_codes &
          !is.na(MICR_TPAMORT_UNADJ_AL_FOREST) &
          MICR_TPAMORT_UNADJ_AL_FOREST > 0
        ]

        # Collapse detailed component names into analysis-ready natural/harvest classes.
        grm_dt[, component_type := fifelse(
          MICR_COMPONENT_AL_FOREST %in% natural_codes, "natural", "harvest"
        )]

        # Join TREE by tree control number because GRM does not carry species/year directly.
        setnames(tree_slim, "CN", "TRE_CN")
        setkey(tree_slim, TRE_CN)
        setkey(grm_dt,    TRE_CN)
        mort_dt <- tree_slim[grm_dt, on = "TRE_CN", nomatch = 0]
        cat(glue("  After GRM-TREE join: {format(nrow(mort_dt), big.mark=',')} records\n"))

        # Restrict joined mortality records to the same inventory window as the other outputs.
        mort_dt <- mort_dt[INVYR >= invyr_min & INVYR <= invyr_max]

        # Attach softwood/hardwood group for mortality summaries.
        setkey(mort_dt, SPCD)
        mort_dt <- ref_sp[mort_dt, on = "SPCD"]

        # Aggregate mortality by plot, year, species, mortality agent, and component type.
        setnames(mort_dt, "MICR_TPAMORT_UNADJ_AL_FOREST", "tpamort_raw")
        mort_agg <- mort_dt[, .(
          tpamort_per_acre = sum(tpamort_raw, na.rm = TRUE)
        ), by = .(PLT_CN, INVYR, SPCD, SFTWD_HRDWD, AGENTCD, component_type)]

        # Write the mortality product after aggregation to keep national reads small.
        dir_create(dirname(mort_out))
        write_parquet_atomic(as_tibble(mort_agg), mort_out, compression = "snappy")
        cat(glue("  Mortality: {format(nrow(mort_agg), big.mark=',')} rows -> {file_size(mort_out)}\n"))
        rm(grm_dt, tree_slim, mort_dt, mort_agg)
      }
    } else {
      cat("  Mortality: output already exists\n")
    }

    # Record elapsed time for this state's seedling/mortality work.
    elapsed <- as.numeric(difftime(Sys.time(), t_st, units = "secs"))
    cat(glue("  Time: {sprintf('%.1fs', elapsed)}\n"))
    n_done <- n_done + 1

  }, error = function(e) {
    # Keep processing other states if one state has an unexpected schema or read error.
    warning(glue("  Error processing {st}: {e$message}"))
    n_failed <- n_failed + 1
  })

  # Encourage memory release between states because some FIA tables are large.
  gc(verbose = FALSE)
}

# ------------------------------------------------------------------------------
# Summary
# ------------------------------------------------------------------------------

elapsed_total <- as.numeric(difftime(Sys.time(), t_total, units = "mins"))

# Print a compact batch summary for logs and reproducibility notes.
cat(glue("\n{strrep('=', 50)}\n"))
cat("Seedling and mortality extraction complete.\n\n")
cat(glue("  Processed: {n_done} state(s)\n"))
cat(glue("  Skipped:   {n_skipped} state(s) (output already exists)\n"))
cat(glue("  Failed:    {n_failed} state(s)\n"))
cat(glue("  Time:      {sprintf('%.1f', elapsed_total)} min\n\n"))

cat("Next step: Rscript 05_fia/scripts/05_build_fia_summaries.R\n")
