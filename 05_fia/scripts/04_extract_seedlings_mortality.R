# ==============================================================================
# 04_extract_seedlings_mortality.R
# Extract SEEDLING counts and TREE_GRM_COMPONENT mortality from FIADB
#
# SEEDLINGS:
#   Counted on the microplot (1/300 acre) by species and condition class.
#   Minimum height: conifers >= 6 inches, hardwoods >= 12 inches.
#   TREECOUNT is the raw count of seedlings on the microplot.
#   Aggregated to plot x INVYR x species level.
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
#
# Output:
#   05_fia/data/processed/seedlings/state={ST}/seedlings_{ST}.parquet
#     Columns: PLT_CN, INVYR, SPCD, SFTWD_HRDWD, treecount_total
#   05_fia/data/processed/mortality/state={ST}/mortality_{ST}.parquet
#     Columns: PLT_CN, INVYR, SPCD, SFTWD_HRDWD, AGENTCD, component_type,
#              tpamort_per_acre
# ==============================================================================

source("scripts/utils/load_config.R")
config <- load_config()

library(here)
library(fs)
library(glue)
library(data.table)
library(arrow)

# ------------------------------------------------------------------------------
# Setup
# ------------------------------------------------------------------------------

fia_config   <- config$raw$fia
raw_dir      <- here(fia_config$local_dir)
out_seedling <- here(config$processed$fia$seedlings$output_dir)
out_mort     <- here(config$processed$fia$mortality$output_dir)
lookup_dir   <- here("05_fia/lookups")

args   <- commandArgs(trailingOnly = TRUE)
states <- if (length(args) > 0) toupper(args) else fia_config$states

invyr_min <- fia_config$invyr_min
invyr_max <- fia_config$invyr_max

# Mortality component codes from config
natural_codes <- as.character(fia_config$mortality_codes$natural)
harvest_codes <- as.character(fia_config$mortality_codes$harvest)
all_codes     <- c(natural_codes, harvest_codes)

cat("FIA Seedling and Mortality Extraction\n")
cat("======================================\n\n")
cat(glue("Output (seedlings):  {out_seedling}\n"))
cat(glue("Output (mortality):  {out_mort}\n"))
cat(glue("States: {length(states)}\n"))
cat(glue("INVYR range: {invyr_min}-{invyr_max}\n"))
cat(glue("Mortality codes: {paste(all_codes, collapse=', ')}\n\n"))

# Load REF_SPECIES once
ref_sp_path <- file.path(lookup_dir, "ref_species.parquet")
if (!file_exists(ref_sp_path)) {
  stop("ref_species.parquet not found. Run 02_inspect_fia.R first.")
}
ref_sp <- as.data.table(read_parquet(ref_sp_path))
ref_sp <- ref_sp[, .(SPCD, SFTWD_HRDWD)]
setkey(ref_sp, SPCD)

seedling_cols <- c("PLT_CN", "CONDID", "SUBP", "SPCD", "TREECOUNT", "INVYR")
grm_cols      <- c("TRE_CN", "PLT_CN", "MICR_COMPONENT_AL_FOREST",
                   "MICR_TPAMORT_UNADJ_AL_FOREST")
tree_slim_cols <- c("CN", "SPCD", "AGENTCD", "INVYR")

# ------------------------------------------------------------------------------
# Per-state extraction loop
# ------------------------------------------------------------------------------

t_total <- Sys.time()
n_done <- 0; n_skipped <- 0; n_failed <- 0

for (i in seq_along(states)) {
  st           <- states[i]
  state_dir    <- file.path(raw_dir, st)
  seed_out     <- file.path(out_seedling, glue("state={st}/seedlings_{st}.parquet"))
  mort_out     <- file.path(out_mort,     glue("state={st}/mortality_{st}.parquet"))

  if (file_exists(seed_out) && file_exists(mort_out)) {
    cat(glue("[{i}/{length(states)}] {st}: output exists - skipping\n"))
    n_skipped <- n_skipped + 1
    next
  }

  seed_file <- file.path(state_dir, glue("{st}_SEEDLING.csv"))
  grm_file  <- file.path(state_dir, glue("{st}_TREE_GRM_COMPONENT.csv"))
  tree_file <- file.path(state_dir, glue("{st}_TREE.csv"))

  if (!file_exists(seed_file) && !file_exists(grm_file)) {
    cat(glue("[{i}/{length(states)}] {st}: SEEDLING and GRM files not found - skipping\n"))
    n_failed <- n_failed + 1
    next
  }

  cat(glue("[{i}/{length(states)}] {st}:\n"))
  t_st <- Sys.time()

  tryCatch({

    # ------ SEEDLINGS ----------------------------------------------------------

    if (!file_exists(seed_out)) {
      if (!file_exists(seed_file)) {
        cat("  SEEDLING.csv not found - skipping seedling extraction\n")
      } else {
        seed_dt <- fread(seed_file, select = seedling_cols, showProgress = FALSE)
        cat(glue("  Loaded: SEEDLING={format(nrow(seed_dt), big.mark=',')}\n"))

        seed_dt <- seed_dt[
          INVYR >= invyr_min & INVYR <= invyr_max &
          !is.na(TREECOUNT) & TREECOUNT > 0
        ]

        # Join species lookup
        setkey(seed_dt, SPCD)
        seed_dt <- ref_sp[seed_dt, on = "SPCD"]

        # Aggregate: plot x INVYR x species (sum TREECOUNT across subplots/conditions)
        seed_agg <- seed_dt[, .(
          treecount_total = sum(TREECOUNT, na.rm = TRUE)
        ), by = .(PLT_CN, INVYR, SPCD, SFTWD_HRDWD)]

        dir_create(dirname(seed_out))
        write_parquet(as_tibble(seed_agg), seed_out, compression = "snappy")
        cat(glue("  Seedlings: {format(nrow(seed_agg), big.mark=',')} rows -> {file_size(seed_out)}\n"))
        rm(seed_dt, seed_agg)
      }
    } else {
      cat("  Seedlings: output already exists\n")
    }

    # ------ MORTALITY (TREE_GRM_COMPONENT) ------------------------------------

    if (!file_exists(mort_out)) {
      if (!file_exists(grm_file)) {
        cat("  TREE_GRM_COMPONENT.csv not found - skipping mortality extraction\n")
      } else if (!file_exists(tree_file)) {
        cat("  TREE.csv not found - cannot join GRM to get INVYR - skipping mortality\n")
      } else {
        grm_dt  <- fread(grm_file,  select = grm_cols,       showProgress = FALSE)
        tree_slim <- fread(tree_file, select = tree_slim_cols, showProgress = FALSE)
        cat(glue("  Loaded: GRM={format(nrow(grm_dt), big.mark=',')}, ",
                 "TREE_slim={format(nrow(tree_slim), big.mark=',')}\n"))

        # Filter to mortality/harvest component codes
        grm_dt <- grm_dt[
          MICR_COMPONENT_AL_FOREST %in% all_codes &
          !is.na(MICR_TPAMORT_UNADJ_AL_FOREST) &
          MICR_TPAMORT_UNADJ_AL_FOREST > 0
        ]

        # Label component type
        grm_dt[, component_type := fifelse(
          MICR_COMPONENT_AL_FOREST %in% natural_codes, "natural", "harvest"
        )]

        # Join TREE (slim) to get SPCD, AGENTCD, INVYR via TRE_CN
        setnames(tree_slim, "CN", "TRE_CN")
        setkey(tree_slim, TRE_CN)
        setkey(grm_dt,    TRE_CN)
        mort_dt <- tree_slim[grm_dt, on = "TRE_CN", nomatch = 0]
        cat(glue("  After GRM-TREE join: {format(nrow(mort_dt), big.mark=',')} records\n"))

        # Filter INVYR range
        mort_dt <- mort_dt[INVYR >= invyr_min & INVYR <= invyr_max]

        # Join species lookup
        setkey(mort_dt, SPCD)
        mort_dt <- ref_sp[mort_dt, on = "SPCD"]

        # Aggregate by plot x INVYR x species x agent x component
        setnames(mort_dt, "MICR_TPAMORT_UNADJ_AL_FOREST", "tpamort_raw")
        mort_agg <- mort_dt[, .(
          tpamort_per_acre = sum(tpamort_raw, na.rm = TRUE)
        ), by = .(PLT_CN, INVYR, SPCD, SFTWD_HRDWD, AGENTCD, component_type)]

        dir_create(dirname(mort_out))
        write_parquet(as_tibble(mort_agg), mort_out, compression = "snappy")
        cat(glue("  Mortality: {format(nrow(mort_agg), big.mark=',')} rows -> {file_size(mort_out)}\n"))
        rm(grm_dt, tree_slim, mort_dt, mort_agg)
      }
    } else {
      cat("  Mortality: output already exists\n")
    }

    elapsed <- as.numeric(difftime(Sys.time(), t_st, units = "secs"))
    cat(glue("  Time: {sprintf('%.1fs', elapsed)}\n"))
    n_done <- n_done + 1

  }, error = function(e) {
    warning(glue("  Error processing {st}: {e$message}"))
    n_failed <- n_failed + 1
  })

  gc(verbose = FALSE)
}

# ------------------------------------------------------------------------------
# Summary
# ------------------------------------------------------------------------------

elapsed_total <- as.numeric(difftime(Sys.time(), t_total, units = "mins"))

cat(glue("\n{'='*50}\n"))
cat("Seedling and mortality extraction complete.\n\n")
cat(glue("  Processed: {n_done} state(s)\n"))
cat(glue("  Skipped:   {n_skipped} state(s) (output already exists)\n"))
cat(glue("  Failed:    {n_failed} state(s)\n"))
cat(glue("  Time:      {sprintf('%.1f', elapsed_total)} min\n\n"))

cat("Next step: Rscript 05_fia/scripts/05_build_fia_summaries.R\n")
