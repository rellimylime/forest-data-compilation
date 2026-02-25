# ==============================================================================
# 03_extract_trees.R
# Extract TREE + COND records from FIADB and compute basal area metrics
#
# For each state: joins TREE to REF_SPECIES, assigns size class and canopy
# layer, and aggregates to per-plot x INVYR x species x stratum level.
# Saves hive-partitioned parquet (one per state) for each output table.
#
# Basal area:
#   BA per tree (sq ft) = 0.005454 * DIA^2
#   Per-acre BA = TPA_UNADJ * BA  (TPA_UNADJ is the expansion factor on TREE)
#
# Size classes (DIA in inches, from TREE):
#   sapling:      1.0 - 4.9
#   intermediate: 5.0 - 11.9
#   mature:       >= 12.0
#
# Canopy layer (TREE.CCLCD):
#   overstory:  CCLCD 1 (open grown), 2 (dominant), 3 (codominant)
#   understory: CCLCD 4 (intermediate), 5 (overtopped)
#   fallback if CCLCD is NA: DIA >= 5.0 = overstory, < 5.0 = understory
#
# Usage:
#   Rscript 05_fia/scripts/03_extract_trees.R
#   Rscript 05_fia/scripts/03_extract_trees.R CO WY MT   # specific states
#
# Output:
#   05_fia/data/processed/trees/state={ST}/trees_{ST}.parquet
#     Columns: PLT_CN, INVYR, SPCD, SFTWD_HRDWD, STATUSCD, size_class,
#              canopy_layer, ba_sqft, ba_per_acre, n_trees_tpa, n_trees_raw
#   05_fia/data/processed/cond/state={ST}/cond_{ST}.parquet
#     Columns: PLT_CN, INVYR, STATECD, CONDID, FORTYPCD, COND_STATUS_CD,
#              CONDPROP_UNADJ
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

fia_config <- config$raw$fia
raw_dir    <- here(fia_config$local_dir)
out_trees  <- here(config$processed$fia$trees$output_dir)
out_cond   <- here(config$processed$fia$cond$output_dir)
lookup_dir <- here("05_fia/lookups")

args   <- commandArgs(trailingOnly = TRUE)
states <- if (length(args) > 0) toupper(args) else fia_config$states

# Filters from config
statuscd_include  <- as.integer(fia_config$tree_filters$statuscd_include)
dia_min           <- fia_config$tree_filters$dia_min_inches
invyr_min         <- fia_config$invyr_min
invyr_max         <- fia_config$invyr_max
overstory_codes   <- as.integer(fia_config$canopy_layers$overstory_codes)
understory_codes  <- as.integer(fia_config$canopy_layers$understory_codes)
fallback_dia      <- fia_config$canopy_layers$fallback_dia_threshold

cat("FIA Tree Extraction\n")
cat("===================\n\n")
cat(glue("Output (trees): {out_trees}\n"))
cat(glue("Output (cond):  {out_cond}\n"))
cat(glue("States: {length(states)}\n"))
cat(glue("INVYR range: {invyr_min}-{invyr_max}\n\n"))

# Required columns
tree_cols <- c("CN", "PLT_CN", "CONDID", "SUBP", "SPCD", "STATUSCD",
               "DIA", "TPA_UNADJ", "CCLCD", "AGENTCD", "INVYR")
plot_cols <- c("CN", "STATECD", "UNITCD", "COUNTYCD", "PLOT", "INVYR",
               "LAT", "LON", "ELEV")
cond_cols <- c("PLT_CN", "CONDID", "FORTYPCD", "COND_STATUS_CD",
               "CONDPROP_UNADJ", "INVYR")

# Load REF_SPECIES once (small, national)
ref_sp_path <- file.path(lookup_dir, "ref_species.parquet")
if (!file_exists(ref_sp_path)) {
  stop("ref_species.parquet not found. Run 02_inspect_fia.R first.")
}
ref_sp <- as.data.table(read_parquet(ref_sp_path))
ref_sp <- ref_sp[, .(SPCD, SFTWD_HRDWD, WOODLAND)]
setkey(ref_sp, SPCD)

cat(glue("Loaded REF_SPECIES: {nrow(ref_sp)} species\n\n"))

# ------------------------------------------------------------------------------
# Per-state extraction loop
# ------------------------------------------------------------------------------

t_total <- Sys.time()
n_done <- 0; n_skipped <- 0; n_failed <- 0

for (i in seq_along(states)) {
  st         <- states[i]
  state_dir  <- file.path(raw_dir, st)
  trees_out  <- file.path(out_trees, glue("state={st}/trees_{st}.parquet"))
  cond_out   <- file.path(out_cond,  glue("state={st}/cond_{st}.parquet"))

  if (file_exists(trees_out) && file_exists(cond_out)) {
    cat(glue("[{i}/{length(states)}] {st}: output exists - skipping\n"))
    n_skipped <- n_skipped + 1
    next
  }

  tree_file <- file.path(state_dir, glue("{st}_TREE.csv"))
  plot_file <- file.path(state_dir, glue("{st}_PLOT.csv"))
  cond_file <- file.path(state_dir, glue("{st}_COND.csv"))

  if (!file_exists(tree_file)) {
    cat(glue("[{i}/{length(states)}] {st}: TREE.csv not found - skipping\n"))
    n_failed <- n_failed + 1
    next
  }

  cat(glue("[{i}/{length(states)}] {st}:\n"))
  t_st <- Sys.time()

  tryCatch({

    # ------ Load tables -------------------------------------------------------
    tree_dt <- fread(tree_file, select = tree_cols, showProgress = FALSE)
    plot_dt <- fread(plot_file, select = plot_cols, showProgress = FALSE)
    cond_dt <- fread(cond_file, select = cond_cols, showProgress = FALSE)
    cat(glue("  Loaded: TREE={format(nrow(tree_dt), big.mark=',')}, ",
             "PLOT={format(nrow(plot_dt), big.mark=',')}, ",
             "COND={format(nrow(cond_dt), big.mark=',')}\n"))

    # ------ Filter trees ------------------------------------------------------
    tree_dt <- tree_dt[
      STATUSCD %in% statuscd_include &
      !is.na(DIA) & DIA >= dia_min &
      !is.na(TPA_UNADJ) & TPA_UNADJ > 0 &
      INVYR >= invyr_min & INVYR <= invyr_max
    ]
    cat(glue("  After filters: {format(nrow(tree_dt), big.mark=',')} trees\n"))

    if (nrow(tree_dt) == 0) {
      cat("  No trees after filtering - skipping state\n")
      n_failed <- n_failed + 1
      next
    }

    # ------ Derived fields ----------------------------------------------------
    # Basal area per tree (sq ft)
    tree_dt[, ba_sqft_tree := 0.005454 * DIA^2]

    # Size class (from DIA)
    tree_dt[, size_class := fcase(
      DIA <  5.0, "sapling",
      DIA < 12.0, "intermediate",
      DIA >= 12.0, "mature",
      default = NA_character_
    )]

    # Canopy layer (CCLCD with DIA fallback for NAs)
    tree_dt[, canopy_layer := fcase(
      !is.na(CCLCD) & CCLCD %in% overstory_codes,  "overstory",
      !is.na(CCLCD) & CCLCD %in% understory_codes, "understory",
      is.na(CCLCD)  & DIA >= fallback_dia,          "overstory",
      is.na(CCLCD)  & DIA <  fallback_dia,          "understory",
      default = NA_character_
    )]

    # Flag trees where CCLCD was missing (for QC)
    cclcd_missing_n <- tree_dt[is.na(CCLCD), .N]
    if (cclcd_missing_n > 0) {
      cat(glue("  Note: {format(cclcd_missing_n, big.mark=',')} trees had NA CCLCD ",
               "(DIA fallback applied)\n"))
    }

    # ------ Join species lookup -----------------------------------------------
    setkey(tree_dt, SPCD)
    tree_dt <- ref_sp[tree_dt, on = "SPCD"]

    # ------ Aggregate to plot x INVYR x species x stratum -------------------
    group_cols <- c("PLT_CN", "INVYR", "SPCD", "SFTWD_HRDWD", "WOODLAND",
                    "STATUSCD", "size_class", "canopy_layer")

    trees_agg <- tree_dt[, .(
      ba_sqft     = sum(ba_sqft_tree,            na.rm = TRUE),
      ba_per_acre = sum(TPA_UNADJ * ba_sqft_tree, na.rm = TRUE),
      n_trees_tpa = sum(TPA_UNADJ,               na.rm = TRUE),
      n_trees_raw = .N
    ), by = group_cols]

    # ------ Save trees parquet ------------------------------------------------
    dir_create(dirname(trees_out))
    write_parquet(as_tibble(trees_agg), trees_out, compression = "snappy")
    cat(glue("  Trees: {format(nrow(trees_agg), big.mark=',')} rows -> {file_size(trees_out)}\n"))

    # ------ COND: filter and add STATECD from PLOT ---------------------------
    cond_filt <- cond_dt[INVYR >= invyr_min & INVYR <= invyr_max]

    # Add STATECD from PLOT table
    setkey(plot_dt, CN)
    statecd_map <- plot_dt[, .(CN, STATECD)]
    setnames(statecd_map, "CN", "PLT_CN")
    setkey(cond_filt, PLT_CN)
    setkey(statecd_map, PLT_CN)
    cond_filt <- statecd_map[cond_filt, on = "PLT_CN"]

    dir_create(dirname(cond_out))
    write_parquet(as_tibble(cond_filt), cond_out, compression = "snappy")
    cat(glue("  Cond:  {format(nrow(cond_filt), big.mark=',')} rows -> {file_size(cond_out)}\n"))

    elapsed <- as.numeric(difftime(Sys.time(), t_st, units = "secs"))
    cat(glue("  Time: {sprintf('%.1fs', elapsed)}\n"))
    n_done <- n_done + 1

  }, error = function(e) {
    warning(glue("  Error processing {st}: {e$message}"))
    n_failed <- n_failed + 1
  })

  rm(tree_dt, plot_dt, cond_dt, trees_agg, cond_filt)
  gc(verbose = FALSE)
}

# ------------------------------------------------------------------------------
# Summary
# ------------------------------------------------------------------------------

elapsed_total <- as.numeric(difftime(Sys.time(), t_total, units = "mins"))

cat(glue("\n{'='*50}\n"))
cat("Tree extraction complete.\n\n")
cat(glue("  Processed: {n_done} state(s)\n"))
cat(glue("  Skipped:   {n_skipped} state(s) (output already exists)\n"))
cat(glue("  Failed:    {n_failed} state(s)\n"))
cat(glue("  Time:      {sprintf('%.1f', elapsed_total)} min\n\n"))

cat("Next step: Rscript 05_fia/scripts/04_extract_seedlings_mortality.R\n")
