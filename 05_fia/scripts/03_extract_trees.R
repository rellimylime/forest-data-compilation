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
#   Rscript 05_fia/scripts/03_extract_trees.R CO WY MT        # specific states
#   Rscript 05_fia/scripts/03_extract_trees.R --force-cond    # re-extract cond only (all states)
#   Rscript 05_fia/scripts/03_extract_trees.R CO --force-cond # re-extract cond only (CO)
#
# --force-cond: skips tree/damage_agent parquets that already exist, but forces
#   re-extraction of the cond parquet.  Use this when cond_cols has been updated
#   (e.g. to add TRTCD1-3) without wanting to redo the slow tree aggregation.
#
# Output:
#   05_fia/data/processed/trees/state={ST}/trees_{ST}.parquet
#     Columns: PLT_CN, INVYR, SPCD, SFTWD_HRDWD, STATUSCD, size_class,
#              canopy_layer, ba_sqft, ba_per_acre, n_trees_tpa, n_trees_raw
#   05_fia/data/processed/cond/state={ST}/cond_{ST}.parquet
#     Columns: PLT_CN, INVYR, STATECD, CONDID, FORTYPCD, COND_STATUS_CD,
#              CONDPROP_UNADJ, LAT, LON, DSTRBCD1-3, DSTRBYR1-3,
#              TRTCD1-3, TRTYR1-3
#   05_fia/data/processed/damage_agents/state={ST}/damage_agents_{ST}.parquet
#     Columns: PLT_CN, INVYR, CONDID, SPCD, SFTWD_HRDWD, DAMAGE_AGENT_CD,
#              ba_per_acre, n_trees_tpa
#   05_fia/data/processed/harvest_flags/state={ST}/harvest_flags_{ST}.parquet
#     Columns: PLT_CN, INVYR, STATECD
#     Rows: only plots where >= 1 dead tree has AGENTCD 80-89 (incidental harvest)
# ==============================================================================

source("scripts/utils/load_config.R")
config <- load_config()

library(here)
library(fs)
library(glue)
library(data.table)
library(tibble)
library(arrow)

# ------------------------------------------------------------------------------
# Setup
# ------------------------------------------------------------------------------

fia_config       <- config$raw$fia
raw_dir          <- here(fia_config$local_dir)
out_trees        <- here(config$processed$fia$trees$output_dir)
out_cond         <- here(config$processed$fia$cond$output_dir)
out_damage_ag    <- here(config$processed$fia$damage_agents$output_dir)
out_harvest_flags <- here(config$processed$fia$harvest_flags$output_dir)
lookup_dir <- here("05_fia/lookups")

# Parse command-line arguments.
# Usage:
#   Rscript 03_extract_trees.R                  # all states, skip existing
#   Rscript 03_extract_trees.R CO WY MT         # specific states, skip existing
#   Rscript 03_extract_trees.R --force-cond     # all states, force re-extract cond only
#   Rscript 03_extract_trees.R CO --force-cond  # specific state(s), force re-extract cond
#
# --force-cond: deletes existing cond parquets before running so TRTCD columns
#   (and any other future cond_cols additions) are picked up without re-running
#   the full tree aggregation (which is the slow part).

args        <- commandArgs(trailingOnly = TRUE)
force_cond  <- "--force-cond" %in% args
state_args  <- toupper(args[!args %in% "--force-cond"])
states      <- if (length(state_args) > 0) state_args else fia_config$states

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
cat(glue("Output (trees):   {out_trees}\n"))
cat(glue("Output (cond):    {out_cond}\n"))
cat(glue("Output (damage):  {out_damage_ag}\n"))
cat(glue("Output (harvest): {out_harvest_flags}\n"))
cat(glue("States: {length(states)}\n"))
cat(glue("INVYR range: {invyr_min}-{invyr_max}\n"))
if (force_cond) cat("Mode: --force-cond (cond parquets will be re-extracted even if they exist)\n")
cat("\n")

# Required columns
tree_cols <- c("CN", "PLT_CN", "CONDID", "SUBP", "SPCD", "STATUSCD",
               "DIA", "TPA_UNADJ", "CCLCD", "AGENTCD", "INVYR",
               "DAMAGE_AGENT_CD1", "DAMAGE_AGENT_CD2", "DAMAGE_AGENT_CD3")
plot_cols <- c("CN", "STATECD", "UNITCD", "COUNTYCD", "PLOT", "INVYR",
               "LAT", "LON", "ELEV")
cond_cols <- c("PLT_CN", "CONDID", "FORTYPCD", "COND_STATUS_CD",
               "CONDPROP_UNADJ", "INVYR",
               "DSTRBCD1", "DSTRBCD2", "DSTRBCD3",
               "DSTRBYR1", "DSTRBYR2", "DSTRBYR3",
               "TRTCD1",  "TRTCD2",  "TRTCD3",
               "TRTYR1",  "TRTYR2",  "TRTYR3")

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
  st              <- states[i]
  state_dir       <- file.path(raw_dir, st)
  trees_out        <- file.path(out_trees,         glue("state={st}/trees_{st}.parquet"))
  cond_out         <- file.path(out_cond,          glue("state={st}/cond_{st}.parquet"))
  damage_ag_out    <- file.path(out_damage_ag,     glue("state={st}/damage_agents_{st}.parquet"))
  harvest_flags_out <- file.path(out_harvest_flags, glue("state={st}/harvest_flags_{st}.parquet"))

  # Determine what needs to be (re-)computed for this state.
  # In --force-cond mode, cond is always re-extracted even if the parquet exists,
  # but trees/damage_agents/harvest_flags are left alone if they already exist.
  need_trees          <- !(file_exists(trees_out) && file_exists(damage_ag_out))
  need_harvest_flags  <- !file_exists(harvest_flags_out)
  need_cond           <- !file_exists(cond_out) || force_cond

  if (!need_trees && !need_harvest_flags && !need_cond) {
    cat(glue("[{i}/{length(states)}] {st}: all outputs exist - skipping\n"))
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

  what <- paste(c(
    if (need_trees)                        "trees+damage+harvest" else NULL,
    if (need_harvest_flags && !need_trees) "harvest_flags"        else NULL,
    if (need_cond)                         "cond"                 else NULL
  ), collapse = " + ")
  cat(glue("[{i}/{length(states)}] {st}: extracting {what}\n"))
  t_st <- Sys.time()

  tryCatch({

    # ------ Load tables (only what's needed) ----------------------------------
    plot_dt <- fread(plot_file, select = plot_cols, showProgress = FALSE)

    if (need_trees) {
      tree_dt <- fread(tree_file, select = tree_cols, showProgress = FALSE)
      cat(glue("  Loaded: TREE={format(nrow(tree_dt), big.mark=',')}"))
    } else if (need_harvest_flags) {
      # Lightweight: only AGENTCD columns needed for harvest flag detection
      tree_dt <- fread(tree_file,
                       select = c("PLT_CN", "INVYR", "AGENTCD"),
                       showProgress = FALSE)
      cat(glue("  Loaded: TREE(AGENTCD)={format(nrow(tree_dt), big.mark=',')}"))
    }
    if (need_cond) {
      # Intersect with available columns: older state files may lack TRTCD
      avail_cond  <- names(fread(cond_file, nrows = 0L, showProgress = FALSE))
      select_cond <- intersect(cond_cols, avail_cond)
      cond_dt <- fread(cond_file, select = select_cond, showProgress = FALSE)
      cat(glue("{if (need_trees || need_harvest_flags) ', ' else '  Loaded: '}",
               "COND={format(nrow(cond_dt), big.mark=',')}"))
    }
    cat(glue(", PLOT={format(nrow(plot_dt), big.mark=',')}\n"))

    # ------ Trees + damage agents (skipped if both parquets already exist) ----
    if (need_trees) {
      # Filter trees
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

      # Derived fields
      tree_dt[, ba_sqft_tree := 0.005454 * DIA^2]

      tree_dt[, size_class := fcase(
        DIA <  5.0, "sapling",
        DIA < 12.0, "intermediate",
        DIA >= 12.0, "mature",
        default = NA_character_
      )]

      tree_dt[, canopy_layer := fcase(
        !is.na(CCLCD) & CCLCD %in% overstory_codes,  "overstory",
        !is.na(CCLCD) & CCLCD %in% understory_codes, "understory",
        is.na(CCLCD)  & DIA >= fallback_dia,          "overstory",
        is.na(CCLCD)  & DIA <  fallback_dia,          "understory",
        default = NA_character_
      )]

      cclcd_missing_n <- tree_dt[is.na(CCLCD), .N]
      if (cclcd_missing_n > 0) {
        cat(glue("  Note: {format(cclcd_missing_n, big.mark=',')} trees had NA CCLCD ",
                 "(DIA fallback applied)\n"))
      }

      setkey(tree_dt, SPCD)
      tree_dt <- ref_sp[tree_dt, on = "SPCD"]

      group_cols <- c("PLT_CN", "INVYR", "SPCD", "SFTWD_HRDWD", "WOODLAND",
                      "STATUSCD", "size_class", "canopy_layer")

      trees_agg <- tree_dt[, .(
        ba_sqft     = sum(ba_sqft_tree,            na.rm = TRUE),
        ba_per_acre = sum(TPA_UNADJ * ba_sqft_tree, na.rm = TRUE),
        n_trees_tpa = sum(TPA_UNADJ,               na.rm = TRUE),
        n_trees_raw = .N
      ), by = group_cols]

      dir_create(dirname(trees_out))
      write_parquet(as_tibble(trees_agg), trees_out, compression = "snappy")
      cat(glue("  Trees:   {format(nrow(trees_agg), big.mark=',')} rows -> {file_size(trees_out)}\n"))

      # Damage agents
      has_damage <- tree_dt[STATUSCD == 1 &
        (!is.na(DAMAGE_AGENT_CD1) & DAMAGE_AGENT_CD1 != 0 |
         !is.na(DAMAGE_AGENT_CD2) & DAMAGE_AGENT_CD2 != 0 |
         !is.na(DAMAGE_AGENT_CD3) & DAMAGE_AGENT_CD3 != 0)]

      if (nrow(has_damage) > 0) {
        da_long <- melt(
          has_damage[, .(PLT_CN, INVYR, CONDID, SPCD, SFTWD_HRDWD,
                         ba_sqft_tree, TPA_UNADJ,
                         DAMAGE_AGENT_CD1, DAMAGE_AGENT_CD2, DAMAGE_AGENT_CD3)],
          id.vars       = c("PLT_CN", "INVYR", "CONDID", "SPCD", "SFTWD_HRDWD",
                            "ba_sqft_tree", "TPA_UNADJ"),
          measure.vars  = c("DAMAGE_AGENT_CD1", "DAMAGE_AGENT_CD2", "DAMAGE_AGENT_CD3"),
          variable.name = "agent_slot",
          value.name    = "DAMAGE_AGENT_CD"
        )
        da_long <- da_long[!is.na(DAMAGE_AGENT_CD) & DAMAGE_AGENT_CD != 0]
        da_agg <- da_long[, .(
          ba_per_acre = sum(TPA_UNADJ * ba_sqft_tree, na.rm = TRUE),
          n_trees_tpa = sum(TPA_UNADJ, na.rm = TRUE)
        ), by = .(PLT_CN, INVYR, CONDID, SPCD, SFTWD_HRDWD, DAMAGE_AGENT_CD)]
      } else {
        da_agg <- data.table(
          PLT_CN = integer(0), INVYR = integer(0), CONDID = integer(0),
          SPCD = integer(0), SFTWD_HRDWD = character(0),
          DAMAGE_AGENT_CD = integer(0), ba_per_acre = numeric(0),
          n_trees_tpa = numeric(0)
        )
      }
      dir_create(dirname(damage_ag_out))
      write_parquet(as_tibble(da_agg), damage_ag_out, compression = "snappy")
      cat(glue("  Damage:  {format(nrow(da_agg), big.mark=',')} rows -> {file_size(damage_ag_out)}\n"))

    }

    # ------ Harvest flags (AGENTCD 80-89 on dead trees) ----------------------
    # tree_dt holds either the full tree table (need_trees path) or just
    # PLT_CN/INVYR/AGENTCD (need_harvest_flags-only path); both work here.
    if (need_harvest_flags) {
      hf_dt <- tree_dt[!is.na(AGENTCD) & AGENTCD >= 80L & AGENTCD <= 89L,
                        .(PLT_CN, INVYR)]
      hf_dt <- unique(hf_dt)
      plot_map_hf <- plot_dt[, .(CN, STATECD)]
      setnames(plot_map_hf, "CN", "PLT_CN")
      setkey(hf_dt, PLT_CN)
      setkey(plot_map_hf, PLT_CN)
      hf_dt <- plot_map_hf[hf_dt, on = "PLT_CN"]
      dir_create(dirname(harvest_flags_out))
      write_parquet(as_tibble(hf_dt), harvest_flags_out, compression = "snappy")
      cat(glue("  Harvest: {format(nrow(hf_dt), big.mark=',')} flagged plots",
               " -> {file_size(harvest_flags_out)}\n"))
    }

    # ------ COND: filter and add STATECD + LAT/LON from PLOT -----------------
    if (need_cond) {
      cond_filt <- cond_dt[INVYR >= invyr_min & INVYR <= invyr_max]

      setkey(plot_dt, CN)
      plot_map <- plot_dt[, .(CN, STATECD, LAT, LON)]
      setnames(plot_map, "CN", "PLT_CN")
      setkey(cond_filt, PLT_CN)
      setkey(plot_map, PLT_CN)
      cond_filt <- plot_map[cond_filt, on = "PLT_CN"]

      dir_create(dirname(cond_out))
      write_parquet(as_tibble(cond_filt), cond_out, compression = "snappy")
      cat(glue("  Cond:    {format(nrow(cond_filt), big.mark=',')} rows -> {file_size(cond_out)}\n"))
    }

    elapsed <- as.numeric(difftime(Sys.time(), t_st, units = "secs"))
    cat(glue("  Time: {sprintf('%.1fs', elapsed)}\n"))
    n_done <- n_done + 1

  }, error = function(e) {
    warning(glue("  Error processing {st}: {e$message}"))
    n_failed <- n_failed + 1
  })

  if (exists("tree_dt"))    rm(tree_dt)
  if (exists("cond_dt"))    rm(cond_dt)
  if (exists("trees_agg"))  rm(trees_agg)
  if (exists("cond_filt"))  rm(cond_filt)
  if (exists("da_agg"))     rm(da_agg)
  if (exists("has_damage")) rm(has_damage)
  if (exists("da_long"))    rm(da_long)
  if (exists("hf_dt"))      rm(hf_dt)
  if (exists("plot_map_hf")) rm(plot_map_hf)
  rm(plot_dt)
  gc(verbose = FALSE)
}

# ------------------------------------------------------------------------------
# Summary
# ------------------------------------------------------------------------------

elapsed_total <- as.numeric(difftime(Sys.time(), t_total, units = "mins"))

cat(glue("\n{strrep('=', 50)}\n"))
cat("Tree extraction complete.\n\n")
cat(glue("  Processed: {n_done} state(s)\n"))
cat(glue("  Skipped:   {n_skipped} state(s) (output already exists)\n"))
cat(glue("  Failed:    {n_failed} state(s)\n"))
cat(glue("  Time:      {sprintf('%.1f', elapsed_total)} min\n\n"))

cat("Next step: Rscript 05_fia/scripts/04_extract_seedlings_mortality.R\n")
