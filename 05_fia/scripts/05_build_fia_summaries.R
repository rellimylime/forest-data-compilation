# ==============================================================================
# 05_build_fia_summaries.R
# Build plot-level FIA summary metrics from extracted parquet files
#
# Reads the per-state partitioned parquets from scripts 03 and 04, and
# aggregates to plot x INVYR level with:
#
#   plot_tree_metrics.parquet
#     - Total and per-stratum basal area (live/dead, soft/hardwood, size class,
#       canopy layer)
#     - Species richness and Shannon diversity index (BA-weighted, live only)
#     - Schema includes empty column for species temperature optima join (TBD)
#
#   plot_seedling_metrics.parquet
#     - Seedling counts per plot x INVYR, with Shannon H (count-weighted)
#
#   plot_mortality_metrics.parquet
#     - Between-measurement mortality per plot x INVYR x species x agent
#
#   plot_cond_fortypcd.parquet
#     - Forest type per plot x INVYR x condition (for transition analysis)
#
# Usage:
#   Rscript 05_fia/scripts/05_build_fia_summaries.R
#
# Output: 05_fia/data/processed/summaries/
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

fia_config  <- config$raw$fia
proc_fia    <- config$processed$fia
out_dir     <- here(proc_fia$summaries$output_dir)
states      <- fia_config$states

dir_create(out_dir)

cat("FIA Plot-Level Summaries\n")
cat("========================\n\n")
cat(glue("Output: {out_dir}\n\n"))

# ------------------------------------------------------------------------------
# Helper: Shannon diversity index
# dt must have group_cols + value_col (one row per species within group)
# Returns data.table with group_cols + shannon_h
# ------------------------------------------------------------------------------

compute_shannon_h <- function(dt, group_cols, value_col) {
  dt <- copy(dt)
  dt[, total := sum(get(value_col), na.rm = TRUE), by = group_cols]
  dt[total > 0, p_i := get(value_col) / total]
  dt[!is.na(p_i) & p_i > 0, h_i := -p_i * log(p_i)]
  dt[, .(shannon_h = sum(h_i, na.rm = TRUE)), by = group_cols]
}

# ------------------------------------------------------------------------------
# Step 1: plot_tree_metrics
# Aggregate trees parquet to plot x INVYR level
# ------------------------------------------------------------------------------

cat("Step 1: plot_tree_metrics\n")
out_tree_metrics <- file.path(out_dir, "plot_tree_metrics.parquet")

if (file_exists(out_tree_metrics)) {
  cat(glue("  Already exists ({file_size(out_tree_metrics)}) - skipping\n\n"))
} else {
  trees_ds <- tryCatch(
    open_dataset(here(proc_fia$trees$output_dir), partitioning = "state"),
    error = function(e) NULL
  )
  if (is.null(trees_ds)) {
    cat("  No tree parquets found. Run 03_extract_trees.R first.\n\n")
  } else {

    t_start <- Sys.time()
    results <- vector("list", length(states))

    for (i in seq_along(states)) {
      st <- states[i]
      dt <- tryCatch({
        trees_ds |> filter(state == st) |> collect() |> as.data.table()
      }, error = function(e) NULL)

      if (is.null(dt) || nrow(dt) == 0) next

      # -- BA totals by stratum (all combinations computed in one pass) --------
      live <- dt[STATUSCD == 1]
      dead <- dt[STATUSCD == 2]

      # Total BA
      ba_live <- live[, .(ba_live_total = sum(ba_per_acre, na.rm = TRUE),
                           n_trees_live  = sum(n_trees_tpa, na.rm = TRUE)),
                       by = .(PLT_CN, INVYR)]
      ba_dead <- dead[, .(ba_dead_total = sum(ba_per_acre, na.rm = TRUE),
                           n_trees_dead  = sum(n_trees_tpa, na.rm = TRUE)),
                       by = .(PLT_CN, INVYR)]

      # By functional group (live only)
      ba_soft <- live[SFTWD_HRDWD == "S",
                       .(ba_live_softwood = sum(ba_per_acre, na.rm = TRUE)),
                       by = .(PLT_CN, INVYR)]
      ba_hard <- live[SFTWD_HRDWD == "H",
                       .(ba_live_hardwood = sum(ba_per_acre, na.rm = TRUE)),
                       by = .(PLT_CN, INVYR)]

      # By size class (live only)
      ba_sz <- dcast(
        live[, .(ba_per_acre = sum(ba_per_acre, na.rm = TRUE)),
              by = .(PLT_CN, INVYR, size_class)],
        PLT_CN + INVYR ~ size_class,
        value.var = "ba_per_acre", fill = 0
      )
      # Rename columns defensively (not all size classes may be present)
      for (sc in c("sapling", "intermediate", "mature")) {
        if (!sc %in% names(ba_sz)) ba_sz[, (paste0("ba_live_", sc)) := NA_real_]
        else setnames(ba_sz, sc, paste0("ba_live_", sc))
      }

      # By canopy layer (live only)
      ba_ly <- dcast(
        live[, .(ba_per_acre = sum(ba_per_acre, na.rm = TRUE)),
              by = .(PLT_CN, INVYR, canopy_layer)],
        PLT_CN + INVYR ~ canopy_layer,
        value.var = "ba_per_acre", fill = 0
      )
      for (ly in c("overstory", "understory")) {
        if (!ly %in% names(ba_ly)) ba_ly[, (paste0("ba_live_", ly)) := NA_real_]
        else setnames(ba_ly, ly, paste0("ba_live_", ly))
      }

      # -- Diversity: species richness and Shannon H (BA-weighted, live) -------
      species_ba <- live[, .(ba_per_acre = sum(ba_per_acre, na.rm = TRUE)),
                          by = .(PLT_CN, INVYR, SPCD)]
      species_ba <- species_ba[ba_per_acre > 0]

      richness <- species_ba[, .(n_species_live = uniqueN(SPCD)), by = .(PLT_CN, INVYR)]
      shannon  <- compute_shannon_h(species_ba, c("PLT_CN", "INVYR"), "ba_per_acre")
      setnames(shannon, "shannon_h", "shannon_h_ba")

      # -- Join all metrics into one row per plot x INVYR ----------------------
      plot_keys <- .(PLT_CN, INVYR)
      result <- Reduce(function(a, b) merge(a, b, by = c("PLT_CN", "INVYR"), all = TRUE),
                       list(ba_live, ba_dead, ba_soft, ba_hard,
                            ba_sz, ba_ly, richness, shannon))

      # Add placeholder column for species temperature optima (thermophilization)
      result[, species_temp_optima_mean := NA_real_]   # join TBD when boss provides data

      result[, state := st]
      results[[i]] <- result

      rm(dt, live, dead, ba_live, ba_dead, ba_soft, ba_hard,
         ba_sz, ba_ly, species_ba, richness, shannon, result)
      gc(verbose = FALSE)

      if (i %% 10 == 0 || i == length(states)) {
        elapsed <- as.numeric(difftime(Sys.time(), t_start, units = "secs"))
        cat(glue("  [{i}/{length(states)}] {sprintf('%.0fs', elapsed)}\n"))
      }
    }

    all_metrics <- rbindlist(Filter(Negate(is.null), results), fill = TRUE)
    write_parquet(as_tibble(all_metrics), out_tree_metrics, compression = "snappy")
    cat(glue("  plot_tree_metrics: {format(nrow(all_metrics), big.mark=',')} rows -> ",
             "{file_size(out_tree_metrics)}\n\n"))
    rm(all_metrics, results)
    gc(verbose = FALSE)
  }
}

# ------------------------------------------------------------------------------
# Step 2: plot_seedling_metrics
# ------------------------------------------------------------------------------

cat("Step 2: plot_seedling_metrics\n")
out_seed_metrics <- file.path(out_dir, "plot_seedling_metrics.parquet")

if (file_exists(out_seed_metrics)) {
  cat(glue("  Already exists ({file_size(out_seed_metrics)}) - skipping\n\n"))
} else {
  seed_ds <- tryCatch(
    open_dataset(here(proc_fia$seedlings$output_dir), partitioning = "state"),
    error = function(e) NULL
  )
  if (is.null(seed_ds)) {
    cat("  No seedling parquets found. Run 04_extract_seedlings_mortality.R first.\n\n")
  } else {
    results <- vector("list", length(states))
    for (i in seq_along(states)) {
      st <- states[i]
      dt <- tryCatch(
        seed_ds |> filter(state == st) |> collect() |> as.data.table(),
        error = function(e) NULL
      )
      if (is.null(dt) || nrow(dt) == 0) next

      richness <- dt[, .(n_species_seedling = uniqueN(SPCD)), by = .(PLT_CN, INVYR)]
      shannon  <- compute_shannon_h(dt, c("PLT_CN", "INVYR"), "treecount_total")
      setnames(shannon, "shannon_h", "shannon_h_count")

      totals <- dt[, .(treecount_total = sum(treecount_total, na.rm = TRUE),
                        count_softwood  = sum(treecount_total[SFTWD_HRDWD == "S"], na.rm = TRUE),
                        count_hardwood  = sum(treecount_total[SFTWD_HRDWD == "H"], na.rm = TRUE)),
                    by = .(PLT_CN, INVYR)]

      result <- Reduce(function(a, b) merge(a, b, by = c("PLT_CN", "INVYR"), all = TRUE),
                       list(totals, richness, shannon))
      result[, state := st]
      results[[i]] <- result
      rm(dt, totals, richness, shannon, result); gc(verbose = FALSE)
    }
    all_seed <- rbindlist(Filter(Negate(is.null), results), fill = TRUE)
    write_parquet(as_tibble(all_seed), out_seed_metrics, compression = "snappy")
    cat(glue("  plot_seedling_metrics: {format(nrow(all_seed), big.mark=',')} rows -> ",
             "{file_size(out_seed_metrics)}\n\n"))
    rm(all_seed, results); gc(verbose = FALSE)
  }
}

# ------------------------------------------------------------------------------
# Step 3: plot_mortality_metrics
# Pass-through of per-state mortality parquets into a single national file
# ------------------------------------------------------------------------------

cat("Step 3: plot_mortality_metrics\n")
out_mort_metrics <- file.path(out_dir, "plot_mortality_metrics.parquet")

if (file_exists(out_mort_metrics)) {
  cat(glue("  Already exists ({file_size(out_mort_metrics)}) - skipping\n\n"))
} else {
  mort_ds <- tryCatch(
    open_dataset(here(proc_fia$mortality$output_dir), partitioning = "state"),
    error = function(e) NULL
  )
  if (is.null(mort_ds)) {
    cat("  No mortality parquets found. Run 04_extract_seedlings_mortality.R first.\n\n")
  } else {
    all_mort <- mort_ds |> collect() |> as.data.table()
    write_parquet(as_tibble(all_mort), out_mort_metrics, compression = "snappy")
    cat(glue("  plot_mortality_metrics: {format(nrow(all_mort), big.mark=',')} rows -> ",
             "{file_size(out_mort_metrics)}\n\n"))
    rm(all_mort); gc(verbose = FALSE)
  }
}

# ------------------------------------------------------------------------------
# Step 4: plot_cond_fortypcd
# Pass-through of per-state cond parquets into a single national file
# ------------------------------------------------------------------------------

cat("Step 4: plot_cond_fortypcd\n")
out_cond_metrics <- file.path(out_dir, "plot_cond_fortypcd.parquet")

if (file_exists(out_cond_metrics)) {
  cat(glue("  Already exists ({file_size(out_cond_metrics)}) - skipping\n\n"))
} else {
  cond_ds <- tryCatch(
    open_dataset(here(proc_fia$cond$output_dir), partitioning = "state"),
    error = function(e) NULL
  )
  if (is.null(cond_ds)) {
    cat("  No cond parquets found. Run 03_extract_trees.R first.\n\n")
  } else {
    all_cond <- cond_ds |> collect() |> as.data.table()
    write_parquet(as_tibble(all_cond), out_cond_metrics, compression = "snappy")
    cat(glue("  plot_cond_fortypcd: {format(nrow(all_cond), big.mark=',')} rows -> ",
             "{file_size(out_cond_metrics)}\n\n"))
    rm(all_cond); gc(verbose = FALSE)
  }
}

# ------------------------------------------------------------------------------
# Done
# ------------------------------------------------------------------------------

cat("FIA summaries complete.\n\n")
cat("Outputs:\n")
for (f in c(out_tree_metrics, out_seed_metrics, out_mort_metrics, out_cond_metrics)) {
  if (file_exists(f)) cat(glue("  {basename(f)}: {file_size(f)}\n"))
}
cat("\nRead with:\n")
cat("  tree_metrics <- arrow::read_parquet('05_fia/data/processed/summaries/plot_tree_metrics.parquet')\n")
