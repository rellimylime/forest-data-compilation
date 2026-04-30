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
#   plot_condition_metadata.parquet
#     - Condition metadata with stable plot IDs, forest type labels, and disturbance flags
#
#   plot_seedling_species.parquet
#     - Species-level seedling composition joined to condition metadata
#
#   plot_disturbance_classification.parquet
#     - Condition-level disturbance class, control eligibility, and timing fields
#
#   plot_treatment_history.parquet
#     - Condition-level treatment code/year history in long format
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
library(dplyr)
library(tibble)

# ------------------------------------------------------------------------------
# Setup
# ------------------------------------------------------------------------------

# Load FIA config paths and state list once for all summary steps.
fia_config  <- config$raw$fia
proc_fia    <- config$processed$fia
out_dir     <- here(proc_fia$summaries$output_dir)
states      <- fia_config$states

# Open cond dataset once because several summary products need condition fields.
cond_ds <- tryCatch(
  open_dataset(here(proc_fia$cond$output_dir), partitioning = "state"),
  error = function(e) NULL
)

# Ensure the summary output directory exists before any step writes parquet files.
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
  # Work on a copy so helper columns do not modify the caller's data.table.
  dt <- copy(dt)

  # Compute total abundance within each plot visit before calculating proportions.
  dt[, total := sum(get(value_col), na.rm = TRUE), by = group_cols]

  # Convert each species or stratum value to a relative abundance.
  dt[total > 0, p_i := get(value_col) / total]

  # Keep Shannon contributions only where the proportion is valid and positive.
  dt[!is.na(p_i) & p_i > 0, h_i := -p_i * log(p_i)]

  # Sum species contributions back to one diversity value per plot visit.
  dt[, .(shannon_h = sum(h_i, na.rm = TRUE)), by = group_cols]
}

# Sum optional fields while preserving NA when the whole field is unavailable.
sum_or_na <- function(x) {
  if (all(is.na(x))) NA_real_ else sum(x, na.rm = TRUE)
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
  # Open state-partitioned tree aggregates lazily so we can collect one state at a time.
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
      # Collect one state's tree aggregates to keep memory bounded.
      st <- states[i]
      dt <- tryCatch({
        trees_ds |> filter(state == st) |> collect() |> as.data.table()
      }, error = function(e) NULL)

      # Some states may be absent locally, especially during partial reruns.
      if (is.null(dt) || nrow(dt) == 0) next

      # -- BA totals by stratum (all combinations computed in one pass) --------
      # Split live and standing-dead trees because they support different metrics.
      live <- dt[STATUSCD == 1]
      dead <- dt[STATUSCD == 2]

      # Summarize total live basal area and live tree density by plot visit.
      ba_live <- live[, .(ba_live_total = sum(ba_per_acre, na.rm = TRUE),
                           n_trees_live  = sum(n_trees_tpa, na.rm = TRUE)),
                       by = .(PLT_CN, INVYR)]

      # Summarize standing-dead basal area and dead tree density by plot visit.
      ba_dead <- dead[, .(ba_dead_total = sum(ba_per_acre, na.rm = TRUE),
                           n_trees_dead  = sum(n_trees_tpa, na.rm = TRUE)),
                       by = .(PLT_CN, INVYR)]

      # Summarize live basal area by broad softwood/hardwood functional group.
      ba_soft <- live[SFTWD_HRDWD == "S",
                       .(ba_live_softwood = sum(ba_per_acre, na.rm = TRUE)),
                       by = .(PLT_CN, INVYR)]
      ba_hard <- live[SFTWD_HRDWD == "H",
                       .(ba_live_hardwood = sum(ba_per_acre, na.rm = TRUE)),
                       by = .(PLT_CN, INVYR)]

      # Pivot live basal area by size class into one column per class.
      ba_sz <- dcast(
        live[, .(ba_per_acre = sum(ba_per_acre, na.rm = TRUE)),
              by = .(PLT_CN, INVYR, size_class)],
        PLT_CN + INVYR ~ size_class,
        value.var = "ba_per_acre", fill = 0
      )
      # Rename columns defensively (not all size classes may be present)
      for (sc in c("sapling", "intermediate", "mature")) {
        # Keep a stable output schema even when a state lacks a size class.
        if (!sc %in% names(ba_sz)) ba_sz[, (paste0("ba_live_", sc)) := NA_real_]
        else setnames(ba_sz, sc, paste0("ba_live_", sc))
      }

      # Pivot live basal area by canopy layer into overstory/understory columns.
      ba_ly <- dcast(
        live[, .(ba_per_acre = sum(ba_per_acre, na.rm = TRUE)),
              by = .(PLT_CN, INVYR, canopy_layer)],
        PLT_CN + INVYR ~ canopy_layer,
        value.var = "ba_per_acre", fill = 0
      )
      for (ly in c("overstory", "understory")) {
        # Keep the layer columns available for downstream joins and plotting.
        if (!ly %in% names(ba_ly)) ba_ly[, (paste0("ba_live_", ly)) := NA_real_]
        else setnames(ba_ly, ly, paste0("ba_live_", ly))
      }

      # -- Diversity: species richness and Shannon H (BA-weighted, live) -------
      # Collapse live tree basal area to one row per species within each plot visit.
      species_ba <- live[, .(ba_per_acre = sum(ba_per_acre, na.rm = TRUE)),
                          by = .(PLT_CN, INVYR, SPCD)]

      # Remove zero-abundance species before richness/diversity calculations.
      species_ba <- species_ba[ba_per_acre > 0]

      # Count live tree species and compute BA-weighted Shannon diversity.
      richness <- species_ba[, .(n_species_live = uniqueN(SPCD)), by = .(PLT_CN, INVYR)]
      shannon  <- compute_shannon_h(species_ba, c("PLT_CN", "INVYR"), "ba_per_acre")
      setnames(shannon, "shannon_h", "shannon_h_ba")

      # -- Join all metrics into one row per plot x INVYR ----------------------
      # Merge all metric tables with full joins so missing strata do not drop plots.
      result <- Reduce(function(a, b) merge(a, b, by = c("PLT_CN", "INVYR"), all = TRUE),
                       list(ba_live, ba_dead, ba_soft, ba_hard,
                            ba_sz, ba_ly, richness, shannon))

      # Add placeholder column for species temperature optima (thermophilization)
      result[, species_temp_optima_mean := NA_real_]   # join TBD when boss provides data

      # Add LAT/LON from cond (one coordinate per PLT_CN)
      if (!is.null(cond_ds)) {
        # Coordinates live in condition outputs, so pull one unique coordinate per plot visit.
        coord_dt <- tryCatch({
          cond_ds |>
            filter(state == st) |>
            select(PLT_CN, LAT, LON) |>
            collect() |>
            as.data.table() |>
            unique(by = "PLT_CN")
        }, error = function(e) NULL)

        # Left join coordinates onto tree metrics when they are available.
        if (!is.null(coord_dt)) result <- coord_dt[result, on = "PLT_CN"]
      }

      # Preserve state as a simple filter column in the national summary.
      result[, state := st]
      results[[i]] <- result

      # Drop per-state intermediates before collecting the next state.
      rm(dt, live, dead, ba_live, ba_dead, ba_soft, ba_hard,
         ba_sz, ba_ly, species_ba, richness, shannon, result)
      gc(verbose = FALSE)

      if (i %% 10 == 0 || i == length(states)) {
        elapsed <- as.numeric(difftime(Sys.time(), t_start, units = "secs"))
        cat(glue("  [{i}/{length(states)}] {sprintf('%.0fs', elapsed)}\n"))
      }
    }

    # Bind all state results into the national plot-level tree metrics table.
    all_metrics <- rbindlist(Filter(Negate(is.null), results), fill = TRUE)

    # Write the final tree summary as a git-trackable parquet product.
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
  # Open species-level seedling extracts lazily so each state can be summarized alone.
  seed_ds <- tryCatch(
    open_dataset(here(proc_fia$seedlings$output_dir), partitioning = "state"),
    error = function(e) NULL
  )
  if (is.null(seed_ds)) {
    cat("  No seedling parquets found. Run 04_extract_seedlings_mortality.R first.\n\n")
  } else {
    results <- vector("list", length(states))
    for (i in seq_along(states)) {
      # Collect one state's species-level seedling rows.
      st <- states[i]
      dt <- tryCatch(
        seed_ds |> filter(state == st) |> collect() |> as.data.table(),
        error = function(e) NULL
      )

      # Skip absent states during partial local runs.
      if (is.null(dt) || nrow(dt) == 0) next

      # Collapse condition/subplot records to one species row before diversity math.
      species_seed <- dt[, .(
        treecount_total = sum(treecount_total, na.rm = TRUE),
        seedlings_tpa = if ("seedlings_tpa" %in% names(dt)) sum_or_na(seedlings_tpa) else NA_real_
      ), by = .(PLT_CN, INVYR, SPCD, SFTWD_HRDWD)]

      # Count seedling species within each plot visit.
      richness <- species_seed[, .(n_species_seedling = uniqueN(SPCD)), by = .(PLT_CN, INVYR)]

      # Compute count-weighted Shannon diversity after collapsing duplicate species rows.
      shannon  <- compute_shannon_h(species_seed, c("PLT_CN", "INVYR"), "treecount_total")
      setnames(shannon, "shannon_h", "shannon_h_count")

      # Sum total seedlings and broad functional group counts by plot visit.
      totals <- species_seed[, .(
        treecount_total = sum(treecount_total, na.rm = TRUE),
        seedlings_tpa = sum_or_na(seedlings_tpa),
        count_softwood = sum(treecount_total[SFTWD_HRDWD == "S"], na.rm = TRUE),
        count_hardwood = sum(treecount_total[SFTWD_HRDWD == "H"], na.rm = TRUE)
      ), by = .(PLT_CN, INVYR)]

      # Merge totals, richness, and diversity to one plot-year seedling summary.
      result <- Reduce(function(a, b) merge(a, b, by = c("PLT_CN", "INVYR"), all = TRUE),
                       list(totals, richness, shannon))

      # Preserve state as a filter column in the national seedling summary.
      result[, state := st]
      results[[i]] <- result

      # Drop state-level seedling intermediates before the next state.
      rm(dt, species_seed, totals, richness, shannon, result); gc(verbose = FALSE)
    }

    # Bind state summaries into the national plot-level seedling metrics table.
    all_seed <- rbindlist(Filter(Negate(is.null), results), fill = TRUE)

    # Write the compact plot-year seedling summary; species identity stays in the upstream extracts.
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
  # Open mortality extracts lazily, then collect because the national table is modest.
  mort_ds <- tryCatch(
    open_dataset(here(proc_fia$mortality$output_dir), partitioning = "state"),
    error = function(e) NULL
  )
  if (is.null(mort_ds)) {
    cat("  No mortality parquets found. Run 04_extract_seedlings_mortality.R first.\n\n")
  } else {
    # This product preserves species, mortality agent, and natural/harvest component type.
    all_mort <- mort_ds |> collect() |> as.data.table()

    # Write a single national mortality table for analysis convenience.
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
} else if (is.null(cond_ds)) {
  cat("  No cond parquets found. Run 03_extract_trees.R first.\n\n")
} else {
  # Pass through condition rows nationally so analysts can inspect raw condition fields.
  all_cond <- cond_ds |> collect() |> as.data.table()

  # Write the condition/forest-type table before building derived metadata products.
  write_parquet(as_tibble(all_cond), out_cond_metrics, compression = "snappy")
  cat(glue("  plot_cond_fortypcd: {format(nrow(all_cond), big.mark=',')} rows -> ",
           "{file_size(out_cond_metrics)}\n\n"))
  rm(all_cond); gc(verbose = FALSE)
}

# ------------------------------------------------------------------------------
# Step 4b: plot_condition_metadata
# Condition-level metadata with stable plot IDs, forest type labels, and flags.
# ------------------------------------------------------------------------------

cat("Step 4b: plot_condition_metadata\n")
out_cond_metadata <- file.path(out_dir, "plot_condition_metadata.parquet")

if (file_exists(out_cond_metadata)) {
  cat(glue("  Already exists ({file_size(out_cond_metadata)}) - skipping\n\n"))
} else if (is.null(cond_ds)) {
  cat("  No cond parquets found. Run 03_extract_trees.R --force-cond first.\n\n")
} else {
  # Define the condition metadata columns needed for stable plot IDs and disturbance flags.
  needed_cols <- c(
    "stable_plot_id", "PLT_CN", "INVYR",
    "STATECD", "UNITCD", "COUNTYCD", "PLOT", "PREV_PLT_CN",
    "LAT", "LON", "ELEV",
    "CONDID", "FORTYPCD", "COND_STATUS_CD", "CONDPROP_UNADJ",
    "DSTRBCD1", "DSTRBCD2", "DSTRBCD3",
    "DSTRBYR1", "DSTRBYR2", "DSTRBYR3",
    "TRTCD1", "TRTCD2", "TRTCD3",
    "TRTYR1", "TRTYR2", "TRTYR3",
    "state"
  )

  # Stop with a useful message if cond parquets have not been regenerated yet.
  # These columns are required because they define repeated-plot identity.
  missing_required <- setdiff(
    c("stable_plot_id", "UNITCD", "COUNTYCD", "PLOT"),
    names(cond_ds)
  )
  if (length(missing_required) > 0) {
    cat(glue("  Missing stable-id columns: {paste(missing_required, collapse=', ')}\n"))
    cat("  Re-run: Rscript 05_fia/scripts/03_extract_trees.R --force-cond\n\n")
  } else {
    # Read only metadata columns so the condition table stays manageable in memory.
    available_cols <- intersect(needed_cols, names(cond_ds))

    # Collect condition metadata after column selection to avoid reading unused fields.
    cond_meta <- cond_ds |>
      select(all_of(available_cols)) |>
      collect() |>
      as.data.table()

    # Add missing optional code columns as NA so old cond parquets fail gracefully.
    for (code_col in c("DSTRBCD1", "DSTRBCD2", "DSTRBCD3",
                       "TRTCD1", "TRTCD2", "TRTCD3")) {
      # Optional disturbance/treatment slots may be absent in older local outputs.
      if (!code_col %in% names(cond_meta)) cond_meta[, (code_col) := NA_integer_]
    }

    # Compute forested plot proportion once and attach it to every condition row.
    forested <- cond_meta[, .(
      n_conditions = .N,
      pct_forested = sum(CONDPROP_UNADJ[COND_STATUS_CD == 1L], na.rm = TRUE)
    ), by = .(PLT_CN, INVYR)]

    # Join plot-visit forested proportion back to condition-level metadata.
    # Each condition row carries the plot-level forested gate used for analysis filters.
    setkey(forested, PLT_CN, INVYR)
    setkey(cond_meta, PLT_CN, INVYR)
    cond_meta <- forested[cond_meta, on = .(PLT_CN, INVYR)]

    # Add forest type labels from the official FIA forest type lookup.
    ref_ft_path <- here("05_fia/lookups/ref_forest_type.parquet")
    if (file_exists(ref_ft_path)) {
      # Forest type lookup uses VALUE for FORTYPCD and MEANING for the label.
      ref_ft <- as.data.table(read_parquet(ref_ft_path))
      ref_ft <- ref_ft[, .(
        FORTYPCD = as.integer(VALUE),
        forest_type_label = MEANING,
        forest_type_group = TYPGRPCD
      )]

      # Keep one label row per forest type code before joining to condition rows.
      ref_ft <- unique(ref_ft, by = "FORTYPCD")

      # Match forest type labels to condition rows without changing unmatched rows.
      cond_meta[, FORTYPCD := as.integer(FORTYPCD)]
      setkey(ref_ft, FORTYPCD)
      setkey(cond_meta, FORTYPCD)
      cond_meta <- ref_ft[cond_meta, on = "FORTYPCD"]
    }

    # Mark forested conditions with FALSE instead of NA for missing status codes.
    cond_meta[, is_forested_condition := COND_STATUS_CD %in% 1L]

    # Flag fire disturbances, keeping crown fire as the stricter severity proxy.
    cond_meta[, has_fire_condition := DSTRBCD1 %in% c(30L, 31L, 32L) |
                                      DSTRBCD2 %in% c(30L, 31L, 32L) |
                                      DSTRBCD3 %in% c(30L, 31L, 32L)]
    cond_meta[, has_crown_fire_condition := DSTRBCD1 %in% 32L |
                                             DSTRBCD2 %in% 32L |
                                             DSTRBCD3 %in% 32L]

    # Flag biological disturbance classes used for treatment-control matching.
    cond_meta[, has_insect_condition := DSTRBCD1 %in% c(10L, 11L, 12L) |
                                        DSTRBCD2 %in% c(10L, 11L, 12L) |
                                        DSTRBCD3 %in% c(10L, 11L, 12L)]
    cond_meta[, has_disease_condition := DSTRBCD1 %in% c(20L, 21L, 22L) |
                                         DSTRBCD2 %in% c(20L, 21L, 22L) |
                                         DSTRBCD3 %in% c(20L, 21L, 22L)]

    # Flag weather and human disturbance classes with NA-safe membership tests.
    cond_meta[, has_wind_condition := DSTRBCD1 %in% 52L | DSTRBCD2 %in% 52L | DSTRBCD3 %in% 52L]
    cond_meta[, has_drought_condition := DSTRBCD1 %in% 54L | DSTRBCD2 %in% 54L | DSTRBCD3 %in% 54L]
    cond_meta[, has_human_dist_condition := DSTRBCD1 %in% 80L | DSTRBCD2 %in% 80L | DSTRBCD3 %in% 80L]

    # Flag cutting treatments separately because they are management, not natural disturbance.
    cond_meta[, has_cutting_treatment := TRTCD1 %in% 10L | TRTCD2 %in% 10L | TRTCD3 %in% 10L]

    # Write one condition-level metadata table for downstream matching and modeling.
    # This table is the main join target for thermophilization analysis setup.
    write_parquet(as_tibble(cond_meta), out_cond_metadata, compression = "snappy")
    cat(glue("  plot_condition_metadata: {format(nrow(cond_meta), big.mark=',')} rows -> ",
             "{file_size(out_cond_metadata)}\n\n"))

    rm(cond_meta, forested)
    if (exists("ref_ft")) rm(ref_ft)

    # Release the large condition metadata table before later summary steps run.
    gc(verbose = FALSE)
  }
}

# ------------------------------------------------------------------------------
# Step 4c: plot_seedling_species
# Analysis-ready species-level seedlings joined to condition metadata.
# ------------------------------------------------------------------------------

cat("Step 4c: plot_seedling_species\n")
out_seed_species <- file.path(out_dir, "plot_seedling_species.parquet")

if (file_exists(out_seed_species)) {
  cat(glue("  Already exists ({file_size(out_seed_species)}) - skipping\n\n"))
} else if (!file_exists(out_cond_metadata)) {
  cat("  plot_condition_metadata.parquet not found. Run Step 4b first.\n\n")
} else {
  # Open per-state seedling extracts lazily because this product starts from species rows.
  seed_ds <- tryCatch(
    open_dataset(here(proc_fia$seedlings$output_dir), partitioning = "state"),
    error = function(e) NULL
  )

  if (is.null(seed_ds)) {
    cat("  No seedling parquets found. Run 04_extract_seedlings_mortality.R first.\n\n")
  } else {
    # Require the refreshed seedling grain before building this analysis product.
    missing_seed_cols <- setdiff(c("CONDID", "SUBP", "treecount_total"), names(seed_ds))
    if (length(missing_seed_cols) > 0) {
      cat(glue("  Seedling parquets missing: {paste(missing_seed_cols, collapse=', ')}\n"))
      cat("  Re-run: Rscript 05_fia/scripts/04_extract_seedlings_mortality.R --force-seedlings\n\n")
    } else {
      # Keep seedling composition/count fields; condition metadata supplies plot identity.
      seed_cols <- intersect(
        c("PLT_CN", "INVYR", "CONDID", "SUBP", "SPCD",
          "COMMON_NAME", "SCIENTIFIC_NAME", "GENUS", "SPECIES",
          "SFTWD_HRDWD", "WOODLAND", "MAJOR_SPGRPCD", "JENKINS_SPGRPCD",
          "treecount_total", "treecount_calc_total", "seedlings_tpa",
          "n_seedling_records", "state"),
        names(seed_ds)
      )

      # Collect the refreshed seedling product for the national condition join.
      seed_species <- seed_ds |>
        select(all_of(seed_cols)) |>
        collect() |>
        as.data.table()

      # Read condition metadata columns needed for matching, filtering, and modeling.
      cond_meta <- as.data.table(read_parquet(out_cond_metadata))
      meta_cols <- intersect(
        c("PLT_CN", "INVYR", "CONDID", "stable_plot_id",
          "STATECD", "UNITCD", "COUNTYCD", "PLOT", "PREV_PLT_CN",
          "LAT", "LON", "ELEV", "FORTYPCD", "forest_type_label",
          "forest_type_group", "COND_STATUS_CD", "CONDPROP_UNADJ",
          "pct_forested", "is_forested_condition",
          "has_fire_condition", "has_crown_fire_condition",
          "has_insect_condition", "has_disease_condition",
          "has_wind_condition", "has_drought_condition",
          "has_human_dist_condition", "has_cutting_treatment"),
        names(cond_meta)
      )
      cond_meta <- cond_meta[, ..meta_cols]

      # Join seedlings to their exact FIA condition so disturbance and forest type match.
      seed_species <- merge(
        seed_species, cond_meta,
        by = c("PLT_CN", "INVYR", "CONDID"),
        all.x = TRUE
      )

      # Report unmatched rows because missing condition joins would break inference.
      n_missing_meta <- sum(is.na(seed_species$stable_plot_id))
      if (n_missing_meta > 0) {
        cat(glue("  Warning: {format(n_missing_meta, big.mark=',')} seedling rows lack condition metadata\n"))
      }

      # Write one national species-level recruitment product for thermophilization analyses.
      write_parquet(as_tibble(seed_species), out_seed_species, compression = "snappy")
      cat(glue("  plot_seedling_species: {format(nrow(seed_species), big.mark=',')} rows -> ",
               "{file_size(out_seed_species)}\n\n"))

      rm(seed_species, cond_meta)
      gc(verbose = FALSE)
    }
  }
}

# ------------------------------------------------------------------------------
# Step 4d: plot_disturbance_classification
# Condition-level disturbance classes and control eligibility for analysis.
# ------------------------------------------------------------------------------

cat("Step 4d: plot_disturbance_classification\n")
out_disturb_class <- file.path(out_dir, "plot_disturbance_classification.parquet")

if (file_exists(out_disturb_class)) {
  cat(glue("  Already exists ({file_size(out_disturb_class)}) - skipping\n\n"))
} else if (!file_exists(out_cond_metadata)) {
  cat("  plot_condition_metadata.parquet not found. Run Step 4b first.\n\n")
} else {
  # Load condition metadata because FIA disturbance and treatment are condition-level.
  cond_class <- as.data.table(read_parquet(out_cond_metadata))

  # Keep a stable schema even if older metadata lacks optional code/year fields.
  for (code_col in c("DSTRBCD1", "DSTRBCD2", "DSTRBCD3",
                     "DSTRBYR1", "DSTRBYR2", "DSTRBYR3",
                     "TRTCD1", "TRTCD2", "TRTCD3",
                     "TRTYR1", "TRTYR2", "TRTYR3")) {
    if (!code_col %in% names(cond_class)) cond_class[, (code_col) := NA_integer_]
  }

  # Recompute class flags from raw FIA codes so this product owns the definitions.
  has_any_code <- function(dt, code_cols, code_set) {
    Reduce(`|`, lapply(code_cols, function(code_col) dt[[code_col]] %in% code_set))
  }

  # Use any nonzero FIA disturbance code to distinguish true controls from unclassified rows.
  dist_code_cols <- c("DSTRBCD1", "DSTRBCD2", "DSTRBCD3")
  dist_year_cols <- c("DSTRBYR1", "DSTRBYR2", "DSTRBYR3")
  trt_code_cols  <- c("TRTCD1", "TRTCD2", "TRTCD3")
  trt_year_cols  <- c("TRTYR1", "TRTYR2", "TRTYR3")

  # FIA disturbance code groups used by the first-pass thermophilization analysis.
  cond_class[, has_fire_condition := has_any_code(cond_class, dist_code_cols, c(30L, 31L, 32L))]
  cond_class[, has_crown_fire_condition := has_any_code(cond_class, dist_code_cols, 32L)]
  cond_class[, has_insect_condition := has_any_code(cond_class, dist_code_cols, c(10L, 11L, 12L))]
  cond_class[, has_disease_condition := has_any_code(cond_class, dist_code_cols, c(20L, 21L, 22L))]
  cond_class[, has_wind_condition := has_any_code(cond_class, dist_code_cols, 52L)]
  cond_class[, has_drought_condition := has_any_code(cond_class, dist_code_cols, 54L)]
  cond_class[, has_other_weather_condition := has_any_code(cond_class, dist_code_cols, c(50L, 51L, 53L))]
  cond_class[, has_animal_condition := has_any_code(cond_class, dist_code_cols, 40L:46L)]
  cond_class[, has_vegetation_condition := has_any_code(cond_class, dist_code_cols, 60L)]
  cond_class[, has_geologic_condition := has_any_code(cond_class, dist_code_cols, 90L:95L)]
  cond_class[, has_unknown_other_condition := has_any_code(cond_class, dist_code_cols, c(70L, 80L))]
  cond_class[, has_human_dist_condition := has_any_code(cond_class, dist_code_cols, 80L)]

  # Treatment flags separate managed plots from natural disturbance and controls.
  cond_class[, has_any_recorded_disturbance := Reduce(
    `|`, lapply(dist_code_cols, function(code_col) !is.na(cond_class[[code_col]]) & cond_class[[code_col]] != 0L)
  )]
  cond_class[, has_any_treatment := Reduce(
    `|`, lapply(trt_code_cols, function(code_col) !is.na(cond_class[[code_col]]) & cond_class[[code_col]] != 0L)
  )]
  cond_class[, has_cutting_treatment := has_any_code(cond_class, trt_code_cols, 10L)]
  cond_class[, is_human_or_harvest := has_human_dist_condition | has_cutting_treatment]

  # Count how many natural classes occur so mixed disturbances can be flagged.
  natural_flag_cols <- c(
    "has_fire_condition", "has_insect_condition", "has_disease_condition",
    "has_wind_condition", "has_drought_condition", "has_other_weather_condition",
    "has_animal_condition", "has_vegetation_condition", "has_geologic_condition"
  )
  cond_class[, n_natural_disturbance_classes := rowSums(.SD, na.rm = TRUE), .SDcols = natural_flag_cols]
  cond_class[, is_multiple_natural_disturbance := n_natural_disturbance_classes > 1L]

  # Keep a natural primary class even when management flags force exclusion later.
  cond_class[, natural_disturbance_primary := fcase(
    has_crown_fire_condition, "crown_fire",
    has_fire_condition, "fire",
    has_insect_condition, "insect",
    has_disease_condition, "disease",
    has_wind_condition, "wind",
    has_drought_condition, "drought",
    has_other_weather_condition, "other_weather",
    has_animal_condition, "animal",
    has_vegetation_condition, "vegetation",
    has_geologic_condition, "geologic",
    default = "none"
  )]
  cond_class[, is_natural_disturbance := natural_disturbance_primary != "none"]

  # Primary class applies exclusion-relevant management before natural classes.
  cond_class[, disturbance_class_primary := fcase(
    is_human_or_harvest, "human_or_harvest",
    has_any_treatment, "other_treatment",
    natural_disturbance_primary != "none", natural_disturbance_primary,
    has_unknown_other_condition, "other_unknown",
    has_any_recorded_disturbance, "other_recorded",
    default = "none"
  )]
  cond_class[, disturbance_class := fcase(
    disturbance_class_primary %in% c("crown_fire", "fire"), "fire",
    disturbance_class_primary %in% c("wind", "drought", "other_weather"), "weather",
    disturbance_class_primary %in% c("animal", "vegetation", "geologic", "other_unknown", "other_recorded"), "other",
    default = disturbance_class_primary
  )]

  # Crown fire is the strongest FIA-only high-severity proxy available in this product.
  cond_class[, is_high_severity_proxy := has_crown_fire_condition]
  cond_class[, high_severity_proxy_type := fifelse(has_crown_fire_condition, "crown_fire", NA_character_)]

  # Convert disturbance years to timing metrics, treating 9999 as continuous/unknown timing.
  valid_year_from_code <- function(code, year) {
    y <- as.integer(year)
    y[is.na(code) | code == 0L | is.na(y) | y %in% c(0L, 9999L)] <- NA_integer_
    y
  }
  valid_dstr_year_cols <- paste0("dstr_year_valid_", seq_along(dist_year_cols))
  valid_trt_year_cols  <- paste0("trt_year_valid_", seq_along(trt_year_cols))
  valid_cut_year_cols  <- paste0("cut_year_valid_", seq_along(trt_year_cols))

  # Build slot-wise valid year columns so pmin/pmax can be vectorized.
  for (j in seq_along(dist_year_cols)) {
    cond_class[, (valid_dstr_year_cols[j]) := valid_year_from_code(get(dist_code_cols[j]), get(dist_year_cols[j]))]
    cond_class[, (valid_trt_year_cols[j]) := valid_year_from_code(get(trt_code_cols[j]), get(trt_year_cols[j]))]
    cond_class[, (valid_cut_year_cols[j]) := valid_year_from_code(fifelse(get(trt_code_cols[j]) == 10L, 10L, 0L), get(trt_year_cols[j]))]
  }

  # Use the latest valid year as the most interpretable time-since-disturbance metric.
  latest_dstr <- do.call(pmax, c(cond_class[, ..valid_dstr_year_cols], na.rm = TRUE))
  earliest_dstr <- do.call(pmin, c(cond_class[, ..valid_dstr_year_cols], na.rm = TRUE))
  latest_trt <- do.call(pmax, c(cond_class[, ..valid_trt_year_cols], na.rm = TRUE))
  latest_cut <- do.call(pmax, c(cond_class[, ..valid_cut_year_cols], na.rm = TRUE))

  # pmin/pmax return +/-Inf when all inputs are NA; replace those with missing values.
  latest_dstr[!is.finite(latest_dstr)] <- NA_integer_
  earliest_dstr[!is.finite(earliest_dstr)] <- NA_integer_
  latest_trt[!is.finite(latest_trt)] <- NA_integer_
  latest_cut[!is.finite(latest_cut)] <- NA_integer_

  # Attach timing fields used for post-disturbance windows and sensitivity checks.
  cond_class[, disturbance_year_latest := as.integer(latest_dstr)]
  cond_class[, disturbance_year_earliest := as.integer(earliest_dstr)]
  cond_class[, treatment_year_latest := as.integer(latest_trt)]
  cond_class[, cutting_year_latest := as.integer(latest_cut)]
  cond_class[, time_since_disturbance := INVYR - disturbance_year_latest]
  cond_class[, time_since_treatment := INVYR - treatment_year_latest]
  cond_class[, time_since_cutting := INVYR - cutting_year_latest]
  cond_class[time_since_disturbance < 0L, time_since_disturbance := NA_integer_]
  cond_class[time_since_treatment < 0L, time_since_treatment := NA_integer_]
  cond_class[time_since_cutting < 0L, time_since_cutting := NA_integer_]

  # Continuous disturbance years should not be interpreted as exact years.
  cond_class[, has_continuous_disturbance_year := Reduce(
    `|`, lapply(seq_along(dist_year_cols), function(j) {
      !is.na(cond_class[[dist_code_cols[j]]]) &
        cond_class[[dist_code_cols[j]]] != 0L &
        cond_class[[dist_year_cols[j]]] == 9999L
    })
  )]
  cond_class[, has_continuous_treatment_year := Reduce(
    `|`, lapply(seq_along(trt_year_cols), function(j) {
      !is.na(cond_class[[trt_code_cols[j]]]) &
        cond_class[[trt_code_cols[j]]] != 0L &
        cond_class[[trt_year_cols[j]]] == 9999L
    })
  )]

  # First-pass West/East grouping follows the hypothesis and uses longitude when available.
  west_states <- c("AK", "AZ", "CA", "CO", "ID", "MT", "NM", "NV", "OR", "UT", "WA", "WY")
  cond_class[, region_east_west := fifelse(
    !is.na(LON), fifelse(LON <= -100, "West", "East"),
    fifelse(state %in% west_states, "West", "East")
  )]
  cond_class[, region_source := fifelse(!is.na(LON), "longitude_-100", "state_fallback")]

  # Define analysis/control gates explicitly so matching code does not rewrite them.
  cond_class[, is_forested_analysis_condition := is_forested_condition & !is.na(pct_forested) & pct_forested >= 0.5]
  cond_class[, is_control_candidate := is_forested_analysis_condition &
               !has_any_recorded_disturbance & !has_any_treatment]
  cond_class[, is_natural_disturbance_candidate := is_forested_analysis_condition &
               is_natural_disturbance & !is_human_or_harvest & !has_any_treatment]
  cond_class[, disturbed_vs_control := fcase(
    is_control_candidate, "control",
    is_natural_disturbance_candidate, "disturbed",
    default = "exclude_or_other"
  )]

  # Record the main reason a row is not an untreated/unimpacted control candidate.
  cond_class[, control_eligibility_reason := fcase(
    !is_forested_condition, "condition_not_forested",
    is.na(pct_forested) | pct_forested < 0.5, "plot_less_than_50pct_forested",
    is_human_or_harvest, "human_or_harvest",
    has_any_treatment, "treated",
    has_any_recorded_disturbance, "recorded_disturbance",
    default = "control_candidate"
  )]

  # Drop temporary valid-year columns before writing the analysis product.
  cond_class[, c(valid_dstr_year_cols, valid_trt_year_cols, valid_cut_year_cols) := NULL]

  # Keep identifiers, raw codes, and derived analysis fields together.
  preferred_cols <- c(
    "stable_plot_id", "PLT_CN", "INVYR", "STATECD", "UNITCD", "COUNTYCD", "PLOT",
    "PREV_PLT_CN", "state", "region_east_west", "region_source",
    "CONDID", "COND_STATUS_CD", "CONDPROP_UNADJ", "pct_forested",
    "is_forested_condition", "is_forested_analysis_condition",
    "LAT", "LON", "ELEV", "FORTYPCD", "forest_type_label", "forest_type_group",
    "DSTRBCD1", "DSTRBCD2", "DSTRBCD3", "DSTRBYR1", "DSTRBYR2", "DSTRBYR3",
    "TRTCD1", "TRTCD2", "TRTCD3", "TRTYR1", "TRTYR2", "TRTYR3",
    "has_any_recorded_disturbance", "has_any_treatment", "has_cutting_treatment",
    "is_human_or_harvest", natural_flag_cols, "has_human_dist_condition",
    "has_unknown_other_condition", "n_natural_disturbance_classes",
    "is_multiple_natural_disturbance", "natural_disturbance_primary",
    "is_natural_disturbance", "disturbance_class_primary", "disturbance_class",
    "is_high_severity_proxy", "high_severity_proxy_type",
    "disturbance_year_latest", "disturbance_year_earliest",
    "treatment_year_latest", "cutting_year_latest",
    "time_since_disturbance", "time_since_treatment", "time_since_cutting",
    "has_continuous_disturbance_year", "has_continuous_treatment_year",
    "is_control_candidate", "is_natural_disturbance_candidate",
    "disturbed_vs_control", "control_eligibility_reason"
  )
  out_cols <- intersect(preferred_cols, names(cond_class))
  cond_class <- cond_class[, ..out_cols]

  # Write the reusable disturbance backbone for matching, filters, and model inputs.
  write_parquet(as_tibble(cond_class), out_disturb_class, compression = "snappy")
  cat(glue("  plot_disturbance_classification: {format(nrow(cond_class), big.mark=',')} rows -> ",
           "{file_size(out_disturb_class)}\n"))

  # Print compact class counts so logs expose obvious classification problems.
  class_counts <- cond_class[, .N, by = disturbance_class_primary][order(-N)]
  print(class_counts)
  cat("\n")

  rm(cond_class, class_counts, latest_dstr, earliest_dstr, latest_trt, latest_cut)
  gc(verbose = FALSE)
}

# ------------------------------------------------------------------------------
# Step 5: plot_disturbance_history
# Pivot DSTRBCD1/2/3 + DSTRBYR1/2/3 to long format and label disturbance types
# ------------------------------------------------------------------------------

cat("Step 5: plot_disturbance_history\n")
out_disturb <- file.path(out_dir, "plot_disturbance_history.parquet")

if (file_exists(out_disturb)) {
  cat(glue("  Already exists ({file_size(out_disturb)}) - skipping\n\n"))
} else if (is.null(cond_ds)) {
  cat("  No cond parquets found. Run 03_extract_trees.R first.\n\n")
} else {
  # Inline disturbance code lookup (FIADB v9.4 Appendix, COND.DSTRBCD)
  ref_disturbance <- data.table(
    DSTRBCD = c(10L, 11L, 12L,
                20L, 21L, 22L,
                30L, 31L, 32L,
                40L, 41L, 42L, 43L, 44L, 45L, 46L,
                50L, 51L, 52L, 53L, 54L,
                60L, 70L, 80L,
                90L, 91L, 92L, 93L, 94L, 95L),
    disturbance_label = c(
      "Insect damage", "Insect damage to understory", "Insect damage to trees",
      "Disease damage", "Disease damage to understory", "Disease damage to trees",
      "Fire damage (general)", "Ground fire", "Crown fire",
      "Animal damage", "Beaver", "Porcupine", "Deer/ungulate",
      "Bear", "Rabbit", "Domestic animal/livestock",
      "Weather damage", "Ice", "Wind/hurricane/tornado", "Flooding", "Drought",
      "Vegetation (competition/vines)", "Unknown/other", "Human-induced",
      "Geologic", "Landslide", "Avalanche", "Volcanic blast zone",
      "Other geologic event", "Earth movement/avalanche"
    ),
    disturbance_category = c(
      "insects", "insects", "insects",
      "disease", "disease", "disease",
      "fire", "fire", "fire",
      "animal", "animal", "animal", "animal", "animal", "animal", "animal",
      "weather", "weather", "weather", "weather", "weather",
      "vegetation", "other", "other",
      "geologic", "geologic", "geologic", "geologic", "geologic", "geologic"
    )
  )
  setkey(ref_disturbance, DSTRBCD)

  all_cond_d <- cond_ds |>
    select(PLT_CN, INVYR, STATECD, CONDID, CONDPROP_UNADJ, LAT, LON,
           DSTRBCD1, DSTRBCD2, DSTRBCD3, DSTRBYR1, DSTRBYR2, DSTRBYR3) |>
    collect() |> as.data.table()

  # Pivot code and year columns together in matched pairs
  disturb_long <- rbindlist(list(
    all_cond_d[, .(PLT_CN, INVYR, STATECD, CONDID, CONDPROP_UNADJ, LAT, LON,
                   disturbance_slot = 1L,
                   DSTRBCD = DSTRBCD1, DSTRBYR = DSTRBYR1)],
    all_cond_d[, .(PLT_CN, INVYR, STATECD, CONDID, CONDPROP_UNADJ, LAT, LON,
                   disturbance_slot = 2L,
                   DSTRBCD = DSTRBCD2, DSTRBYR = DSTRBYR2)],
    all_cond_d[, .(PLT_CN, INVYR, STATECD, CONDID, CONDPROP_UNADJ, LAT, LON,
                   disturbance_slot = 3L,
                   DSTRBCD = DSTRBCD3, DSTRBYR = DSTRBYR3)]
  ))

  # Keep only actual disturbances (non-zero, non-NA)
  disturb_long <- disturb_long[!is.na(DSTRBCD) & DSTRBCD != 0L]

  # Join labels
  setkey(disturb_long, DSTRBCD)
  disturb_long <- ref_disturbance[disturb_long, on = "DSTRBCD"]

  write_parquet(as_tibble(disturb_long), out_disturb, compression = "snappy")
  cat(glue("  plot_disturbance_history: {format(nrow(disturb_long), big.mark=',')} rows -> ",
           "{file_size(out_disturb)}\n\n"))
  rm(all_cond_d, disturb_long); gc(verbose = FALSE)
}

# ------------------------------------------------------------------------------
# Step 5b: plot_treatment_history
# Pivot TRTCD1/2/3 + TRTYR1/2/3 to long format with human-readable labels.
# Mirrors the reference code's trt_check but covers ALL treatment types (not
# just TRTCD==10), and includes treatment year so temporal filtering is possible.
# One row per condition × treatment slot where TRTCD != 0.
# ------------------------------------------------------------------------------

cat("Step 5b: plot_treatment_history\n")
out_treat <- file.path(out_dir, "plot_treatment_history.parquet")

if (file_exists(out_treat)) {
  cat(glue("  Already exists ({file_size(out_treat)}) - skipping\n\n"))
} else if (is.null(cond_ds)) {
  cat("  No cond parquets found. Run 03_extract_trees.R first.\n\n")
} else {
  # Check TRTCD/TRTYR columns are present (require --force-cond re-run of 03)
  avail_trt <- cond_ds$schema$names
  has_trt   <- all(c("TRTCD1", "TRTCD2", "TRTCD3", "TRTYR1", "TRTYR2", "TRTYR3") %in% avail_trt)

  if (!has_trt) {
    cat("  Warning: TRTCD/TRTYR columns not found in cond parquets.\n")
    cat("  Re-run 03_extract_trees.R --force-cond to backfill, then re-run this step.\n\n")
  } else {
    # Treatment code lookup (FIADB v9.4 COND.TRTCD)
    ref_treatment <- data.table(
      TRTCD = c(10L, 20L, 30L, 40L, 50L),
      treatment_label    = c("Cutting", "Site preparation",
                             "Artificial regeneration", "Natural regeneration",
                             "Other silvicultural treatment"),
      treatment_category = c("harvest", "site_prep",
                             "regeneration", "regeneration", "other_silv")
    )
    setkey(ref_treatment, TRTCD)

    all_cond_t <- cond_ds |>
      select(PLT_CN, INVYR, STATECD, CONDID, CONDPROP_UNADJ, LAT, LON,
             TRTCD1, TRTCD2, TRTCD3, TRTYR1, TRTYR2, TRTYR3) |>
      collect() |> as.data.table()

    # Pivot code and year columns in matched pairs (mirrors disturbance history)
    treat_long <- rbindlist(list(
      all_cond_t[, .(PLT_CN, INVYR, STATECD, CONDID, CONDPROP_UNADJ, LAT, LON,
                     treatment_slot = 1L, TRTCD = TRTCD1, TRTYR = TRTYR1)],
      all_cond_t[, .(PLT_CN, INVYR, STATECD, CONDID, CONDPROP_UNADJ, LAT, LON,
                     treatment_slot = 2L, TRTCD = TRTCD2, TRTYR = TRTYR2)],
      all_cond_t[, .(PLT_CN, INVYR, STATECD, CONDID, CONDPROP_UNADJ, LAT, LON,
                     treatment_slot = 3L, TRTCD = TRTCD3, TRTYR = TRTYR3)]
    ))

    # Keep only actual treatments (non-zero, non-NA)
    treat_long <- treat_long[!is.na(TRTCD) & TRTCD != 0L]

    # Join labels
    setkey(treat_long, TRTCD)
    treat_long <- ref_treatment[treat_long, on = "TRTCD"]

    write_parquet(as_tibble(treat_long), out_treat, compression = "snappy")
    cat(glue("  plot_treatment_history: {format(nrow(treat_long), big.mark=',')} rows -> ",
             "{file_size(out_treat)}\n\n"))
    rm(all_cond_t, treat_long); gc(verbose = FALSE)
  }
}

# ------------------------------------------------------------------------------
# Step 6: plot_damage_agents
# Collect per-state damage_agents parquets and join agent code labels
# ------------------------------------------------------------------------------

cat("Step 6: plot_damage_agents\n")
out_damage_ag <- file.path(out_dir, "plot_damage_agents.parquet")

if (file_exists(out_damage_ag)) {
  cat(glue("  Already exists ({file_size(out_damage_ag)}) - skipping\n\n"))
} else {
  da_ds <- tryCatch(
    open_dataset(here(proc_fia$damage_agents$output_dir), partitioning = "state"),
    error = function(e) NULL
  )
  if (is.null(da_ds)) {
    cat("  No damage_agents parquets found. Run 03_extract_trees.R first.\n\n")
  } else {
    # Inline damage agent lookup — category codes + ~30 high-profile species
    # Source: FIADB v9.4 Appendix H (PTIPS/FHAAST codes)
    ref_damage_agent <- data.table(
      DAMAGE_AGENT_CD = c(
        # Category-level codes
        10000L, 11000L, 12000L, 13000L, 14000L, 15000L, 16000L, 17000L, 18000L,
        19000L, 20000L, 21000L, 22000L, 22500L, 23000L, 24000L, 25000L, 26000L, 27000L,
        30000L, 30001L, 30002L, 30003L, 30004L,
        41000L, 42000L, 50000L, 60000L, 70000L, 71000L, 80000L, 90000L, 99000L,
        # Named bark beetle species (11xxx)
        11003L, 11006L, 11007L, 11009L, 11010L, 11019L, 11023L, 11029L, 11045L,
        11800L, 11900L, 11999L,
        # Named defoliators (12xxx)
        12038L, 12039L, 12040L, 12041L, 12083L, 12089L, 12096L, 12197L,
        12800L, 12900L,
        # Sucking insects (14xxx)
        14003L, 14004L, 14016L, 14800L, 14900L,
        # Boring insects (15xxx)
        15082L, 15087L, 15090L, 15800L, 15900L,
        # Root/butt diseases (21xxx)
        21001L, 21010L, 21014L, 21017L, 21019L, 21028L, 21800L, 21900L,
        # Cankers & rusts (22xxx, 26xxx)
        22023L, 22042L, 22086L, 22300L,
        26001L, 26009L, 26800L,
        # Other disease (24xxx, 25xxx)
        24021L, 24022L, 24031L, 24800L,
        25040L, 25043L, 25800L
      ),
      agent_label = c(
        "General insects", "Bark beetles", "Defoliators", "Chewing insects",
        "Sucking insects (adelgids/scales/aphids)", "Boring insects",
        "Seed/cone insects", "Gallmakers", "Insect predators",
        "General diseases", "Biotic damage", "Root/butt diseases",
        "Cankers (non-rust)", "Stem decay", "Parasitic/epiphytic plants",
        "Decline complexes/dieback/wilts", "Foliage diseases",
        "Stem rusts", "Broom rusts",
        "Fire", "Wildfire", "Human-caused fire", "Crown fire", "Ground fire",
        "Wild animals", "Domestic animals", "Abiotic damage",
        "Competition", "Human activities", "Harvest",
        "Multi-damage insect/disease complex", "Other damages", "Unknown",
        # Bark beetles
        "Southern pine beetle", "Mountain pine beetle", "Douglas-fir beetle",
        "Spruce beetle", "Eastern larch beetle", "Pinon ips",
        "Southern pine engraver", "Pine engraver", "Small European elm bark beetle",
        "Other bark beetle (known)", "Unknown bark beetle", "Western bark beetle complex",
        # Defoliators
        "Spruce budworm", "Western pine budworm", "Western spruce budworm",
        "Jack pine budworm", "Hemlock looper", "Gypsy moth",
        "Forest tent caterpillar", "Winter moth",
        "Other defoliator (known)", "Unknown defoliator",
        # Sucking insects
        "Balsam woolly adelgid", "Hemlock woolly adelgid", "Beech scale",
        "Other sucking insect (known)", "Unknown sucking insect",
        # Boring insects
        "Asian longhorned beetle", "Emerald ash borer", "Sirex woodwasp",
        "Other boring insect (known)", "Unknown boring insect",
        # Root/butt diseases
        "Armillaria root disease", "Heterobasidion root disease",
        "Black stain root disease", "Laminated root rot",
        "Phytophthora root rot / littleleaf disease", "Sudden oak death",
        "Other root/butt disease (known)", "Unknown root/butt disease",
        # Cankers & rusts
        "Chestnut blight", "Beech bark disease",
        "Thousand cankers disease", "Other canker (known)",
        "White pine blister rust", "Fusiform rust", "Other stem rust (known)",
        # Other disease
        "Oak wilt", "Dutch elm disease", "Laurel wilt", "Other decline/wilt (known)",
        "Dothistroma needle blight", "Swiss needle cast", "Other foliage disease (known)"
      ),
      agent_category = c(
        "insects", "insects", "insects", "insects", "insects", "insects",
        "insects", "insects", "insects",
        "disease", "disease", "disease", "disease", "disease", "disease",
        "disease", "disease", "disease", "disease",
        "fire", "fire", "fire", "fire", "fire",
        "animal", "animal", "abiotic", "competition", "human", "human",
        "complex", "other", "unknown",
        # Bark beetles
        rep("bark beetles", 12),
        # Defoliators
        rep("defoliators", 10),
        # Sucking insects
        rep("sucking insects", 5),
        # Boring insects
        rep("boring insects", 5),
        # Root/butt diseases
        rep("root/butt disease", 8),
        # Cankers & rusts
        rep("canker/rust", 7),
        # Other disease
        rep("foliage/wilt disease", 7)
      )
    )
    setkey(ref_damage_agent, DAMAGE_AGENT_CD)

    all_da <- da_ds |> collect() |> as.data.table()
    setkey(all_da, DAMAGE_AGENT_CD)
    # Left join: keep all records, label known codes, leave others as NA
    all_da <- ref_damage_agent[all_da, on = "DAMAGE_AGENT_CD"]

    write_parquet(as_tibble(all_da), out_damage_ag, compression = "snappy")
    cat(glue("  plot_damage_agents: {format(nrow(all_da), big.mark=',')} rows -> ",
             "{file_size(out_damage_ag)}\n\n"))
    rm(all_da); gc(verbose = FALSE)
  }
}

# ------------------------------------------------------------------------------
# Step 7: plot_exclusion_flags
# Per-plot flags for common analysis filters:
#   pct_forested         - proportion of plot area in forested conditions
#                          (COND_STATUS_CD == 1); useful for restricting analyses
#                          to forest-dominated plots (e.g. pct_forested >= 0.5)
#   exclude_nonforest    - any condition has COND_STATUS_CD == 5 ("nonsampled,
#                          possibility of forest land" per FIADB v9.4 §2.5.9).
#                          These are portions of accessible forest land plots that
#                          could NOT be measured (denied access, hazard, etc.);
#                          the reason is recorded in COND_NONSAMPLE_REASN_CD.
#                          Flag name is a misnomer (code 5 IS forest land, not
#                          non-forest) but kept for backward compatibility.
#                          Note: FIA samples all land types; COND_STATUS_CD 2/3/4
#                          (bare non-forest, water) are excluded via pct_forested.
#   exclude_human_dist   - any DSTRBCD1/2/3 == 80 (human-induced disturbance)
#   exclude_harvest      - any TRTCD1/2/3 == 10 (cutting treatment, condition-level)
#   exclude_harvest_agent- any tree has AGENTCD 80-89 (incidental harvest, tree-level
#                          cause-of-death; requires harvest_flags parquets from
#                          03_extract_trees.R)
#   exclude_any          - OR of all four flags above
#   has_fire             - any DSTRBCD1/2/3 %in% c(30,31,32)
#   has_insect           - any DSTRBCD1/2/3 %in% c(10,11,12)
#
# Note: TRTCD columns require re-running 03_extract_trees.R --force-cond.
# Note: exclude_harvest_agent requires harvest_flags parquets from 03_extract_trees.R.
# ------------------------------------------------------------------------------

cat("Step 7: plot_exclusion_flags\n")
out_excl_flags <- file.path(out_dir, "plot_exclusion_flags.parquet")

if (file_exists(out_excl_flags)) {
  cat(glue("  Already exists ({file_size(out_excl_flags)}) - skipping\n\n"))
} else if (is.null(cond_ds)) {
  cat("  No cond parquets found. Run 03_extract_trees.R first.\n\n")
} else {

  # Collect condition table (need STATECD, COND_STATUS_CD, CONDPROP_UNADJ, DSTRBCD*, TRTCD*)
  needed_cols <- c("PLT_CN", "INVYR", "STATECD", "CONDID",
                   "COND_STATUS_CD", "CONDPROP_UNADJ",
                   "DSTRBCD1", "DSTRBCD2", "DSTRBCD3",
                   "TRTCD1", "TRTCD2", "TRTCD3")

  # Select only columns that exist in the dataset (TRTCD may be absent on older runs)
  available_cols <- intersect(needed_cols, names(cond_ds))
  has_trtcd <- all(c("TRTCD1", "TRTCD2", "TRTCD3") %in% available_cols)
  if (!has_trtcd) {
    cat("  Warning: TRTCD1/2/3 not found in cond parquets.\n")
    cat("  Re-run 03_extract_trees.R to capture treatment codes.\n")
    cat("  exclude_harvest will be NA until then.\n")
  }

  cond_excl <- cond_ds |>
    select(all_of(available_cols)) |>
    collect() |>
    as.data.table()

  # --- Condition-level flags (one row per PLT_CN x INVYR x CONDID) ---

  # COND_STATUS_CD == 5: "Nonsampled, possibility of forest land" (FIADB v9.4 §2.5.9).
  # These are unsampled portions of accessible forest land plots — crew was denied
  # access, faced a hazard, etc.  NOT "non-forest land with trees" (that is code 2).
  # Flagging these is reasonable (no data available), but the flag name is a misnomer.
  # Codes 2/3/4 (bare non-forest, water) are handled by filtering on pct_forested.
  cond_excl[, is_forested  := COND_STATUS_CD == 1L]
  cond_excl[, cond5        := COND_STATUS_CD == 5L]

  # Human-induced disturbance (any slot)
  cond_excl[, human_dist := (
    (!is.na(DSTRBCD1) & DSTRBCD1 == 80L) |
    (!is.na(DSTRBCD2) & DSTRBCD2 == 80L) |
    (!is.na(DSTRBCD3) & DSTRBCD3 == 80L)
  )]

  # Fire (DSTRBCD 30/31/32 in any slot)
  cond_excl[, fire := (
    (!is.na(DSTRBCD1) & DSTRBCD1 %in% c(30L, 31L, 32L)) |
    (!is.na(DSTRBCD2) & DSTRBCD2 %in% c(30L, 31L, 32L)) |
    (!is.na(DSTRBCD3) & DSTRBCD3 %in% c(30L, 31L, 32L))
  )]

  # Insect damage (DSTRBCD 10/11/12 in any slot)
  cond_excl[, insect := (
    (!is.na(DSTRBCD1) & DSTRBCD1 %in% c(10L, 11L, 12L)) |
    (!is.na(DSTRBCD2) & DSTRBCD2 %in% c(10L, 11L, 12L)) |
    (!is.na(DSTRBCD3) & DSTRBCD3 %in% c(10L, 11L, 12L))
  )]

  # Cutting treatment (TRTCD 10, condition-level); NA if columns absent
  if (has_trtcd) {
    cond_excl[, harvest_trt := (
      (!is.na(TRTCD1) & TRTCD1 == 10L) |
      (!is.na(TRTCD2) & TRTCD2 == 10L) |
      (!is.na(TRTCD3) & TRTCD3 == 10L)
    )]
  } else {
    cond_excl[, harvest_trt := NA]
  }

  # --- Aggregate to plot x INVYR level ---
  plot_flags <- cond_excl[, .(
    STATECD            = STATECD[1],
    n_conditions       = .N,
    pct_forested       = sum(CONDPROP_UNADJ[is_forested], na.rm = TRUE),
    exclude_nonforest  = any(cond5, na.rm = TRUE),
    exclude_human_dist = any(human_dist, na.rm = TRUE),
    exclude_harvest    = if (has_trtcd) any(harvest_trt, na.rm = TRUE) else NA,
    has_fire           = any(fire, na.rm = TRUE),
    has_insect         = any(insect, na.rm = TRUE)
  ), by = .(PLT_CN, INVYR)]

  # --- Join AGENTCD-based harvest flag (tree-level, AGENTCD 80-89) ------------
  # Requires harvest_flags parquets produced by 03_extract_trees.R.
  harvest_flags_dir <- here(proc_fia$harvest_flags$output_dir)
  harvest_ds <- tryCatch(
    open_dataset(harvest_flags_dir, partitioning = "state"),
    error = function(e) NULL
  )
  if (!is.null(harvest_ds)) {
    hf <- harvest_ds |> select(PLT_CN, INVYR) |> collect() |> as.data.table()
    hf <- unique(hf)
    hf[, exclude_harvest_agent := TRUE]
    plot_flags <- hf[plot_flags, on = .(PLT_CN, INVYR)]
    plot_flags[is.na(exclude_harvest_agent), exclude_harvest_agent := FALSE]
    cat(glue("  AGENTCD harvest flags joined: {format(sum(plot_flags$exclude_harvest_agent), big.mark=',')} plots affected\n"))
  } else {
    plot_flags[, exclude_harvest_agent := NA]
    cat("  Note: harvest_flags parquets not found; exclude_harvest_agent will be NA.\n")
    cat("  Re-run 03_extract_trees.R to generate them.\n")
  }

  plot_flags[, exclude_any :=
    exclude_nonforest | exclude_human_dist |
    (!is.na(exclude_harvest)       & exclude_harvest) |
    (!is.na(exclude_harvest_agent) & exclude_harvest_agent)]

  write_parquet(as_tibble(plot_flags), out_excl_flags, compression = "snappy")

  n_total <- nrow(plot_flags)
  n_excl  <- sum(plot_flags$exclude_any, na.rm = TRUE)
  pct_nf  <- round(100 * mean(plot_flags$exclude_nonforest, na.rm = TRUE), 1)
  cat(glue("  plot_exclusion_flags: {format(n_total, big.mark=',')} plot x INVYR rows -> ",
           "{file_size(out_excl_flags)}\n"))
  cat(glue("  exclude_nonforest (COND_STATUS_CD==5) rate: {pct_nf}% of plots\n"))
  cat(glue("  exclude_any rate: {round(100*n_excl/n_total,1)}% of plots\n\n"))
  rm(cond_excl, plot_flags); gc(verbose = FALSE)
}

# ------------------------------------------------------------------------------
# Done
# ------------------------------------------------------------------------------

cat("FIA summaries complete.\n\n")
cat("Outputs:\n")
for (f in c(out_tree_metrics, out_seed_metrics, out_mort_metrics,
            out_cond_metrics, out_cond_metadata, out_seed_species,
            out_disturb_class,
            out_disturb, out_treat, out_damage_ag, out_excl_flags)) {
  if (file_exists(f)) cat(glue("  {basename(f)}: {file_size(f)}\n"))
}
cat("\nRead with:\n")
cat("  arrow::read_parquet('05_fia/data/processed/summaries/plot_tree_metrics.parquet')\n")
cat("  arrow::read_parquet('05_fia/data/processed/summaries/plot_seedling_species.parquet')\n")
cat("  arrow::read_parquet('05_fia/data/processed/summaries/plot_disturbance_classification.parquet')\n")
cat("  arrow::read_parquet('05_fia/data/processed/summaries/plot_disturbance_history.parquet')\n")
cat("  arrow::read_parquet('05_fia/data/processed/summaries/plot_treatment_history.parquet')\n")
cat("  arrow::read_parquet('05_fia/data/processed/summaries/plot_damage_agents.parquet')\n")
cat("  arrow::read_parquet('05_fia/data/processed/summaries/plot_exclusion_flags.parquet')\n")
