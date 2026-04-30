# ------------------------------------------------------------------------------
# build_tree_metrics
# ------------------------------------------------------------------------------

build_tree_metrics <- function(out_dir, proc_fia, states, cond_ds) {
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

  out_tree_metrics
}

