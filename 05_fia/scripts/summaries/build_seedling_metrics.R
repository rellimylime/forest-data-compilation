# ------------------------------------------------------------------------------
# build_seedling_metrics
# ------------------------------------------------------------------------------

build_seedling_metrics <- function(out_dir, proc_fia, states) {
  # Step 2: plot_seedling_metrics
  # ------------------------------------------------------------------------------
  
  cat("Step 2: plot_seedling_metrics\n")
  out_seed_metrics <- file.path(out_dir, "plot_seedling_metrics.parquet")

  rb <- fia_should_rebuild(
    out_seed_metrics,
    input_paths = here(proc_fia$seedlings$output_dir),
    required_cols = c("PLT_CN", "INVYR", "treecount_total", "n_species_seedling"),
    label = "plot_seedling_metrics"
  )
  if (!rb$rebuild) {
    cat(glue("  Up to date ({rb$reason}, {file_size(out_seed_metrics)}) - skipping\n\n"))
  } else {
    if (file_exists(out_seed_metrics)) cat(glue("  Rebuilding ({rb$reason})\n"))
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
      write_parquet_atomic(as_tibble(all_seed), out_seed_metrics, compression = "snappy")
      cat(glue("  plot_seedling_metrics: {format(nrow(all_seed), big.mark=',')} rows -> ",
               "{file_size(out_seed_metrics)}\n\n"))
      rm(all_seed, results); gc(verbose = FALSE)
    }
  }
  
  # ------------------------------------------------------------------------------

  out_seed_metrics
}

