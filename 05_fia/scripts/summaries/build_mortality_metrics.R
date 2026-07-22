# ------------------------------------------------------------------------------
# build_mortality_metrics
# ------------------------------------------------------------------------------

build_mortality_metrics <- function(out_dir, proc_fia) {
  # Step 3: plot_mortality_metrics
  # Pass-through of per-state mortality parquets into a single national file
  # ------------------------------------------------------------------------------
  
  cat("Step 3: plot_mortality_metrics\n")
  out_mort_metrics <- file.path(out_dir, "plot_mortality_metrics.parquet")

  rb <- fia_should_rebuild(
    out_mort_metrics,
    input_paths = here(proc_fia$mortality$output_dir),
    required_cols = c("PLT_CN", "INVYR", "AGENTCD", "component_type", "tpamort_per_acre"),
    label = "plot_mortality_metrics"
  )
  if (!rb$rebuild) {
    cat(glue("  Up to date ({rb$reason}, {file_size(out_mort_metrics)}) - skipping\n\n"))
  } else {
    if (file_exists(out_mort_metrics)) cat(glue("  Rebuilding ({rb$reason})\n"))
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
      write_parquet_atomic(as_tibble(all_mort), out_mort_metrics, compression = "snappy")
      cat(glue("  plot_mortality_metrics: {format(nrow(all_mort), big.mark=',')} rows -> ",
               "{file_size(out_mort_metrics)}\n\n"))
      rm(all_mort); gc(verbose = FALSE)
    }
  }
  
  # ------------------------------------------------------------------------------

  out_mort_metrics
}

