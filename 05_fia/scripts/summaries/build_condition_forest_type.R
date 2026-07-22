# ------------------------------------------------------------------------------
# build_condition_forest_type
# ------------------------------------------------------------------------------

build_condition_forest_type <- function(out_dir, cond_ds, cond_dir = NULL) {
  # Step 4: plot_cond_fortypcd
  # Pass-through of per-state cond parquets into a single national file
  # ------------------------------------------------------------------------------

  cat("Step 4: plot_cond_fortypcd\n")
  out_cond_metrics <- file.path(out_dir, "plot_cond_fortypcd.parquet")

  rb <- fia_should_rebuild(
    out_cond_metrics,
    input_paths = if (!is.null(cond_dir)) cond_dir else character(0),
    required_cols = c("PLT_CN", "INVYR", "CONDID", "FORTYPCD", "CONDPROP_UNADJ"),
    label = "plot_cond_fortypcd"
  )
  if (!rb$rebuild) {
    cat(glue("  Up to date ({rb$reason}, {file_size(out_cond_metrics)}) - skipping\n\n"))
  } else if (is.null(cond_ds)) {
    cat("  No cond parquets found. Run 03_extract_trees.R first.\n\n")
  } else {
    if (file_exists(out_cond_metrics)) cat(glue("  Rebuilding ({rb$reason})\n"))
    # Pass through condition rows nationally so analysts can inspect raw condition fields.
    all_cond <- cond_ds |> collect() |> as.data.table()

    # Write the condition/forest-type table before building derived metadata products.
    write_parquet_atomic(as_tibble(all_cond), out_cond_metrics, compression = "snappy")
    cat(glue("  plot_cond_fortypcd: {format(nrow(all_cond), big.mark=',')} rows -> ",
             "{file_size(out_cond_metrics)}\n\n"))
    rm(all_cond); gc(verbose = FALSE)
  }
  
  # ------------------------------------------------------------------------------

  out_cond_metrics
}

