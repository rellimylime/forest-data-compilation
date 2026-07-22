# ------------------------------------------------------------------------------
# build_condition_metadata
# ------------------------------------------------------------------------------

build_condition_metadata <- function(out_dir, cond_ds) {
  # Step 4b: plot_condition_metadata
  # Condition-level metadata with stable plot IDs, forest type labels, and flags.
  # ------------------------------------------------------------------------------
  
  cat("Step 4b: plot_condition_metadata\n")
  out_cond_metadata <- file.path(out_dir, "plot_condition_metadata.parquet")

  cond_source_files <- list.files(
    here("05_fia/data/processed/cond"),
    pattern = "[.]parquet$",
    recursive = TRUE,
    full.names = TRUE
  )
  source_newer <- file_exists(out_cond_metadata) &&
    length(cond_source_files) > 0 &&
    max(file.info(cond_source_files)$mtime, na.rm = TRUE) >
      file.info(out_cond_metadata)$mtime
  forced <- fia_force_requested("plot_condition_metadata")

  if (file_exists(out_cond_metadata) && !source_newer && !forced) {
    cat(glue("  Already exists ({file_size(out_cond_metadata)}) - skipping\n\n"))
  } else if (is.null(cond_ds)) {
    cat("  No cond parquets found. Run 03_extract_trees.R --force-cond first.\n\n")
  } else {
    if (source_newer) {
      cat("  Condition extracts are newer than metadata - rebuilding\n")
    }
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
      write_parquet_atomic(as_tibble(cond_meta), out_cond_metadata, compression = "snappy")
      cat(glue("  plot_condition_metadata: {format(nrow(cond_meta), big.mark=',')} rows -> ",
               "{file_size(out_cond_metadata)}\n\n"))
  
      rm(cond_meta, forested)
      if (exists("ref_ft")) rm(ref_ft)
  
      # Release the large condition metadata table before later summary steps run.
      gc(verbose = FALSE)
    }
  }
  
  # ------------------------------------------------------------------------------

  out_cond_metadata
}

