# ------------------------------------------------------------------------------
# build_seedling_species
# ------------------------------------------------------------------------------

build_seedling_species <- function(out_dir, proc_fia, out_cond_metadata) {
  # Step 4c: plot_seedling_species
  # Analysis-ready species-level seedlings joined to condition metadata.
  # ------------------------------------------------------------------------------
  
  cat("Step 4c: plot_seedling_species\n")
  out_seed_species <- file.path(out_dir, "plot_seedling_species.parquet")

  seed_source_files <- list.files(
    here(proc_fia$seedlings$output_dir),
    pattern = "[.]parquet$",
    recursive = TRUE,
    full.names = TRUE
  )
  source_newer <- file_exists(out_seed_species) &&
    (
      (
        length(seed_source_files) > 0 &&
          max(file.info(seed_source_files)$mtime, na.rm = TRUE) >
            file.info(out_seed_species)$mtime
      ) ||
        file.info(out_cond_metadata)$mtime >
          file.info(out_seed_species)$mtime
    )

  if (file_exists(out_seed_species) && !source_newer &&
      !fia_force_requested("plot_seedling_species")) {
    cat(glue("  Already exists ({file_size(out_seed_species)}) - skipping\n\n"))
  } else if (!file_exists(out_cond_metadata)) {
    cat("  plot_condition_metadata.parquet not found. Run Step 4b first.\n\n")
  } else {
    if (source_newer) {
      cat("  Seedling extracts or condition metadata are newer - rebuilding\n")
    }
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
        write_parquet_atomic(as_tibble(seed_species), out_seed_species, compression = "snappy")
        cat(glue("  plot_seedling_species: {format(nrow(seed_species), big.mark=',')} rows -> ",
                 "{file_size(out_seed_species)}\n\n"))
  
        rm(seed_species, cond_meta)
        gc(verbose = FALSE)
      }
    }
  }
  
  # ------------------------------------------------------------------------------

  out_seed_species
}

