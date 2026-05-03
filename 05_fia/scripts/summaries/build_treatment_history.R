# ------------------------------------------------------------------------------
# build_treatment_history
# ------------------------------------------------------------------------------

build_treatment_history <- function(out_dir, cond_ds) {
  # Step 5b: plot_treatment_history
  # Pivot TRTCD1/2/3 + TRTYR1/2/3 to long format with human-readable labels.
  # Mirrors the reference code's trt_check but covers ALL treatment types (not
  # just TRTCD==10), and includes treatment year so temporal filtering is possible.
  # One row per condition Ã— treatment slot where TRTCD != 0.
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

      # Keep raw FIADB TRTYR, but add a calendar-year field for plotting and
      # time-window analyses. Values like 1/0/9999 are non-calendar sentinels;
      # years after INVYR are not interpretable as prior treatment timing.
      treat_long[, TRTYR_raw := as.integer(TRTYR)]
      treat_long[, TRTYR_calendar := as.integer(TRTYR)]
      treat_long[
        is.na(TRTYR_calendar) |
          TRTYR_calendar < 1900L |
          TRTYR_calendar == 9999L |
          TRTYR_calendar > INVYR,
        TRTYR_calendar := NA_integer_
      ]
      treat_long[, treatment_year_status := fcase(
        is.na(TRTYR_raw), "missing",
        TRTYR_raw == 9999L, "continuous_or_unknown",
        TRTYR_raw < 1900L, "non_calendar_sentinel",
        TRTYR_raw > INVYR, "after_inventory_year",
        default = "calendar_year"
      )]
  
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

  out_treat
}

