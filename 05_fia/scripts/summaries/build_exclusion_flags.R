# ------------------------------------------------------------------------------
# build_exclusion_flags
# ------------------------------------------------------------------------------

build_exclusion_flags <- function(out_dir, proc_fia, cond_ds, cond_dir = NULL) {
  # Step 7: plot_exclusion_flags
  # Per-plot flags for common analysis filters:
  #   pct_forested         - proportion of plot area in forested conditions
  #                          (COND_STATUS_CD == 1); useful for restricting analyses
  #                          to forest-dominated plots (e.g. pct_forested >= 0.5)
  #   exclude_nonforest    - any condition has COND_STATUS_CD == 5 ("nonsampled,
  #                          possibility of forest land" per FIADB v9.4 Â§2.5.9).
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

  rb <- fia_should_rebuild(
    out_excl_flags,
    input_paths = c(
      if (!is.null(cond_dir)) cond_dir else character(0),
      here(proc_fia$harvest_flags$output_dir)
    ),
    required_cols = c("PLT_CN", "INVYR", "pct_forested", "exclude_any"),
    label = "plot_exclusion_flags"
  )
  if (!rb$rebuild) {
    cat(glue("  Up to date ({rb$reason}, {file_size(out_excl_flags)}) - skipping\n\n"))
  } else if (is.null(cond_ds)) {
    cat("  No cond parquets found. Run 03_extract_trees.R first.\n\n")
  } else {
    if (file_exists(out_excl_flags)) cat(glue("  Rebuilding ({rb$reason})\n"))
  
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
  
    # COND_STATUS_CD == 5: "Nonsampled, possibility of forest land" (FIADB v9.4 Â§2.5.9).
    # These are unsampled portions of accessible forest land plots â€” crew was denied
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
  
    write_parquet_atomic(as_tibble(plot_flags), out_excl_flags, compression = "snappy")
  
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

  out_excl_flags
}

