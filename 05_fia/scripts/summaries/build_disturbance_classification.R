# ------------------------------------------------------------------------------
# build_disturbance_classification
# ------------------------------------------------------------------------------

build_disturbance_classification <- function(out_dir, out_cond_metadata) {
  # Step 4d: plot_disturbance_classification
  # Condition-level disturbance classes and control eligibility for analysis.
  # ------------------------------------------------------------------------------
  
  cat("Step 4d: plot_disturbance_classification\n")
  out_disturb_class <- file.path(out_dir, "plot_disturbance_classification.parquet")

  source_newer <- file_exists(out_disturb_class) &&
    file.info(out_cond_metadata)$mtime >
      file.info(out_disturb_class)$mtime
  forced <- fia_force_requested("plot_disturbance_classification")

  if (file_exists(out_disturb_class) && !source_newer && !forced) {
    cat(glue("  Already exists ({file_size(out_disturb_class)}) - skipping\n\n"))
  } else if (!file_exists(out_cond_metadata)) {
    cat("  plot_condition_metadata.parquet not found. Run Step 4b first.\n\n")
  } else {
    if (source_newer) {
      cat("  Condition metadata is newer than classification - rebuilding\n")
    }
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
    # Start with the primary class, then collapse selected classes to broader groups.
    cond_class[, disturbance_class := disturbance_class_primary]
    cond_class[disturbance_class_primary %in% c("crown_fire", "fire"), disturbance_class := "fire"]
    cond_class[disturbance_class_primary %in% c("wind", "drought", "other_weather"), disturbance_class := "weather"]
    cond_class[
      disturbance_class_primary %in% c("animal", "vegetation", "geologic", "other_unknown", "other_recorded"),
      disturbance_class := "other"
    ]
  
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

    # Type-specific latest/earliest years, separate from the any-type
    # disturbance_year_latest/earliest above. These support checks for whether
    # a survey interval brackets a specific disturbance type (e.g. fire), which
    # the any-type field cannot answer if a different, more recent disturbance
    # type occurred on the same condition.
    valid_year_from_code_in_set <- function(code, year, code_set) {
      y <- as.integer(year)
      y[is.na(code) | !(code %in% code_set) | is.na(y) | y %in% c(0L, 9999L)] <- NA_integer_
      y
    }
    fire_code_set <- c(30L, 31L, 32L)
    insect_code_set <- c(10L, 11L, 12L)
    valid_fire_year_cols <- paste0("fire_year_valid_", seq_along(dist_year_cols))
    valid_insect_year_cols <- paste0("insect_year_valid_", seq_along(dist_year_cols))
    for (j in seq_along(dist_year_cols)) {
      cond_class[, (valid_fire_year_cols[j]) := valid_year_from_code_in_set(get(dist_code_cols[j]), get(dist_year_cols[j]), fire_code_set)]
      cond_class[, (valid_insect_year_cols[j]) := valid_year_from_code_in_set(get(dist_code_cols[j]), get(dist_year_cols[j]), insect_code_set)]
    }
    latest_fire <- do.call(pmax, c(cond_class[, ..valid_fire_year_cols], na.rm = TRUE))
    earliest_fire <- do.call(pmin, c(cond_class[, ..valid_fire_year_cols], na.rm = TRUE))
    latest_insect <- do.call(pmax, c(cond_class[, ..valid_insect_year_cols], na.rm = TRUE))
    earliest_insect <- do.call(pmin, c(cond_class[, ..valid_insect_year_cols], na.rm = TRUE))
    latest_fire[!is.finite(latest_fire)] <- NA_integer_
    earliest_fire[!is.finite(earliest_fire)] <- NA_integer_
    latest_insect[!is.finite(latest_insect)] <- NA_integer_
    earliest_insect[!is.finite(earliest_insect)] <- NA_integer_
    cond_class[, fire_disturbance_year_latest := as.integer(latest_fire)]
    cond_class[, fire_disturbance_year_earliest := as.integer(earliest_fire)]
    cond_class[, insect_disturbance_year_latest := as.integer(latest_insect)]
    cond_class[, insect_disturbance_year_earliest := as.integer(earliest_insect)]
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
  
    # Keep the official condition status separate from the optional whole-plot
    # forest-dominance threshold. The analysis response is condition-level, so
    # a condition recorded by FIA as forest remains eligible even when other
    # portions of the same plot visit are nonforest.
    cond_class[, is_forest_dominated_plot :=
      !is.na(pct_forested) & pct_forested >= 0.5]
    cond_class[, is_forested_analysis_condition := is_forested_condition]

    # Define analysis/control gates explicitly so matching code does not rewrite them.
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
      is_human_or_harvest, "human_or_harvest",
      has_any_treatment, "treated",
      has_any_recorded_disturbance, "recorded_disturbance",
      default = "control_candidate"
    )]
  
    # Drop temporary valid-year columns before writing the analysis product.
    cond_class[, c(
      valid_dstr_year_cols, valid_trt_year_cols, valid_cut_year_cols,
      valid_fire_year_cols, valid_insect_year_cols
    ) := NULL]
  
    # Keep identifiers, raw codes, and derived analysis fields together.
    preferred_cols <- c(
      "stable_plot_id", "PLT_CN", "INVYR", "STATECD", "UNITCD", "COUNTYCD", "PLOT",
      "PREV_PLT_CN", "state", "region_east_west", "region_source",
      "CONDID", "COND_STATUS_CD", "CONDPROP_UNADJ", "pct_forested",
      "is_forested_condition", "is_forest_dominated_plot",
      "is_forested_analysis_condition",
      "LAT", "LON", "ELEV", "FORTYPCD", "forest_type_label", "forest_type_group",
      "DSTRBCD1", "DSTRBCD2", "DSTRBCD3", "DSTRBYR1", "DSTRBYR2", "DSTRBYR3",
      "TRTCD1", "TRTCD2", "TRTCD3", "TRTYR1", "TRTYR2", "TRTYR3",
      "has_any_recorded_disturbance", "has_any_treatment", "has_cutting_treatment",
      "is_human_or_harvest", "has_crown_fire_condition", natural_flag_cols, "has_human_dist_condition",
      "has_unknown_other_condition", "n_natural_disturbance_classes",
      "is_multiple_natural_disturbance", "natural_disturbance_primary",
      "is_natural_disturbance", "disturbance_class_primary", "disturbance_class",
      "is_high_severity_proxy", "high_severity_proxy_type",
      "disturbance_year_latest", "disturbance_year_earliest",
      "fire_disturbance_year_latest", "fire_disturbance_year_earliest",
      "insect_disturbance_year_latest", "insect_disturbance_year_earliest",
      "treatment_year_latest", "cutting_year_latest",
      "time_since_disturbance", "time_since_treatment", "time_since_cutting",
      "has_continuous_disturbance_year", "has_continuous_treatment_year",
      "is_control_candidate", "is_natural_disturbance_candidate",
      "disturbed_vs_control", "control_eligibility_reason"
    )
    out_cols <- intersect(preferred_cols, names(cond_class))
    cond_class <- cond_class[, ..out_cols]
  
    # Write the reusable disturbance backbone for matching, filters, and model inputs.
    write_parquet_atomic(as_tibble(cond_class), out_disturb_class, compression = "snappy")
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

  out_disturb_class
}

