# Shared helpers for condition-aware forest-community and disturbance products.

suppressPackageStartupMessages({
  library(data.table)
})

forest_analysis_required_condition_cols <- c(
  "PLT_CN", "INVYR", "CONDID", "stable_plot_id",
  "COND_STATUS_CD", "CONDPROP_UNADJ"
)

forest_assert_columns <- function(x, required, label) {
  missing <- setdiff(required, names(x))
  if (length(missing) > 0L) {
    stop(label, " is missing required column(s): ", paste(missing, collapse = ", "))
  }
  invisible(TRUE)
}

forest_assert_unique <- function(x, keys, label) {
  duplicate_keys <- as.data.table(x)[, .N, by = keys][N > 1L]
  if (nrow(duplicate_keys) > 0L) {
    stop(
      label, " has ", format(nrow(duplicate_keys), big.mark = ","),
      " duplicate key combination(s) at ", paste(keys, collapse = " x ")
    )
  }
  invisible(TRUE)
}

fia_valid_disturbance_year <- function(year, inventory_year) {
  year <- as.integer(year)
  inventory_year <- as.integer(inventory_year)
  !is.na(year) & year >= 1000L & year != 9999L &
    (is.na(inventory_year) | year <= inventory_year)
}

fia_disturbance_year_status <- function(
  year,
  measurement_year_upper = NA_integer_,
  inventory_year = NA_integer_
) {
  year <- as.integer(year)
  measurement_year_upper <- as.integer(measurement_year_upper)
  inventory_year <- as.integer(inventory_year)
  # Prefer the actual measurement year; use INVYR only when timing is less precise.
  upper_year <- fifelse(
    !is.na(measurement_year_upper),
    measurement_year_upper,
    inventory_year
  )

  fcase(
    is.na(year), "date_unavailable",
    year == 9999L, "continuous_or_unknown",
    year < 1000L | year > 9998L, "invalid_year_code",
    !is.na(upper_year) & year > upper_year, "post_measurement_year",
    default = "valid"
  )
}

build_fia_fire_slot_evidence <- function(
  conditions,
  visit_context = NULL,
  fire_codes = c(30L, 31L, 32L)
) {
  out <- copy(as.data.table(conditions))
  keys <- c("PLT_CN", "INVYR", "CONDID")
  forest_assert_columns(
    out,
    c(keys, paste0("DSTRBCD", 1:3), paste0("DSTRBYR", 1:3)),
    "FIA conditions"
  )
  forest_assert_unique(out, keys, "FIA conditions")

  # Start with INVYR as a fallback for visits lacking measurement context.
  out[, measurement_year_upper__ := NA_integer_]
  out[, fire_year_validation_source__ := fifelse(
    is.na(INVYR),
    "unavailable",
    "INVYR_fallback"
  )]

  # Use the most precise available measurement timing to validate disturbance years.
  if (!is.null(visit_context)) {
    context <- copy(as.data.table(visit_context))
    forest_assert_columns(context, c("PLT_CN", "INVYR"), "FIA visit context")
    forest_assert_unique(context, c("PLT_CN", "INVYR"), "FIA visit context")

    if (!"MEASYEAR" %in% names(context)) context[, MEASYEAR := NA_integer_]
    if (!"measurement_date_upper" %in% names(context)) {
      context[, measurement_date_upper := as.IDate(NA)]
    }
    context[, measurement_year_from_bound__ := suppressWarnings(
      as.integer(format(as.IDate(measurement_date_upper), "%Y"))
    )]
    context[, measurement_year_upper__ := fcoalesce(
      as.integer(MEASYEAR),
      measurement_year_from_bound__,
      as.integer(INVYR)
    )]
    context[, fire_year_validation_source__ := fcase(
      !is.na(MEASYEAR), "MEASYEAR",
      !is.na(measurement_year_from_bound__), "measurement_date_upper",
      !is.na(INVYR), "INVYR_fallback",
      default = "unavailable"
    )]
    out <- merge(
      out,
      context[, .(
        PLT_CN,
        INVYR,
        measurement_year_upper_context__ = measurement_year_upper__,
        fire_year_validation_source_context__ = fire_year_validation_source__
      )],
      by = c("PLT_CN", "INVYR"),
      all.x = TRUE,
      sort = FALSE
    )
    out[, measurement_year_upper__ := fcoalesce(
      measurement_year_upper_context__,
      as.integer(INVYR)
    )]
    out[, fire_year_validation_source__ := fcoalesce(
      fire_year_validation_source_context__,
      fire_year_validation_source__
    )]
    out[, c(
      "measurement_year_upper_context__",
      "fire_year_validation_source_context__"
    ) := NULL]
  } else {
    out[, measurement_year_upper__ := as.integer(INVYR)]
  }

  id_cols <- c(
    keys,
    intersect(
      c("stable_plot_id", "state", "is_forested_condition"),
      names(out)
    ),
    "measurement_year_upper__",
    "fire_year_validation_source__"
  )
  # Melt codes and years together so slot 1 always stays paired with year 1.
  evidence <- melt(
    out,
    id.vars = id_cols,
    measure.vars = list(
      disturbance_code = paste0("DSTRBCD", 1:3),
      disturbance_year = paste0("DSTRBYR", 1:3)
    ),
    variable.name = "disturbance_slot",
    variable.factor = FALSE
  )
  # Keep fire classes only after the same-slot pairing is established.
  evidence <- evidence[
    !is.na(disturbance_code) &
      as.integer(disturbance_code) %in% as.integer(fire_codes)
  ]
  evidence[, disturbance_slot := as.integer(disturbance_slot)]
  if (nrow(evidence) == 0L) {
    evidence[, `:=`(
      disturbance_code = as.integer(disturbance_code),
      disturbance_year = as.integer(disturbance_year),
      fire_year_status = character(),
      valid_fire_year = logical(),
      fire_year_validation_source = character(),
      measurement_year_upper = integer(),
      fire_disturbance_slot_evidence = character()
    )]
    return(evidence[])
  }

  evidence[, `:=`(
    disturbance_code = as.integer(disturbance_code),
    disturbance_year = as.integer(disturbance_year),
    fire_year_status = fia_disturbance_year_status(
      disturbance_year,
      measurement_year_upper__,
      INVYR
    ),
    fire_year_validation_source = fire_year_validation_source__,
    measurement_year_upper = measurement_year_upper__
  )]
  evidence[, valid_fire_year := fire_year_status == "valid"]
  evidence[, fire_year_text := fcase(
    fire_year_status == "valid", as.character(disturbance_year),
    fire_year_status == "continuous_or_unknown", "9999",
    is.na(disturbance_year), "NA",
    default = as.character(disturbance_year)
  )]
  # Store a compact readable trace back to condition, slot, code, and year status.
  evidence[, fire_disturbance_slot_evidence := paste0(
    "cond", CONDID,
    "/slot", disturbance_slot,
    ":", disturbance_code,
    "@", fire_year_text,
    ":", fire_year_status
  )]
  evidence[, c(
    "measurement_year_upper__",
    "fire_year_validation_source__",
    "fire_year_text"
  ) := NULL]
  setorder(evidence, PLT_CN, INVYR, CONDID, disturbance_slot)
  evidence[]
}

collapse_sorted_values <- function(x) {
  x <- sort(unique(x[!is.na(x)]))
  if (length(x) == 0L) NA_character_ else paste(x, collapse = ";")
}

build_forested_condition_foundation <- function(
  conditions,
  proportion_tolerance = 0.02,
  very_small_forest_threshold = 0.05
) {
  out <- copy(as.data.table(conditions))
  forest_assert_columns(
    out,
    forest_analysis_required_condition_cols,
    "FIA condition table"
  )
  forest_assert_unique(
    out,
    c("PLT_CN", "INVYR", "CONDID"),
    "FIA condition table"
  )

  # Add absent disturbance slots so all state extracts share one schema.
  code_cols <- intersect(paste0("DSTRBCD", 1:3), names(out))
  year_cols <- intersect(paste0("DSTRBYR", 1:3), names(out))
  for (column in setdiff(paste0("DSTRBCD", 1:3), names(out))) {
    out[, (column) := NA_integer_]
  }
  for (column in setdiff(paste0("DSTRBYR", 1:3), names(out))) {
    out[, (column) := NA_integer_]
  }
  code_cols <- paste0("DSTRBCD", 1:3)
  year_cols <- paste0("DSTRBYR", 1:3)

  # FIA condition status 1 is the sole forest-membership rule.
  out[, is_forested_condition := !is.na(COND_STATUS_CD) & COND_STATUS_CD == 1L]
  out[, condition_proportion_missing := is.na(CONDPROP_UNADJ)]
  out[, condition_proportion_out_of_bounds :=
    !is.na(CONDPROP_UNADJ) & (CONDPROP_UNADJ < 0 | CONDPROP_UNADJ > 1)]

  # Calculate forest area and proportion QA once per plot visit.
  visit <- out[, .(
    n_conditions = .N,
    n_forested_conditions = sum(is_forested_condition),
    n_nonsampled_conditions = sum(COND_STATUS_CD == 5L, na.rm = TRUE),
    n_missing_condition_proportions = sum(condition_proportion_missing),
    total_condition_proportion = if (all(is.na(CONDPROP_UNADJ))) {
      NA_real_
    } else {
      sum(CONDPROP_UNADJ, na.rm = TRUE)
    },
    forested_plot_proportion = if (all(is.na(CONDPROP_UNADJ[is_forested_condition]))) {
      NA_real_
    } else {
      sum(CONDPROP_UNADJ[is_forested_condition], na.rm = TRUE)
    }
  ), by = .(PLT_CN, INVYR)]

  visit[, no_forested_conditions := n_forested_conditions == 0L]
  visit[, very_small_forested_proportion :=
    !is.na(forested_plot_proportion) &
      forested_plot_proportion > 0 &
      forested_plot_proportion < very_small_forest_threshold]
  visit[, nonsampled_zero_proportion :=
    n_nonsampled_conditions == n_conditions &
      !is.na(total_condition_proportion) &
      total_condition_proportion == 0]
  visit[, condition_proportions_not_one :=
    !nonsampled_zero_proportion &
    !is.na(total_condition_proportion) &
      abs(total_condition_proportion - 1) > proportion_tolerance]
  visit[, condition_proportion_quality_flag := fcase(
    is.na(total_condition_proportion), "all_condition_proportions_missing",
    n_missing_condition_proportions > 0L, "some_condition_proportions_missing",
    nonsampled_zero_proportion, "nonsampled_zero_proportion",
    condition_proportions_not_one, "condition_proportions_not_one",
    no_forested_conditions, "no_forested_conditions",
    very_small_forested_proportion, "very_small_forested_proportion",
    default = "ok"
  )]

  derived_visit_cols <- setdiff(names(visit), c("PLT_CN", "INVYR"))
  out[, (intersect(derived_visit_cols, names(out))) := NULL]
  out <- merge(out, visit, by = c("PLT_CN", "INVYR"), all.x = TRUE, sort = FALSE)
  # Renormalize condition area across forest only so nonforest never dilutes a metric.
  out[, forested_condition_weight := fifelse(
    is_forested_condition &
      !is.na(CONDPROP_UNADJ) &
      !is.na(forested_plot_proportion) &
      forested_plot_proportion > 0,
    CONDPROP_UNADJ / forested_plot_proportion,
    NA_real_
  )]
  out[, has_fire_condition :=
    DSTRBCD1 %in% c(30L, 31L, 32L) |
      DSTRBCD2 %in% c(30L, 31L, 32L) |
      DSTRBCD3 %in% c(30L, 31L, 32L)]
  out[, has_crown_fire_condition :=
    DSTRBCD1 %in% 32L | DSTRBCD2 %in% 32L | DSTRBCD3 %in% 32L]
  out[, has_insect_condition :=
    DSTRBCD1 %in% c(10L, 11L, 12L) |
      DSTRBCD2 %in% c(10L, 11L, 12L) |
      DSTRBCD3 %in% c(10L, 11L, 12L)]
  out[, has_disease_condition :=
    DSTRBCD1 %in% c(20L, 21L, 22L) |
      DSTRBCD2 %in% c(20L, 21L, 22L) |
      DSTRBCD3 %in% c(20L, 21L, 22L)]

  # Use a long table to build readable disturbance evidence efficiently.
  out[, condition_row_id__ := .I]
  disturbance_long <- melt(
    out,
    id.vars = c("condition_row_id__", "INVYR"),
    measure.vars = list(code = code_cols, year = year_cols),
    variable.name = "slot",
    variable.factor = FALSE
  )
  disturbance_long <- disturbance_long[!is.na(code)]
  if (nrow(disturbance_long) > 0L) {
    disturbance_long[, valid_year :=
      fia_valid_disturbance_year(year, INVYR)]
    disturbance_long[, year_text := fcase(
      valid_year, as.character(as.integer(year)),
      !is.na(year) & as.integer(year) == 9999L, "continuous_or_unknown",
      default = "date_unavailable"
    )]
    disturbance_long[, slot_text := paste0(
      "slot", slot, ":", as.integer(code), "@", year_text
    )]
    disturbance_summary <- disturbance_long[, .(
      valid_disturbance_codes =
        collapse_sorted_values(as.integer(code)),
      valid_disturbance_years =
        collapse_sorted_values(as.integer(year[valid_year])),
      valid_disturbance_slots = paste(slot_text, collapse = ";")
    ), by = condition_row_id__]
    out <- merge(
      out,
      disturbance_summary,
      by = "condition_row_id__",
      all.x = TRUE,
      sort = FALSE
    )
  } else {
    out[, `:=`(
      valid_disturbance_codes = NA_character_,
      valid_disturbance_years = NA_character_,
      valid_disturbance_slots = NA_character_
    )]
  }
  out[, condition_row_id__ := NULL]

  setorder(out, stable_plot_id, INVYR, PLT_CN, CONDID)
  out[]
}

forest_weighted_mean <- function(value, weight) {
  ok <- !is.na(value) & !is.na(weight) & weight > 0
  if (!any(ok)) return(NA_real_)
  sum(value[ok] * weight[ok]) / sum(weight[ok])
}

aggregate_forested_condition_cwm <- function(
  condition_metrics,
  forested_conditions,
  metric_cols,
  max_unmatched_condition_fraction = 0.05
) {
  metrics <- copy(as.data.table(condition_metrics))
  foundation <- copy(as.data.table(forested_conditions))
  keys <- c("PLT_CN", "INVYR", "CONDID")

  forest_assert_columns(metrics, c(keys, metric_cols), "Condition CWM table")
  forest_assert_columns(
    foundation,
    c(keys, "stable_plot_id", "is_forested_condition",
      "forested_plot_proportion", "forested_condition_weight"),
    "Forested-condition foundation"
  )
  forest_assert_unique(metrics, keys, "Condition CWM table")
  forest_assert_unique(foundation, keys, "Forested-condition foundation")

  foundation_cols <- intersect(
    c(
      keys, "stable_plot_id", "state", "STATECD", "UNITCD", "COUNTYCD", "PLOT",
      "PREV_PLT_CN", "LAT", "LON", "ELEV",
      "is_forested_condition", "forested_plot_proportion",
      "forested_condition_weight", "n_forested_conditions",
      "condition_proportion_quality_flag"
    ),
    names(foundation)
  )

  # Drop copied metadata so the current condition foundation remains authoritative.
  shadowed <- setdiff(intersect(foundation_cols, names(metrics)), keys)
  if (length(shadowed) > 0L) {
    metrics[, (shadowed) := NULL]
  }

  # Preserve unmatched community rows long enough to count and diagnose them.
  joined <- merge(
    metrics,
    foundation[, ..foundation_cols],
    by = keys,
    all.x = TRUE,
    sort = FALSE
  )

  # Unmatched biological rows cannot be classified as forest, so count them explicitly.
  n_conditions_in <- nrow(joined)
  unmatched <- is.na(joined$is_forested_condition)
  n_unmatched <- sum(unmatched)
  unmatched_fraction <- if (n_conditions_in > 0L) n_unmatched / n_conditions_in else 0
  if (unmatched_fraction > max_unmatched_condition_fraction) {
    stop(
      "Condition CWM table has ", format(n_unmatched, big.mark = ","),
      " of ", format(n_conditions_in, big.mark = ","),
      " condition(s) missing from the forested-condition foundation (",
      sprintf("%.2f%%", 100 * unmatched_fraction),
      "), above the ", sprintf("%.2f%%", 100 * max_unmatched_condition_fraction),
      " tolerance. Check that both tables come from the same FIA extract."
    )
  }

  # Exclude both nonforest and unresolved conditions before community aggregation.
  joined <- joined[!is.na(is_forested_condition) & is_forested_condition]

  if (nrow(joined) == 0L) {
    stop("No forested condition CWM rows remain after the condition join.")
  }

  identity_cols <- intersect(
    c(
      "stable_plot_id", "PLT_CN", "INVYR", "state", "STATECD", "UNITCD",
      "COUNTYCD", "PLOT", "PREV_PLT_CN", "LAT", "LON", "ELEV",
      "forested_plot_proportion", "n_forested_conditions",
      "condition_proportion_quality_flag"
    ),
    names(joined)
  )

  # Combine condition metrics using each condition's share of the forested area.
  result <- joined[, {
    values <- lapply(metric_cols, function(column) {
      forest_weighted_mean(get(column), forested_condition_weight)
    })
    names(values) <- metric_cols
    c(
      values,
      list(
        n_forested_conditions_with_layer = uniqueN(CONDID),
        forested_condition_weight_with_layer =
          sum(forested_condition_weight, na.rm = TRUE),
        forested_condition_weight_with_metric =
          sum(forested_condition_weight[
            rowSums(!is.na(.SD[, metric_cols, with = FALSE])) > 0
          ], na.rm = TRUE)
      )
    )
  }, by = identity_cols]

  result[, `:=`(
    community_grain = "forested_plot_visit",
    forest_conditions_only = TRUE,
    condition_area_weighting = "CONDPROP_UNADJ_normalized_over_forested_conditions",
    partial_forest_handling = "nonforest_excluded_forest_weights_normalized"
  )]
  setorder(result, stable_plot_id, INVYR, PLT_CN)

  # Carried so producers can report what the join did without recomputing it.
  setattr(result, "condition_join_diagnostics", list(
    n_condition_rows_in = n_conditions_in,
    n_conditions_missing_from_foundation = n_unmatched,
    n_forested_condition_rows_aggregated = nrow(joined),
    n_nonforest_condition_rows_excluded =
      n_conditions_in - n_unmatched - nrow(joined),
    shadowed_columns_dropped_from_metrics = shadowed
  ))
  result[]
}
