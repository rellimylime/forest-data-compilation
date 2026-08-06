# Keep visit auditing separate from construction of resolved FIA intervals.

suppressPackageStartupMessages({
  library(data.table)
})

fia_days_in_month <- function(year, month) {
  year <- as.integer(year)
  month <- as.integer(month)
  out <- rep(NA_integer_, length(year))
  valid <- !is.na(year) & !is.na(month) & month >= 1L & month <= 12L
  if (!any(valid)) return(out)

  month_days <- c(31L, 28L, 31L, 30L, 31L, 30L, 31L, 31L, 30L, 31L, 30L, 31L)
  out[valid] <- month_days[month[valid]]
  leap <- valid &
    month == 2L &
    ((year %% 400L == 0L) | (year %% 4L == 0L & year %% 100L != 0L))
  out[leap] <- 29L
  out
}

add_fia_measurement_date_bounds <- function(visits) {
  out <- copy(as.data.table(visits))
  required <- c("INVYR", "MEASYEAR", "MEASMON", "MEASDAY")
  missing <- setdiff(required, names(out))
  if (length(missing) > 0L) {
    stop(
      "Cannot build FIA measurement-date bounds; missing column(s): ",
      paste(missing, collapse = ", ")
    )
  }

  measurement_year <- as.integer(out$MEASYEAR)
  measurement_month <- as.integer(out$MEASMON)
  measurement_day <- as.integer(out$MEASDAY)
  inventory_year <- as.integer(out$INVYR)

  valid_measurement_year <- !is.na(measurement_year) &
    measurement_year >= 1800L & measurement_year <= 2200L
  valid_inventory_year <- !is.na(inventory_year) &
    inventory_year >= 1800L & inventory_year <= 2200L
  valid_month <- valid_measurement_year &
    !is.na(measurement_month) &
    measurement_month >= 1L & measurement_month <= 12L
  days_in_month <- fia_days_in_month(measurement_year, measurement_month)
  valid_day <- valid_month &
    !is.na(measurement_day) &
    measurement_day >= 1L &
    measurement_day <= days_in_month

  # Record the finest timing FIA actually supplies for each visit.
  out[, measurement_date_precision := fcase(
    valid_day, "day",
    valid_month, "month",
    valid_measurement_year, "year",
    valid_inventory_year, "inventory_year_fallback",
    default = "missing"
  )]
  out[, measurement_date_source := fcase(
    valid_day, "MEASYEAR_MEASMON_MEASDAY",
    valid_month, "MEASYEAR_MEASMON",
    valid_measurement_year, "MEASYEAR",
    valid_inventory_year, "INVYR",
    default = NA_character_
  )]
  out[, measurement_date_issue := fcase(
    valid_month & !is.na(measurement_day) & !valid_day, "invalid_measurement_day",
    valid_measurement_year & !is.na(measurement_month) & !valid_month,
      "invalid_measurement_month",
    !valid_measurement_year & !is.na(measurement_year), "invalid_measurement_year",
    !valid_measurement_year & valid_inventory_year, "missing_measurement_year",
    default = NA_character_
  )]

  # Represent missing months or days as date bounds instead of inventing a date.
  bound_year <- fifelse(valid_measurement_year, measurement_year, inventory_year)
  lower_month <- fifelse(valid_month, measurement_month, 1L)
  upper_month <- fifelse(valid_month, measurement_month, 12L)
  lower_day <- fifelse(valid_day, measurement_day, 1L)
  upper_day <- fifelse(
    valid_day,
    measurement_day,
    fifelse(valid_month, fia_days_in_month(bound_year, upper_month), 31L)
  )

  has_bounds <- valid_measurement_year | valid_inventory_year
  lower_text <- rep(NA_character_, nrow(out))
  upper_text <- rep(NA_character_, nrow(out))
  lower_text[has_bounds] <- sprintf(
    "%04d-%02d-%02d",
    bound_year[has_bounds],
    lower_month[has_bounds],
    lower_day[has_bounds]
  )
  upper_text[has_bounds] <- sprintf(
    "%04d-%02d-%02d",
    bound_year[has_bounds],
    upper_month[has_bounds],
    upper_day[has_bounds]
  )

  out[, measurement_date_lower := as.IDate(lower_text)]
  out[, measurement_date_upper := as.IDate(upper_text)]
  out
}

fia_assert_unique <- function(dt, keys, table_name) {
  duplicates <- as.data.table(dt)[, .N, by = keys][N > 1L]
  if (nrow(duplicates) > 0L) {
    stop(
      table_name, " has ", format(nrow(duplicates), big.mark = ","),
      " duplicated key combination(s): ", paste(keys, collapse = " x ")
    )
  }
  invisible(TRUE)
}

fia_interval_id <- function(stable_plot_id, previous_plt_cn, current_plt_cn) {
  # Use a readable key that preserves FIA's 64-bit identifiers exactly.
  paste(
    "fia_interval_v1",
    stable_plot_id,
    as.character(previous_plt_cn),
    as.character(current_plt_cn),
    sep = "|"
  )
}

fia_interval_date_status <- function(
  previous_lower,
  previous_upper,
  current_lower,
  current_upper
) {
  fcase(
    is.na(previous_lower) | is.na(previous_upper) |
      is.na(current_lower) | is.na(current_upper), "missing",
    current_upper < previous_lower, "reversed",
    current_lower <= previous_upper, "overlapping_or_same_period",
    default = "ordered"
  )
}

build_fia_pairing_products <- function(
  visit_context,
  current_invyr_min = -Inf,
  current_invyr_max = Inf
) {
  visits <- copy(as.data.table(visit_context))
  required <- c(
    "stable_plot_id", "PLT_CN", "PREV_PLT_CN", "INVYR",
    "MEASYEAR", "MEASMON", "MEASDAY",
    "measurement_date_lower", "measurement_date_upper",
    "measurement_date_precision", "measurement_date_source",
    "PLOT_STATUS_CD", "is_sampled_plot",
    "LAT", "LON", "STATECD", "state"
  )
  missing <- setdiff(required, names(visits))
  if (length(missing) > 0L) {
    stop(
      "FIA visit context is missing required column(s): ",
      paste(missing, collapse = ", ")
    )
  }

  fia_assert_unique(visits, "PLT_CN", "FIA plot-visit context")

  # Sort every visit to test whether FIA's official link is chronologically adjacent.
  chronology <- copy(visits)
  chronology[, chronology_missing_date := is.na(measurement_date_lower)]
  setorderv(
    chronology,
    c(
      "stable_plot_id", "chronology_missing_date",
      "measurement_date_lower", "measurement_date_upper", "INVYR", "PLT_CN"
    ),
    na.last = TRUE
  )
  chronology[, chronological_previous_PLT_CN := shift(PLT_CN), by = stable_plot_id]
  chronology_map <- chronology[, .(PLT_CN, chronological_previous_PLT_CN)]

  # Build a separate sampled-only predecessor for missing-link audit candidates.
  sampled <- chronology[is_sampled_plot == TRUE]
  sampled[, chronological_previous_sampled_PLT_CN := shift(PLT_CN), by = stable_plot_id]
  sampled_map <- sampled[, .(PLT_CN, chronological_previous_sampled_PLT_CN)]

  # Limit current endpoints while keeping older visits available as link targets.
  current <- visits[
    !is.na(INVYR) &
      INVYR >= current_invyr_min &
      INVYR <= current_invyr_max
  ]
  setnames(
    current,
    old = names(current),
    new = paste0("current_", names(current))
  )
  setnames(current, "current_stable_plot_id", "stable_plot_id")
  setnames(current, "current_STATECD", "STATECD")
  setnames(current, "current_state", "state")

  current <- merge(
    current,
    chronology_map,
    by.x = "current_PLT_CN",
    by.y = "PLT_CN",
    all.x = TRUE,
    sort = FALSE
  )
  current <- merge(
    current,
    sampled_map,
    by.x = "current_PLT_CN",
    by.y = "PLT_CN",
    all.x = TRUE,
    sort = FALSE
  )

  endpoint_cols <- c(
    "PLT_CN", "stable_plot_id", "INVYR",
    "MEASYEAR", "MEASMON", "MEASDAY",
    "measurement_date_lower", "measurement_date_upper",
    "measurement_date_precision", "measurement_date_source",
    "PLOT_STATUS_CD", "is_sampled_plot",
    "LAT", "LON", "STATECD", "state"
  )

  # Look up PREV_PLT_CN against every raw visit, including visits outside the window.
  official_targets <- visits[, ..endpoint_cols]
  setnames(
    official_targets,
    old = names(official_targets),
    new = paste0("official_target_", names(official_targets))
  )
  current <- merge(
    current,
    official_targets,
    by.x = "current_PREV_PLT_CN",
    by.y = "official_target_PLT_CN",
    all.x = TRUE,
    sort = FALSE
  )

  current[, official_link_present := !is.na(current_PREV_PLT_CN)]
  current[, official_target_available := !is.na(official_target_stable_plot_id)]
  current[, official_target_same_stable_plot :=
    official_target_available &
      !is.na(stable_plot_id) &
      official_target_stable_plot_id == stable_plot_id]
  current[, official_target_is_chronological_predecessor :=
    official_target_same_stable_plot &
      !is.na(chronological_previous_PLT_CN) &
      current_PREV_PLT_CN == chronological_previous_PLT_CN]

  # Classify official links independently from whether an interval is usable.
  current[, pairing_class := fcase(
    official_link_present & !official_target_available,
      "previous_link_target_unavailable",
    official_link_present & official_target_available &
      !official_target_same_stable_plot,
      "previous_link_conflict",
    official_target_same_stable_plot &
      official_target_is_chronological_predecessor,
      "structural_match",
    official_target_same_stable_plot &
      !official_target_is_chronological_predecessor,
      "previous_link_to_nonadjacent_visit",
    !official_link_present &
      !is.na(chronological_previous_sampled_PLT_CN),
      "previous_link_missing",
    default = "first_observed_visit"
  )]

  # Assign in stages so bit64 PLT_CN values retain their exact type.
  current[, selected_previous_PLT_CN := current_PREV_PLT_CN]

  # Cross-plot official links remain flagged and are not converted into intervals.
  current[official_target_same_stable_plot != TRUE, selected_previous_PLT_CN := NA]

  # A missing official link gets only a labeled chronological audit candidate.
  current[
    !official_link_present & !is.na(chronological_previous_sampled_PLT_CN),
    selected_previous_PLT_CN := chronological_previous_sampled_PLT_CN
  ]
  current[, selected_pair_source := fcase(
    official_target_same_stable_plot, "official_PREV_PLT_CN",
    !official_link_present & !is.na(chronological_previous_sampled_PLT_CN),
      "chronological_sampled_fallback",
    default = "none"
  )]

  previous_endpoints <- visits[, ..endpoint_cols]
  setnames(
    previous_endpoints,
    old = names(previous_endpoints),
    new = paste0("previous_", names(previous_endpoints))
  )
  current <- merge(
    current,
    previous_endpoints,
    by.x = "selected_previous_PLT_CN",
    by.y = "previous_PLT_CN",
    all.x = TRUE,
    sort = FALSE
  )
  setnames(current, "selected_previous_PLT_CN", "previous_PLT_CN")

  # Compare date bounds so uncertain dates are not treated as exact dates.
  current[, date_status := fia_interval_date_status(
    previous_measurement_date_lower,
    previous_measurement_date_upper,
    current_measurement_date_lower,
    current_measurement_date_upper
  )]
  current[is.na(previous_PLT_CN), date_status := "not_applicable"]

  # This technical flag does not decide which pairing classes belong in an analysis.
  current[, pairing_usable := !is.na(previous_PLT_CN) &
    previous_is_sampled_plot == TRUE &
    current_is_sampled_plot == TRUE &
    date_status == "ordered"]
  current[is.na(pairing_usable), pairing_usable := FALSE]
  current[, pairing_usable_reason := fcase(
    is.na(previous_PLT_CN), "no_resolved_previous_endpoint",
    current_is_sampled_plot != TRUE | is.na(current_is_sampled_plot),
      "current_visit_not_sampled",
    previous_is_sampled_plot != TRUE | is.na(previous_is_sampled_plot),
      "previous_visit_not_sampled",
    date_status == "missing", "missing_measurement_date",
    date_status == "reversed", "reversed_measurement_dates",
    date_status == "overlapping_or_same_period",
      "overlapping_or_same_measurement_period",
    default = "usable"
  )]

  # The audit keeps one row for every current visit, including unresolved links.
  audit <- current
  setorder(audit, state, stable_plot_id, current_measurement_date_lower, current_PLT_CN)
  fia_assert_unique(audit, "current_PLT_CN", "FIA visit-pairing audit")

  # The interval table contains only rows with an available previous endpoint.
  intervals <- audit[!is.na(previous_PLT_CN)]
  intervals[, interval_id := fia_interval_id(
    stable_plot_id,
    previous_PLT_CN,
    current_PLT_CN
  )]
  # Date precision produces minimum and maximum possible interval lengths.
  intervals[, interval_years_min :=
    as.numeric(current_measurement_date_lower - previous_measurement_date_upper) /
      365.2425]
  intervals[, interval_years_max :=
    as.numeric(current_measurement_date_upper - previous_measurement_date_lower) /
      365.2425]

  setnames(
    intervals,
    c(
      "previous_measurement_date_lower", "previous_measurement_date_upper",
      "current_measurement_date_lower", "current_measurement_date_upper",
      "previous_LAT", "previous_LON", "current_LAT", "current_LON"
    ),
    c(
      "previous_date_lower", "previous_date_upper",
      "current_date_lower", "current_date_upper",
      "previous_latitude", "previous_longitude",
      "current_latitude", "current_longitude"
    )
  )
  setcolorder(intervals, c(
    "interval_id", "stable_plot_id",
    "previous_PLT_CN", "current_PLT_CN", "current_PREV_PLT_CN",
    "previous_INVYR", "current_INVYR",
    "previous_MEASYEAR", "previous_MEASMON", "previous_MEASDAY",
    "current_MEASYEAR", "current_MEASMON", "current_MEASDAY",
    "previous_date_lower", "previous_date_upper",
    "current_date_lower", "current_date_upper",
    "interval_years_min", "interval_years_max",
    "pairing_class", "selected_pair_source",
    "date_status", "pairing_usable", "pairing_usable_reason",
    "previous_latitude", "previous_longitude",
    "current_latitude", "current_longitude",
    "STATECD", "state"
  ))
  setorder(intervals, state, stable_plot_id, current_date_lower, current_PLT_CN)
  fia_assert_unique(intervals, "interval_id", "FIA survey intervals")

  # Summarize link patterns by class, dates, state, and measurement decade.
  pairing_counts <- audit[, .(
    n_current_visits = .N,
    n_pairing_usable = sum(pairing_usable, na.rm = TRUE)
  ), by = .(
    pairing_class,
    date_status,
    state,
    previous_INVYR,
    current_INVYR,
    measurement_decade = fifelse(
      !is.na(current_MEASYEAR),
      (as.integer(current_MEASYEAR) %/% 10L) * 10L,
      fifelse(
        !is.na(current_INVYR),
        (as.integer(current_INVYR) %/% 10L) * 10L,
        NA_integer_
      )
    )
  )]
  setorder(
    pairing_counts,
    pairing_class, date_status, state,
    previous_INVYR, current_INVYR, measurement_decade
  )

  list(
    visit_pairing_audit = audit,
    survey_intervals = intervals,
    pairing_counts = pairing_counts
  )
}
