# Evaluate whether FIA surveys occur before and after a dated disturbance.
evaluate_disturbance_windows <- function(type_dated,
                                         dated_events,
                                         slot_events,
                                         survey_years,
                                         min_before = 1L,
                                         min_after = 1L,
                                         require_first = FALSE) {
  # Start with unique dated disturbances of the requested type.
  candidates <- unique(data.table::copy(type_dated)[
    !is.na(stable_plot_id) & !is.na(year),
    .(stable_plot_id, year = as.integer(year))
  ])

  empty_result <- function() {
    data.table::data.table(
      stable_plot_id = character(),
      year = integer(),
      first_disturbance_year = integer(),
      next_disturbance_year = integer(),
      n_before = integer(),
      n_after_before_next = integer(),
      bracketed = logical()
    )
  }
  if (nrow(candidates) == 0) return(empty_result())

  # Use all disturbance types to identify earlier or intervening events.
  event_years <- unique(data.table::copy(dated_events)[
    !is.na(stable_plot_id) & !is.na(year),
    .(stable_plot_id, event_year = as.integer(year))
  ])
  first_years <- event_years[
    , .(first_disturbance_year = min(event_year)),
    by = stable_plot_id
  ]
  candidates <- merge(
    candidates,
    first_years,
    by = "stable_plot_id",
    all.x = TRUE,
    sort = FALSE
  )

  candidates[, next_disturbance_year := NA_integer_]
  # The optional first-event rule excludes plots with unknown disturbance timing.
  if (isTRUE(require_first)) {
    unknown_timing_plots <- unique(data.table::copy(slot_events)[
      is.na(raw_year) | raw_year %in% c(0L, 9999L),
      stable_plot_id
    ])
    candidates <- candidates[
      year == first_disturbance_year &
        !stable_plot_id %in% unknown_timing_plots
    ]
    if (nrow(candidates) == 0) return(empty_result())

    # Find the next dated disturbance after each candidate event.
    future <- merge(
      candidates[, .(stable_plot_id, candidate_year = year)],
      event_years,
      by = "stable_plot_id",
      allow.cartesian = TRUE,
      sort = FALSE
    )
    future <- future[event_year > candidate_year]
    if (nrow(future) > 0) {
      next_years <- future[
        , .(next_disturbance_year = min(event_year)),
        by = .(stable_plot_id, candidate_year)
      ]
      candidate_cols <- setdiff(names(candidates), "next_disturbance_year")
      candidates <- merge(
        candidates[, ..candidate_cols],
        next_years,
        by.x = c("stable_plot_id", "year"),
        by.y = c("stable_plot_id", "candidate_year"),
        all.x = TRUE,
        sort = FALSE
      )
    }
  }

  # Compare each candidate with all observed survey years for the same plot.
  surveys <- unique(data.table::copy(survey_years)[
    !is.na(stable_plot_id) & !is.na(survey_year),
    .(stable_plot_id, survey_year = as.integer(survey_year))
  ])
  joined <- merge(
    candidates,
    surveys,
    by = "stable_plot_id",
    allow.cartesian = TRUE,
    sort = FALSE
  )
  if (nrow(joined) == 0) return(empty_result())

  # Count surveys before the event and after it but before another disturbance.
  out <- joined[, .(
    first_disturbance_year = first(first_disturbance_year),
    next_disturbance_year = first(next_disturbance_year),
    n_before = sum(survey_year < year),
    n_after_before_next = sum(
      survey_year >= year &
        (is.na(next_disturbance_year) | survey_year < next_disturbance_year)
    )
  ), by = .(stable_plot_id, year)]
  out[, bracketed :=
        n_before >= as.integer(min_before) &
        n_after_before_next >= as.integer(min_after)]
  out[]
}
