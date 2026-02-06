# ==============================================================================
# scripts/utils/time_utils.R
# Shared time-handling utilities: calendar <-> water year conversion
#
# Water year definition (US hydrological convention):
#   Water year N runs from October of year N-1 through September of year N.
#   - If calendar month >= 10: water_year = calendar_year + 1, water_year_month = month - 9
#   - If calendar month <  10: water_year = calendar_year,     water_year_month = month + 3
#
# Water year months:
#   Oct=1, Nov=2, Dec=3, Jan=4, Feb=5, Mar=6,
#   Apr=7, May=8, Jun=9, Jul=10, Aug=11, Sep=12
# ==============================================================================

#' Convert calendar year/month to water year/month
#'
#' @param calendar_year Integer vector of calendar years
#' @param calendar_month Integer vector of calendar months (1-12)
#' @return data.frame with columns: water_year, water_year_month
#' @examples
#' calendar_to_water_year(2020, 10)  # water_year=2021, water_year_month=1
#' calendar_to_water_year(2021, 3)   # water_year=2021, water_year_month=6
calendar_to_water_year <- function(calendar_year, calendar_month) {
  stopifnot(
    is.numeric(calendar_year),
    is.numeric(calendar_month),
    all(calendar_month >= 1 & calendar_month <= 12, na.rm = TRUE),
    length(calendar_year) == length(calendar_month)
  )

  water_year <- ifelse(calendar_month >= 10,
                       calendar_year + 1L,
                       calendar_year)
  water_year_month <- ifelse(calendar_month >= 10,
                             calendar_month - 9L,
                             calendar_month + 3L)

  data.frame(water_year = as.integer(water_year),
             water_year_month = as.integer(water_year_month))
}


#' Convert water year/month back to calendar year/month
#'
#' @param water_year Integer vector of water years
#' @param water_year_month Integer vector of water year months (1-12)
#' @return data.frame with columns: calendar_year, calendar_month
water_to_calendar_year <- function(water_year, water_year_month) {
  stopifnot(
    is.numeric(water_year),
    is.numeric(water_year_month),
    all(water_year_month >= 1 & water_year_month <= 12, na.rm = TRUE),
    length(water_year) == length(water_year_month)
  )

  # Water year months 1-3 correspond to Oct-Dec of the previous calendar year
  calendar_month <- ifelse(water_year_month <= 3,
                           water_year_month + 9L,
                           water_year_month - 3L)
  calendar_year <- ifelse(water_year_month <= 3,
                          water_year - 1L,
                          water_year)

  data.frame(calendar_year = as.integer(calendar_year),
             calendar_month = as.integer(calendar_month))
}


#' Add water year columns to a data frame with calendar year/month
#'
#' Expects columns named `year` and `month` (or calendar_year/calendar_month).
#' Appends `water_year` and `water_year_month`.
#'
#' @param df data.frame with year/month or calendar_year/calendar_month columns
#' @return df with water_year and water_year_month appended
add_water_year <- function(df) {
  # Detect column names
  if ("year" %in% names(df) && "month" %in% names(df)) {
    cal_year <- df$year
    cal_month <- df$month
  } else if ("calendar_year" %in% names(df) && "calendar_month" %in% names(df)) {
    cal_year <- df$calendar_year
    cal_month <- df$calendar_month
  } else {
    stop("Data frame must contain (year, month) or (calendar_year, calendar_month) columns.")
  }

  wy <- calendar_to_water_year(cal_year, cal_month)
  df$water_year <- wy$water_year
  df$water_year_month <- wy$water_year_month

  df
}


#' Get a descriptive label for water year month (e.g., "Oct", "Nov", ...)
#'
#' @param water_year_month Integer vector of water year months (1-12)
#' @return Character vector of month abbreviations
water_year_month_label <- function(water_year_month) {
  # Water year month 1 = October, 2 = November, ..., 12 = September
  month_labels <- c("Oct", "Nov", "Dec", "Jan", "Feb", "Mar",
                     "Apr", "May", "Jun", "Jul", "Aug", "Sep")
  month_labels[water_year_month]
}
