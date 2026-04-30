suppressPackageStartupMessages({
  library(testthat)
  library(here)
  library(arrow)
  library(dplyr)
  library(yaml)
})

qa_strict_mode <- tolower(Sys.getenv("STRICT_OUTPUT_CHECKS", "false")) %in% c(
  "1", "true", "yes", "y"
)

qa_path <- function(...) {
  here(...)
}

qa_require_file <- function(path, label = path) {
  if (!file.exists(path)) {
    msg <- sprintf("Missing required file: %s", label)
    if (qa_strict_mode) {
      fail(msg)
    } else {
      skip(paste0(msg, " (set STRICT_OUTPUT_CHECKS=true to fail)"))
    }
  }
  invisible(path)
}

qa_require_dir <- function(path, label = path) {
  if (!dir.exists(path)) {
    msg <- sprintf("Missing required directory: %s", label)
    if (qa_strict_mode) {
      fail(msg)
    } else {
      skip(paste0(msg, " (set STRICT_OUTPUT_CHECKS=true to fail)"))
    }
  }
  invisible(path)
}

qa_expect_cols <- function(data, required_cols) {
  missing_cols <- setdiff(required_cols, names(data))
  expect(
    length(missing_cols) == 0,
    paste("Missing columns:", paste(missing_cols, collapse = ", "))
  )
}

qa_read_parquet_head <- function(path, n = 5000L, cols = NULL) {
  ds <- open_dataset(path)
  if (!is.null(cols)) {
    ds <- ds |> select(any_of(cols))
  }
  ds |> slice_head(n = n) |> collect()
}

qa_list_yearly_parquets <- function(dir_path, prefix) {
  list.files(
    dir_path,
    pattern = paste0("^", prefix, "_\\d{4}\\.parquet$"),
    full.names = TRUE
  )
}

qa_year_from_filename <- function(path) {
  as.integer(sub(".*_(\\d{4})\\.parquet$", "\\1", basename(path)))
}

qa_expect_unique_key <- function(data, key_cols, label = "key") {
  duplicates <- data |> count(across(all_of(key_cols))) |> filter(n > 1)
  expect(
    nrow(duplicates) == 0,
    sprintf("Duplicate rows found for %s", label)
  )
}

qa_calendar_to_water_year <- function(year, month) {
  water_year <- ifelse(month >= 10L, year + 1L, year)
  water_year_month <- ifelse(month >= 10L, month - 9L, month + 3L)
  list(water_year = as.integer(water_year), water_year_month = as.integer(water_year_month))
}

qa_load_config <- function() {
  suppressWarnings(read_yaml(qa_path("config.yaml")))
}

#' Check that all expected years have a corresponding parquet file (no gap years)
qa_expect_year_coverage <- function(dir_path, prefix, years_expected) {
  files       <- qa_list_yearly_parquets(dir_path, prefix)
  years_found <- sort(vapply(files, qa_year_from_filename, integer(1)))
  missing     <- setdiff(years_expected, years_found)
  expect(
    length(missing) == 0,
    sprintf(
      "Missing year files in %s: %s",
      basename(dir_path), paste(missing, collapse = ", ")
    )
  )
}

#' Check that all non-NA values in a column fall within [lo, hi]
qa_expect_value_range <- function(data, col, lo, hi) {
  vals <- data[[col]]
  vals <- vals[!is.na(vals)]
  expect(
    all(vals >= lo & vals <= hi),
    sprintf(
      "'%s' has values outside [%g, %g]: observed range [%g, %g]",
      col, lo, hi, min(vals), max(vals)
    )
  )
}
