# ==============================================================================
# 01_validate_thermophilization_products.R
# Validate core thermophilization data products.
#
# Documentation:
#   07_thermophilization/README.md#qa-csvs
#   docs/DATA_PRODUCTS.md#thermophilization-outputs
#
# This script checks structural assumptions that should hold before modeling:
# file presence, documented row grains, required columns, valid proportions,
# niche-coverage ranges, and repeated-survey rate calculations.
#
# Usage:
#   Rscript 07_thermophilization/qa/scripts/01_validate_thermophilization_products.R
# ==============================================================================

suppressPackageStartupMessages({
  library(here)
  library(glue)
  library(data.table)
  library(arrow)
  library(fs)
})

source(here("scripts/utils/load_config.R"))

# ------------------------------------------------------------------------------
# Paths and configuration
# ------------------------------------------------------------------------------

config <- load_config()
thermo_config <- config$processed$thermophilization
thermo_dir <- here(thermo_config$output_dir)
qa_dir <- here("07_thermophilization/qa/outputs")

dir_create(qa_dir)

check_results_path <- file.path(qa_dir, "thermophilization_validation_checks.csv")
summary_path <- file.path(qa_dir, "thermophilization_validation_summary.csv")

product_paths <- list(
  plot_disturbance_severity = file.path(thermo_dir, thermo_config$files$plot_disturbance_severity),
  plot_community_climate_seedlings = file.path(thermo_dir, thermo_config$files$plot_community_climate_seedlings),
  plot_community_climate_saplings = file.path(thermo_dir, thermo_config$files$plot_community_climate_saplings),
  plot_community_climate_trees = file.path(thermo_dir, thermo_config$files$plot_community_climate_trees),
  forest_plot_visit_cwm_seedlings = file.path(thermo_dir, thermo_config$files$forest_plot_visit_cwm_seedlings),
  forest_plot_visit_cwm_saplings = file.path(thermo_dir, thermo_config$files$forest_plot_visit_cwm_saplings),
  forest_plot_visit_cwm_trees = file.path(thermo_dir, thermo_config$files$forest_plot_visit_cwm_trees),
  forest_visit_interval_change_seedlings = file.path(thermo_dir, thermo_config$files$forest_visit_interval_change_seedlings),
  forest_visit_interval_change_saplings = file.path(thermo_dir, thermo_config$files$forest_visit_interval_change_saplings),
  forest_visit_interval_change_trees = file.path(thermo_dir, thermo_config$files$forest_visit_interval_change_trees),
  forest_first_last_change = file.path(thermo_dir, thermo_config$files$forest_first_last_change),
  forest_first_last_change_by_stage = file.path(thermo_dir, thermo_config$files$forest_first_last_change_by_stage)
)

# ------------------------------------------------------------------------------
# Helpers
# ------------------------------------------------------------------------------

checks <- list()

add_check <- function(product, check_name, severity, passed, n_affected = 0L, details = "") {
  checks[[length(checks) + 1L]] <<- data.table(
    product = as.character(product),
    check_name = as.character(check_name),
    severity = as.character(severity),
    status = if (isTRUE(passed)) "pass" else "fail",
    n_affected = as.integer(n_affected),
    details = as.character(details)
  )
}

write_csv_safely <- function(df, path) {
  dir_create(dirname(path))
  tmp_path <- tempfile(
    pattern = paste0(path_file(path), "_tmp_"),
    tmpdir = dirname(path),
    fileext = ".csv"
  )
  on.exit(unlink(tmp_path, force = TRUE), add = TRUE)

  fwrite(df, tmp_path)
  file_copy(tmp_path, path, overwrite = TRUE)
}

read_product <- function(product_name) {
  path <- product_paths[[product_name]]
  exists <- !is.null(path) && !is.na(path) && file.exists(path)
  add_check(
    product_name,
    "file_exists",
    "error",
    exists,
    if (exists) 0L else 1L,
    path
  )
  if (!exists) return(NULL)

  as.data.table(read_parquet(path))
}

check_required_columns <- function(dt, product_name, required_cols) {
  missing_cols <- setdiff(required_cols, names(dt))
  add_check(
    product_name,
    "required_columns_present",
    "error",
    length(missing_cols) == 0,
    length(missing_cols),
    if (length(missing_cols) == 0) {
      "All required columns present."
    } else {
      paste(missing_cols, collapse = "; ")
    }
  )
}

check_unique_grain <- function(dt, product_name, grain_cols) {
  missing_cols <- setdiff(grain_cols, names(dt))
  if (length(missing_cols) > 0) {
    add_check(
      product_name,
      "documented_grain_unique",
      "error",
      FALSE,
      length(missing_cols),
      glue("Cannot check grain; missing column(s): {paste(missing_cols, collapse = '; ')}")
    )
    return(invisible(FALSE))
  }

  duplicate_count <- dt[, .N, by = grain_cols][N > 1, .N]
  add_check(
    product_name,
    "documented_grain_unique",
    "error",
    duplicate_count == 0,
    duplicate_count,
    glue("Grain: {paste(grain_cols, collapse = ' x ')}")
  )
}

check_columns_in_unit_interval <- function(dt, product_name, cols, severity = "error") {
  cols <- intersect(cols, names(dt))
  if (length(cols) == 0) return(invisible(NULL))

  for (col in cols) {
    bad_n <- dt[!is.na(get(col)) & (get(col) < -1e-9 | get(col) > 1 + 1e-9), .N]
    add_check(
      product_name,
      paste0(col, "_between_0_and_1"),
      severity,
      bad_n == 0,
      bad_n,
      "Values should be proportions or fractions."
    )
  }
}

check_nonnegative_columns <- function(dt, product_name, cols, severity = "error") {
  cols <- intersect(cols, names(dt))
  if (length(cols) == 0) return(invisible(NULL))

  for (col in cols) {
    bad_n <- dt[!is.na(get(col)) & get(col) < -1e-9, .N]
    add_check(
      product_name,
      paste0(col, "_nonnegative"),
      severity,
      bad_n == 0,
      bad_n,
      "Counts, weights, and survey intervals should not be negative."
    )
  }
}

check_layer_value <- function(dt, product_name, expected_layer) {
  if (!"community_layer" %in% names(dt)) return(invisible(NULL))
  bad_n <- dt[is.na(community_layer) | community_layer != expected_layer, .N]
  add_check(
    product_name,
    "community_layer_matches_filename",
    "error",
    bad_n == 0,
    bad_n,
    glue("Expected community_layer == {expected_layer}.")
  )
}

# ------------------------------------------------------------------------------
# Product-specific validators
# ------------------------------------------------------------------------------

validate_disturbance_severity <- function() {
  product <- "plot_disturbance_severity"
  dt <- read_product(product)
  if (is.null(dt)) return(invisible(NULL))

  check_required_columns(
    dt,
    product,
    c(
      "stable_plot_id", "PLT_CN", "INVYR", "prop_fire", "prop_crown_fire",
      "prop_insect", "prop_disease", "prop_weather",
      "dominant_disturbance_class", "is_high_severity_fire"
    )
  )
  check_unique_grain(dt, product, c("stable_plot_id", "PLT_CN", "INVYR"))
  check_columns_in_unit_interval(dt, product, grep("^prop_|^forested_prop_", names(dt), value = TRUE))
  check_nonnegative_columns(dt, product, c("n_conditions", "total_condition_prop", "forested_condition_prop"), "warning")

  if (all(c("prop_crown_fire", "prop_fire") %in% names(dt))) {
    bad_n <- dt[!is.na(prop_crown_fire) & !is.na(prop_fire) & prop_crown_fire > prop_fire + 1e-9, .N]
    add_check(
      product,
      "prop_crown_fire_not_greater_than_prop_fire",
      "error",
      bad_n == 0,
      bad_n,
      "Crown fire is a subset of fire."
    )
  }

  any_checks <- list(
    any_fire = "prop_fire",
    any_crown_fire = "prop_crown_fire",
    any_insect = "prop_insect",
    any_disease = "prop_disease",
    any_weather = "prop_weather",
    any_natural_disturbance = "prop_any_natural_disturbance",
    any_human_or_harvest = "prop_human_or_harvest"
  )
  for (flag_col in names(any_checks)) {
    prop_col <- any_checks[[flag_col]]
    if (all(c(flag_col, prop_col) %in% names(dt))) {
      expected <- dt[[prop_col]] > 0
      observed <- as.logical(dt[[flag_col]])
      bad_n <- sum(!is.na(expected) & !is.na(observed) & expected != observed)
      add_check(
        product,
        paste0(flag_col, "_matches_", prop_col),
        "error",
        bad_n == 0,
        bad_n,
        "Boolean flag should match whether the corresponding proportion is greater than zero."
      )
    }
  }
}

validate_condition_layer <- function(layer) {
  product <- paste0("plot_community_climate_", layer)
  dt <- read_product(product)
  if (is.null(dt)) return(invisible(NULL))

  check_required_columns(
    dt,
    product,
    c(
      "community_layer", "stable_plot_id", "PLT_CN", "INVYR", "CONDID",
      "mean_temp", "median_temp", "mean_cwd", "median_cwd",
      "frac_weight_with_niche", "frac_species_with_niche"
    )
  )
  check_unique_grain(dt, product, c("community_layer", "stable_plot_id", "PLT_CN", "INVYR", "CONDID"))
  check_layer_value(dt, product, layer)
  check_columns_in_unit_interval(dt, product, grep("^frac_", names(dt), value = TRUE))
  check_nonnegative_columns(dt, product, grep("weight|species|source_rows|subplots", names(dt), value = TRUE), "warning")
}

validate_forest_plot_visit_layer <- function(layer) {
  product <- paste0("forest_plot_visit_cwm_", layer)
  dt <- read_product(product)
  if (is.null(dt)) return(invisible(NULL))

  check_required_columns(
    dt,
    product,
    c(
      "community_layer", "stable_plot_id", "PLT_CN", "INVYR",
      "mean_temp", "median_temp", "mean_cwd", "median_cwd",
      "frac_weight_with_niche", "frac_species_with_niche",
      "forested_plot_proportion", "forested_condition_weight_with_layer",
      "forest_conditions_only"
    )
  )
  check_unique_grain(dt, product, c("community_layer", "stable_plot_id", "PLT_CN", "INVYR"))
  check_layer_value(dt, product, layer)
  check_columns_in_unit_interval(dt, product, grep("^frac_", names(dt), value = TRUE))
  check_nonnegative_columns(dt, product, grep("weight|species|source_rows|conditions|subplots", names(dt), value = TRUE), "warning")

  # The product's whole point is that nonforest conditions are excluded.
  if ("forest_conditions_only" %in% names(dt)) {
    bad_n <- dt[
      is.na(forest_conditions_only) | !as.logical(forest_conditions_only),
      .N
    ]
    add_check(
      product,
      "every_row_is_forest_conditions_only",
      "error",
      bad_n == 0,
      bad_n,
      "forest_conditions_only must be TRUE on every row."
    )
  }

  # Forest condition weights are normalized within the visit, so the weight
  # carrying this layer can never exceed the whole forested area.
  if ("forested_condition_weight_with_layer" %in% names(dt)) {
    bad_n <- dt[
      !is.na(forested_condition_weight_with_layer) &
        (forested_condition_weight_with_layer < 0 |
           forested_condition_weight_with_layer > 1 + 1e-9),
      .N
    ]
    add_check(
      product,
      "forested_condition_weight_with_layer_in_unit_interval",
      "error",
      bad_n == 0,
      bad_n,
      "Normalized forest condition weight must lie in [0, 1]."
    )
  }
}

validate_change_layer <- function(layer) {
  product <- paste0("forest_visit_interval_change_", layer)
  dt <- read_product(product)
  if (is.null(dt)) return(invisible(NULL))

  check_required_columns(
    dt,
    product,
    c(
      "community_layer", "stable_plot_id", "previous_PLT_CN", "current_PLT_CN",
      "previous_INVYR", "current_INVYR", "years_between_surveys",
      "delta_mean_temp", "rate_mean_temp_per_year",
      "delta_mean_cwd", "rate_mean_cwd_per_year",
      "current_frac_weight_with_niche", "previous_frac_weight_with_niche",
      "meets_niche_coverage_threshold", "link_status"
    )
  )

  # Only FIA's own remeasurement link may produce an interval.
  if ("link_status" %in% names(dt)) {
    bad_n <- dt[link_status != "official_link_match", .N]
    add_check(
      product,
      "every_interval_uses_the_official_remeasurement_link",
      "error",
      bad_n == 0,
      bad_n,
      "Chronological adjacency alone must not create a change interval."
    )
  }
  check_unique_grain(dt, product, c("community_layer", "stable_plot_id", "previous_PLT_CN", "current_PLT_CN"))
  check_layer_value(dt, product, layer)
  check_columns_in_unit_interval(
    dt,
    product,
    intersect(c("current_frac_weight_with_niche", "previous_frac_weight_with_niche"), names(dt))
  )
  check_columns_in_unit_interval(dt, product, grep("^prop_|^forested_prop_", names(dt), value = TRUE))
  check_nonnegative_columns(dt, product, c("years_between_surveys"), "error")

  if (all(c("previous_INVYR", "current_INVYR", "years_between_surveys") %in% names(dt))) {
    bad_n <- dt[
      is.na(previous_INVYR) |
        is.na(current_INVYR) |
        is.na(years_between_surveys) |
        current_INVYR <= previous_INVYR |
        abs((current_INVYR - previous_INVYR) - years_between_surveys) > 1e-9,
      .N
    ]
    add_check(
      product,
      "survey_interval_years_are_consistent",
      "error",
      bad_n == 0,
      bad_n,
      "current_INVYR must be after previous_INVYR, and years_between_surveys must equal their difference."
    )
  }

  rate_cols <- grep("^rate_.*_per_year$", names(dt), value = TRUE)
  for (rate_col in rate_cols) {
    metric <- sub("^rate_(.*)_per_year$", "\\1", rate_col)
    delta_col <- paste0("delta_", metric)
    if (!delta_col %in% names(dt)) next

    bad_n <- dt[
      !is.na(get(rate_col)) &
        !is.na(get(delta_col)) &
        !is.na(years_between_surveys) &
        years_between_surveys > 0 &
        abs(get(rate_col) - (get(delta_col) / years_between_surveys)) > 1e-8,
      .N
    ]
    add_check(
      product,
      paste0(rate_col, "_matches_delta_divided_by_years"),
      "error",
      bad_n == 0,
      bad_n,
      "Annualized rate should equal delta divided by years_between_surveys."
    )
  }
}

# ------------------------------------------------------------------------------
# Run validation
# ------------------------------------------------------------------------------

validate_first_last_change <- function() {
  product <- "forest_first_last_change"
  dt <- read_product(product)
  if (is.null(dt)) return(invisible(NULL))

  check_required_columns(
    dt,
    product,
    c(
      "stable_plot_id", "first_PLT_CN", "last_PLT_CN",
      "INVYR_first", "INVYR_last", "n_visits_observed",
      "seedlings_change_mean_temp", "saplings_change_mean_temp",
      "trees_change_mean_temp"
    )
  )
  check_unique_grain(dt, product, "stable_plot_id")
  check_nonnegative_columns(dt, product, "n_visits_observed", "error")

  if (all(c("INVYR_first", "INVYR_last") %in% names(dt))) {
    bad_n <- dt[is.na(INVYR_first) | is.na(INVYR_last) | INVYR_last <= INVYR_first, .N]
    add_check(
      product,
      "last_visit_is_after_first_visit",
      "error",
      bad_n == 0,
      bad_n,
      "Endpoint ordering must run forward in time."
    )
  }

  # Every stage's change must be last minus first, for all eight indicators.
  for (stage in c("seedlings", "saplings", "trees")) {
    for (metric in c("mean_temp", "mean_cwd", "mean_pr", "median_temp")) {
      first_col <- paste(stage, "first", metric, sep = "_")
      last_col <- paste(stage, "last", metric, sep = "_")
      change_col <- paste(stage, "change", metric, sep = "_")
      if (!all(c(first_col, last_col, change_col) %in% names(dt))) next

      bad_n <- dt[
        !is.na(get(change_col)) &
          abs(get(change_col) - (get(last_col) - get(first_col))) > 1e-8,
        .N
      ]
      add_check(
        product,
        paste0(change_col, "_matches_last_minus_first"),
        "error",
        bad_n == 0,
        bad_n,
        "Change must equal the last-visit value minus the first-visit value."
      )
    }
  }
}

validate_first_last_by_stage <- function() {
  product <- "forest_first_last_change_by_stage"
  dt <- read_product(product)
  if (is.null(dt)) return(invisible(NULL))

  check_required_columns(
    dt,
    product,
    c(
      "stable_plot_id", "life_stage", "PLT_CN_first", "PLT_CN_last",
      "n_visits_observed", "change_mean_temp"
    )
  )
  check_unique_grain(dt, product, c("stable_plot_id", "life_stage"))

  if ("n_visits_observed" %in% names(dt)) {
    bad_n <- dt[is.na(n_visits_observed) | n_visits_observed < 2L, .N]
    add_check(
      product,
      "every_row_has_at_least_two_visits",
      "error",
      bad_n == 0,
      bad_n,
      "A change value needs two surveys of the same plot."
    )
  }
}

cat("Thermophilization Product Validation\n")
cat("====================================\n\n")

validate_disturbance_severity()

for (layer in c("seedlings", "saplings", "trees")) {
  validate_condition_layer(layer)
  validate_forest_plot_visit_layer(layer)
  validate_change_layer(layer)
}

validate_first_last_change()
validate_first_last_by_stage()

checks_dt <- rbindlist(checks, fill = TRUE)
setorder(checks_dt, severity, status, product, check_name)

summary_dt <- checks_dt[
  ,
  .(
    n_checks = .N,
    n_failed = sum(status == "fail"),
    n_error_failures = sum(status == "fail" & severity == "error"),
    n_warning_failures = sum(status == "fail" & severity == "warning")
  )
]
summary_dt[, validation_passed := n_error_failures == 0]

write_csv_safely(checks_dt, check_results_path)
write_csv_safely(summary_dt, summary_path)

cat(glue("Check results: {check_results_path}"), "\n")
cat(glue("Summary:       {summary_path}"), "\n\n")
print(summary_dt)

if (!isTRUE(summary_dt$validation_passed[[1]])) {
  stop("Thermophilization validation failed. Inspect thermophilization_validation_checks.csv.")
}
