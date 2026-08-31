# ==============================================================================
# validate_disturbance_classification.R
# Validate FIA condition-level disturbance/treatment flags and raw codes
# ==============================================================================

source("scripts/utils/load_config.R")

# Load project paths from config so QC follows the production directory layout.
config <- load_config()

library(here)
library(glue)
library(data.table)
library(arrow)

cat("FIA Disturbance Classification Validation\n")
cat("=========================================\n\n")

# Resolve the classification product produced by 05_build_fia_summaries.R.
class_path <- file.path(
  here(config$processed$fia$summaries$output_dir),
  "fia_condition_disturbance_flags.parquet"
)

# Fail early if the classification product has not been built yet.
if (!file.exists(class_path)) {
  stop(glue("Missing disturbance classification product: {class_path}"))
}

# Read the classification table because the product is compact enough for QC.
class_dt <- as.data.table(read_parquet(class_path))

# Check that downstream analysis fields are present before validating logic.
required_cols <- c(
  "PLT_CN", "INVYR", "CONDID", "stable_plot_id",
  "disturbance_class_primary", "disturbance_class",
  "is_forested_analysis_condition", "has_any_recorded_disturbance",
  "has_any_treatment", "is_human_or_harvest", "has_crown_fire_condition",
  "is_high_severity_proxy", "is_natural_disturbance",
  "time_since_disturbance"
)
missing_cols <- setdiff(required_cols, names(class_dt))
if (length(missing_cols) > 0) {
  stop(glue("Disturbance classification is missing: {paste(missing_cols, collapse=', ')}"))
}

# Print core dimensions and class counts for quick log inspection.
cat(glue("Rows: {format(nrow(class_dt), big.mark=',')}\n"))
cat(glue("Stable IDs missing: {format(sum(is.na(class_dt$stable_plot_id)), big.mark=',')}\n\n"))

cat("Primary disturbance classes:\n")
print(class_dt[, .N, by = disturbance_class_primary][order(-N)])

# Validate that the high-severity v1 proxy is exactly crown fire.
bad_high_severity <- class_dt[
  is_high_severity_proxy == TRUE & has_crown_fire_condition != TRUE
]

# Validate that timing fields do not contain impossible negative lags.
bad_negative_time <- class_dt[
  !is.na(time_since_disturbance) & time_since_disturbance < 0
]

# Summarize validation failures in one small table.
checks <- data.table(
  check = c(
    "bad_high_severity_proxy",
    "bad_negative_time_since_disturbance"
  ),
  n = c(
    nrow(bad_high_severity),
    nrow(bad_negative_time)
  )
)

cat("\nValidation checks:\n")
print(checks)

# Stop if any logical validation failed.
if (any(checks$n > 0)) {
  stop("Disturbance classification validation failed.")
}

cat("\nDisturbance classification validation passed.\n")
