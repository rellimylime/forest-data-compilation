# ==============================================================================
# 07_build_plot_visit_context.R
# Build the canonical FIA PLOT-visit context used by repeated-survey workflows.
#
# One row represents one raw FIADB PLOT record. Unlike condition-derived
# summaries, this product retains nonsampled visits and visits outside the
# configured analysis window so PREV_PLT_CN targets can be audited faithfully.
#
# Output:
#   05_fia/data/processed/summaries/plot_visit_context.parquet
#
# Usage:
#   Rscript 05_fia/scripts/foundations/01_build_plot_visit_context.R
# ==============================================================================

suppressPackageStartupMessages({
  library(here)
  library(glue)
  library(data.table)
  library(bit64)
  library(fs)
})

source(here("scripts/utils/load_config.R"))
source(here("scripts/utils/parquet_atomic.R"))
source(here("scripts/utils/fia_intervals.R"))

config <- load_config()
fia_raw <- config$raw$fia
fia_processed <- config$processed$fia
raw_dir <- here(fia_raw$local_dir)
summary_dir <- here(fia_processed$summaries$output_dir)

context_filename <- fia_processed$summaries$files$plot_visit_context
if (is.null(context_filename) || !nzchar(context_filename)) {
  context_filename <- "plot_visit_context.parquet"
}
out_path <- file.path(summary_dir, context_filename)

requested_fields <- c(
  "CN", "PREV_PLT_CN", "INVYR",
  "STATECD", "UNITCD", "COUNTYCD", "PLOT",
  "PLOT_STATUS_CD", "PLOT_NONSAMPLE_REASN_CD",
  "MEASYEAR", "MEASMON", "MEASDAY",
  "REMPER", "KINDCD", "DESIGNCD", "RDDISTCD",
  "LAT", "LON", "ELEV",
  "MANUAL", "QA_STATUS", "SAMP_METHOD_CD",
  "CYCLE", "SUBCYCLE"
)

cat("Build FIA Plot-Visit Context\n")
cat("============================\n\n")
cat(glue("Raw PLOT directory: {raw_dir}"), "\n")
cat(glue("Output:             {out_path}"), "\n")
cat(glue(
  "Configured window: {fia_raw$invyr_min}-{fia_raw$invyr_max} ",
  "(all raw visits are retained)"
), "\n\n")

read_state_plot <- function(state) {
  path <- file.path(raw_dir, state, glue("{state}_PLOT.csv"))
  if (!file.exists(path)) {
    stop("Missing raw FIA PLOT file: ", path)
  }

  available <- names(fread(path, nrows = 0L, showProgress = FALSE))
  selected <- intersect(requested_fields, available)
  missing <- setdiff(requested_fields, available)
  if (length(missing) > 0L) {
    message(
      state, " PLOT is missing optional field(s): ",
      paste(missing, collapse = ", ")
    )
  }

  dt <- fread(
    path,
    select = selected,
    showProgress = FALSE,
    integer64 = "integer64"
  )
  for (field in missing) dt[, (field) := NA]
  setcolorder(dt, requested_fields)

  # Explicit integer64 identifiers prevent type drift when an all-null
  # PREV_PLT_CN column is bound with populated state files.
  dt[, CN := as.integer64(CN)]
  dt[, PREV_PLT_CN := as.integer64(PREV_PLT_CN)]
  dt[, state := state]
  dt
}

state_tables <- lapply(fia_raw$states, read_state_plot)
visits <- rbindlist(state_tables, use.names = TRUE, fill = TRUE)
rm(state_tables)
gc(verbose = FALSE)

setnames(visits, "CN", "PLT_CN")
visits[, stable_plot_id := paste(
  STATECD, UNITCD, COUNTYCD, PLOT,
  sep = "_"
)]
visits[, is_sampled_plot := PLOT_STATUS_CD == 1L]
visits[is.na(is_sampled_plot), is_sampled_plot := FALSE]
visits[, has_usable_coordinates :=
  !is.na(LAT) & !is.na(LON) &
    LAT >= -90 & LAT <= 90 &
    LON >= -180 & LON <= 180]
visits[, in_configured_inventory_window :=
  !is.na(INVYR) &
    INVYR >= fia_raw$invyr_min &
    INVYR <= fia_raw$invyr_max]

visits <- add_fia_measurement_date_bounds(visits)
fia_assert_unique(visits, "PLT_CN", "FIA plot-visit context")

preferred_order <- c(
  "stable_plot_id", "PLT_CN", "PREV_PLT_CN",
  "INVYR", "MEASYEAR", "MEASMON", "MEASDAY",
  "measurement_date_lower", "measurement_date_upper",
  "measurement_date_precision", "measurement_date_source",
  "measurement_date_issue",
  "PLOT_STATUS_CD", "PLOT_NONSAMPLE_REASN_CD",
  "is_sampled_plot", "in_configured_inventory_window",
  "STATECD", "state", "UNITCD", "COUNTYCD", "PLOT",
  "LAT", "LON", "ELEV", "has_usable_coordinates",
  "REMPER", "KINDCD", "DESIGNCD", "RDDISTCD",
  "MANUAL", "QA_STATUS", "SAMP_METHOD_CD",
  "CYCLE", "SUBCYCLE"
)
setcolorder(visits, preferred_order)
setorder(visits, state, stable_plot_id, measurement_date_lower, INVYR, PLT_CN)

dir_create(summary_dir)
write_parquet_atomic(visits, out_path, compression = "snappy")

cat(glue("Rows:                 {format(nrow(visits), big.mark = ',')}"), "\n")
cat(glue("Stable plots:         {format(uniqueN(visits$stable_plot_id), big.mark = ',')}"), "\n")
cat(glue(
  "Valid exact dates:    ",
  "{format(visits[measurement_date_precision == 'day', .N], big.mark = ',')}"
), "\n")
cat(glue(
  "Sampled plot records: ",
  "{format(visits[is_sampled_plot == TRUE, .N], big.mark = ',')}"
), "\n")
cat("\nDone.\n")
