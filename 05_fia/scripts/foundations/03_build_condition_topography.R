# ==============================================================================
# 03_build_condition_topography.R
# Extract FIA condition slope, aspect, and physiographic class.
#
# One row represents one raw FIADB COND record (PLT_CN x CONDID). Elevation is
# a plot attribute and is already carried by the condition summaries; this
# product adds the condition-level topography fields they omit.
#
# FIADB v9.4 definitions (User Guide section 2.5):
#   SLOPE     percent slope. On annual plots (MANUAL >= 1.0) it is the slope of
#             the subplot covering the largest share of the condition.
#   ASPECT    degrees; north is recorded as 360. 0 means no aspect because the
#             slope is under 5 percent, so 0 must never be read as north.
#   PHYSCLCD  physiographic class: landform and position as they affect
#             moisture available to trees (xeric, mesic, hydric groups).
#
# Output:
#   05_fia/data/processed/summaries/condition_topography.parquet
#
# Usage:
#   Rscript 05_fia/scripts/foundations/03_build_condition_topography.R
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

out_filename <- fia_processed$summaries$files$condition_topography
if (is.null(out_filename) || !nzchar(out_filename)) {
  out_filename <- "condition_topography.parquet"
}
out_path <- file.path(summary_dir, out_filename)

requested_fields <- c("PLT_CN", "INVYR", "CONDID", "STATECD", "SLOPE", "ASPECT", "PHYSCLCD")

cat("Build FIA Condition Topography\n")
cat("==============================\n\n")
cat(glue("Raw COND directory: {raw_dir}"), "\n")
cat(glue("Output:             {out_path}"), "\n\n")

read_state_cond <- function(state) {
  path <- file.path(raw_dir, state, glue("{state}_COND.csv"))
  if (!file.exists(path)) {
    stop("Missing raw FIA COND file: ", path)
  }

  available <- names(fread(path, nrows = 0L, showProgress = FALSE))
  missing <- setdiff(requested_fields, available)
  if (length(missing) > 0L) {
    stop(state, " COND is missing required field(s): ", paste(missing, collapse = ", "))
  }

  dt <- fread(
    path,
    select = requested_fields,
    showProgress = FALSE,
    integer64 = "integer64"
  )
  # Explicit types keep state files bindable when a column is all null in one state.
  dt[, PLT_CN := as.integer64(PLT_CN)]
  dt[, `:=`(
    INVYR = as.integer(INVYR),
    CONDID = as.integer(CONDID),
    STATECD = as.integer(STATECD),
    SLOPE = as.integer(SLOPE),
    ASPECT = as.integer(ASPECT),
    PHYSCLCD = as.integer(PHYSCLCD)
  )]
  dt[, state := state]
  dt
}

topography <- rbindlist(lapply(fia_raw$states, read_state_cond), use.names = TRUE)
fia_assert_unique(topography, c("PLT_CN", "CONDID"), "FIA condition topography")

# ASPECT = 0 is FIA's "no aspect" code, not north; expose that explicitly.
topography[, has_aspect := !is.na(ASPECT) & ASPECT >= 1L & ASPECT <= 360L]

setcolorder(topography, c(
  "PLT_CN", "INVYR", "CONDID", "STATECD", "state",
  "SLOPE", "ASPECT", "has_aspect", "PHYSCLCD"
))
setorder(topography, state, PLT_CN, CONDID)

dir_create(summary_dir)
write_parquet_atomic(topography, out_path, compression = "snappy")

n <- nrow(topography)
pct <- function(x) sprintf("%.1f%%", 100 * sum(x, na.rm = TRUE) / n)
cat(glue("Rows:                    {format(n, big.mark = ',')}"), "\n")
cat(glue("SLOPE present:           {pct(!is.na(topography$SLOPE))}"), "\n")
cat(glue("ASPECT present:          {pct(!is.na(topography$ASPECT))}"), "\n")
cat(glue("ASPECT = 0 (no aspect):  {pct(topography$ASPECT == 0L)}"), "\n")
cat(glue("PHYSCLCD present:        {pct(!is.na(topography$PHYSCLCD))}"), "\n")
cat(glue(
  "SLOPE range:             {min(topography$SLOPE, na.rm = TRUE)}-",
  "{max(topography$SLOPE, na.rm = TRUE)} percent"
), "\n")
out_of_range <- topography[!is.na(ASPECT) & (ASPECT < 0L | ASPECT > 360L), .N]
if (out_of_range > 0L) {
  warning(out_of_range, " condition(s) have ASPECT outside 0-360.")
}
cat("\nDone.\n")
