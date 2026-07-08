# ==============================================================================
# compare_point_climate_outputs.R
# Compare point-climate outputs against a reference copy.
#
# This is a read-only QA script. It is intended for cases where results have
# already been shared and a regenerated output needs to be checked against the
# previously used files.
#
# Usage:
#   Rscript site_climate/scripts/compare_point_climate_outputs.R \
#     --candidate-dir=site_climate/data/processed \
#     --reference-dir=/path/to/reference_outputs
# ==============================================================================

suppressPackageStartupMessages({
  library(arrow)
  library(data.table)
  library(fs)
  library(here)
})

arg_value <- function(name, default = NULL) {
  args <- commandArgs(trailingOnly = TRUE)
  prefix <- paste0("--", name, "=")
  hit <- args[startsWith(args, prefix)]
  if (length(hit) == 0) {
    return(default)
  }
  sub(prefix, "", hit[[length(hit)]], fixed = TRUE)
}

candidate_dir <- arg_value("candidate-dir", here("site_climate/data/processed"))
reference_dir <- arg_value("reference-dir", NULL)
if (is.null(reference_dir)) {
  stop("Provide --reference-dir=/path/to/reference_outputs")
}

candidate_pixel <- file.path(candidate_dir, "site_pixel_map.parquet")
candidate_climate <- file.path(candidate_dir, "site_climate.parquet")
reference_pixel <- file.path(reference_dir, "site_pixel_map.parquet")
reference_climate <- file.path(reference_dir, "site_climate.parquet")

required <- c(candidate_pixel, candidate_climate, reference_pixel, reference_climate)
missing <- required[!file.exists(required)]
if (length(missing) > 0) {
  stop(sprintf("Missing required file(s): %s", paste(missing, collapse = ", ")))
}

qa_dir <- here("site_climate/qa/outputs")
dir_create(qa_dir)

compare_tables <- function(candidate_file, reference_file, key_cols, label) {
  candidate <- as.data.table(read_parquet(candidate_file))
  reference <- as.data.table(read_parquet(reference_file))

  setkeyv(candidate, key_cols)
  setkeyv(reference, key_cols)

  shared_cols <- intersect(names(candidate), names(reference))
  candidate <- candidate[, ..shared_cols]
  reference <- reference[, ..shared_cols]

  same_schema <- identical(names(candidate), names(reference))
  same_rows <- nrow(candidate) == nrow(reference)
  same_content <- same_schema && same_rows && isTRUE(all.equal(candidate, reference, check.attributes = FALSE))

  data.table(
    product = label,
    candidate_rows = nrow(candidate),
    reference_rows = nrow(reference),
    same_schema = same_schema,
    same_row_count = same_rows,
    same_content = same_content
  )
}

summary <- rbindlist(list(
  compare_tables(
    candidate_pixel,
    reference_pixel,
    key_cols = c("site_id"),
    label = "site_pixel_map"
  ),
  compare_tables(
    candidate_climate,
    reference_climate,
    key_cols = c("site_id", "year", "month", "variable"),
    label = "site_climate"
  )
))

out_file <- file.path(qa_dir, "point_climate_output_comparison.csv")
fwrite(summary, out_file)

print(summary)
cat(sprintf("\nWrote comparison summary: %s\n", out_file))

if (!all(summary$same_content)) {
  stop("Candidate outputs differ from reference outputs.")
}
