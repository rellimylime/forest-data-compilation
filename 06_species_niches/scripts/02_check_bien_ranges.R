# ==============================================================================
# 02_check_bien_ranges.R
# Check BIEN range-map availability for the species universe.
#
# This script queries BIEN by binomial name and records whether each species has
# a BIEN range map available. Pseudo/aggregate taxa are excluded from the BIEN
# request but remain documented in the species universe QA outputs.
#
# Usage:
#   Rscript 06_species_niches/scripts/02_check_bien_ranges.R
#   Rscript 06_species_niches/scripts/02_check_bien_ranges.R --limit=100
# ==============================================================================

suppressPackageStartupMessages({
  library(here)
  library(glue)
  library(data.table)
  library(arrow)
  library(tibble)
  library(fs)
})

source(here("scripts/utils/load_config.R"))

args <- commandArgs(trailingOnly = TRUE)

get_arg <- function(flag, default = NULL) {
  hit <- grep(paste0("^", flag, "="), args, value = TRUE)
  if (length(hit) == 0) return(default)
  sub(paste0("^", flag, "="), "", hit[[1]])
}

limit_arg <- get_arg("--limit", NA_character_)
if (!is.na(limit_arg)) limit_arg <- as.integer(limit_arg)
batch_size <- as.integer(get_arg("--batch-size", "70"))

if (!requireNamespace("BIEN", quietly = TRUE)) {
  stop("Package 'BIEN' is required. Install it with install.packages('BIEN').")
}

config <- load_config()
niche_config <- config$processed$species_niches
processed_dir <- here(niche_config$output_dir)
qa_dir <- here("06_species_niches/qa/outputs")
dir_create(processed_dir)
dir_create(qa_dir)

species_universe_path <- file.path(processed_dir, niche_config$files$species_universe)
availability_path <- file.path(processed_dir, niche_config$files$bien_range_availability)

if (!is.na(limit_arg)) {
  availability_path <- file.path(processed_dir, sprintf("bien_range_availability_limit_%d.parquet", limit_arg))
}

if (!file.exists(species_universe_path)) {
  stop(glue("Species universe not found: {species_universe_path}"))
}

make_bien_query_name <- function(scientific_name) {
  gsub("\\s+", "_", trimws(as.character(scientific_name)))
}

query_bien_batch <- function(names_batch) {
  tryCatch(
    BIEN::BIEN_ranges_species(
      species = names_batch,
      match_names_only = TRUE
    ),
    error = function(e) {
      data.table(
        species = names_batch,
        range_lookup_status = "api_error",
        range_lookup_error = conditionMessage(e)
      )
    }
  )
}

normalize_bien_result <- function(result) {
  dt <- as.data.table(result)
  if (nrow(dt) == 0) {
    return(data.table(
      bien_query_name = character(),
      bien_range_available = logical(),
      range_lookup_status = character(),
      range_match_status = character(),
      range_lookup_error = character()
    ))
  }

  names_lower <- tolower(names(dt))
  names(dt) <- names_lower

  query_col <- intersect(c("species", "species_name", "name", "submitted_name"), names(dt))[1]
  if (is.na(query_col)) query_col <- names(dt)[1]

  status_col <- intersect(c("range_lookup_status", "status", "match_status"), names(dt))[1]
  matched_col <- intersect(c("matched_species", "matched_name", "bien_species", "species_matched"), names(dt))[1]

  out <- data.table(
    bien_query_name = make_bien_query_name(dt[[query_col]]),
    range_match_status = if (!is.na(matched_col)) as.character(dt[[matched_col]]) else NA_character_,
    range_lookup_error = if ("range_lookup_error" %in% names(dt)) as.character(dt$range_lookup_error) else NA_character_
  )

  status_text <- if (!is.na(status_col)) tolower(as.character(dt[[status_col]])) else ""
  out[, bien_range_available := !grepl("no|missing|absent|error|fail", status_text) & is.na(range_lookup_error)]
  out[, range_lookup_status := fifelse(bien_range_available, "available", fifelse(!is.na(range_lookup_error), "api_error", "missing"))]
  out[, range_match_status := fifelse(bien_range_available, "available", range_match_status)]

  unique(out)
}

cat("BIEN Range Availability Check\n")
cat("=============================\n\n")

species_universe <- as.data.table(read_parquet(species_universe_path))
to_check <- species_universe[needs_niche == TRUE & has_scientific_name == TRUE]
to_check[, bien_query_name := make_bien_query_name(scientific_name)]
to_check <- unique(to_check, by = "species_key")

if (!is.na(limit_arg)) {
  to_check <- head(to_check, limit_arg)
}

unique_names <- sort(unique(to_check$bien_query_name))
name_batches <- split(unique_names, ceiling(seq_along(unique_names) / batch_size))

cat(glue("Species to check: {format(nrow(to_check), big.mark = ',')}"), "\n")
cat(glue("Unique BIEN names: {format(length(unique_names), big.mark = ',')}"), "\n")
cat(glue("Batches: {length(name_batches)}"), "\n\n")

results <- vector("list", length(name_batches))
for (i in seq_along(name_batches)) {
  cat(glue("  Batch {i}/{length(name_batches)}: {length(name_batches[[i]])} names"), "\n")
  raw <- query_bien_batch(name_batches[[i]])
  normalized <- normalize_bien_result(raw)

  missing_from_result <- setdiff(name_batches[[i]], normalized$bien_query_name)
  if (length(missing_from_result) > 0) {
    normalized <- rbindlist(list(
      normalized,
      data.table(
        bien_query_name = missing_from_result,
        bien_range_available = FALSE,
        range_lookup_status = "missing",
        range_match_status = "not_returned",
        range_lookup_error = NA_character_
      )
    ), fill = TRUE)
  }

  results[[i]] <- normalized
  Sys.sleep(0.2)
}

range_lookup <- rbindlist(results, fill = TRUE)
range_lookup <- unique(range_lookup, by = "bien_query_name")

availability <- merge(to_check, range_lookup, by = "bien_query_name", all.x = TRUE)
availability[is.na(bien_range_available), bien_range_available := FALSE]
availability[is.na(range_lookup_status), range_lookup_status := "missing"]
availability[, needs_range_review := !bien_range_available | range_lookup_status == "api_error"]
availability[, range_review_reason := fifelse(
  needs_range_review,
  fifelse(range_lookup_status == "api_error", "BIEN API error", "No BIEN range returned"),
  NA_character_
)]

setcolorder(availability, c(
  "species_key", "source_code_system", "source_species_code",
  "scientific_name", "common_name", "genus", "specific_epithet",
  "community_layers", "growth_habits", "bien_query_name",
  "bien_range_available", "range_lookup_status", "range_match_status",
  "needs_range_review", "range_review_reason", "range_lookup_error"
))
setorder(availability, source_code_system, scientific_name, species_key)

write_parquet(as_tibble(availability), availability_path, compression = "snappy")

summary <- availability[, .(
  n_species = .N,
  n_available = sum(bien_range_available),
  n_missing = sum(!bien_range_available & range_lookup_status != "api_error"),
  n_api_error = sum(range_lookup_status == "api_error"),
  n_needs_review = sum(needs_range_review),
  pct_available = round(100 * mean(bien_range_available), 1)
)]
fwrite(summary, file.path(qa_dir, if (is.na(limit_arg)) "bien_range_availability_summary.csv" else sprintf("bien_range_availability_summary_limit_%d.csv", limit_arg)))

by_layer <- availability[, .(
  n_species = .N,
  n_available = sum(bien_range_available),
  pct_available = round(100 * mean(bien_range_available), 1)
), by = community_layers]
fwrite(by_layer, file.path(qa_dir, if (is.na(limit_arg)) "bien_range_availability_by_layer.csv" else sprintf("bien_range_availability_by_layer_limit_%d.csv", limit_arg)))

missing_species <- availability[needs_range_review == TRUE]
fwrite(missing_species, file.path(qa_dir, if (is.na(limit_arg)) "bien_range_missing_species.csv" else sprintf("bien_range_missing_species_limit_%d.csv", limit_arg)))

metadata_script <- here("scripts/utils/parquet_metadata.R")
if (file.exists(metadata_script) && is.na(limit_arg)) {
  source(metadata_script)
  write_parquet_metadata(availability_path, sample_size = Inf)
}

cat("\nDone.\n")
cat(glue("Availability parquet: {availability_path}"), "\n")
print(summary)
