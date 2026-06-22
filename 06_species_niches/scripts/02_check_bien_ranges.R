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
is_smoke_run <- !is.na(limit_arg)
batch_size <- as.integer(get_arg("--batch-size", "70"))

if (!requireNamespace("BIEN", quietly = TRUE)) {
  stop("Package 'BIEN' is required. Install it with install.packages('BIEN').")
}

config <- load_config()
niche_config <- config$processed$species_niches
processed_dir <- here(niche_config$output_dir)
smoke_data_dir <- here("06_species_niches/data/smoke")
qa_dir <- if (is_smoke_run) here("06_species_niches/qa/smoke") else here("06_species_niches/qa/outputs")
override_path <- here("06_species_niches/lookups/manual_bien_name_overrides_reviewed.csv")
dir_create(processed_dir)
if (is_smoke_run) dir_create(smoke_data_dir)
dir_create(qa_dir)

# Smoke runs prefer a smoke species universe if script 01 just produced one,
# but fall back to the production universe so this script can still be tested
# independently.
species_universe_path <- file.path(processed_dir, niche_config$files$species_universe)
if (is_smoke_run) {
  smoke_species_universe_path <- file.path(smoke_data_dir, sprintf("species_universe_limit_%d.parquet", limit_arg))
  if (file.exists(smoke_species_universe_path)) {
    species_universe_path <- smoke_species_universe_path
  }
}

availability_path <- file.path(
  if (is_smoke_run) smoke_data_dir else processed_dir,
  niche_config$files$bien_range_availability
)

if (is_smoke_run) {
  availability_path <- file.path(smoke_data_dir, sprintf("bien_range_availability_limit_%d.parquet", limit_arg))
}

if (!file.exists(species_universe_path)) {
  stop(glue("Species universe not found: {species_universe_path}"))
}

make_bien_query_name <- function(scientific_name) {
  gsub("\\s+", "_", trimws(as.character(scientific_name)))
}

read_ready_overrides <- function(path) {
  # Manual overrides are deliberately conservative. Only rows that have already
  # been reviewed and marked ready_for_pipeline are allowed to change the BIEN
  # query name. All other review rows remain documentation only.
  if (!file.exists(path)) {
    return(data.table(
      species_key = character(),
      source_scientific_name = character(),
      recommended_bien_name = character(),
      override_decision = character(),
      override_confidence = character(),
      override_review_status = character(),
      override_notes = character()
    ))
  }

  overrides <- fread(path)
  required_cols <- c(
    "species_key", "source_scientific_name", "recommended_bien_name",
    "decision", "confidence", "review_status", "notes"
  )
  missing_cols <- setdiff(required_cols, names(overrides))
  if (length(missing_cols) > 0) {
    stop(glue("Manual BIEN override table is missing column(s): {paste(missing_cols, collapse = ', ')}"))
  }

  ready <- overrides[
    review_status == "ready_for_pipeline" &
      !is.na(recommended_bien_name) &
      trimws(recommended_bien_name) != "",
    .(
      species_key,
      source_scientific_name,
      recommended_bien_name,
      override_decision = decision,
      override_confidence = confidence,
      override_review_status = review_status,
      override_notes = notes
    )
  ]

  if (anyDuplicated(ready$species_key)) {
    duplicated_keys <- unique(ready$species_key[duplicated(ready$species_key)])
    stop(glue("Manual BIEN override table has duplicate ready species_key value(s): {paste(duplicated_keys, collapse = ', ')}"))
  }

  ready
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

  status_col <- intersect(
    c(
      "range_map_available?", "range_map_available", "rangemapavailable",
      "range_lookup_status", "status", "match_status"
    ),
    names(dt)
  )[1]
  if (is.na(status_col)) {
    range_status_hits <- grep("range.*available", names(dt), value = TRUE)
    if (length(range_status_hits) > 0) status_col <- range_status_hits[[1]]
  }
  matched_col <- intersect(c("matched_species", "matched_name", "bien_species", "species_matched"), names(dt))[1]

  out <- data.table(
    bien_query_name = make_bien_query_name(dt[[query_col]]),
    range_match_status = if (!is.na(matched_col)) as.character(dt[[matched_col]]) else NA_character_,
    range_lookup_error = if ("range_lookup_error" %in% names(dt)) as.character(dt$range_lookup_error) else NA_character_
  )

  status_text <- if (!is.na(status_col)) tolower(trimws(as.character(dt[[status_col]]))) else NA_character_
  out[, bien_range_available := fifelse(
    is.na(status_text),
    FALSE,
    status_text %in% c("yes", "y", "true", "available", "1")
  ) & is.na(range_lookup_error)]
  out[, range_lookup_status := fifelse(bien_range_available, "available", fifelse(!is.na(range_lookup_error), "api_error", "missing"))]
  out[, range_match_status := fifelse(bien_range_available, "available", range_match_status)]

  unique(out)
}

cat("BIEN Range Availability Check\n")
cat("=============================\n\n")

species_universe <- as.data.table(read_parquet(species_universe_path))
to_check <- species_universe[needs_niche == TRUE & has_scientific_name == TRUE]
to_check <- unique(to_check, by = "species_key")

ready_overrides <- read_ready_overrides(override_path)
to_check <- merge(to_check, ready_overrides, by = "species_key", all.x = TRUE)
to_check[, original_bien_query_name := make_bien_query_name(scientific_name)]
to_check[, bien_query_scientific_name := fifelse(
  !is.na(recommended_bien_name) & trimws(recommended_bien_name) != "",
  recommended_bien_name,
  scientific_name
)]
to_check[, bien_query_name := make_bien_query_name(bien_query_scientific_name)]
to_check[, uses_manual_bien_override := !is.na(recommended_bien_name) & trimws(recommended_bien_name) != ""]
to_check[, manual_bien_override_name := fifelse(uses_manual_bien_override, recommended_bien_name, NA_character_)]

if (nrow(ready_overrides) > 0) {
  unused_overrides <- ready_overrides[!species_key %in% to_check$species_key]
  if (nrow(unused_overrides) > 0) {
    fwrite(
      unused_overrides,
      file.path(qa_dir, if (!is_smoke_run) "manual_bien_overrides_unused.csv" else sprintf("manual_bien_overrides_unused_limit_%d.csv", limit_arg))
    )
  }
}

if (!is.na(limit_arg)) {
  to_check <- head(to_check, limit_arg)
}

unique_names <- sort(unique(to_check$bien_query_name))
name_batches <- split(unique_names, ceiling(seq_along(unique_names) / batch_size))

cat(glue("Species to check: {format(nrow(to_check), big.mark = ',')}"), "\n")
cat(glue("Unique BIEN names: {format(length(unique_names), big.mark = ',')}"), "\n")
cat(glue("Manual ready overrides applied: {format(sum(to_check$uses_manual_bien_override, na.rm = TRUE), big.mark = ',')}"), "\n")
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
  "community_layers", "growth_habits",
  "original_bien_query_name", "bien_query_scientific_name",
  "bien_query_name", "uses_manual_bien_override",
  "manual_bien_override_name", "override_decision",
  "override_confidence", "override_review_status",
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
  n_manual_overrides = sum(uses_manual_bien_override, na.rm = TRUE),
  n_manual_overrides_available = sum(uses_manual_bien_override & bien_range_available, na.rm = TRUE),
  pct_available = round(100 * mean(bien_range_available), 1)
)]
fwrite(summary, file.path(qa_dir, if (!is_smoke_run) "bien_range_availability_summary.csv" else sprintf("bien_range_availability_summary_limit_%d.csv", limit_arg)))

by_layer <- availability[, .(
  n_species = .N,
  n_available = sum(bien_range_available),
  pct_available = round(100 * mean(bien_range_available), 1)
), by = community_layers]
fwrite(by_layer, file.path(qa_dir, if (!is_smoke_run) "bien_range_availability_by_layer.csv" else sprintf("bien_range_availability_by_layer_limit_%d.csv", limit_arg)))

missing_species <- availability[needs_range_review == TRUE]
fwrite(missing_species, file.path(qa_dir, if (!is_smoke_run) "bien_range_missing_species.csv" else sprintf("bien_range_missing_species_limit_%d.csv", limit_arg)))

manual_override_audit <- availability[
  uses_manual_bien_override == TRUE,
  .(
    species_key, source_code_system, source_species_code,
    scientific_name, common_name, original_bien_query_name,
    manual_bien_override_name, bien_query_name,
    override_decision, override_confidence, override_review_status,
    bien_range_available, range_lookup_status, range_review_reason
  )
]
fwrite(
  manual_override_audit,
  file.path(qa_dir, if (!is_smoke_run) "manual_bien_overrides_applied.csv" else sprintf("manual_bien_overrides_applied_limit_%d.csv", limit_arg))
)

metadata_script <- here("scripts/utils/parquet_metadata.R")
if (file.exists(metadata_script) && !is_smoke_run) {
  source(metadata_script)
  write_parquet_metadata(availability_path, sample_size = Inf)
}

cat("\nDone.\n")
cat(glue("Availability parquet: {availability_path}"), "\n")
print(summary)
