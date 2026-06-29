# ==============================================================================
# 07_check_tnrs_candidate_bien_ranges.R
# Test whether TNRS candidate names for BIEN-missing taxa have BIEN ranges.
#
# This script does not apply any overrides. It only asks: among species currently
# missing BIEN ranges, how many non-variety names have a TNRS accepted/synonym
# candidate that BIEN can map?
#
# Usage:
#   Rscript 06_species_niches/qa/07_check_tnrs_candidate_bien_ranges.R
#   Rscript 06_species_niches/qa/07_check_tnrs_candidate_bien_ranges.R --limit=50
# ==============================================================================

suppressPackageStartupMessages({
  library(here)
  library(glue)
  library(data.table)
  library(fs)
})

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

qa_dir <- here("06_species_niches/qa/outputs")
dir_create(qa_dir)

paths <- list(
  bien_missing = file.path(qa_dir, "bien_range_missing_species.csv"),
  tnrs_candidates = file.path(qa_dir, "tnrs_bien_missing_name_review_candidates.csv")
)

missing_inputs <- names(paths)[!file.exists(unlist(paths))]
if (length(missing_inputs) > 0) {
  stop(glue("Missing required input(s): {paste(missing_inputs, collapse = ', ')}"))
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
      candidate_bien_query_name = character(),
      candidate_bien_range_available = logical(),
      candidate_range_lookup_status = character(),
      candidate_range_match_status = character(),
      candidate_range_lookup_error = character()
    ))
  }

  names(dt) <- tolower(names(dt))
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
    candidate_bien_query_name = make_bien_query_name(dt[[query_col]]),
    candidate_range_match_status = if (!is.na(matched_col)) as.character(dt[[matched_col]]) else NA_character_,
    candidate_range_lookup_error = if ("range_lookup_error" %in% names(dt)) as.character(dt$range_lookup_error) else NA_character_
  )

  status_text <- if (!is.na(status_col)) {
    tolower(trimws(as.character(dt[[status_col]])))
  } else {
    NA_character_
  }

  out[, candidate_bien_range_available := fifelse(
    is.na(status_text),
    FALSE,
    status_text %in% c("yes", "y", "true", "available", "1")
  ) & is.na(candidate_range_lookup_error)]
  out[, candidate_range_lookup_status := fifelse(
    candidate_bien_range_available,
    "available",
    fifelse(!is.na(candidate_range_lookup_error), "api_error", "missing")
  )]
  out[, candidate_range_match_status := fifelse(
    candidate_bien_range_available,
    "available",
    candidate_range_match_status
  )]

  unique(out)
}

cat("TNRS Candidate BIEN Range Check\n")
cat("===============================\n\n")

bien_missing <- fread(paths$bien_missing)
tnrs_candidates <- fread(paths$tnrs_candidates)

if (!"species_key" %in% names(tnrs_candidates)) {
  stop("TNRS candidate table must contain species_key.")
}
if (!"candidate_bien_query_name" %in% names(tnrs_candidates)) {
  stop("TNRS candidate table must contain candidate_bien_query_name.")
}

review <- merge(
  bien_missing,
  tnrs_candidates[
    ,
    .(
      species_key,
      Overall_score,
      Name_matched,
      Name_score,
      Name_matched_rank,
      Taxonomic_status,
      Accepted_name,
      Accepted_species,
      Accepted_name_rank,
      Accepted_family,
      Source,
      Warnings,
      Unmatched_terms,
      tnrs_review_class,
      candidate_bien_query_name,
      override_review_status
    )
  ],
  by = "species_key",
  all.x = TRUE,
  suffixes = c("", "_tnrs")
)

# Infraspecific source names can have valid parent-species candidates, but using
# the broader species range is an ecological judgment call. Keep them visible in
# this diagnostic, but do not count them as automatic TNRS/BIEN rescue
# candidates.
review[, is_infraspecific_name := grepl("\\b(var|ssp|subsp)\\.", scientific_name, ignore.case = TRUE)]
review[, has_candidate_name := !is.na(candidate_bien_query_name) & trimws(candidate_bien_query_name) != ""]
review[, candidate_bien_query_name := fifelse(
  has_candidate_name,
  make_bien_query_name(candidate_bien_query_name),
  NA_character_
)]

to_test <- review[is_infraspecific_name == FALSE & has_candidate_name == TRUE]
if (!is.na(limit_arg)) {
  to_test <- head(to_test, limit_arg)
}

unique_names <- sort(unique(to_test$candidate_bien_query_name))
name_batches <- split(unique_names, ceiling(seq_along(unique_names) / batch_size))

cat(glue("BIEN-missing species: {format(nrow(bien_missing), big.mark = ',')}"), "\n")
cat(glue("Non-infraspecific BIEN-missing species: {format(sum(review$is_infraspecific_name == FALSE), big.mark = ',')}"), "\n")
cat(glue("Non-infraspecific rows with TNRS candidate names: {format(nrow(to_test), big.mark = ',')}"), "\n")
cat(glue("Unique candidate names to test: {format(length(unique_names), big.mark = ',')}"), "\n\n")

results <- vector("list", length(name_batches))
for (i in seq_along(name_batches)) {
  cat(glue("  Batch {i}/{length(name_batches)}: {length(name_batches[[i]])} names"), "\n")
  raw <- query_bien_batch(name_batches[[i]])
  results[[i]] <- normalize_bien_result(raw)
}

range_results <- rbindlist(results, fill = TRUE)
if (nrow(range_results) == 0) {
  range_results <- data.table(
    candidate_bien_query_name = character(),
    candidate_bien_range_available = logical(),
    candidate_range_lookup_status = character(),
    candidate_range_match_status = character(),
    candidate_range_lookup_error = character()
  )
}

review <- merge(review, range_results, by = "candidate_bien_query_name", all.x = TRUE)
review[has_candidate_name == FALSE, candidate_range_lookup_status := "no_tnrs_candidate"]
review[is_infraspecific_name == TRUE, candidate_range_lookup_status := "not_tested_infraspecific_name"]
review[has_candidate_name == FALSE | is_infraspecific_name == TRUE, candidate_bien_range_available := FALSE]
review[has_candidate_name == FALSE | is_infraspecific_name == TRUE, candidate_range_match_status := NA_character_]
review[has_candidate_name == FALSE | is_infraspecific_name == TRUE, candidate_range_lookup_error := NA_character_]

summary <- review[
  ,
  .(
    n_species = .N,
    n_with_tnrs_candidate = sum(has_candidate_name == TRUE, na.rm = TRUE),
    n_candidate_bien_available = sum(candidate_bien_range_available == TRUE, na.rm = TRUE)
  ),
  by = .(is_infraspecific_name, tnrs_review_class, candidate_range_lookup_status)
][order(is_infraspecific_name, tnrs_review_class, candidate_range_lookup_status)]

available_candidates <- review[
  is_infraspecific_name == FALSE & candidate_bien_range_available == TRUE
][order(-abundance_total, species_key)]

out_all <- file.path(qa_dir, "tnrs_candidate_bien_range_check.csv")
out_available <- file.path(qa_dir, "tnrs_candidate_bien_range_available.csv")
out_summary <- file.path(qa_dir, "tnrs_candidate_bien_range_summary.csv")
out_name_pairs <- file.path(qa_dir, "tnrs_candidate_name_pairs_available.csv")

fwrite(review[order(is_infraspecific_name, -abundance_total, species_key)], out_all)
fwrite(available_candidates, out_available)
fwrite(summary, out_summary)
fwrite(
  available_candidates[
    ,
    .(
      original_name = scientific_name,
      candidate_name = gsub("_", " ", candidate_bien_query_name),
      taxonomic_status = Taxonomic_status,
      review_class = tnrs_review_class,
      species_key
    )
  ][order(original_name, candidate_name, species_key)],
  out_name_pairs
)

cat("\nDone.\n")
cat(glue("Full check:          {out_all}"), "\n")
cat(glue("Available candidates:{out_available}"), "\n")
cat(glue("Name pairs:          {out_name_pairs}"), "\n")
cat(glue("Summary:             {out_summary}"), "\n\n")

print(summary)
