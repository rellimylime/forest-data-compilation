# ==============================================================================
# 09_build_species_taxon_crosswalk.R
# Build a source-code-to-resolved-taxon crosswalk for species niche QA.
#
# The project has two different identities that should not be conflated:
#   - species_key: source-specific FIA SPCD or P2VEG/NRCS code used for joins.
#   - resolved_taxon_key: biological taxon used for species-level counting.
#
# This script does not change the observation products or CWM calculations. It
# creates a crosswalk and summaries so reports can distinguish "source codes
# missing" from "biological taxa missing."
#
# Usage:
#   Rscript 06_species_niches/qa/09_build_species_taxon_crosswalk.R
# ==============================================================================

suppressPackageStartupMessages({
  library(here)
  library(glue)
  library(data.table)
  library(arrow)
  library(sf)
  library(fs)
  library(tibble)
})

source(here("scripts/utils/load_config.R"))

config <- load_config()
niche_dir <- here(config$processed$species_niches$output_dir)
qa_dir <- here("06_species_niches/qa/outputs")
dir_create(niche_dir)
dir_create(qa_dir)

paths <- list(
  species_universe = file.path(niche_dir, "species_universe.parquet"),
  bien_availability = file.path(niche_dir, "bien_range_availability.parquet"),
  species_range_polygons = file.path(niche_dir, "species_range_polygons.gpkg"),
  global_niches = file.path(niche_dir, "species_climate_niches.parquet"),
  study_area_niches = file.path(niche_dir, "species_climate_niches_us_study_area.parquet"),
  tnrs_candidate_check = file.path(qa_dir, "tnrs_candidate_bien_range_check.csv"),
  cwm_missing = here("07_thermophilization/qa/outputs/plot_recruitment_cwm_missing_species.csv")
)

required <- c("species_universe", "bien_availability")
missing_inputs <- required[!file.exists(unlist(paths[required]))]
if (length(missing_inputs) > 0) {
  stop(glue("Missing required input(s): {paste(missing_inputs, collapse = ', ')}"))
}

clean_text <- function(x) {
  x <- as.character(x)
  x <- trimws(x)
  x[x == ""] <- NA_character_
  x
}

make_taxon_key <- function(scientific_name) {
  normalized <- tolower(clean_text(scientific_name))
  normalized <- gsub("[^a-z0-9]+", "_", normalized)
  normalized <- gsub("^_|_$", "", normalized)
  fifelse(
    is.na(normalized) | normalized == "",
    NA_character_,
    paste0("taxon:", normalized)
  )
}

read_optional_csv <- function(path) {
  if (!file.exists(path)) return(data.table())
  fread(path)
}

read_optional_parquet_keys <- function(path) {
  if (!file.exists(path)) return(character())
  unique(as.data.table(read_parquet(path))$species_key)
}

cat("Species Taxon Crosswalk Build\n")
cat("=============================\n\n")

universe <- as.data.table(read_parquet(paths$species_universe))
availability <- as.data.table(read_parquet(paths$bien_availability))

availability_keep <- availability[
  ,
  .(
    species_key,
    original_bien_query_name,
    bien_query_scientific_name,
    bien_query_name,
    current_niche_taxon_name = niche_taxon_name,
    current_niche_taxon_key = niche_taxon_key,
    uses_manual_bien_override,
    manual_bien_override_name,
    override_decision,
    override_confidence,
    override_review_status,
    bien_range_available,
    range_lookup_status,
    range_review_reason
  )
]

crosswalk <- merge(universe, availability_keep, by = "species_key", all.x = TRUE)

crosswalk[, source_taxon_name := clean_text(scientific_name)]
crosswalk[, source_taxon_key := make_taxon_key(source_taxon_name)]

# The current resolved taxon is the taxon that the existing pipeline actually
# uses after reviewed manual overrides. For non-target/pseudo records, it is NA.
crosswalk[, resolved_taxon_name := fifelse(
  needs_niche == TRUE,
  clean_text(current_niche_taxon_name),
  NA_character_
)]
crosswalk[, resolved_taxon_key := fifelse(
  needs_niche == TRUE,
  clean_text(current_niche_taxon_key),
  NA_character_
)]

# Older or partially regenerated products may have missing niche_taxon fields
# even when the BIEN query name is present. Fill those for counting only; this
# does not alter the upstream availability product.
crosswalk[
  needs_niche == TRUE & (is.na(resolved_taxon_name) | is.na(resolved_taxon_key)),
  resolved_taxon_name := clean_text(bien_query_scientific_name)
]
crosswalk[
  needs_niche == TRUE & is.na(resolved_taxon_key),
  resolved_taxon_key := make_taxon_key(resolved_taxon_name)
]

tnrs_check <- read_optional_csv(paths$tnrs_candidate_check)
if (nrow(tnrs_check) > 0) {
  tnrs_keep <- tnrs_check[
    ,
    .(
      species_key,
      tnrs_review_class,
      candidate_bien_query_name,
      candidate_bien_range_available,
      candidate_range_lookup_status,
      candidate_range_match_status,
      Taxonomic_status,
      Accepted_name,
      Accepted_species,
      Warnings
    )
  ]
  tnrs_keep[, candidate_taxon_name := gsub("_", " ", clean_text(candidate_bien_query_name))]
  tnrs_keep[, candidate_taxon_key := make_taxon_key(candidate_taxon_name)]
  crosswalk <- merge(crosswalk, tnrs_keep, by = "species_key", all.x = TRUE)
} else {
  crosswalk[
    ,
    `:=`(
      tnrs_review_class = NA_character_,
      candidate_bien_query_name = NA_character_,
      candidate_bien_range_available = NA,
      candidate_range_lookup_status = NA_character_,
      candidate_range_match_status = NA_character_,
      Taxonomic_status = NA_character_,
      Accepted_name = NA_character_,
      Accepted_species = NA_character_,
      Warnings = NA_character_,
      candidate_taxon_name = NA_character_,
      candidate_taxon_key = NA_character_
    )
  ]
}

global_niche_species <- read_optional_parquet_keys(paths$global_niches)
study_area_niche_species <- read_optional_parquet_keys(paths$study_area_niches)

crosswalk[, has_global_niche_current := species_key %in% global_niche_species]
crosswalk[, has_study_area_niche_current := species_key %in% study_area_niche_species]

cwm_missing <- read_optional_csv(paths$cwm_missing)
if (nrow(cwm_missing) > 0) {
  cwm_keep <- cwm_missing[
    ,
    .(
      species_key,
      cwm_missing_condition_rows = n_condition_rows,
      cwm_missing_weight = cwm_weight_total
    )
  ]
  crosswalk <- merge(crosswalk, cwm_keep, by = "species_key", all.x = TRUE)
} else {
  crosswalk[, `:=`(cwm_missing_condition_rows = NA_integer_, cwm_missing_weight = NA_real_)]
}
crosswalk[is.na(cwm_missing_weight), cwm_missing_weight := 0]
crosswalk[is.na(cwm_missing_condition_rows), cwm_missing_condition_rows := 0L]

if (file.exists(paths$species_range_polygons)) {
  polygons <- st_read(paths$species_range_polygons, quiet = TRUE)
  polygons$species_key <- as.character(polygons$species_key)
  polygon_dt <- as.data.table(st_drop_geometry(polygons))
  if (!"range_area_global_equal_km2_qa" %in% names(polygon_dt) &&
      "range_area_km2_qa" %in% names(polygon_dt)) {
    polygon_dt[, range_area_global_equal_km2_qa := range_area_km2_qa]
  }
  polygon_cols <- intersect(
    c(
      "species_key", "range_area_global_equal_km2_qa",
      "range_area_geodesic_km2_qa", "range_longitude_span",
      "range_latitude_span", "range_extent_review"
    ),
    names(polygon_dt)
  )
  crosswalk <- merge(crosswalk, polygon_dt[, ..polygon_cols], by = "species_key", all.x = TRUE)
} else {
  crosswalk[
    ,
    `:=`(
      range_area_global_equal_km2_qa = NA_real_,
      range_area_geodesic_km2_qa = NA_real_,
      range_longitude_span = NA_real_,
      range_latitude_span = NA_real_,
      range_extent_review = NA
    )
  ]
}

crosswalk[, is_source_code_duplicate_taxon := duplicated(source_taxon_key) | duplicated(source_taxon_key, fromLast = TRUE)]
crosswalk[
  ,
  n_source_codes_for_source_taxon := .N,
  by = source_taxon_key
]
crosswalk[
  !is.na(resolved_taxon_key),
  n_source_codes_for_resolved_taxon := .N,
  by = resolved_taxon_key
]
crosswalk[is.na(n_source_codes_for_resolved_taxon), n_source_codes_for_resolved_taxon := 0L]

crosswalk[, is_small_range_review := !is.na(range_area_global_equal_km2_qa) & range_area_global_equal_km2_qa < 1000]
crosswalk[, is_global_fallback_only_current := has_global_niche_current == TRUE & has_study_area_niche_current == FALSE]
crosswalk[, range_quality_review := fifelse(
  is_global_fallback_only_current & is_small_range_review,
  "global_fallback_small_range_review",
  fifelse(
    is_small_range_review,
    "small_range_review",
    fifelse(range_extent_review == TRUE, "broad_extent_review", "none")
  )
)]

crosswalk[, taxon_resolution_status := fcase(
  needs_niche != TRUE | is.na(needs_niche),
  "not_species_level_or_not_targeted",
  bien_range_available == TRUE & uses_manual_bien_override == TRUE,
  "reviewed_override_has_bien_range",
  bien_range_available == TRUE,
  "source_name_has_bien_range",
  candidate_bien_range_available == TRUE,
  "tnrs_candidate_has_bien_range_unreviewed",
  !is.na(candidate_range_lookup_status) & candidate_range_lookup_status == "missing",
  "tnrs_candidate_missing_bien_range",
  range_lookup_status == "api_error",
  "bien_api_error",
  default = "no_bien_range_for_current_taxon"
)]

crosswalk[, taxon_count_key := fcase(
  taxon_resolution_status == "not_species_level_or_not_targeted",
  NA_character_,
  taxon_resolution_status == "tnrs_candidate_has_bien_range_unreviewed",
  candidate_taxon_key,
  default = resolved_taxon_key
)]
crosswalk[, taxon_count_name := fcase(
  taxon_resolution_status == "not_species_level_or_not_targeted",
  NA_character_,
  taxon_resolution_status == "tnrs_candidate_has_bien_range_unreviewed",
  candidate_taxon_name,
  default = resolved_taxon_name
)]

preferred_order <- c(
  "species_key", "source_code_system", "source_species_code",
  "scientific_name", "common_name", "source_taxon_name", "source_taxon_key",
  "resolved_taxon_name", "resolved_taxon_key", "taxon_count_name", "taxon_count_key",
  "taxon_resolution_status", "tnrs_review_class",
  "candidate_taxon_name", "candidate_taxon_key", "candidate_bien_range_available",
  "bien_range_available", "range_lookup_status", "range_review_reason",
  "has_study_area_niche_current", "has_global_niche_current",
  "range_quality_review", "range_area_global_equal_km2_qa",
  "n_source_codes_for_source_taxon", "n_source_codes_for_resolved_taxon",
  "source_tables", "community_layers", "growth_habits", "states_present",
  "n_states", "n_plot_visits", "n_conditions", "n_source_rows",
  "abundance_total", "cwm_missing_weight"
)
setcolorder(crosswalk, c(intersect(preferred_order, names(crosswalk)), setdiff(names(crosswalk), preferred_order)))
setorder(crosswalk, taxon_resolution_status, taxon_count_name, species_key)

crosswalk_path <- file.path(niche_dir, "species_niche_taxon_crosswalk.parquet")
write_parquet(as_tibble(crosswalk), crosswalk_path, compression = "snappy")

taxon_summary <- crosswalk[
  ,
  .(
    n_source_codes = .N,
    n_unique_source_taxa = uniqueN(source_taxon_key, na.rm = TRUE),
    n_unique_reporting_taxa = uniqueN(taxon_count_key, na.rm = TRUE),
    total_abundance = sum(abundance_total, na.rm = TRUE),
    total_cwm_missing_weight = sum(cwm_missing_weight, na.rm = TRUE)
  ),
  by = taxon_resolution_status
][order(taxon_resolution_status)]

missing_taxon_summary <- crosswalk[
  taxon_resolution_status != "source_name_has_bien_range" &
    taxon_resolution_status != "reviewed_override_has_bien_range",
  .(
    n_source_codes = .N,
    n_unique_source_taxa = uniqueN(source_taxon_key, na.rm = TRUE),
    n_unique_reporting_taxa = uniqueN(taxon_count_key, na.rm = TRUE),
    total_cwm_missing_weight = sum(cwm_missing_weight, na.rm = TRUE)
  ),
  by = .(taxon_resolution_status, tnrs_review_class)
][order(taxon_resolution_status, tnrs_review_class)]

duplicate_taxa <- crosswalk[
  !is.na(source_taxon_key) & n_source_codes_for_source_taxon > 1,
  .(
    source_taxon_name = first(source_taxon_name),
    n_source_codes = .N,
    source_keys = paste(sort(species_key), collapse = ";"),
    source_systems = paste(sort(unique(source_code_system)), collapse = ";"),
    community_layers = paste(sort(unique(unlist(strsplit(na.omit(community_layers), ";", fixed = TRUE)))), collapse = ";"),
    any_bien_range_available = any(bien_range_available == TRUE, na.rm = TRUE),
    any_candidate_bien_range_available = any(candidate_bien_range_available == TRUE, na.rm = TRUE),
    total_abundance = sum(abundance_total, na.rm = TRUE),
    total_cwm_missing_weight = sum(cwm_missing_weight, na.rm = TRUE)
  ),
  by = source_taxon_key
][order(-n_source_codes, source_taxon_name)]

fwrite(taxon_summary, file.path(qa_dir, "species_taxon_resolution_summary.csv"))
fwrite(missing_taxon_summary, file.path(qa_dir, "species_taxon_missing_summary.csv"))
fwrite(duplicate_taxa, file.path(qa_dir, "species_taxon_duplicate_source_codes.csv"))

cat("Done.\n")
cat(glue("Crosswalk:          {crosswalk_path}"), "\n")
cat(glue("Summary:            {file.path(qa_dir, 'species_taxon_resolution_summary.csv')}"), "\n")
cat(glue("Missing summary:    {file.path(qa_dir, 'species_taxon_missing_summary.csv')}"), "\n")
cat(glue("Duplicate taxa:     {file.path(qa_dir, 'species_taxon_duplicate_source_codes.csv')}"), "\n\n")

print(taxon_summary)
