# ==============================================================================
# 01_validate_species_niche_workflow.R
# Validate the species niche workflow handoffs.
#
# This script is intended to be run after scripts 01-03, and again after scripts
# 04-05 when range climate and compact niche products are regenerated.
#
# It writes machine-readable QA outputs and exits with an error if any required
# structural check fails. Review-level issues are written as warnings and CSVs.
#
# Usage:
#   Rscript 06_species_niches/qa/01_validate_species_niche_workflow.R
# ==============================================================================

suppressPackageStartupMessages({
  library(here)
  library(glue)
  library(data.table)
  library(arrow)
  library(sf)
  library(fs)
  library(tools)
})

source(here("scripts/utils/load_config.R"))

config <- load_config()
niche_config <- config$processed$species_niches
processed_dir <- here(niche_config$output_dir)
qa_dir <- here("06_species_niches/qa/outputs")
dir_create(qa_dir)

paths <- list(
  species_universe = file.path(processed_dir, "species_universe.parquet"),
  bien_availability = file.path(processed_dir, "bien_range_availability.parquet"),
  species_range_polygons = file.path(processed_dir, "species_range_polygons.gpkg"),
  range_climate_global = file.path(processed_dir, "species_range_climate.parquet"),
  range_climate_us = file.path(processed_dir, "species_range_climate_us_study_area.parquet"),
  niches_global = file.path(processed_dir, "species_climate_niches.parquet"),
  niches_us = file.path(processed_dir, "species_climate_niches_us_study_area.parquet"),
  cwm = here("07_thermophilization/data/processed/plot_recruitment_cwm.parquet")
)

add_check <- function(checks, check_name, status, severity, observed, expected,
                      details = NA_character_) {
  rbind(
    checks,
    data.table(
      check_name = check_name,
      status = status,
      severity = severity,
      observed = as.character(observed),
      expected = as.character(expected),
      details = as.character(details)
    ),
    fill = TRUE
  )
}

`%||%` <- function(x, y) {
  if (is.null(x) || length(x) == 0 || all(is.na(x))) y else x
}

status_from <- function(ok) if (isTRUE(ok)) "pass" else "fail"

file_meta <- function(path) {
  if (!file.exists(path)) {
    return(data.table(
      path = path,
      exists = FALSE,
      size_bytes = NA_real_,
      modified_time = as.POSIXct(NA),
      md5 = NA_character_
    ))
  }

  info <- file.info(path)
  data.table(
    path = path,
    exists = TRUE,
    size_bytes = as.numeric(info$size),
    modified_time = info$mtime,
    md5 = unname(md5sum(path))
  )
}

read_optional_parquet <- function(path) {
  if (!file.exists(path)) return(NULL)
  as.data.table(read_parquet(path))
}

cat("Species Niche Workflow Validation\n")
cat("=================================\n\n")

checks <- data.table()

for (nm in names(paths)[1:3]) {
  checks <- add_check(
    checks,
    check_name = paste0("required_file_exists_", nm),
    status = status_from(file.exists(paths[[nm]])),
    severity = "error",
    observed = file.exists(paths[[nm]]),
    expected = TRUE,
    details = paths[[nm]]
  )
}

missing_required <- checks[severity == "error" & status == "fail"]
if (nrow(missing_required) > 0) {
  fwrite(checks, file.path(qa_dir, "species_niche_validation_checks.csv"))
  stop("Required species niche files are missing. See species_niche_validation_checks.csv.")
}

universe <- as.data.table(read_parquet(paths$species_universe))
availability <- as.data.table(read_parquet(paths$bien_availability))
polygons <- suppressWarnings(st_read(paths$species_range_polygons, quiet = TRUE))

layer_cols <- intersect(
  c("in_seedlings", "in_saplings", "in_trees", "in_shrubs", "in_forbs", "in_graminoids", "in_p2veg_tree_layers"),
  names(universe)
)

make_layer_coverage <- function(universe_dt, availability_dt) {
  if (length(layer_cols) == 0) return(data.table())

  out <- rbindlist(lapply(layer_cols, function(layer_col) {
    layer_species <- universe_dt[get(layer_col) == TRUE]
    if (nrow(layer_species) == 0) return(NULL)

    layer_availability <- availability_dt[species_key %in% layer_species$species_key]
    data.table(
      layer = sub("^in_", "", layer_col),
      n_species_universe = nrow(layer_species),
      n_species_targets = sum(layer_species$needs_niche == TRUE, na.rm = TRUE),
      n_not_targeted = sum(layer_species$needs_niche != TRUE | is.na(layer_species$needs_niche)),
      n_bien_available = sum(layer_availability$bien_range_available == TRUE, na.rm = TRUE),
      n_bien_missing = sum(layer_availability$bien_range_available == FALSE, na.rm = TRUE),
      pct_targeted = round(100 * sum(layer_species$needs_niche == TRUE, na.rm = TRUE) / nrow(layer_species), 1),
      pct_bien_available_of_targets = round(100 * sum(layer_availability$bien_range_available == TRUE, na.rm = TRUE) /
        max(1, sum(layer_species$needs_niche == TRUE, na.rm = TRUE)), 1)
    )
  }), fill = TRUE)

  out[order(layer)]
}

# ------------------------------------------------------------------------------
# Species universe checks
# ------------------------------------------------------------------------------

required_universe_cols <- c(
  "species_key", "source_code_system", "source_species_code",
  "scientific_name", "community_layers", "is_pseudo_taxon",
  "has_scientific_name", "needs_niche"
)
missing_universe_cols <- setdiff(required_universe_cols, names(universe))
checks <- add_check(
  checks,
  "species_universe_required_columns",
  status_from(length(missing_universe_cols) == 0),
  "error",
  paste(missing_universe_cols, collapse = ";"),
  "none missing"
)

checks <- add_check(
  checks,
  "species_universe_unique_species_key",
  status_from(!anyDuplicated(universe$species_key)),
  "error",
  uniqueN(universe$species_key),
  nrow(universe),
  "species_key should be one row per source species code"
)

universe[, word_count := lengths(strsplit(tolower(trimws(scientific_name)), "\\s+"))]
bad_genus_targets <- universe[needs_niche == TRUE & word_count < 2]
checks <- add_check(
  checks,
  "species_universe_no_genus_only_niche_targets",
  status_from(nrow(bad_genus_targets) == 0),
  "error",
  nrow(bad_genus_targets),
  0,
  "Genus-only records cannot support species-level BIEN niches"
)

bad_pseudo_targets <- universe[needs_niche == TRUE & is_pseudo_taxon == TRUE]
checks <- add_check(
  checks,
  "species_universe_no_pseudo_niche_targets",
  status_from(nrow(bad_pseudo_targets) == 0),
  "error",
  nrow(bad_pseudo_targets),
  0
)

source_coverage <- merge(
  universe[
    ,
    .(
      n_species_universe = .N,
      n_species_targets = sum(needs_niche == TRUE, na.rm = TRUE),
      n_pseudo_or_aggregate = sum(is_pseudo_taxon == TRUE, na.rm = TRUE),
      total_abundance_for_universe = sum(abundance_total, na.rm = TRUE)
    ),
    by = source_code_system
  ],
  availability[
    ,
    .(
      n_bien_available = sum(bien_range_available == TRUE, na.rm = TRUE),
      n_bien_missing = sum(bien_range_available == FALSE, na.rm = TRUE)
    ),
    by = source_code_system
  ],
  by = "source_code_system",
  all.x = TRUE
)
source_coverage[
  ,
  `:=`(
    n_bien_available = fifelse(is.na(n_bien_available), 0L, n_bien_available),
    n_bien_missing = fifelse(is.na(n_bien_missing), 0L, n_bien_missing)
  )
]
source_coverage[, pct_bien_available_of_targets := round(100 * n_bien_available / pmax(1, n_species_targets), 1)]

layer_coverage <- make_layer_coverage(universe, availability)

major_layers <- c("seedlings", "trees", "shrubs", "forbs", "graminoids")
missing_major_layers <- setdiff(major_layers, layer_coverage[n_species_universe > 0, layer])
checks <- add_check(
  checks,
  "species_universe_major_layers_present",
  status_from(length(missing_major_layers) == 0),
  "error",
  paste(missing_major_layers, collapse = ";"),
  "none missing",
  "The universe should include tree regeneration plus P2VEG shrubs, forbs, and graminoids."
)

sapling_layer_rows <- layer_coverage[layer == "saplings", n_species_universe]
checks <- add_check(
  checks,
  "species_universe_sapling_layer_review",
  status_from(length(sapling_layer_rows) > 0 && sapling_layer_rows > 0),
  "warning",
  if (length(sapling_layer_rows) == 0) 0 else sapling_layer_rows,
  "> 0",
  "Saplings may be represented inside TREE summaries rather than as a distinct species-universe layer. Review before claiming sapling-specific coverage."
)

fwrite(
  bad_genus_targets[, .(
    species_key, source_code_system, source_species_code,
    scientific_name, common_name, community_layers
  )],
  file.path(qa_dir, "species_universe_bad_genus_targets.csv")
)

# ------------------------------------------------------------------------------
# BIEN availability checks
# ------------------------------------------------------------------------------

target_keys <- universe[needs_niche == TRUE, species_key]
availability_keys <- availability$species_key

availability_extra <- availability[!species_key %in% target_keys]
target_missing_availability <- universe[needs_niche == TRUE & !species_key %in% availability_keys]

checks <- add_check(
  checks,
  "bien_availability_one_row_per_niche_target",
  status_from(nrow(availability_extra) == 0 && nrow(target_missing_availability) == 0),
  "error",
  glue("extra={nrow(availability_extra)}; missing={nrow(target_missing_availability)}"),
  "extra=0; missing=0"
)

checks <- add_check(
  checks,
  "bien_availability_unique_species_key",
  status_from(!anyDuplicated(availability$species_key)),
  "error",
  uniqueN(availability$species_key),
  nrow(availability)
)

checks <- add_check(
  checks,
  "bien_availability_has_missing_ranges",
  status_from(any(availability$bien_range_available == FALSE, na.rm = TRUE)),
  "warning",
  sum(availability$bien_range_available == FALSE, na.rm = TRUE),
  "> 0",
  "If this is zero, the BIEN status parser is probably broken"
)

missing_fraction <- mean(availability$bien_range_available == FALSE, na.rm = TRUE)
checks <- add_check(
  checks,
  "bien_missing_fraction_review",
  status_from(missing_fraction <= 0.25),
  "warning",
  paste0(round(100 * missing_fraction, 1), "%"),
  "<= 25%",
  "A higher missing fraction is not automatically wrong, but it needs explicit documentation and high-weight species review."
)

bad_availability_status <- availability[
  is.na(bien_range_available) |
    (bien_range_available == FALSE & needs_range_review != TRUE) |
    (bien_range_available == TRUE & needs_range_review == TRUE)
]
checks <- add_check(
  checks,
  "bien_availability_review_flags_consistent",
  status_from(nrow(bad_availability_status) == 0),
  "error",
  nrow(bad_availability_status),
  0
)

fwrite(
  availability_extra,
  file.path(qa_dir, "bien_availability_extra_species.csv")
)
fwrite(
  target_missing_availability[, .(
    species_key, source_code_system, source_species_code,
    scientific_name, common_name, community_layers
  )],
  file.path(qa_dir, "bien_availability_missing_targets.csv")
)

high_abundance_missing_bien <- availability[
  bien_range_available == FALSE,
  .(
    species_key, source_code_system, source_species_code,
    scientific_name, common_name, community_layers,
    n_states, n_plot_visits, n_conditions, abundance_total,
    bien_query_name, range_lookup_status, range_review_reason
  )
][order(-abundance_total)]
fwrite(
  high_abundance_missing_bien,
  file.path(qa_dir, "bien_missing_species_ranked_by_abundance.csv")
)

# ------------------------------------------------------------------------------
# Polygon checks
# ------------------------------------------------------------------------------

available_keys <- availability[bien_range_available == TRUE, species_key]
polygon_keys <- unique(as.character(polygons$species_key))

checks <- add_check(
  checks,
  "bien_polygons_one_feature_per_species",
  status_from(length(polygon_keys) == nrow(polygons)),
  "error",
  glue("unique_species={length(polygon_keys)}; features={nrow(polygons)}"),
  "unique_species equals features",
  "If this fails, downstream extraction must intentionally aggregate multipart or duplicate features."
)

available_missing_polygons <- availability[
  bien_range_available == TRUE & !species_key %in% polygon_keys
]
polygon_without_available <- data.table(species_key = polygon_keys[!polygon_keys %in% available_keys])

checks <- add_check(
  checks,
  "bien_polygons_match_available_species",
  status_from(nrow(available_missing_polygons) == 0 && nrow(polygon_without_available) == 0),
  "error",
  glue("available_missing_polygons={nrow(available_missing_polygons)}; polygon_without_available={nrow(polygon_without_available)}"),
  "available_missing_polygons=0; polygon_without_available=0"
)

empty_polygons <- polygons[st_is_empty(polygons), ]
invalid_polygons <- polygons[!st_is_valid(polygons), ]
checks <- add_check(
  checks,
  "bien_polygons_no_empty_geometries",
  status_from(nrow(empty_polygons) == 0),
  "error",
  nrow(empty_polygons),
  0
)
checks <- add_check(
  checks,
  "bien_polygons_no_invalid_geometries",
  status_from(nrow(invalid_polygons) == 0),
  "error",
  nrow(invalid_polygons),
  0
)

# Script 03 stores geodesic range area because BIEN polygons may cross
# hemispheres or the antimeridian. Reuse it here instead of reprojecting every
# polygon during each validation run.
if ("range_area_geodesic_km2_qa" %in% names(polygons)) {
  polygon_area <- as.numeric(polygons$range_area_geodesic_km2_qa)
} else {
  sf_use_s2(TRUE)
  polygon_area <- as.numeric(
    st_area(st_make_valid(st_transform(polygons, 4326)))
  ) / 1e6
}
negative_area <- data.table(
  species_key = as.character(polygons$species_key),
  bien_query_name = as.character(polygons$bien_query_name),
  area_km2 = polygon_area
)[area_km2 <= 0 | is.na(area_km2)]

polygon_area_summary <- data.table(
  metric = c("n_polygons", "min_area_km2", "p01_area_km2", "median_area_km2", "p99_area_km2", "max_area_km2", "n_nonpositive_area"),
  value = c(
    length(polygon_area),
    min(polygon_area, na.rm = TRUE),
    as.numeric(quantile(polygon_area, 0.01, na.rm = TRUE)),
    median(polygon_area, na.rm = TRUE),
    as.numeric(quantile(polygon_area, 0.99, na.rm = TRUE)),
    max(polygon_area, na.rm = TRUE),
    nrow(negative_area)
  )
)
checks <- add_check(
  checks,
  "bien_polygons_positive_area",
  status_from(nrow(negative_area) == 0),
  "warning",
  nrow(negative_area),
  0,
  "Negative/zero area can indicate antimeridian or geometry-area QA problems"
)

fwrite(available_missing_polygons, file.path(qa_dir, "bien_polygons_missing_available_species.csv"))
fwrite(polygon_without_available, file.path(qa_dir, "bien_polygons_species_not_available.csv"))
fwrite(negative_area, file.path(qa_dir, "bien_polygons_nonpositive_area.csv"))
fwrite(polygon_area_summary, file.path(qa_dir, "bien_polygons_area_summary.csv"))

# ------------------------------------------------------------------------------
# Optional downstream freshness and consistency checks
# ------------------------------------------------------------------------------

range_climate_global <- read_optional_parquet(paths$range_climate_global)
range_climate <- read_optional_parquet(paths$range_climate_us)
global_niches <- read_optional_parquet(paths$niches_global)
niches <- read_optional_parquet(paths$niches_us)
cwm <- read_optional_parquet(paths$cwm)

if (!is.null(range_climate_global)) {
  global_range_species <- unique(range_climate_global$species_key)
  stale_global_range_species <- data.table(species_key = global_range_species[!global_range_species %in% polygon_keys])
  missing_global_range_species <- data.table(species_key = polygon_keys[!polygon_keys %in% global_range_species])
  checks <- add_check(
    checks,
    "global_range_climate_species_match_polygons",
    status_from(nrow(stale_global_range_species) == 0 && nrow(missing_global_range_species) == 0),
    "warning",
    glue("stale={nrow(stale_global_range_species)}; missing={nrow(missing_global_range_species)}"),
    "stale=0; missing=0",
    "Global range-climate products should match the current BIEN polygon species set. Stale species usually mean script 04 should be rerun for --range-scope=global."
  )
  fwrite(stale_global_range_species, file.path(qa_dir, "global_range_climate_stale_species.csv"))
  fwrite(missing_global_range_species, file.path(qa_dir, "global_range_climate_missing_polygon_species.csv"))
} else {
  checks <- add_check(checks, "global_range_climate_species_match_polygons", "warn", "warning", "file missing", "optional")
}

if (!is.null(range_climate)) {
  range_species <- unique(range_climate$species_key)
  stale_range_species <- data.table(species_key = range_species[!range_species %in% polygon_keys])
  missing_range_species <- data.table(species_key = polygon_keys[!polygon_keys %in% range_species])
  checks <- add_check(
    checks,
    "range_climate_species_match_polygons",
    status_from(nrow(stale_range_species) == 0 && nrow(missing_range_species) == 0),
    "warning",
    glue("stale={nrow(stale_range_species)}; missing={nrow(missing_range_species)}"),
    "stale=0; missing=0",
    "Stale species mean script 04 should be rerun. Missing species after a current rerun are study-area climate coverage gaps to document."
  )
  fwrite(stale_range_species, file.path(qa_dir, "range_climate_stale_species.csv"))
  fwrite(missing_range_species, file.path(qa_dir, "range_climate_missing_polygon_species.csv"))
} else {
  checks <- add_check(checks, "range_climate_species_match_polygons", "warn", "warning", "file missing", "optional")
}

if (!is.null(global_niches)) {
  global_niche_species <- unique(global_niches$species_key)
  stale_global_niche_species <- data.table(species_key = global_niche_species[!global_niche_species %in% polygon_keys])
  missing_global_niche_species <- data.table(species_key = polygon_keys[!polygon_keys %in% global_niche_species])
  checks <- add_check(
    checks,
    "global_compact_niche_species_match_polygons",
    status_from(nrow(stale_global_niche_species) == 0 && nrow(missing_global_niche_species) == 0),
    "warning",
    glue("stale={nrow(stale_global_niche_species)}; missing={nrow(missing_global_niche_species)}"),
    "stale=0; missing=0",
    "Global compact niche products should match the current BIEN polygon species set. Stale species usually mean script 05 should be rerun for --range-scope=global after script 04."
  )
  fwrite(stale_global_niche_species, file.path(qa_dir, "global_compact_niche_stale_species.csv"))
  fwrite(missing_global_niche_species, file.path(qa_dir, "global_compact_niche_missing_polygon_species.csv"))
} else {
  checks <- add_check(checks, "global_compact_niche_species_match_polygons", "warn", "warning", "file missing", "optional")
}

if (!is.null(niches)) {
  niche_species <- unique(niches$species_key)
  stale_niche_species <- data.table(species_key = niche_species[!niche_species %in% polygon_keys])
  missing_niche_species <- data.table(species_key = polygon_keys[!polygon_keys %in% niche_species])
  checks <- add_check(
    checks,
    "compact_niche_species_match_polygons",
    status_from(nrow(stale_niche_species) == 0 && nrow(missing_niche_species) == 0),
    "warning",
    glue("stale={nrow(stale_niche_species)}; missing={nrow(missing_niche_species)}"),
    "stale=0; missing=0",
    "Stale species mean script 05 should be rerun. Missing species after a current rerun are unresolved compact-niche gaps to document."
  )
  fwrite(stale_niche_species, file.path(qa_dir, "compact_niche_stale_species.csv"))
  fwrite(missing_niche_species, file.path(qa_dir, "compact_niche_missing_polygon_species.csv"))

  indicator_cols <- c(
    "tmean_annual_mean", "tmean_warmest_month_mean",
    "tmean_coldest_month_mean", "temp_seasonality_mean",
    "cwd_annual_sum", "cwd_max_month_mean", "pr_annual_sum",
    "pr_driest_month_mean"
  )
  missing_indicator_cols <- setdiff(indicator_cols, names(niches))
  checks <- add_check(
    checks,
    "compact_niche_required_indicators",
    status_from(length(missing_indicator_cols) == 0),
    "error",
    paste(missing_indicator_cols, collapse = ";"),
    "none missing"
  )
} else {
  checks <- add_check(checks, "compact_niche_species_match_polygons", "warn", "warning", "file missing", "optional")
}

if (!is.null(cwm) && !is.null(niches)) {
  cwm_with_niche <- cwm[cwm_weight_with_niche > 0]
  cwm_zero_coverage <- cwm[cwm_weight_total > 0 & cwm_weight_with_niche == 0]

  cwm_scopes <- unique(cwm_with_niche$range_scope)
  niche_scopes <- unique(niches$range_scope)
  uses_global_fallback <- "us_study_area_with_global_fallback" %in% cwm_scopes
  fallback_scope_ok <- uses_global_fallback &&
    !is.null(global_niches) &&
    "niche_scopes_used" %in% names(cwm) &&
    all(
      unlist(strsplit(
        unique(na.omit(cwm_with_niche$niche_scopes_used)),
        ";",
        fixed = TRUE
      )) %in% c("us_study_area", "global_fallback", "")
    )

  checks <- add_check(
    checks,
    "cwm_built_from_current_niche_species",
    status_from(
      nrow(cwm_with_niche) > 0 &&
        (all(cwm_scopes %in% niche_scopes) || fallback_scope_ok)
    ),
    "warning",
    paste(cwm_scopes, collapse = ";"),
    paste(c(niche_scopes, if (!is.null(global_niches)) "us_study_area_with_global_fallback" else NULL), collapse = ";"),
    "Checks only rows with nonzero niche coverage. Fallback mode is valid when CWM tracks niche_scopes_used and the global niche table exists."
  )
  checks <- add_check(
    checks,
    "cwm_zero_niche_coverage_review",
    status_from(nrow(cwm_zero_coverage) == 0),
    "warning",
    nrow(cwm_zero_coverage),
    0,
    "Plot-condition rows with seedlings but no usable species niche. These rows should be filtered or documented before modeling."
  )
}

# ------------------------------------------------------------------------------
# Summary outputs
# ------------------------------------------------------------------------------

summary <- data.table(
  metric = c(
    "n_species_universe",
    "n_species_targets",
    "n_pseudo_or_aggregate",
    "n_bien_available",
    "n_bien_missing",
    "n_polygon_species",
    "n_polygon_features"
  ),
  value = c(
    nrow(universe),
    length(target_keys),
    sum(universe$is_pseudo_taxon == TRUE, na.rm = TRUE),
    length(available_keys),
    sum(availability$bien_range_available == FALSE, na.rm = TRUE),
    length(polygon_keys),
    nrow(polygons)
  )
)

failed_errors <- checks[severity == "error" & status == "fail"]
warning_checks <- checks[severity == "warning" & status != "pass"]

decision <- data.table(
  decision_item = c(
    "proceed_to_script_04",
    "proceed_to_modeling",
    "failed_required_checks",
    "failed_warning_checks"
  ),
  value = c(
    as.character(nrow(failed_errors) == 0),
    as.character(nrow(failed_errors) == 0 && nrow(warning_checks) == 0),
    as.character(nrow(failed_errors)),
    as.character(nrow(warning_checks))
  ),
  meaning = c(
    "TRUE means scripts 01-03 are structurally consistent enough to begin or resume TerraClimate extraction.",
    "TRUE means no unresolved validator warnings remain. Current downstream modeling should wait until this is TRUE.",
    "Count of failed error-severity checks. Any value above zero blocks the workflow.",
    "Count of failed warning-severity checks. These do not always block extraction, but must be reviewed before final modeling."
  )
)

manifest <- rbindlist(lapply(paths, file_meta), fill = TRUE, idcol = "product")

fwrite(checks, file.path(qa_dir, "species_niche_validation_checks.csv"))
fwrite(summary, file.path(qa_dir, "species_niche_validation_summary.csv"))
fwrite(manifest, file.path(qa_dir, "species_niche_product_manifest.csv"))
fwrite(source_coverage, file.path(qa_dir, "species_niche_coverage_by_source.csv"))
fwrite(layer_coverage, file.path(qa_dir, "species_niche_coverage_by_layer.csv"))
fwrite(decision, file.path(qa_dir, "species_niche_validation_decision.csv"))

cat("Validation summary:\n")
print(summary)
cat("\nValidation decision:\n")
print(decision)
cat("\nCheck results:\n")
print(checks[, .N, by = .(severity, status)])

if (nrow(failed_errors) > 0) {
  stop(glue("Species niche validation failed {nrow(failed_errors)} required check(s). See species_niche_validation_checks.csv."))
}

if (nrow(warning_checks) > 0) {
  cat(glue("\nValidation completed with {nrow(warning_checks)} warning check(s). Inspect QA CSVs before modeling.\n"))
} else {
  cat("\nValidation passed with no warning checks.\n")
}
