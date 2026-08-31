#!/usr/bin/env Rscript

# Summarize whether the prepared evidence is sufficient to begin analysis
# design. This script reports availability and overlap; it does not choose an
# analysis cohort, temporal endpoints, severity measure, or model.

suppressPackageStartupMessages({
  library(here)
  library(glue)
  library(data.table)
  library(arrow)
  library(dplyr)
  library(fs)
  library(sf)
})

source(here("scripts/utils/load_config.R"))

config <- load_config()
link_cfg <- config$processed$disturbance_linkage
output_dir <- here(link_cfg$output_dir)
qa_dir <- here(link_cfg$qa_dir)
status_path <- file.path(
  output_dir,
  link_cfg$files$fia_visit_spatial_linkage_status
)
agent_dir <- file.path(
  output_dir,
  link_cfg$files$ids_annual_agent_evidence
)
coverage_dir <- file.path(
  output_dir,
  link_cfg$files$ids_annual_survey_coverage
)
mtbs_source <- here(link_cfg$inputs$mtbs_fire_perimeters)
mtbs_output <- file.path(
  output_dir,
  link_cfg$files$mtbs_fire_event_evidence
)
readiness_path <- file.path(
  output_dir,
  link_cfg$files$disturbance_analysis_readiness
)
manifest_path <- file.path(
  output_dir,
  link_cfg$files$disturbance_source_manifest
)
report_path <- here(
  "08_disturbance_linkage",
  "DISTURBANCE_ANALYSIS_READINESS.md"
)

if (!file.exists(status_path)) {
  stop("Coordinate status is missing: ", status_path)
}
dir_create(output_dir)
dir_create(qa_dir)

# Reduce visit-level status to one eligibility decision per stable plot.
status <- as.data.table(read_parquet(status_path))
plot_status <- unique(status[, .(
  stable_plot_id,
  n_distinct_public_coordinates,
  eligible_spatial_linkage,
  spatial_linkage_exclusion_reason
)])
visit_counts <- status[, .(n_visits = uniqueN(PLT_CN)), by = stable_plot_id]

# Read completed years from filenames without opening every IDS partition.
partition_years <- function(path) {
  if (!dir.exists(path)) return(integer())
  files <- dir_ls(path, regexp = "survey_year=[0-9]{4}\\.parquet$")
  as.integer(sub(
    "^survey_year=([0-9]{4})\\.parquet$",
    "\\1",
    basename(files)
  ))
}

ids_path <- here(link_cfg$inputs$ids_layers)
# The IDS source layers define which annual partitions should exist.
expected_agent_years <- as.integer(st_read(
  ids_path,
  query = paste(
    "SELECT DISTINCT SURVEY_YEAR FROM damage_areas",
    "WHERE SURVEY_YEAR IS NOT NULL ORDER BY SURVEY_YEAR"
  ),
  quiet = TRUE
)$SURVEY_YEAR)
expected_coverage_years <- as.integer(st_read(
  ids_path,
  query = paste(
    "SELECT DISTINCT SURVEY_YEAR FROM surveyed_areas",
    "WHERE SURVEY_YEAR IS NOT NULL ORDER BY SURVEY_YEAR"
  ),
  quiet = TRUE
)$SURVEY_YEAR)

# Compare built partitions with source years before calling IDS complete.
built_agent_years <- sort(partition_years(agent_dir))
built_coverage_years <- sort(partition_years(coverage_dir))
agent_complete <- setequal(expected_agent_years, built_agent_years)
coverage_complete <- setequal(expected_coverage_years, built_coverage_years)

# Collect only plot identifiers because readiness needs coverage counts, not events.
ids_detection_ids <- character()
if (length(built_agent_years) > 0L) {
  ids_detection_ids <- open_dataset(agent_dir) |>
    select(stable_plot_id) |>
    distinct() |>
    collect() |>
    pull(stable_plot_id) |>
    as.character()
}
ids_coverage_ids <- character()
if (length(built_coverage_years) > 0L) {
  ids_coverage_ids <- open_dataset(coverage_dir) |>
    filter(surveyed_overlap_fraction > 0) |>
    select(stable_plot_id) |>
    distinct() |>
    collect() |>
    pull(stable_plot_id) |>
    as.character()
}
mtbs_ids <- character()
if (file.exists(mtbs_output)) {
  mtbs_ids <- open_dataset(mtbs_output) |>
    select(stable_plot_id) |>
    distinct() |>
    collect() |>
    pull(stable_plot_id) |>
    as.character()
}

# Store metrics in a long table so new readiness checks need no schema change.
metrics <- list()
add_metric <- function(scope, metric, value, status_label = "available", note = "") {
  metrics[[length(metrics) + 1L]] <<- data.table(
    scope = as.character(scope),
    metric = as.character(metric),
    value = as.character(value),
    status = as.character(status_label),
    note = as.character(note)
  )
}

add_metric("fia", "stable_plots", nrow(plot_status))
add_metric("fia", "plot_visits", nrow(status))
add_metric(
  "fia",
  "stable_plots_with_repeated_measurements",
  visit_counts[n_visits >= 2L, .N]
)
add_metric(
  "coordinates",
  "stable_plots_excluded_multiple_coordinates",
  plot_status[n_distinct_public_coordinates > 1L, .N]
)
add_metric(
  "coordinates",
  "stable_plots_excluded_no_usable_coordinate",
  plot_status[n_distinct_public_coordinates == 0L, .N]
)
add_metric(
  "coordinates",
  "stable_plots_eligible_spatial_linkage",
  plot_status[eligible_spatial_linkage == TRUE, .N]
)
add_metric(
  "ids",
  "agent_year_partitions_built",
  length(built_agent_years),
  if (agent_complete) "complete" else "partial",
  glue("Expected {length(expected_agent_years)} years ({min(expected_agent_years)}-{max(expected_agent_years)}).")
)
add_metric(
  "ids",
  "coverage_year_partitions_built",
  length(built_coverage_years),
  if (coverage_complete) "complete" else "partial",
  glue("Expected {length(expected_coverage_years)} years ({min(expected_coverage_years)}-{max(expected_coverage_years)}).")
)
add_metric(
  "ids",
  "stable_plots_with_at_least_one_detection",
  length(ids_detection_ids),
  if (agent_complete) "complete" else "partial",
  "Count reflects only built year partitions."
)
add_metric(
  "ids",
  "stable_plots_with_at_least_one_surveyed_area_intersection",
  length(ids_coverage_ids),
  if (coverage_complete) "complete" else "partial",
  "Includes full and partial 800 m footprint coverage."
)
add_metric(
  "mtbs",
  "configured_perimeter_source_available",
  file.exists(mtbs_source),
  if (file.exists(mtbs_source)) "available" else "blocked",
  if (file.exists(mtbs_source)) "" else "Configured source is absent; no substitute was downloaded."
)
add_metric(
  "mtbs",
  "stable_plots_with_at_least_one_event",
  if (file.exists(mtbs_output)) length(mtbs_ids) else NA_character_,
  if (file.exists(mtbs_output)) "available" else "blocked",
  if (file.exists(mtbs_output)) "" else "MTBS event evidence was not built because the configured perimeter source is absent."
)

# Reuse existing biological products rather than creating an analysis cohort here.
stage_paths <- c(
  trees = here("05_fia/data/processed/summaries/plot_tree_species.parquet"),
  saplings = here("05_fia/data/processed/summaries/plot_sapling_species.parquet"),
  seedlings = here("05_fia/data/processed/summaries/plot_seedling_species.parquet")
)
eligible_ids <- plot_status[eligible_spatial_linkage == TRUE, stable_plot_id]

for (stage in names(stage_paths)) {
  path <- stage_paths[[stage]]
  if (!file.exists(path)) {
    add_metric(stage, "product_available", FALSE, "missing", path)
    next
  }
  # Availability counts include records assigned to forested FIA conditions only.
  stage_visits <- open_dataset(path) |>
    filter(COND_STATUS_CD == 1L) |>
    select(stable_plot_id, PLT_CN, INVYR) |>
    distinct() |>
    collect() |>
    as.data.table()
  stage_plots <- unique(stage_visits$stable_plot_id)
  repeated_stage <- stage_visits[, .(
    n_visits = uniqueN(PLT_CN)
  ), by = stable_plot_id][n_visits >= 2L, stable_plot_id]

  add_metric(stage, "forested_plot_visits_with_records", nrow(stage_visits))
  add_metric(stage, "stable_plots_with_forested_records", length(stage_plots))
  add_metric(
    stage,
    "stable_plots_with_repeated_forested_records",
    length(repeated_stage)
  )
  add_metric(
    stage,
    "repeated_record_plots_eligible_spatial_linkage",
    sum(repeated_stage %in% eligible_ids)
  )
  add_metric(
    stage,
    "repeated_record_plots_with_ids_detection",
    sum(repeated_stage %in% ids_detection_ids),
    if (agent_complete) "complete" else "partial",
    "Count reflects only built IDS agent partitions."
  )
  add_metric(
    stage,
    "repeated_record_plots_with_ids_coverage",
    sum(repeated_stage %in% ids_coverage_ids),
    if (coverage_complete) "complete" else "partial",
    "Full or partial surveyed-area intersection in a built IDS year."
  )
  add_metric(
    stage,
    "repeated_record_plots_with_mtbs_event",
    if (file.exists(mtbs_output)) sum(repeated_stage %in% mtbs_ids) else NA_character_,
    if (file.exists(mtbs_output)) "available" else "blocked",
    if (file.exists(mtbs_output)) "" else "MTBS perimeter source is absent."
  )
}

# Write machine-readable counts before creating the short human-readable report.
readiness <- rbindlist(metrics)
fwrite(readiness, readiness_path)

source_row <- function(
  source_id,
  path,
  source_role,
  year_range = "",
  citation = "",
  doi = "",
  declared_sha256 = ""
) {
  exists <- file.exists(path)
  info <- if (exists) file.info(path) else NULL
  size <- if (exists) as.numeric(info$size) else NA_real_
  # Avoid rereading very large sources solely to create this lightweight report.
  calculate_checksum <- exists && size <= 500 * 1024^2
  data.table(
    source_id = source_id,
    path = path_rel(path, start = here()),
    source_role = source_role,
    available = exists,
    size_bytes = size,
    modified_utc = if (exists) {
      format(info$mtime, "%Y-%m-%dT%H:%M:%SZ", tz = "UTC")
    } else {
      NA_character_
    },
    checksum_algorithm = if (calculate_checksum) "md5" else NA_character_,
    checksum = if (calculate_checksum) {
      unname(tools::md5sum(path))
    } else {
      NA_character_
    },
    checksum_status = if (!exists) {
      "source_missing"
    } else if (calculate_checksum) {
      "calculated"
    } else {
      "not_calculated_file_over_500_MB"
    },
    year_range = year_range,
    citation = citation,
    doi = doi,
    declared_sha256 = declared_sha256
  )
}

# Record source provenance once rather than repeating it on every evidence row.
manifest <- rbindlist(list(
  source_row(
    "fia_plot_condition_metadata",
    here(link_cfg$inputs$fia_plot_source),
    "FIA visit coordinates and condition-backed visit universe"
  ),
  source_row(
    "fia_plot_visit_context",
    here(link_cfg$inputs$fia_plot_visit_context),
    "FIA visit dates and visit provenance"
  ),
  source_row(
    "fia_plot_footprints",
    here(link_cfg$output_dir, link_cfg$files$plot_footprints),
    "Existing 800 m stable-plot linkage geometry"
  ),
  source_row(
    "ids_cleaned_layers",
    here(link_cfg$inputs$ids_layers),
    "IDS damage detections and surveyed-area coverage",
    "damage_areas=1997-2024; surveyed_areas=1996-2024"
  ),
  source_row(
    "mtbs_fire_perimeters_archive",
    here(link_cfg$inputs$mtbs_fire_perimeters_archive),
    "Original downloaded national MTBS burned-area boundary archive",
    "1984-2026",
    link_cfg$mtbs$source$citation,
    link_cfg$mtbs$source$doi,
    link_cfg$mtbs$source$archive_sha256
  ),
  source_row(
    "mtbs_fire_perimeters_canonical",
    mtbs_source,
    "Validated GeoPackage converted from the cited MTBS archive",
    "1984-2026",
    link_cfg$mtbs$source$citation,
    link_cfg$mtbs$source$doi,
    link_cfg$mtbs$source$archive_sha256
  ),
  source_row(
    "fia_tree_species",
    stage_paths[["trees"]],
    "Existing tree biological records"
  ),
  source_row(
    "fia_sapling_species",
    stage_paths[["saplings"]],
    "Existing sapling biological records"
  ),
  source_row(
    "fia_seedling_species",
    stage_paths[["seedlings"]],
    "Existing seedling biological records"
  )
), fill = TRUE)
fwrite(manifest, manifest_path)

metric_value <- function(scope_name, metric_name) {
  readiness[scope == scope_name & metric == metric_name, value][[1]]
}
# Readiness means the prepared sources are complete, not that a cohort is selected.
ready_for_design <- agent_complete && coverage_complete &&
  file.exists(mtbs_source) && file.exists(mtbs_output)

report <- c(
  "# Disturbance Analysis Readiness",
  "",
  "This is a data-availability report, not an analysis cohort or model.",
  "",
  "## Current counts",
  "",
  glue("- FIA stable plots: {metric_value('fia', 'stable_plots')}"),
  glue("- FIA plot visits: {metric_value('fia', 'plot_visits')}"),
  glue("- Stable plots with repeated measurements: {metric_value('fia', 'stable_plots_with_repeated_measurements')}"),
  glue("- Spatial-linkage eligible plots: {metric_value('coordinates', 'stable_plots_eligible_spatial_linkage')}"),
  glue("- Excluded multi-coordinate plots: {metric_value('coordinates', 'stable_plots_excluded_multiple_coordinates')}"),
  glue("- Excluded plots without a usable coordinate: {metric_value('coordinates', 'stable_plots_excluded_no_usable_coordinate')}"),
  "",
  "## External evidence",
  "",
  glue("- IDS exact-agent year partitions: {length(built_agent_years)} of {length(expected_agent_years)}."),
  glue("- IDS coverage year partitions: {length(built_coverage_years)} of {length(expected_coverage_years)}."),
  if (file.exists(mtbs_source)) {
    "- The configured MTBS perimeter source is available."
  } else {
    "- MTBS is blocked: the configured perimeter source is absent, and no substitute was downloaded."
  },
  "",
  "## Interpretation",
  "",
  if (ready_for_design) {
    "The prepared spatial sources are complete enough to take readiness counts to the PI/postdoc and finalize the first analysis design."
  } else if (file.exists(mtbs_source) && file.exists(mtbs_output)) {
    "MTBS event linkage is complete. Do not treat the combined fire-and-insect preparation as complete yet: review the final IDS counts after all IDS year partitions finish."
  } else {
    "Do not treat this as full fire-and-insect readiness yet. Review the completed IDS counts when all partitions finish; MTBS linkage remains blocked until the configured perimeter source is supplied."
  },
  "",
  "Detailed counts are in `data/processed/disturbance_analysis_readiness.csv`; source provenance is in `data/processed/disturbance_source_manifest.csv`."
)
writeLines(report, report_path, useBytes = TRUE)

cat(glue(
  "Readiness metrics: {readiness_path}\n",
  "Source manifest: {manifest_path}\n",
  "Short report: {report_path}\n"
))
