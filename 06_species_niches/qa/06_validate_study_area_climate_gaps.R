# ==============================================================================
# 06_validate_study_area_climate_gaps.R
# Diagnose species with BIEN polygons but no study-area TerraClimate rows.
#
# These are not BIEN-missing species. They have downloaded BIEN range polygons,
# but did not appear in species_range_climate_us_study_area.parquet. This script
# separates expected outside-study-area cases from extraction/geometry cases and,
# for FIA tree seedling species, estimates how far FIA observations are from the
# BIEN polygon.
#
# Usage:
#   Rscript 06_species_niches/qa/06_validate_study_area_climate_gaps.R
# ==============================================================================

suppressPackageStartupMessages({
  library(here)
  library(glue)
  library(data.table)
  library(arrow)
  library(sf)
  library(fs)
})

source(here("scripts/utils/load_config.R"))

config <- load_config()
niche_dir <- here(config$processed$species_niches$output_dir)
fia_summary_dir <- here(config$processed$fia$summaries$output_dir)
qa_dir <- here("06_species_niches/qa/outputs")
dir_create(qa_dir)

paths <- list(
  missing_species = file.path(qa_dir, "range_climate_missing_polygon_species.csv"),
  species_universe = file.path(niche_dir, "species_universe.parquet"),
  polygons = file.path(niche_dir, "species_range_polygons.gpkg"),
  global_range_climate = file.path(niche_dir, "species_range_climate.parquet"),
  global_niches = file.path(niche_dir, "species_climate_niches.parquet"),
  study_area_range_climate = file.path(niche_dir, "species_range_climate_us_study_area.parquet"),
  seedlings = file.path(fia_summary_dir, "plot_seedling_species.parquet")
)

required <- c("missing_species", "species_universe", "polygons", "study_area_range_climate")
missing_required <- required[!file.exists(unlist(paths[required]))]
if (length(missing_required) > 0) {
  stop(glue("Missing required input(s): {paste(missing_required, collapse = ', ')}"))
}

make_study_area_polygon <- function(config) {
  study_area <- config$params$study_area
  bbox <- st_bbox(
    c(
      xmin = study_area$xmin,
      ymin = study_area$ymin,
      xmax = study_area$xmax,
      ymax = study_area$ymax
    ),
    crs = st_crs(4326)
  )
  st_as_sfc(bbox)
}

distance_summary_for_species <- function(seedlings, polygon_row, species_key) {
  if (!grepl("^fia_spcd:", species_key)) {
    return(data.table(
      species_key = species_key,
      n_fia_observation_locations = NA_integer_,
      min_distance_to_bien_range_km = NA_real_,
      median_distance_to_bien_range_km = NA_real_,
      distance_note = "Distance QA currently implemented for FIA SPCD seedling observations only."
    ))
  }

  spcd <- as.integer(sub("^fia_spcd:", "", species_key))
  obs <- seedlings[SPCD == spcd & !is.na(LON) & !is.na(LAT), .(LON, LAT)]
  obs <- unique(obs)

  if (nrow(obs) == 0) {
    return(data.table(
      species_key = species_key,
      n_fia_observation_locations = 0L,
      min_distance_to_bien_range_km = NA_real_,
      median_distance_to_bien_range_km = NA_real_,
      distance_note = "No FIA seedling observation coordinates available."
    ))
  }

  obs_sf <- st_as_sf(obs, coords = c("LON", "LAT"), crs = 4326, remove = FALSE)
  # Use s2/geodesic distance in longitude-latitude coordinates to avoid using
  # a continental projection for Alaska, Hawaii, and tropical territories.
  distances_km <- as.numeric(st_distance(obs_sf, polygon_row)) / 1000
  min_by_obs <- apply(matrix(distances_km, nrow = nrow(obs_sf)), 1, min, na.rm = TRUE)

  data.table(
    species_key = species_key,
    n_fia_observation_locations = nrow(obs_sf),
    min_distance_to_bien_range_km = min(min_by_obs, na.rm = TRUE),
    median_distance_to_bien_range_km = median(min_by_obs, na.rm = TRUE),
    distance_note = "Distance from FIA seedling locations to the full BIEN polygon."
  )
}

cat("Study-area climate gap validation\n")
cat("=================================\n\n")

missing_species <- fread(paths$missing_species)
if (nrow(missing_species) == 0) {
  cat("No missing study-area range climate species to validate.\n")
  quit(status = 0)
}

universe <- as.data.table(read_parquet(paths$species_universe))
study_area_range_climate <- as.data.table(read_parquet(paths$study_area_range_climate))
global_range_species <- if (file.exists(paths$global_range_climate)) {
  unique(as.data.table(read_parquet(paths$global_range_climate))$species_key)
} else {
  character()
}
global_niche_species <- if (file.exists(paths$global_niches)) {
  unique(as.data.table(read_parquet(paths$global_niches))$species_key)
} else {
  character()
}

polygons <- st_read(paths$polygons, quiet = TRUE)
polygons$species_key <- as.character(polygons$species_key)
polygons <- polygons[polygons$species_key %in% missing_species$species_key, ]
polygons <- st_make_valid(st_transform(polygons, 4326))

study_area_polygon <- make_study_area_polygon(config)
intersects_study_area <- lengths(st_intersects(polygons, study_area_polygon)) > 0

study_area_intersection <- suppressWarnings(st_intersection(polygons, study_area_polygon))
intersection_area_km2 <- rep(0, nrow(polygons))
if (nrow(study_area_intersection) > 0) {
  area_by_species <- data.table(
    species_key = as.character(study_area_intersection$species_key),
    area_km2 = as.numeric(st_area(st_transform(study_area_intersection, config$params$area_crs))) / 1e6
  )[, .(study_area_intersection_area_km2 = sum(area_km2, na.rm = TRUE)), by = species_key]
  intersection_area_km2 <- area_by_species$study_area_intersection_area_km2[
    match(polygons$species_key, area_by_species$species_key)
  ]
  intersection_area_km2[is.na(intersection_area_km2)] <- 0
}

gap_table <- data.table(
  species_key = polygons$species_key,
  bien_query_name = as.character(polygons$bien_query_name),
  polygon_intersects_study_area_bbox = intersects_study_area,
  study_area_intersection_area_km2 = intersection_area_km2,
  has_global_range_climate = polygons$species_key %in% global_range_species,
  has_global_niche = polygons$species_key %in% global_niche_species,
  has_study_area_range_climate = polygons$species_key %in% study_area_range_climate$species_key
)

gap_table <- merge(
  gap_table,
  universe[
    ,
    .(
      species_key, source_code_system, source_species_code,
      scientific_name, common_name, community_layers, abundance_total
    )
  ],
  by = "species_key",
  all.x = TRUE
)

gap_table[, likely_gap_type := fifelse(
  polygon_intersects_study_area_bbox == FALSE,
  "bien_polygon_outside_study_area_bbox",
  fifelse(
    study_area_intersection_area_km2 <= 0 | is.na(study_area_intersection_area_km2),
    "study_area_intersection_area_zero_or_invalid",
    "intersects_study_area_but_no_climate_rows"
  )
)]

seedlings <- if (file.exists(paths$seedlings)) {
  as.data.table(read_parquet(paths$seedlings))
} else {
  data.table()
}

distance_rows <- rbindlist(lapply(seq_len(nrow(polygons)), function(i) {
  distance_summary_for_species(seedlings, polygons[i, ], polygons$species_key[[i]])
}), fill = TRUE)

gap_table <- merge(gap_table, distance_rows, by = "species_key", all.x = TRUE)
setorder(gap_table, likely_gap_type, -abundance_total, species_key)

gap_summary <- gap_table[
  ,
  .(
    n_species = .N,
    total_abundance_for_universe = sum(abundance_total, na.rm = TRUE),
    n_with_global_niche = sum(has_global_niche == TRUE, na.rm = TRUE),
    n_with_global_range_climate = sum(has_global_range_climate == TRUE, na.rm = TRUE),
    n_fia_species_with_distance = sum(!is.na(n_fia_observation_locations))
  ),
  by = likely_gap_type
][order(likely_gap_type)]

fwrite(gap_table, file.path(qa_dir, "study_area_climate_gap_diagnostics.csv"))
fwrite(gap_summary, file.path(qa_dir, "study_area_climate_gap_summary.csv"))

cat("Done.\n")
cat(glue("Diagnostics: {file.path(qa_dir, 'study_area_climate_gap_diagnostics.csv')}"), "\n")
cat(glue("Summary:     {file.path(qa_dir, 'study_area_climate_gap_summary.csv')}"), "\n\n")
print(gap_summary)
