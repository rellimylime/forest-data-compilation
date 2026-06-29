# ==============================================================================
# 08_describe_global_fallback_species.R
# Summarize species with BIEN global niches but no study-area clipped niche.
#
# This script answers two review questions:
#   1. Where is the BIEN global range polygon?
#   2. Where do the project observations occur?
#
# It is a diagnostic script only. It does not alter niche products or modeling
# inputs.
#
# Usage:
#   Rscript 06_species_niches/qa/08_describe_global_fallback_species.R
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
  diagnostics = file.path(qa_dir, "study_area_climate_gap_diagnostics.csv"),
  species_universe = file.path(niche_dir, "species_universe.parquet"),
  polygons = file.path(niche_dir, "species_range_polygons.gpkg"),
  seedlings = file.path(fia_summary_dir, "plot_seedling_species.parquet"),
  trees = file.path(fia_summary_dir, "plot_tree_species.parquet")
)

required <- c("diagnostics", "species_universe", "polygons")
missing_inputs <- required[!file.exists(unlist(paths[required]))]
if (length(missing_inputs) > 0) {
  stop(glue("Missing required input(s): {paste(missing_inputs, collapse = ', ')}"))
}

make_bbox_table <- function(sf_obj, prefix) {
  rows <- lapply(seq_len(nrow(sf_obj)), function(i) {
    bbox <- st_bbox(sf_obj[i, ])
    out <- data.table(
      species_key = as.character(sf_obj$species_key[[i]]),
      xmin = as.numeric(bbox[["xmin"]]),
      ymin = as.numeric(bbox[["ymin"]]),
      xmax = as.numeric(bbox[["xmax"]]),
      ymax = as.numeric(bbox[["ymax"]])
    )
    setnames(
      out,
      old = c("xmin", "ymin", "xmax", "ymax"),
      new = paste0(prefix, c("_xmin", "_ymin", "_xmax", "_ymax"))
    )
    out
  })
  rbindlist(rows, fill = TRUE)
}

summarize_observation_locations <- function(species_keys, seedlings_path, trees_path) {
  obs_parts <- list()

  if (file.exists(seedlings_path)) {
    seedlings <- as.data.table(read_parquet(seedlings_path))
    seedlings <- seedlings[
      !is.na(SPCD) & !is.na(LON) & !is.na(LAT),
      .(
        species_key = paste0("fia_spcd:", SPCD),
        layer = "seedling",
        state,
        stable_plot_id,
        PLT_CN,
        INVYR,
        LAT,
        LON,
        abundance = seedlings_tpa
      )
    ][species_key %in% species_keys]
    obs_parts$seedlings <- seedlings
  }

  if (file.exists(trees_path)) {
    trees <- as.data.table(read_parquet(trees_path))
    trees <- trees[
      species_key %in% species_keys & !is.na(LON) & !is.na(LAT),
      .(
        species_key,
        layer = community_layer,
        state,
        stable_plot_id,
        PLT_CN,
        INVYR,
        LAT,
        LON,
        abundance = abundance_for_cwm
      )
    ]
    obs_parts$trees <- trees
  }

  obs <- rbindlist(obs_parts, fill = TRUE)
  if (nrow(obs) == 0) {
    return(data.table(
      species_key = species_keys,
      observed_coordinate_layers = NA_character_,
      observed_coordinate_states = NA_character_,
      n_observed_coordinate_rows = 0L,
      n_observed_coordinate_plot_visits = 0L,
      n_observed_coordinate_locations = 0L,
      observed_xmin = NA_real_,
      observed_ymin = NA_real_,
      observed_xmax = NA_real_,
      observed_ymax = NA_real_,
      observed_centroid_lon = NA_real_,
      observed_centroid_lat = NA_real_,
      observed_abundance_with_coordinates = NA_real_
    ))
  }

  obs[
    ,
    .(
      observed_coordinate_layers = paste(sort(unique(layer)), collapse = ";"),
      observed_coordinate_states = paste(sort(unique(state)), collapse = ";"),
      n_observed_coordinate_rows = .N,
      n_observed_coordinate_plot_visits = uniqueN(PLT_CN),
      n_observed_coordinate_locations = uniqueN(paste(round(LON, 6), round(LAT, 6))),
      observed_xmin = min(LON, na.rm = TRUE),
      observed_ymin = min(LAT, na.rm = TRUE),
      observed_xmax = max(LON, na.rm = TRUE),
      observed_ymax = max(LAT, na.rm = TRUE),
      observed_centroid_lon = mean(LON, na.rm = TRUE),
      observed_centroid_lat = mean(LAT, na.rm = TRUE),
      observed_abundance_with_coordinates = sum(abundance, na.rm = TRUE)
    ),
    by = species_key
  ]
}

cat("Global Fallback Species Location Diagnostic\n")
cat("==========================================\n\n")

diagnostics <- fread(paths$diagnostics)
universe <- as.data.table(read_parquet(paths$species_universe))

target_keys <- diagnostics$species_key
if (length(target_keys) == 0) {
  cat("No global-fallback/study-area climate gap species found.\n")
  quit(status = 0)
}

polygons <- st_read(paths$polygons, quiet = TRUE)
polygons$species_key <- as.character(polygons$species_key)
polygons <- polygons[polygons$species_key %in% target_keys, ]
polygons <- st_make_valid(st_transform(polygons, 4326))

range_bbox <- make_bbox_table(polygons, "bien_range")

# Use a point guaranteed to lie on the polygon, not a mathematical centroid that
# can fall outside complex multipart geometries.
range_points <- suppressWarnings(st_point_on_surface(polygons))
coords <- st_coordinates(range_points)
range_points_dt <- data.table(
  species_key = polygons$species_key,
  bien_range_point_lon = coords[, "X"],
  bien_range_point_lat = coords[, "Y"]
)

range_area <- data.table(
  species_key = polygons$species_key,
  bien_range_area_global_equal_km2 = as.numeric(st_area(st_transform(polygons, config$params$global_area_crs))) / 1e6
)

obs_summary <- summarize_observation_locations(
  target_keys,
  paths$seedlings,
  paths$trees
)

out <- Reduce(
  function(x, y) merge(x, y, by = "species_key", all.x = TRUE),
  list(
    diagnostics,
    universe[
      ,
      .(
        species_key,
        source_code_system,
        source_species_code,
        scientific_name,
        common_name,
        source_tables,
        community_layers,
        growth_habits,
        states_present,
        n_states,
        n_plot_visits,
        n_conditions,
        n_source_rows,
        abundance_total,
        in_seedlings,
        in_saplings,
        in_trees,
        in_shrubs,
        in_forbs,
        in_graminoids,
        in_p2veg_tree_layers
      )
    ],
    range_bbox,
    range_points_dt,
    range_area,
    obs_summary
  )
)

# `study_area_climate_gap_diagnostics.csv` already carries some universe fields.
# If new fields from species_universe collide during the merge, keep the
# original public column names and use the suffixed copy only as a fallback.
for (col in c(
  "source_code_system", "source_species_code", "scientific_name", "common_name",
  "community_layers", "abundance_total"
)) {
  suffixed <- paste0(col, ".y")
  original <- paste0(col, ".x")
  if (original %in% names(out) && suffixed %in% names(out)) {
    out[, (col) := fifelse(!is.na(get(original)), get(original), get(suffixed))]
    out[, c(original, suffixed) := NULL]
  } else if (original %in% names(out)) {
    setnames(out, original, col)
  } else if (suffixed %in% names(out)) {
    setnames(out, suffixed, col)
  }
}

out[, observation_note := fifelse(
  is.na(observed_coordinate_layers),
  "Observation counts come from species_universe; coordinate summary unavailable for this source layer.",
  "Coordinate summary from FIA seedling/tree/sapling products."
)]

setorder(out, source_code_system, states_present, scientific_name, species_key)

summary <- out[
  ,
  .(
    n_species = .N,
    total_source_rows = sum(n_source_rows, na.rm = TRUE),
    total_plot_visits = sum(n_plot_visits, na.rm = TRUE),
    total_conditions = sum(n_conditions, na.rm = TRUE),
    total_abundance = sum(abundance_total, na.rm = TRUE),
    n_with_coordinate_summary = sum(!is.na(observed_coordinate_layers)),
    n_with_global_niche = sum(has_global_niche == TRUE, na.rm = TRUE),
    n_intersecting_study_area_bbox = sum(polygon_intersects_study_area_bbox == TRUE, na.rm = TRUE)
  ),
  by = .(source_code_system, states_present)
][order(source_code_system, states_present)]

out_file <- file.path(qa_dir, "global_fallback_species_location_diagnostics.csv")
summary_file <- file.path(qa_dir, "global_fallback_species_location_summary.csv")

fwrite(out, out_file)
fwrite(summary, summary_file)

cat("Done.\n")
cat(glue("Diagnostics: {out_file}"), "\n")
cat(glue("Summary:     {summary_file}"), "\n\n")
print(summary)
