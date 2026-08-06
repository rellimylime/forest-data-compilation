# Summarize exact overlap between IDS polygons and FIA search buffers.

M2_PER_US_ACRE <- 4046.8564224

summarize_ids_intersections <- function(intersections, footprint_areas) {
  if (!inherits(intersections, "sf")) {
    stop("intersections must be an sf object")
  }
  required <- c(
    "stable_plot_id", "survey_year", "dca_code", "source_feature_id",
    "source_observation_id", "source_polygon_acres"
  )
  missing <- setdiff(required, names(intersections))
  if (length(missing) > 0) {
    stop("IDS intersections missing: ", paste(missing, collapse = ", "))
  }
  if (nrow(intersections) == 0) {
    return(data.table::data.table())
  }

  # Keep geometry row numbers so grouped attributes can recover their shapes.
  geom <- sf::st_geometry(intersections)
  attrs <- data.table::as.data.table(sf::st_drop_geometry(intersections))
  attrs[, geometry_row := seq_len(.N)]

  # Union overlapping pieces before measuring area to prevent double-counting.
  events <- attrs[, {
    unioned <- sf::st_union(geom[geometry_row])
    # Count each source feature once even if clipping split it into pieces.
    feature_rows <- unique(
      data.table::data.table(
        source_feature_id = as.character(source_feature_id),
        source_polygon_acres = source_polygon_acres
      ),
      by = "source_feature_id"
    )
    feature_ids <- sort(unique(
      as.character(source_feature_id[!is.na(source_feature_id)])
    ))
    observation_ids <- sort(unique(
      as.character(source_observation_id[!is.na(source_observation_id)])
    ))
    list(
      n_source_features = length(feature_ids),
      source_feature_ids = if (length(feature_ids) == 0L) {
        NA_character_
      } else {
        paste(feature_ids, collapse = ";")
      },
      n_source_observations = length(observation_ids),
      source_observation_ids = if (length(observation_ids) == 0L) {
        NA_character_
      } else {
        paste(observation_ids, collapse = ";")
      },
      source_polygon_acres_sum = if (
        all(is.na(feature_rows$source_polygon_acres))
      ) {
        NA_real_
      } else {
        sum(feature_rows$source_polygon_acres, na.rm = TRUE)
      },
      overlap_area_m2 = as.numeric(sf::st_area(unioned))
    )
  }, by = .(stable_plot_id, survey_year, dca_code)]

  # Join the full search-buffer area used as the overlap denominator.
  areas <- data.table::as.data.table(footprint_areas)
  if (!all(c("stable_plot_id", "footprint_area_m2") %in% names(areas))) {
    stop("footprint_areas must contain stable_plot_id and footprint_area_m2")
  }
  if (anyDuplicated(areas$stable_plot_id)) {
    stop("footprint_areas must be unique by stable_plot_id")
  }
  events <- merge(events, areas, by = "stable_plot_id", all.x = TRUE)
  if (any(is.na(events$footprint_area_m2) | events$footprint_area_m2 <= 0)) {
    stop("Every IDS intersection must have a positive footprint area")
  }

  # Report overlap in acres and as a share of the 800 m search buffer.
  events[, `:=`(
    overlap_acres = overlap_area_m2 / M2_PER_US_ACRE,
    footprint_overlap_fraction = overlap_area_m2 / footprint_area_m2
  )]
  if (any(events$footprint_overlap_fraction > 1 + 1e-6, na.rm = TRUE)) {
    stop("Calculated IDS overlap exceeds its FIA footprint area")
  }
  events[, footprint_overlap_fraction := pmin(1, footprint_overlap_fraction)]
  events[]
}
