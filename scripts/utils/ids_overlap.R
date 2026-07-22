# ------------------------------------------------------------------------------
# Exact overlap summaries for IDS aerial-detection polygons and FIA footprints.
# ------------------------------------------------------------------------------

M2_PER_US_ACRE <- 4046.8564224

# Summarize already-clipped IDS/footprint intersection geometries. Overlapping
# source polygons for the same plot/year/agent are unioned before area is
# calculated, preventing double-counting within the footprint.
summarize_ids_intersections <- function(intersections, footprint_areas) {
  if (!inherits(intersections, "sf")) {
    stop("intersections must be an sf object")
  }
  required <- c(
    "stable_plot_id", "survey_year", "dca_code", "source_polygon_acres"
  )
  missing <- setdiff(required, names(intersections))
  if (length(missing) > 0) {
    stop("IDS intersections missing: ", paste(missing, collapse = ", "))
  }
  if (nrow(intersections) == 0) {
    return(data.table::data.table())
  }

  geom <- sf::st_geometry(intersections)
  attrs <- data.table::as.data.table(sf::st_drop_geometry(intersections))
  attrs[, geometry_row := seq_len(.N)]

  events <- attrs[, {
    unioned <- sf::st_union(geom[geometry_row])
    list(
      n_source_polygons = .N,
      source_polygon_acres_sum = if (all(is.na(source_polygon_acres))) {
        NA_real_
      } else {
        sum(source_polygon_acres, na.rm = TRUE)
      },
      overlap_area_m2 = as.numeric(sf::st_area(unioned))
    )
  }, by = .(stable_plot_id, survey_year, dca_code)]

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
