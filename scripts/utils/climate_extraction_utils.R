# ==============================================================================
# scripts/utils/climate_extraction_utils.R
# Generic multi-source climate extraction utilities (pixel-level + full time series)
# ==============================================================================

library(sf)
library(dplyr)
library(purrr)
library(tibble)

#' Climate source catalog for Google Earth Engine extraction
#'
#' `all_variables` is intended to be exhaustive enough for discovery.
#' `suggested_variables` is a smaller starter set for forest disturbance analyses.
climate_source_catalog <- function() {
  list(
    terraclimate = list(
      gee_collection = "IDAHO_EPSCOR/TERRACLIMATE",
      temporal_resolution = "monthly",
      nominal_scale_m = 4638,
      coverage = "global",
      date_format = "%Y-%m-01",
      all_variables = c(
        "aet", "def", "pdsi", "pet", "pr", "ro", "soil", "srad",
        "swe", "tmmn", "tmmx", "vap", "vpd", "vs"
      ),
      suggested_variables = c("pr", "tmmn", "tmmx", "vpd", "srad", "vs", "soil", "pdsi")
    ),
    worldclim = list(
      gee_collection = "WORLDCLIM/V1/MONTHLY",
      temporal_resolution = "monthly_climatology",
      nominal_scale_m = 927,
      coverage = "global",
      date_format = "%m",
      all_variables = c("prec", "tavg", "tmin", "tmax"),
      suggested_variables = c("prec", "tavg", "tmin", "tmax")
    ),
    prism = list(
      gee_collection = "OREGONSTATE/PRISM/AN81m",
      temporal_resolution = "monthly",
      nominal_scale_m = 800,
      coverage = "CONUS",
      date_format = "%Y-%m-01",
      all_variables = c("ppt", "tmean", "tmin", "tmax", "tdmean", "vpdmin", "vpdmax"),
      suggested_variables = c("ppt", "tmean", "tmin", "tmax", "vpdmax")
    ),
    era5_daily = list(
      gee_collection = "ECMWF/ERA5/DAILY",
      temporal_resolution = "daily",
      nominal_scale_m = 27830,
      coverage = "global",
      date_format = "%Y-%m-%d",
      all_variables = c(
        "mean_2m_air_temperature", "minimum_2m_air_temperature", "maximum_2m_air_temperature",
        "total_precipitation", "u_component_of_wind_10m", "v_component_of_wind_10m",
        "surface_solar_radiation_downwards", "snow_depth", "total_evaporation",
        "volumetric_soil_water_layer_1", "volumetric_soil_water_layer_2",
        "volumetric_soil_water_layer_3", "volumetric_soil_water_layer_4"
      ),
      suggested_variables = c(
        "mean_2m_air_temperature", "minimum_2m_air_temperature", "maximum_2m_air_temperature",
        "total_precipitation", "u_component_of_wind_10m", "v_component_of_wind_10m",
        "surface_solar_radiation_downwards", "snow_depth",
        "volumetric_soil_water_layer_1", "volumetric_soil_water_layer_2",
        "volumetric_soil_water_layer_3", "volumetric_soil_water_layer_4"
      )
    )
  )
}

#' Build source specification from source name and variable mode.
resolve_climate_source <- function(source, variable_mode = c("suggested", "all", "custom"), custom_variables = NULL) {
  variable_mode <- match.arg(variable_mode)
  catalog <- climate_source_catalog()

  if (!source %in% names(catalog)) {
    stop(sprintf("Unknown source '%s'. Options: %s", source, paste(names(catalog), collapse = ", ")))
  }

  spec <- catalog[[source]]

  if (variable_mode == "suggested") {
    spec$variables <- spec$suggested_variables
  } else if (variable_mode == "all") {
    spec$variables <- spec$all_variables
  } else {
    if (is.null(custom_variables) || length(custom_variables) == 0) {
      stop("custom_variables must be provided when variable_mode='custom'.")
    }
    spec$variables <- custom_variables
  }

  spec$source <- source
  spec
}

#' Filter climate image collection to relevant date span.
get_source_collection <- function(spec, year, ee) {
  ic <- ee$ImageCollection(spec$gee_collection)

  if (spec$temporal_resolution == "monthly_climatology") {
    return(ic)
  }

  start_date <- sprintf("%d-01-01", as.integer(year))
  end_date <- sprintf("%d-01-01", as.integer(year) + 1)
  ic$filterDate(start_date, end_date)
}

#' Keep only variables that exist in the source image collection.
validate_source_variables <- function(collection, requested_variables, ee) {
  n_images <- collection$size()$getInfo()
  if (n_images == 0) {
    stop("No images found in source collection for requested dates.")
  }

  first_image <- ee$Image(collection$first())
  available_bands <- unlist(first_image$bandNames()$getInfo())

  vars <- intersect(requested_variables, available_bands)
  missing <- setdiff(requested_variables, available_bands)

  if (length(missing) > 0) {
    warning(sprintf(
      "Requested bands not available and will be skipped: %s",
      paste(missing, collapse = ", ")
    ))
  }

  if (length(vars) == 0) {
    stop("None of the requested variables are available in this collection.")
  }

  vars
}

#' Parse sampleRegions getInfo payload to a tibble.
parse_sample_regions <- function(result, longitude_col = "pixel_lon", latitude_col = "pixel_lat") {
  if (length(result$features) == 0) {
    return(tibble())
  }

  rows <- lapply(result$features, function(feature) {
    props <- feature$properties

    if (!is.null(feature$geometry) && !is.null(feature$geometry$coordinates)) {
      props[[longitude_col]] <- feature$geometry$coordinates[[1]]
      props[[latitude_col]] <- feature$geometry$coordinates[[2]]
    }

    as_tibble(props)
  })

  bind_rows(rows)
}

#' Extract all intersecting climate pixels for polygons for every image in collection.
#'
#' Returns one row per polygon x climate pixel x timestamp.
extract_polygon_pixel_timeseries <- function(sf_polygons,
                                             id_col,
                                             source_spec,
                                             year,
                                             ee,
                                             batch_size = 250,
                                             include_pixel_geometry = TRUE,
                                             tile_scale = 4) {
  stopifnot(inherits(sf_polygons, "sf"))

  sf_polygons <- st_make_valid(sf_polygons)

  collection <- get_source_collection(source_spec, year, ee)
  valid_variables <- validate_source_variables(collection, source_spec$variables, ee)
  n_images <- as.integer(collection$size()$getInfo())

  if (n_images == 0) {
    return(tibble())
  }

  image_list <- collection$toList(n_images)
  n_batches <- ceiling(nrow(sf_polygons) / batch_size)

  all_results <- vector("list", length = n_batches)

  for (b in seq_len(n_batches)) {
    start_idx <- (b - 1) * batch_size + 1
    end_idx <- min(b * batch_size, nrow(sf_polygons))
    polygon_batch <- sf_polygons[start_idx:end_idx, ]

    polygon_fc <- sf_polygons_to_ee(polygon_batch, id_col = id_col, ee = ee)
    batch_results <- vector("list", length = n_images)

    for (i in seq_len(n_images)) {
      image <- ee$Image(image_list$get(i - 1))$select(valid_variables)
      image_date <- ee$Date(image$get("system:time_start"))$format("YYYY-MM-dd")$getInfo()
      image_id <- image$get("system:index")$getInfo()

      sampled <- image$sampleRegions(
        collection = polygon_fc,
        scale = source_spec$nominal_scale_m,
        geometries = include_pixel_geometry,
        tileScale = tile_scale
      )

      sampled_info <- sampled$getInfo()
      parsed <- parse_sample_regions(sampled_info)

      if (nrow(parsed) > 0) {
        parsed$climate_source <- source_spec$source
        parsed$climate_date <- image_date
        parsed$climate_image_id <- image_id
      }

      batch_results[[i]] <- parsed
    }

    all_results[[b]] <- bind_rows(batch_results)
  }

  bind_rows(all_results)
}
