# ==============================================================================
# scripts/utils/climate_extract.R
# Core framework for pixel-level climate data extraction
# Supports: TerraClimate (GEE), PRISM (GEE), WorldClim (local), ERA5 (local)
# ==============================================================================

library(sf)
library(terra)
library(dplyr)
library(arrow)
library(exactextractr)
library(here)

# ==============================================================================
# PIXEL MAP CONSTRUCTION
# ==============================================================================

#' Build pixel map for a single sf object
#'
#' Maps each feature to the climate raster pixels it overlaps.
#' For polygons: uses exactextractr to get all pixels with coverage fractions
#' For points: uses terra::cellFromXY to get the containing pixel
#'
#' @param sf_obj sf object (points or polygons)
#' @param reference_raster SpatRaster defining the pixel grid
#' @param id_col Name of the unique ID column
#' @return data.frame with columns: id_col, pixel_id, x, y, coverage_fraction
build_pixel_map <- function(sf_obj, reference_raster, id_col) {

  sf_obj <- st_transform(sf_obj, crs(reference_raster))
  geom_type <- unique(st_geometry_type(sf_obj))


  if (all(geom_type %in% c("POINT", "MULTIPOINT"))) {
    # Point extraction: get cell for each point
    coords <- st_coordinates(sf_obj)
    cells <- cellFromXY(reference_raster, coords[, c("X", "Y")])
    xy <- xyFromCell(reference_raster, cells)

    pixel_map <- data.frame(
      id = sf_obj[[id_col]],
      pixel_id = cells,
      x = xy[, 1],
      y = xy[, 2],
      coverage_fraction = 1.0
    )
    names(pixel_map)[1] <- id_col

  } else {
    # Polygon extraction: get all overlapping pixels with coverage fractions
    extracted <- exact_extract(
      reference_raster,
      sf_obj,
      include_xy = TRUE,
      include_cell = TRUE,
      progress = FALSE
    )

    pixel_map <- bind_rows(lapply(seq_along(extracted), function(i) {
      df <- extracted[[i]]
      if (nrow(df) == 0) return(NULL)
      data.frame(
        id = sf_obj[[id_col]][i],
        pixel_id = df$cell,
        x = df$x,
        y = df$y,
        coverage_fraction = df$coverage_fraction
      )
    }))
    names(pixel_map)[1] <- id_col
  }

  # Remove any rows where pixel_id is NA (outside raster extent)
  pixel_map <- pixel_map[!is.na(pixel_map$pixel_id), ]

  return(pixel_map)
}


#' Build pixel maps for all IDS layers
#'
#' Handles the "pancake feature" issue where multiple observations share the same
#' DAMAGE_AREA_ID geometry. Builds pixel map on unique geometries, then joins
#' back to all OBSERVATION_IDs.
#'
#' @param ids_path Path to IDS geopackage
#' @param reference_raster SpatRaster defining the pixel grid
#' @param output_dir Directory to save pixel map parquet files
#' @param layers Character vector of layer names to process
#' @param conus_only If TRUE, filter to CONUS regions only (exclude R10, R5-HI)
#' @return List of pixel map data.frames (also saved as parquet)
build_ids_pixel_maps <- function(ids_path,
                                  reference_raster,
                                  output_dir,
                                  layers = c("damage_areas", "damage_points", "surveyed_areas"),
                                  conus_only = FALSE) {

  dir.create(output_dir, recursive = TRUE, showWarnings = FALSE)
  results <- list()

  for (layer in layers) {
    cat(sprintf("Building pixel map for %s...\n", layer))

    output_file <- file.path(output_dir, paste0(layer, "_pixel_map.parquet"))

    if (file.exists(output_file)) {
      cat("  Loading existing pixel map\n")
      results[[layer]] <- read_parquet(output_file)
      next
    }

    sf_obj <- st_read(ids_path, layer = layer, quiet = TRUE)

    # Filter to CONUS if requested (for PRISM which only covers CONUS)
    if (conus_only) {
      non_conus_regions <- c("10")  # Alaska
      if ("REGION_ID" %in% names(sf_obj)) {
        sf_obj <- sf_obj[!sf_obj$REGION_ID %in% non_conus_regions, ]
      }
      # Also filter Hawaii if present (R5-HI observations)
      # Hawaii observations would need separate handling
    }

    if (layer == "damage_areas") {
      # Handle pancake features: build map on unique DAMAGE_AREA_IDs
      pixel_map <- .build_damage_areas_pixel_map(sf_obj, reference_raster)
    } else if (layer == "damage_points") {
      pixel_map <- build_pixel_map(sf_obj, reference_raster, "OBSERVATION_ID")
    } else if (layer == "surveyed_areas") {
      pixel_map <- build_pixel_map(sf_obj, reference_raster, "SURVEY_FEATURE_ID")
    }

    write_parquet(pixel_map, output_file)
    cat(sprintf("  Saved %d pixel mappings to %s\n", nrow(pixel_map), basename(output_file)))

    results[[layer]] <- pixel_map
  }

  return(results)
}


#' Build pixel map for damage_areas handling pancake features
#' @keywords internal
.build_damage_areas_pixel_map <- function(sf_obj, reference_raster) {

  # Get unique geometries by DAMAGE_AREA_ID
  unique_geoms <- sf_obj %>%
    group_by(DAMAGE_AREA_ID) %>%
    slice(1) %>%
    ungroup() %>%
    select(DAMAGE_AREA_ID)

  cat(sprintf("  %d observations, %d unique geometries\n",
              nrow(sf_obj), nrow(unique_geoms)))

  # Build pixel map on unique geometries
  geom_pixel_map <- build_pixel_map(unique_geoms, reference_raster, "DAMAGE_AREA_ID")

  # Create lookup from DAMAGE_AREA_ID to OBSERVATION_ID
  obs_lookup <- sf_obj %>%
    st_drop_geometry() %>%
    select(OBSERVATION_ID, DAMAGE_AREA_ID)

  # Join to get OBSERVATION_ID level pixel map
  pixel_map <- geom_pixel_map %>%
    inner_join(obs_lookup, by = "DAMAGE_AREA_ID", relationship = "many-to-many") %>%
    select(OBSERVATION_ID, DAMAGE_AREA_ID, pixel_id, x, y, coverage_fraction)

  return(pixel_map)
}


# ==============================================================================
# GEE EXTRACTION (TerraClimate, PRISM)
# ==============================================================================

#' Extract climate data from GEE ImageCollection at pixel coordinates
#'
#' @param pixel_coords data.frame with x, y columns (unique pixels)
#' @param gee_asset GEE asset path (e.g., "IDAHO_EPSCOR/TERRACLIMATE")
#' @param variables Character vector of band names to extract
#' @param years Integer vector of years to extract
#' @param ee Initialized ee module from init_gee()
#' @param scale Pixel resolution in meters for sampling
#' @param batch_size Number of points per GEE request
#' @param output_dir Directory to save results (one parquet per year)
#' @param output_prefix Prefix for output files
#' @param scale_factors Named list of scale factors to apply
#' @param monthly If TRUE, extract monthly values; if FALSE, annual means
#' @return Invisibly returns NULL; results saved to parquet files
extract_climate_from_gee <- function(pixel_coords,
                                     gee_asset,
                                     variables,
                                     years,
                                     ee,
                                     scale = 4000,
                                     batch_size = 5000,
                                     output_dir,
                                     output_prefix,
                                     scale_factors = NULL,
                                     monthly = TRUE) {

  dir.create(output_dir, recursive = TRUE, showWarnings = FALSE)

  # Ensure unique pixels only
  pixel_coords <- pixel_coords %>%
    distinct(pixel_id, x, y)

  n_pixels <- nrow(pixel_coords)
  cat(sprintf("Extracting %d variables for %d unique pixels across %d years\n",
              length(variables), n_pixels, length(years)))

  for (year in years) {
    output_file <- file.path(output_dir, sprintf("%s_%d.parquet", output_prefix, year))

    if (file.exists(output_file)) {
      cat(sprintf("  %d: exists, skipping\n", year))
      next
    }

    cat(sprintf("  %d: ", year))

    if (monthly) {
      # Extract each month separately
      year_results <- list()

      for (month in 1:12) {
        start_date <- sprintf("%d-%02d-01", year, month)
        if (month == 12) {
          end_date <- sprintf("%d-01-01", year + 1)
        } else {
          end_date <- sprintf("%d-%02d-01", year, month + 1)
        }

        # Get image for this month
        img <- ee$ImageCollection(gee_asset)$
          filterDate(start_date, end_date)$
          select(variables)$
          first()

        # Extract in batches
        month_result <- .extract_gee_batches(
          pixel_coords, img, ee, scale, batch_size,
          verbose = FALSE
        )

        if (!is.null(month_result) && nrow(month_result) > 0) {
          month_result$year <- year
          month_result$month <- month
          year_results[[month]] <- month_result
        }

        cat(".")
      }

      year_data <- bind_rows(year_results)

    } else {
      # Annual mean
      start_date <- sprintf("%d-01-01", year)
      end_date <- sprintf("%d-01-01", year + 1)

      img <- ee$ImageCollection(gee_asset)$
        filterDate(start_date, end_date)$
        select(variables)$
        mean()

      year_data <- .extract_gee_batches(
        pixel_coords, img, ee, scale, batch_size,
        verbose = TRUE
      )

      if (!is.null(year_data) && nrow(year_data) > 0) {
        year_data$year <- year
      }
    }

    # Apply scale factors
    if (!is.null(scale_factors) && nrow(year_data) > 0) {
      for (var in names(scale_factors)) {
        if (var %in% names(year_data)) {
          year_data[[var]] <- year_data[[var]] * scale_factors[[var]]
        }
      }
    }

    write_parquet(year_data, output_file)
    cat(sprintf(" saved %d rows\n", nrow(year_data)))
  }

  invisible(NULL)
}


#' Extract from GEE image in batches
#' @keywords internal
.extract_gee_batches <- function(pixel_coords, image, ee, scale, batch_size, verbose = TRUE) {

  n_pixels <- nrow(pixel_coords)
  n_batches <- ceiling(n_pixels / batch_size)
  results <- list()

  for (i in seq_len(n_batches)) {
    start_idx <- (i - 1) * batch_size + 1
    end_idx <- min(i * batch_size, n_pixels)
    batch <- pixel_coords[start_idx:end_idx, ]

    # Convert to ee.FeatureCollection of points
    features <- lapply(seq_len(nrow(batch)), function(j) {
      geom <- ee$Geometry$Point(c(batch$x[j], batch$y[j]))
      ee$Feature(geom, list(pixel_id = batch$pixel_id[j]))
    })
    fc <- ee$FeatureCollection(features)

    # Sample regions
    sampled <- image$sampleRegions(
      collection = fc,
      scale = scale,
      geometries = FALSE
    )

    result <- sampled$getInfo()

    if (length(result$features) > 0) {
      rows <- lapply(result$features, function(f) {
        as.data.frame(f$properties)
      })
      results[[i]] <- bind_rows(rows)
    }

    if (verbose && n_batches > 1) {
      cat(sprintf("  Batch %d/%d\n", i, n_batches))
    }
  }

  bind_rows(results)
}


# ==============================================================================
# LOCAL RASTER EXTRACTION (WorldClim, ERA5)
# ==============================================================================

#' Extract climate data from local raster files at pixel coordinates
#'
#' @param pixel_coords data.frame with x, y columns (unique pixels)
#' @param raster_dir Directory containing raster files
#' @param file_pattern Function that returns file path given variable and year
#' @param variables Character vector of variable names
#' @param years Integer vector of years
#' @param output_dir Directory to save results
#' @param output_prefix Prefix for output files
#' @param scale_factors Named list of scale factors
#' @param bands_per_year Number of bands per file (12 for monthly, 365 for daily)
#' @param temporal_resolution "monthly" or "daily"
#' @param kelvin_vars Variables to convert from Kelvin to Celsius
#' @return Invisibly returns NULL; results saved to parquet files
extract_climate_from_rasters <- function(pixel_coords,
                                         raster_dir,
                                         file_pattern,
                                         variables,
                                         years,
                                         output_dir,
                                         output_prefix,
                                         scale_factors = NULL,
                                         bands_per_year = 12,
                                         temporal_resolution = "monthly",
                                         kelvin_vars = NULL) {

  dir.create(output_dir, recursive = TRUE, showWarnings = FALSE)

  pixel_coords <- pixel_coords %>%
    distinct(pixel_id, x, y)

  coords_matrix <- as.matrix(pixel_coords[, c("x", "y")])
  n_pixels <- nrow(pixel_coords)

  cat(sprintf("Extracting %d variables for %d unique pixels across %d years\n",
              length(variables), n_pixels, length(years)))

  for (year in years) {
    output_file <- file.path(output_dir, sprintf("%s_%d.parquet", output_prefix, year))

    if (file.exists(output_file)) {
      cat(sprintf("  %d: exists, skipping\n", year))
      next
    }

    cat(sprintf("  %d: ", year))

    year_results <- list()

    for (var in variables) {
      raster_file <- file_pattern(var, year)

      if (!file.exists(raster_file)) {
        cat(sprintf("missing %s, ", var))
        next
      }

      r <- rast(raster_file)

      # Extract all bands at pixel coordinates
      values <- extract(r, coords_matrix)

      # Remove ID column from terra::extract
      if ("ID" %in% names(values)) {
        values <- values[, -1, drop = FALSE]
      }

      # Build result data frame
      if (temporal_resolution == "monthly") {
        for (month in 1:min(nlyr(r), 12)) {
          var_result <- data.frame(
            pixel_id = pixel_coords$pixel_id,
            x = pixel_coords$x,
            y = pixel_coords$y,
            year = year,
            month = month
          )
          var_result[[var]] <- values[, month]
          year_results[[paste(var, month, sep = "_")]] <- var_result
        }
      } else if (temporal_resolution == "daily") {
        n_days <- nlyr(r)
        # Determine dates for each band
        dates <- seq(as.Date(paste0(year, "-01-01")),
                     as.Date(paste0(year, "-12-31")), by = "day")[1:n_days]

        for (day_idx in seq_len(n_days)) {
          var_result <- data.frame(
            pixel_id = pixel_coords$pixel_id,
            x = pixel_coords$x,
            y = pixel_coords$y,
            year = year,
            month = as.integer(format(dates[day_idx], "%m")),
            day = as.integer(format(dates[day_idx], "%d"))
          )
          var_result[[var]] <- values[, day_idx]
          year_results[[paste(var, day_idx, sep = "_")]] <- var_result
        }
      }

      cat(".")
    }

    # Merge all variables for this year
    if (length(year_results) > 0) {
      if (temporal_resolution == "monthly") {
        year_data <- year_results %>%
          reduce(full_join, by = c("pixel_id", "x", "y", "year", "month"))
      } else {
        year_data <- year_results %>%
          reduce(full_join, by = c("pixel_id", "x", "y", "year", "month", "day"))
      }

      # Apply scale factors
      if (!is.null(scale_factors)) {
        for (var in names(scale_factors)) {
          if (var %in% names(year_data)) {
            year_data[[var]] <- year_data[[var]] * scale_factors[[var]]
          }
        }
      }

      # Convert Kelvin to Celsius
      if (!is.null(kelvin_vars)) {
        for (var in kelvin_vars) {
          if (var %in% names(year_data)) {
            year_data[[var]] <- year_data[[var]] - 273.15
          }
        }
      }

      write_parquet(year_data, output_file)
      cat(sprintf(" saved %d rows\n", nrow(year_data)))
    } else {
      cat(" no data\n")
    }
  }

  invisible(NULL)
}


# ==============================================================================
# HELPER FUNCTIONS
# ==============================================================================

#' Get unique pixel coordinates from pixel map
#' @param pixel_map data.frame from build_pixel_map()
#' @return data.frame with unique pixel_id, x, y
get_unique_pixels <- function(pixel_map) {
  pixel_map %>%
    distinct(pixel_id, x, y)
}

#' Load pixel map from parquet
#' @param path Path to parquet file
#' @return data.frame
load_pixel_map <- function(path) {

  read_parquet(path)
}

#' Load pixel values from parquet files
#' @param dir Directory containing parquet files
#' @param prefix File prefix to match
#' @param years Optional vector of years to load
#' @return data.frame with all years combined
load_pixel_values <- function(dir, prefix, years = NULL) {
  files <- list.files(dir, pattern = paste0("^", prefix, "_\\d{4}\\.parquet$"),
                      full.names = TRUE)

  if (!is.null(years)) {
    year_pattern <- paste(years, collapse = "|")
    files <- files[grepl(paste0("_(", year_pattern, ")\\.parquet$"), files)]
  }

  bind_rows(lapply(files, read_parquet))
}

#' Get or create reference raster from GEE
#' @param gee_asset GEE asset path
#' @param output_path Path to save reference raster
#' @param ee Initialized ee module
#' @param band Band name to use
#' @param scale Resolution in meters
#' @param region ee.Geometry for region bounds
#' @return SpatRaster
get_reference_raster_from_gee <- function(gee_asset, output_path, ee,
                                          band = NULL, scale = 4000,
                                          region = NULL) {

  if (file.exists(output_path)) {
    return(rast(output_path))
  }

  dir.create(dirname(output_path), recursive = TRUE, showWarnings = FALSE)

  # Get first image from collection

img <- ee$ImageCollection(gee_asset)$first()

  if (!is.null(band)) {
    img <- img$select(band)
  } else {
    # Select first band
    img <- img$select(0L)
  }

  # Define region if not provided (CONUS + Alaska extent)
  if (is.null(region)) {
    region <- ee$Geometry$Rectangle(c(-180, 17, -64, 72))
  }

  # Get download URL
  url <- img$getDownloadURL(list(
    scale = scale,
    region = region,
    format = "GEO_TIFF"
  ))

  # Download
  download.file(url, output_path, mode = "wb", quiet = TRUE)

  rast(output_path)
}


#' Join pixel values back to observations using pixel map
#' @param pixel_values data.frame of climate values per pixel per time
#' @param pixel_map data.frame mapping observations to pixels
#' @param id_col Name of observation ID column in pixel_map
#' @return data.frame with climate values per observation per time
join_to_observations <- function(pixel_values, pixel_map, id_col = "OBSERVATION_ID") {

  # For observations with multiple pixels, compute weighted mean
  time_cols <- intersect(c("year", "month", "day"), names(pixel_values))
  value_cols <- setdiff(names(pixel_values), c("pixel_id", "x", "y", time_cols))

  # Join pixel values to pixel map
  joined <- pixel_map %>%
    inner_join(pixel_values, by = "pixel_id")

  # Compute weighted mean by observation and time
  result <- joined %>%
    group_by(across(all_of(c(id_col, time_cols)))) %>%
    summarize(
      across(all_of(value_cols),
             ~ weighted.mean(.x, coverage_fraction, na.rm = TRUE)),
      n_pixels = n(),
      .groups = "drop"
    )

  result
}
