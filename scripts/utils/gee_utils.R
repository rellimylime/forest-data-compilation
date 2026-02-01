# ==============================================================================
# scripts/utils/gee_utils.R
# Google Earth Engine utilities using reticulate
# ==============================================================================

library(reticulate)
library(yaml)
library(here)
library(sf)
library(dplyr)

# ==============================================================================
# INITIALIZATION
# ==============================================================================

#' Initialize Google Earth Engine
#' @return ee Python module
init_gee <- function() {
  library(reticulate)
  library(yaml)
  library(here)
  
  local_config <- read_yaml(here("local/user_config.yaml"))
  
  # Python path now set via .Renviron - just import and initialize
  ee <- import("ee")
  ee$Initialize(project = local_config$gee_project)
  
  return(ee)
}

# ==============================================================================
# CONVERSION FUNCTIONS
# ==============================================================================

#' Convert sf points to ee.FeatureCollection
#' @param sf_obj sf object with point geometry
#' @param id_col Name of ID column to preserve
#' @param ee ee module (from init_gee())
#' @return ee.FeatureCollection
sf_points_to_ee <- function(sf_obj, id_col, ee) {
  # Ensure points
  
  if (!all(st_geometry_type(sf_obj) == "POINT")) {
    stop("sf_obj must contain POINT geometries")
  }
  
  # Ensure WGS84
  
  sf_obj <- st_transform(sf_obj, 4326)
  
  # Extract coordinates and IDs
  coords <- st_coordinates(sf_obj)
  ids <- sf_obj[[id_col]]
  
  # Create ee.Feature for each point
  features <- lapply(seq_len(nrow(sf_obj)), function(i) {
    geom <- ee$Geometry$Point(c(coords[i, "X"], coords[i, "Y"]))
    props <- list()
    props[[id_col]] <- ids[i]
    ee$Feature(geom, props)
  })
  
  ee$FeatureCollection(features)
}

#' Convert sf polygons to ee.FeatureCollection
#' @param sf_obj sf object with polygon geometry
#' @param id_col Name of ID column to preserve
#' @param ee ee module (from init_gee())
#' @return ee.FeatureCollection
sf_polygons_to_ee <- function(sf_obj, id_col, ee) {
  # Ensure WGS84
  
  sf_obj <- st_transform(sf_obj, 4326)
  
  features <- lapply(seq_len(nrow(sf_obj)), function(i) {
    # Extract coordinates from geometry
    geom <- st_geometry(sf_obj[i, ])[[1]]
    
    # Handle POLYGON vs MULTIPOLYGON
    if (inherits(geom, "MULTIPOLYGON")) {
      # Take first polygon only
      coords_mat <- geom[[1]][[1]]
    } else {
      # POLYGON - first ring (exterior)
      coords_mat <- geom[[1]]
    }
    
    # Convert to list of [lon, lat] pairs
    coord_list <- lapply(seq_len(nrow(coords_mat)), function(j) {
      c(coords_mat[j, 1], coords_mat[j, 2])
    })
    
    ee_geom <- ee$Geometry$Polygon(list(coord_list))
    
    props <- list()
    props[[id_col]] <- sf_obj[[id_col]][i]
    ee$Feature(ee_geom, props)
  })
  
  ee$FeatureCollection(features)
}

# ==============================================================================
# EXTRACTION FUNCTIONS
# ==============================================================================

#' Extract image values at points
#' @param image ee.Image to extract from
#' @param points_fc ee.FeatureCollection of points
#' @param scale Pixel resolution in meters
#' @param ee ee module
#' @return data.frame with extracted values
extract_at_points <- function(image, points_fc, scale, ee) {
  
  # Sample regions
  sampled <- image$sampleRegions(
    collection = points_fc,
    scale = scale,
    geometries = FALSE
  )
  
  
  # Get info (brings data to R)
  result <- sampled$getInfo()
  
  # Parse features into data.frame
  if (length(result$features) == 0) {
    warning("No features returned from extraction")
    return(data.frame())
  }
  
  # Extract properties from each feature
  rows <- lapply(result$features, function(f) {
    as.data.frame(f$properties)
  })
  
  bind_rows(rows)
}

#' Extract image values for polygons (mean reducer)
#' @param image ee.Image to extract from
#' @param polygons_fc ee.FeatureCollection of polygons
#' @param scale Pixel resolution in meters
#' @param ee ee module
#' @return data.frame with extracted values
extract_polygon_mean <- function(image, polygons_fc, scale, ee) {
  
  # Reduce regions
  reduced <- image$reduceRegions(
    collection = polygons_fc,
    reducer = ee$Reducer$mean(),
    scale = scale
  )
  
  # Get info
  result <- reduced$getInfo()
  
  if (length(result$features) == 0) {
    warning("No features returned from extraction")
    return(data.frame())
  }
  
  rows <- lapply(result$features, function(f) {
    as.data.frame(f$properties)
  })
  
  bind_rows(rows)
}

# ==============================================================================
# TERRACLIMATE HELPERS
# ==============================================================================

#' Get TerraClimate annual mean for a year
#' @param year Integer year
#' @param bands Character vector of band names
#' @param ee ee module
#' @return ee.Image with annual means
get_terraclimate_annual <- function(year, bands, ee) {
  start_date <- paste0(year, "-01-01")
  end_date <- paste0(year + 1, "-01-01")
  
  ee$ImageCollection("IDAHO_EPSCOR/TERRACLIMATE")$
    filterDate(start_date, end_date)$
    select(bands)$
    mean()
}

#' Apply scale factors to TerraClimate data
#' @param df data.frame with raw TerraClimate values
#' @param config config list with variable scale factors
#' @return data.frame with scaled values
apply_terraclimate_scales <- function(df, config) {
  tc_vars <- config$raw$terraclimate$variables
  
  for (var_name in names(tc_vars)) {
    if (var_name %in% names(df)) {
      scale <- tc_vars[[var_name]]$scale
      df[[var_name]] <- df[[var_name]] * scale
    }
  }
  
  df
}

# ==============================================================================
# BATCH PROCESSING
# ==============================================================================

#' Process extraction in batches
#' @param sf_obj sf object to extract for
#' @param id_col ID column name
#' @param image ee.Image to extract from
#' @param ee ee module
#' @param batch_size Number of features per batch (default 5000)
#' @param method "centroid" or "polygon"
#' @param scale Pixel resolution in meters
#' @param verbose Print progress
#' @return data.frame with all extracted values
extract_in_batches <- function(sf_obj, id_col, image, ee, 
                               batch_size = 5000, 
                               method = "centroid",
                               scale = 4000,
                               verbose = TRUE) {
  
  n_total <- nrow(sf_obj)
  n_batches <- ceiling(n_total / batch_size)
  
  if (verbose) {
    cat(sprintf("Extracting %d features in %d batches...\n", n_total, n_batches))
  }
  
  results <- list()
  
  for (i in seq_len(n_batches)) {
    start_idx <- (i - 1) * batch_size + 1
    end_idx <- min(i * batch_size, n_total)
    
    batch <- sf_obj[start_idx:end_idx, ]
    
    if (verbose) {
      cat(sprintf("  Batch %d/%d (features %d-%d)...", i, n_batches, start_idx, end_idx))
    }
    
    t1 <- Sys.time()
    
    if (method == "centroid") {
      centroids <- st_centroid(batch)
      fc <- sf_points_to_ee(centroids, id_col, ee)
      batch_result <- extract_at_points(image, fc, scale, ee)
    } else {
      fc <- sf_polygons_to_ee(batch, id_col, ee)
      batch_result <- extract_polygon_mean(image, fc, scale, ee)
    }
    
    t2 <- Sys.time()
    
    if (verbose) {
      cat(sprintf(" done (%.1f sec)\n", difftime(t2, t1, units = "secs")))
    }
    
    results[[i]] <- batch_result
  }
  
  bind_rows(results)
}