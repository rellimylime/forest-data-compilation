# ==============================================================================
# scripts/utils/cds_utils.R
# ERA5 Climate Data Store (CDS) API utilities using reticulate
# ==============================================================================

library(reticulate)
library(here)
library(yaml)

# ==============================================================================
# INITIALIZATION
# ==============================================================================

#' Initialize CDS API client
#'
#' Requires ~/.cdsapirc file with CDS credentials:
#'   url: https://cds.climate.copernicus.eu/api
#'   key: <your-uid>:<your-api-key>
#'
#' @return cdsapi.Client object
init_cds <- function() {
  cdsapi <- import("cdsapi")
  client <- cdsapi$Client()
  return(client)
}

# ==============================================================================
# DOWNLOAD FUNCTIONS
# ==============================================================================
#' Download ERA5 variable for a single year
#'
#' @param client cdsapi.Client from init_cds()
#' @param variable_name Variable name as used in config
#' @param era5_name ERA5 API variable name
#' @param year Integer year
#' @param area Bounding box as c(north, west, south, east)
#' @param output_path Full path for output NetCDF file
#' @return Path to downloaded file
download_era5_variable <- function(client, variable_name, era5_name, year,
                                   area, output_path) {

  if (file.exists(output_path)) {
    cat(sprintf("  %s %d: exists, skipping\n", variable_name, year))
    return(output_path)
  }

  dir.create(dirname(output_path), recursive = TRUE, showWarnings = FALSE)

  cat(sprintf("  %s %d: downloading...", variable_name, year))

  # Build request
  request <- list(
    product_type = "reanalysis",
    variable = era5_name,
    year = as.character(year),
    month = sprintf("%02d", 1:12),
    day = sprintf("%02d", 1:31),
    time = sprintf("%02d:00", 0:23),
    area = area,
    data_format = "netcdf",
    download_format = "unarchived"
  )

  # Submit request and download
  tryCatch({
    client$retrieve(
      "reanalysis-era5-single-levels",
      request,
      output_path
    )
    cat(" done\n")
  }, error = function(e) {
    cat(sprintf(" ERROR: %s\n", e$message))
    if (file.exists(output_path)) file.remove(output_path)
  })

  return(output_path)
}


#' Download all ERA5 variables for a year
#'
#' @param client cdsapi.Client
#' @param config Full config list
#' @param year Integer year
#' @param output_dir Base output directory
#' @return Vector of downloaded file paths
download_era5_year <- function(client, config, year, output_dir) {

  era5_config <- config$raw$era5
  variables <- era5_config$variables
  area <- era5_config$cds_area

  paths <- character()

  for (var_name in names(variables)) {
    var_config <- variables[[var_name]]
    era5_name <- var_config$era5_name

    output_path <- file.path(output_dir, var_name, sprintf("%s_%d.nc", var_name, year))
    result <- download_era5_variable(client, var_name, era5_name, year, area, output_path)
    paths <- c(paths, result)
  }

  return(paths)
}


#' Download all ERA5 data for multiple years
#'
#' @param config Full config list
#' @param years Integer vector of years
#' @param output_dir Base output directory
#' @return Invisibly returns NULL
download_era5_all <- function(config, years, output_dir) {

  client <- init_cds()

  for (year in years) {
    cat(sprintf("Year %d:\n", year))
    download_era5_year(client, config, year, output_dir)
  }

  invisible(NULL)
}
