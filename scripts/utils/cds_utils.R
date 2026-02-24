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
#' Returns TRUE for variables that are not available as ERA5 monthly means.
#'
#' ERA5 monthly means ("monthly_averaged_reanalysis", stream=moda) exclude a set
#' of "since previous post-processing" extrema/gust variables (e.g., mn2t/mx2t).
#' These repeatedly fail with MarsNoDataError if requested from the monthly means
#' dataset, so we skip them explicitly.
is_unsupported_monthly_mean_variable <- function(era5_name) {
  grepl("_since_previous_post_processing$", era5_name)
}


#' Download ERA5 variable for a single year
#'
#' @param client cdsapi.Client from init_cds()
#' @param variable_name Variable name as used in config
#' @param era5_name ERA5 API variable name
#' @param mars_short_name Optional MARS short name for retry on ambiguous mapping
#' @param year Integer year
#' @param area Bounding box as c(north, west, south, east)
#' @param output_path Full path for output NetCDF file
#' @return Path to downloaded file
download_era5_variable <- function(client, variable_name, era5_name, year,
                                   area, output_path, mars_short_name = NULL) {

  if (file.exists(output_path)) {
    cat(sprintf("  %s %d: exists, skipping\n", variable_name, year))
    return(output_path)
  }

  dir.create(dirname(output_path), recursive = TRUE, showWarnings = FALSE)

  if (is_unsupported_monthly_mean_variable(era5_name)) {
    cat(sprintf(
      "  %s %d: unsupported in ERA5 monthly means (CDS Table 8 \"no mean\"), skipping\n",
      variable_name, year
    ))
    return(output_path)
  }

  cat(sprintf("  %s %d: downloading...", variable_name, year))

  build_request <- function(request_variable) {
    list(
      product_type = "monthly_averaged_reanalysis",
      variable = request_variable,
      year = as.character(year),
      month = sprintf("%02d", 1:12),
      time = "00:00",
      area = area,
      data_format = "netcdf",
      download_format = "unarchived"
    )
  }

  try_retrieve <- function(request_variable) {
    client$retrieve(
      "reanalysis-era5-single-levels-monthly-means",
      build_request(request_variable),
      output_path
    )
  }

  # Submit request and download
  tryCatch({
    try_retrieve(era5_name)
    cat(" done\n")
  }, error = function(e) {
    msg <- as.character(e$message)

    # CDS/MARS can ambiguously map some long names (observed for total_evaporation).
    # Retry with a configured shortName if available.
    if (!is.null(mars_short_name) &&
        nzchar(mars_short_name) &&
        grepl("Ambiguous\\s*:", msg, ignore.case = TRUE)) {
      cat(sprintf(" retrying with MARS short name '%s'...", mars_short_name))
      tryCatch({
        try_retrieve(mars_short_name)
        cat(" done\n")
        return(invisible(NULL))
      }, error = function(e2) {
        msg <<- as.character(e2$message)
      })
    }

    cat(sprintf(" ERROR: %s\n", msg))
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

  supported_var_names <- names(variables)[vapply(
    variables,
    function(v) {
      era5_name <- if (is.null(v$era5_name)) "" else v$era5_name
      !is_unsupported_monthly_mean_variable(era5_name)
    },
    logical(1)
  )]

  for (var_name in supported_var_names) {
    var_config <- variables[[var_name]]
    era5_name <- var_config$era5_name
    mars_short_name <- var_config$mars_short_name
    if (is.null(mars_short_name)) {
      # Backward compatibility for existing configs using the old field name.
      mars_short_name <- var_config$era5_short_name
    }

    output_path <- file.path(output_dir, var_name, sprintf("%s_%d.nc", var_name, year))
    result <- download_era5_variable(
      client, var_name, era5_name, year, area, output_path,
      mars_short_name = mars_short_name
    )
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
