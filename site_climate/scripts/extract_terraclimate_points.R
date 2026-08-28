# ==============================================================================
# extract_terraclimate_points.R
# Extract monthly TerraClimate values for a provided point table.
#
# Input:
#   A CSV with one row per site and these columns:
#     site_id, latitude, longitude, source
#
# Output:
#   site_pixel_map.parquet
#     One row per input site. Records the TerraClimate pixel each point snapped to.
#
#   site_climate.parquet
#     One row per site x year x month x variable for points where TerraClimate
#     returned valid land-pixel values.
#
#   qa/outputs/site_climate_missing_sites.csv
#     Input sites that snapped to a pixel but did not return climate values.
#
# Usage:
#   Rscript site_climate/scripts/extract_terraclimate_points.R
#
# Optional arguments:
#   --input=path/to/site_locations.csv
#   --output-dir=path/to/output_dir
#   --qa-dir=path/to/qa_output_dir
#   --start-year=1958
#   --end-year=2024
#   --variables=tmmx,tmmn,pr,def,pet,aet
#   --batch-size=2500
#   --backend=gee|local-ncss
#
# Prerequisite:
#   For --backend=gee, Google Earth Engine access configured in
#   local/user_config.yaml. The --backend=local-ncss route downloads fixed
#   regional grids from the official TerraClimate THREDDS NCSS and performs all
#   site/pixel matching locally.
# ==============================================================================

# Packages
suppressPackageStartupMessages({
  library(arrow)
  library(dplyr)
  library(fs)
  library(here)
  library(sf)
  library(terra)
  library(tidyr)
})

# Shared repo utilities
source(here("scripts/utils/load_config.R"))
source(here("scripts/utils/climate_extract.R"))
source(here("scripts/utils/gee_utils.R"))
source(here("scripts/utils/time_utils.R"))

# Simple --name=value argument parser
arg_value <- function(name, default = NULL) {
  args <- commandArgs(trailingOnly = TRUE)
  prefix <- paste0("--", name, "=")
  hit <- args[startsWith(args, prefix)]
  if (length(hit) == 0) {
    return(default)
  }
  sub(prefix, "", hit[[length(hit)]], fixed = TRUE)
}

# TerraClimate settings from config.yaml
config <- load_config()
tc_config <- config$raw$terraclimate

# Input and output paths
input_file <- arg_value(
  "input",
  here("site_climate/input/all_site_locations.csv")
)
output_dir <- arg_value(
  "output-dir",
  here("site_climate/data/processed")
)
qa_dir <- arg_value("qa-dir", here("site_climate/qa/outputs"))
dir_create(output_dir)
dir_create(qa_dir)

# Extraction year range
start_year <- as.integer(arg_value("start-year", "1958"))
end_year <- as.integer(arg_value(
  "end-year",
  if (!is.null(tc_config$end_year)) as.character(tc_config$end_year) else format(Sys.Date(), "%Y")
))
years <- start_year:end_year

# Variables and TerraClimate scale factors. The default is unchanged; the
# optional subset supports small, reproducible analysis-specific extractions.
default_site_vars <- c("tmmx", "tmmn", "pr", "def", "pet", "aet")
variables_arg <- arg_value("variables", paste(default_site_vars, collapse = ","))
site_vars <- trimws(strsplit(variables_arg, ",", fixed = TRUE)[[1]])
unknown_vars <- setdiff(site_vars, names(tc_config$variables))
if (length(site_vars) == 0 || length(unknown_vars) > 0) {
  stop(sprintf(
    "Unknown or empty TerraClimate variable selection: %s",
    paste(unknown_vars, collapse = ", ")
  ))
}
batch_size <- as.integer(arg_value("batch-size", "2500"))
if (is.na(batch_size) || batch_size < 1L) {
  stop("--batch-size must be a positive integer.")
}
backend <- arg_value("backend", "gee")
if (!backend %in% c("gee", "local-ncss")) {
  stop("--backend must be either 'gee' or 'local-ncss'.")
}
scale_factors <- vapply(
  tc_config$variables,
  function(v) v$scale,
  numeric(1)
)[site_vars]

# Input file existence
if (!file.exists(input_file)) {
  stop(sprintf("Input point CSV not found: %s", input_file))
}

# Required point-table columns
sites <- read.csv(input_file, stringsAsFactors = FALSE)
required_cols <- c("site_id", "latitude", "longitude", "source")
missing_cols <- setdiff(required_cols, names(sites))
if (length(missing_cols) > 0) {
  stop(sprintf(
    "Input point CSV is missing required column(s): %s",
    paste(missing_cols, collapse = ", ")
  ))
}

# One row per site
if (anyDuplicated(sites$site_id)) {
  stop("Input point CSV contains duplicate site_id values.")
}

# Coordinate sanity checks
bad_coords <- is.na(sites$latitude) | is.na(sites$longitude) |
  sites$latitude < -90 | sites$latitude > 90 |
  sites$longitude < -180 | sites$longitude > 180
if (any(bad_coords)) {
  stop(sprintf("Input point CSV contains %s invalid coordinate row(s).", sum(bad_coords)))
}

cat(sprintf("Loaded %s sites from %s\n", nrow(sites), input_file))

# Preserve input order for output reproducibility
sites$site_order <- seq_len(nrow(sites))

# Convert points to sf
sites_sf <- st_as_sf(
  sites,
  coords = c("longitude", "latitude"),
  crs = "+proj=longlat +datum=WGS84 +no_defs",
  remove = FALSE
)

# Global TerraClimate grid
# TerraClimate is a global 1/24 degree grid. Build that grid directly instead
# of borrowing an extent from another extraction, because region-limited rasters
# can silently assign incorrect cell IDs for points outside their extent.
res_deg <- 1 / 24
tc_global <- rast(
  xmin = -180, xmax = 180,
  ymin = -90, ymax = 90,
  resolution = res_deg,
  crs = "+proj=longlat +datum=WGS84 +no_defs"
)

# Snap sites to TerraClimate cell centers
coords <- st_coordinates(sites_sf)
cells <- cellFromXY(tc_global, coords)
xy <- xyFromCell(tc_global, cells)

# Site-to-pixel lookup table
pixel_map <- data.frame(
  site_id = sites_sf$site_id,
  pixel_id = as.numeric(cells),
  x = xy[, 1],
  y = xy[, 2],
  coverage_fraction = 1.0
)

pixel_map_file <- file.path(output_dir, "site_pixel_map.parquet")
write_parquet(as_tibble(pixel_map), pixel_map_file, compression = "snappy")

n_pixels <- n_distinct(pixel_map$pixel_id)
cat(sprintf(
  "%s sites snapped to %s unique TerraClimate pixels\n",
  nrow(sites), n_pixels
))

cat(sprintf(
  "Extracting %s variables x 12 months x %s years (%s-%s) at %s pixels using %s...\n",
  length(site_vars), length(years), start_year, end_year, n_pixels, backend
))

# Extract each unique TerraClimate pixel once
pixel_coords <- pixel_map |>
  distinct(pixel_id, x, y)

if (backend == "gee") {
  # Annual checkpoint directory
  tmp_dir <- file.path(output_dir, "_gee_annual")
  dir_create(tmp_dir)

  # Write one GEE extraction parquet per year
  ee <- init_gee()
  extract_climate_from_gee(
    pixel_coords = pixel_coords,
    gee_asset = tc_config$gee_asset,
    variables = site_vars,
    years = years,
    ee = ee,
    scale = tc_config$gee_scale,
    batch_size = batch_size,
    output_dir = tmp_dir,
    output_prefix = "sites",
    scale_factors = scale_factors,
    monthly = TRUE
  )
} else {
  # Local NCSS backend: download fixed broad regional grids from the official
  # TerraClimate THREDDS server, extract the already-mapped pixel centers
  # locally, and discard each temporary NetCDF. No site coordinate or site ID is
  # sent to the remote service.
  if (!identical(site_vars, "def")) {
    stop("The local-ncss backend currently supports --variables=def only.")
  }

  tmp_dir <- file.path(output_dir, "_local_annual")
  netcdf_dir <- file.path(output_dir, "_netcdf_temp")
  dir_create(tmp_dir)
  dir_create(netcdf_dir)

  fixed_regions <- data.frame(
    region = c("conus", "alaska", "hawaii"),
    west = c(-125, -155, -161),
    east = c(-66, -129, -154),
    south = c(24, 54, 18),
    north = c(50, 62, 23)
  )
  pixel_coords$region <- ifelse(
    pixel_coords$y < 25 & pixel_coords$x < -150, "hawaii",
    ifelse(pixel_coords$y > 50, "alaska", "conus")
  )

  outside <- pixel_coords |>
    inner_join(fixed_regions, by = "region") |>
    filter(x < west | x > east | y < south | y > north)
  if (nrow(outside) > 0) {
    stop(sprintf("%s mapped pixels fall outside the fixed NCSS regions.", nrow(outside)))
  }

  ncss_base <- paste0(
    "https://tds-proxy.nkn.uidaho.edu/thredds/ncss/grid/",
    "TERRACLIMATE_ALL/data/TerraClimate_def_%d.nc"
  )

  for (year in years) {
    annual_file <- file.path(tmp_dir, sprintf("sites_%d.parquet", year))
    if (file.exists(annual_file)) {
      cat(sprintf("  %d: exists, skipping\n", year))
      next
    }

    cat(sprintf("  %d: ", year))
    region_results <- list()
    year_available <- TRUE

    for (j in seq_len(nrow(fixed_regions))) {
      region_row <- fixed_regions[j, ]
      region_pixels <- pixel_coords |>
        filter(region == region_row$region)
      if (nrow(region_pixels) == 0) next

      query <- paste0(
        sprintf(ncss_base, year),
        "?var=def",
        "&north=", region_row$north,
        "&west=", region_row$west,
        "&east=", region_row$east,
        "&south=", region_row$south,
        "&horizStride=1",
        "&time_start=", year, "-01-01T00%3A00%3A00Z",
        "&time_end=", year, "-12-01T00%3A00%3A00Z",
        "&timeStride=1&accept=netcdf"
      )
      nc_path <- file.path(
        netcdf_dir,
        sprintf("TerraClimate_def_%d_%s.nc", year, region_row$region)
      )

      downloaded <- tryCatch({
        utils::download.file(query, nc_path, mode = "wb", quiet = TRUE,
                             method = "libcurl")
        TRUE
      }, error = function(e) {
        message(sprintf("\n    %s unavailable: %s", region_row$region,
                        conditionMessage(e)))
        FALSE
      })
      if (!downloaded || !file.exists(nc_path)) {
        year_available <- FALSE
        break
      }

      raster <- terra::rast(nc_path)
      raster_dates <- as.Date(terra::time(raster))
      if (length(raster_dates) != 12L || anyNA(raster_dates)) {
        stop(sprintf("Unexpected monthly time axis in %s.", nc_path))
      }
      values <- terra::extract(
        raster,
        as.matrix(region_pixels[, c("x", "y")])
      )
      if ("ID" %in% names(values)) values$ID <- NULL

      month_parts <- lapply(seq_len(terra::nlyr(raster)), function(k) {
        data.frame(
          pixel_id = region_pixels$pixel_id,
          x = region_pixels$x,
          y = region_pixels$y,
          year = as.integer(format(raster_dates[k], "%Y")),
          month = as.integer(format(raster_dates[k], "%m")),
          def = as.numeric(values[[k]])
        )
      })
      region_results[[region_row$region]] <- bind_rows(month_parts) |>
        filter(is.finite(def))

      # The NetCDF is a reproducible download checkpoint, not a retained input.
      unlink(nc_path)
    }

    if (!year_available) {
      existing_nc <- list.files(
        netcdf_dir,
        pattern = sprintf("^TerraClimate_def_%d_.*[.]nc$", year),
        full.names = TRUE
      )
      if (length(existing_nc) > 0) unlink(existing_nc)
      cat("unavailable, skipped\n")
      next
    }

    year_data <- bind_rows(region_results) |>
      select(pixel_id, x, y, year, month, def)
    write_parquet(year_data, annual_file, compression = "snappy")
    cat(sprintf("saved %s pixel-month rows\n", format(nrow(year_data), big.mark = ",")))
  }
}

# Completed annual files
annual_files <- list.files(
  tmp_dir,
  pattern = "^sites_\\d{4}\\.parquet$",
  full.names = TRUE
)
annual_files <- annual_files[vapply(
  annual_files,
  function(f) nrow(read_parquet(f)) > 0,
  logical(1)
)]
if (length(annual_files) == 0) {
  stop("No non-empty annual TerraClimate parquet files found.")
}

cat(sprintf("Consolidating %s annual files...\n", length(annual_files)))

# Pixel-to-site join key, including original input order
pm_slim <- pixel_map |>
  left_join(st_drop_geometry(sites_sf) |> select(site_id, site_order), by = "site_id") |>
  select(site_id, site_order, pixel_id)
long_chunks <- vector("list", length(annual_files))

# Convert annual wide files to long site x month x variable rows
for (i in seq_along(annual_files)) {
  long_chunks[[i]] <- read_parquet(annual_files[i]) |>
    distinct(pixel_id, month, .keep_all = TRUE) |>
    inner_join(pm_slim, by = "pixel_id", relationship = "many-to-many") |>
    select(site_id, site_order, year, month, all_of(site_vars)) |>
    pivot_longer(all_of(site_vars), names_to = "variable", values_to = "value")
}

# Combine years
site_climate <- bind_rows(long_chunks)
rm(long_chunks)
gc(verbose = FALSE)

# Calendar year/month -> water year fields
wy <- calendar_to_water_year(site_climate$year, site_climate$month)
site_climate$water_year <- wy$water_year
site_climate$water_year_month <- wy$water_year_month

# Stable output order and final schema
site_climate <- site_climate |>
  mutate(variable = factor(variable, levels = site_vars)) |>
  arrange(year, month, site_order, variable) |>
  mutate(variable = as.character(variable)) |>
  select(site_id, year, month, water_year, water_year_month, variable, value)

climate_file <- file.path(output_dir, "site_climate.parquet")
write_parquet(as_tibble(site_climate), climate_file, compression = "snappy")

# Sites that snapped to a pixel but returned no TerraClimate values
missing_sites <- sites |>
  filter(!site_id %in% unique(site_climate$site_id)) |>
  left_join(pixel_map, by = "site_id")

if (nrow(missing_sites) > 0) {
  write.csv(
    missing_sites,
    file.path(qa_dir, "site_climate_missing_sites.csv"),
    row.names = FALSE
  )
}

# Run summary
cat("\nDone.\n")
cat(sprintf("Pixel map:    %s\n", pixel_map_file))
cat(sprintf("Climate data: %s\n", climate_file))
cat(sprintf("Sites input:  %s\n", nrow(sites)))
cat(sprintf("Sites output: %s\n", n_distinct(site_climate$site_id)))
cat(sprintf("Missing:      %s\n", nrow(missing_sites)))
