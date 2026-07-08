# ==============================================================================
# 02_extract_terraclimate.R
# Extract TerraClimate monthly data for FIA plot locations (1958-present).
#
# Reads 05_fia/data/processed/site_climate/all_site_locations.csv, maps each
# site to its containing TerraClimate pixel at native resolution, then extracts
# monthly climate values via Google Earth Engine for the full TerraClimate
# record.
#
# Variables extracted:
#   tmmx  - Maximum temperature (deg C, scale 0.1)
#   tmmn  - Minimum temperature (deg C, scale 0.1)
#   pr    - Precipitation (mm, scale 1.0)
#   def   - Climate water deficit (mm, scale 0.1) [CWD = PET - AET]
#   pet   - Reference ET (mm, scale 0.1)
#   aet   - Actual ET (mm, scale 0.1)
#
# Output:
#   05_fia/data/processed/site_climate/site_climate.parquet
#
# Output schema:
#   site_id, year, month, water_year, water_year_month, variable, value
#
# Usage:
#   Rscript 05_fia/scripts/site_climate/02_extract_terraclimate.R
#
# Prerequisite:
#   Google Earth Engine access configured in local/user_config.yaml.
# ==============================================================================

suppressPackageStartupMessages({
  library(here)
  library(sf)
  library(arrow)
  library(dplyr)
  library(tidyr)
  library(fs)
  library(terra)
})

source(here("scripts/utils/load_config.R"))
source(here("scripts/utils/climate_extract.R"))
source(here("scripts/utils/gee_utils.R"))
source(here("scripts/utils/time_utils.R"))

config <- load_config()
tc_config <- config$raw$terraclimate

out_dir <- here("05_fia/data/processed/site_climate")
out_file <- file.path(out_dir, "site_climate.parquet")
dir_create(out_dir)

site_vars <- c("tmmx", "tmmn", "pr", "def", "pet", "aet")
scale_factors <- vapply(
  tc_config$variables,
  function(v) v$scale,
  numeric(1)
)[site_vars]

# ------------------------------------------------------------------------------
# Site locations -> sf
# ------------------------------------------------------------------------------

site_file <- here("05_fia/data/processed/site_climate/all_site_locations.csv")
if (!file.exists(site_file)) {
  stop("Site list not found. Run 05_fia/scripts/site_climate/01_build_site_list.R first.")
}

sites <- read.csv(site_file, stringsAsFactors = FALSE)
required_site_cols <- c("site_id", "latitude", "longitude")
missing_site_cols <- setdiff(required_site_cols, names(sites))
if (length(missing_site_cols) > 0) {
  stop(sprintf(
    "Site list is missing required column(s): %s",
    paste(missing_site_cols, collapse = ", ")
  ))
}
if (anyDuplicated(sites$site_id)) {
  stop("Site list contains duplicate site_id values. Each stable FIA plot must appear once.")
}

invalid_coords <- is.na(sites$latitude) | is.na(sites$longitude) |
  sites$latitude < -90 | sites$latitude > 90 |
  sites$longitude < -180 | sites$longitude > 180
if (any(invalid_coords)) {
  stop(sprintf("Site list contains %s invalid coordinate row(s).", sum(invalid_coords)))
}

cat(sprintf("%d sites loaded\n", nrow(sites)))

sites_sf <- st_as_sf(
  sites,
  coords = c("longitude", "latitude"),
  crs = "+proj=longlat +datum=WGS84 +no_defs",
  remove = FALSE
)

# ------------------------------------------------------------------------------
# Map each site to a TerraClimate pixel
# ------------------------------------------------------------------------------
#
# Multiple FIA sites can fall in the same 4 km TerraClimate cell and therefore
# receive identical values. The script samples each unique pixel once, then joins
# the extracted values back to all site_ids through site_pixel_map.parquet.

res_deg <- 1 / 24
tc_global <- rast(
  xmin = -180, xmax = 180, ymin = -90, ymax = 90,
  resolution = res_deg,
  crs = "+proj=longlat +datum=WGS84 +no_defs"
)

coords <- st_coordinates(sites_sf)
cells <- cellFromXY(tc_global, coords)
xy <- xyFromCell(tc_global, cells)

pixel_map <- data.frame(
  site_id = sites_sf$site_id,
  pixel_id = cells,
  x = xy[, 1],
  y = xy[, 2],
  coverage_fraction = 1.0
)
n_pixels <- n_distinct(pixel_map$pixel_id)
cat(sprintf(
  "%d sites -> %d unique pixels (%.1f sites/pixel)\n",
  nrow(sites), n_pixels, nrow(sites) / n_pixels
))

write_parquet(
  as_tibble(pixel_map),
  file.path(out_dir, "site_pixel_map.parquet"),
  compression = "snappy"
)

# ------------------------------------------------------------------------------
# Extract TerraClimate via GEE
# ------------------------------------------------------------------------------
#
# For each year, GEE builds a stacked image of 6 variables x 12 months and
# samples it at the unique pixel centroids. Results are written as one parquet
# per year under _gee_annual/ so progress is preserved if the run is interrupted.

ee <- init_gee()

start_year <- 1958L
end_year <- as.integer(
  if (!is.null(tc_config$end_year)) tc_config$end_year
  else format(Sys.Date(), "%Y")
)
years <- start_year:end_year

cat(sprintf(
  "Extracting %d variables x 12 months x %d years (%d-%d) at %d pixels...\n",
  length(site_vars), length(years), start_year, end_year, n_pixels
))

pixel_coords <- pixel_map |>
  distinct(pixel_id, x, y)

tmp_dir <- file.path(out_dir, "_gee_annual")
dir_create(tmp_dir)

extract_climate_from_gee(
  pixel_coords = pixel_coords,
  gee_asset = tc_config$gee_asset,
  variables = site_vars,
  years = years,
  ee = ee,
  scale = tc_config$gee_scale,
  batch_size = 2500,
  output_dir = tmp_dir,
  output_prefix = "sites",
  scale_factors = scale_factors,
  monthly = TRUE
)

# ------------------------------------------------------------------------------
# Consolidate to final parquet
# ------------------------------------------------------------------------------
#
# GEE produces one parquet per year, each containing all 12 months. Stack them,
# join pixel_id back to site_id, and pivot from wide format to one row per
# site x year x month x variable.

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
  stop("No non-empty annual GEE parquet files found; extraction did not return usable climate data.")
}
cat(sprintf("Consolidating %d annual files...\n", length(annual_files)))

pm_slim <- pixel_map |> select(site_id, pixel_id)
long_chunks <- vector("list", length(annual_files))

for (i in seq_along(annual_files)) {
  long_chunks[[i]] <- read_parquet(annual_files[i]) |>
    distinct(pixel_id, month, .keep_all = TRUE) |>
    inner_join(pm_slim, by = "pixel_id", relationship = "many-to-many") |>
    select(site_id, year, month, all_of(site_vars)) |>
    pivot_longer(all_of(site_vars), names_to = "variable", values_to = "value")
}

site_climate <- bind_rows(long_chunks)
rm(long_chunks)
gc(verbose = FALSE)

wy <- calendar_to_water_year(site_climate$year, site_climate$month)
site_climate$water_year <- wy$water_year
site_climate$water_year_month <- wy$water_year_month

site_climate <- site_climate |>
  select(site_id, year, month, water_year, water_year_month, variable, value)

write_parquet(as_tibble(site_climate), out_file, compression = "snappy")
cat(sprintf(
  "Saved: %s (%s, %s rows)\n",
  basename(out_file), file_size(out_file),
  format(nrow(site_climate), big.mark = ",")
))
