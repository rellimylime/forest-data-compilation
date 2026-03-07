# ==============================================================================
# 06_extract_site_climate.R
# Extract TerraClimate monthly data for site locations (1958-present)
#
# Reads data/processed/site_climate/all_site_locations.csv (lat/lon for each
# site), maps each site to its containing TerraClimate pixel (~4km), then
# extracts monthly climate values via Google Earth Engine for the full
# TerraClimate record (1958-present).
#
# Variables extracted:
#   tmmx  - Maximum temperature (°C,  scale 0.1)
#   tmmn  - Minimum temperature (°C,  scale 0.1)
#   pr    - Precipitation (mm,         scale 1.0)
#   def   - Climate water deficit (mm, scale 0.1)  [= CWD = PET - AET]
#   pet   - Reference ET (mm,          scale 0.1)
#   aet   - Actual ET (mm,             scale 0.1)
#
# Output: 05_fia/data/processed/site_climate/site_climate.parquet
#   Schema: site_id (character), year (int), month (int),
#           water_year (int), water_year_month (int),
#           variable (character), value (double)
#
# Usage:
#   Rscript 05_fia/scripts/06_extract_site_climate.R
#
# Prerequisites: GEE account configured (see local/user_config.yaml)
#   Rscript 02_terraclimate/scripts/01_build_pixel_maps.R
# ==============================================================================

library(here)
library(sf)
library(arrow)
library(dplyr)
library(tidyr)
library(fs)
library(terra)

source(here("scripts/utils/load_config.R"))
source(here("scripts/utils/climate_extract.R"))
source(here("scripts/utils/gee_utils.R"))
source(here("scripts/utils/time_utils.R"))

config    <- load_config()
tc_config <- config$raw$terraclimate

out_dir  <- here("05_fia/data/processed/site_climate")
out_file <- file.path(out_dir, "site_climate.parquet")
dir_create(out_dir)

site_vars     <- c("tmmx", "tmmn", "pr", "def", "pet", "aet")
scale_factors <- vapply(
  tc_config$variables, function(v) v$scale, numeric(1)
)[site_vars]

# ------------------------------------------------------------------------------
# Site locations → sf
# ------------------------------------------------------------------------------

sites <- read.csv(
  here("05_fia/data/processed/site_climate/all_site_locations.csv"),
  stringsAsFactors = FALSE
)
cat(sprintf("%d sites loaded\n", nrow(sites)))

sites_sf <- st_as_sf(
  sites,
  coords = c("longitude", "latitude"),
  crs    = "+proj=longlat +datum=WGS84 +no_defs",  # proj4: avoids PROJ db lookup
  remove = FALSE
)

# ------------------------------------------------------------------------------
# Map each site to a TerraClimate pixel
# ------------------------------------------------------------------------------
#
# Multiple sites can fall in the same 4km cell and would get identical values,
# so we deduplicate to unique pixels before extracting. site_pixel_map.parquet
# records which pixel each site maps to so we can join results back at the end.
#
# Snap each site to its containing TerraClimate pixel using the global TC grid
# (1/24° resolution, full geographic extent). pixel_id is the global cell number,
# which is the same ID that extract_climate_from_gee() embeds in its output,
# so the join in the consolidation step is unambiguous.

res_deg   <- 1 / 24  # TerraClimate native resolution in degrees
tc_global <- rast(
  xmin = -180, xmax = 180, ymin = -90, ymax = 90,
  resolution = res_deg,
  crs = "+proj=longlat +datum=WGS84 +no_defs"  # proj4: avoids PROJ db lookup
)

coords <- st_coordinates(sites_sf)
cells  <- cellFromXY(tc_global, coords)
xy     <- xyFromCell(tc_global, cells)

pixel_map <- data.frame(
  site_id           = sites_sf$site_id,
  pixel_id          = cells,
  x                 = xy[, 1],
  y                 = xy[, 2],
  coverage_fraction = 1.0
)
n_pixels  <- n_distinct(pixel_map$pixel_id)
cat(sprintf(
  "%d sites → %d unique pixels (%.1f sites/pixel)\n",
  nrow(sites), n_pixels, nrow(sites) / n_pixels
))

write_parquet(
  as_tibble(pixel_map),
  file.path(out_dir, "site_pixel_map.parquet"),
  compression = "snappy"
)

# ------------------------------------------------------------------------------
# Extract TerraClimate via GEE (1958-present)
# ------------------------------------------------------------------------------
#
# For each year, GEE builds a stacked image of 6 variables × 12 months and
# samples it at the unique pixel centroids. Results are written as one parquet
# per year to _gee_annual/ so progress is preserved if the run is interrupted.

ee <- init_gee()

start_year <- 1958L
end_year   <- as.integer(
  if (!is.null(tc_config$end_year)) tc_config$end_year
  else format(Sys.Date(), "%Y")
)
years <- start_year:end_year

cat(sprintf(
  "Extracting %d variables × 12 months × %d years (%d–%d) at %d pixels...\n",
  length(site_vars), length(years), start_year, end_year, n_pixels
))

pixel_coords <- pixel_map |>
  distinct(pixel_id, x, y)

tmp_dir <- file.path(out_dir, "_gee_annual")
dir_create(tmp_dir)

extract_climate_from_gee(
  pixel_coords  = pixel_coords,
  gee_asset     = tc_config$gee_asset,
  variables     = site_vars,
  years         = years,
  ee            = ee,
  scale         = tc_config$gee_scale,
  batch_size    = 2500,
  output_dir    = tmp_dir,
  output_prefix = "sites",
  scale_factors = scale_factors,
  monthly       = TRUE
)

# ------------------------------------------------------------------------------
# Consolidate to final parquet
# ------------------------------------------------------------------------------
#
# GEE produced one parquet per year, each containing all 12 months. Stack them,
# join pixel_id back to site_id, and pivot from wide (one column per variable)
# to long (one row per site × year × month × variable).

annual_files <- list.files(
  tmp_dir, pattern = "^sites_\\d{4}\\.parquet$", full.names = TRUE
)
# Drop empty files (e.g. future years where GEE returned no data)
annual_files <- annual_files[vapply(annual_files, function(f) nrow(read_parquet(f)) > 0, logical(1))]
cat(sprintf("Consolidating %d annual files...\n", length(annual_files)))

pm_slim     <- pixel_map |> select(site_id, pixel_id)
long_chunks <- vector("list", length(annual_files))

for (i in seq_along(annual_files)) {
  long_chunks[[i]] <- read_parquet(annual_files[i]) |>
    distinct(pixel_id, month, .keep_all = TRUE) |>  # drop float near-dup pixels
    inner_join(pm_slim, by = "pixel_id", relationship = "many-to-many") |>
    select(site_id, year, month, all_of(site_vars)) |>
    pivot_longer(all_of(site_vars), names_to = "variable", values_to = "value")
}

site_climate <- bind_rows(long_chunks)
rm(long_chunks)
gc(verbose = FALSE)

# Add water year (Oct-Sep: month >= 10 rolls into the next water year)
wy <- calendar_to_water_year(site_climate$year, site_climate$month)
site_climate$water_year       <- wy$water_year
site_climate$water_year_month <- wy$water_year_month

site_climate <- site_climate |>
  select(site_id, year, month, water_year, water_year_month, variable, value)

write_parquet(as_tibble(site_climate), out_file, compression = "snappy")
cat(sprintf(
  "Saved: %s (%s, %s rows)\n",
  basename(out_file), file_size(out_file),
  format(nrow(site_climate), big.mark = ",")
))
