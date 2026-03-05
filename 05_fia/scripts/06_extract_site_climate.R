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
#   Rscript 02_terraclimate/scripts/01_build_pixel_maps.R  (for reference raster)
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

cat("Site Climate Extraction (TerraClimate via GEE)\n")
cat("===============================================\n\n")

# ------------------------------------------------------------------------------
# Variables and scale factors
# ------------------------------------------------------------------------------

site_vars     <- c("tmmx", "tmmn", "pr", "def", "pet", "aet")
scale_factors <- vapply(tc_config$variables, function(v) v$scale, numeric(1))[site_vars]

cat(sprintf("Variables:     %s\n", paste(site_vars, collapse = ", ")))
cat(sprintf("Scale factors: %s\n\n",
            paste(sprintf("%s=%.1f", names(scale_factors), scale_factors), collapse = ", ")))

# ------------------------------------------------------------------------------
# Step 1: Read site locations and convert to sf
# ------------------------------------------------------------------------------

cat("Step 1: Loading site locations...\n")
sites <- read.csv(here("05_fia/data/processed/site_climate/all_site_locations.csv"),
                  stringsAsFactors = FALSE)
cat(sprintf("  %d sites loaded from all_site_locations.csv\n", nrow(sites)))

sites_sf <- st_as_sf(
  sites,
  coords = c("longitude", "latitude"),
  crs    = "+proj=longlat +datum=WGS84 +no_defs",  # proj4: avoids PROJ db lookup (conda conflict)
  remove = FALSE
)

# ------------------------------------------------------------------------------
# Step 2: Map each site to a TerraClimate pixel
# ------------------------------------------------------------------------------
#
# Multiple sites can fall in the same 4km cell and would get identical values,
# so we deduplicate to unique pixels before extracting. site_pixel_map.parquet
# records which pixel each site maps to so we can join results back in Step 5.
#
# ref_rast reconstructs the TerraClimate grid from stored pixel-center
# coordinates so cellFromXY() can assign a pixel_id to each site. The raster
# extent is defined as the cell centers ± half a cell width, which converts
# from center coordinates to the outer-edge coordinates that rast() expects.
# ------------------------------------------------------------------------------

cat("\nStep 2: Building pixel map...\n")

pixel_vals_dir <- here(tc_config$output_dir, "pixel_values")
ref_parquet    <- list.files(pixel_vals_dir, pattern = "\\.parquet$", full.names = TRUE)[1]

if (is.na(ref_parquet) || !file.exists(ref_parquet)) {
  stop(paste(
    "No pixel_values parquet found in", pixel_vals_dir,
    "\nRun 02_terraclimate/scripts/02_extract_terraclimate.R first."
  ))
}

# Load unique pixel center coordinates from any existing TerraClimate parquet
ref_grid <- read_parquet(ref_parquet, col_select = c("pixel_id", "x", "y")) %>%
  distinct(pixel_id, x, y)

res_deg  <- 1 / 24  # TerraClimate native resolution in degrees
ref_rast <- rast(
  xmin       = min(ref_grid$x) - res_deg / 2,
  xmax       = max(ref_grid$x) + res_deg / 2,
  ymin       = min(ref_grid$y) - res_deg / 2,
  ymax       = max(ref_grid$y) + res_deg / 2,
  resolution = res_deg,
  crs        = "+proj=longlat +datum=WGS84 +no_defs"  # proj4: avoids PROJ db lookup (conda conflict)
)
values(ref_rast) <- NA_real_

pixel_map <- build_pixel_map(sites_sf, ref_rast, id_col = "site_id")

n_sites  <- nrow(sites)
n_pixels <- n_distinct(pixel_map$pixel_id)
cat(sprintf("  %d sites mapped to %d unique TerraClimate pixels\n", n_sites, n_pixels))
cat(sprintf("  (%.1f sites/pixel — nearby plots may share the same 4km cell)\n\n",
            n_sites / n_pixels))

write_parquet(as_tibble(pixel_map), file.path(out_dir, "site_pixel_map.parquet"),
              compression = "snappy")
cat("  Saved: site_pixel_map.parquet\n\n")

# ------------------------------------------------------------------------------
# Step 3: Initialize GEE
# ------------------------------------------------------------------------------

cat("Step 3: Initializing GEE...\n")
ee <- init_gee()
cat("  GEE initialized\n\n")

# ------------------------------------------------------------------------------
# Step 4: Extract TerraClimate via GEE
# ------------------------------------------------------------------------------
# For each year, GEE builds a stacked image of 6 variables × 12 months and
# samples it at the unique pixel centroids. Results are written as one parquet
# per year to _gee_annual/ so progress is preserved if the run is interrupted.
# ------------------------------------------------------------------------------

start_year <- 1958L
end_year   <- as.integer(if (!is.null(tc_config$end_year)) tc_config$end_year else format(Sys.Date(), "%Y"))
years      <- start_year:end_year

cat(sprintf("Step 4: Extracting %d variables x 12 months x %d years (%d-%d)\n",
            length(site_vars), length(years), start_year, end_year))
cat(sprintf("        at %d unique site pixels...\n\n", n_pixels))

# Use exact pixel-center coordinates from the reference grid for GEE sampling
pixel_coords <- pixel_map %>%
  distinct(pixel_id) %>%
  inner_join(ref_grid, by = "pixel_id")

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

cat("\n  GEE extraction complete.\n\n")

# ------------------------------------------------------------------------------
# Step 5: Consolidate annual parquets → long format with site IDs
# ------------------------------------------------------------------------------
# GEE extracted one parquet per year, but each file contains all 12 months.
# Here we stack them, join pixel_id back to site_id, and pivot from wide
# (one column per variable) to long (one row per site × year × month × variable).

cat("Step 5: Consolidating and joining site IDs...\n")

annual_files <- list.files(tmp_dir, pattern = "^sites_\\d{4}\\.parquet$", full.names = TRUE)
cat(sprintf("  Found %d annual parquet files\n", length(annual_files)))

pm_slim      <- pixel_map %>% select(site_id, pixel_id)
long_chunks  <- vector("list", length(annual_files))

for (i in seq_along(annual_files)) {
  long_chunks[[i]] <- read_parquet(annual_files[i]) %>%
    distinct(pixel_id, month, .keep_all = TRUE) %>%     # drop float near-duplicate pixels
    inner_join(pm_slim, by = "pixel_id", relationship = "many-to-many") %>%
    select(site_id, year, month, all_of(site_vars)) %>%
    pivot_longer(all_of(site_vars), names_to = "variable", values_to = "value")
}

site_climate <- bind_rows(long_chunks)
rm(long_chunks); gc(verbose = FALSE)

# Add water year (Oct-Sep: month >= 10 rolls into the next water year)
wy <- calendar_to_water_year(site_climate$year, site_climate$month)
site_climate$water_year       <- wy$water_year
site_climate$water_year_month <- wy$water_year_month

site_climate <- site_climate %>%
  select(site_id, year, month, water_year, water_year_month, variable, value)

cat(sprintf("  %s rows x %d columns\n",
            format(nrow(site_climate), big.mark = ","), ncol(site_climate)))

# ------------------------------------------------------------------------------
# Step 6: Write output
# ------------------------------------------------------------------------------

cat("\nStep 6: Writing output...\n")
write_parquet(as_tibble(site_climate), out_file, compression = "snappy")
cat(sprintf("  Saved: %s (%s)\n\n", basename(out_file), file_size(out_file)))

cat("Site climate extraction complete.\n\n")
cat(sprintf("Output: %s\n\n", out_file))
cat("Read with:\n")
cat("  library(arrow); library(dplyr)\n")
cat("  clim <- read_parquet('05_fia/data/processed/site_climate/site_climate.parquet')\n")
cat("  # e.g. annual summer max temp per site:\n")
cat("  clim |> filter(variable == 'tmmx', month %in% 6:8) |>\n")
cat("    group_by(site_id, year) |> summarise(tmmx_jja = mean(value, na.rm=TRUE))\n")
