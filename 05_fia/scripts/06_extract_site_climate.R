# ==============================================================================
# 06_extract_site_climate.R
# Extract TerraClimate monthly data for FIA site locations (1958-present)
#
# Reads all_site_locations.csv (lat/lon for each FIA site), maps each site to
# its containing TerraClimate pixel (~4km), then extracts monthly climate values
# via Google Earth Engine for the full TerraClimate record (1958-present).
#
# Variables extracted:
#   tmmx  - Maximum temperature (°C,  scale 0.1)
#   tmmn  - Minimum temperature (°C,  scale 0.1)
#   pr    - Precipitation (mm,         scale 1.0)
#   def   - Climate water deficit (mm, scale 0.1)  [= CWD = PET - AET]
#   pet   - Reference ET (mm,          scale 0.1)
#   aet   - Actual ET (mm,             scale 0.1)
#
# Output: 05_fia/data/processed/site_climate/fia_site_climate.parquet
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

source(here("scripts/utils/load_config.R"))
source(here("scripts/utils/climate_extract.R"))
source(here("scripts/utils/gee_utils.R"))
source(here("scripts/utils/time_utils.R"))

config    <- load_config()
tc_config <- config$raw$terraclimate

out_dir  <- here("05_fia/data/processed/site_climate")
out_file <- file.path(out_dir, "fia_site_climate.parquet")
dir_create(out_dir)

cat("FIA Site Climate Extraction (TerraClimate via GEE)\n")
cat("====================================================\n\n")

# ------------------------------------------------------------------------------
# Variables and scale factors (subset of full TerraClimate set)
# ------------------------------------------------------------------------------

site_vars <- c("tmmx", "tmmn", "pr", "def", "pet", "aet")
all_scale_factors <- vapply(tc_config$variables, function(v) v$scale, numeric(1))
scale_factors <- all_scale_factors[site_vars]

cat(sprintf("Variables:  %s\n", paste(site_vars, collapse = ", ")))
cat(sprintf("Scale factors: %s\n\n",
            paste(sprintf("%s=%.1f", names(scale_factors), scale_factors),
                  collapse = ", ")))

# ------------------------------------------------------------------------------
# Step 1: Read site locations and convert to sf
# ------------------------------------------------------------------------------

cat("Step 1: Loading site locations...\n")
sites <- read.csv(here("all_site_locations.csv"), stringsAsFactors = FALSE)
cat(sprintf("  %d sites loaded from all_site_locations.csv\n", nrow(sites)))

sites_sf <- st_as_sf(
  sites,
  coords  = c("longitude", "latitude"),
  crs     = "+proj=longlat +datum=WGS84 +no_defs",  # proj4: avoids PROJ db lookup (conda conflict)
  remove  = FALSE
)

# ------------------------------------------------------------------------------
# Step 2: Build pixel map (site_id -> TerraClimate pixel)
# ------------------------------------------------------------------------------

cat("\nStep 2: Building pixel map...\n")

# Load reference raster from an existing pixel_values parquet.
# We reconstruct the TerraClimate grid from stored x/y coordinates.
pixel_vals_dir <- here(tc_config$output_dir, "pixel_values")
ref_parquet    <- list.files(pixel_vals_dir, pattern = "\\.parquet$",
                             full.names = TRUE)[1]

if (is.na(ref_parquet) || !file.exists(ref_parquet)) {
  stop(paste(
    "No pixel_values parquet found in", pixel_vals_dir,
    "\nRun 02_terraclimate/scripts/02_extract_terraclimate.R first to build",
    "the reference pixel grid."
  ))
}

# Build reference SpatRaster from existing pixel grid definition
ref_sample <- read_parquet(ref_parquet, col_select = c("pixel_id", "x", "y"))
ref_sample <- distinct(ref_sample, pixel_id, x, y)

# TerraClimate native resolution: 1/24 degree (~4444m)
res_deg <- 1 / 24

library(terra)
ref_rast <- rast(
  xmin = min(ref_sample$x) - res_deg / 2,
  xmax = max(ref_sample$x) + res_deg / 2,
  ymin = min(ref_sample$y) - res_deg / 2,
  ymax = max(ref_sample$y) + res_deg / 2,
  resolution = res_deg,
  crs = "+proj=longlat +datum=WGS84 +no_defs"  # proj4: avoids PROJ db lookup (conda conflict)
)
values(ref_rast) <- NA_real_

pixel_map <- build_pixel_map(sites_sf, ref_rast, id_col = "site_id")

n_sites   <- nrow(sites)
n_pixels  <- n_distinct(pixel_map$pixel_id)
cat(sprintf("  %d sites mapped to %d unique TerraClimate pixels\n",
            n_sites, n_pixels))
cat(sprintf("  (%.1f sites per pixel on average — coordinate fuzz collapses nearby plots)\n\n",
            n_sites / n_pixels))

# Save pixel map for reference
write_parquet(as_tibble(pixel_map),
              file.path(out_dir, "fia_site_pixel_map.parquet"),
              compression = "snappy")
cat("  Saved: fia_site_pixel_map.parquet\n\n")

# ------------------------------------------------------------------------------
# Step 3: Initialize GEE and extract TerraClimate (1958-present)
# ------------------------------------------------------------------------------

cat("Step 3: Initializing GEE...\n")
ee <- init_gee()
cat("  GEE initialized\n\n")

# Year range: 1958 = first TerraClimate year; end at current data config end
start_year <- 1958L
end_year   <- as.integer(if (!is.null(tc_config$end_year)) tc_config$end_year else format(Sys.Date(), "%Y"))
years      <- start_year:end_year

cat(sprintf("Step 4: Extracting %d variables x %d months x %d years (%d-%d)\n",
            length(site_vars), 12L, length(years), start_year, end_year))
cat(sprintf("        for %d unique site pixels...\n\n", n_pixels))

# Pixel coords for GEE (unique pixels from the site pixel map)
pixel_coords_sites <- pixel_map %>%
  distinct(pixel_id, x, y) %>%
  left_join(ref_sample %>% distinct(pixel_id, x, y), by = "pixel_id",
            suffix = c("", ".ref")) %>%
  # prefer coordinates from the reference sample (exact grid centers)
  mutate(
    x = coalesce(x.ref, x),
    y = coalesce(y.ref, y)
  ) %>%
  select(pixel_id, x, y)

tmp_dir <- file.path(out_dir, "_gee_annual")
dir_create(tmp_dir)

extract_climate_from_gee(
  pixel_coords  = pixel_coords_sites,
  gee_asset     = tc_config$gee_asset,
  variables     = site_vars,
  years         = years,
  ee            = ee,
  scale         = tc_config$gee_scale,
  batch_size    = 2500,
  output_dir    = tmp_dir,
  output_prefix = "fia_sites",
  scale_factors = scale_factors,
  monthly       = TRUE
)

cat("\n  GEE extraction complete.\n\n")

# ------------------------------------------------------------------------------
# Step 5: Consolidate annual parquets, join site IDs, pivot to long format
# ------------------------------------------------------------------------------

cat("Step 5: Consolidating and joining site IDs...\n")

annual_files <- list.files(tmp_dir, pattern = "^fia_sites_\\d{4}\\.parquet$",
                           full.names = TRUE)
cat(sprintf("  Found %d annual parquet files\n", length(annual_files)))

# Pixel map slim: just site_id <-> pixel_id (points have coverage_fraction = 1.0)
pm_slim <- pixel_map %>% select(site_id, pixel_id)

# Process in chunks to avoid loading all years at once (~few GB total)
long_chunks <- vector("list", length(annual_files))

for (i in seq_along(annual_files)) {
  yr_data <- read_parquet(annual_files[i])

  # Join site_id via pixel_id (one pixel can serve multiple sites)
  yr_long <- yr_data %>%
    distinct(pixel_id, month, .keep_all = TRUE) %>%   # guard: drop float near-dup pixels
    inner_join(pm_slim, by = "pixel_id", relationship = "many-to-many") %>%
    select(site_id, year, month, all_of(site_vars)) %>%
    pivot_longer(
      cols      = all_of(site_vars),
      names_to  = "variable",
      values_to = "value"
    )

  long_chunks[[i]] <- yr_long
}

site_climate <- bind_rows(long_chunks)
rm(long_chunks); gc(verbose = FALSE)

# Add water year
wy <- calendar_to_water_year(site_climate$year, site_climate$month)
site_climate$water_year       <- wy$water_year
site_climate$water_year_month <- wy$water_year_month

# Reorder columns
site_climate <- site_climate %>%
  select(site_id, year, month, water_year, water_year_month, variable, value)

cat(sprintf("  %s rows x %d columns\n",
            format(nrow(site_climate), big.mark = ","),
            ncol(site_climate)))

# ------------------------------------------------------------------------------
# Step 6: Write output
# ------------------------------------------------------------------------------

cat("\nStep 6: Writing output...\n")
write_parquet(as_tibble(site_climate), out_file, compression = "snappy")
cat(sprintf("  Saved: %s (%s)\n\n", basename(out_file), file_size(out_file)))

cat("FIA site climate extraction complete.\n\n")
cat(sprintf("Output: %s\n\n", out_file))
cat("Read with:\n")
cat("  library(arrow); library(dplyr)\n")
cat("  clim <- read_parquet('05_fia/data/processed/site_climate/fia_site_climate.parquet')\n")
cat("  # e.g. annual summer max temp per site:\n")
cat("  clim |> filter(variable == 'tmmx', month %in% 6:8) |>\n")
cat("    group_by(site_id, year) |> summarise(tmmx_jja = mean(value, na.rm=TRUE))\n")
