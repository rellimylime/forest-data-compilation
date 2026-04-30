# ==============================================================================
# 06a_build_fia_site_list.R
# Build the FIA plot site list consumed by 06_extract_site_climate.R.
#
# Reads the FIA cond extract, takes one row per distinct stable plot location,
# and writes 05_fia/data/processed/site_climate/all_site_locations.csv with
# columns: site_id, latitude, longitude, source.
#
# site_id is set to stable_plot_id so that downstream products
# (trees/seedlings/cond) can join to the climate output without an intermediate
# coordinate lookup.
#
# Usage:
#   Rscript 05_fia/scripts/06a_build_fia_site_list.R
#
# Note: This will overwrite all_site_locations.csv. The previous file is
# recoverable from git history. After regenerating, clear stale GEE checkpoints
# at 05_fia/data/processed/site_climate/_gee_annual/ before re-running
# 06_extract_site_climate.R, since pixel ids depend on the input site set.
# ==============================================================================

library(arrow)
library(dplyr)
library(here)

source(here("scripts/utils/load_config.R"))
config <- load_config()

cond_dir <- here(config$processed$fia$cond$output_dir)
out_file <- here("05_fia/data/processed/site_climate/all_site_locations.csv")

cond_plots <- open_dataset(cond_dir, partitioning = "state") |>
  select(stable_plot_id, STATECD, LAT, LON) |>
  distinct() |>
  collect()

cat(sprintf(
  "cond plot-location rows (pre-filter): %s\n",
  format(nrow(cond_plots), big.mark = ",")
))

cond_plots <- cond_plots |>
  filter(!is.na(LAT), !is.na(LON), LAT != 0, LON != 0)

# A small fraction of stable_plot_ids may carry slightly different LAT/LON
# across visits because FIA fuzzes coordinates. Pick one representative
# coordinate per stable plot so the CSV has exactly one row per plot location.
sites <- cond_plots |>
  arrange(stable_plot_id) |>
  group_by(stable_plot_id) |>
  summarise(
    latitude  = first(LAT),
    longitude = first(LON),
    .groups   = "drop"
  ) |>
  transmute(
    site_id   = stable_plot_id,
    latitude,
    longitude,
    source    = "FIA"
  )

cat(sprintf(
  "Distinct FIA plot locations:          %s\n",
  format(nrow(sites), big.mark = ",")
))

dir.create(dirname(out_file), recursive = TRUE, showWarnings = FALSE)
write.csv(sites, out_file, row.names = FALSE)

cat(sprintf(
  "Saved: %s (%s rows)\n",
  out_file, format(nrow(sites), big.mark = ",")
))
