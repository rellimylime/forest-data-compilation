source(here::here("tests/testthat/helpers.R"))

suppressPackageStartupMessages({
  library(arrow)
  library(dplyr)
  library(terra)
})

test_that("05_fia lookup and partitioned extraction outputs exist", {
  lookup_species <- qa_require_file(
    qa_path("05_fia/lookups/ref_species.parquet"),
    "05_fia/lookups/ref_species.parquet"
  )
  lookup_fortyp <- qa_require_file(
    qa_path("05_fia/lookups/ref_forest_type.parquet"),
    "05_fia/lookups/ref_forest_type.parquet"
  )

  sp <- qa_read_parquet_head(lookup_species, n = 5000)
  ft <- qa_read_parquet_head(lookup_fortyp, n = 5000)
  qa_expect_cols(sp, c("SPCD", "COMMON_NAME", "SCIENTIFIC_NAME", "SFTWD_HRDWD", "WOODLAND"))
  qa_expect_cols(ft, c("VALUE", "MEANING"))

  partition_dirs <- c(
    "05_fia/data/processed/trees",
    "05_fia/data/processed/cond",
    "05_fia/data/processed/seedlings",
    "05_fia/data/processed/mortality"
  )

  for (d in partition_dirs) {
    qa_require_dir(qa_path(d), d)
    files <- list.files(qa_path(d), pattern = "\\.parquet$", full.names = TRUE, recursive = TRUE)
    expect(length(files) > 0, paste("Expected partition files in", d))
  }
})

test_that("05_fia national summary files exist with expected schema", {
  summaries_dir <- qa_require_dir(
    qa_path("05_fia/data/processed/summaries"),
    "05_fia/data/processed/summaries"
  )

  required_files <- list(
    plot_tree_metrics = c("PLT_CN", "INVYR", "state", "ba_live_total", "n_species_live", "shannon_h_ba"),
    plot_seedling_metrics = c("PLT_CN", "INVYR", "state", "treecount_total", "n_species_seedling"),
    plot_mortality_metrics = c("PLT_CN", "INVYR", "SPCD", "AGENTCD", "component_type", "tpamort_per_acre"),
    plot_cond_fortypcd = c("PLT_CN", "INVYR", "STATECD", "CONDID", "FORTYPCD"),
    plot_disturbance_history = c("PLT_CN", "INVYR", "DSTRBCD", "disturbance_label", "disturbance_category"),
    plot_damage_agents = c("PLT_CN", "INVYR", "CONDID", "SPCD", "DAMAGE_AGENT_CD", "ba_per_acre"),
    plot_treatment_history = c("PLT_CN", "INVYR", "TRTCD", "treatment_label", "treatment_category"),
    plot_exclusion_flags = c("PLT_CN", "INVYR", "pct_forested", "exclude_any", "has_fire", "has_insect")
  )

  for (nm in names(required_files)) {
    f <- qa_require_file(file.path(summaries_dir, paste0(nm, ".parquet")), paste0(nm, ".parquet"))
    dat <- qa_read_parquet_head(f, n = 100000)
    qa_expect_cols(dat, required_files[[nm]])
    expect(nrow(dat) > 0, paste("Expected non-empty summary:", nm))
  }

  tree <- qa_read_parquet_head(file.path(summaries_dir, "plot_tree_metrics.parquet"), n = 200000)
  flags <- qa_read_parquet_head(file.path(summaries_dir, "plot_exclusion_flags.parquet"), n = 200000)
  qa_expect_unique_key(tree, c("PLT_CN", "INVYR"), "plot_tree_metrics (PLT_CN, INVYR)")
  qa_expect_unique_key(flags, c("PLT_CN", "INVYR"), "plot_exclusion_flags (PLT_CN, INVYR)")
})

test_that("05_fia site pixel map preserves global TerraClimate pixel semantics", {
  sites_path <- qa_require_file(
    qa_path("05_fia/data/processed/site_climate/all_site_locations.csv"),
    "05_fia/data/processed/site_climate/all_site_locations.csv"
  )
  pixel_map_path <- qa_require_file(
    qa_path("05_fia/data/processed/site_climate/site_pixel_map.parquet"),
    "05_fia/data/processed/site_climate/site_pixel_map.parquet"
  )

  sites <- read.csv(sites_path, stringsAsFactors = FALSE)
  pixel_map <- read_parquet(pixel_map_path)

  qa_expect_cols(pixel_map, c("site_id", "pixel_id", "x", "y", "coverage_fraction"))
  expect_equal(nrow(pixel_map), nrow(sites))
  expect_equal(sum(is.na(pixel_map$pixel_id)), 0)
  qa_expect_unique_key(pixel_map, c("site_id"), "site_pixel_map site_id")
  expect_true(all(pixel_map$coverage_fraction == 1.0))

  merged <- sites |> inner_join(pixel_map, by = "site_id")
  expect_equal(nrow(merged), nrow(sites))

  res_deg <- 1 / 24
  tc_global <- rast(
    xmin = -180, xmax = 180, ymin = -90, ymax = 90,
    resolution = res_deg,
    crs = "+proj=longlat +datum=WGS84 +no_defs"
  )

  expected_cells <- cellFromXY(tc_global, as.matrix(merged[, c("longitude", "latitude")]))
  expect_equal(as.integer(merged$pixel_id), as.integer(expected_cells))

  expected_xy <- xyFromCell(tc_global, expected_cells)
  expect_equal(merged$x, expected_xy[, 1], tolerance = 1e-9)
  expect_equal(merged$y, expected_xy[, 2], tolerance = 1e-9)

  expect_true(all(abs(merged$longitude - merged$x) <= (res_deg / 2 + 1e-12)))
  expect_true(all(abs(merged$latitude - merged$y) <= (res_deg / 2 + 1e-12)))
})

test_that("05_fia site climate extraction has expected keys and time semantics", {
  site_climate_path <- qa_require_file(
    qa_path("05_fia/data/processed/site_climate/site_climate.parquet"),
    "05_fia/data/processed/site_climate/site_climate.parquet"
  )

  vars_expected <- c("tmmx", "tmmn", "pr", "def", "pet", "aet")

  dat <- qa_read_parquet_head(site_climate_path, n = 200000)
  qa_expect_cols(dat, c("site_id", "year", "month", "water_year", "water_year_month", "variable", "value"))

  expect_true(all(dat$month %in% 1:12))
  expect_true(all(dat$water_year_month %in% 1:12))
  expect_true(all(dat$year >= 1958))
  expect_true(all(dat$variable %in% vars_expected))

  key_dups <- dat |> count(site_id, year, month, variable) |> filter(n > 1)
  expect_equal(nrow(key_dups), 0)

  wy <- qa_calendar_to_water_year(dat$year, dat$month)
  expect_equal(dat$water_year, wy$water_year)
  expect_equal(dat$water_year_month, wy$water_year_month)
})

test_that("05_fia site climate is complete and values are within physical bounds", {
  site_climate_path <- qa_require_file(
    qa_path("05_fia/data/processed/site_climate/site_climate.parquet"),
    "05_fia/data/processed/site_climate/site_climate.parquet"
  )

  dat <- read_parquet(site_climate_path)

  # Every site that has any data must have all 12 months x all years x 6 vars
  n_sites <- n_distinct(dat$site_id)
  n_years <- n_distinct(dat$year)
  n_vars  <- n_distinct(dat$variable)
  expected_rows <- n_sites * n_years * 12L * n_vars
  expect_equal(nrow(dat), expected_rows,
               label = sprintf(
                 "site_climate row count: expected %d, got %d",
                 expected_rows, nrow(dat)
               ))

  # Physical value ranges (subset for speed)
  check <- dat |> filter(site_id %in% head(unique(site_id), 200))
  tmmx <- check$value[check$variable == "tmmx"]
  tmmn <- check$value[check$variable == "tmmn"]
  pr   <- check$value[check$variable == "pr"]

  qa_expect_value_range(data.frame(tmmx = tmmx), "tmmx", -60,  70)
  qa_expect_value_range(data.frame(tmmn = tmmn), "tmmn", -70,  60)
  qa_expect_value_range(data.frame(pr   = pr),   "pr",     0, 3000)
})
