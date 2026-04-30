source(here::here("tests/testthat/helpers.R"))

suppressPackageStartupMessages({
  library(arrow)
  library(dplyr)
})

test_that("03_prism pixel maps exist with valid coverage fractions", {
  pixel_map_dir <- qa_require_dir(
    qa_path("03_prism/data/processed/pixel_maps"),
    "03_prism/data/processed/pixel_maps"
  )

  map_files <- c(
    damage_areas = file.path(pixel_map_dir, "damage_areas_pixel_map.parquet"),
    damage_points = file.path(pixel_map_dir, "damage_points_pixel_map.parquet"),
    surveyed_areas = file.path(pixel_map_dir, "surveyed_areas_pixel_map.parquet")
  )

  for (nm in names(map_files)) {
    pm_path <- qa_require_file(map_files[[nm]], basename(map_files[[nm]]))
    pm <- qa_read_parquet_head(pm_path, n = 50000)

    qa_expect_cols(pm, c("pixel_id", "x", "y", "coverage_fraction"))
    expect_true(all(!is.na(pm$pixel_id)))
    expect_true(all(!is.na(pm$x)))
    expect_true(all(!is.na(pm$y)))
    expect_true(all(pm$coverage_fraction > 0 & pm$coverage_fraction <= 1, na.rm = TRUE))

    if (nm == "damage_points") {
      expect_true(all(pm$coverage_fraction == 1.0))
    }
  }
})

test_that("03_prism yearly pixel values have expected schema and key ranges", {
  values_dir <- qa_require_dir(
    qa_path("03_prism/data/processed/pixel_values"),
    "03_prism/data/processed/pixel_values"
  )

  yearly_files <- qa_list_yearly_parquets(values_dir, "prism")
  expect_gt(length(yearly_files), 0)

  prism_vars <- names(qa_load_config()$raw$prism$variables)
  check_files <- unique(yearly_files[c(1, ceiling(length(yearly_files) / 2), length(yearly_files))])

  for (f in check_files) {
    year_expected <- qa_year_from_filename(f)
    dat <- qa_read_parquet_head(
      f,
      n = 100000,
      cols = c("pixel_id", "x", "y", "year", "month", prism_vars)
    )

    qa_expect_cols(dat, c("pixel_id", "x", "y", "year", "month", prism_vars))
    expect_true(all(dat$year == year_expected))
    expect_true(all(dat$month %in% 1:12))
    expect_true(all(!is.na(dat$pixel_id)))
  }
})

test_that("03_prism summaries are internally consistent", {
  sum_dir <- qa_require_dir(
    qa_path("processed/climate/prism/damage_areas_summaries"),
    "processed/climate/prism/damage_areas_summaries"
  )

  prism_vars <- names(qa_load_config()$raw$prism$variables)

  for (var in prism_vars) {
    f <- file.path(sum_dir, paste0(var, ".parquet"))
    f <- qa_require_file(f, basename(f))

    s <- qa_read_parquet_head(f, n = 50000)
    qa_expect_cols(
      s,
      c(
        "DAMAGE_AREA_ID", "calendar_year", "calendar_month",
        "water_year", "water_year_month", "variable", "weighted_mean",
        "value_min", "value_max", "n_pixels", "n_pixels_with_data",
        "sum_coverage_fraction"
      )
    )

    expect_true(all(s$variable == var))
    expect_true(all(s$calendar_month %in% 1:12))
    expect_true(all(s$water_year_month %in% 1:12))
    expect_true(all(s$n_pixels_with_data <= s$n_pixels, na.rm = TRUE))
    expect_true(all(s$sum_coverage_fraction > 0, na.rm = TRUE))

    wy <- qa_calendar_to_water_year(s$calendar_year, s$calendar_month)
    expect_equal(s$water_year, wy$water_year)
    expect_equal(s$water_year_month, wy$water_year_month)
  }
})

test_that("03_prism yearly pixel values have no year gaps", {
  values_dir <- qa_require_dir(
    qa_path("03_prism/data/processed/pixel_values"),
    "03_prism/data/processed/pixel_values"
  )
  cfg        <- qa_load_config()
  start_year <- as.integer(cfg$raw$prism$start_year)
  end_year   <- as.integer(cfg$raw$prism$end_year)
  qa_expect_year_coverage(values_dir, "prism", start_year:end_year)
})

test_that("03_prism pixel values are within physical bounds", {
  values_dir <- qa_require_dir(
    qa_path("03_prism/data/processed/pixel_values"),
    "03_prism/data/processed/pixel_values"
  )
  yearly_files <- qa_list_yearly_parquets(values_dir, "prism")
  f   <- yearly_files[ceiling(length(yearly_files) / 2)]
  dat <- qa_read_parquet_head(f, n = 50000)

  if ("tmean"  %in% names(dat)) qa_expect_value_range(dat, "tmean",   -60,  60)
  if ("tmax"   %in% names(dat)) qa_expect_value_range(dat, "tmax",    -60,  70)
  if ("tmin"   %in% names(dat)) qa_expect_value_range(dat, "tmin",    -70,  60)
  if ("ppt"    %in% names(dat)) qa_expect_value_range(dat, "ppt",       0, 3000)
  if ("vpdmax" %in% names(dat)) qa_expect_value_range(dat, "vpdmax",    0,   20)
  if ("vpdmin" %in% names(dat)) qa_expect_value_range(dat, "vpdmin",    0,   15)
})
