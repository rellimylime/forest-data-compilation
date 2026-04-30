source(here::here("tests/testthat/helpers.R"))

suppressPackageStartupMessages({
  library(arrow)
  library(dplyr)
})

test_that("02_terraclimate pixel maps exist with valid geometry-to-pixel attributes", {
  pixel_map_dir <- qa_require_dir(
    qa_path("02_terraclimate/data/processed/pixel_maps"),
    "02_terraclimate/data/processed/pixel_maps"
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

test_that("02_terraclimate yearly pixel values have expected schema and temporal keys", {
  values_dir <- qa_require_dir(
    qa_path("02_terraclimate/data/processed/pixel_values"),
    "02_terraclimate/data/processed/pixel_values"
  )

  yearly_files <- qa_list_yearly_parquets(values_dir, "terraclimate")
  expect_gt(length(yearly_files), 0)

  check_files <- unique(yearly_files[c(1, ceiling(length(yearly_files) / 2), length(yearly_files))])
  tc_vars <- names(qa_load_config()$raw$terraclimate$variables)

  for (f in check_files) {
    year_expected <- qa_year_from_filename(f)
    dat <- qa_read_parquet_head(
      f,
      n = 100000,
      cols = c("pixel_id", "x", "y", "year", "month", tc_vars)
    )

    qa_expect_cols(dat, c("pixel_id", "x", "y", "year", "month", tc_vars))
    expect_true(all(dat$year == year_expected))
    expect_true(all(dat$month %in% 1:12))
    expect_true(all(!is.na(dat$pixel_id)))
  }
})

test_that("02_terraclimate damage area summaries are internally consistent", {
  sum_dir <- qa_require_dir(
    qa_path("processed/climate/terraclimate/damage_areas_summaries"),
    "processed/climate/terraclimate/damage_areas_summaries"
  )

  tc_vars <- names(qa_load_config()$raw$terraclimate$variables)

  for (var in tc_vars) {
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

test_that("02_terraclimate yearly pixel values have no year gaps", {
  values_dir <- qa_require_dir(
    qa_path("02_terraclimate/data/processed/pixel_values"),
    "02_terraclimate/data/processed/pixel_values"
  )
  cfg        <- qa_load_config()
  start_year <- as.integer(cfg$raw$terraclimate$start_year)
  end_year   <- as.integer(cfg$raw$terraclimate$end_year)
  qa_expect_year_coverage(values_dir, "terraclimate", start_year:end_year)
})

test_that("02_terraclimate pixel values are within physical bounds", {
  values_dir <- qa_require_dir(
    qa_path("02_terraclimate/data/processed/pixel_values"),
    "02_terraclimate/data/processed/pixel_values"
  )
  yearly_files <- qa_list_yearly_parquets(values_dir, "terraclimate")
  f   <- yearly_files[ceiling(length(yearly_files) / 2)]  # middle year
  dat <- qa_read_parquet_head(f, n = 50000)

  if ("tmmx" %in% names(dat)) qa_expect_value_range(dat, "tmmx", -60,  70)
  if ("tmmn" %in% names(dat)) qa_expect_value_range(dat, "tmmn", -70,  60)
  if ("pr"   %in% names(dat)) qa_expect_value_range(dat, "pr",     0, 3000)
  if ("def"  %in% names(dat)) qa_expect_value_range(dat, "def",    0, 1000)
  if ("pet"  %in% names(dat)) qa_expect_value_range(dat, "pet",    0, 1000)
  if ("aet"  %in% names(dat)) qa_expect_value_range(dat, "aet",    0, 1000)
})
