source(here::here("tests/testthat/helpers.R"))

suppressPackageStartupMessages({
  library(sf)
  library(arrow)
  library(dplyr)
  library(DBI)
  library(RSQLite)
})

test_that("01_ids lookup tables exist and are non-empty", {
  lookup_files <- c(
    "01_ids/lookups/host_code_lookup.csv",
    "01_ids/lookups/dca_code_lookup.csv",
    "01_ids/lookups/damage_type_lookup.csv",
    "01_ids/lookups/percent_affected_lookup.csv",
    "01_ids/lookups/legacy_severity_lookup.csv",
    "01_ids/lookups/region_lookup.csv"
  )

  for (f in lookup_files) {
    path <- qa_require_file(qa_path(f), f)
    dat <- read.csv(path, stringsAsFactors = FALSE)
    expect(nrow(dat) > 0, paste("Expected non-empty lookup:", f))
  }
})

test_that("01_ids cleaned geopackage has expected layers and core fields", {
  gpkg <- qa_require_file(
    qa_path("01_ids/data/processed/ids_layers_cleaned.gpkg"),
    "01_ids/data/processed/ids_layers_cleaned.gpkg"
  )

  layer_names <- st_layers(gpkg)$name
  expect_setequal(layer_names, c("damage_areas", "damage_points", "surveyed_areas"))

  con <- dbConnect(RSQLite::SQLite(), gpkg)
  on.exit(dbDisconnect(con), add = TRUE)

  damage_fields <- dbListFields(con, "damage_areas")
  qa_expect_cols(
    as.data.frame(setNames(replicate(length(damage_fields), logical(0), simplify = FALSE), damage_fields)),
    c(
      "OBSERVATION_ID", "DAMAGE_AREA_ID", "SURVEY_YEAR", "REGION_ID",
      "DCA_CODE", "HOST_CODE", "ACRES", "OBSERVATION_COUNT", "SOURCE_FILE", "geom"
    )
  )
  damage_n <- dbGetQuery(con, "SELECT COUNT(*) AS n FROM damage_areas")$n[1]
  expect(damage_n > 0, "Expected non-empty damage_areas layer")

  points_fields <- dbListFields(con, "damage_points")
  qa_expect_cols(
    as.data.frame(setNames(replicate(length(points_fields), logical(0), simplify = FALSE), points_fields)),
    c(
      "OBSERVATION_ID", "SURVEY_YEAR", "REGION_ID",
      "DCA_CODE", "HOST_CODE", "ACRES", "OBSERVATION_COUNT", "SOURCE_FILE", "geom"
    )
  )
  points_n <- dbGetQuery(con, "SELECT COUNT(*) AS n FROM damage_points")$n[1]
  expect(points_n > 0, "Expected non-empty damage_points layer")

  surveyed_fields <- dbListFields(con, "surveyed_areas")
  qa_expect_cols(
    as.data.frame(setNames(replicate(length(surveyed_fields), logical(0), simplify = FALSE), surveyed_fields)),
    c("SURVEY_YEAR", "REGION_ID", "SOURCE_FILE", "SURVEY_FEATURE_ID", "geom")
  )
  surveyed_n <- dbGetQuery(con, "SELECT COUNT(*) AS n FROM surveyed_areas")$n[1]
  expect(surveyed_n > 0, "Expected non-empty surveyed_areas layer")
})

test_that("01_ids derived assignment and area metrics look sane", {
  assignment_path <- qa_require_file(
    qa_path("processed/ids/damage_area_to_surveyed_area.parquet"),
    "processed/ids/damage_area_to_surveyed_area.parquet"
  )
  metrics_path <- qa_require_file(
    qa_path("processed/ids/damage_area_area_metrics.parquet"),
    "processed/ids/damage_area_area_metrics.parquet"
  )

  assignment <- qa_read_parquet_head(assignment_path, n = 50000)
  qa_expect_cols(
    assignment,
    c("DAMAGE_AREA_ID", "SURVEYED_AREA_ID", "overlap_m2", "match_quality_flag")
  )
  expect_true(all(!is.na(assignment$DAMAGE_AREA_ID)))
  expect_true(all(assignment$overlap_m2 >= 0, na.rm = TRUE))
  expect_true(all(assignment$match_quality_flag %in% c("matched", "no_survey")))

  metrics <- qa_read_parquet_head(metrics_path, n = 50000)
  qa_expect_cols(
    metrics,
    c("DAMAGE_AREA_ID", "damage_area_m2", "SURVEYED_AREA_ID", "survey_area_m2", "damage_frac_of_survey")
  )
  expect_true(all(metrics$damage_area_m2 >= 0, na.rm = TRUE))
  expect_true(all(metrics$survey_area_m2 >= 0, na.rm = TRUE))
  expect_true(all(metrics$damage_frac_of_survey >= 0, na.rm = TRUE))
})

test_that("01_ids feature counts and year ranges are plausible", {
  gpkg <- qa_require_file(
    qa_path("01_ids/data/processed/ids_layers_cleaned.gpkg"),
    "01_ids/data/processed/ids_layers_cleaned.gpkg"
  )
  con <- DBI::dbConnect(RSQLite::SQLite(), gpkg)
  on.exit(DBI::dbDisconnect(con), add = TRUE)

  # Row count plausibility (IDS has ~4.5M damage areas, ~1.2M points, ~74k surveyed)
  da_n <- DBI::dbGetQuery(con, "SELECT COUNT(*) AS n FROM damage_areas")$n
  dp_n <- DBI::dbGetQuery(con, "SELECT COUNT(*) AS n FROM damage_points")$n
  sa_n <- DBI::dbGetQuery(con, "SELECT COUNT(*) AS n FROM surveyed_areas")$n
  expect(da_n > 1e6, sprintf("damage_areas row count suspiciously low: %d", da_n))
  expect(dp_n > 1e5, sprintf("damage_points row count suspiciously low: %d", dp_n))
  expect(sa_n > 1e4, sprintf("surveyed_areas row count suspiciously low: %d", sa_n))

  # SURVEY_YEAR range
  da_years <- DBI::dbGetQuery(
    con, "SELECT MIN(SURVEY_YEAR) AS mn, MAX(SURVEY_YEAR) AS mx FROM damage_areas"
  )
  expect(da_years$mn >= 1997,
         sprintf("damage_areas SURVEY_YEAR min %d < 1997", da_years$mn))
  expect(da_years$mx <= as.integer(format(Sys.Date(), "%Y")),
         sprintf("damage_areas SURVEY_YEAR max %d is in the future", da_years$mx))

  # DCA_CODE coverage: all codes in data must appear in lookup
  dca_lookup <- read.csv(qa_path("01_ids/lookups/dca_code_lookup.csv"),
                         stringsAsFactors = FALSE)
  da_codes <- DBI::dbGetQuery(
    con, "SELECT DISTINCT DCA_CODE FROM damage_areas WHERE DCA_CODE IS NOT NULL"
  )$DCA_CODE
  unknown <- setdiff(as.character(da_codes), as.character(dca_lookup$CODE))
  expect(length(unknown) == 0,
         sprintf("%d DCA_CODE(s) in damage_areas not in lookup: %s",
                 length(unknown), paste(head(unknown, 5), collapse = ", ")))
})
