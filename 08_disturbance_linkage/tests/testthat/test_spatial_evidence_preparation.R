source(here::here("tests/testthat/helpers.R"))

suppressPackageStartupMessages({
  library(arrow)
  library(data.table)
  library(sf)
})

test_that("visit coordinate status applies exclusions at stable-plot grain", {
  path <- qa_path(
    paste0(
      "08_disturbance_linkage/data/processed/",
      "fia_visit_spatial_linkage_status.parquet"
    )
  )
  qa_require_file(path)
  status <- as.data.table(read_parquet(path))

  expect_equal(
    status[, .N, by = .(stable_plot_id, PLT_CN, INVYR)][N > 1L, .N],
    0L
  )
  expect_true(all(
    status[, uniqueN(eligible_spatial_linkage), by = stable_plot_id]$V1 == 1L
  ))
  expect_true(all(
    status[n_distinct_public_coordinates > 1L, !eligible_spatial_linkage]
  ))
  expect_true(all(
    status[
      n_distinct_public_coordinates > 1L,
      spatial_linkage_exclusion_reason
    ] == "multiple_public_coordinate_pairs"
  ))
  expect_true(all(
    status[
      n_distinct_public_coordinates == 1L,
      eligible_spatial_linkage
    ]
  ))
})

test_that("footprints contain exactly the eligible stable plots", {
  status_path <- qa_path(
    paste0(
      "08_disturbance_linkage/data/processed/",
      "fia_visit_spatial_linkage_status.parquet"
    )
  )
  footprint_path <- qa_path(
    "08_disturbance_linkage/data/processed/plot_footprints.gpkg"
  )
  qa_require_file(status_path)
  qa_require_file(footprint_path)

  eligible <- open_dataset(status_path) |>
    dplyr::filter(eligible_spatial_linkage) |>
    dplyr::select(stable_plot_id) |>
    dplyr::distinct() |>
    dplyr::collect()
  footprint_ids <- st_read(
    footprint_path,
    query = "SELECT stable_plot_id FROM plot_footprints",
    quiet = TRUE
  )$stable_plot_id
  expect_setequal(as.character(eligible$stable_plot_id), as.character(footprint_ids))
})

test_that("IDS detection and coverage partitions retain separate grains", {
  agent_path <- qa_path(
    paste0(
      "08_disturbance_linkage/data/processed/",
      "ids_annual_agent_evidence/survey_year=2020.parquet"
    )
  )
  coverage_path <- qa_path(
    paste0(
      "08_disturbance_linkage/data/processed/",
      "ids_annual_survey_coverage/survey_year=2020.parquet"
    )
  )
  qa_require_file(agent_path)
  qa_require_file(coverage_path)

  agents <- as.data.table(read_parquet(agent_path))
  coverage <- as.data.table(read_parquet(coverage_path))

  expect_equal(
    agents[, .N, by = .(stable_plot_id, survey_year, dca_code)][N > 1L, .N],
    0L
  )
  expect_equal(
    coverage[, .N, by = .(stable_plot_id, survey_year)][N > 1L, .N],
    0L
  )
  expect_false(any(is.na(agents$dca_code)))
  expect_true(all(agents$n_source_features >= 1L))
  expect_true(all(!is.na(agents$source_feature_ids)))
  expect_type(agents$ids_percent_mid_min, "double")
  expect_type(agents$ids_percent_mid_max, "double")
  expect_type(agents$ids_legacy_tpa_min, "double")
  expect_type(agents$ids_legacy_tpa_max, "double")
  expect_type(agents$ids_legacy_no_trees_sum, "double")
  expect_equal(uniqueN(agents$source_snapshot), 1L)
  expect_equal(uniqueN(coverage$source_snapshot), 1L)
  expect_true(all(coverage$coverage_relationship %in% c(
    "full_surveyed_area_intersection",
    "partial_surveyed_area_intersection",
    "no_surveyed_area_intersection",
    "coverage_unknown"
  )))
  expect_true(all(
    coverage$surveyed_overlap_fraction >= 0 &
      coverage$surveyed_overlap_fraction <= 1
  ))
})

test_that("missing MTBS source does not create a placeholder event product", {
  source_path <- qa_path(
    "08_disturbance_linkage/data/raw/mtbs/mtbs_fire_perimeters.gpkg"
  )
  output_path <- qa_path(
    "08_disturbance_linkage/data/processed/mtbs_fire_event_evidence.parquet"
  )
  if (!file.exists(source_path)) {
    expect_false(file.exists(output_path))
  } else {
    qa_require_file(output_path)
    events <- as.data.table(read_parquet(output_path))
    expect_equal(
      events[, .N, by = .(stable_plot_id, mtbs_event_id)][N > 1L, .N],
      0L
    )
    expect_true(all(!is.na(events$mtbs_map_id)))
    expect_true(all(!is.na(events$fire_ignition_date)))
    expect_true(all(events$search_buffer_overlap_fraction > 0))
    expect_true(all(events$search_buffer_overlap_fraction <= 1))
    expect_false(any(c(
      "search_buffer_m", "buffer_intersection", "spatial_relationship",
      "source_layer", "source_publication_date", "source_doi",
      "source_archive_sha256", "source_snapshot", "n_source_perimeters",
      "source_map_program"
    ) %in% names(events)))
  }
})

test_that("prepared MTBS source matches its validated national snapshot", {
  source_path <- qa_path(
    "08_disturbance_linkage/data/raw/mtbs/mtbs_fire_perimeters.gpkg"
  )
  validation_path <- qa_path(
    "08_disturbance_linkage/qa/outputs/mtbs_perimeter_source_validation.csv"
  )
  qa_require_file(source_path)
  qa_require_file(validation_path)

  validation <- fread(validation_path)
  expect_true(all(validation$passed))
  expect_equal(
    as.integer(validation[check == "feature_count", value]),
    30606L
  )
  expect_equal(
    as.integer(validation[check == "unique_fire_id", value]),
    30606L
  )
  expect_equal(
    as.integer(validation[check == "unique_map_id", value]),
    30606L
  )
  expect_equal(
    as.integer(validation[check == "minimum_year", value]),
    1984L
  )
  expect_equal(
    as.integer(validation[check == "maximum_year", value]),
    2026L
  )
})
