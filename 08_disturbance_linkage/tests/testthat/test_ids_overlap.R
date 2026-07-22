library(testthat)
library(data.table)
library(sf)
library(here)

source(here("scripts/utils/ids_overlap.R"))

test_that("IDS overlap uses clipped union area and retains DCA grain", {
  footprint <- st_sf(
    stable_plot_id = "p1",
    geometry = st_sfc(st_polygon(list(rbind(
      c(0, 0), c(100, 0), c(100, 100), c(0, 100), c(0, 0)
    ))), crs = 5070)
  )
  damage <- st_sf(
    survey_year = c(2020L, 2020L, 2020L),
    dca_code = c(11006L, 11006L, 12000L),
    source_polygon_acres = c(100, 200, 300),
    geometry = st_sfc(
      st_polygon(list(rbind(
        c(0, 0), c(60, 0), c(60, 100), c(0, 100), c(0, 0)
      ))),
      st_polygon(list(rbind(
        c(40, 0), c(100, 0), c(100, 100), c(40, 100), c(40, 0)
      ))),
      st_polygon(list(rbind(
        c(0, 0), c(50, 0), c(50, 100), c(0, 100), c(0, 0)
      ))),
      crs = 5070
    )
  )
  clipped <- suppressWarnings(st_intersection(footprint, damage))
  areas <- data.table(stable_plot_id = "p1", footprint_area_m2 = 10000)

  result <- summarize_ids_intersections(clipped, areas)

  expect_equal(nrow(result), 2L)
  expect_equal(result[dca_code == 11006L, overlap_area_m2], 10000)
  expect_equal(result[dca_code == 11006L, footprint_overlap_fraction], 1)
  expect_equal(result[dca_code == 11006L, n_source_polygons], 2L)
  expect_equal(result[dca_code == 11006L, source_polygon_acres_sum], 300)
  expect_equal(result[dca_code == 12000L, overlap_area_m2], 5000)
})

test_that("IDS overlap rejects duplicate footprint-area keys", {
  x <- st_sf(
    stable_plot_id = "p1",
    survey_year = 2020L,
    dca_code = 11006L,
    source_polygon_acres = 10,
    geometry = st_sfc(st_point(c(0, 0)), crs = 5070)
  )
  areas <- data.table(
    stable_plot_id = c("p1", "p1"),
    footprint_area_m2 = c(1, 1)
  )

  expect_error(
    summarize_ids_intersections(x, areas),
    "unique by stable_plot_id"
  )
})
