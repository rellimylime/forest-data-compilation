library(testthat)
library(sf)
library(dplyr)
library(here)

source(here("scripts/utils/ids_dedupe.R"))

# One square metre polygon, offset so distinct features can be built cheaply.
sq <- function(x = 0, y = 0) {
  st_polygon(list(rbind(
    c(x, y), c(x + 1, y), c(x + 1, y + 1), c(x, y + 1), c(x, y)
  )))
}

make_layer <- function(...) {
  rows <- list(...)
  st_sf(
    OBSERVATION_ID = vapply(rows, `[[`, character(1), "id"),
    DAMAGE_AREA_ID = vapply(rows, `[[`, character(1), "area"),
    SURVEY_YEAR    = vapply(rows, `[[`, numeric(1), "year"),
    ACRES          = vapply(rows, `[[`, numeric(1), "acres"),
    SOURCE_FILE    = vapply(rows, `[[`, character(1), "src"),
    geometry       = st_sfc(lapply(rows, function(r) sq(r$x, 0)), crs = 4326)
  )
}

row_spec <- function(id, area, year, acres, src, x = 0) {
  list(id = id, area = area, year = year, acres = acres, src = src, x = x)
}

qa_tmp <- function() file.path(tempdir(), paste0("qa_", as.integer(runif(1, 1, 1e9))))

test_that("copies of one observation from different archives collapse to one row", {
  layer <- make_layer(
    row_spec("obs1", "a1", 2019, 100, "CONUS_Region1_AllYears.gdb"),
    row_spec("obs1", "a1", 2019, 100, "CONUS_Region2_AllYears.gdb"),
    row_spec("obs2", "a2", 2019, 50,  "CONUS_Region1_AllYears.gdb", x = 10)
  )

  out <- dedupe_merged_layer(layer, c("OBSERVATION_ID", "DAMAGE_AREA_ID"),
                             "damage_areas", qa_tmp())

  expect_equal(nrow(out), 2)
  expect_false(any(duplicated(out$OBSERVATION_ID)))
  # Acreage must fall by exactly the redundant copy.
  expect_equal(sum(out$ACRES), 150)
})

test_that("the archives that supplied a collapsed row are all recorded", {
  layer <- make_layer(
    row_spec("obs1", "a1", 2019, 100, "CONUS_Region2_AllYears.gdb"),
    row_spec("obs1", "a1", 2019, 100, "CONUS_Region1_AllYears.gdb")
  )

  out <- dedupe_merged_layer(layer, c("OBSERVATION_ID", "DAMAGE_AREA_ID"),
                             "damage_areas", qa_tmp())

  expect_equal(nrow(out), 1)
  expect_equal(out$SOURCE_FILE,
               "CONUS_Region1_AllYears.gdb;CONUS_Region2_AllYears.gdb")
  expect_equal(out$N_SOURCE_COPIES, 2)
})

test_that("exact duplicates within a single archive are also removed", {
  layer <- make_layer(
    row_spec("obs1", "a1", 2019, 100, "HI_Region5_AllYears.gdb"),
    row_spec("obs1", "a1", 2019, 100, "HI_Region5_AllYears.gdb")
  )

  out <- dedupe_merged_layer(layer, c("OBSERVATION_ID", "DAMAGE_AREA_ID"),
                             "damage_areas", qa_tmp())

  expect_equal(nrow(out), 1)
})

test_that("rows sharing an id but differing on a field are kept and reported", {
  layer <- make_layer(
    row_spec("obs1", "a1", 2019, 100, "CONUS_Region1_AllYears.gdb"),
    row_spec("obs1", "a1", 2019, 250, "CONUS_Region2_AllYears.gdb")
  )
  qa <- qa_tmp()

  out <- dedupe_merged_layer(layer, c("OBSERVATION_ID", "DAMAGE_AREA_ID"),
                             "damage_areas", qa)

  # Different ACRES means these are not copies. Neither may be silently dropped.
  expect_equal(nrow(out), 2)
  conflict_file <- file.path(qa, "damage_areas_identifier_conflicts.csv")
  expect_true(file.exists(conflict_file))
  expect_equal(nrow(read.csv(conflict_file)), 2)
})

test_that("identical attributes in different places are kept", {
  # Same id and attributes but genuinely different geometry: not a redundant copy.
  layer <- make_layer(
    row_spec("obs1", "a1", 2019, 100, "CONUS_Region1_AllYears.gdb", x = 0),
    row_spec("obs1", "a1", 2019, 100, "CONUS_Region2_AllYears.gdb", x = 50)
  )

  out <- dedupe_merged_layer(layer, c("OBSERVATION_ID", "DAMAGE_AREA_ID"),
                             "damage_areas", qa_tmp())

  expect_equal(nrow(out), 2)
})

test_that("a layer with no redundancy is returned unchanged", {
  layer <- make_layer(
    row_spec("obs1", "a1", 2019, 100, "CONUS_Region1_AllYears.gdb", x = 0),
    row_spec("obs2", "a2", 2019, 50,  "CONUS_Region1_AllYears.gdb", x = 10)
  )

  out <- dedupe_merged_layer(layer, c("OBSERVATION_ID", "DAMAGE_AREA_ID"),
                             "damage_areas", qa_tmp())

  expect_equal(nrow(out), 2)
  expect_equal(sum(out$ACRES), 150)
})

test_that("a layer with no source identifier is left alone", {
  layer <- make_layer(
    row_spec("obs1", "a1", 2019, 100, "CONUS_Region1_AllYears.gdb"),
    row_spec("obs1", "a1", 2019, 100, "CONUS_Region2_AllYears.gdb")
  )

  out <- dedupe_merged_layer(layer, character(0), "surveyed_areas", qa_tmp())

  expect_equal(nrow(out), 2)
})
