source(here::here("tests/testthat/helpers.R"))

suppressPackageStartupMessages({
  library(arrow)
  library(dplyr)
  library(data.table)
  library(tibble)
})

source(here::here("scripts/utils/fia_year_schema.R"))

# ---- Unit tests of the producer/reader year contract (no product dependency) --

test_that("cast_fia_year_fields promotes all-NA logical to integer and keeps years", {
  dt <- data.table(
    TRTYR1 = c(2001L, 2005L, NA),
    TRTYR3 = c(NA, NA, NA),            # logical all-NA, as fread would infer
    DSTRBYR1 = c(1999, 2010, NA)       # numeric
  )
  expect_true(is.logical(dt$TRTYR3))
  cast_fia_year_fields(dt)
  expect_true(is.integer(dt$TRTYR1))
  expect_true(is.integer(dt$TRTYR3))
  expect_true(is.integer(dt$DSTRBYR1))
  expect_equal(dt$TRTYR1, c(2001L, 2005L, NA_integer_))
  expect_true(all(is.na(dt$TRTYR3)))
  expect_equal(dt$DSTRBYR1, c(1999L, 2010L, NA_integer_))
})

# Build a synthetic two-state hive dataset: one Boolean (all-empty) partition and
# one integer partition holding real years. This reproduces the field condition
# and proves the fix is independent of which partition arrow reads first.
make_synthetic_cond <- function(bool_state_first) {
  root <- file.path(tempfile("cond_ds_"))
  dir.create(root)
  # Names chosen so the Boolean partition sorts first or last as requested.
  bool_state <- if (bool_state_first) "AA" else "ZZ"
  int_state  <- if (bool_state_first) "ZZ" else "AA"
  dir.create(file.path(root, paste0("state=", bool_state)))
  dir.create(file.path(root, paste0("state=", int_state)))
  # Boolean (all-empty) TRTYR3, integer everything else.
  write_parquet(
    tibble(PLT_CN = c(1, 2), INVYR = c(2010L, 2011L), TRTYR3 = c(NA, NA)),
    file.path(root, paste0("state=", bool_state), "cond.parquet")
  )
  # Integer partition with real years.
  write_parquet(
    tibble(PLT_CN = c(3, 4), INVYR = c(2012L, 2013L), TRTYR3 = c(2008L, 2015L)),
    file.path(root, paste0("state=", int_state), "cond.parquet")
  )
  root
}

test_that("open_cond_dataset forces integer years regardless of partition order", {
  for (bool_first in c(TRUE, FALSE)) {
    root <- make_synthetic_cond(bool_state_first = bool_first)
    ds <- open_cond_dataset(root, partitioning = "state")
    expect_true(fia_year_fields_are_integer(ds))
    d <- ds |> select(TRTYR3) |> collect()
    expect_true(is.integer(d$TRTYR3))
    # Real years survive as years -- NOT coerced to TRUE/1.
    expect_setequal(sort(d$TRTYR3[!is.na(d$TRTYR3)]), c(2008L, 2015L))
    unlink(root, recursive = TRUE)
  }
})

test_that("a plain open_dataset would corrupt years when the bool partition sorts first", {
  # Demonstrates the bug the fix defends against: naive open takes the first
  # fragment's schema. This is a guard that the *defensive reader* is required.
  root <- make_synthetic_cond(bool_state_first = TRUE)
  naive <- arrow::open_dataset(root, partitioning = "state")
  expect_false(fia_year_fields_are_integer(naive))  # first fragment is Boolean
  unlink(root, recursive = TRUE)
})

test_that("assert_fia_year_schema stops on a non-integer year schema", {
  root <- make_synthetic_cond(bool_state_first = TRUE)
  naive <- arrow::open_dataset(root, partitioning = "state")
  expect_error(assert_fia_year_schema(naive), "schema contract violated")
  unlink(root, recursive = TRUE)
})

# ---- Product-level regression assertions (run after canonical rebuild) --------

year_int_fields <- c("TRTYR1", "TRTYR2", "TRTYR3",
                     "DSTRBYR1", "DSTRBYR2", "DSTRBYR3")

test_that("every state cond partition uses nullable integer year fields", {
  cond_dir <- qa_path("05_fia/data/processed/cond")
  qa_require_dir(cond_dir)
  files <- list.files(cond_dir, pattern = "[.]parquet$", recursive = TRUE,
                      full.names = TRUE)
  if (length(files) == 0) skip("no cond partitions present")
  bad <- character(0)
  for (f in files) {
    sch <- arrow::open_dataset(f)$schema
    present <- intersect(year_int_fields, sch$names)
    for (nm in present) {
      if (sch$GetFieldByName(nm)$type$ToString() != "int32") {
        bad <- c(bad, paste0(basename(dirname(f)), ":", nm))
      }
    }
  }
  expect(length(bad) == 0, paste("Non-integer year fields:", paste(bad, collapse = ", ")))
})

test_that("national products keep TRTYR3 as real years, not Boolean coercion", {
  for (rel in c("05_fia/data/processed/summaries/plot_condition_metadata.parquet",
                "05_fia/data/processed/summaries/plot_disturbance_classification.parquet")) {
    p <- qa_path(rel)
    qa_require_file(p)
    sch <- arrow::open_dataset(p)$schema
    if (!"TRTYR3" %in% sch$names) next
    expect_equal(sch$GetFieldByName("TRTYR3")$type$ToString(), "int32",
                 info = paste(basename(p), "TRTYR3 type"))
    d <- open_dataset(p) |> select(TRTYR3) |> collect()
    non_na <- d$TRTYR3[!is.na(d$TRTYR3)]
    # If any TRTYR3 present, they must be plausible 4-digit years, never 0/1.
    if (length(non_na) > 0) {
      expect_true(all(non_na >= 1900 & non_na <= 2100),
                  info = paste(basename(p), "TRTYR3 values in year range"))
    }
  }
})

test_that("derived treatment/cutting timing has no Boolean-coercion artifacts", {
  p <- qa_path("05_fia/data/processed/summaries/plot_disturbance_classification.parquet")
  qa_require_file(p)
  d <- open_dataset(p) |>
    select(any_of(c("cutting_year_latest", "treatment_year_latest",
                    "time_since_cutting", "time_since_treatment"))) |>
    collect() |> as.data.table()
  for (col in intersect(c("cutting_year_latest", "treatment_year_latest"), names(d))) {
    vals <- d[[col]][!is.na(d[[col]])]
    if (length(vals) > 0) {
      # A TRUE->1 coercion would appear as a "year" of 1.
      expect_true(all(vals >= 1900 & vals <= 2100),
                  info = paste(col, "plausible years (no TRUE->1)"))
    }
  }
  for (col in intersect(c("time_since_cutting", "time_since_treatment"), names(d))) {
    vals <- d[[col]][!is.na(d[[col]])]
    if (length(vals) > 0) {
      expect_true(max(vals) < 200,
                  info = paste(col, "no absurd time-since from year=1"))
    }
  }
})
