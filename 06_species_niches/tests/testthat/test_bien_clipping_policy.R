source(here::here("tests/testthat/helpers.R"))
source(here::here("06_species_niches/qa/scripts/10_bien_clipping_supplement.R"))

test_that("clipping tolerance ignores harmless edge precision", {
  result <- classify_clipping(
    global_area_km2 = c(1e6, 1e6, 100),
    retained_area_km2 = c(1e6 - 0.5, 1e6 - 2, 0),
    absolute_tolerance_km2 = 1,
    fraction_tolerance = 1e-6
  )
  expect_false(result$meaningfully_clipped[[1]])
  expect_true(result$meaningfully_clipped[[2]])
  expect_true(result$wholly_outside_bbox[[3]])
})

test_that("sensitivity policy uses global only when clipping requires it", {
  scope <- choose_sensitivity_scope(
    has_global = c(TRUE, TRUE, TRUE, FALSE),
    has_study = c(TRUE, TRUE, FALSE, FALSE),
    wholly_outside_bbox = c(FALSE, FALSE, TRUE, FALSE),
    meaningfully_clipped = c(FALSE, TRUE, TRUE, FALSE)
  )
  expect_equal(scope, c("us_study_area", "global", "global", "none"))
})

test_that("coverage categories distinguish non-targets and failure stages", {
  category <- coverage_category(
    needs_niche = c(FALSE, TRUE, TRUE, TRUE, TRUE),
    bien_available = c(NA, FALSE, TRUE, TRUE, TRUE),
    has_global = c(FALSE, FALSE, FALSE, TRUE, TRUE),
    has_study = c(FALSE, FALSE, FALSE, FALSE, TRUE)
  )
  expect_equal(category, c(
    "not_targeted_non_species_level",
    "no_bien_range_returned",
    "bien_range_no_usable_global_niche",
    "study_area_niche_unavailable_global_fallback",
    "usable_study_area_niche"
  ))
})


test_that("policy merge count collisions retain both grains explicitly", {
  x <- data.table::data.table(
    n_plot_visits.x = 2L, n_conditions.x = 3L,
    n_plot_visits.y = 20L, n_conditions.y = 30L
  )
  result <- rename_policy_merge_counts(x)
  expect_equal(result$group_n_conditions, 3L)
  expect_equal(result$universe_n_conditions, 30L)
  expect_false(any(grepl("\\.[xy]$", names(result))))
})
