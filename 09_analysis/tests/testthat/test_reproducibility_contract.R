library(testthat)

window_path <- here::here(
  "09_analysis", "config", "analysis_window.csv"
)
window <- read.csv(window_path, stringsAsFactors = FALSE)

read_sql <- function(name) {
  paste(readLines(here::here("09_analysis", "scripts", name)), collapse = "\n")
}

sql <- list(
  histories = read_sql("03_select_complete_condition_histories.sql"),
  site_cwd = read_sql("06_add_cumulative_site_cwd.sql"),
  pooled = read_sql("07_build_pooled_community_cwm.sql")
)

test_that("one tracked row fixes the site-CWD source and window", {
  expect_equal(nrow(window), 1L)
  expect_identical(window$climate_variable, "def")
  expect_identical(window$climate_start_year, 1997L)
  expect_identical(window$climate_end_year, 2025L)
  expect_identical(window$last_eligible_history_month, "2025-12-01")
  expect_identical(window$climate_backend, "local-ncss")
  expect_identical(
    window$climate_source_id,
    paste0(
      "https://tds-proxy.nkn.uidaho.edu/thredds/ncss/grid/",
      "TERRACLIMATE_ALL/data/TerraClimate_def_{year}.nc"
    )
  )
})

test_that("persisted SQL reductions use one thread and explicit order", {
  expect_true(all(vapply(
    sql,
    grepl,
    logical(1),
    pattern = "SET threads = 1;",
    fixed = TRUE
  )))
  expect_match(sql$histories, "sum\\(interval_years ORDER BY")
  expect_match(sql$site_cwd, "SUM\\(c\\.site_CWD_mm ORDER BY")
  expect_match(sql$pooled, "sum\\(j\\.abundance_adjusted ORDER BY")
  expect_false(grepl("any_value", sql$pooled, fixed = TRUE))
})
