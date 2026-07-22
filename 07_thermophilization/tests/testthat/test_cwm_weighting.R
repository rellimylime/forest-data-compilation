source(here::here("tests/testthat/helpers.R"))

suppressPackageStartupMessages({
  library(arrow)
  library(dplyr)
  library(data.table)
})

# Reference implementations of the two competing estimands.
pooled_cwm <- function(trait, weight) {          # correct: weight once
  ok <- !is.na(trait) & !is.na(weight) & weight > 0
  if (!any(ok)) return(NA_real_)
  sum(trait[ok] * weight[ok]) / sum(weight[ok])
}
squared_condprop_cwm <- function(trait, weight, condprop) {  # the former bug
  w <- weight * condprop
  pooled_cwm(trait, w)
}

# ---- Unit tests: the corrected estimand and how it differs from the bug -------

test_that("pooled CWM equals the direct abundance-weighted trait mean", {
  trait  <- c(10, 20, 30)
  weight <- c(1, 3, 6)   # per-acre TPA/BA contributions (already area-weighted)
  expect_equal(pooled_cwm(trait, weight),
               sum(trait * weight) / sum(weight))
})

test_that("re-multiplying by CONDPROP changes multi-condition results (the bug)", {
  # Species A in a large condition (condprop 0.8), species B in a small one (0.2).
  trait    <- c(5, 25)
  weight   <- c(48, 12)      # summed TPA already = density x condprop
  condprop <- c(0.8, 0.2)
  pooled  <- pooled_cwm(trait, weight)
  squared <- squared_condprop_cwm(trait, weight, condprop)
  expect_false(isTRUE(all.equal(pooled, squared)))
  # Squared weighting over-weights the large condition (pulls toward trait 5).
  expect_lt(squared, pooled)
})

test_that("single-condition plots are identical under both estimands", {
  trait    <- c(8, 16, 4)
  weight   <- c(2, 5, 1)
  condprop <- rep(1, 3)      # one condition -> condprop 1 everywhere
  expect_equal(pooled_cwm(trait, weight),
               squared_condprop_cwm(trait, weight, condprop))
})

# ---- Product-level regression assertions -------------------------------------

cwm_products <- c(
  seedlings = "07_thermophilization/data/processed/plot_year_community_cwm_seedlings.parquet",
  saplings  = "07_thermophilization/data/processed/plot_year_community_cwm_saplings.parquet",
  trees     = "07_thermophilization/data/processed/plot_year_community_cwm_trees.parquet"
)

test_that("CWM products expose explicit weighting-basis metadata, not the old flag", {
  for (layer in names(cwm_products)) {
    p <- qa_path(cwm_products[[layer]])
    qa_require_file(p)
    sch <- arrow::open_dataset(p)$schema
    qa_expect_cols(
      as.data.frame(matrix(ncol = length(sch$names), nrow = 0,
                           dimnames = list(NULL, sch$names))),
      c("weighting_basis", "condition_area_weighting", "conditions_pooled",
        "community_grain", "life_stage")
    )
    expect_false("condition_prop_weighted" %in% sch$names,
                 info = paste(layer, "no ambiguous condition_prop_weighted column"))
    d <- open_dataset(p) |>
      select(weighting_basis, condition_area_weighting) |>
      head(1) |> collect()
    expect_true(d$weighting_basis[1] %in%
                  c("pooled_basal_area", "pooled_stem_abundance"))
    expect_equal(d$condition_area_weighting[1], "none")
  }
})

test_that("trees primary CWM uses basal area; seedlings/saplings use stem TPA", {
  bases <- c(seedlings = "pooled_stem_abundance",
             saplings = "pooled_stem_abundance",
             trees = "pooled_basal_area")
  for (layer in names(cwm_products)) {
    p <- qa_path(cwm_products[[layer]])
    qa_require_file(p)
    b <- open_dataset(p) |> select(weighting_basis) |> head(1) |> collect()
    expect_equal(b$weighting_basis[1], bases[[layer]], info = layer)
  }
})

test_that("seedling plot CWM matches a pooled (single-count) recompute, incl. multi-condition", {
  p_cwm <- qa_path(cwm_products[["seedlings"]])
  p_seed <- qa_path("05_fia/data/processed/summaries/plot_seedling_species.parquet")
  niche_path <- qa_path("06_species_niches/data/processed/species_climate_niches_us_study_area.parquet")
  qa_require_file(p_cwm); qa_require_file(p_seed); qa_require_file(niche_path)

  niche <- as.data.table(read_parquet(niche_path))
  niche <- niche[source_code_system == "fia_spcd", .(species_key, tmean_annual_mean)]

  ss <- as.data.table(read_parquet(
    p_seed, col_select = c("PLT_CN", "INVYR", "CONDID", "SPCD", "seedlings_tpa")))
  ss[, species_key := paste0("fia_spcd:", as.integer(SPCD))]
  ss <- merge(ss, niche, by = "species_key", all.x = TRUE)
  ss <- ss[!is.na(seedlings_tpa) & seedlings_tpa > 0]

  # Restrict to plot-visits where EVERY seedling species has a study-area niche,
  # so the pipeline's global fallback is irrelevant and the recompute is exact.
  ss[, all_have_niche := all(!is.na(tmean_annual_mean)), by = .(PLT_CN, INVYR)]
  ss[, ncond := uniqueN(CONDID), by = .(PLT_CN, INVYR)]
  keep <- ss[all_have_niche == TRUE]

  pooled <- keep[, .(
    ncond = ncond[1],
    cwm_pooled = sum(tmean_annual_mean * seedlings_tpa) / sum(seedlings_tpa)
  ), by = .(PLT_CN, INVYR)]

  od <- as.data.table(read_parquet(
    p_cwm, col_select = c("PLT_CN", "INVYR", "cwm_temp")))
  m <- merge(pooled, od, by = c("PLT_CN", "INVYR"))
  if (nrow(m) == 0) skip("no comparable seedling plot-visits")

  # Overall agreement with the pooled recompute.
  expect_lt(max(abs(m$cwm_temp - m$cwm_pooled), na.rm = TRUE), 1e-6)
  # And specifically on multi-condition plots (where the double-count would show).
  mm <- m[ncond > 1]
  if (nrow(mm) > 0) {
    expect_lt(max(abs(mm$cwm_temp - mm$cwm_pooled), na.rm = TRUE), 1e-6)
  }
})
