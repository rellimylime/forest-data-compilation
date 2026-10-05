testthat::test_that("tree weighting comparison preserves explicit direction", {
  source(here::here(
    "09_analysis/scripts/sensitivities/01_build_tree_weighting_cwm.R"
  ), local = TRUE)
  abundance <- data.table::data.table(
    PLT_CN = c(1, 2), INVYR = c(2000L, 2010L), CONDID = 1L,
    temperature = c(10, 12)
  )
  basal <- data.table::data.table(
    PLT_CN = c(1, 2), INVYR = c(2000L, 2010L), CONDID = 1L,
    temperature = c(11, 10)
  )
  got <- compare_products(
    abundance, basal, c("PLT_CN", "INVYR", "CONDID"), "temperature"
  )
  testthat::expect_equal(
    got$temperature_basal_area_minus_abundance, c(1, -2)
  )
  testthat::expect_equal(nrow(got), 2L)
})

testthat::test_that("canonical abundance export labels FIA stem density", {
  source(here::here(
    "09_analysis/scripts/sensitivities/01_build_tree_weighting_cwm.R"
  ), local = TRUE)
  x <- data.table::data.table(
    PLT_CN = 1, INVYR = 2000L, CONDID = 1L,
    total_individual_abundance = 20,
    temperature_niche_abundance = 15,
    precipitation_niche_abundance = 10,
    CWD_niche_abundance = 20,
    temperature = 10, precipitation = 1000, CWD = 500
  )
  got <- canonical_abundance_product(x)
  testthat::expect_equal(got$weight_column, "n_trees_tpa")
  testthat::expect_equal(got$weighting_basis, "individual_abundance")
  testthat::expect_equal(got$temperature_weight_coverage, 0.75)
  testthat::expect_equal(got$temperature_weighted_sum, 150)
})

testthat::test_that("understory grouping is explicit", {
  source(here::here(
    "09_analysis/scripts/sensitivities/02_build_understory_cwm_diagnostic.R"
  ), local = TRUE)
  got <- p2_group(c("shrub", "forb/herb", "graminoid", "large tree", NA))
  testthat::expect_equal(got, c(
    "understory_shrubs", "understory_forbs", "understory_graminoids",
    "understory_tree_layers", "understory_other"
  ))
})

testthat::test_that("understory unavailable cases never masquerade as zero", {
  source(here::here(
    "09_analysis/scripts/sensitivities/02_build_understory_cwm_diagnostic.R"
  ), local = TRUE)
  got <- cwm_status(
    n_structure_subplots = c(NA, 4, 4, 4, 4, 4),
    structure_cover = c(NA, 0, 10, 10, 10, 10),
    n_species_rows = c(NA, NA, 0, 2, 2, 2),
    total_cover = c(NA, NA, NA, 5, 5, 5),
    species_level_cover = c(NA, NA, NA, 0, 4, 4),
    niche_cover = c(NA, NA, NA, 0, 0, 3)
  )
  testthat::expect_equal(got, c(
    "NU_NO_P2VEG_STRUCTURE_SURVEY",
    "NU_NO_RECORDED_GROUP_COVER",
    "NU_STRUCTURE_COVER_WITHOUT_SPECIES_RECORDS",
    "NU_NO_SPECIES_LEVEL_COVER",
    "NU_NO_USABLE_NICHE_COVER",
    "CWM_AVAILABLE"
  ))
})
