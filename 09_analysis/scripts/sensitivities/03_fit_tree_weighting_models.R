#!/usr/bin/env Rscript

# Fit adult-tree models with abundance- and basal-area-weighted CWM responses
# on identical complete-case histories. Run after 01_build_tree_weighting_cwm.R.

suppressPackageStartupMessages({
  library(arrow)
  library(data.table)
  library(fs)
  library(here)
  library(lmtest)
  library(sandwich)
})

source(here("scripts/utils/parquet_atomic.R"))

responses <- c("temperature", "precipitation", "CWD")
predictors <- c(
  "fire_cumulative_mortality_pct",
  "insect_cumulative_mortality_pct",
  "disease_cumulative_mortality_pct",
  "cumulative_site_CWD_mm",
  "full_survey_period_years"
)

build_history_model_data <- function(adult_model, basal_cwm) {
  if (anyDuplicated(adult_model$history_id)) {
    stop("Adult model input has duplicate history_id values")
  }
  basal_cwm <- copy(basal_cwm)
  basal_cwm[, PLT_CN := as.character(PLT_CN)]
  if (anyDuplicated(basal_cwm[, .(PLT_CN, CONDID)])) {
    stop("Basal-area CWM input has duplicate condition-visit keys")
  }
  fields <- c(
    "n_species", "community_weight_total",
    unlist(lapply(responses, function(x) {
      c(x, paste0(x, "_weight_coverage"))
    }))
  )
  first <- basal_cwm[, c("PLT_CN", "CONDID", fields), with = FALSE]
  last <- copy(first)
  setnames(first, c("PLT_CN", fields), c("first_PLT_CN", paste0("first_basal_area_", fields)))
  setnames(last, c("PLT_CN", fields), c("last_PLT_CN", paste0("last_basal_area_", fields)))
  out <- merge(
    copy(adult_model), first,
    by = c("first_PLT_CN", "CONDID"), all.x = TRUE, sort = FALSE
  )
  out <- merge(
    out, last,
    by = c("last_PLT_CN", "CONDID"), all.x = TRUE, sort = FALSE
  )
  for (response in responses) {
    old <- paste0("delta_", response)
    setnames(out, old, paste0(old, "_abundance"))
    out[, (paste0(old, "_basal_area")) :=
      get(paste0("last_basal_area_", response)) -
      get(paste0("first_basal_area_", response))]
  }
  setorder(out, history_id)
  out
}

fit_weighting_models <- function(model_data) {
  coef_parts <- list()
  fit_parts <- list()
  sample_parts <- list()
  part <- 0L
  for (response in responses) {
    outcomes <- paste0("delta_", response, "_", c("abundance", "basal_area"))
    needed <- c(outcomes, predictors, "stable_plot_id", "history_id")
    sample <- model_data[
      cumulative_site_CWD_complete %in% TRUE &
        complete.cases(model_data[, ..needed])
    ]
    if (!nrow(sample)) stop("No complete model rows for ", response)
    sample_parts[[response]] <- data.table(
      response,
      available_adult_histories = nrow(model_data),
      common_complete_histories = nrow(sample),
      excluded_histories = nrow(model_data) - nrow(sample),
      stable_plots = uniqueN(sample$stable_plot_id),
      positive_fire = sum(sample$fire_cumulative_mortality_pct > 0),
      positive_insect = sum(sample$insect_cumulative_mortality_pct > 0),
      positive_disease = sum(sample$disease_cumulative_mortality_pct > 0)
    )
    for (basis in c("abundance", "basal_area")) {
      part <- part + 1L
      outcome <- paste0("delta_", response, "_", basis)
      model <- lm(reformulate(predictors, response = outcome), data = sample)
      covariance <- sandwich::vcovCL(
        model, cluster = sample$stable_plot_id, type = "HC1"
      )
      test <- as.matrix(lmtest::coeftest(model, vcov. = covariance))
      critical <- qt(0.975, df = df.residual(model))
      model_id <- paste("trees", response, basis, sep = "__")
      coef_parts[[part]] <- data.table(
        model_id, group = "trees", response, weighting_basis = basis,
        term = rownames(test), estimate = test[, 1L],
        std_error = test[, 2L], statistic = test[, 3L],
        p_value = test[, 4L],
        conf_low = test[, 1L] - critical * test[, 2L],
        conf_high = test[, 1L] + critical * test[, 2L],
        n = nobs(model), stable_plots = uniqueN(sample$stable_plot_id),
        covariance = "HC1 clustered by stable_plot_id",
        sample_definition = "common complete cases for both weighting bases"
      )
      fit_parts[[part]] <- data.table(
        model_id, group = "trees", response, weighting_basis = basis,
        n = nobs(model), stable_plots = uniqueN(sample$stable_plot_id),
        r_squared = summary(model)$r.squared,
        adjusted_r_squared = summary(model)$adj.r.squared,
        residual_standard_error = summary(model)$sigma,
        covariance = "HC1 clustered by stable_plot_id",
        sample_definition = "common complete cases for both weighting bases"
      )
    }
  }
  coefficients <- rbindlist(coef_parts)
  fits <- rbindlist(fit_parts)
  samples <- rbindlist(sample_parts)
  abundance <- coefficients[weighting_basis == "abundance", .(
    response, term, estimate_abundance = estimate,
    std_error_abundance = std_error, p_value_abundance = p_value,
    conf_low_abundance = conf_low, conf_high_abundance = conf_high
  )]
  basal <- coefficients[weighting_basis == "basal_area", .(
    response, term, estimate_basal_area = estimate,
    std_error_basal_area = std_error, p_value_basal_area = p_value,
    conf_low_basal_area = conf_low, conf_high_basal_area = conf_high
  )]
  comparison <- merge(abundance, basal, by = c("response", "term"))
  comparison[, `:=`(
    estimate_basal_area_minus_abundance =
      estimate_basal_area - estimate_abundance,
    estimate_direction_agrees =
      sign(estimate_basal_area) == sign(estimate_abundance),
    abundance_p_below_0_05 = p_value_abundance < 0.05,
    basal_area_p_below_0_05 = p_value_basal_area < 0.05
  )]
  setorder(coefficients, response, weighting_basis, term)
  setorder(fits, response, weighting_basis)
  setorder(samples, response)
  setorder(comparison, response, term)
  list(coefficients = coefficients, fits = fits, samples = samples,
       comparison = comparison)
}

main <- function() {
  output_dir <- here("09_analysis/data/sensitivity/tree_weighting")
  dir_create(output_dir)
  adult_model <- as.data.table(read_parquet(
    here("09_analysis/data/processed/lifestage_model_data.parquet")
  ))[layer == "trees"]
  basal_cwm <- as.data.table(read_parquet(
    path(output_dir, "tree_condition_visit_cwm_basal_area.parquet")
  ))
  model_data <- build_history_model_data(adult_model, basal_cwm)
  results <- fit_weighting_models(model_data)
  write_parquet_atomic(
    model_data, path(output_dir, "tree_weighting_history_model_data.parquet")
  )
  fwrite(results$coefficients, path(output_dir, "tree_weighting_model_coefficients.csv"))
  fwrite(results$fits, path(output_dir, "tree_weighting_model_fit.csv"))
  fwrite(results$samples, path(output_dir, "tree_weighting_model_sample_flow.csv"))
  fwrite(results$comparison, path(output_dir, "tree_weighting_model_comparison.csv"))

  checks <- data.table(
    check = c("one_row_per_history", "six_models_fit",
              "paired_model_sample_sizes_match", "comparison_complete"),
    passed = c(
      !anyDuplicated(model_data$history_id),
      nrow(results$fits) == 6L,
      results$fits[, uniqueN(n), by = response][, all(V1 == 1L)],
      nrow(results$comparison) == 3L * (length(predictors) + 1L)
    )
  )
  qa_dir <- here("09_analysis/qa/outputs/tree_weighting")
  dir_create(qa_dir)
  fwrite(checks, path(qa_dir, "tree_weighting_model_checks.csv"))
  if (!all(checks$passed)) stop("Tree-weighting model validation failed")
  message("Fit paired adult-tree abundance and basal-area CWM models")
}

if (sys.nframe() == 0L) main()
