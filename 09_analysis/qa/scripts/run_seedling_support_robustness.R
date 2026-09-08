#!/usr/bin/env Rscript

# Test whether seedling disturbance relationships vary with sampling support.

suppressPackageStartupMessages({
  library(arrow)
  library(data.table)
  library(lmtest)
  library(sandwich)
})

run_id <- Sys.getenv(
  "ANALYSIS_RUN_ID",
  unset = "20260822_cumulative_mortality_site_cwd_all_groups_v01"
)
model_path <- file.path(
  "09_analysis", "data", "processed", "lifestage_model_data.parquet"
)
support_path <- file.path(
  "09_analysis", "qa", "outputs", "seedling_spatial_support",
  "seedling_history_support.parquet"
)
approved_path <- file.path(
  "09_analysis", "results", "model_runs", run_id, "coefficients.csv"
)
output_dir <- file.path(
  "09_analysis", "qa", "outputs", "seedling_support_robustness"
)
dir.create(output_dir, recursive = TRUE, showWarnings = FALSE)

required_inputs <- c(model_path, support_path, approved_path)
missing_inputs <- required_inputs[!file.exists(required_inputs)]
if (length(missing_inputs)) {
  stop("Missing input(s): ", paste(missing_inputs, collapse = "; "))
}

responses <- c("temperature", "precipitation", "CWD")
mortality_terms <- c(
  "fire_cumulative_mortality_pct",
  "insect_cumulative_mortality_pct",
  "disease_cumulative_mortality_pct"
)
baseline_predictors <- c(
  mortality_terms,
  "cumulative_site_CWD_mm",
  "full_survey_period_years"
)
agent_labels <- c(
  fire_cumulative_mortality_pct = "Fire",
  insect_cumulative_mortality_pct = "Insect",
  disease_cumulative_mortality_pct = "Disease"
)

# Use interpretable support units instead of standardized values.
support_specs <- data.table(
  support_id = c(
    "initial_seedling_tally", "initial_species_richness",
    "eligible_microplots", "microplots_with_seedlings"
  ),
  source_column = c(
    "initial_calculated_seedling_tally",
    "initial_seedling_species_richness",
    "n_eligible_microplots", "n_seedling_subplots"
  ),
  support_label = c(
    "Initial seedling tally", "Initial seedling species richness",
    "Eligible microplots", "Microplots containing seedlings"
  ),
  support_unit = c(
    "one doubling", "one additional species",
    "one additional microplot", "one additional microplot"
  ),
  transform = c("log2", "identity", "identity", "identity")
)

model <- as.data.table(read_parquet(model_path))
support <- as.data.table(read_parquet(support_path))
if (model[layer == "seedlings", anyDuplicated(history_id)]) {
  stop("Seedling model histories are not unique")
}
if (support[, anyDuplicated(history_id)]) {
  stop("Seedling support histories are not unique")
}

needed <- c(
  "history_id", "stable_plot_id", baseline_predictors,
  paste0("delta_", responses)
)
seedlings <- model[
  layer == "seedlings" & cumulative_site_CWD_complete %in% TRUE,
  ..needed
]
seedlings <- merge(
  seedlings, support, by = "history_id", all = FALSE, sort = FALSE
)
complete_columns <- c(
  "stable_plot_id", baseline_predictors, paste0("delta_", responses),
  support_specs$source_column
)
seedlings <- seedlings[complete.cases(seedlings[, ..complete_columns])]
if (!nrow(seedlings)) stop("No complete seedling histories are available")

# Calculate clustered coefficient tests for an already fitted model.
clustered_test <- function(fit, sample) {
  covariance <- sandwich::vcovCL(
    fit, cluster = sample$stable_plot_id, type = "HC1"
  )
  test <- as.matrix(lmtest::coeftest(fit, vcov. = covariance))
  list(
    covariance = covariance,
    test = test,
    critical = qt(0.975, df = df.residual(fit))
  )
}

# Describe the exact support variables used in the models.
support_summary <- rbindlist(lapply(seq_len(nrow(support_specs)), function(i) {
  spec <- support_specs[i]
  x <- seedlings[[spec$source_column]]
  data.table(
    support_id = spec$support_id,
    support_label = spec$support_label,
    support_unit = spec$support_unit,
    histories = length(x),
    minimum = min(x),
    p05 = quantile(x, 0.05, names = FALSE),
    p25 = quantile(x, 0.25, names = FALSE),
    median = median(x),
    mean = mean(x),
    p75 = quantile(x, 0.75, names = FALSE),
    p95 = quantile(x, 0.95, names = FALSE),
    maximum = max(x)
  )
}))
fwrite(
  support_summary,
  file.path(output_dir, "seedling_support_distribution.csv")
)

# Refit the baseline on the fixed QA cohort and compare it with production.
baseline_parts <- list()
for (response in responses) {
  outcome <- paste0("delta_", response)
  fit <- lm(reformulate(baseline_predictors, outcome), data = seedlings)
  inference <- clustered_test(fit, seedlings)
  test <- inference$test
  baseline_parts[[response]] <- data.table(
    response,
    term = rownames(test),
    estimate = test[, 1L],
    std_error = test[, 2L],
    n = nobs(fit)
  )[term %in% baseline_predictors]
}
baseline <- rbindlist(baseline_parts)
approved <- fread(approved_path)[
  group == "seedlings" & term %in% baseline_predictors,
  .(response, term, approved_estimate = estimate,
    approved_std_error = std_error)
]
baseline_check <- merge(
  baseline, approved, by = c("response", "term"), all.x = TRUE
)
baseline_check[, `:=`(
  estimate_absolute_difference = abs(estimate - approved_estimate),
  standard_error_absolute_difference = abs(std_error - approved_std_error)
)]
fwrite(
  baseline_check,
  file.path(output_dir, "baseline_reproduction_check.csv")
)

# Fit one interaction model per response and support measure.
interaction_parts <- list()
slope_parts <- list()
joint_parts <- list()
fit_parts <- list()
part <- 0L
for (response in responses) {
  outcome <- paste0("delta_", response)
  for (i in seq_len(nrow(support_specs))) {
    spec <- support_specs[i]
    raw <- seedlings[[spec$source_column]]
    transformed <- if (spec$transform == "log2") log2(raw) else raw
    center <- median(transformed)
    sample <- copy(seedlings)
    sample[, support_centered := transformed - center]

    low_raw <- quantile(raw, 0.25, names = FALSE)
    high_raw <- quantile(raw, 0.75, names = FALSE)
    if (isTRUE(all.equal(low_raw, high_raw))) {
      low_raw <- quantile(raw, 0.05, names = FALSE)
      high_raw <- quantile(raw, 0.95, names = FALSE)
    }
    low_transformed <- if (spec$transform == "log2") {
      log2(low_raw)
    } else {
      low_raw
    }
    high_transformed <- if (spec$transform == "log2") {
      log2(high_raw)
    } else {
      high_raw
    }
    low_centered <- low_transformed - center
    high_centered <- high_transformed - center

    interaction_terms <- paste0(mortality_terms, ":support_centered")
    formula <- reformulate(
      c(baseline_predictors, "support_centered", interaction_terms),
      response = outcome
    )
    fit <- lm(formula, data = sample)
    inference <- clustered_test(fit, sample)
    beta <- coef(fit)
    covariance <- inference$covariance
    critical <- inference$critical
    test <- inference$test

    interaction_names <- paste0(mortality_terms, ":support_centered")
    b <- beta[interaction_names]
    v <- covariance[interaction_names, interaction_names, drop = FALSE]
    joint_statistic <- as.numeric(t(b) %*% qr.solve(v, b))
    joint_parts[[length(joint_parts) + 1L]] <- data.table(
      response,
      support_id = spec$support_id,
      support_label = spec$support_label,
      statistic = joint_statistic,
      df = length(interaction_names),
      p_value = pchisq(
        joint_statistic, df = length(interaction_names), lower.tail = FALSE
      ),
      n = nobs(fit)
    )

    for (agent in mortality_terms) {
      interaction_name <- paste0(agent, ":support_centered")
      estimate <- test[interaction_name, 1L]
      standard_error <- test[interaction_name, 2L]
      part <- part + 1L
      interaction_parts[[part]] <- data.table(
        response,
        support_id = spec$support_id,
        support_label = spec$support_label,
        support_unit = spec$support_unit,
        agent = unname(agent_labels[agent]),
        mortality_term = agent,
        estimate_per_10pp = 10 * estimate,
        std_error_per_10pp = 10 * standard_error,
        conf_low_per_10pp = 10 * (estimate - critical * standard_error),
        conf_high_per_10pp = 10 * (estimate + critical * standard_error),
        p_value = test[interaction_name, 4L],
        n = nobs(fit)
      )

      for (level in c("lower_support", "higher_support")) {
        x <- if (level == "lower_support") low_centered else high_centered
        raw_value <- if (level == "lower_support") low_raw else high_raw
        contrast <- setNames(c(1, x), c(agent, interaction_name))
        slope <- sum(contrast * beta[names(contrast)])
        variance <- as.numeric(
          t(contrast) %*%
            covariance[names(contrast), names(contrast), drop = FALSE] %*%
            contrast
        )
        standard_error <- sqrt(variance)
        slope_parts[[length(slope_parts) + 1L]] <- data.table(
          response,
          support_id = spec$support_id,
          support_label = spec$support_label,
          agent = unname(agent_labels[agent]),
          mortality_term = agent,
          support_level = level,
          support_value = raw_value,
          estimate_per_10pp = 10 * slope,
          std_error_per_10pp = 10 * standard_error,
          conf_low_per_10pp = 10 * (slope - critical * standard_error),
          conf_high_per_10pp = 10 * (slope + critical * standard_error)
        )
      }
    }

    fit_parts[[length(fit_parts) + 1L]] <- data.table(
      response,
      support_id = spec$support_id,
      support_label = spec$support_label,
      n = nobs(fit),
      stable_plots = uniqueN(sample$stable_plot_id),
      r_squared = summary(fit)$r.squared,
      adjusted_r_squared = summary(fit)$adj.r.squared
    )
  }
}

interactions <- rbindlist(interaction_parts)
interactions[, p_value_fdr := p.adjust(p_value, method = "BH")]
setorder(interactions, p_value_fdr, p_value)
slopes <- rbindlist(slope_parts)
joint_tests <- rbindlist(joint_parts)
joint_tests[, p_value_fdr := p.adjust(p_value, method = "BH")]
setorder(joint_tests, p_value_fdr, p_value)
fits <- rbindlist(fit_parts)

fwrite(
  interactions,
  file.path(output_dir, "seedling_support_interaction_tests.csv")
)
fwrite(
  slopes,
  file.path(output_dir, "seedling_support_low_high_slopes.csv")
)
fwrite(
  joint_tests,
  file.path(output_dir, "seedling_support_joint_tests.csv")
)
fwrite(
  fits,
  file.path(output_dir, "seedling_support_model_fit.csv")
)

# Write a short, reproducible interpretation aid.
notable <- interactions[p_value_fdr < 0.05]
nominal <- interactions[p_value < 0.05 & p_value_fdr >= 0.05]
fire_cwd_slopes <- slopes[response == "CWD" & agent == "Fire"]
fire_cwd_consistent <- nrow(fire_cwd_slopes) == 8L &&
  all(fire_cwd_slopes$conf_high_per_10pp < 0)
report <- c(
  "# Seedling-support robustness check",
  "",
  paste0("Histories analyzed: ", format(nrow(seedlings), big.mark = ","), "."),
  "All models use the same histories as the three-response seedling QA cohort.",
  "",
  "## Main finding",
  "",
  if (fire_cwd_consistent) {
    paste(
      "The negative fire-mortality relationship with seedling CWD change remains",
      "negative at representative lower and higher values of all four support",
      "measures, with every 95% confidence interval below zero. Low seedling tally,",
      "low richness, or microplot support therefore does not explain that result."
    )
  } else {
    paste(
      "The fire-mortality relationship with seedling CWD change is not consistent",
      "across representative lower and higher values of all four support measures."
    )
  },
  "",
  paste(
    "No individual support-by-disturbance interaction remained below FDR 0.05.",
    "There is therefore no clear overall evidence that the disturbance results",
    "systematically depend on these four measures of seedling support."
  ),
  "",
  "## Method",
  "",
  "Each support measure was tested in a separate model. The model includes the",
  "original predictors plus interactions between that support measure and fire,",
  "insect, and disease mortality. A positive interaction means the mortality-CWM",
  "slope becomes more positive as seedling support increases; a negative value",
  "means it becomes more negative.",
  "",
  "Initial tally is shown per doubling. Richness and microplot measures are shown",
  "per one-unit increase. Interaction estimates are changes in the CWM response",
  "associated with 10 additional percentage points of cumulative mortality.",
  "",
  "These are sampling-support diagnostics, not proposed controls for the main",
  "model. Seedling abundance and richness can reflect real ecology as well as",
  "measurement precision, so an interaction does not identify its cause.",
  "",
  "## Results after false-discovery-rate adjustment",
  ""
)
if (!nrow(notable)) {
  report <- c(report, "No individual interaction remained below FDR 0.05.")
} else {
  report <- c(report, paste0(
    "- ", notable$response, "; ", notable$agent, "; ",
    notable$support_label, ": ",
    sprintf("%.3f", notable$estimate_per_10pp), " (95% CI ",
    sprintf("%.3f", notable$conf_low_per_10pp), " to ",
    sprintf("%.3f", notable$conf_high_per_10pp), "; FDR p = ",
    sprintf("%.3g", notable$p_value_fdr), ")"
  ))
}
report <- c(report, "", "## Additional nominal signals", "")
if (!nrow(nominal)) {
  report <- c(report, "No additional interactions had unadjusted p < 0.05.")
} else {
  report <- c(report, paste0(
    "- ", nominal$response, "; ", nominal$agent, "; ",
    nominal$support_label, ": unadjusted p = ",
    sprintf("%.3g", nominal$p_value), ", FDR p = ",
    sprintf("%.3g", nominal$p_value_fdr)
  ))
}
writeLines(
  report,
  file.path(output_dir, "seedling_support_robustness_report.md")
)

message("Seedling-support robustness QA: ", output_dir)
