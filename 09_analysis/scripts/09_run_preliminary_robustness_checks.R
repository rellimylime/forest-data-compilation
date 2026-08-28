#!/usr/bin/env Rscript

# Test whether the preliminary coefficients depend on sample or model choices.

suppressPackageStartupMessages({
  library(arrow)
  library(data.table)
  library(ggplot2)
  library(lmtest)
  library(rmarkdown)
  library(sandwich)
})

run_id <- Sys.getenv(
  "ANALYSIS_RUN_ID",
  unset = "20260822_cumulative_mortality_site_cwd_all_groups_v01"
)
data_dir <- file.path("09_analysis", "data", "processed")
run_dir <- file.path("09_analysis", "results", "model_runs", run_id)
output_dir <- file.path(run_dir, "robustness")
figure_dir <- file.path(output_dir, "figures")
dir.create(figure_dir, recursive = TRUE, showWarnings = FALSE)

responses <- c("temperature", "precipitation", "CWD")
groups <- c("seedlings", "saplings", "trees", "community")
stage_groups <- groups[1:3]
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
mean_cwd_predictors <- c(
  mortality_terms,
  "mean_monthly_site_CWD_mm",
  "full_survey_period_years"
)
no_duration_predictors <- c(
  mortality_terms,
  "cumulative_site_CWD_mm"
)
condition_support_threshold <- 0.80
scenario_order <- c(
  "baseline", "common_histories", "condition_prop_80",
  "state_fixed_effects", "mean_monthly_CWD", "no_survey_period"
)
group_labels <- c(
  seedlings = "Seedlings",
  saplings = "Saplings",
  trees = "Adults",
  community = "Pooled all live"
)

stage_data <- as.data.table(read_parquet(
  file.path(data_dir, "lifestage_model_data.parquet")
))
community_data <- as.data.table(read_parquet(
  file.path(data_dir, "pooled_model_data.parquet")
))
intervals <- as.data.table(read_parquet(
  file.path(data_dir, "stable_condition_intervals.parquet"),
  col_select = c(
    "stable_plot_id", "remeasurement_component_id", "CONDID",
    "T1_CONDPROP_UNADJ", "T2_CONDPROP_UNADJ"
  )
))
site_cwd <- as.data.table(read_parquet(
  file.path(data_dir, "history_site_cwd.parquet"),
  col_select = c("history_id", "mean_monthly_site_CWD_mm")
))

# Summarize how much of the plot the retained condition occupies over time.
support <- intervals[, .(
  minimum_condition_proportion = min(
    c(T1_CONDPROP_UNADJ, T2_CONDPROP_UNADJ), na.rm = TRUE
  )
), by = .(stable_plot_id, remeasurement_component_id, CONDID)]

attach_qa_fields <- function(x) {
  out <- merge(
    x, support,
    by = c("stable_plot_id", "remeasurement_component_id", "CONDID"),
    all.x = TRUE, sort = FALSE
  )
  merge(out, site_cwd, by = "history_id", all.x = TRUE, sort = FALSE)
}

stage_data <- attach_qa_fields(stage_data)
community_data <- attach_qa_fields(community_data)
source_data <- list(
  seedlings = stage_data[layer == "seedlings"],
  saplings = stage_data[layer == "saplings"],
  trees = stage_data[layer == "trees"],
  community = community_data
)

model_sample <- function(d, outcome, predictors) {
  needed <- c(outcome, predictors, "stable_plot_id", "history_id")
  d[
    cumulative_site_CWD_complete %in% TRUE &
      complete.cases(d[, ..needed])
  ]
}

fit_model <- function(d, response, group, scenario, predictors, extra = NULL) {
  outcome <- paste0("delta_", response)
  sample_columns <- predictors
  if (identical(extra, "factor(state)")) {
    sample_columns <- c(sample_columns, "state")
  }
  sample <- model_sample(d, outcome, sample_columns)
  rhs <- c(predictors, extra)
  formula <- reformulate(rhs, response = outcome)
  model <- lm(formula, data = sample)
  vcov <- sandwich::vcovCL(
    model, cluster = sample$stable_plot_id, type = "HC1"
  )
  test <- as.matrix(lmtest::coeftest(model, vcov. = vcov))
  critical <- qt(0.975, df = df.residual(model))
  coefficients <- data.table(
    scenario,
    response,
    group,
    group_label = unname(group_labels[group]),
    term = rownames(test),
    estimate = test[, 1L],
    std_error = test[, 2L],
    statistic = test[, 3L],
    p_value = test[, 4L],
    conf_low = test[, 1L] - critical * test[, 2L],
    conf_high = test[, 1L] + critical * test[, 2L],
    n = nobs(model),
    stable_plots = uniqueN(sample$stable_plot_id)
  )
  fit <- data.table(
    scenario,
    response,
    group,
    n = nobs(model),
    stable_plots = uniqueN(sample$stable_plot_id),
    r_squared = summary(model)$r.squared,
    adjusted_r_squared = summary(model)$adj.r.squared
  )
  list(coefficients = coefficients, fit = fit, sample = sample)
}

# Find histories with complete responses for all three life stages.
common_history_ids <- list()
common_history_summary <- list()
for (response in responses) {
  outcome <- paste0("delta_", response)
  complete_stage <- stage_data[
    cumulative_site_CWD_complete %in% TRUE &
      complete.cases(stage_data[, c(outcome, baseline_predictors), with = FALSE])
  ]
  common_ids <- complete_stage[, .(n_stages = uniqueN(layer)), by = history_id][
    n_stages == length(stage_groups), history_id
  ]
  common_history_ids[[response]] <- common_ids
  common_history_summary[[response]] <- data.table(
    response,
    common_histories = length(common_ids),
    seedling_baseline_histories = uniqueN(
      complete_stage[layer == "seedlings", history_id]
    ),
    sapling_baseline_histories = uniqueN(
      complete_stage[layer == "saplings", history_id]
    ),
    adult_baseline_histories = uniqueN(
      complete_stage[layer == "trees", history_id]
    )
  )
}

coefficient_parts <- list()
fit_parts <- list()
part <- 0L
add_fit <- function(result) {
  part <<- part + 1L
  coefficient_parts[[part]] <<- result$coefficients
  fit_parts[[part]] <<- result$fit
}

for (response in responses) {
  for (group in groups) {
    d <- source_data[[group]]
    add_fit(fit_model(
      d, response, group, "baseline", baseline_predictors
    ))
    add_fit(fit_model(
      d[minimum_condition_proportion >= condition_support_threshold],
      response, group, "condition_prop_80", baseline_predictors
    ))
    add_fit(fit_model(
      d, response, group, "state_fixed_effects", baseline_predictors,
      extra = "factor(state)"
    ))
    add_fit(fit_model(
      d, response, group, "mean_monthly_CWD", mean_cwd_predictors
    ))
    add_fit(fit_model(
      d, response, group, "no_survey_period", no_duration_predictors
    ))
    if (group %in% stage_groups) {
      add_fit(fit_model(
        d[history_id %in% common_history_ids[[response]]],
        response, group, "common_histories", baseline_predictors
      ))
    }
  }
}

coefficients <- rbindlist(coefficient_parts, fill = TRUE)
fits <- rbindlist(fit_parts, fill = TRUE)
coefficients[, scenario := factor(scenario, levels = scenario_order)]
fits[, scenario := factor(scenario, levels = scenario_order)]
setorder(coefficients, response, group, scenario, term)
setorder(fits, response, group, scenario)

focal_terms <- unique(c(
  baseline_predictors,
  "mean_monthly_site_CWD_mm"
))
focal_coefficients <- coefficients[term %in% focal_terms]
fwrite(focal_coefficients, file.path(output_dir, "coefficients.csv"))
fwrite(fits, file.path(output_dir, "model_fit.csv"))
fwrite(
  rbindlist(common_history_summary),
  file.path(output_dir, "common_history_summary.csv")
)

# Confirm the robustness script reproduces the approved baseline coefficients.
approved <- fread(file.path(run_dir, "coefficients.csv"))
baseline_check <- merge(
  focal_coefficients[scenario == "baseline"],
  approved,
  by = c("response", "group", "term"),
  suffixes = c("_robustness", "_approved")
)
baseline_check[, `:=`(
  estimate_absolute_difference = abs(
    estimate_robustness - estimate_approved
  ),
  standard_error_absolute_difference = abs(
    std_error_robustness - std_error_approved
  )
)]
fwrite(baseline_check, file.path(output_dir, "baseline_reproduction.csv"))
if (baseline_check[, max(estimate_absolute_difference)] > 1e-12 ||
    baseline_check[, max(standard_error_absolute_difference)] > 1e-12) {
  stop("Robustness baseline does not reproduce the approved model output.")
}

# Compare the disturbance coefficients across model checks.
mortality_stability <- dcast(
  focal_coefficients[term %in% mortality_terms],
  response + group + group_label + term ~ scenario,
  value.var = c("estimate", "conf_low", "conf_high", "p_value", "n")
)
scenario_estimates <- paste0("estimate_", scenario_order)
available_estimates <- intersect(scenario_estimates, names(mortality_stability))
mortality_stability[, sign_consistent := apply(
  .SD, 1L, function(x) {
    x <- x[!is.na(x)]
    length(unique(sign(x[x != 0]))) <= 1L
  }
), .SDcols = available_estimates]
fwrite(mortality_stability, file.path(output_dir, "mortality_stability.csv"))

# Report sample size as condition support becomes stricter.
support_thresholds <- c(0.30, 0.50, 0.80, 0.95)
support_summary <- rbindlist(lapply(groups, function(group) {
  d <- source_data[[group]]
  rbindlist(lapply(support_thresholds, function(threshold) {
    data.table(
      group,
      group_label = unname(group_labels[group]),
      threshold,
      histories = uniqueN(
        d[minimum_condition_proportion >= threshold, history_id]
      ),
      stable_plots = uniqueN(
        d[minimum_condition_proportion >= threshold, stable_plot_id]
      )
    )
  }))
}))
fwrite(support_summary, file.path(output_dir, "condition_support_summary.csv"))

# Test life-stage coefficient differences on the identical history sample.
interaction_tests <- list()
pairwise_tests <- list()
interaction_index <- 0L
pairwise_index <- 0L
for (response in responses) {
  outcome <- paste0("delta_", response)
  d <- stage_data[
    history_id %in% common_history_ids[[response]] &
      layer %in% stage_groups
  ]
  d[, layer := factor(layer, levels = stage_groups)]
  formula <- as.formula(paste0(
    outcome, " ~ layer * (",
    paste(baseline_predictors, collapse = " + "), ")"
  ))
  model <- lm(formula, data = d)
  vcov <- sandwich::vcovCL(
    model, cluster = d$stable_plot_id, type = "HC1"
  )
  beta <- coef(model)
  critical <- qt(0.975, df = df.residual(model))

  for (predictor in baseline_predictors) {
    interaction_names <- names(beta)[
      grepl(":", names(beta), fixed = TRUE) &
        grepl(predictor, names(beta), fixed = TRUE)
    ]
    sapling_name <- interaction_names[
      grepl("layersaplings", interaction_names, fixed = TRUE)
    ]
    adult_name <- interaction_names[
      grepl("layertrees", interaction_names, fixed = TRUE)
    ]
    if (length(sapling_name) != 1L || length(adult_name) != 1L) {
      stop("Could not identify life-stage interactions for ", predictor)
    }

    joint_names <- c(sapling_name, adult_name)
    b <- beta[joint_names]
    v <- vcov[joint_names, joint_names, drop = FALSE]
    statistic <- as.numeric(t(b) %*% qr.solve(v, b))
    interaction_index <- interaction_index + 1L
    interaction_tests[[interaction_index]] <- data.table(
      response,
      predictor,
      histories = length(common_history_ids[[response]]),
      statistic,
      df = 2L,
      p_value = pchisq(statistic, df = 2L, lower.tail = FALSE)
    )

    contrasts <- list(
      "Saplings - Seedlings" = setNames(1, sapling_name),
      "Adults - Seedlings" = setNames(1, adult_name),
      "Adults - Saplings" = setNames(c(-1, 1), c(sapling_name, adult_name))
    )
    for (comparison in names(contrasts)) {
      weights <- contrasts[[comparison]]
      estimate <- sum(weights * beta[names(weights)])
      variance <- as.numeric(
        t(weights) %*% vcov[names(weights), names(weights), drop = FALSE] %*%
          weights
      )
      standard_error <- sqrt(variance)
      pairwise_index <- pairwise_index + 1L
      pairwise_tests[[pairwise_index]] <- data.table(
        response,
        predictor,
        comparison,
        estimate,
        std_error = standard_error,
        conf_low = estimate - critical * standard_error,
        conf_high = estimate + critical * standard_error,
        p_value = 2 * pt(
          abs(estimate / standard_error),
          df = df.residual(model), lower.tail = FALSE
        )
      )
    }
  }
}

interaction_tests <- rbindlist(interaction_tests)
pairwise_tests <- rbindlist(pairwise_tests)
fwrite(
  interaction_tests,
  file.path(output_dir, "life_stage_interaction_tests.csv")
)
fwrite(
  pairwise_tests,
  file.path(output_dir, "life_stage_pairwise_differences.csv")
)

# Keep the cumulative-CWD and duration concern visible in one table.
duration_sensitivity <- focal_coefficients[
  scenario %in% c("baseline", "mean_monthly_CWD", "no_survey_period") &
    term %in% c(
      mortality_terms,
      "cumulative_site_CWD_mm",
      "mean_monthly_site_CWD_mm",
      "full_survey_period_years"
    )
]
fwrite(
  duration_sensitivity,
  file.path(output_dir, "cwd_duration_sensitivity.csv")
)

# Plot disturbance coefficient stability in raw response units.
plot_data <- copy(focal_coefficients[term %in% mortality_terms])
plot_data[, `:=`(
  scenario = factor(scenario, levels = scenario_order),
  group_label = factor(
    group_label,
    levels = unname(group_labels[groups])
  ),
  agent = factor(
    term,
    levels = mortality_terms,
    labels = c("Fire", "Insect", "Disease")
  ),
  response_label = factor(
    response,
    levels = responses,
    labels = c("Temperature CWM", "Precipitation CWM", "CWD CWM")
  )
)]

stability_plot <- ggplot(
  plot_data,
  aes(x = estimate, y = scenario, color = group_label)
) +
  geom_vline(xintercept = 0, color = "grey60", linewidth = 0.4) +
  geom_errorbar(
    aes(xmin = conf_low, xmax = conf_high),
    orientation = "y", width = 0,
    alpha = 0.45, position = position_dodge(width = 0.55)
  ) +
  geom_point(position = position_dodge(width = 0.55), size = 1.8) +
  facet_grid(response_label ~ agent, scales = "free_x") +
  scale_y_discrete(labels = c(
    baseline = "Baseline",
    common_histories = "Same histories",
    condition_prop_80 = "Condition >= 80%",
    state_fixed_effects = "State effects",
    mean_monthly_CWD = "Mean monthly CWD",
    no_survey_period = "No duration term"
  )) +
  labs(
    x = "Mortality coefficient in raw response units",
    y = NULL,
    color = "Community group",
    title = "Disturbance coefficient stability",
    subtitle = "Points are estimates; bars are plot-clustered 95% confidence intervals"
  ) +
  theme_minimal(base_size = 10.5) +
  theme(
    panel.grid.minor = element_blank(),
    strip.text = element_text(face = "bold"),
    legend.position = "bottom"
  )
ggsave(
  file.path(figure_dir, "mortality_coefficient_stability.png"),
  stability_plot, width = 13, height = 10, dpi = 180
)

for (response_value in levels(plot_data$response_label)) {
  response_plot <- ggplot(
    plot_data[response_label == response_value],
    aes(x = estimate, y = scenario, color = group_label)
  ) +
    geom_vline(xintercept = 0, color = "grey60", linewidth = 0.4) +
    geom_errorbar(
      aes(xmin = conf_low, xmax = conf_high),
      orientation = "y", width = 0,
      alpha = 0.45, position = position_dodge(width = 0.55)
    ) +
    geom_point(position = position_dodge(width = 0.55), size = 2) +
    facet_wrap(~agent, scales = "free_x", nrow = 1L) +
    scale_y_discrete(labels = c(
      baseline = "Baseline",
      common_histories = "Same histories",
      condition_prop_80 = "Condition >= 80%",
      state_fixed_effects = "State effects",
      mean_monthly_CWD = "Mean monthly CWD",
      no_survey_period = "No duration term"
    )) +
    labs(
      x = "Mortality coefficient in raw response units",
      y = NULL,
      color = "Community group",
      title = paste(response_value, "disturbance coefficient stability"),
      subtitle = paste(
        "Points are estimates; bars are plot-clustered 95% confidence intervals"
      )
    ) +
    theme_minimal(base_size = 11) +
    theme(
      panel.grid.minor = element_blank(),
      strip.text = element_text(face = "bold"),
      legend.position = "bottom"
    )
  file_slug <- tolower(strsplit(response_value, " ", fixed = TRUE)[[1]][1])
  ggsave(
    file.path(
      figure_dir,
      paste0(file_slug, "_mortality_coefficient_stability.png")
    ),
    response_plot, width = 12, height = 5.8, dpi = 180
  )
}

manifest <- data.table(
  item = c(
    "primary_model_run", "condition_support_threshold",
    "common_history_definition", "covariance", "report"
  ),
  value = c(
    run_id,
    as.character(condition_support_threshold),
    "Same history has complete outcome and predictors for all three life stages",
    "HC1 clustered by stable_plot_id",
    "robustness_results.html"
  )
)
fwrite(manifest, file.path(output_dir, "manifest.csv"))

report_source <- file.path(
  "09_analysis", "scripts", "templates",
  "preliminary_robustness_report.Rmd"
)
Sys.setenv(ANALYSIS_RUN_DIR = normalizePath(run_dir, winslash = "/"))
rmarkdown::render(
  report_source,
  output_file = "robustness_results.html",
  output_dir = output_dir,
  quiet = TRUE,
  envir = new.env(parent = globalenv())
)

message("Robustness results: ", output_dir)
