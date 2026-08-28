#!/usr/bin/env Rscript

# Fit the twelve preliminary models and build Joan's single-file report.
# twelve models (three climate-niche responses by four vegetation groups), raw
# complete-case relationships with descriptive GAM smooths, and clustered-HC1
# marginal predictions from ggeffects::ggpredict().

suppressPackageStartupMessages({
  library(arrow)
  library(data.table)
  library(ggeffects)
  library(ggplot2)
  library(lmtest)
  library(mgcv)
  library(parameters)
  library(rmarkdown)
  library(sandwich)
  library(scales)
  library(sjPlot)
})

run_id <- Sys.getenv(
  "ANALYSIS_RUN_ID",
  unset = "20260822_cumulative_mortality_site_cwd_all_groups_v01"
)
data_dir <- file.path("09_analysis", "data", "processed")
run_dir <- file.path("09_analysis", "results", "model_runs", run_id)
output_dir <- run_dir
report_dir <- run_dir
table_dir <- file.path(run_dir, "tables")
figure_dir <- file.path(run_dir, "figures")
raw_figure_dir <- file.path(figure_dir, "raw_relationships")
effect_figure_dir <- file.path(figure_dir, "marginal_effects")

for (path in c(
  output_dir, report_dir, table_dir, figure_dir,
  raw_figure_dir, effect_figure_dir
)) {
  dir.create(path, recursive = TRUE, showWarnings = FALSE)
}

stage_input <- file.path(data_dir, "lifestage_model_data.parquet")
community_input <- file.path(data_dir, "pooled_model_data.parquet")
report_source <- file.path(
  "09_analysis", "scripts", "templates",
  "preliminary_cumulative_mortality_full_report.Rmd"
)
stopifnot(
  file.exists(stage_input),
  file.exists(community_input),
  file.exists(report_source)
)

responses <- c("temperature", "precipitation", "CWD")
groups <- c("seedlings", "saplings", "trees", "community")
predictors <- c(
  "fire_cumulative_mortality_pct",
  "insect_cumulative_mortality_pct",
  "disease_cumulative_mortality_pct",
  "cumulative_site_CWD_mm",
  "full_survey_period_years"
)
drivers <- predictors[1:4]
group_labels <- c(
  seedlings = "Seedlings",
  saplings = "Saplings",
  trees = "Adults",
  community = "Pooled all live"
)
response_labels <- c(
  temperature = "Temperature CWM change",
  precipitation = "Precipitation CWM change",
  CWD = "CWD CWM change"
)
response_y_labels <- c(
  temperature = "Change in temperature CWM",
  precipitation = "Change in precipitation CWM",
  CWD = "Change in CWD CWM"
)
driver_labels <- c(
  fire_cumulative_mortality_pct = "Fire mortality",
  insect_cumulative_mortality_pct = "Insect mortality",
  disease_cumulative_mortality_pct = "Disease mortality",
  cumulative_site_CWD_mm = "Cumulative site CWD"
)
driver_axis_labels <- c(
  fire_cumulative_mortality_pct = "Cumulative fire mortality (%)",
  insect_cumulative_mortality_pct = "Cumulative insect mortality (%)",
  disease_cumulative_mortality_pct = "Cumulative disease mortality (%)",
  cumulative_site_CWD_mm = "Cumulative site CWD (mm)"
)
driver_slugs <- c(
  fire_cumulative_mortality_pct = "fire_mortality",
  insect_cumulative_mortality_pct = "insect_mortality",
  disease_cumulative_mortality_pct = "disease_mortality",
  cumulative_site_CWD_mm = "site_cwd"
)
predictor_table_labels <- c(
  "Intercept",
  "Cumulative fire mortality (%)",
  "Cumulative insect mortality (%)",
  "Cumulative disease mortality (%)",
  "Cumulative site CWD (mm)",
  "Full survey period (years)"
)

vcov_plot_hc1 <- function(x, ...) {
  cluster <- attr(x, "cluster", exact = TRUE)
  if (is.null(cluster) || length(cluster) != nobs(x) || anyNA(cluster)) {
    stop("Missing or misaligned stable_plot_id cluster vector.")
  }
  sandwich::vcovCL(x, cluster = cluster, type = "HC1")
}

stage_data <- as.data.table(read_parquet(stage_input))
community_data <- as.data.table(read_parquet(community_input))
source_data <- list(
  seedlings = stage_data[layer == "seedlings"],
  saplings = stage_data[layer == "saplings"],
  trees = stage_data[layer == "trees"],
  community = community_data
)

models <- list()
model_data <- list()
coefficient_parts <- list()
fit_parts <- list()
sample_parts <- list()

for (group_name in groups) {
  group_source <- source_data[[group_name]]
  for (response_name in responses) {
    outcome <- paste0("delta_", response_name)
    needed <- c(outcome, predictors, "stable_plot_id", "history_id")
    keep <- group_source$cumulative_site_CWD_complete %in% TRUE &
      complete.cases(group_source[, ..needed])
    d <- group_source[keep]
    model_id <- paste(group_name, response_name, sep = "__")
    formula <- as.formula(paste(
      outcome, "~", paste(predictors, collapse = " + ")
    ))
    model <- lm(formula, data = d)
    attr(model, "cluster") <- d$stable_plot_id
    test <- coeftest(model, vcov. = vcov_plot_hc1(model))
    test_matrix <- as.matrix(test)
    ci_critical <- qt(0.975, df = df.residual(model))

    models[[model_id]] <- model
    model_data[[model_id]] <- d
    coefficient_parts[[model_id]] <- data.table(
      model_id = model_id,
      group = group_name,
      group_label = unname(group_labels[group_name]),
      response = response_name,
      term = rownames(test_matrix),
      estimate = test_matrix[, 1L],
      std_error = test_matrix[, 2L],
      statistic = test_matrix[, 3L],
      p_value = test_matrix[, 4L],
      conf_low = test_matrix[, 1L] - ci_critical * test_matrix[, 2L],
      conf_high = test_matrix[, 1L] + ci_critical * test_matrix[, 2L]
    )
    fit_parts[[model_id]] <- data.table(
      model_id = model_id,
      group = group_name,
      group_label = unname(group_labels[group_name]),
      response = response_name,
      n = nobs(model),
      stable_plots = uniqueN(d$stable_plot_id),
      r_squared = summary(model)$r.squared,
      adjusted_r_squared = summary(model)$adj.r.squared,
      vcov = "HC1 clustered by stable_plot_id"
    )
    sample_parts[[model_id]] <- data.table(
      model_id = model_id,
      group = group_name,
      group_label = unname(group_labels[group_name]),
      response = response_name,
      available_rows = nrow(group_source),
      complete_rows = nrow(d),
      excluded_rows = nrow(group_source) - nrow(d),
      stable_plots = uniqueN(d$stable_plot_id),
      positive_fire = sum(d$fire_cumulative_mortality_pct > 0),
      positive_insect = sum(d$insect_cumulative_mortality_pct > 0),
      positive_disease = sum(d$disease_cumulative_mortality_pct > 0)
    )
  }
}

coefficients <- rbindlist(coefficient_parts)
fits <- rbindlist(fit_parts)
samples <- rbindlist(sample_parts)
coefficients[, `:=`(
  response_order = match(response, responses),
  group_order = match(group, groups),
  term_order = match(term, c("(Intercept)", predictors))
)]
setorder(coefficients, response_order, group_order, term_order)
coefficients[, c("response_order", "group_order", "term_order") := NULL]
samples[, `:=`(
  response_order = match(response, responses),
  group_order = match(group, groups)
)]
setorder(samples, response_order, group_order)
samples[, c("response_order", "group_order") := NULL]
fits[, `:=`(
  response_order = match(response, responses),
  group_order = match(group, groups)
)]
setorder(fits, response_order, group_order)
fits[, c("response_order", "group_order") := NULL]

coefficient_path <- file.path(
  output_dir, "coefficients.csv"
)
fit_path <- file.path(output_dir, "model_fit.csv")
sample_path <- file.path(
  output_dir, "sample_flow.csv"
)
fwrite(coefficients, coefficient_path)
fwrite(fits, fit_path)
fwrite(samples, sample_path)

# Common prediction reference: no competing agent mortality, and the median
# site CWD and survey duration among the pooled-community histories. This makes
# all four vegetation-group curves describe the same covariate setting.
reference_source <- community_data[
  cumulative_site_CWD_complete %in% TRUE &
    complete.cases(community_data[, ..predictors])
]
common_reference <- list(
  fire_cumulative_mortality_pct = 0,
  insect_cumulative_mortality_pct = 0,
  disease_cumulative_mortality_pct = 0,
  cumulative_site_CWD_mm = median(reference_source$cumulative_site_CWD_mm),
  full_survey_period_years = median(reference_source$full_survey_period_years)
)
reference_table <- data.table(
  predictor = names(common_reference),
  reference_value = as.numeric(unlist(common_reference)),
  definition = c(
    "No fire-attributed mortality",
    "No insect-attributed mortality",
    "No disease-attributed mortality",
    "Median among pooled-community histories with complete predictors",
    "Median among pooled-community histories with complete predictors"
  )
)
reference_path <- file.path(
  output_dir, "prediction_reference.csv"
)
fwrite(reference_table, reference_path)

# Four-column sjPlot model tables, one for each response.
table_manifest_parts <- list()
style_written <- FALSE
for (response_name in responses) {
  response_models <- unname(lapply(groups, function(group_name) {
    models[[paste(group_name, response_name, sep = "__")]]
  }))
  sj_table <- sjPlot::tab_model(
    response_models,
    transform = NULL,
    title = response_labels[[response_name]],
    dv.labels = unname(group_labels),
    pred.labels = predictor_table_labels,
    auto.label = FALSE,
    show.intercept = TRUE,
    show.est = TRUE,
    show.std = NULL,
    show.se = TRUE,
    show.ci = 0.95,
    show.p = TRUE,
    p.val = "wald",
    show.stat = FALSE,
    show.df = FALSE,
    show.obs = TRUE,
    show.r2 = TRUE,
    show.aic = FALSE,
    show.fstat = FALSE,
    vcov.fun = vcov_plot_hc1,
    vcov.args = list(),
    string.pred = "Predictor",
    string.est = "Estimate",
    string.se = "HC1 SE",
    string.ci = "95% CI",
    digits = 7,
    digits.p = 3,
    digits.rsq = 4,
    p.style = "numeric",
    use.viewer = FALSE
  )
  standalone_path <- file.path(
    table_dir, paste0(tolower(response_name), "_models.html")
  )
  fragment_path <- file.path(
    table_dir, paste0(tolower(response_name), "_models_fragment.html")
  )
  writeLines(sj_table$page.complete, standalone_path, useBytes = TRUE)
  writeLines(sj_table$knitr, fragment_path, useBytes = TRUE)
  if (!style_written) {
    writeLines(
      sj_table$page.style,
      file.path(table_dir, "sjplot_table_style.html"),
      useBytes = TRUE
    )
    style_written <- TRUE
  }
  table_manifest_parts[[response_name]] <- data.table(
    response = tolower(response_name),
    response_label = response_labels[[response_name]],
    standalone_table_path = standalone_path,
    report_fragment_path = fragment_path,
    model_columns = paste(unname(group_labels), collapse = ";"),
    engine = "sjPlot::tab_model",
    sjplot_version = as.character(packageVersion("sjPlot")),
    covariance = "HC1 clustered by stable_plot_id"
  )
}
table_manifest <- rbindlist(table_manifest_parts)
table_manifest_path <- file.path(table_dir, "table_manifest.csv")
fwrite(table_manifest, table_manifest_path)

raw_theme <- theme_minimal(base_size = 11.5) +
  theme(
    panel.grid.minor = element_blank(),
    strip.text = element_text(face = "bold"),
    plot.title = element_text(face = "bold", size = 14),
    plot.subtitle = element_text(size = 10.5),
    plot.caption = element_text(hjust = 0, size = 9),
    axis.title = element_text(face = "bold"),
    legend.position = "none"
  )

figure_manifest_parts <- list()
figure_counter <- 0L

for (response_name in responses) {
  outcome <- paste0("delta_", response_name)
  response_sample <- samples[response == response_name]
  panel_n <- setNames(
    paste0(
      response_sample$group_label,
      "\nN = ", comma(response_sample$complete_rows)
    ),
    response_sample$group
  )

  for (driver_name in drivers) {
    figure_counter <- figure_counter + 1L
    raw_parts <- lapply(groups, function(group_name) {
      d <- model_data[[paste(group_name, response_name, sep = "__")]]
      data.table(
        x = d[[driver_name]],
        y = d[[outcome]],
        group = group_name
      )
    })
    raw_data <- rbindlist(raw_parts)
    if (driver_name %in% predictors[1:3]) {
      positive_share <- raw_data[, .(positive = mean(x > 0)), by = group]
      panel_labels <- setNames(
        paste0(
          unname(group_labels[positive_share$group]),
          "\nN = ", comma(response_sample[match(positive_share$group, group), complete_rows]),
          "; >0 = ", percent(positive_share$positive, accuracy = 0.1)
        ),
        positive_share$group
      )
    } else {
      panel_labels <- panel_n
    }
    raw_data[, panel := factor(
      unname(panel_labels[group]),
      levels = unname(panel_labels[groups])
    )]

    # Show the empirical cloud without allowing tens of thousands of exact-zero
    # mortality histories, or a handful of response outliers, to obscure it.
    # The descriptive GAM below is still fit to every complete-case history.
    set.seed(20260822L + figure_counter)
    if (driver_name %in% predictors[1:3]) {
      raw_points <- rbindlist(list(
        raw_data[x > 0],
        raw_data[x == 0, .SD[sample.int(.N, min(.N, 2500L))], by = group]
      ), use.names = TRUE)
      point_description <- paste(
        "Points: all positive-mortality histories plus up to 2,500 randomly",
        "selected zero-mortality histories per panel."
      )
    } else {
      raw_points <- raw_data[
        , .SD[sample.int(.N, min(.N, 5000L))],
        by = group
      ]
      point_description <- paste(
        "Points: up to 5,000 randomly selected complete-case histories",
        "per panel."
      )
    }
    y_limits <- as.numeric(
      quantile(raw_data$y, probs = c(0.005, 0.995), na.rm = TRUE)
    )
    if (!all(is.finite(y_limits)) || diff(y_limits) <= 0) {
      y_limits <- range(raw_data$y, finite = TRUE)
    }
    outside_y <- sum(raw_data$y < y_limits[1] | raw_data$y > y_limits[2])

    raw_plot <- ggplot(raw_points, aes(x = x, y = y)) +
      geom_point(
        color = "#4C5B66", alpha = 0.12, size = 0.45,
        shape = 16, stroke = 0
      ) +
      geom_smooth(
        data = raw_data,
        method = "gam",
        formula = y ~ s(x, bs = "cs", k = 5),
        method.args = list(method = "REML"),
        se = TRUE,
        level = 0.95,
        color = "#C44E52",
        fill = "#C44E52",
        alpha = 0.18,
        linewidth = 0.9,
        n = 160
      ) +
      facet_wrap(~panel, ncol = 2, scales = "fixed") +
      scale_x_continuous(
        labels = label_number(big.mark = ","),
        expand = expansion(mult = c(0.02, 0.05))
      ) +
      scale_y_continuous(labels = label_number(big.mark = ",")) +
      coord_cartesian(ylim = y_limits) +
      labs(
        title = paste(
          "Raw relationship:", response_labels[[response_name]], "vs",
          driver_labels[[driver_name]]
        ),
        subtitle = paste(
          point_description,
          "\nRed GAM and 95% CI use all complete-case histories."
        ),
        x = driver_axis_labels[[driver_name]],
        y = response_y_labels[[response_name]],
        caption = paste(
          "The viewing window spans the 0.5th-99.5th response percentiles;",
          comma(outside_y), "histories fall outside it but remain in the GAM fit.",
          "The GAM is descriptive and is not the fitted linear model."
        )
      ) +
      raw_theme

    raw_path <- file.path(
      raw_figure_dir,
      paste0(
        tolower(response_name), "__", driver_slugs[[driver_name]], ".png"
      )
    )
    ggsave(
      raw_path, raw_plot,
      width = 10.5, height = 7.2, dpi = 150, bg = "white"
    )
    figure_manifest_parts[[paste0("raw_", figure_counter)]] <- data.table(
      section = "raw_relationship",
      response = tolower(response_name),
      response_label = response_labels[[response_name]],
      driver = driver_name,
      driver_label = driver_labels[[driver_name]],
      figure_path = raw_path,
      caption = paste(
        "Complete-case raw observations and descriptive GAM for",
        paste(unname(group_labels), collapse = ", ")
      ),
      plot_order = 2L * figure_counter - 1L,
      panels = 4L,
      uncertainty = "Descriptive GAM 95% confidence interval"
    )

    # Marginal predictions use a common x grid and the same reference values
    # across all four models so the panels are directly comparable.
    x_grid <- if (driver_name %in% predictors[1:3]) {
      seq(0, 100, length.out = 51L)
    } else {
      observed <- unlist(lapply(groups, function(group_name) {
        model_data[[paste(group_name, response_name, sep = "__")]][[driver_name]]
      }), use.names = FALSE)
      seq(min(observed), max(observed), length.out = 61L)
    }
    prediction_parts <- lapply(groups, function(group_name) {
      model_id <- paste(group_name, response_name, sep = "__")
      model <- models[[model_id]]
      condition <- common_reference[setdiff(predictors, driver_name)]
      prediction <- ggeffects::ggpredict(
        model,
        terms = setNames(list(x_grid), driver_name),
        ci_level = 0.95,
        type = "fixed",
        typical = "mean",
        condition = condition,
        interval = "confidence",
        back_transform = TRUE,
        vcov = vcov_plot_hc1,
        vcov_args = list(),
        verbose = FALSE
      )
      data.table(
        x = prediction$x,
        predicted = prediction$predicted,
        conf_low = prediction$conf.low,
        conf_high = prediction$conf.high,
        group = group_name
      )
    })
    prediction_data <- rbindlist(prediction_parts)
    prediction_data[, panel := factor(
      unname(panel_n[group]),
      levels = unname(panel_n[groups])
    )]

    effect_plot <- ggplot(
      prediction_data,
      aes(x = x, y = predicted, ymin = conf_low, ymax = conf_high)
    ) +
      geom_ribbon(fill = "#4C78A8", alpha = 0.20) +
      geom_line(color = "#2F5D8A", linewidth = 0.95) +
      facet_wrap(~panel, ncol = 2, scales = "fixed") +
      scale_x_continuous(
        labels = label_number(big.mark = ","),
        expand = expansion(mult = c(0.02, 0.05))
      ) +
      scale_y_continuous(labels = label_number(big.mark = ",")) +
      labs(
        title = paste(
          "Adjusted marginal effect:", driver_labels[[driver_name]]
        ),
        subtitle = paste(
          "ggpredict estimates with HC1 plot-clustered 95% confidence intervals",
          "at one common covariate reference."
        ),
        x = driver_axis_labels[[driver_name]],
        y = paste("Predicted", response_labels[[response_name]]),
        caption = paste(
          "Other agent mortalities are fixed at 0; site CWD and survey duration",
          "are fixed at their pooled-history medians when they are not focal."
        )
      ) +
      raw_theme

    effect_path <- file.path(
      effect_figure_dir,
      paste0(
        tolower(response_name), "__", driver_slugs[[driver_name]], ".png"
      )
    )
    ggsave(
      effect_path, effect_plot,
      width = 10.5, height = 7.2, dpi = 150, bg = "white"
    )
    figure_manifest_parts[[paste0("effect_", figure_counter)]] <- data.table(
      section = "marginal_effect",
      response = tolower(response_name),
      response_label = response_labels[[response_name]],
      driver = driver_name,
      driver_label = driver_labels[[driver_name]],
      figure_path = effect_path,
      caption = paste(
        "HC1 plot-clustered ggpredict estimates for",
        paste(unname(group_labels), collapse = ", ")
      ),
      plot_order = 2L * figure_counter,
      panels = 4L,
      uncertainty = "HC1 standard errors clustered by stable_plot_id"
    )
  }
}

figure_manifest <- rbindlist(figure_manifest_parts)
setorder(figure_manifest, response, section, plot_order)
figure_manifest_path <- file.path(figure_dir, "figure_manifest.csv")
fwrite(figure_manifest, figure_manifest_path)

writeLines(c(
  "# Preliminary full-report figures",
  "",
  "These 24 figures are generated for Joan's long preliminary-results report:",
  "12 raw complete-case relationships with descriptive GAMs and 12 adjusted",
  "marginal-effect figures created with `ggeffects::ggpredict()`.",
  "",
  "Every figure has four panels in this order: Seedlings, Saplings, Adults, and",
  "Pooled all live. See `figure_manifest.csv` for response, driver, method, and",
  "provenance. Run `09_analysis/scripts/08_fit_preliminary_models_and_report.R`",
  "to regenerate the figures and self-contained HTML."
), file.path(figure_dir, "README.md"))

writeLines(c(
  "# Preliminary full-report model tables",
  "",
  "These three four-column tables are generated with `sjPlot::tab_model()` and",
  "contain Seedlings, Saplings, Adults, and Pooled all live models. The",
  "`*_fragment.html` files are embedded in the long report; the other HTML files",
  "are standalone versions. See `table_manifest.csv` for provenance."
), file.path(table_dir, "README.md"))

report_output <- file.path(
  report_dir, "preliminary_results.html"
)
Sys.setenv(ANALYSIS_RUN_DIR = normalizePath(
  run_dir, winslash = "/", mustWork = TRUE
))
rmarkdown::render(
  input = report_source,
  output_file = basename(report_output),
  output_dir = report_dir,
  knit_root_dir = normalizePath(".", winslash = "/", mustWork = TRUE),
  envir = new.env(parent = globalenv()),
  clean = TRUE,
  quiet = FALSE
)

stopifnot(file.exists(report_output), file.info(report_output)$size > 100000L)
report_text <- paste(readLines(report_output, warn = FALSE), collapse = "\n")
report_visible_text <- gsub("<[^>]+>", " ", report_text)
report_visible_text <- gsub("[[:space:]]+", " ", report_visible_text)
required_report_text <- c(
  "Seedlings", "Saplings", "Adults", "Pooled all live",
  "Raw relationships", "Adjusted marginal effects",
  "Cumulative fire mortality", "Cumulative insect mortality",
  "Cumulative disease mortality", "Cumulative site CWD"
)
stopifnot(all(vapply(
  required_report_text,
  grepl,
  logical(1),
  x = report_visible_text,
  fixed = TRUE
)))
embedded_pngs <- lengths(regmatches(
  report_text,
  gregexpr("data:image/png;base64,", report_text, fixed = TRUE)
))
stopifnot(embedded_pngs >= 24L)

report_manifest <- data.table(
  run_id = run_id,
  report_file = report_output,
  source_file = report_source,
  producer_script = file.path(
    "09_analysis", "scripts", "08_fit_preliminary_models_and_report.R"
  ),
  generated_at_utc = format(Sys.time(), "%Y-%m-%dT%H:%M:%SZ", tz = "UTC"),
  n_models = length(models),
  n_model_tables = nrow(table_manifest),
  n_raw_figures = figure_manifest[section == "raw_relationship", .N],
  n_marginal_effect_figures = figure_manifest[section == "marginal_effect", .N],
  n_embedded_pngs = embedded_pngs,
  report_size_bytes = file.info(report_output)$size,
  sjplot_version = as.character(packageVersion("sjPlot")),
  ggeffects_version = as.character(packageVersion("ggeffects")),
  covariance = "HC1 clustered by stable_plot_id",
  four_plotted_drivers = paste(drivers, collapse = ";"),
  retained_control = "full_survey_period_years"
)
report_manifest_path <- file.path(
  report_dir, "manifest.csv"
)
fwrite(report_manifest, report_manifest_path)

model_formulas <- data.table(
  model_id = names(models),
  formula = vapply(models, function(model) {
    paste(deparse(formula(model)), collapse = " ")
  }, character(1))
)
fwrite(model_formulas, file.path(run_dir, "model_formulas.csv"))

writeLines(c(
  paste0("# ", run_id),
  "",
  "Twelve preliminary linear models: three CWM responses by seedlings,",
  "saplings, adults, and the pooled live community.",
  "",
  "- `preliminary_results.html`: self-contained report to review or send",
  "- `coefficients.csv`: estimates, clustered HC1 uncertainty, and p-values",
  "- `model_fit.csv`: sample sizes and R-squared values",
  "- `sample_flow.csv`: complete-case counts",
  "- `tables/`: sjPlot model tables",
  "- `figures/`: raw relationships and ggeffects marginal predictions"
), file.path(run_dir, "README.md"))

message("Wrote Joan's self-contained preliminary full report: ", report_output)
