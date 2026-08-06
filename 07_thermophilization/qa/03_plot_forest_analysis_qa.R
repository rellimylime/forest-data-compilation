#!/usr/bin/env Rscript

# Compact review figures for the forest-condition, pairing, fire, and insect QA.

suppressPackageStartupMessages({
  library(here)
  library(data.table)
  library(arrow)
  library(dplyr)
  library(ggplot2)
  library(fs)
})

qa_dir <- here("07_thermophilization/qa/outputs")
figure_dir <- here("07_thermophilization/qa/figures")
link_dir <- here("08_disturbance_linkage/data/processed")
dir_create(figure_dir)

theme_set(theme_minimal(base_size = 11))

forest_visits <- fread(file.path(
  here("05_fia/qa/outputs"),
  "forested_condition_visit_qa.csv"
))
forest_plot <- ggplot(
  forest_visits[!is.na(forested_plot_proportion)],
  aes(forested_plot_proportion)
) +
  geom_histogram(binwidth = 0.025, boundary = 0, color = "white") +
  labs(
    x = "Forested proportion of FIA plot visit",
    y = "Plot visits",
    title = "Forested area retained as a continuous visit attribute"
  )
ggsave(
  file.path(figure_dir, "forested_plot_proportion_distribution.png"),
  forest_plot,
  width = 7,
  height = 4,
  dpi = 160
)

pairing <- as.data.table(read_parquet(file.path(
  link_dir,
  "fia_visit_pairing_audit.parquet"
)))
pairing_counts <- pairing[, .N, by = pairing_class][order(N)]
pairing_counts[, pairing_class := factor(
  pairing_class,
  levels = pairing_class
)]
pairing_plot <- ggplot(pairing_counts, aes(N, pairing_class)) +
  geom_col() +
  geom_text(aes(label = format(N, big.mark = ",")), hjust = -0.05, size = 3) +
  scale_x_continuous(
    labels = scales::label_number(big.mark = ","),
    expand = expansion(mult = c(0, 0.18))
  ) +
  labs(
    x = "Current FIA visits",
    y = NULL,
    title = "Official and fallback FIA visit-pairing classes"
  )
ggsave(
  file.path(figure_dir, "fia_visit_pairing_classes.png"),
  pairing_plot,
  width = 8,
  height = 4.5,
  dpi = 160
)

fire <- as.data.table(read_parquet(
  file.path(link_dir, "fia_forest_disturbance_measures.parquet"),
  col_select = c(
    "prop_any_fire_of_forested",
    "prop_crown_fire_of_forested"
  )
))
fire_long <- melt(
  fire[
    prop_any_fire_of_forested > 0 |
      prop_crown_fire_of_forested > 0
  ],
  measure.vars = c(
    "prop_any_fire_of_forested",
    "prop_crown_fire_of_forested"
  ),
  variable.name = "measure",
  value.name = "proportion"
)
fire_long <- fire_long[proportion > 0]
fire_long[, measure := fcase(
  measure == "prop_any_fire_of_forested", "Any FIA fire",
  default = "FIA crown fire"
)]
fire_plot <- ggplot(fire_long, aes(proportion)) +
  geom_histogram(binwidth = 0.025, boundary = 0, color = "white") +
  facet_wrap(~measure, ncol = 1, scales = "free_y") +
  labs(
    x = "Proportion of forested area",
    y = "Plot visits with positive extent",
    title = "FIA fire extent remains continuous"
  )
ggsave(
  file.path(figure_dir, "fia_fire_extent_distributions.png"),
  fire_plot,
  width = 7,
  height = 6,
  dpi = 160
)

insect <- open_dataset(
  file.path(link_dir, "fia_insect_severity_candidates")
) |>
  select(
    tree_record_fraction,
    tpa_unadj_fraction,
    basal_area_fraction
  ) |>
  collect() |>
  as.data.table()
insect_long <- melt(
  insect,
  measure.vars = names(insect),
  variable.name = "measure",
  value.name = "fraction"
)
insect_long[, measure := factor(
  measure,
  levels = c(
    "tree_record_fraction",
    "tpa_unadj_fraction",
    "basal_area_fraction"
  ),
  labels = c("Tree records", "Stem abundance", "Basal area")
)]
insect_plot <- ggplot(insect_long, aes(fraction)) +
  geom_histogram(binwidth = 0.02, boundary = 0, color = "white") +
  facet_wrap(~measure, ncol = 1, scales = "free_y") +
  labs(
    x = "Affected fraction",
    y = "Condition-agent rows",
    title = "Candidate FIA insect impact measures"
  )
ggsave(
  file.path(figure_dir, "fia_insect_candidate_distributions.png"),
  insect_plot,
  width = 7,
  height = 7,
  dpi = 160
)

cat("Wrote QA figures to ", figure_dir, "\n", sep = "")
