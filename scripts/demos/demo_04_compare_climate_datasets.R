# ==============================================================================
# Cross-Dataset Comparison — MPB Climate Analysis
# ==============================================================================
#
# Plots TerraClimate, PRISM, and WorldClim results on the same axes so the
# three datasets can be directly compared.
#
# Prerequisites — all three demo runs must have completed first:
#   Rscript scripts/demo_mpb_climate_analysis.R terraclimate
#   Rscript scripts/demo_mpb_climate_analysis.R prism
#   Rscript scripts/demo_mpb_climate_analysis.R worldclim
#
# Output: output/demo_mpb_comparison/  (2 PNG figures)
# ==============================================================================

library(here)
library(dplyr)
library(tidyr)
library(ggplot2)
library(scales)

output_dir <- here("output/demo_mpb_comparison")
dir.create(output_dir, recursive = TRUE, showWarnings = FALSE)

# Load the annual summary saved by each demo run
datasets <- c("terraclimate", "prism", "worldclim")

annual <- lapply(datasets, function(ds) {
  path <- here("output", paste0("demo_mpb_", ds), "annual_summary.csv")
  if (!file.exists(path)) {
    stop("Missing: ", path, "\nRun first:  Rscript scripts/demo_mpb_climate_analysis.R ", ds)
  }
  read.csv(path) %>% mutate(dataset = ds)
}) %>%
  bind_rows() %>%
  mutate(dataset = factor(dataset, levels = datasets))

# Note: TerraClimate uses WorldClim climatological normals as its
# bias-correction baseline, so the two datasets share a common reference
# and typically agree within ~5% for precipitation and fractions of a
# degree for temperature. Close agreement between TerraClimate and
# WorldClim lines is expected. PRISM uses an independent orographic
# model and will diverge more noticeably, particularly at high-elevation
# sites where orographic precipitation enhancement is significant.
dataset_colors <- c(terraclimate = "#2166AC", prism = "#1A9641", worldclim = "#D73027")

climate_labels <- c(
  tmax_c    = "Peak Max Temp (°C)",
  tmin_c    = "Lowest Min Temp (°C)",
  precip_mm = "Total Precipitation (mm)"
)

# ==============================================================================
# Figure 1: Climate time series — all datasets on the same axes
# ==============================================================================

p1 <- annual %>%
  select(SURVEY_YEAR, dataset, tmax_c, tmin_c, precip_mm) %>%
  pivot_longer(c(tmax_c, tmin_c, precip_mm), names_to = "variable", values_to = "value") %>%
  mutate(variable = factor(variable, names(climate_labels), climate_labels)) %>%
  ggplot(aes(x = SURVEY_YEAR, y = value, color = dataset)) +
  geom_line(linewidth = 0.9) +
  geom_point(size = 1.5) +
  facet_wrap(~variable, ncol = 1, scales = "free_y") +
  scale_x_continuous(breaks = seq(1997, 2024, 3)) +
  scale_color_manual(values = dataset_colors, name = NULL) +
  labs(
    title   = "Water-Year Climate at MPB Damage Sites — Dataset Comparison",
    x       = "Water Year",
    caption = "Source: IDS + TerraClimate / PRISM / WorldClim"
  ) +
  theme_minimal(base_size = 13) +
  theme(
    plot.title       = element_text(face = "bold"),
    panel.grid.minor = element_blank(),
    strip.text       = element_text(face = "bold"),
    axis.title.y     = element_blank(),
    legend.position  = "bottom"
  )

ggsave(file.path(output_dir, "01_climate_comparison.png"),
       p1, width = 10, height = 9, dpi = 150)

# ==============================================================================
# Figure 2: Outbreak severity vs climate — all datasets on the same axes
# ==============================================================================

p2 <- annual %>%
  select(SURVEY_YEAR, dataset, total_acres, tmax_c, tmin_c, precip_mm) %>%
  pivot_longer(c(tmax_c, tmin_c, precip_mm), names_to = "variable", values_to = "value") %>%
  mutate(variable = factor(variable, names(climate_labels), climate_labels)) %>%
  ggplot(aes(x = value, y = total_acres / 1e6, color = dataset)) +
  geom_point(size = 2, alpha = 0.7) +
  geom_smooth(method = "lm", se = FALSE, linewidth = 0.8) +
  facet_wrap(~variable, scales = "free_x") +
  scale_y_continuous(labels = label_comma(suffix = "M")) +
  scale_color_manual(values = dataset_colors, name = NULL) +
  labs(
    title   = "MPB Outbreak Severity vs Climate — Dataset Comparison",
    x       = NULL,
    y       = "Affected Acres (millions)",
    caption = "Source: IDS + TerraClimate / PRISM / WorldClim | Lines = linear fit"
  ) +
  theme_minimal(base_size = 13) +
  theme(
    plot.title       = element_text(face = "bold"),
    panel.grid.minor = element_blank(),
    strip.text       = element_text(face = "bold"),
    legend.position  = "bottom"
  )

ggsave(file.path(output_dir, "02_outbreak_vs_climate_comparison.png"),
       p2, width = 12, height = 5, dpi = 150)

cat("\nFigures saved to output/demo_mpb_comparison/\n")
cat("  01_climate_comparison.png              — climate time series, all datasets\n")
cat("  02_outbreak_vs_climate_comparison.png  — outbreak severity vs climate, all datasets\n")
