# ==============================================================================
# scripts/demo_mpb_climate_analysis.R
#
# Mountain Pine Beetle (Dendroctonus ponderosae) x Climate Conditions
#
# Explores the relationship between MPB outbreak severity and water-year
# climate using three variables common to all climate datasets:
#   - tmax  : peak maximum temperature (max of monthly value_max)
#   - tmin  : lowest minimum temperature (min of monthly value_min)
#   - precip: total annual precipitation (sum of monthly weighted_mean)
#
# Usage:
#   Rscript scripts/demo_mpb_climate_analysis.R                   # default: terraclimate
#   Rscript scripts/demo_mpb_climate_analysis.R prism
#   Rscript scripts/demo_mpb_climate_analysis.R worldclim
#
# Run all three and compare output/ directories to validate dataset agreement.
#
# Variable name mapping across datasets:
#   concept  | terraclimate | prism | worldclim
#   ---------|--------------|-------|----------
#   tmax     | tmmx         | tmax  | tmax
#   tmin     | tmmn         | tmin  | tmin
#   precip   | pr           | ppt   | prec
#
# Output: output/demo_mpb_<dataset>/
# ==============================================================================

library(here)
library(dplyr)
library(tidyr)
library(arrow)
library(ggplot2)
library(sf)
library(scales)

options(warn = 1)

tick <- function(t0, label = "") {
  elapsed <- as.numeric(difftime(Sys.time(), t0, units = "secs"))
  cat(sprintf("  [%.1fs] %s\n", elapsed, label))
  invisible(Sys.time())
}

# ==============================================================================
# Configuration
# ==============================================================================

args    <- commandArgs(trailingOnly = TRUE)
dataset <- if (length(args) >= 1) args[1] else "terraclimate"

valid_datasets <- c("terraclimate", "prism", "worldclim")
if (!dataset %in% valid_datasets) {
  stop("Unknown dataset: '", dataset, "'. Choose from: ",
       paste(valid_datasets, collapse = ", "))
}

# Variable name for each concept in each dataset
var_map <- list(
  terraclimate = list(tmax = "tmmx",  tmin = "tmmn",  precip = "pr"),
  prism        = list(tmax = "tmax",  tmin = "tmin",  precip = "ppt"),
  worldclim    = list(tmax = "tmax",  tmin = "tmin",  precip = "prec")
)
vars <- var_map[[dataset]]

summaries_path <- here("processed/climate", dataset, "damage_areas_summaries")
MPB_CODE       <- 11006
output_dir     <- here("output", paste0("demo_mpb_", dataset))
dir.create(output_dir, recursive = TRUE, showWarnings = FALSE)

cat("\n================================================================\n")
cat(sprintf(" Mountain Pine Beetle x Climate | dataset: %s\n", toupper(dataset)))
cat("================================================================\n\n")
cat(sprintf("  tmax   -> %s\n", vars$tmax))
cat(sprintf("  tmin   -> %s\n", vars$tmin))
cat(sprintf("  precip -> %s\n", vars$precip))
if (dataset == "prism") {
  cat("  NOTE: PRISM is CONUS-only; Alaskan MPB observations will be absent\n")
}
cat("\n")

script_start <- Sys.time()

# ==============================================================================
# Step 1: Load IDS MPB observations
# ==============================================================================

cat("Step 1: Loading MPB observations from GeoPackage...\n")
t0 <- Sys.time()

ids <- st_read(
  here("01_ids/data/processed/ids_layers_cleaned.gpkg"),
  layer = "damage_areas",
  query = sprintf("SELECT * FROM damage_areas WHERE DCA_CODE = %d", MPB_CODE),
  quiet = TRUE
)

tick(t0, sprintf("%s observations loaded", format(nrow(ids), big.mark = ",")))
cat(sprintf("  Year range : %d-%d\n", min(ids$SURVEY_YEAR), max(ids$SURVEY_YEAR)))
cat(sprintf("  Unique IDs : %s\n", format(length(unique(ids$DAMAGE_AREA_ID)), big.mark = ",")))
cat(sprintf("  Acres range: %.0f - %s\n",
            min(ids$ACRES, na.rm = TRUE),
            format(max(ids$ACRES, na.rm = TRUE), big.mark = ",")))

# ==============================================================================
# Step 2: Load climate summaries — join MPB lookup into Arrow BEFORE groupby
# ==============================================================================
# Strategy: build a small lookup of (DAMAGE_AREA_ID, water_year) for MPB obs,
# join it into each Arrow query so only MPB rows enter the groupby.
# This avoids grouping all 4.4M damage areas (OOM with Arrow).
# ==============================================================================

cat(sprintf("\nStep 2: Loading climate summaries (%s)...\n", dataset))

if (!dir.exists(summaries_path)) {
  stop("Summaries not found: ", summaries_path,
       "\nRun: Rscript scripts/build_climate_summaries.R ", dataset)
}

ds <- open_dataset(summaries_path)
cat(sprintf("  Variables in dataset: %s\n",
            paste(ds %>% distinct(variable) %>% pull() %>% sort(), collapse = ", ")))

# Build MPB lookup: one row per unique (DAMAGE_AREA_ID, water_year)
ids_attrs <- ids %>%
  st_drop_geometry() %>%
  select(DAMAGE_AREA_ID, SURVEY_YEAR, ACRES) %>%
  distinct()

mpb_lookup <- ids_attrs %>%
  distinct(DAMAGE_AREA_ID, water_year = SURVEY_YEAR) %>%
  mutate(DAMAGE_AREA_ID = as.character(DAMAGE_AREA_ID),
         water_year     = as.integer(water_year)) %>%
  arrow_table()

cat(sprintf("  MPB lookup: %s unique (DAMAGE_AREA_ID, water_year) pairs\n",
            format(nrow(mpb_lookup), big.mark = ",")))

# Helper: extract one climate concept per MPB area per water year
extract_concept <- function(var_name, concept, agg_col, agg_fn) {
  cat(sprintf("  %s [%s]: %s per water year...\n", concept, var_name, agg_col))
  t0 <- Sys.time()

  result <- ds %>%
    filter(variable == var_name) %>%
    select(DAMAGE_AREA_ID, water_year, value = !!sym(agg_col)) %>%
    mutate(DAMAGE_AREA_ID = cast(DAMAGE_AREA_ID, utf8())) %>%
    inner_join(mpb_lookup, by = c("DAMAGE_AREA_ID", "water_year")) %>%
    group_by(DAMAGE_AREA_ID, water_year) %>%
    collect() %>%
    summarize(wy_value = agg_fn(value), .groups = "drop") %>%
    mutate(concept = concept)

  tick(t0, sprintf("%s: %s rows | range %.2f to %.2f",
                   concept, format(nrow(result), big.mark = ","),
                   min(result$wy_value, na.rm = TRUE),
                   max(result$wy_value, na.rm = TRUE)))
  result
}

tmax_wy   <- extract_concept(vars$tmax,   "tmax",   "value_max",    function(x) max(x,  na.rm = TRUE))
tmin_wy   <- extract_concept(vars$tmin,   "tmin",   "value_min",    function(x) min(x,  na.rm = TRUE))
precip_wy <- extract_concept(vars$precip, "precip", "weighted_mean",function(x) sum(x,  na.rm = TRUE))

all_wy <- bind_rows(tmax_wy, tmin_wy, precip_wy)
cat(sprintf("  Combined: %s rows (3 variables x ~%s areas)\n",
            format(nrow(all_wy), big.mark = ","),
            format(nrow(mpb_lookup), big.mark = ",")))

# ==============================================================================
# Step 3: Join ACRES back and sanity check
# ==============================================================================

cat("\nStep 3: Joining ACRES, validating...\n")
t0 <- Sys.time()

wy_climate <- all_wy %>%
  inner_join(ids_attrs, by = c("DAMAGE_AREA_ID", "water_year" = "SURVEY_YEAR"))

tick(t0, sprintf("%s rows after ACRES join", format(nrow(wy_climate), big.mark = ",")))
cat(sprintf("  Concepts  : %s\n", paste(sort(unique(wy_climate$concept)), collapse = ", ")))
cat(sprintf("  Year range: %d-%d\n",
            min(wy_climate$water_year), max(wy_climate$water_year)))

n_na <- sum(is.na(wy_climate$wy_value))
cat(sprintf("  NAs in wy_value: %d %s\n", n_na,
            if (n_na == 0) "(good)" else "<-- WARNING"))

# ==============================================================================
# Step 4: Build annual outbreak + climate summary
# ==============================================================================

cat("\nStep 4: Building annual summaries...\n")
t0 <- Sys.time()

outbreak <- ids %>%
  st_drop_geometry() %>%
  group_by(SURVEY_YEAR) %>%
  summarize(
    total_acres = sum(ACRES, na.rm = TRUE),
    n_obs = n(),
    .groups = "drop"
  )

annual_climate <- wy_climate %>%
  group_by(SURVEY_YEAR = water_year, concept) %>%
  summarize(mean_value = mean(wy_value, na.rm = TRUE), .groups = "drop") %>%
  pivot_wider(names_from = concept, values_from = mean_value)

annual <- outbreak %>%
  inner_join(annual_climate, by = "SURVEY_YEAR")

tick(t0, sprintf("%d annual rows", nrow(annual)))
cat(sprintf("  Columns: %s\n", paste(names(annual), collapse = ", ")))
print(as.data.frame(head(annual, 5)))

# ==============================================================================
# Figure 1: MPB outbreak timeline
# ==============================================================================

cat("\nGenerating figures...\n")
t0 <- Sys.time()

p1 <- ggplot(outbreak, aes(x = SURVEY_YEAR, y = total_acres / 1e6)) +
  geom_col(fill = "#8B4513", alpha = 0.8) +
  scale_x_continuous(breaks = seq(1997, 2024, by = 3)) +
  scale_y_continuous(labels = label_comma(suffix = "M")) +
  labs(
    title = "Mountain Pine Beetle Damage (1997-2024)",
    x = "Survey Year",
    y = "Total Affected Acres (millions)",
    caption = "Data: USDA Forest Service IDS"
  ) +
  theme_minimal(base_size = 12) +
  theme(plot.title = element_text(face = "bold"), panel.grid.minor = element_blank())

ggsave(file.path(output_dir, "01_mpb_outbreak_timeline.png"),
       p1, width = 10, height = 5, dpi = 150)
tick(t0, "Saved: 01_mpb_outbreak_timeline.png")

# ==============================================================================
# Figure 2: Water-year climate at MPB sites over time (faceted)
# ==============================================================================

concept_labels <- c(
  tmax   = sprintf("Peak Max Temp (\u00b0C)  [%s]",   vars$tmax),
  tmin   = sprintf("Lowest Min Temp (\u00b0C)  [%s]", vars$tmin),
  precip = sprintf("Total Precip (mm)  [%s]",          vars$precip)
)

climate_long <- annual %>%
  pivot_longer(cols = c(tmax, tmin, precip),
               names_to = "concept", values_to = "value") %>%
  mutate(concept = factor(concept,
                          levels = c("tmax", "tmin", "precip"),
                          labels = concept_labels))

t0 <- Sys.time()
p2 <- ggplot(climate_long, aes(x = SURVEY_YEAR, y = value)) +
  geom_line(color = "#8B4513", linewidth = 0.8) +
  geom_point(color = "#8B4513", size = 1.5) +
  facet_wrap(~concept, ncol = 1, scales = "free_y") +
  scale_x_continuous(breaks = seq(1997, 2024, by = 3)) +
  labs(
    title = sprintf("Water-Year Climate at MPB Damage Sites  [%s]", toupper(dataset)),
    x = "Water Year",
    caption = sprintf("Data: IDS + %s", toupper(dataset))
  ) +
  theme_minimal(base_size = 12) +
  theme(
    plot.title       = element_text(face = "bold"),
    panel.grid.minor = element_blank(),
    strip.text       = element_text(face = "bold"),
    axis.title.y     = element_blank()
  )

ggsave(file.path(output_dir, "02_mpb_climate_timeseries.png"),
       p2, width = 10, height = 8, dpi = 150)
tick(t0, "Saved: 02_mpb_climate_timeseries.png")

# ==============================================================================
# Figure 3: Outbreak severity vs climate (scatterplots)
# ==============================================================================

scatter_data <- annual %>%
  pivot_longer(cols = c(tmax, tmin, precip),
               names_to = "concept", values_to = "value") %>%
  mutate(concept = factor(concept,
                          levels = c("tmax", "tmin", "precip"),
                          labels = concept_labels))

t0 <- Sys.time()
p3 <- ggplot(scatter_data, aes(x = value, y = total_acres / 1e6)) +
  geom_point(color = "#8B4513", size = 2.5, alpha = 0.7) +
  geom_smooth(method = "lm", se = TRUE, color = "#D2691E", linewidth = 0.8) +
  facet_wrap(~concept, scales = "free_x") +
  scale_y_continuous(labels = label_comma(suffix = "M")) +
  labs(
    title = sprintf("MPB Outbreak Severity vs Water-Year Climate  [%s]", toupper(dataset)),
    x = NULL,
    y = "Total Affected Acres (millions)",
    caption = sprintf("Data: IDS + %s | Line = linear fit", toupper(dataset))
  ) +
  theme_minimal(base_size = 12) +
  theme(
    plot.title       = element_text(face = "bold"),
    panel.grid.minor = element_blank(),
    strip.text       = element_text(face = "bold")
  )

ggsave(file.path(output_dir, "03_mpb_climate_scatter.png"),
       p3, width = 12, height = 5, dpi = 150)
tick(t0, "Saved: 03_mpb_climate_scatter.png")

# ==============================================================================
# Summary
# ==============================================================================

total_elapsed <- as.numeric(difftime(Sys.time(), script_start, units = "secs"))

cat("\n================================================================\n")
cat(" COMPLETE\n")
cat("================================================================\n")
cat(sprintf("  Dataset        : %s\n", toupper(dataset)))
cat(sprintf("  Variables used : tmax=%s  tmin=%s  precip=%s\n",
            vars$tmax, vars$tmin, vars$precip))
cat(sprintf("  Total runtime  : %.1fs (%.1f min)\n", total_elapsed, total_elapsed / 60))
cat(sprintf("  Output         : %s\n", output_dir))
cat(sprintf("  MPB obs        : %s\n", format(nrow(ids), big.mark = ",")))
cat(sprintf("  Annual rows    : %d (%d-%d)\n",
            nrow(annual), min(annual$SURVEY_YEAR), max(annual$SURVEY_YEAR)))
cat("  Figures:\n")
cat("    01_mpb_outbreak_timeline.png   - Damage acres by year (same for all datasets)\n")
cat("    02_mpb_climate_timeseries.png  - tmax, tmin, precip at MPB sites over time\n")
cat("    03_mpb_climate_scatter.png     - Outbreak severity vs climate\n")
cat("================================================================\n\n")
cat("To compare across datasets, run all three:\n")
cat("  Rscript scripts/demo_mpb_climate_analysis.R terraclimate\n")
cat("  Rscript scripts/demo_mpb_climate_analysis.R prism\n")
cat("  Rscript scripts/demo_mpb_climate_analysis.R worldclim\n\n")
