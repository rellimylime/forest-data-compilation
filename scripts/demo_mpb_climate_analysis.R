# ==============================================================================
# Mountain Pine Beetle x Climate — Example Analysis
# ==============================================================================
#
# Demonstrates how the compiled datasets from this project can be used:
#
#   Step 1  Load IDS disturbance data (01_ids workflow)
#   Step 2  Open climate summaries (02_terraclimate / 03_prism / 04_worldclim)
#   Step 3  Extract climate conditions at MPB damage sites
#   Step 4  Build annual outbreak + climate summaries
#   Step 5  Generate figures
#
# Usage:
#   Rscript scripts/demo_mpb_climate_analysis.R                  # terraclimate (default)
#   Rscript scripts/demo_mpb_climate_analysis.R prism
#   Rscript scripts/demo_mpb_climate_analysis.R worldclim
#
# Prerequisites (data must be compiled first):
#   01_ids/data/processed/ids_layers_cleaned.gpkg
#   processed/climate/<dataset>/damage_areas_summaries/
#
# Output: output/demo_mpb_<dataset>/  (3 PNG figures)
# ==============================================================================

library(here)
library(dplyr)
library(tidyr)
library(arrow)
library(ggplot2)
library(sf)
library(scales)

# Dataset is set by command-line argument; defaults to terraclimate
args    <- commandArgs(trailingOnly = TRUE)
dataset <- if (length(args) >= 1) args[1] else "terraclimate"

if (!dataset %in% c("terraclimate", "prism", "worldclim")) {
  stop("Unknown dataset '", dataset, "'. Choose: terraclimate, prism, worldclim")
}

# Variable names differ across datasets but map to the same three concepts
var_map <- list(
  terraclimate = list(tmax = "tmmx", tmin = "tmmn", precip = "pr"),
  prism        = list(tmax = "tmax", tmin = "tmin", precip = "ppt"),
  worldclim    = list(tmax = "tmax", tmin = "tmin", precip = "prec")
)
vars <- var_map[[dataset]]

output_dir <- here("output", paste0("demo_mpb_", dataset))
dir.create(output_dir, recursive = TRUE, showWarnings = FALSE)

cat(sprintf("Dataset: %s  (tmax=%s  tmin=%s  precip=%s)\n",
            dataset, vars$tmax, vars$tmin, vars$precip))

# ==============================================================================
# Step 1: Load IDS disturbance data — Mountain Pine Beetle only
# ==============================================================================
#
# The compiled IDS file holds 4.4 million damage observations (all agents,
# all years). A SQL query at read time filters to Mountain Pine Beetle
# (DCA_CODE = 11006), so only the ~1.3 million relevant rows are loaded.
#

mpb <- st_read(
  here("01_ids/data/processed/ids_layers_cleaned.gpkg"),
  layer = "damage_areas",
  query = "SELECT DAMAGE_AREA_ID, SURVEY_YEAR, ACRES
           FROM damage_areas
           WHERE DCA_CODE = 11006",
  quiet = TRUE
) %>% st_drop_geometry()

cat(sprintf("MPB observations loaded: %s  (%d-%d)\n",
            format(nrow(mpb), big.mark = ","),
            min(mpb$SURVEY_YEAR), max(mpb$SURVEY_YEAR)))

# ==============================================================================
# Step 2: Open the climate summaries
# ==============================================================================
#
# Climate summaries are stored as partitioned parquet files — one file per
# variable, covering all 4.4M damage areas across all years. open_dataset()
# connects without loading data into memory; queries are executed lazily.
#
# Each row: DAMAGE_AREA_ID | variable | water_year | calendar_month | weighted_mean | ...
#

climate <- open_dataset(
  here("processed/climate", dataset, "damage_areas_summaries")
)

# ==============================================================================
# Step 3: Extract climate at MPB sites and aggregate to water year
# ==============================================================================
#
# For each MPB damage area, compute three water-year summaries:
#   tmax_c    = peak maximum temperature       (°C)
#   tmin_c    = lowest minimum temperature     (°C)
#   precip_mm = total annual precipitation     (mm)
#
# The join is performed inside Arrow before collecting, so only MPB rows
# are pulled into memory (not the full 4.4M-area dataset).
#

mpb_lookup <- mpb %>%
  distinct(DAMAGE_AREA_ID = as.character(DAMAGE_AREA_ID),
           water_year     = as.integer(SURVEY_YEAR)) %>%
  arrow_table()

get_climate <- function(var_name, value_col, agg_fn, out_col) {
  climate %>%
    filter(variable == var_name) %>%
    select(DAMAGE_AREA_ID, water_year, value = !!sym(value_col)) %>%
    mutate(DAMAGE_AREA_ID = cast(DAMAGE_AREA_ID, utf8())) %>%
    inner_join(mpb_lookup, by = c("DAMAGE_AREA_ID", "water_year")) %>%
    collect() %>%
    group_by(DAMAGE_AREA_ID, water_year) %>%
    summarize(!!out_col := agg_fn(value, na.rm = TRUE), .groups = "drop")
}

climate_mpb <-
  get_climate(vars$tmax,   "value_max",    max, "tmax_c") %>%
  left_join(get_climate(vars$tmin,   "value_min",    min, "tmin_c"),    by = c("DAMAGE_AREA_ID", "water_year")) %>%
  left_join(get_climate(vars$precip, "weighted_mean", sum, "precip_mm"), by = c("DAMAGE_AREA_ID", "water_year"))

# ==============================================================================
# Step 4: Build annual summaries
# ==============================================================================

annual <- mpb %>%
  group_by(SURVEY_YEAR) %>%
  summarize(total_acres = sum(ACRES, na.rm = TRUE),
            n_obs       = n(),
            .groups     = "drop") %>%
  left_join(
    climate_mpb %>%
      group_by(SURVEY_YEAR = water_year) %>%
      summarize(tmax_c    = mean(tmax_c,    na.rm = TRUE),
                tmin_c    = mean(tmin_c,    na.rm = TRUE),
                precip_mm = mean(precip_mm, na.rm = TRUE),
                .groups   = "drop"),
    by = "SURVEY_YEAR"
  )

write.csv(annual, file.path(output_dir, "annual_summary.csv"), row.names = FALSE)

# ==============================================================================
# Step 5: Figures
# ==============================================================================

# -- Figure 1: MPB damage extent over time ------------------------------------

p1 <- ggplot(annual, aes(x = SURVEY_YEAR, y = total_acres / 1e6)) +
  geom_col(fill = "#8B4513", alpha = 0.85) +
  scale_x_continuous(breaks = seq(1997, 2024, 3)) +
  scale_y_continuous(labels = label_comma(suffix = "M")) +
  labs(
    title   = "Mountain Pine Beetle Damage Extent, 1997-2024",
    x       = "Survey Year",
    y       = "Affected Acres (millions)",
    caption = "Source: USDA Forest Service Insect and Disease Survey (IDS)"
  ) +
  theme_minimal(base_size = 13) +
  theme(plot.title = element_text(face = "bold"), panel.grid.minor = element_blank())

ggsave(file.path(output_dir, "01_outbreak_timeline.png"),
       p1, width = 10, height = 5, dpi = 150)

# -- Figure 2: Climate trends at MPB sites over time --------------------------

climate_labels <- c(
  tmax_c    = "Peak Max Temp (°C)",
  tmin_c    = "Lowest Min Temp (°C)",
  precip_mm = "Total Precipitation (mm)"
)

p2 <- annual %>%
  select(SURVEY_YEAR, tmax_c, tmin_c, precip_mm) %>%
  pivot_longer(-SURVEY_YEAR, names_to = "variable", values_to = "value") %>%
  mutate(variable = factor(variable, names(climate_labels), climate_labels)) %>%
  ggplot(aes(x = SURVEY_YEAR, y = value)) +
  geom_line(color = "#8B4513", linewidth = 0.9) +
  geom_point(color = "#8B4513", size = 2) +
  facet_wrap(~variable, ncol = 1, scales = "free_y") +
  scale_x_continuous(breaks = seq(1997, 2024, 3)) +
  labs(
    title   = sprintf("Water-Year Climate at MPB Damage Sites (%s)", toupper(dataset)),
    x       = "Water Year",
    caption = sprintf("Source: IDS + %s", toupper(dataset))
  ) +
  theme_minimal(base_size = 13) +
  theme(
    plot.title       = element_text(face = "bold"),
    panel.grid.minor = element_blank(),
    strip.text       = element_text(face = "bold"),
    axis.title.y     = element_blank()
  )

ggsave(file.path(output_dir, "02_climate_timeseries.png"),
       p2, width = 10, height = 8, dpi = 150)

# -- Figure 3: Outbreak severity vs climate -----------------------------------

p3 <- annual %>%
  select(SURVEY_YEAR, total_acres, tmax_c, tmin_c, precip_mm) %>%
  pivot_longer(c(tmax_c, tmin_c, precip_mm), names_to = "variable", values_to = "value") %>%
  mutate(variable = factor(variable, names(climate_labels), climate_labels)) %>%
  ggplot(aes(x = value, y = total_acres / 1e6)) +
  geom_point(color = "#8B4513", size = 2.5, alpha = 0.75) +
  geom_smooth(method = "lm", se = TRUE, color = "#D2691E", linewidth = 0.8) +
  facet_wrap(~variable, scales = "free_x") +
  scale_y_continuous(labels = label_comma(suffix = "M")) +
  labs(
    title   = sprintf("MPB Outbreak Severity vs Water-Year Climate (%s)", toupper(dataset)),
    x       = NULL,
    y       = "Affected Acres (millions)",
    caption = sprintf("Source: IDS + %s | Line = linear fit with 95%% CI", toupper(dataset))
  ) +
  theme_minimal(base_size = 13) +
  theme(
    plot.title       = element_text(face = "bold"),
    panel.grid.minor = element_blank(),
    strip.text       = element_text(face = "bold")
  )

ggsave(file.path(output_dir, "03_outbreak_vs_climate.png"),
       p3, width = 12, height = 5, dpi = 150)

cat(sprintf("\nFigures saved to %s/\n", output_dir))
cat("  01_outbreak_timeline.png   — MPB damage acres by year\n")
cat("  02_climate_timeseries.png  — temperature and precip at MPB sites over time\n")
cat("  03_outbreak_vs_climate.png — outbreak severity vs water-year climate\n")

# ==============================================================================
# Appendix: FIA plot filtering and site-level climate extraction
# ==============================================================================
#
# The following examples show two common operations for FIA-based analyses:
#   A. Filter out non-forested / human-disturbed plots before analysis
#   B. Query long-term TerraClimate data at FIA site locations
#
# These require the FIA pipeline to have been run through
# 05_build_fia_summaries.R (for A) and 06_extract_site_climate.R (for B).
# ==============================================================================

# ------------------------------------------------------------------------------
# A. FIA clean-plot filtering using plot_exclusion_flags.parquet
# ------------------------------------------------------------------------------
#
# ~12% of FIA plots have been deforested or are non-forested.  Most analyses
# should remove these plots, plus any with human-induced disturbance (DSTRBCD 80).
# Use the pre-built exclusion flags parquet for a single join:
#
# Key flags:
#   exclude_nonforest   — COND_STATUS_CD != 1 in any condition (deforested)
#   exclude_human_dist  — DSTRBCD 80 in any slot (human-induced, e.g. logging)
#   exclude_harvest     — TRTCD 10 in any slot (cutting treatment)
#   exclude_any         — OR of all three above  ← use this for standard cleaning
#   has_fire            — DSTRBCD 30/31/32        ← identify burned plots
#   has_insect          — DSTRBCD 10/11/12        ← identify insect-damaged plots

if (FALSE) {  # Set to TRUE once FIA pipeline has been run

  flags <- read_parquet(
    here("05_fia/data/processed/summaries/plot_exclusion_flags.parquet")
  )

  tree_metrics <- read_parquet(
    here("05_fia/data/processed/summaries/plot_tree_metrics.parquet")
  )

  # Standard filter: remove deforested, human-disturbed, and harvested plots
  clean_plots <- tree_metrics |>
    inner_join(
      flags |> filter(!exclude_any) |> select(PLT_CN, INVYR),
      by = c("PLT_CN", "INVYR")
    )

  cat(sprintf("Original: %s plot-years\n", format(nrow(tree_metrics), big.mark = ",")))
  cat(sprintf("After clean filter: %s plot-years (%.1f%% retained)\n",
              format(nrow(clean_plots), big.mark = ","),
              100 * nrow(clean_plots) / nrow(tree_metrics)))

  # Separately: plots that burned (for fire-effects analyses)
  burned_plots <- tree_metrics |>
    inner_join(
      flags |> filter(has_fire) |> select(PLT_CN, INVYR, has_fire),
      by = c("PLT_CN", "INVYR")
    )

  cat(sprintf("Burned plots: %s plot-years\n", format(nrow(burned_plots), big.mark = ",")))
}

# ------------------------------------------------------------------------------
# B. FIA site-level TerraClimate (1958-present)
# ------------------------------------------------------------------------------
#
# fia_site_climate.parquet contains monthly tmmx, tmmn, pr, def, pet, aet for
# all sites in all_site_locations.csv.  The 'def' variable is climate water
# deficit (CWD = PET - AET) — a key predictor of drought stress and disturbance.
#
# Schema: site_id | year | month | water_year | water_year_month | variable | value

if (FALSE) {  # Set to TRUE once 06_extract_site_climate.R has been run

  site_clim <- read_parquet(
    here("05_fia/data/processed/site_climate/fia_site_climate.parquet")
  )

  cat(sprintf("Site climate rows: %s\n", format(nrow(site_clim), big.mark = ",")))
  cat(sprintf("Sites: %d  |  Years: %d-%d  |  Variables: %s\n",
              n_distinct(site_clim$site_id),
              min(site_clim$year), max(site_clim$year),
              paste(unique(site_clim$variable), collapse = ", ")))

  # Annual water-year CWD (deficit) per site
  annual_cwd <- site_clim |>
    filter(variable == "def") |>
    group_by(site_id, water_year) |>
    summarise(cwd_mm = sum(value, na.rm = TRUE), .groups = "drop")

  # Summer (JJA) mean max temperature per site-year
  summer_tmax <- site_clim |>
    filter(variable == "tmmx", month %in% 6:8) |>
    group_by(site_id, year) |>
    summarise(tmax_jja_c = mean(value, na.rm = TRUE), .groups = "drop")

  # Long-term mean annual CWD per site (all years available)
  ltm_cwd <- annual_cwd |>
    group_by(site_id) |>
    summarise(cwd_ltm_mm = mean(cwd_mm, na.rm = TRUE), .groups = "drop")

  cat(sprintf("Annual CWD computed for %d site-years\n",
              nrow(annual_cwd)))
  cat(sprintf("Long-term mean CWD range: %.0f - %.0f mm\n",
              min(ltm_cwd$cwd_ltm_mm, na.rm = TRUE),
              max(ltm_cwd$cwd_ltm_mm, na.rm = TRUE)))
}
