# ==============================================================================
# scripts/demo_mpb_climate_analysis.R
#
# Demo: Mountain Pine Beetle Outbreaks and Climate Conditions
#
# This script demonstrates how the compiled IDS + TerraClimate dataset can be
# used to explore relationships between forest pest damage and climate.
#
# Focus: Mountain pine beetle (Dendroctonus ponderosae) — the most
# economically significant bark beetle in western North American forests.
# MPB outbreaks are strongly linked to winter temperatures (cold snaps kill
# overwintering larvae) and drought stress (weakens host tree defenses).
#
# Usage:
#   Rscript scripts/demo_mpb_climate_analysis.R
#   # Or source interactively in RStudio
#
# Output: Figures saved to output/demo_mpb/
# ==============================================================================

library(here)
library(dplyr)
library(tidyr)
library(arrow)
library(ggplot2)
library(sf)
library(scales)

# ==============================================================================
# Configuration
# ==============================================================================

# Mountain pine beetle DCA code
MPB_CODE <- 11006

# Output directory for figures
output_dir <- here("output/demo_mpb")
dir.create(output_dir, recursive = TRUE, showWarnings = FALSE)

# IDS lookup tables
dca_lookup <- read.csv(here("01_ids/lookups/dca_code_lookup.csv"))
host_lookup <- read.csv(here("01_ids/lookups/host_code_lookup.csv"))

cat("\n")
cat("================================================================\n")
cat(" Mountain Pine Beetle x Climate: Demo Analysis\n")
cat("================================================================\n\n")

# ==============================================================================
# Step 1: Load IDS damage observations for mountain pine beetle
# ==============================================================================

cat("Step 1: Loading IDS mountain pine beetle observations...\n")

ids <- st_read(
  here("01_ids/data/processed/ids_layers_cleaned.gpkg"),
  layer = "damage_areas",
  query = sprintf(
    "SELECT * FROM damage_areas WHERE DCA_CODE = %d", MPB_CODE
  ),
  quiet = TRUE
)

cat(sprintf("  Total MPB observations: %s\n", format(nrow(ids), big.mark = ",")))
cat(sprintf("  Year range: %d - %d\n", min(ids$SURVEY_YEAR), max(ids$SURVEY_YEAR)))
cat(sprintf("  Regions: %s\n", paste(sort(unique(ids$REGION_ID)), collapse = ", ")))

# Add host species names
ids <- ids %>%
  left_join(host_lookup, by = "HOST_CODE")

# Top host species
top_hosts <- ids %>%
  st_drop_geometry() %>%
  count(HOST_CODE, HOST, sort = TRUE) %>%
  head(8)

cat("\n  Top host species:\n")
for (i in seq_len(nrow(top_hosts))) {
  cat(sprintf("    %-30s  %s observations\n",
              top_hosts$HOST[i], format(top_hosts$n[i], big.mark = ",")))
}

# ==============================================================================
# Step 2: Load TerraClimate summaries for MPB observations
# ==============================================================================

cat("\nStep 2: Loading TerraClimate summaries...\n")

# Get unique DAMAGE_AREA_IDs for MPB observations
mpb_damage_area_ids <- unique(ids$DAMAGE_AREA_ID)
cat(sprintf("  Unique MPB damage areas: %s\n",
            format(length(mpb_damage_area_ids), big.mark = ",")))

# Open the summaries dataset and filter to our observations + key variables
# Focus on: tmmn (winter cold), tmmx (summer heat), pr (precipitation),
#           vpd (vapor pressure deficit / drought stress), pdsi (drought index)
key_vars <- c("tmmn", "tmmx", "pr", "vpd", "pdsi")

summaries_ds <- open_dataset(
  here("processed/climate/terraclimate/damage_areas_summaries")
)

cat("  Filtering to MPB damage areas and key climate variables...\n")
cat(sprintf("  Variables: %s\n", paste(key_vars, collapse = ", ")))

# Pull just the summaries for MPB observations
# Use Arrow for efficient filtering before collecting to memory
mpb_climate <- summaries_ds %>%
  filter(
    DAMAGE_AREA_ID %in% mpb_damage_area_ids,
    variable %in% key_vars
  ) %>%
  select(DAMAGE_AREA_ID, calendar_year, calendar_month,
         water_year, water_year_month, variable, weighted_mean) %>%
  collect()

cat(sprintf("  Loaded %s climate-observation rows\n",
            format(nrow(mpb_climate), big.mark = ",")))

# ==============================================================================
# Step 3: Join IDS attributes to climate data
# ==============================================================================

cat("\nStep 3: Joining IDS attributes to climate summaries...\n")

# Get IDS attributes (without geometry) for joining
ids_attrs <- ids %>%
  st_drop_geometry() %>%
  select(OBSERVATION_ID, DAMAGE_AREA_ID, SURVEY_YEAR,
         REGION_ID, HOST_CODE, HOST, ACRES)

# Join: each climate row gets the IDS observation context
mpb_joined <- mpb_climate %>%
  inner_join(
    ids_attrs %>% select(DAMAGE_AREA_ID, SURVEY_YEAR, REGION_ID, HOST, ACRES) %>% distinct(),
    by = "DAMAGE_AREA_ID"
  )

cat(sprintf("  Joined dataset: %s rows\n",
            format(nrow(mpb_joined), big.mark = ",")))

# ==============================================================================
# Step 4: Annual MPB outbreak timeline
# ==============================================================================

cat("\nStep 4: Creating annual MPB outbreak timeline...\n")

outbreak_timeline <- ids %>%
  st_drop_geometry() %>%
  group_by(SURVEY_YEAR) %>%
  summarize(
    n_observations = n(),
    total_acres = sum(ACRES, na.rm = TRUE),
    n_damage_areas = n_distinct(DAMAGE_AREA_ID),
    .groups = "drop"
  )

p1 <- ggplot(outbreak_timeline, aes(x = SURVEY_YEAR)) +
  geom_col(aes(y = total_acres / 1e6), fill = "#8B4513", alpha = 0.8) +
  geom_line(aes(y = n_observations / max(n_observations) * max(total_acres / 1e6)),
            color = "#D2691E", linewidth = 0.8) +
  scale_x_continuous(breaks = seq(1997, 2024, by = 3)) +
  scale_y_continuous(labels = label_comma(suffix = "M")) +
  labs(
    title = "Mountain Pine Beetle Damage Across the US (1997-2024)",
    subtitle = "Bars = total affected acres | Line = observation count (scaled)",
    x = "Survey Year",
    y = "Total Affected Acres (millions)",
    caption = "Data: USDA Forest Service IDS"
  ) +
  theme_minimal(base_size = 12) +
  theme(
    plot.title = element_text(face = "bold"),
    panel.grid.minor = element_blank()
  )

ggsave(file.path(output_dir, "01_mpb_outbreak_timeline.png"),
       p1, width = 10, height = 5, dpi = 150)
cat("  Saved: 01_mpb_outbreak_timeline.png\n")

# ==============================================================================
# Step 5: Winter minimum temperatures at MPB damage sites
# ==============================================================================

cat("\nStep 5: Analyzing winter temperatures at MPB damage sites...\n")

# Winter = Dec (month 12), Jan (month 1), Feb (month 2)
winter_temps <- mpb_joined %>%
  filter(variable == "tmmn", calendar_month %in% c(12, 1, 2)) %>%
  group_by(DAMAGE_AREA_ID, SURVEY_YEAR, calendar_year) %>%
  summarize(
    winter_tmmn = mean(weighted_mean, na.rm = TRUE),
    .groups = "drop"
  )

# For each damage area, get the winter temperature in the year damage was observed
# and the year before (potential "trigger" year)
winter_at_damage <- winter_temps %>%
  filter(calendar_year == SURVEY_YEAR | calendar_year == SURVEY_YEAR - 1) %>%
  mutate(timing = ifelse(calendar_year == SURVEY_YEAR, "Damage year", "Year before")) %>%
  group_by(SURVEY_YEAR, timing) %>%
  summarize(
    mean_winter_tmmn = mean(winter_tmmn, na.rm = TRUE),
    median_winter_tmmn = median(winter_tmmn, na.rm = TRUE),
    .groups = "drop"
  )

p2 <- ggplot(winter_at_damage, aes(x = SURVEY_YEAR, y = mean_winter_tmmn,
                                    color = timing)) +
  geom_line(linewidth = 0.8) +
  geom_point(size = 1.5) +
  geom_hline(yintercept = -40, linetype = "dashed", color = "blue", alpha = 0.5) +
  annotate("text", x = 2020, y = -39, label = "-40°C: lethal threshold for MPB larvae",
           size = 3, color = "blue", hjust = 1) +
  scale_color_manual(values = c("Damage year" = "#D2691E", "Year before" = "#4682B4")) +
  scale_x_continuous(breaks = seq(1997, 2024, by = 3)) +
  labs(
    title = "Winter Minimum Temperatures at MPB Damage Sites",
    subtitle = "Mean of Dec-Jan-Feb TerraClimate tmmn across all MPB damage areas per year",
    x = "Survey Year",
    y = "Winter Min Temperature (°C)",
    color = NULL,
    caption = "Data: IDS + TerraClimate"
  ) +
  theme_minimal(base_size = 12) +
  theme(
    plot.title = element_text(face = "bold"),
    legend.position = "top",
    panel.grid.minor = element_blank()
  )

ggsave(file.path(output_dir, "02_mpb_winter_temperatures.png"),
       p2, width = 10, height = 5, dpi = 150)
cat("  Saved: 02_mpb_winter_temperatures.png\n")

# ==============================================================================
# Step 6: Drought conditions (PDSI) at MPB damage sites
# ==============================================================================

cat("\nStep 6: Analyzing drought conditions (PDSI) at MPB sites...\n")

# Summer PDSI (Jun-Aug) in the year damage was detected
summer_pdsi <- mpb_joined %>%
  filter(variable == "pdsi", calendar_month %in% 6:8) %>%
  group_by(DAMAGE_AREA_ID, SURVEY_YEAR, calendar_year) %>%
  summarize(
    summer_pdsi = mean(weighted_mean, na.rm = TRUE),
    .groups = "drop"
  ) %>%
  filter(calendar_year == SURVEY_YEAR | calendar_year == SURVEY_YEAR - 1) %>%
  mutate(timing = ifelse(calendar_year == SURVEY_YEAR, "Damage year", "Year before"))

pdsi_annual <- summer_pdsi %>%
  group_by(SURVEY_YEAR, timing) %>%
  summarize(
    mean_pdsi = mean(summer_pdsi, na.rm = TRUE),
    q25 = quantile(summer_pdsi, 0.25, na.rm = TRUE),
    q75 = quantile(summer_pdsi, 0.75, na.rm = TRUE),
    .groups = "drop"
  )

p3 <- ggplot(pdsi_annual %>% filter(timing == "Damage year"),
             aes(x = SURVEY_YEAR)) +
  geom_ribbon(aes(ymin = q25, ymax = q75), fill = "#8B4513", alpha = 0.2) +
  geom_line(aes(y = mean_pdsi), color = "#8B4513", linewidth = 0.8) +
  geom_hline(yintercept = 0, linetype = "solid", color = "gray50") +
  geom_hline(yintercept = -2, linetype = "dashed", color = "red", alpha = 0.5) +
  annotate("text", x = 2020, y = -1.8, label = "PDSI < -2: moderate drought",
           size = 3, color = "red", hjust = 1) +
  scale_x_continuous(breaks = seq(1997, 2024, by = 3)) +
  labs(
    title = "Drought Conditions at Mountain Pine Beetle Damage Sites",
    subtitle = "Summer (Jun-Aug) Palmer Drought Severity Index | Ribbon = IQR across damage areas",
    x = "Survey Year",
    y = "Summer PDSI\n(negative = drought)",
    caption = "Data: IDS + TerraClimate"
  ) +
  theme_minimal(base_size = 12) +
  theme(
    plot.title = element_text(face = "bold"),
    panel.grid.minor = element_blank()
  )

ggsave(file.path(output_dir, "03_mpb_drought_pdsi.png"),
       p3, width = 10, height = 5, dpi = 150)
cat("  Saved: 03_mpb_drought_pdsi.png\n")

# ==============================================================================
# Step 7: Summer VPD (vapor pressure deficit) — tree stress indicator
# ==============================================================================

cat("\nStep 7: Analyzing vapor pressure deficit at MPB sites...\n")

summer_vpd <- mpb_joined %>%
  filter(variable == "vpd", calendar_month %in% 6:8) %>%
  group_by(SURVEY_YEAR, calendar_year) %>%
  summarize(
    mean_vpd = mean(weighted_mean, na.rm = TRUE),
    .groups = "drop"
  ) %>%
  filter(calendar_year == SURVEY_YEAR)

p4 <- ggplot(summer_vpd, aes(x = SURVEY_YEAR, y = mean_vpd)) +
  geom_col(fill = "#CC5500", alpha = 0.7) +
  scale_x_continuous(breaks = seq(1997, 2024, by = 3)) +
  labs(
    title = "Summer Vapor Pressure Deficit at MPB Damage Sites",
    subtitle = "Mean Jun-Aug VPD across all MPB observations | Higher VPD = more tree stress",
    x = "Survey Year",
    y = "VPD (kPa)",
    caption = "Data: IDS + TerraClimate"
  ) +
  theme_minimal(base_size = 12) +
  theme(
    plot.title = element_text(face = "bold"),
    panel.grid.minor = element_blank()
  )

ggsave(file.path(output_dir, "04_mpb_summer_vpd.png"),
       p4, width = 10, height = 5, dpi = 150)
cat("  Saved: 04_mpb_summer_vpd.png\n")

# ==============================================================================
# Step 8: Regional comparison — climate at MPB sites by USFS region
# ==============================================================================

cat("\nStep 8: Regional climate comparison...\n")

region_labels <- c(
  "1" = "R1 Northern",
  "2" = "R2 Rocky Mtn",
  "3" = "R3 Southwest",
  "4" = "R4 Intermtn",
  "5" = "R5 Pacific SW",
  "6" = "R6 Pacific NW",
  "10" = "R10 Alaska"
)

# Annual mean summer tmmx by region
regional_heat <- mpb_joined %>%
  filter(variable == "tmmx", calendar_month %in% 6:8,
         calendar_year == SURVEY_YEAR,
         REGION_ID %in% names(region_labels)) %>%
  group_by(REGION_ID, SURVEY_YEAR) %>%
  summarize(
    mean_summer_tmmx = mean(weighted_mean, na.rm = TRUE),
    n_obs = n_distinct(DAMAGE_AREA_ID),
    .groups = "drop"
  ) %>%
  mutate(region_label = region_labels[as.character(REGION_ID)])

p5 <- ggplot(regional_heat, aes(x = SURVEY_YEAR, y = mean_summer_tmmx,
                                 color = region_label)) +
  geom_line(linewidth = 0.7, alpha = 0.8) +
  scale_color_brewer(palette = "Set2") +
  scale_x_continuous(breaks = seq(1997, 2024, by = 5)) +
  labs(
    title = "Summer Maximum Temperature at MPB Sites by USFS Region",
    subtitle = "Mean Jun-Aug tmmx for mountain pine beetle damage areas",
    x = "Survey Year",
    y = "Summer Max Temperature (°C)",
    color = "Region",
    caption = "Data: IDS + TerraClimate"
  ) +
  theme_minimal(base_size = 12) +
  theme(
    plot.title = element_text(face = "bold"),
    legend.position = "right",
    panel.grid.minor = element_blank()
  )

ggsave(file.path(output_dir, "05_mpb_regional_summer_temps.png"),
       p5, width = 11, height = 5, dpi = 150)
cat("  Saved: 05_mpb_regional_summer_temps.png\n")

# ==============================================================================
# Step 9: Monthly climate profile for peak vs low outbreak years
# ==============================================================================

cat("\nStep 9: Monthly climate profiles for peak vs low outbreak years...\n")

# Identify peak and low outbreak years by total acres
year_severity <- outbreak_timeline %>%
  arrange(desc(total_acres)) %>%
  mutate(rank = row_number())

peak_years <- year_severity %>% head(5) %>% pull(SURVEY_YEAR)
low_years  <- year_severity %>% tail(5) %>% pull(SURVEY_YEAR)

cat(sprintf("  Peak outbreak years (top 5 by acres): %s\n",
            paste(sort(peak_years), collapse = ", ")))
cat(sprintf("  Low outbreak years (bottom 5 by acres): %s\n",
            paste(sort(low_years), collapse = ", ")))

month_names <- c("Jan", "Feb", "Mar", "Apr", "May", "Jun",
                 "Jul", "Aug", "Sep", "Oct", "Nov", "Dec")

monthly_profiles <- mpb_joined %>%
  filter(variable %in% c("tmmx", "pr"),
         calendar_year == SURVEY_YEAR,
         SURVEY_YEAR %in% c(peak_years, low_years)) %>%
  mutate(
    era = ifelse(SURVEY_YEAR %in% peak_years, "Peak outbreak years", "Low outbreak years"),
    month_label = factor(month_names[calendar_month], levels = month_names)
  ) %>%
  group_by(era, calendar_month, month_label, variable) %>%
  summarize(
    mean_value = mean(weighted_mean, na.rm = TRUE),
    .groups = "drop"
  )

p6 <- ggplot(monthly_profiles %>% filter(variable == "tmmx"),
             aes(x = month_label, y = mean_value, color = era, group = era)) +
  geom_line(linewidth = 0.8) +
  geom_point(size = 2) +
  scale_color_manual(values = c("Peak outbreak years" = "#D2691E",
                                 "Low outbreak years" = "#4682B4")) +
  labs(
    title = "Monthly Temperature at MPB Sites: Peak vs Low Outbreak Years",
    subtitle = sprintf("Peak years: %s | Low years: %s",
                        paste(sort(peak_years), collapse = ", "),
                        paste(sort(low_years), collapse = ", ")),
    x = "Month",
    y = "Max Temperature (°C)",
    color = NULL,
    caption = "Data: IDS + TerraClimate"
  ) +
  theme_minimal(base_size = 12) +
  theme(
    plot.title = element_text(face = "bold"),
    legend.position = "top",
    axis.text.x = element_text(angle = 45, hjust = 1),
    panel.grid.minor = element_blank()
  )

ggsave(file.path(output_dir, "06_mpb_monthly_temp_profile.png"),
       p6, width = 10, height = 5, dpi = 150)
cat("  Saved: 06_mpb_monthly_temp_profile.png\n")

# ==============================================================================
# Summary
# ==============================================================================

cat("\n================================================================\n")
cat(" COMPLETE\n")
cat("================================================================\n")
cat(sprintf("  Output directory: %s\n", output_dir))
cat(sprintf("  Figures generated: 6\n"))
cat(sprintf("  MPB observations analyzed: %s\n",
            format(nrow(ids), big.mark = ",")))
cat(sprintf("  Climate-observation rows: %s\n",
            format(nrow(mpb_climate), big.mark = ",")))
cat("\n  Figures:\n")
cat("    01_mpb_outbreak_timeline.png       - MPB damage across US, 1997-2024\n")
cat("    02_mpb_winter_temperatures.png     - Winter min temps at damage sites\n")
cat("    03_mpb_drought_pdsi.png            - Palmer Drought Index at damage sites\n")
cat("    04_mpb_summer_vpd.png              - Vapor pressure deficit (tree stress)\n")
cat("    05_mpb_regional_summer_temps.png   - Regional temperature comparison\n")
cat("    06_mpb_monthly_temp_profile.png    - Monthly temp: peak vs low years\n")
cat("\n  Key ecological context:\n")
cat("    - MPB larvae die when winter temps drop below ~-40°C\n")
cat("    - Warmer winters allow higher overwinter survival -> larger outbreaks\n")
cat("    - Drought-stressed trees produce less defensive resin -> easier attack\n")
cat("    - VPD measures atmospheric drought demand on trees\n")
cat("    - These figures show how the compiled dataset enables climate-pest analysis\n")
cat("================================================================\n\n")
