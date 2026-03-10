# ==============================================================================
# demo_03_site_climate.R
# Point-Based TerraClimate — FIA Sites and Custom Location Lists
# ==============================================================================
#
# Demonstrates how to query and analyze site_climate.parquet, which holds
# monthly TerraClimate (1958–present) for 6,956 FIA plot locations.
#
# The same pipeline can extract climate for ANY lat/lon CSV using
# 05_fia/data/processed/site_climate/all_site_locations.csv as the input template.
#
#   Part A  Understand the site list (all_site_locations.csv)
#   Part B  Explore site_climate.parquet structure
#   Part C  Annual water-year summaries per site
#   Part D  Long-term mean climate across sites
#   Part E  Figures — temporal trends and spatial variation
#   Part F  Example: filter to a geographic subset
#   Part G  How to add custom sites
#
# Usage:
#   Rscript scripts/demo_03_site_climate.R
#
# Prerequisites:
#   Rscript 05_fia/scripts/06_extract_site_climate.R
#
# Output: output/demo_03_site_climate/ (figures + CSV summaries)
#
# See also:
#   demo_01_ids_climate.R  — Area-weighted gridded climate for IDS polygons
#   demo_02_fia_forest.R   — FIA plot/condition data and exclusion flags
# ==============================================================================

library(here)
library(dplyr)
library(tidyr)
library(arrow)
library(ggplot2)
library(scales)

output_dir <- here("output", "demo_03_site_climate")
dir.create(output_dir, recursive = TRUE, showWarnings = FALSE)

cat("FIA Site Climate Demo\n")
cat("=====================\n\n")

# ==============================================================================
# Part A: Understand the site list
# ==============================================================================
#
# 05_fia/data/processed/site_climate/all_site_locations.csv defines the sites
# for which climate was extracted. It is the *input* to 06_extract_site_climate.R; the output is
# site_climate.parquet.
#
# Schema: site_id, latitude, longitude, source
#   site_id   — unique location identifier (FIA PLT_CN for FIA plots)
#   source    — origin of the point ("FIA" for all current rows)
#

sites <- read.csv(here("05_fia/data/processed/site_climate/all_site_locations.csv"))

cat(sprintf("Site locations: %s  |  Source(s): %s\n",
            format(nrow(sites), big.mark = ","),
            paste(unique(sites$source), collapse = ", ")))
cat(sprintf("Lat range: %.2f – %.2f\n", min(sites$latitude),  max(sites$latitude)))
cat(sprintf("Lon range: %.2f – %.2f\n", min(sites$longitude), max(sites$longitude)))

# ==============================================================================
# Part B: Explore site_climate.parquet structure
# ==============================================================================
#
# Schema: site_id | year | month | water_year | water_year_month | variable | value
#
# Variables:
#   tmmx — Maximum temperature   (°C)
#   tmmn — Minimum temperature   (°C)
#   pr   — Precipitation         (mm)
#   def  — Climate water deficit (mm) [= CWD = PET - AET]
#   pet  — Reference ET          (mm)
#   aet  — Actual ET             (mm)
#
# Period: 1958–present | ~23.5 million rows total
#

clim <- read_parquet(
  here("05_fia/data/processed/site_climate/site_climate.parquet")
)

cat(sprintf("\nSite climate rows: %s\n", format(nrow(clim), big.mark = ",")))
cat(sprintf("Sites: %s  |  Years: %d–%d  |  Variables: %s\n",
            format(n_distinct(clim$site_id), big.mark = ","),
            min(clim$year), max(clim$year),
            paste(sort(unique(clim$variable)), collapse = ", ")))

# ==============================================================================
# Part C: Annual water-year summaries per site
# ==============================================================================
#
# Water year = October of the previous calendar year through September of the
# named year (e.g. water_year 2020 = Oct 2019 – Sep 2020).
# This convention is already computed in the parquet.
#

annual_site <- clim |>
  filter(variable %in% c("def", "pr", "tmmx", "tmmn")) |>
  group_by(site_id, water_year, variable) |>
  summarize(
    value_annual = case_when(
      first(variable) == "pr"   ~ sum(value,  na.rm = TRUE),   # precip = sum
      first(variable) == "def"  ~ sum(value,  na.rm = TRUE),   # CWD   = sum
      TRUE                      ~ mean(value, na.rm = TRUE)    # temps = mean
    ),
    .groups = "drop"
  )

cat(sprintf("\nAnnual water-year summaries: %s rows\n",
            format(nrow(annual_site), big.mark = ",")))

# ==============================================================================
# Part D: Long-term mean climate across sites
# ==============================================================================
#
# Use the full record to compute a long-term climatology per site.
# Restricting to 1981–2010 gives the WMO standard 30-year baseline.
#

ltm_site <- annual_site |>
  filter(water_year >= 1981, water_year <= 2010) |>
  group_by(site_id, variable) |>
  summarize(ltm = mean(value_annual, na.rm = TRUE), .groups = "drop") |>
  pivot_wider(names_from = variable, values_from = ltm)

cat(sprintf("\nLong-term means (1981–2010) for %s sites:\n",
            format(nrow(ltm_site), big.mark = ",")))
cat(sprintf("  CWD  (def):  %.0f – %.0f mm/yr\n",
            min(ltm_site$def,  na.rm = TRUE), max(ltm_site$def,  na.rm = TRUE)))
cat(sprintf("  Precip (pr): %.0f – %.0f mm/yr\n",
            min(ltm_site$pr,   na.rm = TRUE), max(ltm_site$pr,   na.rm = TRUE)))
cat(sprintf("  Max temp:    %.1f – %.1f °C (annual mean of monthly max)\n",
            min(ltm_site$tmmx, na.rm = TRUE), max(ltm_site$tmmx, na.rm = TRUE)))

write.csv(ltm_site, file.path(output_dir, "ltm_by_site.csv"), row.names = FALSE)

# ==============================================================================
# Part E: Figures
# ==============================================================================

# -- Figure 1: Annual mean CWD across all sites, 1958-present -----------------

annual_us <- annual_site |>
  filter(variable == "def") |>
  group_by(water_year) |>
  summarize(
    cwd_mean  = mean(value_annual, na.rm = TRUE),
    cwd_p25   = quantile(value_annual, 0.25, na.rm = TRUE),
    cwd_p75   = quantile(value_annual, 0.75, na.rm = TRUE),
    .groups   = "drop"
  )

p_cwd <- ggplot(annual_us, aes(x = water_year, y = cwd_mean)) +
  geom_ribbon(aes(ymin = cwd_p25, ymax = cwd_p75), fill = "#c1440e", alpha = 0.25) +
  geom_line(color = "#c1440e", linewidth = 0.9) +
  geom_smooth(method = "lm", se = FALSE, color = "#7b1d0a", linewidth = 0.7, linetype = "dashed") +
  labs(
    title   = "Annual Climate Water Deficit (CWD) — All FIA Sites, 1958–Present",
    subtitle = "Shaded band: 25th–75th percentile across sites",
    x       = "Water Year",
    y       = "CWD (mm/yr)",
    caption = "Source: TerraClimate via GEE | CWD = PET − AET"
  ) +
  scale_x_continuous(breaks = seq(1960, 2025, 5)) +
  theme_minimal(base_size = 13) +
  theme(plot.title    = element_text(face = "bold"),
        plot.subtitle = element_text(color = "#666"),
        panel.grid.minor = element_blank())

ggsave(file.path(output_dir, "01_cwd_timeseries.png"), p_cwd, width = 12, height = 5, dpi = 150)

# -- Figure 2: JJA mean max temp trend ----------------------------------------

summer_tmax <- clim |>
  filter(variable == "tmmx", month %in% 6:8) |>
  group_by(water_year) |>
  summarize(tmax_jja = mean(value, na.rm = TRUE), .groups = "drop")

p_tmax <- ggplot(summer_tmax, aes(x = water_year, y = tmax_jja)) +
  geom_point(color = "#e63946", size = 1.5, alpha = 0.7) +
  geom_smooth(method = "loess", span = 0.3, se = TRUE, color = "#c1121f", linewidth = 1) +
  labs(
    title   = "Summer (JJA) Mean Max Temperature — All FIA Sites",
    x       = "Water Year",
    y       = "Mean JJA Tmax (°C)",
    caption = "Source: TerraClimate tmmx | Mean across all 6,956 site-months"
  ) +
  scale_x_continuous(breaks = seq(1960, 2025, 5)) +
  theme_minimal(base_size = 13) +
  theme(plot.title = element_text(face = "bold"), panel.grid.minor = element_blank())

ggsave(file.path(output_dir, "02_summer_tmax.png"), p_tmax, width = 12, height = 5, dpi = 150)

# -- Figure 3: Long-term CWD vs precip across sites (climate space) -----------

p_space <- ggplot(ltm_site |> filter(!is.na(def), !is.na(pr)),
                  aes(x = pr, y = def)) +
  geom_point(alpha = 0.4, size = 1.5, color = "#457b9d") +
  geom_smooth(method = "lm", se = TRUE, color = "#1d3557", linewidth = 0.8) +
  labs(
    title   = "Long-Term Mean CWD vs Precipitation — FIA Site Climatology (1981–2010)",
    x       = "Mean Annual Precipitation (mm/yr)",
    y       = "Mean Annual CWD (mm/yr)",
    caption = "Each point = one FIA plot location | CWD = PET − AET"
  ) +
  theme_minimal(base_size = 13) +
  theme(plot.title = element_text(face = "bold"))

ggsave(file.path(output_dir, "03_climate_space.png"), p_space, width = 9, height = 6, dpi = 150)

# ==============================================================================
# Part F: Geographic subset — Colorado sites only
# ==============================================================================
#
# Filter using the site list to get Colorado FIA plots, then query climate.
#

co_sites <- sites |>
  filter(latitude  >= 37.0, latitude  <= 41.1,
         longitude >= -109.1, longitude <= -102.0)

cat(sprintf("\nColorado sites: %d\n", nrow(co_sites)))

co_clim <- clim |>
  filter(site_id %in% co_sites$site_id)

co_annual_cwd <- co_clim |>
  filter(variable == "def") |>
  group_by(water_year) |>
  summarize(cwd_mean = mean(value, na.rm = TRUE), .groups = "drop")

cat(sprintf("Colorado water-year CWD range: %.0f – %.0f mm\n",
            min(co_annual_cwd$cwd_mean), max(co_annual_cwd$cwd_mean)))

write.csv(co_annual_cwd, file.path(output_dir, "colorado_annual_cwd.csv"), row.names = FALSE)

# ==============================================================================
# Part G: How to add custom sites
# ==============================================================================
#
# To extract TerraClimate for additional lat/lon locations:
#
#   1. Append rows to 05_fia/data/processed/site_climate/all_site_locations.csv:
#
#      custom_sites <- data.frame(
#        site_id   = c("my_site_01", "my_site_02"),
#        latitude  = c(44.5, 46.1),
#        longitude = c(-110.3, -108.7),
#        source    = "custom"
#      )
#      write.csv(rbind(existing_sites, custom_sites),
#                here("05_fia/data/processed/site_climate/all_site_locations.csv"), row.names = FALSE)
#
#   2. Re-run the extraction script:
#
#      Rscript 05_fia/scripts/06_extract_site_climate.R
#
#   The script maps each site to its 4km TerraClimate pixel (nearest centroid),
#   then extracts monthly values for all variables via Google Earth Engine.
#   The output site_climate.parquet is updated with the new rows.
#
#   Note: GEE authentication required — see local/user_config.yaml.
#

cat("\nPart G: To add custom sites, see script comments above.\n")

cat(sprintf("\nAll outputs written to %s/\n", output_dir))
