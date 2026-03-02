# ==============================================================================
# demo_02_fia_forest.R
# FIA Forest Inventory — Plot Filtering, Stand Metrics, and Disturbance Analysis
# ==============================================================================
#
# Demonstrates how to work with the compiled FIA plot-level summary parquets:
#
#   Part A  Plot exclusion flags — filtering to analysis-ready plots
#   Part B  Tree metrics — stand structure and species diversity
#   Part C  Disturbance history — fire, insect, and wind events
#   Part D  Damage agents — tree-level insect/disease attribution
#   Part E  Treatment history — harvests, planting, and other silviculture
#   Part F  Seedling regeneration
#   Part G  Mortality between measurements
#
# Usage:
#   Rscript scripts/demo_02_fia_forest.R
#
# Prerequisites (run FIA pipeline first):
#   Rscript 05_fia/scripts/05_build_fia_summaries.R
#
# Output: output/demo_02_fia_forest/  (figures + CSV summaries)
#
# See also:
#   demo_01_ids_climate.R   — IDS outbreak analysis with gridded climate
#   demo_03_site_climate.R  — Point-based TerraClimate at FIA sites
# ==============================================================================

library(here)
library(dplyr)
library(tidyr)
library(arrow)
library(ggplot2)
library(scales)

output_dir <- here("output", "demo_02_fia_forest")
dir.create(output_dir, recursive = TRUE, showWarnings = FALSE)

cat("FIA Forest Inventory Demo\n")
cat("=========================\n\n")

# ==============================================================================
# Part A: Plot exclusion flags
# ==============================================================================
#
# ~12% of FIA plots have been deforested, harvested, or subjected to human
# disturbance. Use plot_exclusion_flags.parquet for a single pre-built filter.
#
# Key flags:
#   pct_forested        — fraction of plot in forest condition (primary gate)
#   exclude_nonforest   — any condition with COND_STATUS_CD != 1 (deforested)
#   exclude_human_dist  — DSTRBCD 80 in any slot (human-induced disturbance)
#   exclude_harvest     — TRTCD 10 in any slot (cutting/harvest treatment)
#   exclude_any         — OR of all three above  ← use for standard cleaning
#   has_fire            — DSTRBCD 30/31/32       ← identify burned plots
#   has_insect          — DSTRBCD 10/11/12       ← identify insect-affected plots
#

flags <- read_parquet(
  here("05_fia/data/processed/summaries/plot_exclusion_flags.parquet")
)

cat(sprintf("Total plot-years in flags: %s\n", format(nrow(flags), big.mark = ",")))

# Standard filter: forested + no human disturbance + no harvest
clean <- flags |> filter(pct_forested >= 0.5, !exclude_any)
cat(sprintf("After clean filter:        %s (%.1f%% retained)\n",
            format(nrow(clean), big.mark = ","),
            100 * nrow(clean) / nrow(flags)))

# Subsets by disturbance type
burned  <- flags |> filter(has_fire)
insect  <- flags |> filter(has_insect)
cat(sprintf("Burned plots:              %s\n", format(nrow(burned),  big.mark = ",")))
cat(sprintf("Insect-affected plots:     %s\n", format(nrow(insect),  big.mark = ",")))

# Flag summary by state
flag_summary <- flags |>
  group_by(STATECD) |>
  summarize(
    n_total         = n(),
    pct_clean       = 100 * mean(!exclude_any & pct_forested >= 0.5),
    pct_nonforest   = 100 * mean(exclude_nonforest),
    pct_harvested   = 100 * mean(exclude_harvest),
    pct_burned      = 100 * mean(has_fire),
    pct_insect      = 100 * mean(has_insect),
    .groups         = "drop"
  )

write.csv(flag_summary, file.path(output_dir, "flag_summary_by_state.csv"), row.names = FALSE)

# ==============================================================================
# Part B: Tree metrics — stand structure and diversity
# ==============================================================================
#
# plot_tree_metrics.parquet has one row per PLT_CN × INVYR.
# Key columns:
#   ba_live_total       — total live basal area (ft²/acre)
#   ba_dead_total       — total dead basal area
#   ba_live_softwood    — softwood fraction
#   ba_live_hardwood    — hardwood fraction
#   ba_live_sapling     — sapling-layer BA (DBH 1-5")
#   ba_live_mature      — mature-layer BA (DBH 5-12")
#   ba_live_overstory   — overstory BA (DBH > 12")
#   n_species_live      — live-tree species richness
#   shannon_h_ba        — Shannon diversity index (BA-weighted)
#   species_temp_optima_mean — mean temperature optima of species present (°C)
#

trees <- read_parquet(
  here("05_fia/data/processed/summaries/plot_tree_metrics.parquet")
)

cat(sprintf("\nTree metrics: %s plot-years | %d states\n",
            format(nrow(trees), big.mark = ","),
            n_distinct(trees$state)))

# Join clean-plot filter before any analysis
trees_clean <- trees |>
  inner_join(clean |> select(PLT_CN, INVYR), by = c("PLT_CN", "INVYR"))

cat(sprintf("Clean-filtered tree metrics: %s plot-years\n",
            format(nrow(trees_clean), big.mark = ",")))

# Annual mean live BA by year (clean plots only)
ba_annual <- trees_clean |>
  group_by(INVYR) |>
  summarize(
    ba_mean   = mean(ba_live_total, na.rm = TRUE),
    ba_median = median(ba_live_total, na.rm = TRUE),
    n_plots   = n(),
    .groups   = "drop"
  ) |>
  filter(INVYR >= 2000)

write.csv(ba_annual, file.path(output_dir, "ba_annual.csv"), row.names = FALSE)

p_ba <- ggplot(ba_annual, aes(x = INVYR, y = ba_mean)) +
  geom_ribbon(aes(ymin = ba_median, ymax = ba_mean), alpha = 0.2, fill = "#2d6a4f") +
  geom_line(color = "#2d6a4f", linewidth = 1) +
  geom_point(color = "#2d6a4f", size = 2) +
  labs(
    title   = "Mean Live Basal Area — Clean FIA Plots (2000–present)",
    x       = "Inventory Year",
    y       = "Basal Area (ft²/acre)",
    caption = "Clean plots: pct_forested >= 0.5 & exclude_any == FALSE"
  ) +
  theme_minimal(base_size = 13) +
  theme(plot.title = element_text(face = "bold"), panel.grid.minor = element_blank())

ggsave(file.path(output_dir, "01_ba_annual.png"), p_ba, width = 10, height = 5, dpi = 150)

# Species diversity: distribution of Shannon H across clean plots
p_div <- ggplot(trees_clean |> filter(!is.na(shannon_h_ba)),
                aes(x = shannon_h_ba)) +
  geom_histogram(fill = "#2d6a4f", bins = 50, alpha = 0.8) +
  labs(
    title   = "Distribution of BA-Weighted Species Diversity (Shannon H)",
    x       = "Shannon H (basal area weighted)",
    y       = "Number of plot-years",
    caption = "Clean plots only"
  ) +
  theme_minimal(base_size = 13) +
  theme(plot.title = element_text(face = "bold"))

ggsave(file.path(output_dir, "02_diversity_dist.png"), p_div, width = 8, height = 5, dpi = 150)

# ==============================================================================
# Part C: Disturbance history
# ==============================================================================
#
# plot_disturbance_history.parquet is long-format: one row per condition ×
# disturbance slot (up to 3 slots per condition).
#
# Key columns:
#   DSTRBCD            — USFS disturbance code
#   DSTRBYR            — reported disturbance year (may be NA)
#   disturbance_label  — human-readable label (e.g. "Insect damage")
#   disturbance_category — grouped category: insects, fire, wind, other, human
#

disturb <- read_parquet(
  here("05_fia/data/processed/summaries/plot_disturbance_history.parquet")
)

cat(sprintf("\nDisturbance records: %s rows\n", format(nrow(disturb), big.mark = ",")))

# Count of records by category
disturb_counts <- disturb |>
  count(disturbance_category, disturbance_label, sort = TRUE)

cat("\nTop disturbance types:\n")
print(head(disturb_counts, 15), n = 15)

# Annual disturbance area fraction
dist_annual <- disturb |>
  filter(!is.na(DSTRBYR), DSTRBYR >= 1990) |>
  count(DSTRBYR, disturbance_category) |>
  group_by(DSTRBYR) |>
  mutate(pct = n / sum(n)) |>
  ungroup()

p_dist <- ggplot(dist_annual, aes(x = DSTRBYR, y = n, fill = disturbance_category)) +
  geom_col(alpha = 0.85) +
  scale_fill_manual(
    values = c(insects = "#8B4513", fire = "#E63946", wind = "#457B9D",
               human = "#6A0572", other = "#888"),
    name = "Category"
  ) +
  labs(
    title   = "FIA Disturbance Records by Year and Category",
    x       = "Disturbance Year",
    y       = "Number of condition-slots",
    caption = "Source: COND.DSTRBCD1/2/3 | DSTRBYR reported by FIA field crew"
  ) +
  theme_minimal(base_size = 13) +
  theme(plot.title = element_text(face = "bold"), panel.grid.minor = element_blank(),
        legend.position = "right")

ggsave(file.path(output_dir, "03_disturbance_annual.png"),
       p_dist, width = 12, height = 6, dpi = 150)

# ==============================================================================
# Part D: Damage agents (tree-level)
# ==============================================================================
#
# plot_damage_agents.parquet: one row per PLT_CN × SPCD × DAMAGE_AGENT_CD.
# Columns: ba_per_acre, n_trees_tpa, agent_label, agent_category.
#

agents <- read_parquet(
  here("05_fia/data/processed/summaries/plot_damage_agents.parquet")
)

cat(sprintf("\nDamage agent records: %s rows\n", format(nrow(agents), big.mark = ",")))

# Top agents by total BA affected
top_agents <- agents |>
  group_by(agent_label, agent_category) |>
  summarize(total_ba = sum(ba_per_acre, na.rm = TRUE),
            n_plots  = n_distinct(PLT_CN),
            .groups  = "drop") |>
  arrange(desc(total_ba)) |>
  head(20)

write.csv(top_agents, file.path(output_dir, "top_damage_agents.csv"), row.names = FALSE)
cat("\nTop 10 damage agents by total BA affected:\n")
print(head(top_agents, 10))

# ==============================================================================
# Part E: Treatment history
# ==============================================================================
#
# plot_treatment_history.parquet: one row per PLT_CN × INVYR × CONDID × TRTCD.
# TRTCD codes: 10=cutting, 20=site_prep, 30=planting, 40=natural_regen, 50=other
#

treat <- read_parquet(
  here("05_fia/data/processed/summaries/plot_treatment_history.parquet")
)

cat(sprintf("\nTreatment records: %s rows\n", format(nrow(treat), big.mark = ",")))

treat_counts <- treat |>
  count(treatment_label, treatment_category, sort = TRUE)

cat("\nTreatment type counts:\n")
print(treat_counts)

# ==============================================================================
# Part F: Seedling regeneration
# ==============================================================================

seed <- read_parquet(
  here("05_fia/data/processed/summaries/plot_seedling_metrics.parquet")
)

cat(sprintf("\nSeedling records: %s plot-years\n", format(nrow(seed), big.mark = ",")))
cat(sprintf("Mean seedlings per acre: %.0f\n",
            mean(seed$treecount_total, na.rm = TRUE)))

# ==============================================================================
# Part G: Mortality
# ==============================================================================
#
# plot_mortality_metrics.parquet: one row per PLT_CN × INVYR × AGENTCD ×
# component_type (natural / harvest).
#

mort <- read_parquet(
  here("05_fia/data/processed/summaries/plot_mortality_metrics.parquet")
)

cat(sprintf("\nMortality records: %s rows\n", format(nrow(mort), big.mark = ",")))

# Natural mortality by agent
nat_mort <- mort |>
  filter(component_type == "natural") |>
  group_by(AGENTCD) |>
  summarize(mean_tpa_mort = mean(tpamort_per_acre, na.rm = TRUE),
            n_plots       = n_distinct(PLT_CN),
            .groups       = "drop") |>
  arrange(desc(mean_tpa_mort))

cat("\nNatural mortality by agent code (top 10):\n")
print(head(nat_mort, 10))
write.csv(nat_mort, file.path(output_dir, "natural_mortality_by_agent.csv"), row.names = FALSE)

cat(sprintf("\nAll outputs written to %s/\n", output_dir))
