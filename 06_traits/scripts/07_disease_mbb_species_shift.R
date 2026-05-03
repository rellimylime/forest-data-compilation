# ==============================================================================
# 07_disease_mbb_species_shift.R
# Identify which species are over-represented at disease plots in maple-beech-
# birch (forest_type_group 800) forests, compared to climatically-matched
# undisturbed plots in the same forest type.
#
# This is the direct test for the +0.74 deg C signal in maple-beech-birch
# disease plots reported in the disease-East drilldown. If beech bark / beech
# leaf disease are killing American beech and warmer-affinity species are
# replacing it, those replacement species (red maple, sweet birch, yellow-
# poplar, black cherry, etc.) should appear with higher per-plot abundance at
# disease plots than at matched control plots.
#
# Inputs:
#   06_traits/data/processed/plot_matches.parquet
#     Source for the matched (disturbed, control) pairs in MBB disease plots.
#   05_fia/data/processed/summaries/plot_seedling_species.parquet
#     Per (PLT_CN, INVYR, CONDID, SUBP, SPCD) seedling counts with names.
#   06_traits/data/processed/species_climate_affinity.parquet
#     Per-species temp_mean, joined for plain-language warm/cool tagging.
#
# Outputs:
#   06_traits/data/processed/disease_mbb_species_comparison.parquet
#     One row per species with:
#       - mean per-plot share at MBB disease plots
#       - mean per-plot share at matched MBB control plots
#       - delta_share (disease - control)
#       - the species' temp_mean (climate affinity)
#       - n_plots present (each side)
#
# Reading the output: positive delta_share means that species is more abundant
# (as a share of all seedlings on the plot) at disease plots than at matched
# control plots. If the species with positive delta_share are warmer than the
# species with negative delta_share, the +0.74 deg C signal has a clean
# species-level explanation.
# ==============================================================================

library(arrow)
library(dplyr)
library(data.table)
library(here)
library(fs)
library(glue)

MBB_FOREST_TYPE_GROUP <- 800L
MIN_PLOTS_PRESENT     <- 20L  # only show species present at >= this many plots
                              # on either side; prevents one-plot rarities from
                              # cluttering the headline output.

cat("Maple-Beech-Birch Disease vs Control: Species Shift\n")
cat("====================================================\n\n")

# ------------------------------------------------------------------------------
# 1. Identify MBB disease plots and their matched controls
# ------------------------------------------------------------------------------

cat("[1/5] Selecting MBB disease plots and their matched controls...\n")

matches <- read_parquet(here("06_traits/data/processed/plot_matches.parquet")) |>
  as.data.table()

mbb <- matches[disturbance_class == "disease" &
               region_east_west  == "East" &
               forest_type_group == MBB_FOREST_TYPE_GROUP]

dist_keys <- unique(mbb[, .(PLT_CN = dist_PLT_CN,
                            INVYR  = dist_INVYR,
                            CONDID = dist_CONDID)])
ctrl_keys <- unique(mbb[, .(PLT_CN = ctrl_PLT_CN,
                            INVYR  = ctrl_INVYR,
                            CONDID = ctrl_CONDID)])

cat(glue("       MBB disease plots: {format(nrow(dist_keys), big.mark=',')}\n"))
cat(glue("       distinct matched MBB control plots: ",
         "{format(nrow(ctrl_keys), big.mark=',')}\n"))

# ------------------------------------------------------------------------------
# 2. Load seedling species at those plots
# ------------------------------------------------------------------------------

cat("[2/5] Loading seedling species records at those plots...\n")

seed <- read_parquet(here("05_fia/data/processed/summaries/plot_seedling_species.parquet")) |>
  as.data.table()
seed <- seed[!is.na(treecount_total) & treecount_total > 0,
             .(PLT_CN, INVYR, CONDID, SPCD, COMMON_NAME, treecount_total)]

# Sum across SUBP within (PLT_CN, INVYR, CONDID, SPCD) so each species appears
# once per condition, matching the grain used by the CWM builder.
seed <- seed[, .(seedling_count = sum(treecount_total, na.rm = TRUE),
                 COMMON_NAME    = first(COMMON_NAME)),
             by = .(PLT_CN, INVYR, CONDID, SPCD)]

setkey(seed, PLT_CN, INVYR, CONDID)
setkey(dist_keys, PLT_CN, INVYR, CONDID)
setkey(ctrl_keys, PLT_CN, INVYR, CONDID)

seed_dist <- seed[dist_keys, nomatch = NULL]
seed_ctrl <- seed[ctrl_keys, nomatch = NULL]
rm(seed); gc(verbose = FALSE)

cat(glue("       seedling rows at MBB disease plots: ",
         "{format(nrow(seed_dist), big.mark=',')}\n"))
cat(glue("       seedling rows at MBB control plots: ",
         "{format(nrow(seed_ctrl), big.mark=',')}\n"))

# ------------------------------------------------------------------------------
# 3. Per-plot species shares, then mean share per species across plots
# ------------------------------------------------------------------------------
# Per-plot share: each species's count divided by total seedlings on that plot.
# This makes each plot contribute equally and prevents a few high-count plots
# from dominating the comparison.

cat("[3/5] Computing per-plot species shares...\n")

add_plot_share <- function(dt) {
  dt[, plot_total := sum(seedling_count), by = .(PLT_CN, INVYR, CONDID)]
  dt[, plot_share := seedling_count / plot_total]
  dt
}

seed_dist <- add_plot_share(seed_dist)
seed_ctrl <- add_plot_share(seed_ctrl)

# Per species: mean per-plot share, and number of plots the species appears on.
# Plots where the species is absent (share = 0) need to be counted in the mean
# denominator. We treat "absent on a plot in the relevant pool" as share = 0.

n_dist_plots <- nrow(dist_keys)
n_ctrl_plots <- nrow(ctrl_keys)

# Sum of per-plot shares per species; mean = sum / total plots in pool.
spp_dist <- seed_dist[, .(
  sum_share        = sum(plot_share),
  n_plots_present  = uniqueN(paste(PLT_CN, INVYR, CONDID))
), by = .(SPCD, COMMON_NAME)]
spp_dist[, mean_share_dist := sum_share / n_dist_plots]

spp_ctrl <- seed_ctrl[, .(
  sum_share        = sum(plot_share),
  n_plots_present  = uniqueN(paste(PLT_CN, INVYR, CONDID))
), by = .(SPCD, COMMON_NAME)]
spp_ctrl[, mean_share_ctrl := sum_share / n_ctrl_plots]

# Merge: outer-join so species present in only one side are kept (with NA on
# the missing side, which we treat as zero share).
shift <- merge(
  spp_dist[, .(SPCD, COMMON_NAME, mean_share_dist,
               n_plots_present_dist = n_plots_present)],
  spp_ctrl[, .(SPCD, COMMON_NAME, mean_share_ctrl,
               n_plots_present_ctrl = n_plots_present)],
  by = c("SPCD", "COMMON_NAME"), all = TRUE
)

shift[is.na(mean_share_dist), mean_share_dist := 0]
shift[is.na(mean_share_ctrl), mean_share_ctrl := 0]
shift[is.na(n_plots_present_dist), n_plots_present_dist := 0L]
shift[is.na(n_plots_present_ctrl), n_plots_present_ctrl := 0L]

shift[, delta_share := mean_share_dist - mean_share_ctrl]

# ------------------------------------------------------------------------------
# 4. Join species traits for warm/cool tagging
# ------------------------------------------------------------------------------

cat("[4/5] Joining species climate-affinity traits...\n")

traits <- read_parquet(here("06_traits/data/processed/species_climate_affinity.parquet")) |>
  as.data.table()
traits_keep <- traits[, .(SPCD,
                          species_temp_mean = temp_mean,
                          n_occurrences)]

shift <- merge(shift, traits_keep, by = "SPCD", all.x = TRUE)

nat_median_temp <- median(traits$temp_mean, na.rm = TRUE)
shift[, temp_vs_national_median := round(species_temp_mean - nat_median_temp, 2)]

# Filter for headline display: keep species present on >= MIN_PLOTS_PRESENT
# plots on either side. Full species table is still written to parquet.
display <- shift[(n_plots_present_dist >= MIN_PLOTS_PRESENT |
                  n_plots_present_ctrl >= MIN_PLOTS_PRESENT)]

cat(glue("\n       National median species temp_mean: {round(nat_median_temp, 2)} deg C\n"))
cat(glue("       Species shown below: present on >= {MIN_PLOTS_PRESENT} plots on either side ",
         "({nrow(display)} of {nrow(shift)} total)\n\n"))

# Top 15 species *more abundant* at disease plots than control plots
top_up <- display[order(-delta_share)][seq_len(min(15L, .N))]
cat("       Top 15 species OVER-represented at MBB disease plots:\n")
print(top_up[, .(SPCD, COMMON_NAME,
                 mean_share_dist = round(mean_share_dist, 4),
                 mean_share_ctrl = round(mean_share_ctrl, 4),
                 delta_share     = round(delta_share, 4),
                 species_temp_mean = round(species_temp_mean, 2),
                 temp_vs_national_median,
                 n_plots_present_dist, n_plots_present_ctrl)])

# Top 15 species less abundant at disease plots (under-represented)
top_dn <- display[order(delta_share)][seq_len(min(15L, .N))]
cat("\n       Top 15 species UNDER-represented at MBB disease plots:\n")
print(top_dn[, .(SPCD, COMMON_NAME,
                 mean_share_dist = round(mean_share_dist, 4),
                 mean_share_ctrl = round(mean_share_ctrl, 4),
                 delta_share     = round(delta_share, 4),
                 species_temp_mean = round(species_temp_mean, 2),
                 temp_vs_national_median,
                 n_plots_present_dist, n_plots_present_ctrl)])

# Headline summary: are the over-represented species warmer than the under-
# represented ones? Weighted by abs(delta_share) so the species driving the
# CWM shift get more weight.
weighted_temp_up <- sum(top_up$delta_share * top_up$species_temp_mean,
                        na.rm = TRUE) / sum(top_up$delta_share, na.rm = TRUE)
weighted_temp_dn <- sum((-top_dn$delta_share) * top_dn$species_temp_mean,
                        na.rm = TRUE) / sum(-top_dn$delta_share, na.rm = TRUE)

cat(glue("\n       Share-weighted mean temp_mean of OVER-represented species:  ",
         "{round(weighted_temp_up, 2)} deg C\n"))
cat(glue("       Share-weighted mean temp_mean of UNDER-represented species: ",
         "{round(weighted_temp_dn, 2)} deg C\n"))
cat(glue("       Difference (UP minus DOWN): ",
         "{round(weighted_temp_up - weighted_temp_dn, 2)} deg C\n"))
cat("       (Positive difference means recruits gaining ground are warmer-",
    "affinity than recruits losing ground -- the species-level mechanism for ",
    "the +0.74 deg C MBB CWM signal.)\n", sep = "")

# ------------------------------------------------------------------------------
# 5. Write
# ------------------------------------------------------------------------------

cat("\n[5/5] Writing output...\n")

setorder(shift, -delta_share)

out_dir <- here("06_traits/data/processed")
dir_create(out_dir)
out_file <- file.path(out_dir, "disease_mbb_species_comparison.parquet")
write_parquet(as_tibble(shift), out_file, compression = "snappy")

cat("\nDone.\n")
cat(glue("Output: {out_file} ({file_size(out_file)})\n"))
cat(glue("Rows: {format(nrow(shift), big.mark=',')}\n"))
