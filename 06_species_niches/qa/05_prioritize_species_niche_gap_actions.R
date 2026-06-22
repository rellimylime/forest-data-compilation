# ==============================================================================
# 05_prioritize_species_niche_gap_actions.R
# Prioritize species niche gaps for manual review before modeling.
#
# This script reads the gap ledger from 04_document_species_niche_gaps.R and
# classifies missing species into practical action categories. It does not change
# the niche data. It creates a review queue that can be used to decide which
# species need synonym fixes, genus-level treatment, exclusion, or documentation.
#
# Usage:
#   Rscript 06_species_niches/qa/05_prioritize_species_niche_gap_actions.R
# ==============================================================================

suppressPackageStartupMessages({
  library(here)
  library(glue)
  library(data.table)
  library(fs)
})

qa_dir <- here("06_species_niches/qa/outputs")
dir_create(qa_dir)

ledger_path <- file.path(qa_dir, "species_niche_gap_ledger.csv")
if (!file.exists(ledger_path)) {
  stop("Gap ledger not found. Run 06_species_niches/qa/04_document_species_niche_gaps.R first.")
}

ledger <- fread(ledger_path)

required_cols <- c(
  "species_key", "source_code_system", "source_species_code",
  "scientific_name", "common_name", "community_layers", "gap_stage",
  "cwm_missing_weight_total", "cwm_missing_seedlings_tpa"
)
missing_cols <- setdiff(required_cols, names(ledger))
if (length(missing_cols) > 0) {
  stop(glue("Gap ledger is missing required columns: {paste(missing_cols, collapse = ', ')}"))
}

ledger[, cwm_missing_weight_total := fifelse(
  is.na(cwm_missing_weight_total),
  0,
  as.numeric(cwm_missing_weight_total)
)]
ledger[, cwm_missing_seedlings_tpa := fifelse(
  is.na(cwm_missing_seedlings_tpa),
  0,
  as.numeric(cwm_missing_seedlings_tpa)
)]

ledger[, scientific_name_lower := tolower(scientific_name)]
ledger[, is_sp_spp_name := grepl("\\b(sp|spp)\\.?\\b", scientific_name_lower)]
ledger[, is_genus_or_pseudo := gap_stage == "not_targeted_for_niche"]
ledger[, is_infraspecific := grepl("\\b(var|ssp|subsp)\\.?\\b", scientific_name_lower)]
ledger[, is_unknown_tree_code := grepl("unknown|tree broadleaf|tree needleleaf", scientific_name_lower)]

ledger[, recommended_action := fifelse(
  gap_stage == "usable_study_area_niche",
  "no_action",
  fifelse(
    is_unknown_tree_code == TRUE,
    "document_exclusion_unknown_taxon",
    fifelse(
      is_sp_spp_name == TRUE,
      "exclude_sp_spp_from_main_cwm",
      fifelse(
        is_genus_or_pseudo == TRUE,
        "exclude_or_genus_level_sensitivity",
      fifelse(
        gap_stage == "no_bien_range_map" & is_infraspecific == TRUE,
        "review_infraspecific_parent_species_fallback",
        fifelse(
          gap_stage == "no_bien_range_map",
          "try_synonym_or_alternate_range_source",
          fifelse(
            gap_stage %in% c("outside_or_empty_after_study_area_clip", "range_climate_or_study_area_niche_missing"),
            "document_study_area_or_climate_gap",
            "manual_review"
          )
        )
      )
    )
    )
  )
)]

ledger[, priority := fifelse(
  cwm_missing_weight_total >= 1000000,
  "high",
  fifelse(
    cwm_missing_weight_total >= 100000,
    "medium",
    fifelse(cwm_missing_weight_total > 0, "low", "documentation")
  )
)]

action_queue <- ledger[
  gap_stage != "usable_study_area_niche",
  .(
    priority,
    recommended_action,
    species_key,
    source_code_system,
    source_species_code,
    scientific_name,
    common_name,
    community_layers,
    gap_stage,
    is_sp_spp_name,
    cwm_missing_weight_total,
    cwm_missing_seedlings_tpa,
    range_lookup_status,
    range_match_status,
    range_review_reason,
    gap_reason
  )
][order(
  factor(priority, levels = c("high", "medium", "low", "documentation")),
  recommended_action,
  -cwm_missing_weight_total
)]

action_summary <- action_queue[
  ,
  .(
    n_species = .N,
    n_seedling_cwm_missing_species = sum(cwm_missing_weight_total > 0),
    total_cwm_missing_weight = sum(cwm_missing_weight_total, na.rm = TRUE)
  ),
  by = .(priority, recommended_action, gap_stage)
][order(
  factor(priority, levels = c("high", "medium", "low", "documentation")),
  recommended_action,
  gap_stage
)]

fwrite(action_queue, file.path(qa_dir, "species_niche_gap_action_queue.csv"))
fwrite(action_summary, file.path(qa_dir, "species_niche_gap_action_summary.csv"))

cat("Done.\n")
cat(glue("Action queue:   {file.path(qa_dir, 'species_niche_gap_action_queue.csv')}"), "\n")
cat(glue("Action summary: {file.path(qa_dir, 'species_niche_gap_action_summary.csv')}"), "\n\n")
print(action_summary)
