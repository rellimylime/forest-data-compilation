# ==============================================================================
# 04_document_species_niche_gaps.R
# Build an explicit gap ledger for the species niche workflow.
#
# This QA script does not create analysis inputs. It explains why species are
# absent from later products:
#
#   species universe -> BIEN range availability -> range climate -> niche table
#                    -> FIA seedling CWM join
#
# The outputs are meant for documentation, handoff, and model filtering decisions.
#
# Usage:
#   Rscript 06_species_niches/qa/04_document_species_niche_gaps.R
# ==============================================================================

suppressPackageStartupMessages({
  library(here)
  library(glue)
  library(data.table)
  library(arrow)
  library(fs)
})

source(here("scripts/utils/load_config.R"))

config <- load_config()
niche_dir <- here(config$processed$species_niches$output_dir)
qa_dir <- here("06_species_niches/qa/outputs")
thermo_qa_dir <- here("07_thermophilization/qa/outputs")
dir_create(qa_dir)

paths <- list(
  species_universe = file.path(niche_dir, "species_universe.parquet"),
  bien_availability = file.path(niche_dir, "bien_range_availability.parquet"),
  global_niches = file.path(niche_dir, "species_climate_niches.parquet"),
  study_area_niches = file.path(niche_dir, "species_climate_niches_us_study_area.parquet"),
  study_area_range_failures = file.path(qa_dir, "species_range_climate_failures_us_study_area.csv"),
  study_area_niche_missing = file.path(qa_dir, "species_climate_niches_missing_us_study_area.csv"),
  cwm_missing_species = file.path(thermo_qa_dir, "plot_recruitment_cwm_missing_species.csv")
)

validation_checks_path <- file.path(qa_dir, "species_niche_validation_checks.csv")

missing_inputs <- names(paths)[!file.exists(unlist(paths))]
if (length(missing_inputs) > 0) {
  stop(glue("Missing required input(s): {paste(missing_inputs, collapse = ', ')}"))
}

if (!file.exists(validation_checks_path)) {
  stop("Run 06_species_niches/qa/01_validate_species_niche_workflow.R before documenting final gaps.")
}

validation_checks <- fread(validation_checks_path)
failed_required <- validation_checks[severity == "error" & status == "fail"]
if (nrow(failed_required) > 0) {
  stop("Required species-niche validation checks are failing. Fix those before documenting final gaps.")
}

stale_handoff_checks <- validation_checks[
  check_name %in% c("range_climate_species_match_polygons", "compact_niche_species_match_polygons") &
    status != "pass"
]
if (nrow(stale_handoff_checks) > 0) {
  stop(
    "Range-climate or compact niche products are stale relative to current BIEN polygons. ",
    "Rerun scripts 04 and 05, then rerun validation before documenting final gaps."
  )
}

read_csv_dt <- function(path) {
  if (file.size(path) == 0) return(data.table())
  fread(path)
}

universe <- as.data.table(read_parquet(paths$species_universe))
availability <- as.data.table(read_parquet(paths$bien_availability))
global_niches <- as.data.table(read_parquet(paths$global_niches))
study_area_niches <- as.data.table(read_parquet(paths$study_area_niches))
range_failures <- read_csv_dt(paths$study_area_range_failures)
niche_missing <- read_csv_dt(paths$study_area_niche_missing)
cwm_missing <- read_csv_dt(paths$cwm_missing_species)

base_cols <- c(
  "species_key", "source_code_system", "source_species_code",
  "scientific_name", "common_name", "community_layers", "growth_habits",
  "n_states", "n_plot_visits", "n_conditions", "abundance_total",
  "source_tables", "is_pseudo_taxon", "needs_niche"
)

ledger <- copy(universe[, intersect(base_cols, names(universe)), with = FALSE])

availability_keep <- availability[
  ,
  .(
    species_key,
    bien_query_name,
    bien_range_available,
    range_lookup_status,
    range_match_status,
    needs_range_review,
    range_review_reason,
    range_lookup_error
  )
]
ledger <- merge(ledger, availability_keep, by = "species_key", all.x = TRUE)

ledger[, has_global_niche := species_key %in% global_niches$species_key]
ledger[, has_study_area_niche := species_key %in% study_area_niches$species_key]
ledger[, in_cwm_missing_species := species_key %in% cwm_missing$species_key]

if (nrow(cwm_missing) > 0) {
  cwm_keep <- cwm_missing[
    ,
    .(
      species_key,
      cwm_missing_n_condition_rows = n_condition_rows,
      cwm_missing_treecount_total = treecount_total,
      cwm_missing_seedlings_tpa = seedlings_tpa,
      cwm_missing_weight_total = cwm_weight_total
    )
  ]
  ledger <- merge(ledger, cwm_keep, by = "species_key", all.x = TRUE)
}

ledger[, gap_stage := fifelse(
  is.na(needs_niche) | needs_niche == FALSE,
  "not_targeted_for_niche",
  fifelse(
    is.na(bien_range_available),
    "not_checked_against_bien",
    fifelse(
      bien_range_available == FALSE,
      "no_bien_range_map",
      fifelse(
        has_study_area_niche == TRUE,
        "usable_study_area_niche",
        fifelse(
          has_global_niche == TRUE,
          "outside_or_empty_after_study_area_clip",
          "range_climate_or_study_area_niche_missing"
        )
      )
    )
  )
)]

ledger[, gap_reason := fifelse(
  gap_stage == "not_targeted_for_niche",
  "Species record is a pseudo taxon, lacks a scientific name, or was otherwise marked as not requiring a niche.",
  fifelse(
    gap_stage == "not_checked_against_bien",
    "Species was in the universe but not present in the BIEN availability product.",
    fifelse(
      gap_stage == "no_bien_range_map",
      range_review_reason,
      fifelse(
        gap_stage == "range_climate_or_study_area_niche_missing",
        "BIEN reported a range, but the study-area range climate or compact niche product does not contain this species.",
        fifelse(
          gap_stage == "outside_or_empty_after_study_area_clip",
          "A global BIEN range/niche exists, but clipping to the configured all-U.S. study-area bounding box left no usable TerraClimate rows.",
          fifelse(
            gap_stage == "available_niche_but_missing_from_cwm_join",
            "The species appears in the CWM missing-species QA. This usually indicates a synonym/code mismatch, pseudo taxon, or a CWM table built before the niche was available.",
            "Species has a study-area-clipped niche and is available for downstream analysis."
          )
        )
      )
    )
  )
)]

if (nrow(range_failures) > 0) {
  failure_keep <- range_failures[
    ,
    .(
      species_key,
      study_area_range_failure_stage = failure_stage,
      study_area_range_failure_reason = failure_reason
    )
  ]
  ledger <- merge(ledger, failure_keep, by = "species_key", all.x = TRUE)
} else {
  ledger[, `:=`(
    study_area_range_failure_stage = NA_character_,
    study_area_range_failure_reason = NA_character_
  )]
}

if (nrow(niche_missing) > 0) {
  niche_missing_keep <- niche_missing[
    ,
    .(
      species_key,
      study_area_niche_missing_indicators = paste(
        names(.SD)[which(is.na(.SD[1]))],
        collapse = "; "
      )
    ),
    .SDcols = intersect(
      c(
        "tmean_annual_mean", "tmean_warmest_month_mean",
        "tmean_coldest_month_mean", "temp_seasonality_mean",
        "cwd_annual_sum", "cwd_max_month_mean", "pr_annual_sum",
        "pr_driest_month_mean"
      ),
      names(niche_missing)
    ),
    by = species_key
  ]
  ledger <- merge(ledger, niche_missing_keep, by = "species_key", all.x = TRUE)
} else {
  ledger[, study_area_niche_missing_indicators := NA_character_]
}

summary <- ledger[
  ,
  .(
    n_species = .N,
    n_seedling_cwm_missing_species = sum(in_cwm_missing_species == TRUE, na.rm = TRUE),
    total_cwm_missing_weight = sum(cwm_missing_weight_total, na.rm = TRUE),
    total_cwm_missing_seedlings_tpa = sum(cwm_missing_seedlings_tpa, na.rm = TRUE)
  ),
  by = gap_stage
][order(gap_stage)]

top_cwm_gaps <- ledger[
  in_cwm_missing_species == TRUE,
  .(
    species_key,
    source_species_code,
    scientific_name,
    common_name,
    gap_stage,
    gap_reason,
    cwm_missing_n_condition_rows,
    cwm_missing_weight_total,
    range_lookup_status,
    range_match_status,
    range_review_reason
  )
][order(-cwm_missing_weight_total)]

fwrite(ledger[order(gap_stage, species_key)], file.path(qa_dir, "species_niche_gap_ledger.csv"))
fwrite(summary, file.path(qa_dir, "species_niche_gap_summary.csv"))
fwrite(top_cwm_gaps, file.path(qa_dir, "species_niche_top_cwm_gaps.csv"))

cat("Done.\n")
cat(glue("Gap ledger:       {file.path(qa_dir, 'species_niche_gap_ledger.csv')}"), "\n")
cat(glue("Gap summary:      {file.path(qa_dir, 'species_niche_gap_summary.csv')}"), "\n")
cat(glue("Top CWM gaps:     {file.path(qa_dir, 'species_niche_top_cwm_gaps.csv')}"), "\n\n")
print(summary)
