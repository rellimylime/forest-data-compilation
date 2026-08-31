#!/usr/bin/env Rscript

# Build the canonical forest-normalized FIA condition disturbance measures.
# Each disturbance year is paired only with the disturbance code in the same
# FIA slot. Known-wrong development outputs are not retained as products.

suppressPackageStartupMessages({
  library(here)
  library(glue)
  library(data.table)
  library(arrow)
  library(fs)
})

source(here("scripts/utils/load_config.R"))
source(here("scripts/utils/parquet_atomic.R"))
source(here("scripts/utils/forest_analysis.R"))

config <- load_config()
fia_cfg <- config$processed$fia
link_cfg <- config$processed$disturbance_linkage
foundation_path <- file.path(
  here(fia_cfg$summaries$output_dir),
  fia_cfg$summaries$files$forested_condition_foundation
)
visit_context_path <- here(link_cfg$inputs$fia_plot_visit_context)
out_dir <- here(link_cfg$output_dir)
output_name <- link_cfg$files$fia_forest_disturbance_measures
if (is.null(output_name) || !nzchar(output_name)) {
  output_name <- "fia_forest_disturbance_measures.parquet"
}
out_path <- file.path(out_dir, output_name)
fire_evidence_path <- file.path(
  out_dir,
  link_cfg$files$fia_fire_disturbance_slot_evidence
)
qa_dir <- here(link_cfg$qa_dir)

if (!file.exists(foundation_path)) {
  stop(
    "Forested-condition foundation is missing: ", foundation_path, "\n",
      "Run: Rscript 05_fia/scripts/foundations/02_build_forested_condition_foundation.R"
  )
}
if (!file.exists(visit_context_path)) {
  stop(
    "FIA plot-visit context is missing: ", visit_context_path, "\n",
      "Run: Rscript 05_fia/scripts/foundations/01_build_plot_visit_context.R"
  )
}

# FIA disturbance extent is defined only over conditions classified as forest.
conditions <- as.data.table(read_parquet(foundation_path))
forest <- conditions[is_forested_condition == TRUE]
if (nrow(forest) == 0L) stop("No forested FIA conditions found.")
visit_context <- as.data.table(read_parquet(
  visit_context_path,
  col_select = c(
    "PLT_CN", "INVYR", "MEASYEAR", "MEASMON",
    "measurement_date_lower", "measurement_date_upper",
    "measurement_date_precision", "measurement_date_source"
  )
))

# Pair each fire code only with the year stored in the same FIA disturbance slot.
fire_evidence <- build_fia_fire_slot_evidence(forest, visit_context)

# Preserve rows that old INVYR-only validation would have rejected incorrectly.
fire_years_restored_by_measurement_timing <- merge(
  fire_evidence[
    valid_fire_year == TRUE &
      !is.na(INVYR) &
      disturbance_year > INVYR,
    .(
      stable_plot_id, state, PLT_CN, INVYR, CONDID,
      disturbance_slot, disturbance_code, disturbance_year,
      fire_year_validation_source, measurement_year_upper,
      new_validity_status = fire_year_status
    )
  ],
  visit_context[, .(
    PLT_CN, INVYR, MEASYEAR, MEASMON
  )],
  by = c("PLT_CN", "INVYR"),
  all.x = TRUE,
  sort = FALSE
)
fire_years_restored_by_measurement_timing[, `:=`(
  previous_validity_status = "post_inventory_year_under_previous_logic",
  is_after_measurement_year =
    !is.na(MEASYEAR) & disturbance_year > MEASYEAR,
  disturbance_minus_inventory_year = disturbance_year - INVYR,
  measurement_minus_inventory_year = MEASYEAR - INVYR
)]
setcolorder(
  fire_years_restored_by_measurement_timing,
  c(
    "stable_plot_id", "state", "PLT_CN", "CONDID",
    "disturbance_slot", "disturbance_code", "disturbance_year",
    "MEASYEAR", "MEASMON", "INVYR",
    "previous_validity_status", "new_validity_status",
    "fire_year_validation_source", "measurement_year_upper",
    "is_after_measurement_year",
    "disturbance_minus_inventory_year",
    "measurement_minus_inventory_year"
  )
)
setorder(
  fire_years_restored_by_measurement_timing,
  state, PLT_CN, INVYR, CONDID, disturbance_slot
)
# Collapse slot evidence to one timing summary per forested plot visit.
fire_summary <- fire_evidence[, .(
  valid_fire_years = collapse_sorted_values(
    disturbance_year[valid_fire_year == TRUE]
  ),
  first_valid_fire_year = if (any(valid_fire_year == TRUE)) {
    min(disturbance_year[valid_fire_year == TRUE])
  } else {
    NA_integer_
  },
  latest_valid_fire_year = if (any(valid_fire_year == TRUE)) {
    max(disturbance_year[valid_fire_year == TRUE])
  } else {
    NA_integer_
  },
  fire_disturbance_slot_evidence = paste(
    fire_disturbance_slot_evidence,
    collapse = ";"
  ),
  n_fire_disturbance_slots = .N,
  n_valid_fire_year_slots = sum(valid_fire_year),
  n_continuous_or_unknown_fire_year_slots =
    sum(fire_year_status == "continuous_or_unknown"),
  n_unavailable_fire_year_slots =
    sum(fire_year_status == "date_unavailable"),
  n_invalid_fire_year_slots =
    sum(fire_year_status == "invalid_year_code"),
  n_post_measurement_fire_year_slots =
    sum(fire_year_status == "post_measurement_year"),
  fire_year_validation_sources = paste(
    sort(unique(fire_year_validation_source)),
    collapse = ";"
  )
), by = .(stable_plot_id, PLT_CN, INVYR)]

# Retain all valid disturbance slots separately from the fire-only evidence.
general_evidence <- forest[
  !is.na(valid_disturbance_slots),
  .(
    general_disturbance_slot_evidence = paste(
      paste0("cond", CONDID, "/", valid_disturbance_slots),
      collapse = ";"
    )
  ),
  by = .(stable_plot_id, PLT_CN, INVYR)
]

# Sum each disturbed condition once, even when the same class appears in several slots.
measures <- forest[, .(
  forested_plot_proportion = first(forested_plot_proportion),
  n_forested_conditions = uniqueN(CONDID),
  has_any_fire = any(has_fire_condition),
  has_crown_fire = any(has_crown_fire_condition),
  has_insect_condition = any(has_insect_condition),
  has_disease_condition = any(has_disease_condition),
  prop_any_fire_whole_plot =
    sum(CONDPROP_UNADJ[has_fire_condition], na.rm = TRUE),
  prop_crown_fire_whole_plot =
    sum(CONDPROP_UNADJ[has_crown_fire_condition], na.rm = TRUE),
  forested_fire_numerator =
    sum(CONDPROP_UNADJ[has_fire_condition], na.rm = TRUE),
  forested_crown_fire_numerator =
    sum(CONDPROP_UNADJ[has_crown_fire_condition], na.rm = TRUE),
  forested_insect_numerator =
    sum(CONDPROP_UNADJ[has_insect_condition], na.rm = TRUE),
  forested_disease_numerator =
    sum(CONDPROP_UNADJ[has_disease_condition], na.rm = TRUE)
), by = .(
  stable_plot_id, PLT_CN, INVYR, state, STATECD,
  UNITCD, COUNTYCD, PLOT, PREV_PLT_CN, LAT, LON, ELEV
)]

measures <- merge(
  measures,
  fire_summary,
  by = c("stable_plot_id", "PLT_CN", "INVYR"),
  all.x = TRUE,
  sort = FALSE
)
measures <- merge(
  measures,
  general_evidence,
  by = c("stable_plot_id", "PLT_CN", "INVYR"),
  all.x = TRUE,
  sort = FALSE
)
count_cols <- c(
  "n_fire_disturbance_slots",
  "n_valid_fire_year_slots",
  "n_continuous_or_unknown_fire_year_slots",
  "n_unavailable_fire_year_slots",
  "n_invalid_fire_year_slots",
  "n_post_measurement_fire_year_slots"
)
for (column in count_cols) {
  set(measures, which(is.na(measures[[column]])), column, 0L)
}

# Divide by forested area so nonforest conditions never dilute disturbance extent.
measures[, `:=`(
  prop_any_fire_of_forested = fifelse(
    forested_plot_proportion > 0,
    forested_fire_numerator / forested_plot_proportion,
    NA_real_
  ),
  prop_crown_fire_of_forested = fifelse(
    forested_plot_proportion > 0,
    forested_crown_fire_numerator / forested_plot_proportion,
    NA_real_
  ),
  prop_insect_of_forested = fifelse(
    forested_plot_proportion > 0,
    forested_insect_numerator / forested_plot_proportion,
    NA_real_
  ),
  prop_disease_of_forested = fifelse(
    forested_plot_proportion > 0,
    forested_disease_numerator / forested_plot_proportion,
    NA_real_
  ),
  fire_extent_definition =
    "FIA_CONDPROP_UNADJ_normalized_over_forested_conditions",
  fire_timing_definition =
    "same_slot_DSTRBCD1-3_to_DSTRBYR1-3_measurement_year_validated"
)]
setorder(measures, state, stable_plot_id, INVYR, PLT_CN)

# Record timing restoration and proportion bounds in a compact generated QA table.
qa <- data.table(
  metric = c(
    "forest_plot_visits",
    "visits_with_any_fire",
    "visits_with_crown_fire",
    "visits_with_insect_condition",
    "visits_with_disease_condition",
    "fire_disturbance_slots",
    "valid_fire_year_slots",
    "continuous_or_unknown_fire_year_slots",
    "unavailable_fire_year_slots",
    "invalid_fire_year_slots",
    "post_measurement_fire_year_slots",
    "fire_year_slots_restored_using_measurement_year",
    "visits_with_fire_years_restored_using_measurement_year",
    "restored_fire_years_after_measurement_year",
    "maximum_restored_fire_year_minus_inventory_year",
    "max_prop_crown_fire_of_forested",
    "n_prop_crown_fire_out_of_bounds"
  ),
  value = c(
    nrow(measures),
    sum(measures$has_any_fire),
    sum(measures$has_crown_fire),
    sum(measures$has_insect_condition),
    sum(measures$has_disease_condition),
    sum(measures$n_fire_disturbance_slots),
    sum(measures$n_valid_fire_year_slots),
    sum(measures$n_continuous_or_unknown_fire_year_slots),
    sum(measures$n_unavailable_fire_year_slots),
    sum(measures$n_invalid_fire_year_slots),
    sum(measures$n_post_measurement_fire_year_slots),
    nrow(fire_years_restored_by_measurement_timing),
    uniqueN(
      fire_years_restored_by_measurement_timing,
      by = c("PLT_CN", "INVYR")
    ),
    sum(
      fire_years_restored_by_measurement_timing$is_after_measurement_year,
      na.rm = TRUE
    ),
    if (nrow(fire_years_restored_by_measurement_timing) > 0L) {
      max(
        fire_years_restored_by_measurement_timing$
          disturbance_minus_inventory_year,
        na.rm = TRUE
      )
    } else {
      NA_real_
    },
    max(measures$prop_crown_fire_of_forested, na.rm = TRUE),
    measures[
      prop_crown_fire_of_forested < 0 |
        prop_crown_fire_of_forested > 1 + 1e-8,
      .N
    ]
  )
)

dir_create(out_dir)
dir_create(qa_dir)
# Publish the visit summary and long fire-slot evidence only after both are complete.
write_parquet_atomic(measures, out_path, compression = "snappy")
fwrite(qa, file.path(qa_dir, "fia_forest_disturbance_summary.csv"))
fwrite(
  fire_years_restored_by_measurement_timing,
  file.path(
    qa_dir,
    "fia_fire_years_restored_by_measurement_timing.csv"
  )
)
write_parquet_atomic(
  fire_evidence,
  fire_evidence_path,
  compression = "snappy"
)

cat(glue(
  "Wrote {format(nrow(measures), big.mark = ',')} FIA forest visits -> ",
  "{out_path}\n"
))
cat(glue(
  "Wrote {format(nrow(fire_evidence), big.mark = ',')} fire-slot rows -> ",
  "{fire_evidence_path}\n"
))
