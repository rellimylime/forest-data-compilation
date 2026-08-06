#!/usr/bin/env Rscript

# Associate every MTBS perimeter touching an 800 m public-coordinate buffer.
# This workflow uses MTBS for event timing/history only; it does not read or
# classify burn-severity rasters.

suppressPackageStartupMessages({
  library(here)
  library(glue)
  library(data.table)
  library(arrow)
  library(sf)
  library(fs)
})

source(here("scripts/utils/load_config.R"))
source(here("scripts/utils/parquet_atomic.R"))

config <- load_config()
link_cfg <- config$processed$disturbance_linkage
out_dir <- here(link_cfg$output_dir)
qa_dir <- here(link_cfg$qa_dir)
footprints_path <- file.path(out_dir, link_cfg$files$plot_footprints)
mtbs_path <- here(link_cfg$inputs$mtbs_fire_perimeters)
evidence_path <- file.path(
  out_dir,
  link_cfg$files$mtbs_fire_event_evidence
)

dir_create(qa_dir)
readiness_path <- file.path(qa_dir, "mtbs_fire_history_input_readiness.csv")
# Record missing inputs explicitly instead of downloading or substituting a source.
readiness <- data.table(
  input = c(
    "plot_footprints",
    "mtbs_fire_perimeters"
  ),
  path = c(
    footprints_path,
    mtbs_path
  )
)
readiness[, available := file.exists(path)]
fwrite(readiness, readiness_path)

if (!file.exists(mtbs_path)) {
  stop(
    "MTBS perimeter input is unavailable: ", mtbs_path, "\n",
    "Place a vector perimeter layer at that configured path. The workflow did ",
    "not substitute severity rasters or enlarge the 800 m buffer.\n",
    "Readiness QA: ", readiness_path
  )
}
for (path in c(footprints_path)) {
  if (!file.exists(path)) stop("Required input is missing: ", path)
}

# Accept common MTBS field-name variants while keeping required fields strict.
pick_field <- function(fields, candidates, required = TRUE) {
  match <- candidates[candidates %in% fields][1]
  if (is.na(match) && required) {
    stop(
      "MTBS perimeter layer lacks a required field. Tried: ",
      paste(candidates, collapse = ", ")
    )
  }
  match
}

# The denominator for overlap is the full 800 m search buffer area.
footprints <- st_read(footprints_path, quiet = TRUE)
footprints$search_buffer_area_m2 <- as.numeric(st_area(footprints))
fires <- st_read(mtbs_path, quiet = TRUE)
fire_fields <- names(fires)
event_id_field <- pick_field(
  fire_fields,
  c("Event_ID", "EVENT_ID", "event_id", "Fire_ID", "FIRE_ID", "Incid_Num")
)
perimeter_id_field <- pick_field(
  fire_fields,
  c(
    "Map_ID", "MAP_ID", "map_id",
    "Event_ID", "EVENT_ID", "event_id", "Fire_ID", "FIRE_ID"
  ),
  required = FALSE
)
name_field <- pick_field(
  fire_fields,
  c("Incid_Name", "INCIDENT_NAME", "Fire_Name", "FIRE_NAME", "fire_name"),
  required = FALSE
)
date_field <- pick_field(
  fire_fields,
  c("Ig_Date", "IG_DATE", "StartDate", "START_DATE", "fire_date"),
  required = FALSE
)
year_field <- pick_field(
  fire_fields,
  c("Ig_Year", "IG_YEAR", "YEAR", "Fire_Year", "FIRE_YEAR", "fire_year"),
  required = is.na(date_field)
)
type_field <- pick_field(
  fire_fields,
  c("Fire_Type", "FIRE_TYPE", "fire_type"),
  required = FALSE
)
acres_field <- pick_field(
  fire_fields,
  c("Acres", "ACRES", "fire_acres"),
  required = FALSE
)

fires$mtbs_event_id <- as.character(fires[[event_id_field]])
fires$mtbs_map_id <- if (!is.na(perimeter_id_field)) {
  as.character(fires[[perimeter_id_field]])
} else {
  as.character(seq_len(nrow(fires)))
}
fires$fire_name <- if (!is.na(name_field)) {
  as.character(fires[[name_field]])
} else {
  NA_character_
}
# Parse known MTBS date encodings without inventing a day for year-only values.
parse_mtbs_date <- function(values) {
  text <- trimws(as.character(values))
  text[text %in% c("", "NA", "NULL")] <- NA_character_
  output <- as.IDate(rep(NA_character_, length(text)))
  for (format in c("%Y%m%d", "%Y-%m-%d", "%m/%d/%Y")) {
    missing <- is.na(output) & !is.na(text)
    output[missing] <- suppressWarnings(as.IDate(text[missing], format = format))
  }
  output
}
event_date <- if (!is.na(date_field)) {
  parse_mtbs_date(fires[[date_field]])
} else {
  as.IDate(rep(NA_character_, nrow(fires)))
}
event_year <- if (!is.na(year_field)) {
  as.integer(fires[[year_field]])
} else {
  as.integer(format(event_date, "%Y"))
}
event_year[is.na(event_year) & !is.na(event_date)] <-
  as.integer(format(event_date[is.na(event_year) & !is.na(event_date)], "%Y"))
fires$fire_year <- event_year
fires$fire_ignition_date <- event_date
fires$fire_type <- if (!is.na(type_field)) {
  as.character(fires[[type_field]])
} else {
  NA_character_
}
fires$mtbs_burn_area_acres <- if (!is.na(acres_field)) {
  as.numeric(fires[[acres_field]])
} else {
  NA_real_
}
keep_fire_cols <- c(
  "mtbs_event_id", "mtbs_map_id", "fire_name", "fire_type",
  "mtbs_burn_area_acres", "fire_year", "fire_ignition_date"
)
fires <- fires[, keep_fire_cols]
# Reproject perimeters to the footprint CRS before measuring overlap area.
fires <- st_transform(fires, st_crs(footprints))
fires <- fires[!is.na(fires$mtbs_event_id), ]

cat("Intersecting MTBS perimeters with 800 m FIA buffers...\n")
# Any perimeter contact with the search buffer is retained as event evidence.
clipped <- suppressWarnings(st_intersection(
  footprints[c("stable_plot_id", "search_buffer_area_m2")],
  fires
))
if (nrow(clipped) == 0L) {
  stop("No MTBS fire perimeter touched an FIA 800 m footprint.")
}
# This fraction describes the search buffer, not an FIA condition or fire perimeter.
clipped$search_buffer_overlap_area_m2 <- as.numeric(st_area(clipped))
clipped$search_buffer_overlap_fraction <-
  pmin(
    1,
    clipped$search_buffer_overlap_area_m2 / clipped$search_buffer_area_m2
  )
evidence <- as.data.table(st_drop_geometry(clipped))
# Merge multipart pieces into one row per stable plot and MTBS event.
evidence <- evidence[, .(
  mtbs_map_id = first(mtbs_map_id),
  fire_name = first(fire_name),
  fire_type = first(fire_type),
  mtbs_burn_area_acres = first(mtbs_burn_area_acres),
  fire_year = first(fire_year),
  fire_ignition_date = first(fire_ignition_date),
  search_buffer_overlap_area_m2 = sum(
    search_buffer_overlap_area_m2,
    na.rm = TRUE
  ),
  search_buffer_overlap_fraction = min(
    1,
    sum(search_buffer_overlap_fraction, na.rm = TRUE)
  )
), by = .(stable_plot_id, mtbs_event_id)]
setorder(evidence, stable_plot_id, fire_ignition_date, mtbs_event_id)

dir_create(out_dir)
# Keep the event table neutral: all fire types and every touching event remain.
write_parquet_atomic(evidence, evidence_path, compression = "snappy")
qa <- rbindlist(list(
  data.table(metric = c(
    "plot_event_rows", "stable_plots_with_event", "distinct_mtbs_events",
    "minimum_fire_year", "maximum_fire_year", "duplicate_plot_event_keys",
    "missing_fire_dates", "missing_mtbs_map_ids"
  ), value = as.character(c(
    nrow(evidence), uniqueN(evidence$stable_plot_id),
    uniqueN(evidence$mtbs_event_id), min(evidence$fire_year, na.rm = TRUE),
    max(evidence$fire_year, na.rm = TRUE),
    nrow(evidence) - uniqueN(paste(evidence$stable_plot_id, evidence$mtbs_event_id)),
    sum(is.na(evidence$fire_ignition_date)),
    sum(is.na(evidence$mtbs_map_id) | evidence$mtbs_map_id == "")
  ))),
  data.table(
    metric = paste0(
      "search_buffer_overlap_fraction_",
      c("minimum", "p01", "p05", "median", "p95", "p99", "maximum")
    ),
    value = as.character(as.numeric(quantile(
      evidence$search_buffer_overlap_fraction,
      probs = c(0, 0.01, 0.05, 0.5, 0.95, 0.99, 1),
      na.rm = TRUE,
      names = FALSE
    )))
  )
))
fwrite(qa, file.path(qa_dir, "mtbs_fire_event_evidence_qa.csv"))
cat(glue("Evidence rows: {format(nrow(evidence), big.mark = ',')}\n"))
