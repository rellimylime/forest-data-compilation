# ==============================================================================
# 03_extract_ids_agents.R
# Overlay IDS damage polygons with plot footprints to get dated, agent-level
# insect/disease events per plot -- including bark beetle, which FIA's condition
# disturbance code cannot isolate.
#
# Uses the real IDS schema (damage_areas: DCA_CODE, SURVEY_YEAR, ACRES). Source
# polygons are processed one survey year at a time, clipped to FIA footprints,
# and unioned within plot/year/agent before overlap area is calculated.
#
# Output grain: one row per stable_plot_id x survey_year x DCA_CODE
#   plot_ids_agent_events.parquet
#
# Input:  plot_footprints.gpkg (script 01)
#         01_ids/data/processed/ids_layers_cleaned.gpkg  (layer: damage_areas)
#         01_ids/lookups/dca_code_lookup.csv
#
# Usage:
#   Rscript 08_disturbance_linkage/scripts/03_extract_ids_agents.R
# ==============================================================================

suppressPackageStartupMessages({
  library(here)
  library(glue)
  library(data.table)
  library(arrow)
  library(sf)
  library(fs)
})

source(here("scripts/utils/load_config.R"))
source(here("scripts/utils/ids_overlap.R"))
source(here("scripts/utils/parquet_atomic.R"))

config <- load_config()
link_cfg <- config$processed$disturbance_linkage
area_crs <- link_cfg$area_crs

out_dir <- here(link_cfg$output_dir)
footprints_path <- file.path(out_dir, link_cfg$files$plot_footprints)
ids_path <- here(link_cfg$inputs$ids_layers)
dca_lookup_path <- here("01_ids/lookups/dca_code_lookup.csv")
out_path <- file.path(out_dir, link_cfg$files$plot_ids_agent_events)

if (!file.exists(footprints_path)) stop(glue("Run script 01 first: {footprints_path} missing."))
if (!file.exists(ids_path)) stop(glue("IDS layers not found: {ids_path}"))

cat("Extract IDS Agents\n==================\n\n")

footprints <- st_transform(st_read(footprints_path, quiet = TRUE), area_crs)
if (anyDuplicated(footprints$stable_plot_id)) {
  stop("Footprints must be unique by stable_plot_id before IDS extraction.")
}
footprint_areas <- data.table(
  stable_plot_id = footprints$stable_plot_id,
  footprint_area_m2 = as.numeric(st_area(footprints))
)

# The source contains millions of polygons, so process one survey year at a time
# rather than materializing the whole national layer. The SQL query keeps only
# fields needed for the overlap contract.
con <- DBI::dbConnect(RSQLite::SQLite(), ids_path)
survey_years <- DBI::dbGetQuery(
  con,
  paste0(
    "SELECT DISTINCT SURVEY_YEAR FROM damage_areas ",
    "WHERE SURVEY_YEAR IS NOT NULL ORDER BY SURVEY_YEAR"
  )
)$SURVEY_YEAR
DBI::dbDisconnect(con)

results <- vector("list", length(survey_years))
for (i in seq_along(survey_years)) {
  yr <- as.integer(survey_years[[i]])
  cat(glue("  [{i}/{length(survey_years)}] IDS survey year {yr}\n"))
  sql <- sprintf(
    paste0(
      "SELECT fid, geom, DCA_CODE, SURVEY_YEAR, ACRES ",
      "FROM damage_areas WHERE SURVEY_YEAR = %d"
    ),
    yr
  )
  damage <- st_read(ids_path, query = sql, quiet = TRUE)
  if (nrow(damage) == 0) next
  damage <- st_transform(damage, area_crs)
  damage <- damage[!is.na(damage$DCA_CODE), ]
  if (nrow(damage) == 0) next
  names(damage)[names(damage) == "DCA_CODE"] <- "dca_code"
  names(damage)[names(damage) == "SURVEY_YEAR"] <- "survey_year"
  names(damage)[names(damage) == "ACRES"] <- "source_polygon_acres"

  # st_intersection returns the actual clipped geometry. The helper unions
  # overlapping source polygons within an agent/year before measuring area.
  clipped <- suppressWarnings(st_intersection(
    footprints["stable_plot_id"],
    damage[c("survey_year", "dca_code", "source_polygon_acres")]
  ))
  if (nrow(clipped) == 0) next
  results[[i]] <- summarize_ids_intersections(clipped, footprint_areas)
  rm(damage, clipped)
  gc(verbose = FALSE)
}

events <- rbindlist(Filter(Negate(is.null), results), fill = TRUE)
if (nrow(events) == 0) {
  stop("No IDS polygons overlapped the configured FIA footprints.")
}

# Attach agent names and flag bark beetle.
dca <- fread(dca_lookup_path)
setnames(dca, c("DCA_CODE", "DCA_COMMON_NAME"), c("dca_code", "dca_common_name"), skip_absent = TRUE)
events <- merge(events, dca, by = "dca_code", all.x = TRUE)

# IDS bark-beetle causal agents share the 11000-11999 block (11000 "bark beetles",
# 11006 mountain pine beetle, 11009 spruce beetle, ...). TODO: reconcile the few
# edge codes (e.g. 80007 spruce beetle complex) against dca_code_lookup.csv.
events[, is_bark_beetle := !is.na(dca_code) & dca_code >= 11000L & dca_code < 12000L]
events[, linkage_method := "clipped_polygon_overlap_with_public_fia_buffer"]

setcolorder(events, c(
  "stable_plot_id", "survey_year", "dca_code", "dca_common_name",
  "is_bark_beetle", "overlap_acres", "footprint_overlap_fraction",
  "overlap_area_m2", "footprint_area_m2", "n_source_polygons",
  "source_polygon_acres_sum", "linkage_method"
))
setorder(events, stable_plot_id, survey_year, dca_code)

dir_create(out_dir)
write_parquet_atomic(events, out_path, compression = "snappy")

cat(glue("Wrote {format(nrow(events), big.mark = ',')} plot-year-agent events -> {out_path}"), "\n")
cat(glue("Bark-beetle events: {format(sum(events$is_bark_beetle), big.mark = ',')}"), "\n")
