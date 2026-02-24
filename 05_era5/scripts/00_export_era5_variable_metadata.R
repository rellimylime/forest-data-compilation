#!/usr/bin/env Rscript

# ==============================================================================
# 05_era5/scripts/00_export_era5_variable_metadata.R
# Build a metadata CSV for configured ERA5 variables (one row per variable)
# using project config + curated metadata from the ERA5 documentation tables.
# ==============================================================================

suppressPackageStartupMessages({
  library(here)
  library(yaml)
})

source(here("scripts/utils/load_config.R"))
source(here("scripts/utils/cds_utils.R"))  # monthly support filter helper

config <- load_config()
era5_cfg <- config$raw$era5
time_cfg <- config$params$time_range

vars <- era5_cfg$variables
var_names <- names(vars)

# ------------------------------------------------------------------------------
# Curated ERA5 docs metadata (mapped from ERA5 data documentation parameter tables)
# ------------------------------------------------------------------------------

table_lookup <- c(
  # Table 2: instantaneous
  t2m = "Table 2 (surface/single level: instantaneous)",
  d2m = "Table 2 (surface/single level: instantaneous)",
  sd = "Table 2 (surface/single level: instantaneous)",
  sp = "Table 2 (surface/single level: instantaneous)",
  u10 = "Table 2 (surface/single level: instantaneous)",
  v10 = "Table 2 (surface/single level: instantaneous)",
  u100 = "Table 2 (surface/single level: instantaneous)",
  v100 = "Table 2 (surface/single level: instantaneous)",
  stl1 = "Table 2 (surface/single level: instantaneous)",
  stl2 = "Table 2 (surface/single level: instantaneous)",
  stl3 = "Table 2 (surface/single level: instantaneous)",
  stl4 = "Table 2 (surface/single level: instantaneous)",
  swvl1 = "Table 2 (surface/single level: instantaneous)",
  swvl2 = "Table 2 (surface/single level: instantaneous)",
  swvl3 = "Table 2 (surface/single level: instantaneous)",
  swvl4 = "Table 2 (surface/single level: instantaneous)",
  lai_hv = "Table 2 (surface/single level: instantaneous)",
  lai_lv = "Table 2 (surface/single level: instantaneous)",
  skt = "Table 2 (surface/single level: instantaneous)",
  snowc = "Table 2 (surface/single level: instantaneous)",
  tcc = "Table 2 (surface/single level: instantaneous)",
  msl = "Table 2 (surface/single level: instantaneous)",
  cape = "Table 2 (surface/single level: instantaneous)",
  blh = "Table 2 (surface/single level: instantaneous)",
  lcc = "Table 2 (surface/single level: instantaneous)",
  mcc = "Table 2 (surface/single level: instantaneous)",
  hcc = "Table 2 (surface/single level: instantaneous)",
  fal = "Table 2 (surface/single level: instantaneous)",
  rsn = "Table 2 (surface/single level: instantaneous)",

  # Table 3: accumulations
  tp = "Table 3 (surface/single level: accumulations)",
  sf = "Table 3 (surface/single level: accumulations)",
  ssrd = "Table 3 (surface/single level: accumulations)",
  ssr = "Table 3 (surface/single level: accumulations)",
  str = "Table 3 (surface/single level: accumulations)",
  e = "Table 3 (surface/single level: accumulations)",
  pev = "Table 3 (surface/single level: accumulations)",
  smlt = "Table 3 (surface/single level: accumulations)",
  ro = "Table 3 (surface/single level: accumulations)",
  sro = "Table 3 (surface/single level: accumulations)",
  strd = "Table 3 (surface/single level: accumulations)",
  cp = "Table 3 (surface/single level: accumulations)",
  sshf = "Table 3 (surface/single level: accumulations)",
  slhf = "Table 3 (surface/single level: accumulations)",
  fdir = "Table 3 (surface/single level: accumulations)",

  # Table 5: min/max since previous post processing
  mn2t = "Table 5 (surface/single level: minimum/maximum)",
  mx2t = "Table 5 (surface/single level: minimum/maximum)",
  i10fg = "Table 5 (surface/single level: minimum/maximum)",

  # Table 6: vertical integrals / total column
  tcwv = "Table 6 (surface/single level: vertical integrals and total column)"
)

type_lookup <- c(
  t2m = "temperature", d2m = "temperature", skt = "temperature",
  stl1 = "soil temperature", stl2 = "soil temperature",
  stl3 = "soil temperature", stl4 = "soil temperature",
  mn2t = "temperature extrema", mx2t = "temperature extrema",
  tp = "precipitation", cp = "precipitation", sf = "snow/precipitation",
  sd = "snow", snowc = "snow", smlt = "snow", rsn = "snow",
  e = "evaporation", pev = "evaporation",
  ro = "runoff", sro = "runoff",
  sp = "pressure", msl = "pressure",
  u10 = "wind", v10 = "wind", u100 = "wind", v100 = "wind",
  i10fg = "wind",
  ssrd = "radiation", ssr = "radiation", str = "radiation",
  strd = "radiation", fdir = "radiation",
  sshf = "surface energy flux", slhf = "surface energy flux",
  swvl1 = "soil moisture", swvl2 = "soil moisture",
  swvl3 = "soil moisture", swvl4 = "soil moisture",
  lai_hv = "vegetation", lai_lv = "vegetation", fal = "surface properties",
  tcc = "clouds", lcc = "clouds", mcc = "clouds", hcc = "clouds",
  tcwv = "atmospheric composition / moisture",
  cape = "convection / instability",
  blh = "boundary layer"
)

temporal_nature_lookup <- c(
  t2m = "instantaneous", d2m = "instantaneous", sd = "instantaneous",
  sp = "instantaneous", u10 = "instantaneous", v10 = "instantaneous",
  u100 = "instantaneous", v100 = "instantaneous", stl1 = "instantaneous",
  stl2 = "instantaneous", stl3 = "instantaneous", stl4 = "instantaneous",
  swvl1 = "instantaneous", swvl2 = "instantaneous", swvl3 = "instantaneous",
  swvl4 = "instantaneous", lai_hv = "instantaneous", lai_lv = "instantaneous",
  skt = "instantaneous", snowc = "instantaneous", tcc = "instantaneous",
  msl = "instantaneous", cape = "instantaneous", blh = "instantaneous",
  lcc = "instantaneous", mcc = "instantaneous", hcc = "instantaneous",
  fal = "instantaneous", rsn = "instantaneous",
  tp = "accumulation", sf = "accumulation", ssrd = "accumulation",
  ssr = "accumulation", str = "accumulation", e = "accumulation",
  pev = "accumulation", smlt = "accumulation", ro = "accumulation",
  sro = "accumulation", strd = "accumulation", cp = "accumulation",
  sshf = "accumulation", slhf = "accumulation", fdir = "accumulation",
  mn2t = "minimum/maximum since previous post-processing",
  mx2t = "minimum/maximum since previous post-processing",
  i10fg = "minimum/maximum since previous post-processing",
  tcwv = "total column instantaneous"
)

monthly_unavailable_table8 <- c("mn2t", "mx2t", "i10fg")

computed_in_era5 <- list(
  snowc = list(
    computed_from = "sd, rsn",
    method = "snow_cover = min(1, (1000 * sd / rsn) / 0.1)",
    notes = "ERA5-provided diagnostic snow cover fraction computed from snow depth and snow density; MARS long and short names can be ambiguous."
  )
)

analysis_notes <- list(
  fal = "Forecast albedo is a forecast parameter. Not equivalent to broadband albedo from net/downward shortwave fluxes.",
  fdir = "Direct-beam shortwave at surface; diffuse shortwave can be derived as ssrd - fdir.",
  cp = "Large-scale precipitation can be derived as tp - cp.",
  rsn = "Use with sd to estimate physical snow depth (sd in m water equivalent): depth_m ~= sd * 1000 / rsn.",
  e = "MARS ambiguity observed for CDS variable name 'total_evaporation'; use MARS fallback short name 'e'.",
  snowc = "MARS ambiguity observed for both CDS and short name in some requests; may require paramId fallback if short-name retry fails."
)

outdated_shortname_note <- paste(
  "ERA5 documentation notes some shortNames in the tables may be outdated;",
  "use the ECMWF parameter database as the official reference."
)

default_sources <- c(
  "https://confluence.ecmwf.int/display/CKB/ERA5%3A%2Bdata%2Bdocumentation#heading-Parameterlistings",
  "https://apps.ecmwf.int/codes/grib/param-db/"
)

# ------------------------------------------------------------------------------
# Helpers
# ------------------------------------------------------------------------------

`%||%` <- function(x, y) if (is.null(x)) y else x

as_flag <- function(x) if (isTRUE(x)) "TRUE" else "FALSE"

monthly_status_for <- function(var_name, era5_name) {
  if (var_name %in% monthly_unavailable_table8 || is_unsupported_monthly_mean_variable(era5_name)) {
    return("not available in CDS monthly means (Table 8 exception / no mean)")
  }
  return("available in CDS monthly means")
}

raw_or_computed_for <- function(var_name) {
  if (var_name %in% names(computed_in_era5)) "computed_in_ERA5" else "raw_ERA5_parameter"
}

computed_from_for <- function(var_name) {
  if (!(var_name %in% names(computed_in_era5))) return("")
  computed_in_era5[[var_name]]$computed_from %||% ""
}

computed_method_for <- function(var_name) {
  if (!(var_name %in% names(computed_in_era5))) return("")
  computed_in_era5[[var_name]]$method %||% ""
}

docs_note_for <- function(var_name) {
  bits <- character()
  if (var_name %in% names(computed_in_era5)) {
    bits <- c(bits, computed_in_era5[[var_name]]$notes %||% "")
  }
  if (var_name %in% names(analysis_notes)) {
    bits <- c(bits, analysis_notes[[var_name]])
  }
  paste(bits[nzchar(bits)], collapse = " | ")
}

infer_mars_short_name <- function(var_name, var_cfg) {
  # Prefer explicit config field, then backward-compatible field, else project key.
  var_cfg$mars_short_name %||% var_cfg$era5_short_name %||% var_name
}

# ------------------------------------------------------------------------------
# Build table
# ------------------------------------------------------------------------------

rows <- lapply(var_names, function(vn) {
  v <- vars[[vn]]
  era5_name <- v$era5_name %||% ""
  mars_short_name <- infer_mars_short_name(vn, v)
  monthly_available <- !(vn %in% monthly_unavailable_table8) &&
    !is_unsupported_monthly_mean_variable(era5_name)

  data.frame(
    project_variable = vn,
    description = v$description %||% "",
    units_project = v$units %||% "",
    scale_applied_in_pipeline = as.character(v$scale %||% ""),
    convert_kelvin = as_flag(v$convert_kelvin %||% FALSE),

    cds_variable_name = era5_name,
    mars_short_name = mars_short_name,
    mars_short_name_source = if (!is.null(v$mars_short_name)) "config:mars_short_name"
      else if (!is.null(v$era5_short_name)) "config:era5_short_name (legacy)"
      else "inferred_from_project_variable_key",
    mars_param_id = "",
    mars_param_id_source = "",

    era5_docs_table = table_lookup[[vn]] %||% "",
    temporal_nature = temporal_nature_lookup[[vn]] %||% "",
    variable_type_family = type_lookup[[vn]] %||% "other",

    monthly_available_cds_monthly_means = if (monthly_available) "TRUE" else "FALSE",
    monthly_availability_status = monthly_status_for(vn, era5_name),
    monthly_product_type = "monthly_averaged_reanalysis",
    monthly_exception_table = if (vn %in% monthly_unavailable_table8) "Table 8" else "",

    value_origin = raw_or_computed_for(vn),
    computed_from_variables = computed_from_for(vn),
    computed_method_or_equation = computed_method_for(vn),

    notes = docs_note_for(vn),
    outdated_shortname_note = outdated_shortname_note,

    era5_dataset_scope = "ERA5 surface/single-level parameters (project subset)",
    era5_temporal_coverage_docs = "January 1940 to present (ERA5 docs page; includes back extension context)",
    project_time_range_requested = sprintf("%s-%s", time_cfg$start_year, time_cfg$end_year),
    project_spatial_coverage = era5_cfg$coverage %||% "",
    cds_dataset_id = "reanalysis-era5-single-levels-monthly-means",
    cds_dataset_source_url = "https://cds.climate.copernicus.eu/datasets/reanalysis-era5-single-levels-monthly-means",
    era5_docs_parameter_listings_url = default_sources[[1]],
    ecmwf_parameter_db_url = default_sources[[2]],

    generated_utc = format(Sys.time(), tz = "UTC", usetz = TRUE),
    stringsAsFactors = FALSE
  )
})

meta <- do.call(rbind, rows)

# Helpful ordering
meta <- meta[order(meta$era5_docs_table, meta$project_variable), ]
row.names(meta) <- NULL

# ------------------------------------------------------------------------------
# Write CSV
# ------------------------------------------------------------------------------

out_dir <- here("05_era5/data/metadata")
dir.create(out_dir, recursive = TRUE, showWarnings = FALSE)
out_path <- file.path(out_dir, "era5_variable_metadata.csv")

write.csv(meta, out_path, row.names = FALSE, na = "")

cat("Wrote ERA5 metadata CSV:\n")
cat(out_path, "\n")
cat(sprintf("Rows: %d\n", nrow(meta)))
