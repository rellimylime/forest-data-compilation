#!/usr/bin/env Rscript

# ==============================================================================
# 05_era5/scripts/00_export_era5_variable_metadata.R
# Export ERA5 parameter metadata CSV from locally saved ECMWF ERA5 documentation
# HTML (Tables 1-13 parameter listings), plus project-specific download flags.
# ==============================================================================

suppressPackageStartupMessages({
  library(here)
  library(yaml)
  library(xml2)
  library(rvest)
})

source(here("scripts/utils/load_config.R"))
source(here("scripts/utils/cds_utils.R"))

`%||%` <- function(x, y) if (is.null(x)) y else x

as_chr <- function(x) {
  y <- as.character(x)
  y[is.na(y)] <- ""
  trimws(y)
}

as_flag <- function(x) if (isTRUE(x)) "TRUE" else "FALSE"

norm_col <- function(x) {
  x <- gsub("[^A-Za-z0-9]+", "_", x)
  x <- gsub("_+", "_", x)
  x <- gsub("^_|_$", "", x)
  tolower(x)
}

first_non_empty <- function(...) {
  vals <- list(...)
  for (v in vals) {
    if (!is.null(v) && length(v) > 0) {
      vv <- as_chr(v)
      if (length(vv) > 0 && nzchar(vv[1])) return(vv[1])
    }
  }
  ""
}

find_era5_html <- function() {
  candidates <- c(
    here("ERA5_ data documentation - Copernicus Knowledge Base - ECMWF Confluence Wiki.html"),
    here("ERA5: data documentation - Copernicus Knowledge Base - ECMWF Confluence Wiki.html")
  )
  for (p in candidates) if (file.exists(p)) return(p)

  matches <- Sys.glob(here("*ERA5*data documentation*Confluence Wiki.html"))
  if (length(matches) == 0) {
    stop("Could not find locally saved ERA5 documentation HTML in project root.")
  }
  matches[[1]]
}

extract_table_title <- function(table_node) {
  h3 <- xml_find_first(table_node, "preceding::h3[1]")
  if (!inherits(h3, "xml_missing")) {
    txt <- xml_text(h3, trim = TRUE)
    if (nzchar(txt)) return(txt)
  }
  ""
}

clean_table_df <- function(df) {
  names(df) <- norm_col(names(df))
  df[] <- lapply(df, as_chr)

  # Drop duplicate header row parsed as data (common in Confluence tables)
  if (all(c("count", "name") %in% names(df))) {
    keep <- !(tolower(df$count) == "count" & tolower(df$name) == "name")
    df <- df[keep, , drop = FALSE]
  }

  # Drop all-empty rows
  nonempty <- apply(df, 1, function(r) any(nzchar(trimws(r))))
  df[nonempty, , drop = FALSE]
}

parse_table_number <- function(table_title) {
  m <- regexec("^Table\\s+([0-9]+):", table_title)
  x <- regmatches(table_title, m)[[1]]
  if (length(x) >= 2) as.integer(x[2]) else NA_integer_
}

canonicalize_param_table <- function(df, table_title, html_table_index) {
  df <- clean_table_df(df)

  col_or_blank <- function(col_name, i = NULL) {
    if (!(col_name %in% names(df))) return("")
    if (is.null(i)) return(df[[col_name]])
    df[[col_name]][i]
  }

  out <- data.frame(
    era5_docs_table_number = parse_table_number(table_title),
    era5_docs_table_title = table_title,
    html_table_index = html_table_index,
    docs_count = first_non_empty(col_or_blank("count")),
    docs_name = first_non_empty(col_or_blank("name")),
    units_docs = first_non_empty(col_or_blank("units")),
    cds_variable_name_docs = first_non_empty(col_or_blank("variable_name_in_cds"), col_or_blank("variable_name_in_cds_")),
    mars_short_name_docs = first_non_empty(col_or_blank("shortname")),
    mars_param_id = first_non_empty(col_or_blank("paramid")),
    native_grid = first_non_empty(col_or_blank("native_grid")),
    an_docs = first_non_empty(col_or_blank("an")),
    fc_docs = first_non_empty(col_or_blank("fc")),
    stringsAsFactors = FALSE
  )[FALSE, ]

  if (nrow(df) == 0) return(out)

  rows <- vector("list", nrow(df))
  for (i in seq_len(nrow(df))) {
    rows[[i]] <- data.frame(
      era5_docs_table_number = parse_table_number(table_title),
      era5_docs_table_title = table_title,
      html_table_index = html_table_index,
      docs_count = first_non_empty(col_or_blank("count", i)),
      docs_name = first_non_empty(col_or_blank("name", i)),
      units_docs = first_non_empty(col_or_blank("units", i)),
      cds_variable_name_docs = first_non_empty(col_or_blank("variable_name_in_cds", i), col_or_blank("variable_name_in_cds_", i)),
      mars_short_name_docs = first_non_empty(col_or_blank("shortname", i)),
      mars_param_id = first_non_empty(col_or_blank("paramid", i)),
      native_grid = first_non_empty(col_or_blank("native_grid", i)),
      an_docs = first_non_empty(col_or_blank("an", i)),
      fc_docs = first_non_empty(col_or_blank("fc", i)),
      stringsAsFactors = FALSE
    )
  }
  do.call(rbind, rows)
}

clean_docs_name <- function(x) {
  # Remove trailing footnote markers appended to names in some Confluence tables
  gsub("([A-Za-z\\)])([0-9]+)$", "\\1", x)
}

availability_from_cell <- function(x) {
  if (is.null(x) || length(x) == 0 || is.na(x)) return("")
  v <- tolower(trimws(as.character(x)))
  if (!nzchar(v) || identical(v, "na")) return("")
  if (v == "x") return("TRUE")
  if (grepl("no mean", v, fixed = TRUE)) return("FALSE")
  v
}

monthly_status_label <- function(value) {
  if (identical(value, "TRUE")) return("available in monthly means")
  if (identical(value, "FALSE")) return("not available in monthly means (no mean)")
  if (!nzchar(value)) return("unknown / not assessed from Table 8")
  paste("docs value:", value)
}

infer_variable_family <- function(name_txt, cds_name, short_name) {
  x <- tolower(paste(name_txt, cds_name, short_name))

  rules <- c(
    "temperature" = "temperature",
    "dewpoint" = "temperature",
    "wind|gust" = "wind",
    "precip|snowfall|snowmelt" = "precipitation/snow",
    "snow " = "snow/cryosphere",
    "snow_" = "snow/cryosphere",
    "sea ice|ice " = "cryosphere",
    "pressure" = "pressure",
    "radiation|radiative|albedo|solar|thermal|longwave|shortwave|uv" = "radiation",
    "evap|latent heat|sensible heat|flux|stress" = "surface flux/evaporation",
    "soil|runoff|land|lake|vegetation|leaf area|root" = "land surface",
    "cloud" = "clouds",
    "aerosol|ozone|methane|carbon|water vapour|water vapor|column" = "atmosphere composition / column",
    "convective|cape|cin|boundary layer|vorticity|divergence" = "dynamics/convection",
    "wave|swell" = "waves/ocean"
  )

  for (pat in names(rules)) {
    if (grepl(pat, x)) return(unname(rules[[pat]]))
  }
  "other"
}

compute_value_origin <- function(cds_name, short_name) {
  if (identical(cds_name, "snow_cover") || identical(short_name, "snowc")) {
    return("computed_in_ERA5")
  }
  if (identical(short_name, "10si")) {
    return("computed_in_ERA5")
  }
  "raw_ERA5_parameter"
}

computed_from_vars <- function(cds_name, short_name) {
  if (identical(cds_name, "snow_cover") || identical(short_name, "snowc")) return("sd, rsn")
  if (identical(short_name, "10si")) return("u10, v10")
  ""
}

computed_equation <- function(cds_name, short_name) {
  if (identical(cds_name, "snow_cover") || identical(short_name, "snowc")) {
    return("snow_cover = min(1, (1000 * sd / rsn) / 0.1)")
  }
  if (identical(short_name, "10si")) {
    return("10m_wind_speed = sqrt(u10^2 + v10^2) (see ERA5 docs table footnote)")
  }
  ""
}

computed_notes <- function(cds_name, short_name) {
  if (identical(cds_name, "snow_cover") || identical(short_name, "snowc")) {
    return("ERA5 diagnostic snow cover fraction derived from snow water equivalent and snow density (IFS formulation).")
  }
  if (identical(short_name, "10si")) {
    return("10m wind speed is a derived wind speed magnitude listed in Table 8 exceptions.")
  }
  ""
}

# ------------------------------------------------------------------------------
# Load inputs
# ------------------------------------------------------------------------------

config <- load_config()
era5_cfg <- config$raw$era5
time_cfg <- config$params$time_range
html_path <- find_era5_html()

doc <- read_html(html_path)
table_nodes <- html_elements(doc, "table")

# Confluence saved page includes 23 tables total; parameter tables are 1-13,
# which correspond to these HTML table positions (1-based) in the saved file.
param_table_node_indices <- c(6, 7, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19)

table_rows <- lapply(param_table_node_indices, function(idx) {
  tbl_title <- extract_table_title(table_nodes[[idx]])
  df <- html_table(table_nodes[[idx]], fill = TRUE)
  canonicalize_param_table(df, tbl_title, idx)
})

all_params <- do.call(rbind, table_rows)
all_params$docs_name_clean <- clean_docs_name(all_params$docs_name)
all_params$mars_param_id <- as_chr(all_params$mars_param_id)
all_params$cds_variable_name_docs <- as_chr(all_params$cds_variable_name_docs)
all_params$mars_short_name_docs <- as_chr(all_params$mars_short_name_docs)
all_params$an_docs <- as_chr(all_params$an_docs)
all_params$fc_docs <- as_chr(all_params$fc_docs)

# Split Table 8 (monthly exceptions) from base parameter listings
tab8 <- all_params[all_params$era5_docs_table_number == 8, , drop = FALSE]
base <- all_params[all_params$era5_docs_table_number != 8, , drop = FALSE]

join_key <- function(df) {
  ifelse(nzchar(df$mars_param_id), paste0("param:", df$mars_param_id),
    ifelse(nzchar(df$cds_variable_name_docs), paste0("cds:", tolower(df$cds_variable_name_docs)),
      ifelse(nzchar(df$mars_short_name_docs), paste0("sn:", tolower(df$mars_short_name_docs)), "")
    )
  )
}

base$join_key <- join_key(base)
tab8$join_key <- join_key(tab8)

tab8_map <- tab8[, c(
  "join_key", "docs_name", "cds_variable_name_docs", "mars_short_name_docs",
  "mars_param_id", "an_docs", "fc_docs", "era5_docs_table_title"
), drop = FALSE]
names(tab8_map) <- c(
  "join_key", "tab8_docs_name", "tab8_cds_name", "tab8_short_name",
  "tab8_param_id", "tab8_an", "tab8_fc", "tab8_table_title"
)

merged <- merge(base, tab8_map, by = "join_key", all.x = TRUE, sort = FALSE)

# Add Table 8 entries not represented in Tables 1-7 (still useful to include)
tab8_only <- tab8[!(tab8$join_key %in% base$join_key), , drop = FALSE]
if (nrow(tab8_only) > 0) {
  tab8_only$tab8_docs_name <- tab8_only$docs_name
  tab8_only$tab8_cds_name <- tab8_only$cds_variable_name_docs
  tab8_only$tab8_short_name <- tab8_only$mars_short_name_docs
  tab8_only$tab8_param_id <- tab8_only$mars_param_id
  tab8_only$tab8_an <- tab8_only$an_docs
  tab8_only$tab8_fc <- tab8_only$fc_docs
  tab8_only$tab8_table_title <- tab8_only$era5_docs_table_title
  merged <- rbind(merged, tab8_only[, names(merged), drop = FALSE])
}

# ------------------------------------------------------------------------------
# Monthly availability from docs (Table 8 applies to Tables 1-7 + Table 8 rows)
# ------------------------------------------------------------------------------

merged$monthly_scope_table8_applies <- ifelse(
  merged$era5_docs_table_number %in% 1:8, "TRUE", "FALSE"
)

merged$monthly_an_available_from_docs <- ""
merged$monthly_fc_available_from_docs <- ""
merged$monthly_available_from_docs <- ""
merged$monthly_availability_basis <- ""

for (i in seq_len(nrow(merged))) {
  tn <- merged$era5_docs_table_number[i]
  base_an <- availability_from_cell(merged$an_docs[i])
  base_fc <- availability_from_cell(merged$fc_docs[i])
  tab8_an <- availability_from_cell(merged$tab8_an[i])
  tab8_fc <- availability_from_cell(merged$tab8_fc[i])

  if (tn %in% 1:7) {
    an_val <- if (nzchar(tab8_an)) tab8_an else base_an
    fc_val <- if (nzchar(tab8_fc)) tab8_fc else base_fc
    merged$monthly_an_available_from_docs[i] <- an_val
    merged$monthly_fc_available_from_docs[i] <- fc_val

    any_true <- any(c(an_val, fc_val) == "TRUE")
    any_false_only <- all(c(an_val, fc_val)[nzchar(c(an_val, fc_val))] == "FALSE")
    merged$monthly_available_from_docs[i] <- if (any_true) "TRUE" else if (any_false_only) "FALSE" else ""
    merged$monthly_availability_basis[i] <- "Tables 1-7 with Table 8 exceptions overlay"
  } else if (tn == 8) {
    an_val <- if (nzchar(tab8_an)) tab8_an else base_an
    fc_val <- if (nzchar(tab8_fc)) tab8_fc else base_fc
    merged$monthly_an_available_from_docs[i] <- an_val
    merged$monthly_fc_available_from_docs[i] <- fc_val
    any_true <- any(c(an_val, fc_val) == "TRUE")
    any_false_only <- all(c(an_val, fc_val)[nzchar(c(an_val, fc_val))] == "FALSE")
    merged$monthly_available_from_docs[i] <- if (any_true) "TRUE" else if (any_false_only) "FALSE" else ""
    merged$monthly_availability_basis[i] <- "Table 8 exception entry"
  } else {
    merged$monthly_availability_basis[i] <- "Not determined here (Table 8 only documents exceptions for Tables 1-7)"
  }
}

merged$monthly_availability_status <- vapply(
  merged$monthly_available_from_docs, monthly_status_label, character(1)
)

# ------------------------------------------------------------------------------
# Project config join + download flags
# ------------------------------------------------------------------------------

cfg_vars <- era5_cfg$variables
cfg_names <- names(cfg_vars)

cfg_rows <- lapply(cfg_names, function(vn) {
  v <- cfg_vars[[vn]]
  data.frame(
    project_variable = vn,
    description_project = v$description %||% "",
    units_project = v$units %||% "",
    scale_applied_in_pipeline = as_chr(v$scale %||% ""),
    convert_kelvin = as_flag(v$convert_kelvin %||% FALSE),
    cds_variable_name_project = as_chr(v$era5_name %||% ""),
    mars_short_name_project = as_chr(v$mars_short_name %||% (v$era5_short_name %||% "")),
    configured_monthly_supported_pattern = as_flag(!is_unsupported_monthly_mean_variable(as_chr(v$era5_name %||% ""))),
    stringsAsFactors = FALSE
  )
})
cfg_df <- do.call(rbind, cfg_rows)

cfg_by_cds <- cfg_df[nzchar(cfg_df$cds_variable_name_project), , drop = FALSE]
cfg_by_cds$cds_variable_name_docs <- cfg_by_cds$cds_variable_name_project
merged2 <- merge(merged, cfg_by_cds, by = "cds_variable_name_docs", all.x = TRUE, sort = FALSE)

# Fallback match by shortName for rows lacking CDS variable name
unmatched <- is.na(merged2$project_variable) | !nzchar(as_chr(merged2$project_variable))
cfg_by_short <- cfg_df[nzchar(cfg_df$mars_short_name_project), c("project_variable", "mars_short_name_project"), drop = FALSE]
short_map <- setNames(cfg_by_short$project_variable, cfg_by_short$mars_short_name_project)
fill_idx <- which(unmatched & nzchar(merged2$mars_short_name_docs))
for (i in fill_idx) {
  sn <- merged2$mars_short_name_docs[i]
  if (sn %in% names(short_map) && merged2$era5_docs_table_number[i] %in% 1:8) {
    pv <- short_map[[sn]]
    row <- cfg_df[cfg_df$project_variable == pv, ][1, ]
    for (nm in names(row)) merged2[i, nm] <- row[[nm]]
  }
}

merged2$configured_in_project <- ifelse(
  !is.na(merged2$project_variable) & nzchar(as_chr(merged2$project_variable)), "TRUE", "FALSE"
)

# Add supplemental rows for configured variables not present in parsed parameter tables
matched_project_vars <- unique(as_chr(merged2$project_variable[merged2$configured_in_project == "TRUE"]))
missing_project_vars <- setdiff(cfg_df$project_variable, matched_project_vars)
if (length(missing_project_vars) > 0) {
  for (pv in missing_project_vars) {
    cfr <- cfg_df[cfg_df$project_variable == pv, , drop = FALSE][1, ]

    blank <- merged2[1, , drop = FALSE]
    blank[,] <- ""
    blank$join_key <- paste0("project_only:", pv)

    # Populate docs-like columns from project config where possible.
    blank$cds_variable_name_docs <- cfr$cds_variable_name_project
    blank$mars_short_name_docs <- if (nzchar(cfr$mars_short_name_project)) cfr$mars_short_name_project else pv
    blank$docs_name <- cfr$description_project
    blank$docs_name_clean <- cfr$description_project
    blank$units_docs <- cfr$units_project
    blank$era5_docs_table_number <- ""
    blank$era5_docs_table_title <- "Not found in parsed ERA5 parameter tables (local HTML); added from project config"
    blank$html_table_index <- ""
    blank$docs_count <- ""
    blank$native_grid <- ""
    blank$an_docs <- ""
    blank$fc_docs <- ""
    blank$tab8_docs_name <- ""
    blank$tab8_cds_name <- ""
    blank$tab8_short_name <- ""
    blank$tab8_param_id <- ""
    blank$tab8_an <- ""
    blank$tab8_fc <- ""
    blank$tab8_table_title <- ""
    blank$monthly_scope_table8_applies <- "FALSE"
    blank$monthly_an_available_from_docs <- ""
    blank$monthly_fc_available_from_docs <- ""
    blank$monthly_available_from_docs <- ""
    blank$monthly_availability_basis <- "Not found in parsed parameter tables; no Table 8 assessment available"
    blank$monthly_availability_status <- "unknown / not assessed from Table 8"

    for (nm in names(cfr)) blank[[nm]] <- cfr[[nm]]
    blank$configured_in_project <- "TRUE"

    merged2 <- rbind(merged2, blank)
  }
}

merged2$download_in_project <- "FALSE"
merged2$download_in_project_reason <- "not in project ERA5 config"
for (i in seq_len(nrow(merged2))) {
  if (merged2$configured_in_project[i] != "TRUE") next

  monthly_ok <- identical(merged2$monthly_available_from_docs[i], "TRUE")
  monthly_unknown <- !nzchar(merged2$monthly_available_from_docs[i])
  pattern_ok <- identical(merged2$configured_monthly_supported_pattern[i], "TRUE")

  # "download_in_project" reflects what the current monthly downloader attempts to request.
  # Docs monthly flags are still recorded separately and can veto explicit no-mean variables.
  if (pattern_ok && (monthly_ok || monthly_unknown)) {
    merged2$download_in_project[i] <- "TRUE"
    if (monthly_ok) {
      merged2$download_in_project_reason[i] <- "configured + monthly available in docs + passes monthly filter"
    } else {
      merged2$download_in_project_reason[i] <- "configured + passes monthly filter (docs monthly availability unknown in parsed tables)"
    }
  } else if (!monthly_ok && !monthly_unknown) {
    merged2$download_in_project_reason[i] <- "configured but docs indicate no monthly mean"
  } else if (!pattern_ok) {
    merged2$download_in_project_reason[i] <- "configured but filtered by monthly unsupported pattern"
  } else if (monthly_unknown) {
    merged2$download_in_project_reason[i] <- "configured but monthly availability not determined in this docs export"
  }
}

# ------------------------------------------------------------------------------
# Add user-requested descriptive columns
# ------------------------------------------------------------------------------

merged2$docs_name_clean <- clean_docs_name(merged2$docs_name)
merged2$variable_type_family <- mapply(
  infer_variable_family, merged2$docs_name_clean, merged2$cds_variable_name_docs, merged2$mars_short_name_docs,
  USE.NAMES = FALSE
)

merged2$value_origin <- mapply(
  compute_value_origin, merged2$cds_variable_name_docs, merged2$mars_short_name_docs,
  USE.NAMES = FALSE
)
merged2$computed_from_variables <- mapply(
  computed_from_vars, merged2$cds_variable_name_docs, merged2$mars_short_name_docs,
  USE.NAMES = FALSE
)
merged2$computed_method_or_equation <- mapply(
  computed_equation, merged2$cds_variable_name_docs, merged2$mars_short_name_docs,
  USE.NAMES = FALSE
)
merged2$computed_notes <- mapply(
  computed_notes, merged2$cds_variable_name_docs, merged2$mars_short_name_docs,
  USE.NAMES = FALSE
)

merged2$outdated_shortname_note <- paste(
  "ERA5 docs note some shortNames in the parameter tables may be outdated;",
  "use the ECMWF parameter database as authoritative."
)

merged2$era5_temporal_coverage_docs <- "ERA5 coverage documented on ERA5 data documentation page (global ERA5 archive; see page for product-specific periods)"
merged2$project_time_range_requested <- sprintf("%s-%s", time_cfg$start_year, time_cfg$end_year)
merged2$project_era5_monthly_dataset_id <- "reanalysis-era5-single-levels-monthly-means"
merged2$project_era5_monthly_dataset_url <- "https://cds.climate.copernicus.eu/datasets/reanalysis-era5-single-levels-monthly-means"
merged2$era5_docs_parameter_listings_url <- "https://confluence.ecmwf.int/display/CKB/ERA5%3A%2Bdata%2Bdocumentation#heading-Parameterlistings"
merged2$cds_mars_internal_guide_url <- "https://confluence.ecmwf.int/display/CKB/Climate+Data+Store+%28CDS%29+documentation#ClimateDataStore(CDS)documentation-DatahostedontheCDS/'MARSinternal'"
merged2$ecmwf_parameter_db_url <- "https://apps.ecmwf.int/codes/grib/param-db/"
merged2$source_html_file <- basename(html_path)
merged2$generated_utc <- format(Sys.time(), tz = "UTC", usetz = TRUE)

# Specific notes for known ambiguity cases in project workflow
merged2$project_download_notes <- ""
is_e <- merged2$cds_variable_name_docs == "total_evaporation"
is_snowc <- merged2$cds_variable_name_docs == "snow_cover" | merged2$mars_short_name_docs == "snowc"
merged2$project_download_notes[is_e] <- "MARS ambiguity observed for CDS name; project retries with mars_short_name = 'e'."
merged2$project_download_notes[is_snowc] <- "MARS ambiguity observed for both CDS name and shortName in some requests; may require paramId fallback."

# ------------------------------------------------------------------------------
# Output formatting / ordering
# ------------------------------------------------------------------------------

# Stable per-row identifier (one row per parameter entry context)
merged2$row_id <- paste0(
  "T", merged2$era5_docs_table_number, "_",
  ifelse(nzchar(merged2$mars_param_id), merged2$mars_param_id,
    ifelse(nzchar(merged2$mars_short_name_docs), merged2$mars_short_name_docs, gsub("\\s+", "_", tolower(merged2$docs_name_clean)))
  )
)

merged2 <- merged2[order(
  merged2$era5_docs_table_number,
  suppressWarnings(as.numeric(ifelse(nzchar(merged2$docs_count), merged2$docs_count, NA))),
  merged2$docs_name_clean
), , drop = FALSE]
row.names(merged2) <- NULL

out <- merged2[, c(
  "row_id",
  "era5_docs_table_number",
  "era5_docs_table_title",
  "docs_count",
  "docs_name",
  "docs_name_clean",
  "units_docs",
  "cds_variable_name_docs",
  "mars_short_name_docs",
  "mars_param_id",
  "native_grid",
  "an_docs",
  "fc_docs",
  "monthly_scope_table8_applies",
  "monthly_an_available_from_docs",
  "monthly_fc_available_from_docs",
  "monthly_available_from_docs",
  "monthly_availability_status",
  "monthly_availability_basis",
  "variable_type_family",
  "value_origin",
  "computed_from_variables",
  "computed_method_or_equation",
  "computed_notes",
  "configured_in_project",
  "download_in_project",
  "download_in_project_reason",
  "project_variable",
  "description_project",
  "units_project",
  "scale_applied_in_pipeline",
  "convert_kelvin",
  "cds_variable_name_project",
  "mars_short_name_project",
  "configured_monthly_supported_pattern",
  "project_download_notes",
  "outdated_shortname_note",
  "era5_temporal_coverage_docs",
  "project_time_range_requested",
  "project_era5_monthly_dataset_id",
  "project_era5_monthly_dataset_url",
  "era5_docs_parameter_listings_url",
  "cds_mars_internal_guide_url",
  "ecmwf_parameter_db_url",
  "source_html_file",
  "generated_utc"
), drop = FALSE]
out[] <- lapply(out, as_chr)

# ------------------------------------------------------------------------------
# Review CSV (cleaner columns, plain-language statuses)
# ------------------------------------------------------------------------------

to_tf_unknown <- function(x) {
  if (identical(x, "TRUE")) return("TRUE")
  if (identical(x, "FALSE")) return("FALSE")
  "UNKNOWN"
}

subdaily_tf_from_docs <- function(cell_value, docs_row_found) {
  if (!identical(docs_row_found, "TRUE")) return("UNKNOWN")
  v <- availability_from_cell(cell_value)
  if (identical(v, "TRUE")) return("TRUE")
  if (identical(v, "FALSE")) return("FALSE")
  # In the parameter tables, blank an/fc cells mean the parameter is not available
  # for that type (analysis or forecast).
  "FALSE"
}

infer_can_make_monthly <- function(an_avail, fc_avail) {
  if (an_avail == "TRUE" || fc_avail == "TRUE") return("TRUE")
  if (an_avail == "FALSE" && fc_avail == "FALSE") return("FALSE")
  if ((an_avail == "FALSE" && fc_avail == "UNKNOWN") || (an_avail == "UNKNOWN" && fc_avail == "FALSE")) return("FALSE")
  "UNKNOWN"
}

monthly_summary_for_review <- function(direct_monthly, can_make_monthly, basis, table_num) {
  if (direct_monthly == "TRUE") {
    return("Direct ERA5 monthly mean available")
  }
  if (direct_monthly == "FALSE" && can_make_monthly == "TRUE") {
    return("No direct ERA5 monthly mean (docs 'no mean'), but can be aggregated from sub-daily ERA5")
  }
  if (direct_monthly == "FALSE" && can_make_monthly == "FALSE") {
    return("No direct monthly mean and no sub-daily source indicated in parsed docs")
  }
  if (table_num %in% c("9","10","11","12","13") && can_make_monthly == "TRUE") {
    return("Monthly status not covered by Table 8 (upper-air table), but can be aggregated from sub-daily ERA5")
  }
  if (grepl("Not found in parsed ERA5 parameter tables", basis, fixed = TRUE)) {
    return("Not found in parsed parameter tables; project-config row added manually")
  }
  if (can_make_monthly == "TRUE") {
    return("Direct monthly status unclear in parsed docs, but sub-daily availability suggests monthly aggregation is possible")
  }
  "Monthly availability unclear from parsed docs"
}

row_found_in_docs_param_tables <- ifelse(nzchar(out$era5_docs_table_number), "TRUE", "FALSE")
subdaily_an <- mapply(subdaily_tf_from_docs, out$an_docs, row_found_in_docs_param_tables, USE.NAMES = FALSE)
subdaily_fc <- mapply(subdaily_tf_from_docs, out$fc_docs, row_found_in_docs_param_tables, USE.NAMES = FALSE)
monthly_direct <- vapply(out$monthly_available_from_docs, to_tf_unknown, character(1))
can_make_monthly <- mapply(infer_can_make_monthly, subdaily_an, subdaily_fc, USE.NAMES = FALSE)

preferred_name <- ifelse(
  nzchar(out$description_project),
  out$description_project,
  ifelse(nzchar(out$docs_name_clean), out$docs_name_clean, out$docs_name)
)

cds_name_for_review <- ifelse(
  nzchar(out$cds_variable_name_project),
  out$cds_variable_name_project,
  out$cds_variable_name_docs
)

mars_short_for_review <- ifelse(
  nzchar(out$mars_short_name_project),
  out$mars_short_name_project,
  out$mars_short_name_docs
)

unit_for_review <- ifelse(
  nzchar(out$units_project),
  out$units_project,
  out$units_docs
)

an_fc_notes <- "Docs columns: an = analysis field available in ERA5 archive; fc = forecast field available in ERA5 archive"

monthly_method_notes <- ifelse(
  can_make_monthly == "TRUE",
  "If no direct monthly mean is available, monthly values can usually be created by aggregating sub-daily ERA5 (respecting variable type: instantaneous vs accumulation/flux/rate).",
  ""
)

review_notes <- paste(
  ifelse(nzchar(out$computed_notes), out$computed_notes, ""),
  ifelse(nzchar(out$project_download_notes), out$project_download_notes, ""),
  ifelse(nzchar(out$download_in_project_reason), paste0("Current project downloader: ", out$download_in_project_reason), ""),
  sep = " | "
)
review_notes <- gsub("^\\s*\\|\\s*|\\s*\\|\\s*$", "", review_notes)
review_notes <- gsub("\\s*\\|\\s*\\|\\s*", " | ", review_notes)

review <- data.frame(
  keep_for_review = "",
  currently_configured_in_project = out$configured_in_project,
  currently_downloaded_by_project_monthly_script = out$download_in_project,
  project_variable_key = out$project_variable,

  era5_name_display = preferred_name,
  cds_variable_name = cds_name_for_review,
  mars_short_name = mars_short_for_review,
  mars_param_id = out$mars_param_id,
  units = unit_for_review,
  variable_type_family = out$variable_type_family,
  value_origin = out$value_origin,

  era5_docs_table_number = out$era5_docs_table_number,
  era5_docs_table_title = out$era5_docs_table_title,
  docs_row_found_in_parameter_tables_1_13 = row_found_in_docs_param_tables,

  subdaily_analysis_available_an = subdaily_an,
  subdaily_forecast_available_fc = subdaily_fc,
  an_fc_column_meaning = an_fc_notes,

  direct_era5_monthly_mean_available_from_docs = monthly_direct,
  can_make_monthly_from_subdaily = can_make_monthly,
  monthly_availability_summary = mapply(
    monthly_summary_for_review,
    monthly_direct,
    can_make_monthly,
    out$era5_docs_table_title,
    out$era5_docs_table_number,
    USE.NAMES = FALSE
  ),
  monthly_docs_basis = out$monthly_availability_basis,
  monthly_method_notes = monthly_method_notes,

  computed_from_variables = out$computed_from_variables,
  computed_method_or_equation = out$computed_method_or_equation,

  project_description = out$description_project,
  project_units = out$units_project,
  project_scale_applied = out$scale_applied_in_pipeline,
  project_convert_kelvin = out$convert_kelvin,

  why_not_downloaded_now = ifelse(out$download_in_project == "TRUE", "", out$download_in_project_reason),
  review_notes = review_notes,

  project_time_range_requested = out$project_time_range_requested,
  project_era5_monthly_dataset_id = out$project_era5_monthly_dataset_id,
  project_era5_monthly_dataset_url = out$project_era5_monthly_dataset_url,
  era5_docs_parameter_listings_url = out$era5_docs_parameter_listings_url,
  cds_mars_internal_guide_url = out$cds_mars_internal_guide_url,
  ecmwf_parameter_db_url = out$ecmwf_parameter_db_url,

  source_html_file = out$source_html_file,
  generated_utc = out$generated_utc,
  stringsAsFactors = FALSE
)

# Sort for decision-making: configured first, then currently downloaded, then docs table/order
table_num_sort <- suppressWarnings(as.integer(ifelse(nzchar(out$era5_docs_table_number), out$era5_docs_table_number, NA)))
table_num_sort[is.na(table_num_sort)] <- 999
cfg_sort <- ifelse(review$currently_configured_in_project == "TRUE", 0L, 1L)
dl_sort <- ifelse(review$currently_downloaded_by_project_monthly_script == "TRUE", 0L, 1L)
name_sort <- tolower(ifelse(nzchar(review$era5_name_display), review$era5_name_display, review$cds_variable_name))
review <- review[order(cfg_sort, dl_sort, table_num_sort, name_sort), , drop = FALSE]
row.names(review) <- NULL

# ------------------------------------------------------------------------------
# Forest-focused trimming for review CSV (remove strict ocean/wave-only variables)
# ------------------------------------------------------------------------------

is_strict_ocean_wave <- function(df) {
  table7_wave <- df$era5_docs_table_number == "7"
  wave_table8_exceptions <- (
    df$era5_docs_table_number == "8" &
      (
        grepl("(^|[^a-z])altimeter", tolower(df$era5_name_display)) |
        grepl("2d wave spectra", tolower(df$era5_name_display), fixed = TRUE) |
        grepl("ocean_surface_stress_equivalent_10m_neutral_wind_speed", df$cds_variable_name, fixed = TRUE)
      )
  )
  table7_wave | wave_table8_exceptions
}

ocean_wave_mask <- is_strict_ocean_wave(review)
review_removed_count <- sum(ocean_wave_mask)
review <- review[!ocean_wave_mask, , drop = FALSE]
row.names(review) <- NULL

out_dir <- here("05_era5/data/metadata")
dir.create(out_dir, recursive = TRUE, showWarnings = FALSE)
out_path <- file.path(out_dir, "era5_variable_metadata.csv")
review_path <- file.path(out_dir, "era5_variable_metadata_review.csv")

write.csv(out, out_path, row.names = FALSE, na = "")
write.csv(review, review_path, row.names = FALSE, na = "")

cat("Wrote ERA5 metadata CSV from local ERA5 docs HTML:\n")
cat(out_path, "\n")
cat(sprintf("Rows: %d\n", nrow(out)))
cat(sprintf("Configured in project: %d\n", sum(out$configured_in_project == "TRUE")))
cat(sprintf("download_in_project == TRUE: %d\n", sum(out$download_in_project == "TRUE")))
cat("\nWrote forest-focused review CSV:\n")
cat(review_path, "\n")
cat(sprintf("Forest-focused review rows removed (strict ocean/wave-only): %d\n", review_removed_count))
