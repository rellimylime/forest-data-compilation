# 05_explore_ids_coverage.R
# Explore IDS cleaned data for missingness (pre/post 2015), era-specific columns,
# value distributions, and regional temporal coverage.

suppressPackageStartupMessages({
  library(data.table)
  library(sf)
  library(stringr)
})

cat("=== IDS DATA EXPLORATION ===\n\n")

input_file <- "01_ids/data/processed/ids_damage_areas_cleaned.gpkg"
output_dir <- "01_ids/data/processed/ids_exploration"

if (!file.exists(input_file)) {
  stop(
    "Cleaned IDS file not found at ", input_file, "\n",
    "Run 01_ids/scripts/03_clean_ids.R first or update input_file path."
  )
}

dir.create(output_dir, recursive = TRUE, showWarnings = FALSE)

cat("[1] Reading cleaned IDS data...\n")
ids_sf <- st_read(input_file, quiet = TRUE)
ids_dt <- as.data.table(st_drop_geometry(ids_sf))

if (!"SURVEY_YEAR" %in% names(ids_dt)) {
  stop("SURVEY_YEAR column is required for era splitting.")
}
if (!"REGION_ID" %in% names(ids_dt)) {
  stop("REGION_ID column is required for regional coverage.")
}

ids_dt[, era := fifelse(SURVEY_YEAR < 2015, "pre_2015", "post_2015")]

cat("[2] Column availability by era...\n")
cols <- setdiff(names(ids_dt), "era")

cols_by_era <- ids_dt[, lapply(.SD, function(x) sum(!is.na(x)) > 0),
                      by = era, .SDcols = cols]
cols_long <- melt(cols_by_era, id.vars = "era",
                  variable.name = "column", value.name = "has_values")
cols_wide <- dcast(cols_long, column ~ era, value.var = "has_values")

unique_pre <- cols_wide[pre_2015 & !post_2015, column]
unique_post <- cols_wide[post_2015 & !pre_2015, column]
consistent <- cols_wide[pre_2015 & post_2015, column]

fwrite(cols_wide, file = file.path(output_dir, "ids_columns_by_era.csv"))

cat("  Columns with values only pre-2015: ", length(unique_pre), "\n")
cat("  Columns with values only post-2015: ", length(unique_post), "\n")
cat("  Columns with values in both eras: ", length(consistent), "\n\n")

cat("[3] Missingness by era (fraction NA)...\n")
missing_by_era <- ids_dt[, lapply(.SD, function(x) mean(is.na(x))),
                         by = era, .SDcols = cols]
missing_long <- melt(missing_by_era, id.vars = "era",
                     variable.name = "column", value.name = "missing_frac")
missing_wide <- dcast(missing_long, column ~ era, value.var = "missing_frac")

fwrite(missing_wide, file = file.path(output_dir, "ids_missing_by_era.csv"))

cat("[4] Value summaries for era-specific columns...\n")
value_summary <- function(dt, col) {
  x <- dt[[col]]
  if (is.numeric(x)) {
    data.table(
      column = col,
      type = "numeric",
      n = sum(!is.na(x)),
      min = suppressWarnings(min(x, na.rm = TRUE)),
      p25 = suppressWarnings(quantile(x, 0.25, na.rm = TRUE)),
      median = suppressWarnings(median(x, na.rm = TRUE)),
      p75 = suppressWarnings(quantile(x, 0.75, na.rm = TRUE)),
      max = suppressWarnings(max(x, na.rm = TRUE))
    )
  } else {
    tab <- sort(table(x, useNA = "no"), decreasing = TRUE)
    top_vals <- head(tab, 10)
    data.table(
      column = col,
      type = class(x)[1],
      n = sum(!is.na(x)),
      top_values = paste(names(top_vals), top_vals, sep = ": ", collapse = "; ")
    )
  }
}

summaries_pre <- rbindlist(lapply(unique_pre, function(col) {
  value_summary(ids_dt[era == "pre_2015"], col)
}), fill = TRUE)

summaries_post <- rbindlist(lapply(unique_post, function(col) {
  value_summary(ids_dt[era == "post_2015"], col)
}), fill = TRUE)

fwrite(summaries_pre, file.path(output_dir, "ids_value_summary_pre_2015.csv"))
fwrite(summaries_post, file.path(output_dir, "ids_value_summary_post_2015.csv"))

cat("[5] Regional temporal coverage...\n")
coverage <- ids_dt[, .(
  min_year = min(SURVEY_YEAR, na.rm = TRUE),
  max_year = max(SURVEY_YEAR, na.rm = TRUE),
  n_years = uniqueN(SURVEY_YEAR),
  years = list(sort(unique(SURVEY_YEAR)))
), by = REGION_ID]

coverage[, missing_years := lapply(seq_len(.N), function(i) {
  yr_seq <- seq(coverage$min_year[i], coverage$max_year[i])
  setdiff(yr_seq, coverage$years[[i]])
})]

coverage_out <- coverage[, .(
  REGION_ID,
  min_year,
  max_year,
  n_years,
  years = sapply(years, paste, collapse = ","),
  missing_years = sapply(missing_years, paste, collapse = ",")
)]

fwrite(coverage_out, file.path(output_dir, "ids_region_coverage.csv"))

cat("\nOutputs written to: ", output_dir, "\n")
cat("Done.\n")
