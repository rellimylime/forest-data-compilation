# ==============================================================================
# scripts/utils/metadata_utils.R
# Functions for extracting metadata and generating data dictionaries
# ==============================================================================

library(sf)
library(dplyr)
library(readr)
library(here)
library(glue)

# ==============================================================================
# GEODATABASE FUNCTIONS
# ==============================================================================

#' List layers in a geodatabase
#' @param gdb_path Path to .gdb directory
#' @return sf layers object
list_gdb_layers <- function(gdb_path) {
  st_layers(gdb_path)
}

#' Get layer name by prefix
#' 
#' @param gdb_path Path to .gdb directory
#' @param prefix Layer name prefix (e.g., "DAMAGE_AREAS_FLAT")
#' @return Full layer name
get_layer_name <- function(gdb_path, prefix) {
  info <- st_layers(gdb_path)
  layer <- info$name[grepl(paste0("^", prefix), info$name)][1]
  if (is.na(layer)) stop(glue("No layer matching '{prefix}' in {basename(gdb_path)}"))
  layer
}

#' Generate summary report for a geodatabase
#' 
#' @param gdb_path Path to .gdb directory
#' @return List with summary info for all layers
summarize_gdb <- function(gdb_path) {
  
  info <- st_layers(gdb_path)
  
  summary <- list(
    path = gdb_path,
    n_layers = nrow(info),
    layers = lapply(seq_len(nrow(info)), function(i) {
      list(
        name = info$name[i],
        geometry_type = info$geomtype[[i]],
        n_features = info$features[i],
        n_fields = info$fields[i]
      )
    })
  )
  
  names(summary$layers) <- info$name
  
  # Get CRS from first layer
  first_layer <- st_read(gdb_path, layer = info$name[1], 
                         query = glue("SELECT * FROM \"{info$name[1]}\" LIMIT 1"),
                         quiet = TRUE)
  summary$crs <- st_crs(first_layer)$input
  
  class(summary) <- c("gdb_summary", class(summary))
  summary
}

#' Print gdb summary
print.gdb_summary <- function(x, ...) {
  cat(glue("\n=== Geodatabase Summary ===\n"))
  cat(glue("Path: {x$path}\n"))
  cat(glue("CRS: {x$crs}\n"))
  cat(glue("Layers: {x$n_layers}\n\n"))
  
  for (layer in x$layers) {
    cat(glue("  {layer$name}:\n"))
    cat(glue("    Type: {layer$geometry_type}\n"))
    cat(glue("    Features: {format(layer$n_features, big.mark = ',')}\n"))
    cat(glue("    Fields: {layer$n_fields}\n\n"))
  }
  
  invisible(x)
}

# ==============================================================================
# METADATA EXTRACTION
# ==============================================================================

#' Extract metadata from a geodatabase layer
#' 
#' @param gdb_path Path to .gdb directory
#' @param layer_name Name of layer to extract
#' @param sample_size Number of records to sample for stats (default 1000)
#' @return data.frame with variable metadata
extract_gdb_metadata <- function(gdb_path, layer_name, sample_size = 1000) {
  
  message(glue("Extracting metadata from {layer_name}..."))
  
  # Get total feature count first
  layer_info <- st_layers(gdb_path)
  idx <- which(layer_info$name == layer_name)
  n_total <- layer_info$features[idx]
  
  # Read sample to get structure
  query <- glue("SELECT * FROM \"{layer_name}\" LIMIT {sample_size}")
  layer <- st_read(gdb_path, layer = layer_name, query = query, quiet = TRUE)
  
  # Store geometry info before dropping
  geom_type <- as.character(st_geometry_type(layer, by_geometry = FALSE))
  crs_info <- st_crs(layer)$input
  
  # Drop geometry for attribute analysis
  df <- st_drop_geometry(layer)
  
  # Build metadata for each column
  metadata <- lapply(names(df), function(col) {
    x <- df[[col]]
    
    list(
      field_name = col,
      r_class = paste(class(x), collapse = ", "),
      n_unique = n_distinct(x, na.rm = TRUE),
      n_missing = sum(is.na(x)),
      pct_missing = round(mean(is.na(x)) * 100, 2),
      sample_values = get_sample_values(x),
      range_or_levels = get_range_or_levels(x)
    )
  }) |> bind_rows()
  
  # Add layer info as attributes
  attr(metadata, "source_path") <- gdb_path
  attr(metadata, "layer_name") <- layer_name
  attr(metadata, "geometry_type") <- geom_type
  attr(metadata, "crs") <- crs_info
  attr(metadata, "n_features_total") <- n_total
  attr(metadata, "n_features_sampled") <- nrow(df)
  attr(metadata, "extraction_date") <- Sys.Date()
  
  metadata
}

#' Get range (numeric) or unique levels (character/factor)
#' @param x Vector
#' @param max_levels Maximum levels to show for categorical
#' @return Character string describing range or levels
get_range_or_levels <- function(x, max_levels = 10) {
  if (is.numeric(x)) {
    rng <- range(x, na.rm = TRUE)
    if (all(is.finite(rng))) {
      paste0(rng[1], " to ", rng[2])
    } else {
      NA_character_
    }
  } else if (is.character(x) || is.factor(x)) {
    lvls <- sort(unique(na.omit(x)))
    if (length(lvls) == 0) {
      NA_character_
    } else if (length(lvls) <= max_levels) {
      paste(lvls, collapse = ", ")
    } else {
      paste0(length(lvls), " unique values")
    }
  } else if (inherits(x, "POSIXt") || inherits(x, "Date")) {
    rng <- range(x, na.rm = TRUE)
    paste(as.character(rng), collapse = " to ")
  } else {
    NA_character_
  }
}

#' Get sample values for a column
#' @param x Vector
#' @param n Number of samples
#' @return Character string with sample values
get_sample_values <- function(x, n = 3) {
  vals <- na.omit(unique(x))
  if (length(vals) == 0) return(NA_character_)
  samples <- head(vals, n)
  paste(samples, collapse = ", ")
}

# ==============================================================================
# DATA DICTIONARY
# ==============================================================================

#' Write data dictionary to CSV
#' 
#' @param dict Data dictionary data.frame
#' @param output_path Path to save CSV
write_data_dictionary <- function(dict, output_path) {
  write_csv(dict, output_path)
  message(glue("Wrote data dictionary to: {output_path}"))
  invisible(dict)
}