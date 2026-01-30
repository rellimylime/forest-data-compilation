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
# GENERIC METADATA EXTRACTION
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
      range_or_levels = get_range_or_levels(x),
      description = "",
      units = "",
      notes = ""
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

#' List layers in a geodatabase
#' @param gdb_path Path to .gdb directory
#' @return sf layers object
list_gdb_layers <- function(gdb_path) {
  st_layers(gdb_path)
}

# ==============================================================================
# SCHEMA MANAGEMENT
# ==============================================================================

#' Load official schema for a dataset
#' 
#' @param dataset_name Name of dataset (e.g., "ids", "terraclimate")
#' @return data.frame with schema definition
load_schema <- function(dataset_name) {
  schema_path <- here(dataset_folder, "schema.csv")
  
  if (!file.exists(schema_path)) {
    stop(glue("Schema not found: {schema_path}"))
  }
  
  read_csv(schema_path, show_col_types = FALSE)
}

#' Get schema for a specific layer
#' 
#' @param schema Full schema data.frame
#' @param layer_name Name of layer
#' @return Filtered schema for that layer
get_layer_schema <- function(schema, layer_name) {
  schema |> filter(layer == layer_name)
}

# ==============================================================================
# VALIDATION
# ==============================================================================

#' Validate extracted metadata against official schema
#' 
#' @param extracted Metadata from extract_gdb_metadata()
#' @param schema Schema from load_schema()
#' @param layer_name Layer name to validate
#' @return List with validation results
validate_metadata <- function(extracted, schema, layer_name) {
  
  layer_schema <- get_layer_schema(schema, layer_name)
  
  expected_fields <- layer_schema$field_name
  actual_fields <- extracted$field_name
  
  results <- list(
    layer = layer_name,
    n_expected = length(expected_fields),
    n_actual = length(actual_fields),
    
    # Fields in schema but missing from data
    missing_fields = setdiff(expected_fields, actual_fields),
    
    # Fields in data but not in schema (unexpected)
    extra_fields = setdiff(actual_fields, expected_fields),
    
    # Fields present in both
    matched_fields = intersect(expected_fields, actual_fields),
    
    # Detailed issues
    issues = list()
  )
  
  # Check for high missing rates on required fields
  required_fields <- layer_schema |> 
    filter(required == "yes") |> 
    pull(field_name)
  
  for (field in required_fields) {
    if (field %in% actual_fields) {
      pct_missing <- extracted |> 
        filter(field_name == field) |> 
        pull(pct_missing)
      
      if (pct_missing > 5) {
        results$issues[[length(results$issues) + 1]] <- list(
          field = field,
          type = "high_missing",
          message = glue("{field}: {pct_missing}% missing (required field)")
        )
      }
    }
  }
  
  # Summary
  results$is_valid <- length(results$missing_fields) == 0 && 
    length(results$issues) == 0
  
  class(results) <- c("metadata_validation", class(results))
  results
}

#' Print validation results
#' @param x Validation results
#' @param ... Additional arguments
print.metadata_validation <- function(x, ...) {
  cat(glue("\n=== Validation: {x$layer} ===\n\n"))
  cat(glue("Expected fields: {x$n_expected}\n"))
  cat(glue("Actual fields: {x$n_actual}\n"))
  cat(glue("Matched: {length(x$matched_fields)}\n\n"))
  
  if (length(x$missing_fields) > 0) {
    cat("MISSING from data:\n")
    cat(paste(" -", x$missing_fields, collapse = "\n"), "\n\n")
  }
  
  if (length(x$extra_fields) > 0) {
    cat("EXTRA in data (not in schema):\n")
    cat(paste(" -", x$extra_fields, collapse = "\n"), "\n\n")
  }
  
  if (length(x$issues) > 0) {
    cat("ISSUES:\n")
    for (issue in x$issues) {
      cat(glue(" - {issue$message}\n"))
    }
    cat("\n")
  }
  
  if (x$is_valid) {
    cat("Status: VALID\n")
  } else {
    cat("Status: NEEDS ATTENTION\n")
  }
  
  invisible(x)
}

# ==============================================================================
# DATA DICTIONARY GENERATION
# ==============================================================================

#' Generate data dictionary by merging extracted metadata with schema
#' 
#' @param extracted Metadata from extract_gdb_metadata()
#' @param schema Schema from load_schema()
#' @param layer_name Layer name
#' @return data.frame formatted as data dictionary
generate_data_dictionary <- function(extracted, schema, layer_name) {
  
  layer_schema <- get_layer_schema(schema, layer_name)
  
  # Merge extracted stats with schema descriptions
  dict <- extracted |>
    left_join(
      layer_schema |> select(field_name, description, domain, required, notes),
      by = "field_name"
    ) |>
    # Reorder and select final columns
    select(
      field_name,
      description,
      r_class,
      domain,
      range_or_levels,
      pct_missing,
      required,
      sample_values,
      notes
    ) |>
    # Fill in missing descriptions with placeholder
    mutate(
      description = if_else(is.na(description) | description == "", 
                            "[NOT IN SCHEMA - VERIFY]", 
                            description)
    )
  
  # Add layer info
  attr(dict, "layer_name") <- layer_name
  attr(dict, "generated_date") <- Sys.Date()
  
  dict
}

#' Write data dictionary to CSV
#' 
#' @param dict Data dictionary from generate_data_dictionary()
#' @param output_path Path to save CSV
write_data_dictionary <- function(dict, output_path) {
  
  write_csv(dict, output_path)
  message(glue("Wrote data dictionary to: {output_path}"))
  invisible(dict)
}

# ==============================================================================
# SUMMARY FUNCTIONS
# ==============================================================================

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