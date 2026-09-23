# Shared model-sample helpers.

# Return histories represented in every required layer. Rows from any other
# layer are ignored, so an unrelated pair (for example seedlings plus adults)
# cannot be mistaken for the sapling-plus-adult common sample.
complete_history_ids_for_layers <- function(data, required_layers) {
  if (!data.table::is.data.table(data)) {
    stop("data must be a data.table.")
  }
  missing_columns <- setdiff(c("history_id", "layer"), names(data))
  if (length(missing_columns)) {
    stop("Missing columns: ", paste(missing_columns, collapse = ", "))
  }
  required_layers <- unique(required_layers)
  if (!length(required_layers) || anyNA(required_layers)) {
    stop("required_layers must contain at least one non-missing layer.")
  }

  present <- unique(
    data[layer %in% required_layers, c("history_id", "layer"), with = FALSE]
  )
  present[, .(n_required_layers = data.table::uniqueN(layer)), by = history_id][
    n_required_layers == length(required_layers), history_id
  ]
}
