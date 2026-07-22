# ------------------------------------------------------------------------------
# MTBS thematic burn-severity helpers.
# ------------------------------------------------------------------------------

# Summarize one vector of MTBS thematic classes. Non-processing mask classes are
# reported but excluded from all severity denominators. The class numbers are
# categories, not a continuous scale, so this helper deliberately does not
# calculate a numeric mean of class codes.
summarize_mtbs_classes <- function(severity,
                                   valid_classes,
                                   high_severity_classes,
                                   non_processing_classes) {
  severity <- as.integer(severity)
  severity <- severity[!is.na(severity) & severity %in% valid_classes]

  n_pixels_total <- length(severity)
  is_masked <- severity %in% non_processing_classes
  observed <- severity[!is_masked]
  n_pixels_masked <- sum(is_masked)
  n_pixels_valid <- length(observed)

  dominant <- NA_integer_
  frac_high <- NA_real_
  if (n_pixels_valid > 0) {
    counts <- table(observed)
    dominant <- as.integer(names(counts)[which.max(counts)])
    frac_high <- mean(observed %in% high_severity_classes)
  }

  list(
    n_pixels_total = as.integer(n_pixels_total),
    n_pixels_valid = as.integer(n_pixels_valid),
    n_pixels_masked = as.integer(n_pixels_masked),
    frac_pixels_masked = if (n_pixels_total > 0) {
      n_pixels_masked / n_pixels_total
    } else {
      NA_real_
    },
    frac_high_severity = frac_high,
    dominant_severity_class = dominant
  )
}
