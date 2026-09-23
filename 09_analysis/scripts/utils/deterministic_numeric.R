# Deterministic floating-point reductions for persisted analysis products.
#
# Base and database sums may combine values in input-, thread-, or
# platform-dependent orders. Sorting the finite summands canonically and using
# compensated addition makes the reduction independent of source row order
# while retaining more precision than a simple left-to-right sum.

deterministic_sum <- function(x, na.rm = FALSE) {
  x <- as.double(x)
  if (na.rm) {
    x <- x[!is.na(x)]
  } else {
    if (any(is.na(x) & !is.nan(x))) return(NA_real_)
    }
  if (!length(x)) return(0)

  # Keep infinities and NaNs consistent with sum(). Persisted analysis inputs
  # are expected to be finite, but explicit handling avoids an unstable sort.
  if (any(is.nan(x))) return(NaN)
  if (any(is.infinite(x))) return(sum(x))

  x <- x[order(abs(x), x, method = "radix")]
  total <- 0
  compensation <- 0
  for (value in x) {
    adjusted <- value - compensation
    updated <- total + adjusted
    compensation <- (updated - total) - adjusted
    total <- updated
  }
  total
}

deterministic_weighted_mean <- function(value, weight) {
  usable <- !is.na(value) & !is.na(weight) & weight > 0
  if (!any(usable)) return(NA_real_)
  deterministic_sum(value[usable] * weight[usable]) /
    deterministic_sum(weight[usable])
}
