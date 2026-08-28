# Connected remeasurement components over FIA's official PREV_PLT_CN links.
#
# One physical FIA plot (`stable_plot_id`) can hold several separate remeasurement
# histories. FIA supplies a previous-visit link only when it considers the two
# records a genuine remeasurement; when that link is absent, the visits are not
# connected even though they share a location. This file turns the official links
# into connected components so no calculated change can cross a break.
#
#   2000 A
#   2005 B  PREV_PLT_CN = A
#   2010 C  PREV_PLT_CN = NULL      <- break
#   2015 D  PREV_PLT_CN = C
#
# gives components {A, B} and {C, D}. A -> D is never a valid response.
#
# Two rules that are easy to get backwards:
#
#   * Chronological adjacency is not evidence. A missing PREV_PLT_CN stays
#     missing; the nearest earlier visit is not a substitute.
#   * Chronological adjacency is not required either. If FIA links a 2010 visit
#     to a 2000 visit, that edge is real and is followed even though a 2005 visit
#     sits between them.
#
# Component identifiers are derived from the smallest PLT_CN in the component,
# so the same inputs always produce the same identifier regardless of row order,
# partition order, or how many states were processed in one pass.

suppressPackageStartupMessages({
  library(data.table)
})

FIA_COMPONENT_ID_VERSION <- "fia_component_v1"

# ------------------------------------------------------------------------------
# Edge construction
# ------------------------------------------------------------------------------

#' Classify every visit's official predecessor link.
#'
#' Returns the input with one row per visit plus the columns needed to decide
#' whether the link may become a scientific edge. Nothing is dropped: visits with
#' unusable links are retained and labelled so they can be counted as attrition.
fia_classify_visit_links <- function(visits) {
  out <- copy(as.data.table(visits))
  required <- c("PLT_CN", "PREV_PLT_CN", "stable_plot_id")
  missing <- setdiff(required, names(out))
  if (length(missing) > 0L) {
    stop(
      "Cannot classify FIA visit links; missing column(s): ",
      paste(missing, collapse = ", ")
    )
  }
  if (anyDuplicated(out$PLT_CN)) {
    stop("FIA visit table must be unique by PLT_CN before building components.")
  }

  # Compare identifiers as text so bit64 PLT_CN values never lose precision.
  out[, plt_cn_key__ := as.character(PLT_CN)]
  out[, prev_key__ := as.character(PREV_PLT_CN)]

  targets <- out[, .(plt_cn_key__, target_stable_plot_id__ = stable_plot_id)]
  out <- merge(
    out,
    targets,
    by.x = "prev_key__",
    by.y = "plt_cn_key__",
    all.x = TRUE,
    sort = FALSE
  )

  out[, official_link_present := !is.na(prev_key__)]
  out[, official_target_available := !is.na(target_stable_plot_id__)]
  out[, official_target_same_stable_plot :=
    official_target_available &
      !is.na(stable_plot_id) &
      target_stable_plot_id__ == stable_plot_id]

  # Connectivity only. All three conditions, and nothing else:
  #   1. an explicit PREV_PLT_CN is present,
  #   2. it resolves to a visit available in the snapshot,
  #   3. that visit belongs to the same stable_plot_id.
  #
  # Deliberately NOT part of this flag: whether either endpoint was sampled,
  # whether the dates are ordered, whether the visit carries community data for
  # some life stage, or whether it can serve as a response endpoint. Those are
  # downstream usability criteria applied per analysis. Folding any of them in
  # here would let a plot's connectivity change depending on which life stage
  # someone happened to be modelling.
  out[, connectivity_edge_valid := official_link_present &
    official_target_available &
    official_target_same_stable_plot]
  out[is.na(connectivity_edge_valid), connectivity_edge_valid := FALSE]

  out[, visit_link_status := fcase(
    !official_link_present, "no_official_link",
    !official_target_available, "official_target_unavailable",
    !official_target_same_stable_plot, "official_target_other_stable_plot",
    default = "official_link_valid"
  )]

  out[, target_stable_plot_id__ := NULL]
  out[]
}

#' Assign a deterministic remeasurement component to every visit.
#'
#' `visits` needs `PLT_CN`, `PREV_PLT_CN`, and `stable_plot_id`. Components are
#' built only from official same-plot links; every other visit becomes its own
#' single-visit component.
fia_add_remeasurement_components <- function(visits) {
  out <- fia_classify_visit_links(visits)

  n <- nrow(out)
  if (n == 0L) {
    out[, `:=`(
      remeasurement_component_id = character(),
      n_visits_in_component = integer()
    )]
    return(out[])
  }

  vertex_index <- seq_len(n)
  names(vertex_index) <- out$plt_cn_key__

  edges <- out[connectivity_edge_valid == TRUE, .(prev_key__, plt_cn_key__)]
  label <- vertex_index

  if (nrow(edges) > 0L) {
    from <- unname(vertex_index[edges$prev_key__])
    to <- unname(vertex_index[edges$plt_cn_key__])

    # Label propagation: every vertex takes the smallest label it can reach.
    # FIA remeasurement chains are short, so this settles in a few passes and
    # avoids adding a graph-library dependency for one operation.
    iteration <- 0L
    repeat {
      iteration <- iteration + 1L
      pair_min <- pmin(label[from], label[to])
      # Scatter-min: a vertex may appear in several edges, so aggregate first.
      scattered <- data.table(
        vertex = c(from, to),
        candidate = c(pair_min, pair_min)
      )[, .(smallest = min(candidate)), by = vertex]

      new_label <- label
      new_label[scattered$vertex] <- pmin(
        label[scattered$vertex],
        scattered$smallest
      )
      if (identical(new_label, label)) break
      label <- new_label
      if (iteration > 1000L) {
        stop("Component labelling failed to converge; check the edge list.")
      }
    }
  }

  out[, component_root__ := label]
  # Name the component after the smallest PLT_CN it contains, so the identifier
  # is a property of the data rather than of the iteration order.
  out[, component_anchor__ := min(plt_cn_key__), by = component_root__]
  out[, remeasurement_component_id := paste(
    FIA_COMPONENT_ID_VERSION,
    stable_plot_id,
    component_anchor__,
    sep = "|"
  )]
  out[, n_visits_in_component := .N, by = remeasurement_component_id]

  out[, c("component_root__", "component_anchor__") := NULL]
  out[]
}

#' Reduce a visit table to one row per component with its endpoints.
#'
#' `order_cols` decides which visit is first and which is last. Pass measurement
#' date bounds rather than INVYR alone when they are available, so the direction
#' of change matches the direction of time.
#'
#' Only visits flagged usable contribute endpoints, but every visit in the
#' component is counted, because an intermediate visit still establishes
#' connectivity even when it carries no community data.
fia_component_endpoints <- function(
  visits,
  usable_col = NULL,
  order_cols = c("measurement_date_lower", "INVYR", "PLT_CN")
) {
  out <- copy(as.data.table(visits))
  required <- c("remeasurement_component_id", "stable_plot_id", "PLT_CN")
  missing <- setdiff(required, names(out))
  if (length(missing) > 0L) {
    stop(
      "Cannot select component endpoints; missing column(s): ",
      paste(missing, collapse = ", ")
    )
  }
  order_cols <- intersect(order_cols, names(out))
  if (length(order_cols) == 0L) {
    stop("No usable ordering column was supplied for component endpoints.")
  }

  out[, n_visits_in_component := .N, by = remeasurement_component_id]

  usable <- if (is.null(usable_col)) {
    out
  } else {
    if (!usable_col %in% names(out)) {
      stop("Usable-visit column not found: ", usable_col)
    }
    out[out[[usable_col]] == TRUE]
  }
  if (nrow(usable) == 0L) {
    return(usable[0])
  }

  setorderv(usable, c("remeasurement_component_id", order_cols))
  usable[, usable_visit_number := seq_len(.N), by = remeasurement_component_id]
  usable[, n_usable_visits_in_component := .N, by = remeasurement_component_id]

  endpoints <- usable[
    usable_visit_number == 1L |
      usable_visit_number == n_usable_visits_in_component
  ]
  endpoints[, component_endpoint := fifelse(
    usable_visit_number == 1L,
    "first",
    "last"
  )]
  # A one-visit component has no direction, so it cannot supply a change response.
  endpoints[n_usable_visits_in_component < 2L, component_endpoint := "only"]
  endpoints[]
}

#' Confirm that stable-condition rows remain inside an official component.
#'
#' PREV_PLT_CN defines visit connectivity. The current analysis may select the
#' same numeric CONDID inside those visits, but CONDID cannot define the visit
#' history and SUBP_COND_CHNG_MTRX is not used.
fia_assert_condition_history_linkage <- function(
  response,
  key_cols = NULL,
  label = "response table"
) {
  column_names <- names(as.data.table(response))

  if (!is.null(key_cols)) {
    keyed_on_condition <- any(grepl("^condid$", key_cols, ignore.case = TRUE))
    keyed_on_component <- any(grepl(
      "^remeasurement_component_id$", key_cols, ignore.case = TRUE
    ))
    if (keyed_on_condition && !keyed_on_component) {
      stop(
        label, " uses CONDID without remeasurement_component_id. ",
        "PREV_PLT_CN, not CONDID, must define the visit history."
      )
    }
  }

  lower_names <- tolower(column_names)
  if (all(c("t1_condid", "t2_condid") %in% lower_names)) {
    t1 <- response[[column_names[match("t1_condid", lower_names)]]]
    t2 <- response[[column_names[match("t2_condid", lower_names)]]]
    if (any(!is.na(t1) & !is.na(t2) & t1 != t2)) {
      stop(label, " contains different T1 and T2 CONDID values.")
    }
  }

  if ("condid" %in% lower_names &&
      !"remeasurement_component_id" %in% lower_names &&
      any(grepl("(^|_)(first|last|previous|prev|current|t1|t2)(_|$)",
                lower_names))) {
    stop(
      label, " carries a stable CONDID history without a remeasurement component."
    )
  }

  if ("prevcond" %in% lower_names) {
    stop(
      label, " contains PREVCOND. The current analysis does not use the ",
      "condition-change matrix."
    )
  }

  invisible(TRUE)
}

#' Confirm a paired table never joins two different components.
#'
#' Intended as a guard inside producers, not only inside tests: a silent
#' cross-component pair is the failure mode this whole file exists to prevent.
fia_assert_within_component <- function(
  pairs,
  first_component_col,
  last_component_col,
  label = "paired table"
) {
  d <- as.data.table(pairs)
  crossing <- d[
    !is.na(get(first_component_col)) &
      !is.na(get(last_component_col)) &
      get(first_component_col) != get(last_component_col)
  ]
  if (nrow(crossing) > 0L) {
    stop(
      label, " has ", format(nrow(crossing), big.mark = ","),
      " row(s) whose endpoints belong to different remeasurement components. ",
      "A change response may not cross a break in FIA's PREV_PLT_CN chain."
    )
  }
  invisible(TRUE)
}
