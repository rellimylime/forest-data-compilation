# ==============================================================================
# fia_seedling.R
# Seedling eligibility contract.
#
# FIA includes a seedling record in its calculated abundance via TREECOUNT_CALC
# ("tree count used in calculations", which drives SEEDLING.TPA_UNADJ), NOT via
# the raw field count TREECOUNT, which is null in many states/inventories
# (e.g. ME ~21%, OR/CA ~20%). Eligibility therefore follows the calculated
# abundance: a positive TREECOUNT_CALC OR a valid positive TPA_UNADJ. Raw
# TREECOUNT is used only as a last-resort fallback when neither calculated field
# is available at all (older files). TREECOUNT's missingness never discards a
# record that FIA counts.
# ==============================================================================

seedling_eligible <- function(treecount, treecount_calc, tpa_unadj) {
  has_calc <- !is.na(treecount_calc) & treecount_calc > 0
  has_tpa  <- !is.na(tpa_unadj) & tpa_unadj > 0
  calc_fields_available <- any(!is.na(treecount_calc)) || any(!is.na(tpa_unadj))
  fallback <- (!calc_fields_available) & !is.na(treecount) & treecount > 0
  has_calc | has_tpa | fallback
}
