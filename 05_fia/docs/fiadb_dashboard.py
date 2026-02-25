"""
FIA Database Schema Navigator
==============================
Streamlit app for navigating the Forest Inventory and Analysis Database (FIADB v9.4).

Metadata-first design: all schema navigation runs off PRAGMA table_info() calls,
which are instantaneous on a 70 GB SQLite file. No bulk row loading unless the
user explicitly requests a preview.

Usage:
  streamlit run fiadb_dashboard.py

  Then paste the full path to your FIADB SQLite file in the sidebar.

Requires:
  pip install streamlit pandas pyvis   # pyvis recommended for interactive graph
  pip install graphviz                  # fallback static diagram

Source: FIADB User Guide v9.4, August 2025
"""

import os
import sqlite3
from typing import Dict, List, Optional, Tuple

import pandas as pd
import streamlit as st
import streamlit.components.v1 as components

try:
    from pyvis.network import Network
    _PYVIS_AVAILABLE = True
except ImportError:
    _PYVIS_AVAILABLE = False

try:
    from graphviz import Digraph
    _GRAPHVIZ_AVAILABLE = True
except ImportError:
    _GRAPHVIZ_AVAILABLE = False


# =============================================================================
# HARDCODED FIA METADATA  (FIADB User Guide v9.4, August 2025)
# =============================================================================

TABLE_CATEGORIES: Dict[str, List[str]] = {
    "Location Level": [
        "SURVEY", "PROJECT", "COUNTY", "PLOT", "COND",
        "SUBPLOT", "SUBP_COND", "BOUNDARY", "SUBP_COND_CHNG_MTRX",
    ],
    "Tree Level": [
        "TREE", "TREE_WOODLAND_STEMS", "TREE_GRM_COMPONENT",
        "TREE_GRM_THRESHOLD", "TREE_GRM_MIDPT", "TREE_GRM_BEGIN",
        "TREE_GRM_ESTN", "BEGINEND", "SEEDLING", "SITETREE",
    ],
    "Invasive / Understory Vegetation": [
        "INVASIVE_SUBPLOT_SPP", "P2VEG_SUBPLOT_SPP", "P2VEG_SUBP_STRUCTURE",
    ],
    "Down Woody Material": [
        "DWM_VISIT", "DWM_COARSE_WOODY_DEBRIS", "DWM_DUFF_LITTER_FUEL",
        "DWM_FINE_WOODY_DEBRIS", "DWM_MICROPLOT_FUEL",
        "DWM_RESIDUAL_PILE", "DWM_TRANSECT_SEGMENT", "COND_DWM_CALC",
    ],
    "NRS Tree Regeneration": [
        "PLOT_REGEN", "SUBPLOT_REGEN", "SEEDLING_REGEN",
    ],
    "Ground Cover (PNWRS)": [
        "GRND_CVR", "GRND_LYR_FNCTL_GRP", "GRND_LYR_MICROQUAD",
    ],
    "Soils (PNWRS)": [
        "SUBP_SOIL_SAMPLE_LOC", "SUBP_SOIL_SAMPLE_LAYER",
    ],
    "Population": [
        "POP_ESTN_UNIT", "POP_EVAL", "POP_EVAL_ATTRIBUTE",
        "POP_EVAL_GRP", "POP_EVAL_TYP",
        "POP_PLOT_STRATUM_ASSGN", "POP_STRATUM",
    ],
    "Plot Geometry / Snapshot": ["PLOTGEOM", "PLOTSNAP"],
    "Reference": [
        "REF_SPECIES", "REF_FOREST_TYPE", "REF_FOREST_TYPE_GROUP",
        "REF_SPECIES_GROUP", "REF_PLANT_DICTIONARY",
        "REF_INVASIVE_SPECIES", "REF_DAMAGE_AGENT", "REF_DAMAGE_AGENT_GROUP",
        "REF_GRM_TYPE", "REF_POP_ATTRIBUTE", "REF_POP_EVAL_TYP_DESCR",
        "REF_HABTYP_DESCRIPTION", "REF_HABTYP_PUBLICATION",
        "REF_CITATION", "REF_FIADB_VERSION",
        "REF_STATE_ELEV", "REF_UNIT", "REF_RESEARCH_STATION",
        "REF_OWNGRPCD", "REF_SIEQN",
        "REF_INTL_TO_DOYLE_FACTOR", "REF_TREE_CARBON_RATIO_DEAD",
        "REF_TREE_DECAY_PROP", "REF_TREE_STND_DEAD_CR_PROP",
        "REF_GRND_LYR", "REF_STD_NORM_DIST",
        "REF_NVCS_HIERARCHY_STRCT",
        "REF_NVCS_LEVEL_1_CODES", "REF_NVCS_LEVEL_2_CODES",
        "REF_NVCS_LEVEL_3_CODES", "REF_NVCS_LEVEL_4_CODES",
        "REF_NVCS_LEVEL_5_CODES", "REF_NVCS_LEVEL_6_CODES",
        "REF_NVCS_LEVEL_7_CODES", "REF_NVCS_LEVEL_8_CODES",
        "REF_FVS_VAR_NAME", "REF_FVS_LOC_NAME",
        "REF_DIFFERENCE_TEST_PER_ACRE", "REF_DIFFERENCE_TEST_TOTALS",
    ],
}

TABLE_DESCRIPTIONS: Dict[str, str] = {
    "SURVEY": (
        "One record per state per inventory year. Root administrative table. "
        "FK out: PRJ_CN → PROJECT.CN. "
        "FK in: PLOT.SRV_CN → SURVEY.CN. "
        "Key fields: STATECD, STATEAB, INVYR, RSCD (research station), ANN_INVENTORY."
    ),
    "PROJECT": (
        "Administrative project grouping for surveys. "
        "PK: CN. UK: RSCD, NAME. "
        "Referenced by SURVEY via PRJ_CN."
    ),
    "COUNTY": (
        "Reference table mapping STATECD + UNITCD + COUNTYCD to county name. "
        "PK: CN. UK: STATECD, UNITCD, COUNTYCD. "
        "Referenced by PLOT via CTY_CN."
    ),
    "PLOT": (
        "Central field measurement unit — one record per plot per inventory visit. "
        "Contains STATECD, COUNTYCD, INVYR, LAT/LON (fuzzed ~1 mile), PLOT_STATUS_CD, MEASYEAR. "
        "PK: CN. UK: STATECD, INVYR, UNITCD, COUNTYCD, PLOT. "
        "FKs out: SRV_CN → SURVEY, CTY_CN → COUNTY. "
        "FKs in from: COND, TREE, SEEDLING, SUBPLOT, DWM_*, POP_PLOT_STRATUM_ASSGN, etc."
    ),
    "COND": (
        "Forest condition record. A plot may have multiple conditions (CONDID 1, 2, 3…) "
        "where land cover, owner class, forest type, or stand size differs. "
        "Key fields: PLT_CN, CONDID, COND_STATUS_CD, FORTYPCD, STDSZCD, OWNGRPCD, "
        "CONDPROP_UNADJ (proportion of plot in this condition, sums to 1.0), "
        "DSTRBCD1/YR1 (disturbance type + year, up to 3). "
        "PK: CN. UK: PLT_CN, CONDID. FK: PLT_CN → PLOT.CN."
    ),
    "SUBPLOT": (
        "Individual subplot measurements (4 subplots per plot, each 24-ft radius = 1/24 acre). "
        "Contains subplot-level SLOPE, ASPECT, SUBPCOND (condition at subplot center). "
        "FK: PLT_CN → PLOT.CN."
    ),
    "SUBP_COND": (
        "Maps each subplot portion to a condition. Required for area-proportion calculations. "
        "SUBPCONDPROP_UNADJ = fraction of subplot in the condition. "
        "FK: PLT_CN → PLOT.CN."
    ),
    "BOUNDARY": (
        "Boundary line between adjacent conditions on a plot. "
        "Provides azimuth and distance for mapping condition areas. "
        "FK: PLT_CN → PLOT.CN."
    ),
    "SUBP_COND_CHNG_MTRX": (
        "Tracks how condition class proportions changed between successive visits to the same plot. "
        "Used for area-change estimation. "
        "FK: PLT_CN → PLOT.CN."
    ),
    "TREE": (
        "Individual tree record. DIA ≥ 1.0 inch on subplot; root collar measurement for woodland species. "
        "Key fields: PLT_CN, SUBP, TREE, INVYR, SPCD, DIA, HT, ACTUALHT, STATUSCD, CCLCD, "
        "TPA_UNADJ (per-acre expansion factor), AGENTCD (mortality cause). "
        "BA per tree = 0.005454 × DIA². BA per acre = BA × TPA_UNADJ. "
        "PK: CN. UK: PLT_CN, SUBP, TREE, INVYR. FK: PLT_CN → PLOT.CN."
    ),
    "TREE_WOODLAND_STEMS": (
        "Stem count records for multi-stem woodland tree species measured at root collar. "
        "FK: TRE_CN → TREE.CN."
    ),
    "TREE_GRM_COMPONENT": (
        "Growth/Removal/Mortality components between measurement periods. "
        "MICR_COMPONENT_AL_FOREST codes: SURVIVOR, INGROWTH, MORTALITY1/2 (natural), CUT1/2 (harvest). "
        "MICR_TPAMORT_UNADJ_AL_FOREST = per-acre mortality expansion factor. "
        "Note: does not carry INVYR directly — join to TREE to get INVYR. "
        "FK: TRE_CN → TREE.CN."
    ),
    "TREE_GRM_THRESHOLD": (
        "Diameter threshold values used in GRM computations (ingrowth boundary, etc.). "
        "FK: TRE_CN → TREE.CN."
    ),
    "TREE_GRM_MIDPT": (
        "Midpoint diameter estimates for trees straddling the GRM measurement window. "
        "FK: TRE_CN → TREE.CN."
    ),
    "TREE_GRM_BEGIN": (
        "Beginning-of-period tree attributes for the GRM estimation window. "
        "FK: TRE_CN → TREE.CN."
    ),
    "TREE_GRM_ESTN": (
        "Growth/removal/mortality per-acre estimates and expansion factors. "
        "FK: TRE_CN → TREE.CN."
    ),
    "BEGINEND": (
        "Links a tree's beginning-of-period record to its end-of-period record across inventory visits."
    ),
    "SEEDLING": (
        "Seedling counts on the microplot (6.8-ft radius = 1/300 acre) per species per condition. "
        "TREECOUNT = raw count. Per-acre estimate = TREECOUNT × 300. "
        "Includes conifers ≥ 6 inches tall and hardwoods ≥ 12 inches tall, DBH < 1.0 inch. "
        "FK: PLT_CN → PLOT.CN."
    ),
    "SITETREE": (
        "Site trees used to compute the condition-level site index (COND.SICOND). "
        "Contains total age, height, and species for selected dominant/co-dominant trees. "
        "FK: PLT_CN → PLOT.CN."
    ),
    "INVASIVE_SUBPLOT_SPP": (
        "Invasive plant species presence/cover class by subplot. "
        "JOIN on ITIS_TSN to REF_INVASIVE_SPECIES for species names. "
        "FK: PLT_CN → PLOT.CN."
    ),
    "P2VEG_SUBPLOT_SPP": (
        "Phase 2 vegetation subplot species cover estimates (understory/shrub layer). "
        "FK: PLT_CN → PLOT.CN."
    ),
    "P2VEG_SUBP_STRUCTURE": (
        "Phase 2 vegetation structural attributes (canopy cover strata) by subplot. "
        "FK: PLT_CN → PLOT.CN."
    ),
    "DWM_VISIT": (
        "Down woody material visit-level metadata — sampling method and transect layout. "
        "FK: PLT_CN → PLOT.CN."
    ),
    "DWM_COARSE_WOODY_DEBRIS": (
        "Coarse woody debris (CWD) transect tally — decay class, diameter, species. "
        "FK: PLT_CN → PLOT.CN."
    ),
    "DWM_DUFF_LITTER_FUEL": (
        "Duff, litter, and fine fuel depth and loading estimates per transect point. "
        "FK: PLT_CN → PLOT.CN."
    ),
    "DWM_FINE_WOODY_DEBRIS": (
        "Fine woody debris (FWD) line-intercept tallies by size class. "
        "FK: PLT_CN → PLOT.CN."
    ),
    "DWM_MICROPLOT_FUEL": (
        "Microplot-level fuel load (shrubs, forbs, grasses). "
        "FK: PLT_CN → PLOT.CN."
    ),
    "DWM_RESIDUAL_PILE": (
        "Residual slash pile measurements (height, cover). "
        "FK: PLT_CN → PLOT.CN."
    ),
    "DWM_TRANSECT_SEGMENT": (
        "Transect segment descriptions for DWM sampling layout. "
        "FK: PLT_CN → PLOT.CN."
    ),
    "COND_DWM_CALC": (
        "Condition-level calculated down woody material load summaries. "
        "FK: PLT_CN → PLOT.CN."
    ),
    "PLOT_REGEN": (
        "NRS (Northern Research Station) plot-level regeneration summary. "
        "FK: PLT_CN → PLOT.CN."
    ),
    "SUBPLOT_REGEN": (
        "NRS subplot-level regeneration counts. "
        "FK: PLT_CN → PLOT.CN."
    ),
    "SEEDLING_REGEN": (
        "NRS seedling regeneration detail records. "
        "FK: PLT_CN → PLOT.CN."
    ),
    "GRND_CVR": (
        "PNWRS (Pacific Northwest Research Station) ground cover percent by cover type. "
        "FK: PLT_CN → PLOT.CN."
    ),
    "GRND_LYR_FNCTL_GRP": (
        "PNWRS ground layer functional groups (mosses, lichens, forbs, etc.). "
        "FK: PLT_CN → PLOT.CN."
    ),
    "GRND_LYR_MICROQUAD": (
        "PNWRS microquadrat-level ground layer data. "
        "FK: PLT_CN → PLOT.CN."
    ),
    "SUBP_SOIL_SAMPLE_LOC": (
        "PNWRS subplot soil sample location metadata. "
        "FK: PLT_CN → PLOT.CN."
    ),
    "SUBP_SOIL_SAMPLE_LAYER": (
        "PNWRS soil sample layer attributes (depth, bulk density, texture, carbon). "
        "FK: PLT_CN → PLOT.CN."
    ),
    "POP_ESTN_UNIT": (
        "Population estimation unit — a geographic subdivision of a state used in stratified estimation. "
        "Typically county groups. FK: EVAL_CN → POP_EVAL.CN."
    ),
    "POP_EVAL": (
        "Population evaluation — a set of plots used to estimate a specific attribute for a state/region. "
        "Defines the START_INVYR to END_INVYR window. "
        "FK: EVAL_GRP_CN → POP_EVAL_GRP.CN."
    ),
    "POP_EVAL_ATTRIBUTE": (
        "Lists which estimation attributes (tree volume, area, mortality, etc.) are included in an evaluation. "
        "FK: EVAL_CN → POP_EVAL.CN."
    ),
    "POP_EVAL_GRP": (
        "Evaluation group — groups multiple evaluations sharing the same stratification. "
        "One per state per estimation approach."
    ),
    "POP_EVAL_TYP": (
        "Evaluation type classification (EXPVOL, EXPGROW, EXPMORT, EXPCURR, etc.). "
        "FK: EVAL_CN → POP_EVAL.CN."
    ),
    "POP_PLOT_STRATUM_ASSGN": (
        "Assigns each plot to a stratum within a population evaluation. "
        "Critical for computing population-level estimates using expansion factors. "
        "FK: PLT_CN → PLOT.CN, STRATUM_CN → POP_STRATUM.CN."
    ),
    "POP_STRATUM": (
        "Stratum-level attributes including area estimates (EXPNS), adjustment factors "
        "(ADJ_FACTOR_SUBP, ADJ_FACTOR_MACR), and plot counts. "
        "FK: ESTN_UNIT_CN → POP_ESTN_UNIT.CN."
    ),
    "PLOTGEOM": (
        "Plot geometry — stores higher-precision coordinates for research use (not fuzzed). "
        "Access may be restricted. FK: CN → PLOT.CN (1:1)."
    ),
    "PLOTSNAP": (
        "Snapshot of plot-level attribute summaries used in population estimation. "
        "FK: PLT_CN → PLOT.CN."
    ),
    "REF_SPECIES": (
        "Master species lookup. SPCD → COMMON_NAME, GENUS, SPECIES, SCIENTIFIC_NAME, "
        "SFTWD_HRDWD ('S'/'H'), WOODLAND ('Y'/'N'), SPGRPCD, MAJOR_SPGRPCD, JENKINS_SPGRPCD."
    ),
    "REF_FOREST_TYPE": (
        "Forest type code lookup. FORTYPCD → MEANING (e.g. 221 = 'Ponderosa pine'). "
        "Also contains VALUE (same as FORTYPCD) and TYPGRPCD (forest type group)."
    ),
    "REF_FOREST_TYPE_GROUP": (
        "Groups forest types into broader categories. TYPGRPCD → NAME (e.g. 'Fir / spruce / mountain hemlock')."
    ),
    "REF_SPECIES_GROUP": "Species group codes and descriptions used in GRM and stocking computations.",
    "REF_PLANT_DICTIONARY": "Extended plant taxonomy dictionary. PLANTS_CD → scientific and common names.",
    "REF_INVASIVE_SPECIES": "Reference codes for invasive plant species. ITIS_TSN → common/scientific names.",
    "REF_DAMAGE_AGENT": "Damage agent code lookup. AGENTCD → description (e.g. 10 = 'Insects').",
    "REF_DAMAGE_AGENT_GROUP": "Groups of damage agent codes into broader categories.",
    "REF_GRM_TYPE": "GRM component type codes and descriptions.",
    "REF_POP_ATTRIBUTE": (
        "Reference table for population estimation attribute numbers, SQL query strings, "
        "and estimation basis (per-acre vs. totals)."
    ),
    "REF_POP_EVAL_TYP_DESCR": "Descriptions of population evaluation type codes (EXPVOL, EXPMORT, etc.).",
    "REF_HABTYP_DESCRIPTION": "Habitat type classification description reference.",
    "REF_HABTYP_PUBLICATION": "Publications associated with habitat type classifications.",
    "REF_CITATION": "Bibliography of references cited in FIADB documentation.",
    "REF_FIADB_VERSION": "Database version history and change log.",
    "REF_STATE_ELEV": "Reference elevation ranges by state.",
    "REF_UNIT": "FIA survey unit codes (UNITCD) and descriptions per state.",
    "REF_RESEARCH_STATION": "FIA research station codes (RSCD) and contact information.",
    "REF_OWNGRPCD": "Owner group code descriptions (10=Forest Service, 20=Other Federal, 30=State/Local, 40=Private).",
    "REF_SIEQN": "Site index equation reference by species, region, and base age.",
    "REF_INTL_TO_DOYLE_FACTOR": "Volume conversion factors between International 1/4-inch and Doyle board-foot rules.",
    "REF_TREE_CARBON_RATIO_DEAD": "Carbon ratio reference values by decay class for dead trees.",
    "REF_TREE_DECAY_PROP": "Decay proportion by species group and decay class.",
    "REF_TREE_STND_DEAD_CR_PROP": "Standing dead crown ratio proportions by decay class.",
    "REF_GRND_LYR": "Ground layer cover type codes and descriptions.",
    "REF_STD_NORM_DIST": "Standard normal distribution reference values for difference tests.",
    "REF_NVCS_HIERARCHY_STRCT": "National Vegetation Classification Standard (NVCS) hierarchy structure.",
    "REF_NVCS_LEVEL_1_CODES": "NVCS level 1 classification codes (Formation Division).",
    "REF_NVCS_LEVEL_2_CODES": "NVCS level 2 classification codes.",
    "REF_NVCS_LEVEL_3_CODES": "NVCS level 3 classification codes.",
    "REF_NVCS_LEVEL_4_CODES": "NVCS level 4 classification codes.",
    "REF_NVCS_LEVEL_5_CODES": "NVCS level 5 classification codes.",
    "REF_NVCS_LEVEL_6_CODES": "NVCS level 6 classification codes.",
    "REF_NVCS_LEVEL_7_CODES": "NVCS level 7 classification codes.",
    "REF_NVCS_LEVEL_8_CODES": "NVCS level 8 classification codes (Association level).",
    "REF_FVS_VAR_NAME": "Forest Vegetation Simulator (FVS) variant names.",
    "REF_FVS_LOC_NAME": "FVS location names by variant.",
    "REF_DIFFERENCE_TEST_PER_ACRE": "Reference values for per-acre difference significance tests.",
    "REF_DIFFERENCE_TEST_TOTALS": "Reference values for totals difference significance tests.",
}

# (child_table, child_fk_col, parent_table, parent_pk_col, edge_label)
RELATIONSHIPS: List[Tuple[str, str, str, str, str]] = [
    # Administrative hierarchy
    ("SURVEY",               "PRJ_CN",       "PROJECT",        "CN",       "PRJ_CN"),
    ("PLOT",                 "SRV_CN",       "SURVEY",         "CN",       "SRV_CN"),
    ("PLOT",                 "CTY_CN",       "COUNTY",         "CN",       "CTY_CN"),
    # Location → condition / subplot
    ("COND",                 "PLT_CN",       "PLOT",           "CN",       "PLT_CN"),
    ("SUBPLOT",              "PLT_CN",       "PLOT",           "CN",       "PLT_CN"),
    ("SUBP_COND",            "PLT_CN",       "PLOT",           "CN",       "PLT_CN"),
    ("BOUNDARY",             "PLT_CN",       "PLOT",           "CN",       "PLT_CN"),
    ("SUBP_COND_CHNG_MTRX",  "PLT_CN",       "PLOT",           "CN",       "PLT_CN"),
    # Tree chain
    ("TREE",                 "PLT_CN",       "PLOT",           "CN",       "PLT_CN"),
    ("TREE_WOODLAND_STEMS",  "TRE_CN",       "TREE",           "CN",       "TRE_CN"),
    ("TREE_GRM_COMPONENT",   "TRE_CN",       "TREE",           "CN",       "TRE_CN"),
    ("TREE_GRM_THRESHOLD",   "TRE_CN",       "TREE",           "CN",       "TRE_CN"),
    ("TREE_GRM_MIDPT",       "TRE_CN",       "TREE",           "CN",       "TRE_CN"),
    ("TREE_GRM_BEGIN",       "TRE_CN",       "TREE",           "CN",       "TRE_CN"),
    ("TREE_GRM_ESTN",        "TRE_CN",       "TREE",           "CN",       "TRE_CN"),
    ("SEEDLING",             "PLT_CN",       "PLOT",           "CN",       "PLT_CN"),
    ("SITETREE",             "PLT_CN",       "PLOT",           "CN",       "PLT_CN"),
    # Species lookups
    ("TREE",                 "SPCD",         "REF_SPECIES",    "SPCD",     "SPCD"),
    ("SEEDLING",             "SPCD",         "REF_SPECIES",    "SPCD",     "SPCD"),
    # Forest type lookup
    ("COND",                 "FORTYPCD",     "REF_FOREST_TYPE","FORTYPCD", "FORTYPCD"),
    # Vegetation / invasive
    ("INVASIVE_SUBPLOT_SPP", "PLT_CN",       "PLOT",           "CN",       "PLT_CN"),
    ("P2VEG_SUBPLOT_SPP",    "PLT_CN",       "PLOT",           "CN",       "PLT_CN"),
    ("P2VEG_SUBP_STRUCTURE", "PLT_CN",       "PLOT",           "CN",       "PLT_CN"),
    # Down woody material
    ("DWM_VISIT",             "PLT_CN",      "PLOT",           "CN",       "PLT_CN"),
    ("DWM_COARSE_WOODY_DEBRIS","PLT_CN",     "PLOT",           "CN",       "PLT_CN"),
    ("DWM_DUFF_LITTER_FUEL",  "PLT_CN",      "PLOT",           "CN",       "PLT_CN"),
    ("DWM_FINE_WOODY_DEBRIS", "PLT_CN",      "PLOT",           "CN",       "PLT_CN"),
    ("DWM_MICROPLOT_FUEL",    "PLT_CN",      "PLOT",           "CN",       "PLT_CN"),
    ("DWM_RESIDUAL_PILE",     "PLT_CN",      "PLOT",           "CN",       "PLT_CN"),
    ("DWM_TRANSECT_SEGMENT",  "PLT_CN",      "PLOT",           "CN",       "PLT_CN"),
    ("COND_DWM_CALC",         "PLT_CN",      "PLOT",           "CN",       "PLT_CN"),
    # NRS regeneration
    ("PLOT_REGEN",            "PLT_CN",      "PLOT",           "CN",       "PLT_CN"),
    ("SUBPLOT_REGEN",         "PLT_CN",      "PLOT",           "CN",       "PLT_CN"),
    ("SEEDLING_REGEN",        "PLT_CN",      "PLOT",           "CN",       "PLT_CN"),
    # PNWRS ground / soil
    ("GRND_CVR",              "PLT_CN",      "PLOT",           "CN",       "PLT_CN"),
    ("GRND_LYR_FNCTL_GRP",    "PLT_CN",      "PLOT",           "CN",       "PLT_CN"),
    ("GRND_LYR_MICROQUAD",    "PLT_CN",      "PLOT",           "CN",       "PLT_CN"),
    ("SUBP_SOIL_SAMPLE_LOC",  "PLT_CN",      "PLOT",           "CN",       "PLT_CN"),
    ("SUBP_SOIL_SAMPLE_LAYER","PLT_CN",      "PLOT",           "CN",       "PLT_CN"),
    # Population chain
    ("POP_EVAL",              "EVAL_GRP_CN", "POP_EVAL_GRP",   "CN",       "EVAL_GRP_CN"),
    ("POP_ESTN_UNIT",         "EVAL_CN",     "POP_EVAL",       "CN",       "EVAL_CN"),
    ("POP_EVAL_ATTRIBUTE",    "EVAL_CN",     "POP_EVAL",       "CN",       "EVAL_CN"),
    ("POP_EVAL_TYP",          "EVAL_CN",     "POP_EVAL",       "CN",       "EVAL_CN"),
    ("POP_STRATUM",           "ESTN_UNIT_CN","POP_ESTN_UNIT",  "CN",       "ESTN_UNIT_CN"),
    ("POP_PLOT_STRATUM_ASSGN","PLT_CN",      "PLOT",           "CN",       "PLT_CN"),
    ("POP_PLOT_STRATUM_ASSGN","STRATUM_CN",  "POP_STRATUM",    "CN",       "STRATUM_CN"),
    # Plot geometry / snapshot
    ("PLOTGEOM",              "CN",          "PLOT",           "CN",       "CN (1:1)"),
    ("PLOTSNAP",              "PLT_CN",      "PLOT",           "CN",       "PLT_CN"),
]

FIELD_CODEBOOK: Dict[str, Dict] = {
    "STATUSCD": {
        "description": "Tree status — live or dead. Filter STATUSCD=1 for live-tree analyses.",
        "codes": {"0": "Dead, removed from plot", "1": "Live tree", "2": "Standing dead tree"},
    },
    "CCLCD": {
        "description": "Crown class code — canopy position. Used to define overstory vs. understory.",
        "codes": {
            "1": "Open grown (no competition)",
            "2": "Dominant (crown above general canopy level)",
            "3": "Co-dominant (crown at general canopy level)",
            "4": "Intermediate (crown below general canopy)",
            "5": "Overtopped (crown entirely below canopy)",
        },
    },
    "AGENTCD": {
        "description": (
            "Mortality agent — what killed the tree. On TREE records and in TREE_GRM_COMPONENT. "
            "10-series = insects, 20-series = disease, 30-series = fire."
        ),
        "codes": {
            "00": "Unknown / not recorded",
            "10": "Insects (general)",
            "11": "Bark beetles",
            "12": "Defoliating insects",
            "20": "Disease (general)",
            "21": "Canker disease / stem blights",
            "22": "Root disease",
            "23": "Dwarf mistletoe",
            "30": "Fire (crown or ground)",
            "40": "Animal damage",
            "50": "Weather (wind, ice, snow)",
            "54": "Drought",
            "60": "Vegetation competition",
            "70": "Unknown / other",
            "80": "Human-caused (non-harvest)",
            "90": "Geologic disturbance",
            "95": "Logging / harvesting",
        },
    },
    "COND_STATUS_CD": {
        "description": (
            "Condition land classification. Filter COND_STATUS_CD=1 to restrict to "
            "accessible forest land for tree/area analyses."
        ),
        "codes": {
            "1": "Accessible forest land (≥10% canopy cover by live tally trees, ≥1 acre, ≥120 ft wide)",
            "2": "Nonforest land",
            "3": "Noncensus water (30–200 ft wide / 1.0–4.5 acres)",
            "4": "Census water (≥200 ft wide or ≥4.5 acres)",
            "5": "Nonsampled — possible forest land",
        },
    },
    "PLOT_STATUS_CD": {
        "description": "Whether the plot was successfully sampled this visit.",
        "codes": {
            "1": "Sampled (data collected)",
            "2": "Nonsampled — denied access",
            "3": "Nonsampled — other reason",
        },
    },
    "SFTWD_HRDWD": {
        "description": "Softwood vs. hardwood designation (from REF_SPECIES). Determines measurement method and volume equations.",
        "codes": {"S": "Softwood (conifer)", "H": "Hardwood (broadleaf)"},
    },
    "WOODLAND": {
        "description": (
            "Woodland species flag (from REF_SPECIES). Woodland species are measured at root collar "
            "diameter (DRC), not DBH. Affects volume and basal area equations."
        ),
        "codes": {"Y": "Woodland species (root collar measurement)", "N": "Standard timber species (breast height)"},
    },
    "OWNGRPCD": {
        "description": "Ownership group for the condition. Useful for filtering public vs. private forest.",
        "codes": {
            "10": "National Forest / Forest Service",
            "20": "Other Federal (NPS, BLM, FWS, DoD, etc.)",
            "30": "State and local government",
            "40": "Private (individual, corporate, NGO)",
        },
    },
    "STDSZCD": {
        "description": (
            "Stand-size class based on predominant diameter of live trees (algorithm-assigned). "
            "See also FLDSZCD (field-assigned)."
        ),
        "codes": {
            "1": "Large diameter (softwood ≥9.0″ d.b.h. / hardwood ≥11.0″)",
            "2": "Medium diameter (5.0–8.9″ softwood / 5.0–10.9″ hardwood)",
            "3": "Small diameter (predominance <5.0″)",
            "5": "Nonstocked (all live stocking value <10)",
        },
    },
    "DSTRBCD1": {
        "description": (
            "Disturbance code — type of disturbance observed since last visit or within 5 years. "
            "Up to three disturbances per condition (DSTRBCD1/2/3 + DSTRBYR1/2/3)."
        ),
        "codes": {
            "0":  "No visible disturbance",
            "10": "Insect damage (general)",
            "11": "Insect damage to understory vegetation",
            "12": "Insect damage to trees (incl. seedlings/saplings)",
            "20": "Disease damage (general)",
            "21": "Disease damage to understory vegetation",
            "22": "Disease damage to trees (incl. seedlings/saplings)",
            "30": "Fire damage (crown or ground, prescribed or natural)",
            "31": "Ground fire damage",
            "32": "Crown fire damage",
            "40": "Animal damage (general)",
            "50": "Weather damage (general)",
            "51": "Ice",
            "52": "Wind (hurricane, tornado)",
            "53": "Flooding (weather-induced)",
            "54": "Drought",
            "60": "Vegetation competition (suppression, vines)",
            "70": "Unknown / other",
            "80": "Human-induced (not in TREATMENT codes)",
            "90": "Geologic disturbance",
            "91": "Landslide",
            "92": "Avalanche track",
            "93": "Volcanic blast zone",
        },
    },
    "TRTCD1": {
        "description": "Treatment code — type of stand treatment since last visit or within 5 years (up to 3 per condition).",
        "codes": {
            "00": "No observable treatment",
            "10": "Cutting — removal of one or more trees",
            "20": "Site preparation (clearing, slash burning, disking, bedding)",
            "30": "Artificial regeneration (planting or seeding following disturbance)",
            "40": "Natural regeneration (natural seeding/sprouting following disturbance)",
            "50": "Other silvicultural treatment (fertilizing, herbicide, girdling, pruning)",
        },
    },
    "MICR_COMPONENT_AL_FOREST": {
        "description": (
            "GRM component type — what happened to the tree between the two measurement periods. "
            "Filter to MORTALITY1/2 for natural mortality, CUT1/2 for harvest."
        ),
        "codes": {
            "SURVIVOR":   "Tree survived from T1 to T2",
            "INGROWTH":   "Tree grew into the tallied size class (DIA ≥ 1.0″) during the period",
            "MORTALITY1": "Natural mortality (primary period)",
            "MORTALITY2": "Natural mortality (secondary period)",
            "CUT1":       "Harvested (primary cut period)",
            "CUT2":       "Harvested (secondary cut period)",
        },
    },
    "FORTYPCD": {
        "description": "Forest type code (algorithm-assigned). Join to REF_FOREST_TYPE for name. 999 = nonstocked.",
        "codes": {
            "101": "Jack pine",
            "121": "White / red / jack pine",
            "201": "Douglas-fir",
            "202": "Port-Orford-cedar",
            "221": "Ponderosa pine",
            "260": "Fir / spruce / mountain hemlock",
            "280": "Lodgepole pine",
            "301": "Western hemlock",
            "341": "Tanoak",
            "400": "Oak / pine",
            "500": "Oak / hickory",
            "503": "Post oak / blackjack oak",
            "701": "Elm / ash / cottonwood",
            "801": "Maple / beech / birch",
            "999": "Nonstocked (formerly forested, <10% stocking)",
        },
    },
}

PIPELINE_TEMPLATES = [
    {
        "name": "Tree basal area by species over time",
        "goal": "Core metric for diversity / thermophilization studies — per-species BA trends across inventory years",
        "tables": ["PLOT", "TREE", "REF_SPECIES"],
        "steps": [
            "PLOT: filter STATECD, INVYR range, PLOT_STATUS_CD = 1 (sampled plots only)",
            "JOIN TREE ON PLOT.CN = TREE.PLT_CN",
            "TREE: filter STATUSCD = 1 (live), DIA ≥ 1.0, TPA_UNADJ > 0",
            "Compute per-tree BA: ba_sqft = 0.005454 × DIA²",
            "Compute per-acre BA: ba_per_acre = ba_sqft × TPA_UNADJ",
            "JOIN REF_SPECIES ON TREE.SPCD = REF_SPECIES.SPCD — add COMMON_NAME, SFTWD_HRDWD, WOODLAND",
            "GROUP BY PLT_CN, INVYR, SPCD — SUM(ba_per_acre)",
        ],
        "key_columns": {
            "PLOT":       ["CN", "STATECD", "COUNTYCD", "INVYR", "LAT", "LON", "PLOT_STATUS_CD"],
            "TREE":       ["PLT_CN", "SPCD", "DIA", "STATUSCD", "TPA_UNADJ", "CCLCD", "CONDID"],
            "REF_SPECIES":["SPCD", "COMMON_NAME", "GENUS", "SPECIES", "SFTWD_HRDWD", "WOODLAND"],
        },
        "sql": (
            "SELECT\n"
            "  p.STATECD, p.COUNTYCD, p.INVYR, p.CN AS PLT_CN,\n"
            "  t.SPCD, s.COMMON_NAME, s.SFTWD_HRDWD,\n"
            "  SUM(0.005454 * t.DIA * t.DIA * t.TPA_UNADJ) AS ba_per_acre\n"
            "FROM PLOT p\n"
            "  JOIN TREE t ON p.CN = t.PLT_CN\n"
            "  JOIN REF_SPECIES s ON t.SPCD = s.SPCD\n"
            "WHERE p.PLOT_STATUS_CD = 1\n"
            "  AND t.STATUSCD = 1\n"
            "  AND t.DIA >= 1.0\n"
            "  AND t.TPA_UNADJ > 0\n"
            "GROUP BY p.STATECD, p.COUNTYCD, p.INVYR, p.CN, t.SPCD, s.COMMON_NAME, s.SFTWD_HRDWD\n"
            "ORDER BY p.STATECD, p.INVYR, ba_per_acre DESC"
        ),
    },
    {
        "name": "Tree mortality by cause code",
        "goal": "Which disturbance agents (bark beetles, fire, disease) are killing trees and at what per-acre rate",
        "tables": ["PLOT", "TREE", "TREE_GRM_COMPONENT", "REF_SPECIES"],
        "steps": [
            "PLOT: filter STATECD, INVYR range, PLOT_STATUS_CD = 1",
            "JOIN TREE ON PLOT.CN = TREE.PLT_CN — get SPCD, AGENTCD (mortality cause code)",
            "JOIN TREE_GRM_COMPONENT ON TREE.CN = TREE_GRM_COMPONENT.TRE_CN",
            "Filter MICR_COMPONENT_AL_FOREST IN ('MORTALITY1','MORTALITY2') for natural mortality",
            "Filter MICR_TPAMORT_UNADJ_AL_FOREST > 0",
            "JOIN REF_SPECIES ON TREE.SPCD = REF_SPECIES.SPCD",
            "GROUP BY INVYR, AGENTCD, SPCD — SUM(MICR_TPAMORT_UNADJ_AL_FOREST)",
            "Note: TREE_GRM_COMPONENT has no INVYR — TREE.INVYR is the T2 (end) year of the period",
        ],
        "key_columns": {
            "TREE":               ["CN", "PLT_CN", "SPCD", "AGENTCD", "INVYR", "STATUSCD"],
            "TREE_GRM_COMPONENT": ["TRE_CN", "MICR_COMPONENT_AL_FOREST", "MICR_TPAMORT_UNADJ_AL_FOREST"],
            "REF_SPECIES":        ["SPCD", "COMMON_NAME", "SFTWD_HRDWD"],
        },
        "sql": (
            "SELECT\n"
            "  p.STATECD, t.INVYR, t.SPCD, s.COMMON_NAME,\n"
            "  t.AGENTCD, g.MICR_COMPONENT_AL_FOREST,\n"
            "  SUM(g.MICR_TPAMORT_UNADJ_AL_FOREST) AS tpa_mortality_per_acre\n"
            "FROM PLOT p\n"
            "  JOIN TREE t ON p.CN = t.PLT_CN\n"
            "  JOIN TREE_GRM_COMPONENT g ON t.CN = g.TRE_CN\n"
            "  JOIN REF_SPECIES s ON t.SPCD = s.SPCD\n"
            "WHERE p.PLOT_STATUS_CD = 1\n"
            "  AND g.MICR_COMPONENT_AL_FOREST IN ('MORTALITY1','MORTALITY2')\n"
            "  AND g.MICR_TPAMORT_UNADJ_AL_FOREST > 0\n"
            "GROUP BY p.STATECD, t.INVYR, t.SPCD, s.COMMON_NAME, t.AGENTCD, g.MICR_COMPONENT_AL_FOREST\n"
            "ORDER BY tpa_mortality_per_acre DESC"
        ),
    },
    {
        "name": "Seedling regeneration by species",
        "goal": "Which species are recruiting — key signal for post-disturbance recovery and thermophilization",
        "tables": ["PLOT", "SEEDLING", "REF_SPECIES"],
        "steps": [
            "PLOT: filter STATECD, INVYR range, PLOT_STATUS_CD = 1",
            "JOIN SEEDLING ON PLOT.CN = SEEDLING.PLT_CN",
            "Filter TREECOUNT > 0 (non-zero seedling counts)",
            "JOIN REF_SPECIES ON SEEDLING.SPCD = REF_SPECIES.SPCD",
            "TREECOUNT is raw count on 1/300-acre microplot → multiply × 300 for per-acre estimate",
            "GROUP BY PLT_CN, INVYR, SPCD — SUM(TREECOUNT)",
        ],
        "key_columns": {
            "SEEDLING":   ["PLT_CN", "INVYR", "SPCD", "CONDID", "SUBP", "TREECOUNT"],
            "REF_SPECIES":["SPCD", "COMMON_NAME", "SFTWD_HRDWD"],
        },
        "sql": (
            "SELECT\n"
            "  p.STATECD, p.INVYR, p.CN AS PLT_CN,\n"
            "  sd.SPCD, s.COMMON_NAME, s.SFTWD_HRDWD,\n"
            "  SUM(sd.TREECOUNT)         AS seedling_count_raw,\n"
            "  SUM(sd.TREECOUNT) * 300.0 AS seedlings_per_acre\n"
            "FROM PLOT p\n"
            "  JOIN SEEDLING sd ON p.CN = sd.PLT_CN\n"
            "  JOIN REF_SPECIES s ON sd.SPCD = s.SPCD\n"
            "WHERE p.PLOT_STATUS_CD = 1\n"
            "  AND sd.TREECOUNT > 0\n"
            "GROUP BY p.STATECD, p.INVYR, p.CN, sd.SPCD, s.COMMON_NAME, s.SFTWD_HRDWD\n"
            "ORDER BY p.STATECD, p.INVYR, seedlings_per_acre DESC"
        ),
    },
    {
        "name": "Forest type composition by county",
        "goal": "Track dominant forest types at county level across inventory years — coarse spatial trend",
        "tables": ["PLOT", "COND", "REF_FOREST_TYPE"],
        "steps": [
            "PLOT: get STATECD, COUNTYCD, INVYR, CN",
            "JOIN COND ON PLOT.CN = COND.PLT_CN",
            "Filter COND_STATUS_CD = 1 (accessible forest land), CONDPROP_UNADJ > 0",
            "JOIN REF_FOREST_TYPE ON COND.FORTYPCD = REF_FOREST_TYPE.FORTYPCD",
            "CONDPROP_UNADJ = proportion of the plot in this condition (sums to 1.0 per plot)",
            "GROUP BY STATECD, COUNTYCD, INVYR, FORTYPCD — COUNT plots, AVG(CONDPROP_UNADJ)",
        ],
        "key_columns": {
            "COND":            ["PLT_CN", "CONDID", "COND_STATUS_CD", "FORTYPCD", "CONDPROP_UNADJ", "OWNGRPCD", "STDSZCD"],
            "REF_FOREST_TYPE": ["FORTYPCD", "MEANING"],
        },
        "sql": (
            "SELECT\n"
            "  p.STATECD, p.COUNTYCD, p.INVYR,\n"
            "  c.FORTYPCD, ft.MEANING AS FOREST_TYPE_NAME,\n"
            "  COUNT(DISTINCT p.CN) AS n_plots,\n"
            "  AVG(c.CONDPROP_UNADJ)  AS mean_cond_prop\n"
            "FROM PLOT p\n"
            "  JOIN COND c ON p.CN = c.PLT_CN\n"
            "  JOIN REF_FOREST_TYPE ft ON c.FORTYPCD = ft.FORTYPCD\n"
            "WHERE p.PLOT_STATUS_CD = 1\n"
            "  AND c.COND_STATUS_CD = 1\n"
            "  AND c.CONDPROP_UNADJ > 0\n"
            "GROUP BY p.STATECD, p.COUNTYCD, p.INVYR, c.FORTYPCD, ft.MEANING\n"
            "ORDER BY p.STATECD, p.COUNTYCD, p.INVYR, n_plots DESC"
        ),
    },
    {
        "name": "Disturbance history by condition",
        "goal": "Find plots affected by specific disturbance agents (insects, fire, disease) and when they occurred",
        "tables": ["PLOT", "COND"],
        "steps": [
            "PLOT: filter STATECD, INVYR range, PLOT_STATUS_CD = 1",
            "JOIN COND ON PLOT.CN = COND.PLT_CN",
            "COND_STATUS_CD = 1 for forest land",
            "DSTRBCD1 codes: 10-12 = insects, 20-22 = disease, 30-32 = fire, 50-54 = weather/drought",
            "DSTRBYR1 = estimated year the disturbance occurred (9999 = ongoing/continuous)",
            "Up to 3 disturbances per condition: DSTRBCD1/YR1, DSTRBCD2/YR2, DSTRBCD3/YR3",
        ],
        "key_columns": {
            "COND": ["PLT_CN", "CONDID", "COND_STATUS_CD", "CONDPROP_UNADJ",
                     "DSTRBCD1", "DSTRBYR1", "DSTRBCD2", "DSTRBYR2", "DSTRBCD3", "DSTRBYR3"],
        },
        "sql": (
            "-- Insect-disturbed plots (DSTRBCD 10-12)\n"
            "SELECT\n"
            "  p.STATECD, p.COUNTYCD, p.INVYR, p.CN AS PLT_CN,\n"
            "  c.CONDID, c.DSTRBCD1, c.DSTRBYR1,\n"
            "  c.DSTRBCD2, c.DSTRBYR2, c.DSTRBCD3, c.DSTRBYR3\n"
            "FROM PLOT p\n"
            "  JOIN COND c ON p.CN = c.PLT_CN\n"
            "WHERE p.PLOT_STATUS_CD = 1\n"
            "  AND c.COND_STATUS_CD = 1\n"
            "  AND (\n"
            "    (c.DSTRBCD1 BETWEEN 10 AND 12)\n"
            "    OR (c.DSTRBCD2 BETWEEN 10 AND 12)\n"
            "    OR (c.DSTRBCD3 BETWEEN 10 AND 12)\n"
            "  )\n"
            "ORDER BY p.STATECD, p.INVYR"
        ),
    },
    {
        "name": "Plot-level diversity index (Shannon H)",
        "goal": "BA-weighted Shannon diversity — requires aggregating tree data per plot",
        "tables": ["PLOT", "TREE", "REF_SPECIES"],
        "steps": [
            "PLOT: filter STATECD, INVYR range, PLOT_STATUS_CD = 1",
            "JOIN TREE ON PLOT.CN = TREE.PLT_CN — filter STATUSCD=1, DIA≥1.0, TPA_UNADJ>0",
            "Compute ba_per_acre = 0.005454 × DIA² × TPA_UNADJ for each tree record",
            "Aggregate: total_ba_plot = SUM(ba_per_acre) per PLT_CN",
            "Aggregate: species_ba = SUM(ba_per_acre) per PLT_CN per SPCD",
            "Compute p_i = species_ba / total_ba_plot (species proportion)",
            "Shannon H = -SUM(p_i × ln(p_i)) — compute in application layer (not pure SQL)",
            "Note: Arrow/R cannot compute log() in lazy evaluation; collect first then compute H",
        ],
        "key_columns": {
            "PLOT": ["CN", "STATECD", "COUNTYCD", "INVYR"],
            "TREE": ["PLT_CN", "SPCD", "DIA", "STATUSCD", "TPA_UNADJ"],
        },
        "sql": (
            "-- Step 1: species BA per plot\n"
            "WITH species_ba AS (\n"
            "  SELECT\n"
            "    t.PLT_CN, t.SPCD,\n"
            "    SUM(0.005454 * t.DIA * t.DIA * t.TPA_UNADJ) AS ba_sp\n"
            "  FROM TREE t\n"
            "  WHERE t.STATUSCD = 1 AND t.DIA >= 1.0 AND t.TPA_UNADJ > 0\n"
            "  GROUP BY t.PLT_CN, t.SPCD\n"
            "),\n"
            "plot_ba AS (\n"
            "  SELECT PLT_CN, SUM(ba_sp) AS ba_total FROM species_ba GROUP BY PLT_CN\n"
            ")\n"
            "-- Step 2: proportions (Shannon H computed in application)\n"
            "SELECT\n"
            "  p.STATECD, p.INVYR, s.PLT_CN, s.SPCD,\n"
            "  s.ba_sp, pb.ba_total,\n"
            "  s.ba_sp / pb.ba_total AS p_i\n"
            "FROM species_ba s\n"
            "  JOIN plot_ba pb ON s.PLT_CN = pb.PLT_CN\n"
            "  JOIN PLOT p ON s.PLT_CN = p.CN\n"
            "WHERE p.PLOT_STATUS_CD = 1\n"
            "ORDER BY s.PLT_CN, p_i DESC"
        ),
    },
]


# =============================================================================
# SCHEMA HELPERS
# =============================================================================

def get_tables(conn: sqlite3.Connection) -> List[str]:
    cur = conn.execute("SELECT name FROM sqlite_master WHERE type='table'")
    return sorted(r[0] for r in cur.fetchall() if not r[0].startswith("sqlite_"))


def get_columns(conn: sqlite3.Connection, table: str) -> pd.DataFrame:
    cur = conn.execute(f"PRAGMA table_info({table})")
    rows = cur.fetchall()
    if not rows:
        return pd.DataFrame()
    df = pd.DataFrame(rows, columns=["cid", "name", "type", "notnull", "dflt_value", "pk"])
    df["Key"]      = df["pk"].apply(lambda x: "PK" if x else "")
    df["Not Null"] = df["notnull"].apply(lambda x: "✓" if x else "")
    df["Codebook"] = df["name"].apply(lambda n: "✓" if n in FIELD_CODEBOOK else "")
    return df[["name", "type", "Key", "Not Null", "Codebook"]].rename(columns={"name": "Column", "type": "Type"})


def fast_row_count(conn: sqlite3.Connection, table: str) -> Optional[int]:
    """Estimate row count from sqlite_stat1 if available (no full-table scan)."""
    try:
        r = conn.execute(
            "SELECT stat FROM sqlite_stat1 WHERE tbl=? AND idx IS NULL", (table,)
        ).fetchone()
        if r:
            return int(r[0].split()[0])
    except Exception:
        pass
    return None


# =============================================================================
# GRAPH BUILDERS
# =============================================================================

CATEGORY_COLORS: Dict[str, str] = {
    "Location Level":                  "#4e79a7",
    "Tree Level":                      "#59a14f",
    "Invasive / Understory Vegetation":"#9c755f",
    "Down Woody Material":             "#e15759",
    "NRS Tree Regeneration":           "#76b7b2",
    "Ground Cover (PNWRS)":            "#edc948",
    "Soils (PNWRS)":                   "#b07aa1",
    "Population":                      "#ff9da7",
    "Plot Geometry / Snapshot":        "#bab0ac",
    "Reference":                       "#f28e2b",
}

_TABLE_TO_CAT: Dict[str, str] = {
    t: cat for cat, tables in TABLE_CATEGORIES.items() for t in tables
}


def build_pyvis_graph(db_tables: List[str]) -> str:
    net = Network(height="650px", width="100%", directed=True, notebook=False)
    net.set_options("""{
      "physics": {
        "barnesHut": {"gravitationalConstant": -8000, "springLength": 120},
        "stabilization": {"iterations": 250}
      },
      "edges": {
        "arrows": {"to": {"enabled": true, "scaleFactor": 0.5}},
        "color": {"color": "#999"},
        "font": {"size": 9, "align": "top"}
      },
      "nodes": {"font": {"size": 12}, "borderWidth": 1.5},
      "interaction": {"hover": true, "tooltipDelay": 200}
    }""")

    present = set(db_tables) if db_tables else set(_TABLE_TO_CAT.keys())
    nodes_added: set = set()

    for child, child_fk, parent, parent_pk, label in RELATIONSHIPS:
        for t in (child, parent):
            if t in present and t not in nodes_added:
                cat   = _TABLE_TO_CAT.get(t, "Unknown")
                color = CATEGORY_COLORS.get(cat, "#cccccc")
                size  = 25 if t in ("PLOT", "TREE", "COND") else 16
                title = f"<b>{t}</b><br>{TABLE_DESCRIPTIONS.get(t, '')[:200]}"
                net.add_node(t, label=t, color=color, size=size, title=title)
                nodes_added.add(t)

    for child, child_fk, parent, parent_pk, label in RELATIONSHIPS:
        if child in nodes_added and parent in nodes_added:
            net.add_edge(child, parent, label=label,
                         title=f"{child}.{child_fk} → {parent}.{parent_pk}")

    return net.generate_html()


def build_graphviz_graph(db_tables: List[str]) -> "Digraph":
    """Static fallback — shows only core analytical tables."""
    CORE = {"PLOT", "COND", "TREE", "SEEDLING", "TREE_GRM_COMPONENT",
            "REF_SPECIES", "REF_FOREST_TYPE", "SURVEY", "COUNTY"}
    present = set(db_tables) if db_tables else CORE
    show = CORE & present

    dot = Digraph(comment="FIA Core Tables", format="png")
    dot.attr(rankdir="TB", fontname="Helvetica")
    dot.attr("node", shape="box", style="filled", fontname="Helvetica", fontcolor="white")

    for t in sorted(show):
        cat   = _TABLE_TO_CAT.get(t, "")
        color = CATEGORY_COLORS.get(cat, "#888888")
        dot.node(t, t, fillcolor=color)

    for child, child_fk, parent, parent_pk, label in RELATIONSHIPS:
        if child in show and parent in show:
            dot.edge(child, parent, label=label)

    return dot


# =============================================================================
# MAIN
# =============================================================================

def main() -> None:
    st.set_page_config(page_title="FIA Schema Navigator", layout="wide")
    st.title("FIA Database Schema Navigator")
    st.caption(
        "FIADB v9.4 · August 2025 · Schema browsing via PRAGMA — zero bulk data loading"
    )

    # ── Sidebar ─────────────────────────────────────────────────────────────
    with st.sidebar:
        st.header("Database")
        db_path = st.text_input(
            "Path to FIADB SQLite file",
            placeholder="/path/to/FIADB_NATIONAL.db",
            help="Full absolute path. Schema metadata is read instantly via PRAGMA.",
        )
        st.markdown("---")
        st.markdown("**Color key**")
        for cat, color in CATEGORY_COLORS.items():
            st.markdown(
                f'<span style="background:{color};color:white;padding:2px 8px;'
                f'border-radius:3px;display:inline-block;margin:2px;font-size:0.8em">'
                f'{cat}</span>',
                unsafe_allow_html=True,
            )

    # ── Database connection ──────────────────────────────────────────────────
    conn: Optional[sqlite3.Connection] = None
    db_tables: List[str] = []

    if db_path and os.path.isfile(db_path):
        try:
            conn = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
            db_tables = get_tables(conn)
            st.sidebar.success(f"{len(db_tables)} tables found")
        except Exception as e:
            st.sidebar.error(f"Cannot open: {e}")
    elif db_path:
        st.sidebar.warning("File not found — showing static metadata only")

    tabs = st.tabs([
        "Overview",
        "Schema Browser",
        "Relationship Map",
        "Column Search",
        "Analysis Pipelines",
    ])

    # ────────────────────────────────────────────────────────────────────────
    # TAB 1 · OVERVIEW
    # ────────────────────────────────────────────────────────────────────────
    with tabs[0]:
        st.header("Database Overview")

        c1, c2, c3 = st.columns(3)
        total_spec = sum(len(v) for v in TABLE_CATEGORIES.values())
        c1.metric("Tables in v9.4 spec", total_spec)
        if db_tables:
            c2.metric("Tables in your DB", len(db_tables))
            c3.metric("Tables recognised", len(set(db_tables) & set(_TABLE_TO_CAT)))

        st.markdown("---")
        st.subheader("Table groups")
        for cat, tables in TABLE_CATEGORIES.items():
            in_db = [t for t in tables if t in db_tables] if db_tables else []
            badge = (
                f"  &nbsp;({len(in_db)}/{len(tables)} in DB)" if db_tables
                else f"  &nbsp;({len(tables)} tables)"
            )
            color = CATEGORY_COLORS.get(cat, "#888")
            header_html = (
                f'<span style="background:{color};color:white;padding:2px 8px;'
                f'border-radius:3px;font-size:0.9em">{cat}</span>{badge}'
            )
            with st.expander(cat + badge):
                for t in tables:
                    prefix = "✓ " if t in db_tables else "   "
                    desc   = TABLE_DESCRIPTIONS.get(t, "")
                    st.markdown(
                        f"`{prefix}{t}` — {desc[:130]}{'…' if len(desc) > 130 else ''}"
                    )

        st.markdown("---")
        st.subheader("Key design rules")
        st.markdown("""
**Primary key:** Every table has a `CN` column (sequence number) as its primary key.

**Foreign key naming:** FK columns follow `{TABLEABBREV}_CN` (e.g. `PLT_CN`, `TRE_CN`).

**PLOT is the hub.** Almost every observation table links to PLOT via `PLT_CN`.

**Multiple conditions per plot.** `CONDID` distinguishes forest conditions on the same plot
where land cover, owner class, forest type, or stand size differs.
`CONDPROP_UNADJ` (sums to 1.0 per plot) gives the proportion of the plot in each condition.

**Longitudinal tracking.** The physical plot location is identified by
`STATECD + UNITCD + COUNTYCD + PLOT`. `INVYR` tracks each visit. `PREV_PLT_CN` links
a PLOT record back to the previous visit.

**Fuzzed coordinates.** LAT/LON are displaced up to ~1 mile. `STATECD` / `COUNTYCD`
are exact and reliable for county-level spatial joins.

**Reference tables (REF_ prefix)** decode all coded fields — always join
`REF_SPECIES` for species names and `REF_FOREST_TYPE` for forest type names.

**GRM tables** (TREE_GRM_*) capture what happened *between* visits.
`TREE_GRM_COMPONENT.TRE_CN` links to `TREE.CN` of the T2 (ending) record.
        """)

    # ────────────────────────────────────────────────────────────────────────
    # TAB 2 · SCHEMA BROWSER
    # ────────────────────────────────────────────────────────────────────────
    with tabs[1]:
        st.header("Schema Browser")

        all_known = [t for cat in TABLE_CATEGORIES.values() for t in cat]
        choices   = db_tables if db_tables else all_known
        selected  = st.selectbox("Select a table", choices)

        if selected:
            cat  = _TABLE_TO_CAT.get(selected, "Unknown")
            desc = TABLE_DESCRIPTIONS.get(selected, "No description available.")

            col_badge, col_desc = st.columns([1, 4])
            color = CATEGORY_COLORS.get(cat, "#888")
            col_badge.markdown(
                f'<span style="background:{color};color:white;padding:4px 10px;'
                f'border-radius:4px">{cat}</span>',
                unsafe_allow_html=True,
            )
            col_desc.info(desc)

            # Column list
            if conn:
                col_df = get_columns(conn, selected)
                if not col_df.empty:
                    st.subheader("Columns")
                    st.dataframe(col_df, use_container_width=True, hide_index=True)
                    st.caption(f"{len(col_df)} columns  ·  ✓ in Codebook = annotated coded field")
                else:
                    st.warning(f"`{selected}` not present in the connected database.")
            else:
                st.info("Connect a database file (sidebar) to see column details.")

            # Codebook for this table's coded columns
            if conn and not (col_df := get_columns(conn, selected)).empty:
                coded_cols = [c for c in col_df["Column"] if c in FIELD_CODEBOOK]
                if coded_cols:
                    st.subheader("Coded columns reference")
                    for col in coded_cols:
                        cb = FIELD_CODEBOOK[col]
                        with st.expander(f"`{col}` — {cb['description']}"):
                            st.dataframe(
                                pd.DataFrame([{"Code": k, "Meaning": v} for k, v in cb["codes"].items()]),
                                use_container_width=True,
                                hide_index=True,
                            )

            # Relationships
            outgoing = [(c, ck, p, pk, lb) for c, ck, p, pk, lb in RELATIONSHIPS if c == selected]
            incoming = [(c, ck, p, pk, lb) for c, ck, p, pk, lb in RELATIONSHIPS if p == selected]
            if outgoing or incoming:
                st.subheader("Relationships")
                if outgoing:
                    st.markdown("**References (FK out) →**")
                    for c, ck, p, pk, lb in outgoing:
                        st.markdown(f"- `{c}.{ck}` → `{p}.{pk}`")
                if incoming:
                    st.markdown("**Referenced by (FK in) ←**")
                    for c, ck, p, pk, lb in incoming:
                        st.markdown(f"- `{c}.{ck}` → `{selected}.{pk}`")

            # Row count + peek
            if conn:
                est = fast_row_count(conn, selected)
                if est:
                    st.caption(f"Estimated row count (from sqlite_stat1): {est:,}")
                if st.button(f"Load 5 sample rows from {selected}", key="peek"):
                    try:
                        df_peek = pd.read_sql_query(f"SELECT * FROM {selected} LIMIT 5", conn)
                        st.dataframe(df_peek, use_container_width=True)
                    except Exception as e:
                        st.error(str(e))

    # ────────────────────────────────────────────────────────────────────────
    # TAB 3 · RELATIONSHIP MAP
    # ────────────────────────────────────────────────────────────────────────
    with tabs[2]:
        st.header("Relationship Map")
        st.caption(
            "Nodes = tables · Edges = foreign key links · Colors = table category · "
            "Hover a node for its description · Drag to rearrange"
        )

        if _PYVIS_AVAILABLE:
            html = build_pyvis_graph(db_tables)
            components.html(html, height=680, scrolling=False)
        elif _GRAPHVIZ_AVAILABLE:
            st.info(
                "pyvis not installed — showing a static core-table diagram. "
                "Run `pip install pyvis` for the full interactive map."
            )
            dot = build_graphviz_graph(db_tables)
            st.graphviz_chart(dot)
        else:
            st.warning(
                "Install pyvis (`pip install pyvis`) for the interactive map, "
                "or graphviz (`pip install graphviz`) for a static diagram."
            )

        st.markdown("---")
        st.subheader("All declared relationships")
        rel_df = pd.DataFrame(
            [(c, ck, p, pk) for c, ck, p, pk, lb in RELATIONSHIPS],
            columns=["Child table", "FK column", "Parent table", "PK column"],
        )
        st.dataframe(rel_df, use_container_width=True, hide_index=True)

    # ────────────────────────────────────────────────────────────────────────
    # TAB 4 · COLUMN SEARCH
    # ────────────────────────────────────────────────────────────────────────
    with tabs[3]:
        st.header("Column Search")
        st.caption("Find which tables contain a given column — useful for tracing join paths")

        query = st.text_input("Column name (partial match OK)", placeholder="e.g. SPCD, PLT_CN, AGENTCD")

        if query and conn:
            results = []
            for table in db_tables:
                try:
                    cur = conn.execute(f"PRAGMA table_info({table})")
                    for row in cur.fetchall():
                        if query.upper() in row[1].upper():
                            results.append({"Table": table, "Column": row[1], "Type": row[2]})
                except Exception:
                    pass
            if results:
                res_df = pd.DataFrame(results)
                st.dataframe(res_df, use_container_width=True, hide_index=True)
                st.caption(f"{len(results)} matches across {res_df['Table'].nunique()} tables")
            else:
                st.info("No matches found.")
        elif query:
            st.info("Connect a database file (sidebar) to search columns.")

        st.markdown("---")
        st.subheader("Universal join keys")
        st.markdown("""
| Column | Role | Key tables |
|--------|------|------------|
| `CN` | Primary key (sequence number) | **Every** table |
| `PLT_CN` | → PLOT.CN — links child records to the plot | COND, TREE, SEEDLING, SUBPLOT, DWM_*, POP_PLOT_STRATUM_ASSGN, … |
| `TRE_CN` | → TREE.CN — links GRM/woodland records to the tree | TREE_GRM_COMPONENT, TREE_WOODLAND_STEMS, TREE_GRM_* |
| `SPCD` | FIA species code | TREE, SEEDLING, REF_SPECIES, SITETREE |
| `FORTYPCD` | Forest type code | COND, REF_FOREST_TYPE |
| `CONDID` | Condition number within a plot (1, 2, 3…) | COND, TREE, SEEDLING, SUBPLOT, SUBP_COND |
| `STATECD` | FIPS state code | PLOT, COND, TREE, SURVEY, COUNTY, … |
| `INVYR` | Inventory year (visit year) | PLOT, COND, TREE, SEEDLING, SURVEY, … |
| `EVAL_CN` | → POP_EVAL.CN | POP_ESTN_UNIT, POP_EVAL_ATTRIBUTE, POP_EVAL_TYP |
| `STRATUM_CN` | → POP_STRATUM.CN | POP_PLOT_STRATUM_ASSGN |
| `SRV_CN` | → SURVEY.CN | PLOT |
| `CTY_CN` | → COUNTY.CN | PLOT |
| `PRJ_CN` | → PROJECT.CN | SURVEY |
        """)

    # ────────────────────────────────────────────────────────────────────────
    # TAB 5 · ANALYSIS PIPELINES
    # ────────────────────────────────────────────────────────────────────────
    with tabs[4]:
        st.header("Analysis Pipelines")
        st.caption("Common analysis goals with table-join paths and ready-to-run SQL")

        for tmpl in PIPELINE_TEMPLATES:
            with st.expander(f"**{tmpl['name']}**  —  *{tmpl['goal']}*"):
                st.markdown(
                    "**Tables:** " + " &rarr; ".join(f"`{t}`" for t in tmpl["tables"])
                )
                st.markdown("**Steps:**")
                for i, step in enumerate(tmpl["steps"], 1):
                    st.markdown(f"{i}. {step}")

                if "key_columns" in tmpl:
                    st.markdown("**Key columns per table:**")
                    for tbl, cols in tmpl["key_columns"].items():
                        st.markdown(f"- `{tbl}`: {', '.join(f'`{c}`' for c in cols)}")

                st.markdown("**Sample SQL:**")
                st.code(tmpl["sql"], language="sql")

        st.markdown("---")
        st.subheader("Field codebook")
        st.caption("All coded fields used in analyses — with code → meaning lookup")
        for field, cb in FIELD_CODEBOOK.items():
            with st.expander(f"`{field}` — {cb['description']}"):
                st.dataframe(
                    pd.DataFrame([{"Code": k, "Meaning": v} for k, v in cb["codes"].items()]),
                    use_container_width=True,
                    hide_index=True,
                )


if __name__ == "__main__":
    main()
