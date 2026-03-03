"""
FIA Database Schema Navigator
==============================
Streamlit app for navigating the Forest Inventory and Analysis Database (FIADB v9.4).

Metadata-first design: all schema navigation runs off PRAGMA table_info() calls,
which are instantaneous on a 70 GB SQLite file. No bulk row loading unless the
user explicitly requests a preview.

Usage:
  streamlit run fiadb_dashboard.py

  Local use: optionally paste the full path to your FIADB SQLite file in the sidebar.
  Hosted use (e.g., Streamlit Cloud): set FIADB_DB_PATH on the server and the app
  will connect automatically without showing a public path textbox.

Requires:
  pip install streamlit pandas pyvis   # pyvis recommended for interactive graph
  pip install graphviz                  # fallback static diagram

Source: FIADB User Guide v9.4, August 2025
"""

import json
import os
import re
import sqlite3
from collections import defaultdict
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

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

CATEGORY_DESCRIPTIONS: Dict[str, str] = {
    "Location Level": (
        "The core spatial hierarchy of the FIA sampling design. Every measurement starts here. "
        "SURVEY groups states by inventory year. PLOT is the physical sample location visited by "
        "field crews on a roughly 5-year cycle. COND (condition) records the land cover state of "
        "each portion of the plot — a single plot can straddle a forest edge and have two or more "
        "conditions with different forest types, owner classes, or stand sizes."
    ),
    "Tree Level": (
        "All individual-tree records. TREE contains every stem tallied at ≥1.0\" diameter on the "
        "subplot. SEEDLING tracks young trees (<1\" DBH) counted on the smaller microplot. "
        "The TREE_GRM_* tables (Growth/Removal/Mortality) record what happened to each tree "
        "between visits — whether it survived, grew into the tally, was harvested, or died "
        "naturally — along with per-acre expansion factors for each outcome."
    ),
    "Invasive / Understory Vegetation": (
        "Records of non-tree plant species observed on a subset of FIA plots. "
        "INVASIVE_SUBPLOT_SPP captures presence and cover class of invasive plants by subplot. "
        "The P2VEG tables record general understory and shrub-layer cover estimates. "
        "These are Phase 2 protocols collected on intensified plots, not every plot."
    ),
    "Down Woody Material": (
        "Measurements of dead wood on the forest floor — coarse logs, fine branches, duff, "
        "and litter. Collected via line-intercept transects and fixed-area plots. Used for "
        "carbon accounting, wildfire fuel load estimation, and coarse woody debris habitat "
        "assessment. The DWM_VISIT table records the transect layout; other DWM_ tables "
        "record the actual material by type."
    ),
    "NRS Tree Regeneration": (
        "Enhanced regeneration monitoring tables collected by the Northern Research Station (NRS). "
        "Provides additional detail on seedling and sapling recruitment beyond what the standard "
        "SEEDLING table captures — useful for studying post-disturbance forest recovery in "
        "the northeastern and north-central U.S."
    ),
    "Ground Cover (PNWRS)": (
        "Ground-layer vegetation and cover type data collected by the Pacific Northwest Research "
        "Station (PNWRS). Includes percent cover by functional group (mosses, lichens, forbs, "
        "grasses) and microquadrat-level detail. Relevant for studies of understory composition "
        "and carbon in Pacific Northwest forests."
    ),
    "Soils (PNWRS)": (
        "Soil sample data collected by the Pacific Northwest Research Station. Records sample "
        "location, depth horizon, bulk density, texture, and carbon content. These tables are "
        "key inputs for below-ground carbon stock estimation and soil health assessment in "
        "PNWRS-region forests."
    ),
    "Population": (
        "Statistical design tables used to scale plot-level measurements up to area-wide "
        "(population-level) estimates — for example, the total forested acres in a state or "
        "the total volume of live timber. Most analysts working with raw plot data do not need "
        "these tables. They are required for EVALIDator-style estimates that account for the "
        "stratified sampling design and adjustment factors."
    ),
    "Plot Geometry / Snapshot": (
        "PLOTGEOM stores higher-precision, non-fuzzed plot coordinates available to authorized "
        "researchers (access may be restricted). PLOTSNAP is a pre-computed summary of "
        "plot-level attributes used in population estimation workflows — essentially a cached "
        "version of key PLOT fields aligned to specific evaluation periods."
    ),
    "Reference": (
        "Lookup and decode tables for coded fields throughout the database. These tables translate "
        "numeric codes into meaningful labels. Always join REF_SPECIES (on SPCD) to get species "
        "common names, genus, and softwood/hardwood designation. Join REF_FOREST_TYPE (on FORTYPCD) "
        "to decode forest type names. Other REF_ tables decode damage agents, owner groups, "
        "vegetation classifications (NVCS), FVS model variants, and more."
    ),
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
# USER GUIDE (PDF INDEX CACHE) HELPERS
# =============================================================================

USER_GUIDE_INDEX_JSON = Path(__file__).with_name("fiadb_user_guide_index_v94.json")
_JOIN_EXPR_RE = re.compile(r"([A-Z0-9_]+\.[A-Z0-9_]+)\s*=\s*([A-Z0-9_]+\.[A-Z0-9_]+)")

VARIABLE_FAMILY_ORDER = [
    "Identifiers / Join keys",
    "Time / Dates",
    "Geography / Location",
    "Species / Taxonomy",
    "Codes / Classifications",
    "Sampling / Estimation",
    "Measurements / Counts",
    "Derived metrics / Carbon / Volume",
    "Provenance / Audit / Metadata",
    "Text / Labels",
    "Other",
]

VARIABLE_FAMILY_DESCRIPTIONS: Dict[str, str] = {
    "Identifiers / Join keys": "Primary keys, foreign keys, sequence IDs, and row-linking fields.",
    "Time / Dates": "Inventory years, measurement dates, disturbance years, and longitudinal timing fields.",
    "Geography / Location": "State/county/unit codes, coordinates, aspect/slope/azimuth, and plot location descriptors.",
    "Species / Taxonomy": "Species codes and names, taxonomic groupings, and vegetation identifiers.",
    "Codes / Classifications": "Categorical code fields (often ending in _CD or CD) that require a lookup/codebook.",
    "Sampling / Estimation": "Evaluation, stratum, expansion, and adjustment factor fields used in population estimation.",
    "Measurements / Counts": "Raw observed dimensions, counts, percentages, proportions, and sampled quantities.",
    "Derived metrics / Carbon / Volume": "Calculated or modeled values (growth, volume, biomass, carbon, basal area, rates).",
    "Provenance / Audit / Metadata": "Created/modified/version/source/active fields and process metadata.",
    "Text / Labels": "Human-readable names, meanings, descriptions, citations, and free-text labels.",
    "Other": "Fields that do not fit cleanly in the categories above.",
}

CONCEPT_LOCATORS: List[Dict[str, Any]] = [
    {
        "name": "Plot identity and revisit chain",
        "what": "Find the physical plot ID, visit year, and links to previous visits of the same location.",
        "tables": ["PLOT", "SURVEY", "COUNTY"],
        "columns": ["CN", "STATECD", "UNITCD", "COUNTYCD", "PLOT", "INVYR", "PREV_PLT_CN", "SRV_CN", "CTY_CN"],
        "notes": "PLOT is the hub. The physical location is tracked by STATECD+UNITCD+COUNTYCD+PLOT across INVYR.",
    },
    {
        "name": "Forest condition classification (per plot portions)",
        "what": "Find condition-level forest type, ownership, stand size, and area proportion fields.",
        "tables": ["COND", "SUBP_COND", "BOUNDARY", "REF_FOREST_TYPE", "REF_OWNGRPCD"],
        "columns": ["PLT_CN", "CONDID", "COND_STATUS_CD", "FORTYPCD", "OWNGRPCD", "STDSZCD", "CONDPROP_UNADJ"],
        "notes": "A single plot may have multiple COND records. CONDPROP_UNADJ proportions sum to 1.0 by plot.",
    },
    {
        "name": "Live tree measurements (species, diameter, status)",
        "what": "Find individual-tree observations and the fields needed for most plot-level tree analyses.",
        "tables": ["TREE", "REF_SPECIES", "COND"],
        "columns": ["PLT_CN", "CONDID", "SUBP", "TREE", "SPCD", "DIA", "HT", "STATUSCD", "TPA_UNADJ", "CCLCD"],
        "notes": "Join TREE.SPCD -> REF_SPECIES.SPCD for names. Filter STATUSCD=1 for live trees in many analyses.",
    },
    {
        "name": "Tree mortality / growth / removals between visits",
        "what": "Find GRM component and estimate tables that describe what happened between inventories.",
        "tables": ["TREE_GRM_COMPONENT", "TREE_GRM_ESTN", "TREE_GRM_BEGIN", "TREE", "BEGINEND"],
        "columns": ["TRE_CN", "MICR_COMPONENT_AL_FOREST", "MICR_TPAMORT_UNADJ_AL_FOREST", "ANN_NET_GROWTH", "INVYR"],
        "notes": "GRM tables link through TRE_CN to TREE.CN (generally the T2/end record). TREE supplies INVYR/SPCD.",
    },
    {
        "name": "Seedlings and regeneration",
        "what": "Find seedling counts and enhanced NRS regeneration indicators.",
        "tables": ["SEEDLING", "PLOT_REGEN", "SUBPLOT_REGEN", "SEEDLING_REGEN", "REF_SPECIES"],
        "columns": ["PLT_CN", "CONDID", "SUBP", "SPCD", "TREECOUNT", "BROWSE_IMPACT"],
        "notes": "SEEDLING is the standard national table; NRS regeneration tables add regional detail.",
    },
    {
        "name": "Disturbance and treatment history",
        "what": "Find condition-level disturbance and treatment codes/years and decode where needed.",
        "tables": ["COND", "REF_DAMAGE_AGENT", "REF_DAMAGE_AGENT_GROUP"],
        "columns": ["DSTRBCD1", "DSTRBYR1", "DSTRBCD2", "DSTRBYR2", "DSTRBCD3", "DSTRBYR3", "TRTCD1", "TRTYR1"],
        "notes": "COND stores up to 3 disturbance and 3 treatment events per condition.",
    },
    {
        "name": "Down woody material / fuels / floor carbon",
        "what": "Find dead wood, duff/litter, and transect-based fuels measurements.",
        "tables": [
            "DWM_VISIT", "DWM_COARSE_WOODY_DEBRIS", "DWM_DUFF_LITTER_FUEL",
            "DWM_FINE_WOODY_DEBRIS", "DWM_MICROPLOT_FUEL", "DWM_RESIDUAL_PILE",
            "DWM_TRANSECT_SEGMENT", "COND_DWM_CALC",
        ],
        "columns": ["PLT_CN", "CONDID", "TRANSECT", "CARBON", "CARBON_AC_UNADJ", "DECAYCD"],
        "notes": "Most DWM tables join to PLOT via PLT_CN and many also align to COND via PLT_CN+CONDID.",
    },
    {
        "name": "Population estimation factors (EVALIDator-style workflows)",
        "what": "Find the evaluation/stratum tables and adjustment/expansion factors used in statistical estimation.",
        "tables": [
            "POP_EVAL", "POP_EVAL_GRP", "POP_EVAL_TYP", "POP_ESTN_UNIT",
            "POP_STRATUM", "POP_PLOT_STRATUM_ASSGN", "PLOTSNAP",
        ],
        "columns": ["EVAL_CN", "EVAL_GRP_CN", "STRATUM_CN", "ESTN_UNIT_CN", "EXPNS", "ADJ_FACTOR_SUBP"],
        "notes": "These tables are essential for population estimates but not required for many plot-level analyses.",
    },
    {
        "name": "Reference lookups / code decoding",
        "what": "Find code->label tables for species, forest type, owner groups, damage agents, and more.",
        "tables": [
            "REF_SPECIES", "REF_FOREST_TYPE", "REF_FOREST_TYPE_GROUP", "REF_OWNGRPCD",
            "REF_DAMAGE_AGENT", "REF_DAMAGE_AGENT_GROUP", "REF_GRM_TYPE", "REF_UNIT",
        ],
        "columns": ["SPCD", "FORTYPCD", "TYPGRPCD", "CODE", "VALUE", "MEANING", "COMMON_NAME"],
        "notes": "Reference tables make coded fields readable. Join them early when building exploratory outputs.",
    },
]


def classify_variable_family(column_name: str, descriptive_name: str = "") -> str:
    c = (column_name or "").upper()
    d = (descriptive_name or "").lower()

    if (
        c == "CN" or c.endswith("_CN") or c.endswith("_ID") or c in {
            "PLTID", "CONDID", "SUBP", "TREE", "EVALID", "INVYR", "PLOT"
        }
    ):
        # Keep INVYR and PLOT in other categories if more specific rules apply below.
        if c not in {"INVYR", "PLOT"}:
            return "Identifiers / Join keys"

    if any(tok in c for tok in ["DATE", "_DT", "YEAR", "_YR", "INVYR", "MEASYEAR", "MEASMON", "MEASDAY"]):
        return "Time / Dates"

    if (
        c in {"STATECD", "COUNTYCD", "UNITCD", "LAT", "LON", "AZIMUTH", "ASPECT", "SLOPE", "ELEV", "ELEVATION"}
        or any(tok in c for tok in ["LAT", "LON", "AZIMUTH", "ASPECT", "SLOPE", "ELEV", "COUNTY", "STATE", "UNIT"])
        or any(tok in d for tok in ["county", "state", "latitude", "longitude", "azimuth", "aspect", "slope"])
    ):
        return "Geography / Location"

    if (
        "SPCD" in c or "SPECIES" in c or "GENUS" in c or c in {
            "COMMON_NAME", "SCIENTIFIC_NAME", "SYMBOL", "ITIS_TSN", "PLANTS_CD"
        }
        or "species" in d or "taxon" in d
    ):
        return "Species / Taxonomy"

    if any(tok in c for tok in ["EVAL", "ESTN", "STRATUM", "EXPNS", "ADJ_FACTOR", "EXP_", "ADJ_"]):
        return "Sampling / Estimation"
    if "expansion factor" in d or "adjustment factor" in d or "estimation" in d or "evaluation" in d:
        return "Sampling / Estimation"

    if (
        any(tok in c for tok in ["CARBON", "BIOMASS", "VOLUME", "_VOL", "GROWTH", "MORT", "REMV", "BA", "BASAL"])
        or any(tok in d for tok in ["carbon", "biomass", "volume", "growth", "mortality", "removal", "basal area"])
    ):
        return "Derived metrics / Carbon / Volume"

    if (
        c.endswith("CD") or "_CD" in c or c.endswith("CLASS") or "CLASS" in c or "TYPE" in c or "STATUS" in c
        or any(tok in d for tok in [" code", "classification", "status code", "type code"])
    ):
        return "Codes / Classifications"

    if (
        any(tok in c for tok in [
            "DIA", "HT", "HEIGHT", "COUNT", "TREECOUNT", "PCT", "PROP", "RATIO",
            "AREA", "DENSITY", "LENGTH", "WIDTH", "DEPTH", "WT", "WEIGHT", "TPA",
        ])
        or any(tok in d for tok in [
            "diameter", "height", "count", "percent", "proportion", "area", "density",
            "length", "depth", "weight", "per acre",
        ])
    ):
        return "Measurements / Counts"

    if (
        any(tok in c for tok in ["CREATED", "MODIFIED", "INSTANCE", "VERSION", "ACTIVE", "SOURCE", "CITATION"])
        or any(tok in d for tok in ["created", "modified", "version", "source", "citation"])
    ):
        return "Provenance / Audit / Metadata"

    if (
        any(tok in c for tok in ["NAME", "MEANING", "DESCR", "DESCRIPTION", "ABBR", "AUTHOR", "TITLE"])
        or any(tok in d for tok in ["name", "description", "meaning", "author", "citation"])
    ):
        return "Text / Labels"

    if c in {"CN", "PLOT", "CONDID"}:
        return "Identifiers / Join keys"
    return "Other"


def _parse_num_tuple(value: str) -> Tuple[int, ...]:
    try:
        return tuple(int(x) for x in str(value).split("."))
    except Exception:
        return (999999,)


def _split_guide_description(text: str) -> Tuple[str, List[str]]:
    lines = [ln.strip() for ln in (text or "").splitlines() if ln.strip()]
    intro_parts: List[str] = []
    bullets: List[str] = []
    for ln in lines:
        if ln.startswith("- "):
            bullets.append(ln[2:].strip())
        else:
            intro_parts.append(ln)
    intro = " ".join(intro_parts).strip()
    return intro, bullets


def _extract_join_examples_from_text(text: str) -> List[str]:
    pairs = []
    for left, right in _JOIN_EXPR_RE.findall(text or ""):
        pairs.append(f"{left} = {right}")
    # Keep order, de-duplicate
    seen = set()
    out = []
    for p in pairs:
        if p not in seen:
            seen.add(p)
            out.append(p)
    return out


@st.cache_data(show_spinner=False)
def load_user_guide_index() -> Dict[str, Any]:
    if not USER_GUIDE_INDEX_JSON.exists():
        return {}
    try:
        return json.loads(USER_GUIDE_INDEX_JSON.read_text(encoding="utf-8"))
    except Exception:
        return {}


def prepare_user_guide_lookup(guide_data: Dict[str, Any]) -> Dict[str, Any]:
    if not guide_data:
        return {
            "source": {},
            "summary": {},
            "toc": [],
            "chapters": [],
            "tables_by_name": {},
            "table_rows": [],
            "columns_rows": [],
            "columns_by_table": {},
            "columns_by_name": {},
            "families": [],
        }

    table_sections = guide_data.get("table_sections", {}) or {}
    table_rows: List[Dict[str, Any]] = []
    tables_by_name: Dict[str, Dict[str, Any]] = {}

    for row in (guide_data.get("tables_index", []) or []):
        t = dict(row)
        t.update(table_sections.get(row.get("oracle_table", ""), {}))
        t["table_category"] = _TABLE_TO_CAT.get(t.get("oracle_table", ""), "Unknown")
        intro, bullets = _split_guide_description(t.get("description", ""))
        t["official_summary"] = intro
        t["official_bullets"] = bullets
        t["documented_joins"] = _extract_join_examples_from_text(t.get("description", ""))
        table_rows.append(t)
        if t.get("oracle_table"):
            tables_by_name[t["oracle_table"]] = t

    table_rows.sort(key=lambda r: _parse_num_tuple(r.get("section", "")))

    columns_rows: List[Dict[str, Any]] = []
    columns_by_table: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    columns_by_name: Dict[str, List[Dict[str, Any]]] = defaultdict(list)

    for row in (guide_data.get("columns_index", []) or []):
        r = dict(row)
        r["guide_section"] = ".".join(str(r.get("subsection", "")).split(".")[:2])
        r["table_category"] = _TABLE_TO_CAT.get(r.get("oracle_table", ""), "Unknown")
        r["variable_family"] = classify_variable_family(
            r.get("column_name", ""),
            r.get("descriptive_name", ""),
        )

        table_meta = tables_by_name.get(r.get("oracle_table", ""))
        if table_meta:
            r["guide_start_page"] = table_meta.get("guide_start_page")
            r["guide_end_page"] = table_meta.get("guide_end_page")
            r["table_section"] = table_meta.get("section")
        else:
            r["guide_start_page"] = None
            r["guide_end_page"] = None
            r["table_section"] = r["guide_section"]

        columns_rows.append(r)
        if r.get("oracle_table"):
            columns_by_table[r["oracle_table"]].append(r)
        if r.get("column_name"):
            columns_by_name[r["column_name"]].append(r)

    for table, rows in columns_by_table.items():
        rows.sort(key=lambda r: (_parse_num_tuple(r.get("subsection", "")), r.get("column_name", "")))
    for col, rows in columns_by_name.items():
        rows.sort(key=lambda r: (r.get("oracle_table", ""), _parse_num_tuple(r.get("subsection", ""))))

    toc = guide_data.get("toc", []) or []
    chapters = [r for r in toc if r.get("level") == 1]

    families = sorted(
        {r["variable_family"] for r in columns_rows},
        key=lambda name: VARIABLE_FAMILY_ORDER.index(name) if name in VARIABLE_FAMILY_ORDER else 999,
    )

    return {
        "source": guide_data.get("source", {}) or {},
        "summary": guide_data.get("summary", {}) or {},
        "toc": toc,
        "chapters": chapters,
        "tables_by_name": tables_by_name,
        "table_rows": table_rows,
        "columns_rows": columns_rows,
        "columns_by_table": dict(columns_by_table),
        "columns_by_name": dict(columns_by_name),
        "families": families,
    }


def filter_guide_columns(
    rows: List[Dict[str, Any]],
    *,
    query: str = "",
    search_desc: bool = True,
    exact: bool = False,
    table_filter: Optional[str] = None,
    table_category_filter: Optional[str] = None,
    family_filter: Optional[str] = None,
) -> List[Dict[str, Any]]:
    q = (query or "").strip().upper()
    out = []
    for r in rows:
        if table_filter and table_filter != "All" and r.get("oracle_table") != table_filter:
            continue
        if table_category_filter and table_category_filter != "All" and r.get("table_category") != table_category_filter:
            continue
        if family_filter and family_filter != "All" and r.get("variable_family") != family_filter:
            continue

        if q:
            col = str(r.get("column_name", "")).upper()
            desc = str(r.get("descriptive_name", "")).upper()
            tbl = str(r.get("oracle_table", "")).upper()
            if exact:
                matched = (col == q) or (tbl == q)
                if search_desc:
                    matched = matched or (desc == q)
            else:
                matched = (q in col) or (q in tbl)
                if search_desc:
                    matched = matched or (q in desc)
            if not matched:
                continue
        out.append(r)
    return out


def guide_rows_to_df(rows: List[Dict[str, Any]], include_table: bool = True) -> pd.DataFrame:
    if not rows:
        return pd.DataFrame()
    data = []
    for r in rows:
        row = {
            "Column": r.get("column_name"),
            "Guide name": r.get("descriptive_name"),
            "Family": r.get("variable_family"),
            "Guide subsection": r.get("subsection"),
            "Field Guide": r.get("field_guide_section") or "",
        }
        if include_table:
            row["Table"] = r.get("oracle_table")
            row["Table group"] = r.get("table_category", "Unknown")
            if r.get("guide_start_page"):
                row["Guide pages"] = (
                    f"{r['guide_start_page']}-{r['guide_end_page']}"
                    if r.get("guide_end_page") and r.get("guide_end_page") != r.get("guide_start_page")
                    else str(r["guide_start_page"])
                )
            else:
                row["Guide pages"] = ""
        data.append(row)
    df = pd.DataFrame(data)
    order = [
        "Column", "Guide name", "Family", "Table", "Table group",
        "Guide subsection", "Field Guide", "Guide pages",
    ] if include_table else [
        "Column", "Guide name", "Family", "Guide subsection", "Field Guide"
    ]
    cols = [c for c in order if c in df.columns]
    return df[cols]


# =============================================================================
# ANALYSIS PIPELINE CODE RENDERING HELPERS
# =============================================================================

APP_UI_MODES = ["Newbie", "Expert"]
PIPELINE_CODE_LANGUAGES = ["SQL", "Python", "R"]


def _sql_to_python_string_literal(sql: str) -> str:
    """Return a safe Python string literal for embedding SQL."""
    sql = sql or ""
    # Prefer triple quotes for readability when possible.
    if '"""' not in sql:
        return f'"""{sql}"""'
    # Fallback to JSON escaping (valid Python double-quoted string literal).
    return json.dumps(sql)


def _sql_to_r_string_literal(sql: str) -> str:
    """Return a safe R string literal with escaped characters."""
    sql = sql or ""
    # JSON escaping is compatible with a regular R double-quoted string for our use.
    return json.dumps(sql)


def build_pipeline_sql_code(tmpl: Dict[str, Any]) -> str:
    sql = tmpl.get("sql", "")
    if not isinstance(sql, str):
        return ""
    return sql


def build_pipeline_python_wrapper(sql: str) -> str:
    sql_literal = _sql_to_python_string_literal(sql)
    return (
        "import sqlite3\n"
        "import pandas as pd\n\n"
        'db_path = "/path/to/FIADB.db"\n'
        'conn = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)\n\n'
        f"sql = {sql_literal}\n\n"
        "df = pd.read_sql_query(sql, conn)\n"
        "print(df.head())\n"
    )


def build_pipeline_r_wrapper(sql: str) -> str:
    sql_literal = _sql_to_r_string_literal(sql)
    return (
        "library(DBI)\n"
        "library(RSQLite)\n\n"
        'db_path <- "/path/to/FIADB.db"\n'
        "con <- DBI::dbConnect(RSQLite::SQLite(), dbname = db_path)\n\n"
        f"sql <- {sql_literal}\n\n"
        "df <- DBI::dbGetQuery(con, sql)\n"
        "head(df)\n"
    )


def _pipeline_lang_code_block_name(lang: str) -> str:
    lang_key = (lang or "SQL").strip().upper()
    if lang_key == "PYTHON":
        return "python"
    if lang_key == "R":
        return "r"
    return "sql"


def _get_native_pipeline_example(tmpl: Dict[str, Any], selected_language: str) -> Optional[Dict[str, Any]]:
    examples = tmpl.get("examples")
    if not isinstance(examples, dict):
        return None

    requested = (selected_language or "SQL").strip().upper()
    match = None
    for key, value in examples.items():
        if str(key).strip().upper() == requested:
            match = value
            break
    if match is None:
        return None

    default_block_lang = _pipeline_lang_code_block_name(selected_language)
    default_label = f"Sample {(selected_language or 'SQL').strip() or 'SQL'}"

    if isinstance(match, str):
        return {
            "label": default_label,
            "code": match,
            "code_lang": default_block_lang,
            "source_kind": "native",
            "note": None,
        }

    if isinstance(match, dict):
        code = match.get("code", "")
        if not isinstance(code, str) or not code.strip():
            return None
        return {
            "label": str(match.get("label", default_label)),
            "code": code,
            "code_lang": str(match.get("code_lang", default_block_lang)),
            "source_kind": str(match.get("source_kind", "native")),
            "note": str(match.get("note")) if match.get("note") is not None else None,
        }

    return None


def render_pipeline_code(tmpl: Dict[str, Any], selected_language: str) -> Dict[str, Any]:
    native_example = _get_native_pipeline_example(tmpl, selected_language)
    if native_example:
        return native_example

    sql = build_pipeline_sql_code(tmpl)
    if not sql.strip():
        return {
            "label": "Sample Code",
            "code": "-- No example is available for this pipeline yet.",
            "code_lang": "sql",
            "source_kind": "missing",
            "note": None,
        }

    lang = (selected_language or "SQL").strip().upper()
    if lang == "PYTHON":
        return {
            "label": "Sample Python",
            "code": build_pipeline_python_wrapper(sql),
            "code_lang": "python",
            "source_kind": "wrapper",
            "note": "This Python example is a wrapper around the canonical SQL.",
        }
    if lang == "R":
        return {
            "label": "Sample R",
            "code": build_pipeline_r_wrapper(sql),
            "code_lang": "r",
            "source_kind": "wrapper",
            "note": "This R example is a wrapper around the canonical SQL.",
        }
    return {
        "label": "Sample SQL",
        "code": sql,
        "code_lang": "sql",
        "source_kind": "canonical_sql",
        "note": None,
    }


def _first_sentence(text: str) -> str:
    text = (text or "").strip()
    if not text:
        return ""
    first = re.split(r"(?<=[.!?])\s+", text, maxsplit=1)[0]
    return first.strip()


def describe_table_for_newbie(table_name: str) -> str:
    desc = TABLE_DESCRIPTIONS.get(table_name, "")
    summary = _first_sentence(desc)
    if summary:
        return summary
    return f"FIA table `{table_name}`."


def render_mode_hint_box(
    *,
    is_newbie_mode: bool,
    newbie_title: str,
    newbie_body: str,
    expert_title: str,
    expert_body: str,
) -> None:
    """Compact mode-aware callout used at the top of tabs."""
    if is_newbie_mode:
        color = "#36a36f"
        bg = "#36a36f15"
        title = newbie_title
        body = newbie_body
    else:
        color = "#d18a2b"
        bg = "#d18a2b15"
        title = expert_title
        body = expert_body
    st.markdown(
        (
            f'<div style="margin:6px 0 12px;padding:10px 12px;border-radius:8px;'
            f'border-left:4px solid {color};background:{bg};">'
            f'<div style="color:{color};font-weight:700;margin-bottom:4px;">{title}</div>'
            f'<div style="color:#d9d9d9;line-height:1.35;">{body}</div>'
            f"</div>"
        ),
        unsafe_allow_html=True,
    )



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
    # Reserve 460 px for the graph; the info panel sits below it in the same iframe.
    net = Network(
        height="460px",
        width="100%",
        directed=True,
        notebook=False,
        bgcolor="#0e1117",
        font_color="#ffffff",
    )
    net.set_options("""{
      "physics": {
        "barnesHut": {"gravitationalConstant": -8000, "springLength": 120},
        "stabilization": {"iterations": 250}
      },
      "edges": {
        "arrows": {"to": {"enabled": true, "scaleFactor": 0.5}},
        "color": {"color": "#555"},
        "font": {"size": 9, "align": "top", "color": "#aaa", "strokeWidth": 0}
      },
      "nodes": {
        "font": {"size": 12, "color": "#fff"},
        "borderWidth": 1.5,
        "scaling": {"label": {"enabled": false, "drawThreshold": 3}}
      },
      "interaction": {"hover": true, "tooltipDelay": 100}
    }""")

    # Core hub tables get a larger font so their labels stay readable when
    # the graph is auto-fitted to show all nodes at once.
    _CORE_NODES = {"PLOT", "TREE", "COND"}

    present = set(db_tables) if db_tables else set(_TABLE_TO_CAT.keys())
    nodes_added: set = set()
    table_info: dict = {}

    for child, child_fk, parent, parent_pk, label in RELATIONSHIPS:
        for t in (child, parent):
            if t in present and t not in nodes_added:
                cat   = _TABLE_TO_CAT.get(t, "Unknown")
                color = CATEGORY_COLORS.get(cat, "#cccccc")
                size  = 25 if t in _CORE_NODES else 16
                desc  = TABLE_DESCRIPTIONS.get(t, "No description available.")
                font  = ({"size": 16, "color": "#fff"} if t in _CORE_NODES
                         else {"size": 11, "color": "#fff"})
                net.add_node(t, label=t, color=color, size=size, title=t, font=font)
                table_info[t] = {"category": cat, "description": desc, "color": color}
                nodes_added.add(t)

    for child, child_fk, parent, parent_pk, label in RELATIONSHIPS:
        if child in nodes_added and parent in nodes_added:
            net.add_edge(child, parent, label=label,
                         title=f"{child}.{child_fk} \u2192 {parent}.{parent_pk}")

    html_str = net.generate_html()

    # ── Inject info panel (below graph) + JS event handler ───────────────────
    table_info_json = json.dumps(table_info, ensure_ascii=False)

    injection = f"""
<style>
  html, body {{
    margin: 0; padding: 0; background: #0e1117;
    display: flex; flex-direction: column; height: auto; overflow-x: hidden;
  }}
  #mynetwork {{ background: #0e1117 !important; flex: 0 0 460px; }}
  #fia-node-panel {{
    flex: 0 0 auto;
    padding: 12px 18px 10px;
    background: #0d1117;
    border-top: 3px solid #333;
    font-family: -apple-system, BlinkMacSystemFont, sans-serif;
    font-size: 13px; line-height: 1.5; color: #ddd;
    transition: border-top-color 0.25s;
    min-height: 72px;
  }}
</style>
<div id="fia-node-panel">
  <p id="fia-placeholder" style="margin:6px 0;color:#555;font-style:italic;">
    &#8593; Click any node to see its description.
  </p>
  <div id="fia-info" style="display:none;">
    <div style="display:flex;align-items:center;gap:10px;margin-bottom:8px;">
      <span id="fia-node-name" style="font-weight:700;font-size:15px;color:#fff;"></span>
      <span id="fia-node-cat"  style="padding:2px 10px;border-radius:4px;color:white;font-size:11px;flex-shrink:0;"></span>
    </div>
    <div id="fia-node-desc" style="color:#bbb;border-top:1px solid #2a2a2a;padding-top:8px;font-size:12px;"></div>
    <div style="margin-top:6px;font-size:10px;color:#444;">Click empty space to clear.</div>
  </div>
</div>
<script>
var FIA_TABLE_INFO = {table_info_json};
(function waitForNetwork() {{
    if (typeof network === 'undefined') {{ setTimeout(waitForNetwork, 100); return; }}
    // Fit on stabilized event, plus a belt-and-suspenders timeout fallback
    network.once("stabilized", function() {{
        network.fit({{animation: {{duration: 600, easingFunction: "easeInOutQuad"}}}});
    }});
    setTimeout(function() {{ network.fit(); }}, 1800);
    network.on("click", function(params) {{
        var panel       = document.getElementById("fia-node-panel");
        var placeholder = document.getElementById("fia-placeholder");
        var infoEl      = document.getElementById("fia-info");
        if (params.nodes.length > 0) {{
            var nodeId = params.nodes[0];
            var info = FIA_TABLE_INFO[nodeId];
            if (!info) return;
            document.getElementById("fia-node-name").textContent = nodeId;
            var catEl = document.getElementById("fia-node-cat");
            catEl.textContent = info.category;
            catEl.style.background = info.color;
            document.getElementById("fia-node-desc").textContent = info.description;
            panel.style.borderTopColor = info.color;
            placeholder.style.display = "none";
            infoEl.style.display = "block";
        }} else {{
            placeholder.style.display = "block";
            infoEl.style.display = "none";
            panel.style.borderTopColor = "#333";
        }}
    }});
}})();
</script>
"""
    html_str = html_str.replace("</body>", injection + "\n</body>")
    return html_str


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
    if "ui_mode" not in st.session_state:
        st.session_state["ui_mode"] = "Newbie"

    # ── Sidebar — experience mode (placed first for global visibility) ───────
    with st.sidebar:
        st.header("Experience Mode")
        selected_ui_mode = st.radio(
            "Choose interface mode",
            APP_UI_MODES,
            key="ui_mode",
            help=(
                "Newbie mode hides FIA field names/codes in some views until you opt in. "
                "Expert mode shows technical details up front."
            ),
        )
        _mode_meta = {
            "Newbie": {
                "color": "#36a36f",
                "bg": "#36a36f22",
                "title": "Newbie mode active",
                "body": (
                    "Concept-first explanations. FIA field names, codes, and runnable examples stay hidden "
                    "until you turn on technical details in a tab."
                ),
            },
            "Expert": {
                "color": "#d18a2b",
                "bg": "#d18a2b22",
                "title": "Expert mode active",
                "body": (
                    "Technical FIA details are shown up front, including table names, join steps, key columns, "
                    "and code examples."
                ),
            },
        }[selected_ui_mode]
        st.markdown(
            (
                f'<div style="margin:0 0 8px;padding:10px 12px;border-radius:8px;'
                f'border-left:4px solid {_mode_meta["color"]};background:{_mode_meta["bg"]};'
                f'font-size:0.86em;">'
                f'<div style="color:{_mode_meta["color"]};font-weight:700;margin-bottom:4px;">'
                f'{_mode_meta["title"]}</div>'
                f'<div style="color:#d7d7d7;line-height:1.35;">{_mode_meta["body"]}</div>'
                f'</div>'
            ),
            unsafe_allow_html=True,
        )

    is_newbie_mode = selected_ui_mode == "Newbie"
    if is_newbie_mode:
        st.markdown(
            """
<div style="margin:6px 0 10px;padding:10px 12px;border-radius:8px;
border-left:4px solid #36a36f;background:#36a36f15;color:#d8f0e4;">
  <strong>Newbie mode:</strong> concept-first explanations and progressive disclosure are enabled.
  Use the sidebar to switch to Expert mode.
</div>
            """,
            unsafe_allow_html=True,
        )
    else:
        st.markdown(
            """
<div style="margin:6px 0 10px;padding:10px 12px;border-radius:8px;
border-left:4px solid #d18a2b;background:#d18a2b15;color:#f3e6d0;">
  <strong>Expert mode:</strong> technical FIA schema details and code examples are shown by default.
  Use the sidebar to switch to Newbie mode.
</div>
            """,
            unsafe_allow_html=True,
        )

    guide_data = load_user_guide_index()
    guide_lookup = prepare_user_guide_lookup(guide_data)
    guide_summary = guide_lookup.get("summary", {})
    guide_loaded = bool(guide_lookup.get("columns_rows"))
    if guide_loaded:
        st.caption(
            f"User Guide index cache loaded: {guide_summary.get('tables_index_rows', '?')} tables, "
            f"{guide_summary.get('column_index_rows', '?')} table-column entries"
        )
    st.caption(
        "FIADB v9.4 · August 2025 · Schema browsing via PRAGMA — zero bulk data loading"
    )

    # ── Sidebar — database path (must come first so we can connect before buttons) ──
    with st.sidebar:
        st.header("Database")
        hosted_db_path = os.getenv("FIADB_DB_PATH", "").strip()
        allow_local_path_input = os.getenv("FIADB_ENABLE_LOCAL_PATH_INPUT", "").strip().lower() in {
            "1", "true", "yes", "on"
        }

        db_path = hosted_db_path
        if hosted_db_path:
            st.success("Using server-side FIADB database (FIADB_DB_PATH).")
        elif allow_local_path_input:
            db_path = st.text_input(
                "Path to FIADB SQLite file",
                placeholder="/path/to/FIADB_NATIONAL.db",
                help="Full absolute path. Schema metadata is read instantly via PRAGMA.",
            )
        else:
            st.info("Metadata-only mode (no database connected).")
            st.caption("Set `FIADB_DB_PATH` to enable live schema introspection.")

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

    # ── Sidebar — table group buttons ────────────────────────────────────────
    with st.sidebar:
        st.markdown("---")
        st.markdown("**Table groups**")

        if "sidebar_cat" not in st.session_state:
            st.session_state["sidebar_cat"] = None

        for cat, color in CATEGORY_COLORS.items():
            is_active = st.session_state["sidebar_cat"] == cat
            if st.button(
                cat,
                key=f"btn_{cat}",
                use_container_width=True,
                type="secondary",
            ):
                # Toggle off if already selected, otherwise select
                st.session_state["sidebar_cat"] = None if is_active else cat
                st.rerun()

            if is_active:
                s_tables = TABLE_CATEGORIES.get(cat, [])
                s_desc   = CATEGORY_DESCRIPTIONS.get(cat, "")
                in_db    = [t for t in s_tables if t in db_tables] if db_tables else []
                label    = (f"{len(in_db)}/{len(s_tables)} in DB"
                            if db_tables else f"{len(s_tables)} tables")
                st.markdown(
                    f'<div style="margin:-4px 0 8px;padding:10px 12px;'
                    f'background:{color}22;border-left:3px solid {color};'
                    f'border-radius:0 4px 4px 0;font-size:0.82em;color:#ccc;">'
                    f'<strong style="color:#fff;font-size:0.95em;">{cat}</strong>'
                    f'&nbsp;&nbsp;<span style="color:#888;">{label}</span>'
                    f'<br><br>{s_desc}</div>',
                    unsafe_allow_html=True,
                )
                for t in s_tables:
                    prefix = "✓ " if t in db_tables else "&nbsp;&nbsp;&nbsp;"
                    st.markdown(
                        f'<span style="font-size:0.85em;">{prefix}`{t}`</span>',
                        unsafe_allow_html=True,
                    )

        # ── JS: colour each sidebar button with its category colour ──────────
        # components.html runs in a sandboxed iframe; window.parent gives access
        # to the Streamlit page so we can style the real sidebar buttons.
        _color_map_js = json.dumps({cat: clr for cat, clr in CATEGORY_COLORS.items()})
        _active_js    = json.dumps(st.session_state.get("sidebar_cat"))
        components.html(
            f"""<script>
var _CM = {_color_map_js};
var _AC = {_active_js};
function _applyBtnColors() {{
    var sb = window.parent.document.querySelector('section[data-testid="stSidebar"]');
    if (!sb) return;
    sb.querySelectorAll('button').forEach(function(b) {{
        var lbl = b.innerText.trim();
        var col = _CM[lbl];
        if (!col) return;
        var active = (lbl === _AC);
        b.style.setProperty('background-color', active ? col : col + '55', 'important');
        b.style.setProperty('color', 'white', 'important');
        b.style.setProperty('border', '2px solid ' + col + (active ? '' : '88'), 'important');
        b.style.setProperty('font-weight', active ? '600' : '400', 'important');
    }});
}}
_applyBtnColors();
[150, 500, 1200].forEach(function(t) {{ setTimeout(_applyBtnColors, t); }});
var _sb = window.parent.document.querySelector('section[data-testid="stSidebar"]');
if (_sb) {{
    new MutationObserver(_applyBtnColors).observe(
        _sb, {{childList: true, subtree: true, attributeFilter: ['class', 'style']}}
    );
}}
</script>""",
            height=0,
        )

    tabs = st.tabs([
        "Overview",
        "Schema Browser",
        "Relationship Map",
        "Variable Explorer",
        "Analysis Pipelines",
    ])

    # ────────────────────────────────────────────────────────────────────────
    # TAB 1 · OVERVIEW
    # ────────────────────────────────────────────────────────────────────────
    with tabs[0]:
        st.header("Database Overview")
        st.markdown("""
The **Forest Inventory and Analysis (FIA)** database is the U.S. government's most comprehensive
record of forest conditions nationwide. Field crews visit thousands of plots every year and measure
every tree, track which trees died, count seedlings, and record soil and stand conditions.
All of that data lives in this database as interconnected tables.

**This tool helps you navigate the structure** — which tables exist, what columns they contain,
and how they link together — without loading the actual data. You don't need to be a database
expert to use it: the tabs above walk you through everything from a high-level overview to
ready-to-run analysis code.
        """)
        st.markdown("---")

        c1, c2, c3 = st.columns(3)
        total_spec = sum(len(v) for v in TABLE_CATEGORIES.values())
        c1.metric("Tables in v9.4 spec", total_spec)
        if db_tables:
            c2.metric("Tables in your DB", len(db_tables))
            c3.metric("Tables recognised", len(set(db_tables) & set(_TABLE_TO_CAT)))
        elif guide_loaded:
            c2.metric("Guide tables indexed", guide_summary.get("tables_index_rows", 0))
            c3.metric("Guide variable names", guide_summary.get("unique_column_names", 0))

        if guide_loaded:
            g1, g2, g3 = st.columns(3)
            g1.metric("Guide column entries", guide_summary.get("column_index_rows", 0))
            g2.metric("Guide TOC entries", guide_summary.get("toc_entries", 0))
            g3.metric("PDF pages", guide_summary.get("pdf_page_count", 0))

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

        if guide_loaded:
            with st.expander("User Guide chapter map (TOC)", expanded=False):
                toc_rows = []
                for ch in guide_lookup.get("chapters", []):
                    toc_rows.append(
                        {
                            "Section": ch.get("title"),
                            "Guide pages": (
                                f"{ch.get('start_page')}-{ch.get('end_page')}"
                                if ch.get("end_page") and ch.get("end_page") != ch.get("start_page")
                                else str(ch.get("start_page", ""))
                            ),
                        }
                    )
                if toc_rows:
                    st.dataframe(pd.DataFrame(toc_rows), use_container_width=True, hide_index=True)
                    st.caption(
                        "Page numbers above are the User Guide's printed page numbers. "
                        "Use them to jump directly to the right chapter in the PDF."
                    )

            with st.expander("Where common values live", expanded=False):
                concept_names = [c["name"] for c in CONCEPT_LOCATORS]
                concept_name = st.selectbox(
                    "Pick a concept",
                    concept_names,
                    key="overview_concept_locator",
                )
                concept = next((c for c in CONCEPT_LOCATORS if c["name"] == concept_name), None)
                if concept:
                    st.markdown(f"**What this helps you find:** {concept['what']}")
                    st.markdown("**Start with tables:** " + ", ".join(f"`{t}`" for t in concept["tables"]))
                    st.markdown("**Key columns:** " + ", ".join(f"`{c}`" for c in concept["columns"]))
                    st.caption(concept["notes"])

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
        render_mode_hint_box(
            is_newbie_mode=is_newbie_mode,
            newbie_title="Newbie view: guided table reading",
            newbie_body=(
                "Start by choosing one table and reading the plain-language description. "
                "Turn on technical sections when you are ready to inspect codebooks, join paths, and FK links."
            ),
            expert_title="Expert view: schema-first inspection",
            expert_body=(
                "Technical sections are intended to be used immediately: coded columns, join links, and live schema metadata."
            ),
        )
        if is_newbie_mode:
            st.markdown("""
Select a table to learn what it represents in the field first. The tab can also reveal the exact FIA field names,
coded columns, and join relationships once you turn on the technical sections toggle below.
            """)
        else:
            st.markdown("""
Select any table from the dropdown to see its columns, data types, and what each coded number means
in plain English. At the bottom of each table's page you'll also see its **connections** — which other
tables it links to and which tables link back to it.

> **Tip:** If you see a column like `STATUSCD` with values 1, 2, 3 — the *Coded columns reference*
> section below the column list will tell you exactly what each number means.
            """)
        st.markdown("---")

        all_known = [t for cat in TABLE_CATEGORIES.values() for t in cat]
        choices   = sorted(set(all_known) | set(db_tables)) if db_tables else all_known
        selected  = st.selectbox("Select a table", choices)

        schema_tech_default = not is_newbie_mode
        if "schema_browser_last_ui_mode" not in st.session_state:
            st.session_state["schema_browser_last_ui_mode"] = selected_ui_mode
        if st.session_state["schema_browser_last_ui_mode"] != selected_ui_mode:
            st.session_state["schema_browser_show_technical_sections"] = schema_tech_default
            st.session_state["schema_browser_last_ui_mode"] = selected_ui_mode
        elif "schema_browser_show_technical_sections" not in st.session_state:
            st.session_state["schema_browser_show_technical_sections"] = schema_tech_default
        show_schema_technical_sections = st.toggle(
            "Show technical schema sections (coded fields + join links)",
            value=schema_tech_default,
            key="schema_browser_show_technical_sections",
            help=(
                "When off, this tab stays focused on table purpose and columns. "
                "When on, it also shows coded-field codebooks and explicit join relationships."
            ),
        )

        if selected:
            cat  = _TABLE_TO_CAT.get(selected, "Unknown")
            desc = TABLE_DESCRIPTIONS.get(selected, "No description available.")
            guide_table_meta = guide_lookup.get("tables_by_name", {}).get(selected)
            guide_table_cols = guide_lookup.get("columns_by_table", {}).get(selected, [])

            col_badge, col_desc = st.columns([1, 4])
            color = CATEGORY_COLORS.get(cat, "#888")
            col_badge.markdown(
                f'<span style="background:{color};color:white;padding:4px 10px;'
                f'border-radius:4px">{cat}</span>',
                unsafe_allow_html=True,
            )
            col_desc.info(desc)

            if guide_table_meta:
                page_span = ""
                if guide_table_meta.get("guide_start_page"):
                    if (
                        guide_table_meta.get("guide_end_page")
                        and guide_table_meta.get("guide_end_page") != guide_table_meta.get("guide_start_page")
                    ):
                        page_span = f"{guide_table_meta['guide_start_page']}-{guide_table_meta['guide_end_page']}"
                    else:
                        page_span = str(guide_table_meta["guide_start_page"])
                guide_line = (
                    f"User Guide section `{guide_table_meta.get('section', '')}`"
                    f" - {guide_table_meta.get('official_table_name', selected)}"
                )
                if page_span:
                    guide_line += f" (pages {page_span})"
                st.caption(guide_line)

                with st.expander("Official User Guide summary (Index of Tables)", expanded=False):
                    if guide_table_meta.get("official_summary"):
                        st.markdown(guide_table_meta["official_summary"])
                    bullets = guide_table_meta.get("official_bullets", [])
                    if bullets:
                        st.markdown("**Documented relationships / notes from the guide:**")
                        for bullet in bullets:
                            st.markdown(f"- {bullet}")
                    joins = guide_table_meta.get("documented_joins", [])
                    if joins:
                        st.markdown("**Join expressions parsed from the guide text:**")
                        for expr in joins:
                            st.markdown(f"- `{expr}`")

            # Column list (live schema + User Guide variable index for this table)
            col_df = get_columns(conn, selected) if conn else pd.DataFrame()
            st.subheader("Columns")

            filter_col1, filter_col2 = st.columns([2, 1])
            table_col_query = filter_col1.text_input(
                "Filter variables in this table",
                placeholder="e.g. SPCD, status, carbon, owner",
                key=f"schema_table_col_query_{selected}",
            )
            table_family_options = ["All"] + [
                fam for fam in guide_lookup.get("families", [])
                if any(r.get("variable_family") == fam for r in guide_table_cols)
            ]
            table_family_filter = filter_col2.selectbox(
                "Variable family",
                table_family_options if table_family_options else ["All"],
                key=f"schema_table_family_{selected}",
            )

            filtered_guide_table_cols = filter_guide_columns(
                guide_table_cols,
                query=table_col_query,
                search_desc=True,
                exact=False,
                family_filter=table_family_filter,
            )

            if conn and not col_df.empty:
                live_display = col_df.copy()
                guide_by_col = {r["column_name"]: r for r in guide_table_cols}
                live_display["Guide name"] = live_display["Column"].map(
                    lambda c: guide_by_col.get(c, {}).get("descriptive_name", "")
                )
                live_display["Family"] = live_display["Column"].map(
                    lambda c: guide_by_col.get(c, {}).get("variable_family", "")
                )
                live_display["Guide subsection"] = live_display["Column"].map(
                    lambda c: guide_by_col.get(c, {}).get("subsection", "")
                )
                live_display["Field Guide"] = live_display["Column"].map(
                    lambda c: guide_by_col.get(c, {}).get("field_guide_section") or ""
                )

                if table_col_query:
                    q = table_col_query.upper()
                    live_display = live_display[
                        live_display["Column"].str.upper().str.contains(q, na=False)
                        | live_display["Guide name"].str.upper().str.contains(q, na=False)
                    ]
                if table_family_filter != "All":
                    live_display = live_display[live_display["Family"] == table_family_filter]

                live_cols_order = [
                    "Column", "Type", "Key", "Not Null", "Codebook",
                    "Guide name", "Family", "Guide subsection", "Field Guide",
                ]
                live_display = live_display[[c for c in live_cols_order if c in live_display.columns]]

                st.dataframe(live_display, use_container_width=True, hide_index=True)
                guide_match_count = int((live_display["Guide name"] != "").sum()) if "Guide name" in live_display else 0
                st.caption(
                    f"{len(live_display)} displayed columns from connected DB for `{selected}` "
                    f"(Guide metadata matched for {guide_match_count})"
                )

                if guide_table_cols:
                    live_names = set(col_df["Column"].tolist())
                    guide_only = [r for r in filtered_guide_table_cols if r["column_name"] not in live_names]
                    if guide_only:
                        with st.expander(
                            f"Guide-indexed variables not present in connected `{selected}` table ({len(guide_only)})",
                            expanded=False,
                        ):
                            st.dataframe(
                                guide_rows_to_df(guide_only, include_table=False),
                                use_container_width=True,
                                hide_index=True,
                            )
                else:
                    st.caption("No User Guide column-index entries found for this table in the cached PDF index.")

            elif conn and col_df.empty:
                st.warning(f"`{selected}` not present in the connected database.")
                if filtered_guide_table_cols:
                    st.markdown("**User Guide indexed variables for this table (from the PDF):**")
                    st.dataframe(
                        guide_rows_to_df(filtered_guide_table_cols, include_table=False),
                        use_container_width=True,
                        hide_index=True,
                    )
                    st.caption(
                        f"{len(filtered_guide_table_cols)} guide-indexed entries shown (table absent in connected DB)."
                    )
            else:
                if filtered_guide_table_cols:
                    st.dataframe(
                        guide_rows_to_df(filtered_guide_table_cols, include_table=False),
                        use_container_width=True,
                        hide_index=True,
                    )
                    st.caption(
                        f"{len(filtered_guide_table_cols)} variables indexed from the User Guide for `{selected}`. "
                        "Connect a database file to see SQLite types, PK flags, and nullability."
                    )
                elif guide_loaded and guide_table_meta and selected == "BOUNDARY":
                    st.info(
                        "The cached User Guide column index has no entries for `BOUNDARY` (the table is documented in the "
                        "Index of Tables / chapter sections, but not listed in the extracted Index of Column Names rows). "
                        "Connect a database file to inspect the actual columns."
                    )
                else:
                    st.info("Connect a database file (sidebar) to see column details.")

            # Codebook for this table's coded columns (works from live schema or guide index)
            if not col_df.empty:
                codebook_source_cols = col_df["Column"].tolist()
            else:
                codebook_source_cols = [r["column_name"] for r in guide_table_cols]

            outgoing = [(c, ck, p, pk, lb) for c, ck, p, pk, lb in RELATIONSHIPS if c == selected]
            incoming = [(c, ck, p, pk, lb) for c, ck, p, pk, lb in RELATIONSHIPS if p == selected]
            coded_cols = [c for c in codebook_source_cols if c in FIELD_CODEBOOK]
            if show_schema_technical_sections:
                if coded_cols:
                    st.subheader("Coded columns reference")
                    for col in coded_cols:
                        cb = FIELD_CODEBOOK[col]
                        with st.expander(f"`{col}` - {cb['description']}"):
                            st.dataframe(
                                pd.DataFrame([{"Code": k, "Meaning": v} for k, v in cb["codes"].items()]),
                                use_container_width=True,
                                hide_index=True,
                            )

                # Relationships
                if outgoing or incoming:
                    st.subheader("Table connections")
                    if outgoing:
                        st.markdown(
                            "**Links to →** "
                            "<span style='color:gray;font-size:0.9em'>"
                            "This table borrows an ID from another table — use these columns to join.</span>",
                            unsafe_allow_html=True,
                        )
                        for c, ck, p, pk, lb in outgoing:
                            st.markdown(f"- `{c}.{ck}` → `{p}.{pk}`")
                    if incoming:
                        st.markdown(
                            "**Linked from ←** "
                            "<span style='color:gray;font-size:0.9em'>"
                            "These tables have a column that points back to this one.</span>",
                            unsafe_allow_html=True,
                        )
                        for c, ck, p, pk, lb in incoming:
                            st.markdown(f"- `{c}.{ck}` → `{selected}.{pk}`")
            elif coded_cols or outgoing or incoming:
                st.info(
                    "Technical coded-field codebooks and explicit join links are hidden. "
                    "Turn on the toggle above to reveal them."
                )

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
        st.markdown("""
This diagram shows every FIA table as a node and every connection between tables as an arrow.
An arrow from Table A to Table B means: *"Table A has a column that stores a row ID from Table B"*
— in other words, you can join them on that column.

**PLOT** is the central hub — almost every other table links back to it via `PLT_CN`.
The color of each node shows which group the table belongs to (see the color key in the sidebar).

> **How to use:** Hover over a node to see a description. Drag nodes to rearrange the layout.
> Click a table name in the *Schema Browser* tab to explore its columns and connections in detail.
        """)
        st.caption("Nodes = tables · Arrows = connections · Colors = table group · Drag to rearrange")

        if _PYVIS_AVAILABLE:
            html = build_pyvis_graph(db_tables)
            components.html(html, height=640, scrolling=False)
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
        st.subheader("All table connections")
        st.caption(
            "Each row means: the 'From table' has a column (Join column) that stores an ID "
            "pointing to a row in the 'Links to table'."
        )
        rel_df = pd.DataFrame(
            [(c, ck, p, pk) for c, ck, p, pk, lb in RELATIONSHIPS],
            columns=["From table", "Join column", "Links to table", "Matched column"],
        )
        st.dataframe(rel_df, use_container_width=True, hide_index=True)

    # ────────────────────────────────────────────────────────────────────────
    # TAB 4 · COLUMN SEARCH
    # ────────────────────────────────────────────────────────────────────────
    with tabs[3]:
        st.header("Variable Explorer")
        render_mode_hint_box(
            is_newbie_mode=is_newbie_mode,
            newbie_title="Newbie view: concept-first variable search",
            newbie_body=(
                "Start with the concept locator, then run a simple keyword search (for example: species, disturbance, owner). "
                "Advanced filters are hidden until you turn them on."
            ),
            expert_title="Expert view: full index search controls",
            expert_body=(
                "All search controls are available for direct filtering by table group, table, variable family, and exact-name matching."
            ),
        )
        if is_newbie_mode:
            st.markdown("""
Use this tab to answer: **What is the FIA field name for the thing I care about?** Start with a plain-language term,
then reveal advanced filters if you need to narrow by table group or exact column name.
            """)
        else:
            st.markdown("""
Use this tab to answer questions like **Where does this value live?** and **Which tables use this variable name?**

The explorer is powered by the FIADB User Guide's *Index of Column Names* (cached from the PDF), so it works
**even when no SQLite database is connected**. When a database is connected, you can also run a live PRAGMA-based
schema search to compare the guide index with the tables actually present in your file.
            """)

        if guide_loaded:
            with st.expander("Where do I find... ? (concept locator)", expanded=is_newbie_mode):
                concept_names = [c["name"] for c in CONCEPT_LOCATORS]
                concept_name = st.selectbox(
                    "Choose a concept",
                    concept_names,
                    key="variable_explorer_concept",
                )
                concept = next((c for c in CONCEPT_LOCATORS if c["name"] == concept_name), None)
                if concept:
                    st.markdown(f"**What to look for:** {concept['what']}")
                    st.markdown("**Start with tables:** " + ", ".join(f"`{t}`" for t in concept["tables"]))
                    st.markdown("**Key columns to search:** " + ", ".join(f"`{c}`" for c in concept["columns"]))
                    st.caption(concept["notes"])

            query = st.text_input(
                "Variable / column / table search",
                placeholder="e.g. SPCD, AGENTCD, PREV_PLT_CN, carbon, owner group",
                key="guide_var_query",
            )
            if is_newbie_mode:
                st.caption(
                    "Try simple terms first (e.g., `species`, `seedling`, `mortality`, `owner`, `forest type`). "
                    "Enable advanced controls to filter by table group or exact column name."
                )

            var_advanced_default = not is_newbie_mode
            if "variable_explorer_last_ui_mode" not in st.session_state:
                st.session_state["variable_explorer_last_ui_mode"] = selected_ui_mode
            if st.session_state["variable_explorer_last_ui_mode"] != selected_ui_mode:
                st.session_state["variable_explorer_show_advanced"] = var_advanced_default
                st.session_state["variable_explorer_last_ui_mode"] = selected_ui_mode
            elif "variable_explorer_show_advanced" not in st.session_state:
                st.session_state["variable_explorer_show_advanced"] = var_advanced_default

            show_variable_advanced = st.toggle(
                "Show advanced search controls",
                value=var_advanced_default,
                key="variable_explorer_show_advanced",
                help=(
                    "Advanced controls include exact-name matching, description search, and filters by table group, family, and table."
                ),
            )

            table_groups = sorted(TABLE_CATEGORIES.keys())
            if show_variable_advanced:
                q1, q2, q3 = st.columns([2, 1, 1])
                exact = q2.checkbox("Exact column match", value=False, key="guide_var_exact")
                search_desc = q3.checkbox("Search descriptions", value=True, key="guide_var_desc")

                f1, f2, f3 = st.columns(3)
                table_category_filter = f1.selectbox(
                    "Table group",
                    ["All"] + table_groups,
                    key="guide_var_table_group",
                )
                family_filter = f2.selectbox(
                    "Variable family",
                    ["All"] + guide_lookup.get("families", []),
                    key="guide_var_family",
                )
                table_filter = f3.selectbox(
                    "Table",
                    ["All"] + sorted(guide_lookup.get("tables_by_name", {}).keys()),
                    key="guide_var_table",
                )

                view_mode = st.radio(
                    "View",
                    ["Occurrences", "Grouped by variable name"],
                    horizontal=True,
                    key="guide_var_view_mode",
                )
            else:
                exact = False
                search_desc = True
                table_category_filter = "All"
                family_filter = "All"
                table_filter = "All"
                view_mode = "Grouped by variable name"
                st.caption("Advanced filters are off: searching all tables and variable families.")

            filters_active = any([
                bool((query or "").strip()),
                table_category_filter != "All",
                family_filter != "All",
                table_filter != "All",
            ])

            if filters_active:
                filtered_rows = filter_guide_columns(
                    guide_lookup.get("columns_rows", []),
                    query=query,
                    search_desc=search_desc,
                    exact=exact,
                    table_filter=table_filter,
                    table_category_filter=table_category_filter,
                    family_filter=family_filter,
                )
            else:
                filtered_rows = []

            if filters_active and filtered_rows:
                unique_columns = len({r["column_name"] for r in filtered_rows})
                unique_tables = len({r["oracle_table"] for r in filtered_rows})
                m1, m2, m3 = st.columns(3)
                m1.metric("Matches (occurrences)", len(filtered_rows))
                m2.metric("Unique variable names", unique_columns)
                m3.metric("Tables covered", unique_tables)

                if view_mode == "Occurrences":
                    df = guide_rows_to_df(filtered_rows, include_table=True)
                    if db_tables:
                        df["In connected DB"] = df["Table"].apply(lambda t: "Yes" if t in db_tables else "No")
                        cols = list(df.columns)
                        if "In connected DB" in cols:
                            cols = cols[:-1] + ["In connected DB"]
                            df = df[cols]
                    st.dataframe(df, use_container_width=True, hide_index=True)
                else:
                    grouped = []
                    by_name = defaultdict(list)
                    for r in filtered_rows:
                        by_name[r["column_name"]].append(r)
                    for col_name, rows in sorted(by_name.items()):
                        tables = sorted({r["oracle_table"] for r in rows})
                        groups = sorted({r.get("table_category", "Unknown") for r in rows})
                        guide_names = sorted({r.get("descriptive_name", "") for r in rows if r.get("descriptive_name")})
                        families = sorted({r.get("variable_family", "Other") for r in rows})
                        grouped.append(
                            {
                                "Column": col_name,
                                "Occurrences": len(rows),
                                "Tables": ", ".join(tables),
                                "Table groups": ", ".join(groups),
                                "Guide names": " | ".join(guide_names[:3]) + (" ..." if len(guide_names) > 3 else ""),
                                "Family": ", ".join(families),
                            }
                        )
                    st.dataframe(pd.DataFrame(grouped), use_container_width=True, hide_index=True)
                    st.caption(
                        "Grouped view helps you see reuse patterns (for example, one variable name appearing in TREE, "
                        "SEEDLING, and reference tables)."
                    )
            elif filters_active:
                st.info("No matches found in the cached User Guide column index.")
            else:
                st.info(
                    "Enter a search term or apply a filter to explore the User Guide variable index. "
                    "Examples: `SPCD`, `PLT_CN`, `owner`, `carbon`, `dstrb`, `evaluation`."
                )
                common_rows = []
                for col_name, rows in guide_lookup.get("columns_by_name", {}).items():
                    common_rows.append(
                        {
                            "Column": col_name,
                            "Occurrences": len(rows),
                            "Tables": len({r["oracle_table"] for r in rows}),
                            "Example tables": ", ".join(sorted({r["oracle_table"] for r in rows})[:4]),
                            "Family": rows[0].get("variable_family", "Other"),
                        }
                    )
                if common_rows:
                    common_df = pd.DataFrame(common_rows).sort_values(
                        ["Occurrences", "Tables", "Column"], ascending=[False, False, True]
                    ).head(25)
                    st.markdown("**Frequently reused variable names (from the guide index)**")
                    st.dataframe(common_df, use_container_width=True, hide_index=True)

            if conn and (query or filters_active):
                with st.expander("Connected DB schema search (live PRAGMA)", expanded=False):
                    live_results = []
                    q_live = (query or "").strip()
                    if q_live:
                        for table in db_tables:
                            try:
                                cur = conn.execute(f"PRAGMA table_info({table})")
                                for row in cur.fetchall():
                                    col_name = row[1]
                                    if q_live.upper() in col_name.upper():
                                        live_results.append(
                                            {
                                                "Table": table,
                                                "Column": col_name,
                                                "Type": row[2],
                                                "Table group": _TABLE_TO_CAT.get(table, "Unknown"),
                                            }
                                        )
                            except Exception:
                                pass
                    if live_results:
                        st.dataframe(pd.DataFrame(live_results), use_container_width=True, hide_index=True)
                        st.caption(
                            f"{len(live_results)} live schema matches across "
                            f"{pd.DataFrame(live_results)['Table'].nunique()} tables in the connected database"
                        )
                    else:
                        st.caption("No live PRAGMA matches (or no text query provided).")
        else:
            st.warning(
                "User Guide index cache not found (`fiadb_user_guide_index_v94.json`). "
                "The advanced variable explorer is unavailable until that file is present."
            )
            query = st.text_input(
                "Column name (partial match OK)",
                placeholder="e.g. SPCD, PLT_CN, AGENTCD",
                key="legacy_column_search_query",
            )
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
                    st.dataframe(pd.DataFrame(results), use_container_width=True, hide_index=True)
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
| `PLT_CN` | -> PLOT.CN - links child records to the plot | COND, TREE, SEEDLING, SUBPLOT, DWM_*, POP_PLOT_STRATUM_ASSGN, ... |
| `TRE_CN` | -> TREE.CN - links GRM/woodland records to the tree | TREE_GRM_COMPONENT, TREE_WOODLAND_STEMS, TREE_GRM_* |
| `SPCD` | FIA species code | TREE, SEEDLING, REF_SPECIES, SITETREE |
| `FORTYPCD` | Forest type code | COND, REF_FOREST_TYPE |
| `CONDID` | Condition number within a plot (1, 2, 3...) | COND, TREE, SEEDLING, SUBPLOT, SUBP_COND |
| `STATECD` | FIPS state code | PLOT, COND, TREE, SURVEY, COUNTY, ... |
| `INVYR` | Inventory year (visit year) | PLOT, COND, TREE, SEEDLING, SURVEY, ... |
| `EVAL_CN` | -> POP_EVAL.CN | POP_ESTN_UNIT, POP_EVAL_ATTRIBUTE, POP_EVAL_TYP |
| `STRATUM_CN` | -> POP_STRATUM.CN | POP_PLOT_STRATUM_ASSGN |
| `SRV_CN` | -> SURVEY.CN | PLOT |
| `CTY_CN` | -> COUNTY.CN | PLOT |
| `PRJ_CN` | -> PROJECT.CN | SURVEY |
        """)

        if guide_loaded:
            st.markdown("---")
            st.subheader("Variable families used in this explorer")
            fam_rows = [
                {"Family": fam, "Meaning": VARIABLE_FAMILY_DESCRIPTIONS.get(fam, "")}
                for fam in VARIABLE_FAMILY_ORDER
                if fam in set(guide_lookup.get("families", []))
            ]
            st.dataframe(pd.DataFrame(fam_rows), use_container_width=True, hide_index=True)

    # TAB 5 · ANALYSIS PIPELINES
    # ────────────────────────────────────────────────────────────────────────
    with tabs[4]:
        st.header("Analysis Pipelines")
        if is_newbie_mode:
            st.markdown("""
These are beginner-friendly FIA analysis recipes. Start with the **question** each analysis answers,
then optionally reveal the FIA-specific table names, column names, codes, and runnable code examples.

Use the **language selector** below to pick which code examples to show when technical details are enabled.
`SQL` is the canonical query. `Python` and `R` may be native examples (when provided) or wrappers around SQL.

Click any pipeline below to expand it.
            """)
        else:
            st.markdown("""
These are step-by-step recipes for the most common FIA analyses. Each one explains the goal in plain
language, lists the tables you need to pull from and why, and includes a ready-to-run code example.

Use the **language selector** below to switch code examples for all pipelines in this tab.
`SQL` is the canonical query. `Python` and `R` may be native examples (when provided) or wrappers around SQL.

Click any pipeline below to expand it.
            """)

        if "analysis_pipeline_language" not in st.session_state:
            st.session_state["analysis_pipeline_language"] = "SQL"

        technical_default = not is_newbie_mode
        if "analysis_pipeline_last_ui_mode" not in st.session_state:
            st.session_state["analysis_pipeline_last_ui_mode"] = selected_ui_mode
        if st.session_state["analysis_pipeline_last_ui_mode"] != selected_ui_mode:
            st.session_state["analysis_pipeline_show_technical"] = technical_default
            st.session_state["analysis_pipeline_last_ui_mode"] = selected_ui_mode
        elif "analysis_pipeline_show_technical" not in st.session_state:
            st.session_state["analysis_pipeline_show_technical"] = technical_default

        selected_pipeline_language = st.radio(
            "Code example language",
            PIPELINE_CODE_LANGUAGES,
            horizontal=True,
            key="analysis_pipeline_language",
        )
        st.caption("Global selector: applies to every pipeline card in this tab.")
        show_pipeline_technical = st.toggle(
            "Show FIA table names, column names, codes, and code examples",
            value=technical_default,
            key="analysis_pipeline_show_technical",
            help=(
                "Turn this on to reveal detailed joins, FIA variable names, coded fields, and runnable examples."
            ),
        )

        for tmpl in PIPELINE_TEMPLATES:
            with st.expander(f"**{tmpl['name']}**  -  *{tmpl['goal']}*"):
                st.markdown("**What this answers:** " + tmpl["goal"])

                if is_newbie_mode and not show_pipeline_technical:
                    st.markdown("**Data you will use (concepts first):**")
                    for t in tmpl.get("tables", []):
                        st.markdown(
                            f"- **{t.replace('_', ' ').title()}**: {describe_table_for_newbie(t)}"
                        )
                    st.info(
                        "Technical FIA table names, join steps, field codes, and runnable SQL/Python/R examples "
                        "are hidden in Newbie mode. Turn on the toggle above to reveal them."
                    )
                else:
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

                    code_example = render_pipeline_code(tmpl, selected_pipeline_language)
                    st.markdown(f"**{code_example['label']}:**")
                    st.code(code_example["code"], language=code_example["code_lang"])
                    if code_example.get("note"):
                        st.caption(str(code_example["note"]))

        st.markdown("---")
        st.subheader("Field codebook")
        if is_newbie_mode and not show_pipeline_technical:
            st.caption("Field codes are hidden in Newbie mode until you enable technical details.")
            with st.expander("Show FIA field codebook (technical)"):
                for field, cb in FIELD_CODEBOOK.items():
                    with st.expander(f"`{field}` — {cb['description']}"):
                        st.dataframe(
                            pd.DataFrame([{"Code": k, "Meaning": v} for k, v in cb["codes"].items()]),
                            use_container_width=True,
                            hide_index=True,
                        )
        else:
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
