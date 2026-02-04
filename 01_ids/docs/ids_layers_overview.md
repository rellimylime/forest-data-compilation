# IDS Layers Overview (Cleaned Outputs)

## Temporal Extent
- **Coverage:** 1997–2024 (all layers)
- **Notes:** Coverage reflects survey effort; early years have fewer records and some regions have sparse sampling.

## Cleaned Outputs
All cleaned layers are stored in a single geopackage for easy access:
- `01_ids/data/processed/ids_layers_cleaned.gpkg`
  - `damage_areas` — polygon observations of damage
  - `damage_points` — point observations of damage
  - `surveyed_areas` — polygons for surveyed coverage

## Core Fields (Present Across Layers Where Applicable)
| Field | Meaning | Notes |
| --- | --- | --- |
| `SURVEY_YEAR` | Year of observation | 1997–2024; use for temporal joins |
| `REGION_ID` | USFS region identifier | See `01_ids/lookups/region_lookup.csv` |
| `ACRES` | Area affected or surveyed | May be absent on some records |
| `AREA_TYPE` | Collection method | `POLYGON` or `GRID_240/480/960/1920` where present |
| `SOURCE_FILE` | Original .gdb name | Distinguishes CA vs HI Region 5 |
| `SURVEY_FEATURE_ID` | Surveyed area identifier | Only in `surveyed_areas` (generated during cleaning) |

## Damage Layer Fields (damage_areas, damage_points)
| Field | Meaning | Notes |
| --- | --- | --- |
| `OBSERVATION_ID` | Unique observation ID | Unique per damage observation |
| `DAMAGE_AREA_ID` | Geometry identifier | Shared by pancake features |
| `HOST_CODE` | Host species code | Lookup: `lookups/host_code_lookup.csv` |
| `DCA_CODE` | Damage agent code | Lookup: `lookups/dca_code_lookup.csv` |
| `DAMAGE_TYPE_CODE` | Damage type code | Lookup: `lookups/damage_type_lookup.csv` |
| `OBSERVATION_COUNT` | Single vs multiple observations | `SINGLE`/`MULTIPLE` |
| `PERCENT_AFFECTED_CODE` | Canopy damage class | DMSM method (2015+); see lookup |
| `PERCENT_MID` | Midpoint canopy % | DMSM method |
| `LEGACY_TPA` | Trees per acre | Legacy method (pre-2015) |
| `LEGACY_NO_TREES` | Total tree count | Legacy method |
| `LEGACY_SEVERITY_CODE` | Severity class | Legacy method; see lookup |

## Methodology Change (~2015)
- **Legacy method (pre-2015):** Intensity recorded via `LEGACY_TPA`, `LEGACY_NO_TREES`, and `LEGACY_SEVERITY_CODE`.
- **DMSM method (2015+):** Intensity recorded via `PERCENT_AFFECTED_CODE` and `PERCENT_MID`.
- **Transition nuance:** 2015 includes placeholder `PERCENT_AFFECTED_CODE = -1` (recoded to `NA`), and some overlap exists in 2015–2016 where both systems contain data. These measures are **not** directly comparable.

## Consistency Notes
- `SURVEY_YEAR`, `REGION_ID`, and `SOURCE_FILE` are consistent keys across all cleaned layers.
- Intensity fields are only meaningful in the **damage** layers and are method-specific (Legacy vs DMSM).

## Pancake Features (Overlapping Observations)
- IDS can store multiple observations with identical geometry under a shared `DAMAGE_AREA_ID` and distinct `OBSERVATION_ID` values.
- These are flagged by `OBSERVATION_COUNT = MULTIPLE` and should not be naively summed for area totals.
- **Guidance:** when summarizing area, aggregate by `DAMAGE_AREA_ID` first (e.g., distinct geometries) to avoid double counting.
