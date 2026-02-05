# IDS Data Cleaning Log

**Dataset:** USDA Forest Service Insect and Disease Detection Survey (IDS)
**Data Manager:** Emily Miller
**Institution:** UCSB, Bren School, Landscapes of Change Lab
**Log Created:** 2026-01-30
**Last Updated:** 2026-02-03

---

## Dataset Overview

- **Source:** https://www.fs.usda.gov/foresthealth/
- **Format:** Geodatabase (.gdb), 10 regional files
- **Temporal range:** 1997-2024
- **Total DAMAGE_AREAS features:** 4,475,827
- **Total DAMAGE_POINTS features:** 1,243,890
- **Total SURVEYED_AREAS features:** 74,505
- **Original CRS:** Mixed (see Issue #008)
- **Output CRS:** EPSG:4326 (WGS84)

---

## Inspection Summary (02_inspect_ids.R)

### Layer Structure
Each regional .gdb contains 3 layers with region-specific suffixes:
- `DAMAGE_AREAS_FLAT_AllYears_<region>` — Polygons of observed damage
- `DAMAGE_POINTS_FLAT_Allyears_<region>` — Points for small damage clusters
- `SURVEYED_AREAS_FLAT_AllYears_<region>` — Survey boundary polygons

### Field Consistency
All 10 regions have identical field structure (44 fields). No cross-region inconsistencies.

### Temporal Distribution (R5 California sample)
- Range: 1997-2024
- Peak years: 2010-2016 (20k-40k records/year)
- Early years sparse: 1997-2001 (<500 records/year)
- Survey effort increased dramatically ~2006

---

## Data Quality Issues

### Issue #001: Legacy vs DMSM Data Split

**Date identified:** 2025-01-30
**Fields affected:** PERCENT_AFFECTED_*, LEGACY_*

**Description:**
Data collection methodology changed ~2015 from legacy system to DMSM (Digital Mobile Sketch Mapping).
- Pre-2015: Uses LEGACY_TPA (trees per acre), LEGACY_NO_TREES, LEGACY_SEVERITY_CODE
- Post-2015: Uses PERCENT_AFFECTED_CODE (1-5 scale of canopy damage)

These are NOT directly comparable measures of damage intensity.

**Verification (post-2015 sample, n=5000 from 2016):**
- PERCENT_AFFECTED_CODE: 0% missing (populated as expected)
- LEGACY_TPA: 83% non-zero (ALSO populated — both systems overlap)

**Finding:** The transition is not a clean break. Both measurement systems have data in 2016.

**Decision:** Keep both sets of fields. Let analysis scripts decide which to use based on research question.

---

### Issue #002: Administrative Metadata Missing

**Date identified:** 2025-01-30
**Fields affected:** CREATED_DATE, MODIFIED_DATE, FEATURE_USER_ID, OBSERVATION_USER_ID, LABEL

**Description:**
These administrative/metadata fields are 100% missing in the sample.

**Decision:** Drop these fields during cleaning to reduce file size.

---

### Issue #003: Code vs Text Field Redundancy

**Date identified:** 2025-01-30
**Fields affected:** Multiple code/text pairs

**Description:**
Several fields have both numeric codes and text descriptions:
- HOST_CODE / HOST (HOST 92% missing)
- DCA_CODE / DCA_COMMON_NAME
- DAMAGE_TYPE_CODE / DAMAGE_TYPE
- PERCENT_AFFECTED_CODE / PERCENT_AFFECTED
- LEGACY_SEVERITY_CODE / LEGACY_SEVERITY
- HOST_GROUP_CODE / HOST_GROUP (100% missing)

**Decision:** Keep codes only, drop text columns. Create lookup tables for text names (stored in `lookups/`).

---

### Issue #004: DMSM-Specific Fields Empty in Legacy Data

**Date identified:** 2025-01-30
**Fields affected:** COLLECTION_MODE, SNAPGRID_ROW, SNAPGRID_COLUMN, AREA_TYPE

**Description:**
Grid-based collection fields only populated for DMSM data.
- AREA_TYPE in legacy: always "POLYGON"
- AREA_TYPE in DMSM: "POLYGON" or "GRID_240/480/960/1920"

**Decision:** Keep AREA_TYPE (useful for distinguishing collection method). Drop SNAPGRID_* and COLLECTION_MODE.

---

### Issue #005: Pancake Features (Multiple Observations)

**Date identified:** 2025-01-30
**Fields affected:** DAMAGE_AREA_ID, OBSERVATION_ID, OBSERVATION_COUNT

**Description:**
Per documentation, overlapping damage from multiple agents creates "pancake" features:
- Same DAMAGE_AREA_ID (same geometry)
- Different OBSERVATION_ID (different damage observation)
- Flagged as OBSERVATION_COUNT = "MULTIPLE"

**Quantified:** 14.7% of R5 features have OBSERVATION_COUNT = "MULTIPLE"

**Decision:** Keep all rows (one per observation). ACRES should not be summed naively — group by DAMAGE_AREA_ID first if calculating total area. For TerraClimate extraction, use unique DAMAGE_AREA_ID geometries and join results back to observations.

---

### Issue #006: PERCENT_AFFECTED_CODE = -1

**Date identified:** 2025-01-30
**Records affected:** 121,648 in R5 alone (~35% of region), all from 2015

**Description:**
Records with PERCENT_AFFECTED_CODE = -1, exclusively from 2015 (transition year). Appears across all damage types. Likely placeholder during legacy→DMSM transition. LEGACY_* fields are populated for these records.

**Decision:** Recode -1 to NA during cleaning. Use LEGACY_* fields for intensity on these records.

---

### Issue #007: OBSERVATION_COUNT Capitalization

**Date identified:** 2025-01-30
**Description:** Values are "SINGLE", "Single", and "MULTIPLE" (inconsistent case).

**Decision:** Standardize to uppercase during cleaning.

---

### Issue #008: CRS Mismatch Across Regions

**Date identified:** 2025-01-30
**Regions affected:** All

**Description:**
Three different coordinate reference systems across regions:
- **CONUS (R1-R6, R8-R9):** USA_Contiguous_Albers_Equal_Area_Conic_USGS_version
- **Alaska (R10):** NAD83 / Alaska Albers
- **Hawaii (R5-HI):** Hawaii Albers Equal Area Conic

Cannot merge without transformation.

**Decision:** Transform all regions to EPSG:4326 (WGS84) during cleaning. This matches TerraClimate CRS for downstream merge.

---

### Issue #009: US_AREA Column Uninformative

**Date identified:** 2025-01-30
**Description:**
US_AREA column contains only 3 values: "CONUS", "ALASKA", "HAWAII". Redundant with REGION_ID and SOURCE_FILE.

**Decision:** Drop US_AREA column. Use region_lookup.csv to get US_AREA from REGION_ID if needed. SOURCE_FILE column distinguishes CA vs HI within Region 5.

---

## Version History

| Version | Date | Changes |
|---------|------|---------|
| 0.1 | 2025-01-30 | Initial inspection findings |
| 0.2 | 2025-01-30 | Added CRS issue, US_AREA decision, lookup tables, finalized field list |
| 0.3 | 2025-01-30 | Added scripts table, updated progress tracking |
