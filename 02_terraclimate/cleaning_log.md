# TerraClimate Extraction Log

**Dataset:** TerraClimate extracted at IDS observation locations
**Data Manager:** Emily Miller
**Institution:** UCSB, Bren School, Landscapes of Change Lab
**Log Created:** 2026-01-31
**Last Updated:** 2026-02-03

---

## Dataset Overview

- **Source:** TerraClimate via Google Earth Engine
- **GEE Asset:** IDAHO_EPSCOR/TERRACLIMATE
- **Native Resolution:** ~4km (1/24th degree)
- **Temporal Resolution:** Monthly → aggregated to annual means
- **Variables Extracted:** 14 climate/water balance variables (see `data_dictionary.csv`)
- **Extraction Method:** Point sampling at IDS polygon centroids and point locations
- **Output Format:** CSV files (one per layer per region-year batch)

---

## Data Quality Issues

### Issue #001: Raw Values Require Scaling

**Date identified:** 2025-01-31
**Fields affected:** All 14 climate variables

**Description:**
TerraClimate stores values as integers for storage efficiency. Each variable has a scale factor that must be applied to convert to physical units (e.g., tmmx raw value 254 x 0.1 = 25.4 degrees C).

**Decision:** Keep raw values in extraction CSVs. Apply scale factors during merge step using config.yaml definitions. See `data_dictionary.csv` for all scale factors.

---

### Issue #002: Annual Means vs Annual Totals

**Date identified:** 2025-01-31
**Fields affected:** pr, aet, pet, def, ro (flux variables)

**Description:**
Extraction calculates the mean of 12 monthly values. For flux variables (precipitation, evapotranspiration, runoff, deficit), the annual total is more scientifically meaningful. For state variables (temperature, soil moisture, drought index), annual mean is appropriate.

**Decision:** Store annual means. Analysis scripts should multiply flux variables by 12 to get annual totals if needed.

---

### Issue #003: Invalid Centroid Coordinates

**Date identified:** 2025-01-31
**Records affected:** 10 out of 4,475,827 (0.0002%)

**Description:**
Ten IDS observations produced NaN coordinates when computing centroids with `st_point_on_surface()`. Likely degenerate geometries (slivers, self-intersecting polygons).

**Decision:** Exclude from extraction. Identifiable by missing OBSERVATION_IDs in TerraClimate output.

---

### Issue #004: TerraClimate Temporal Lag

**Date identified:** 2025-01-31
**Potential impact:** 2024 data

**Description:**
TerraClimate data release lags by several months to over a year. 2024 data was available and extracted successfully at time of extraction.

**Decision:** 2024 extraction succeeded. If future analysis reveals data quality issues for 2024, consider using 2023 climate as proxy.

---

### Issue #005: Coastal/Edge NoData Pixels

**Date identified:** 2025-01-31

**Description:**
TerraClimate has NoData values over oceans and at dataset edges. IDS observations near coastlines may fall in NoData pixels if the centroid lands offshore or in an unmapped area.

**Decision:** Check for systematic missingness in coastal regions during merge step. Quantified in Issue #010.

---

### Issue #006: st_point_on_surface Warning

**Date identified:** 2025-01-31
**Type:** Warning (not error)

**Description:**
R generates a warning when using `st_point_on_surface()` on geodetic (lat/lon) coordinates. At the scale of individual IDS polygons (typically <1km) and TerraClimate resolution (~4km), this introduces negligible error.

**Decision:** Ignore warning. Sub-kilometer centroid precision is not meaningful at 4km pixel resolution.

---

### Issue #007: Duplicate OBSERVATION_IDs in TerraClimate Output

**Date identified:** 2025-02-03
**Records affected:** 3,499 duplicate pairs (6,998 rows total)

**Description:**
Extraction produced duplicate rows for ~3,499 OBSERVATION_IDs within the same region-year batch. Caused by off-by-one error at sub-batch boundaries (features at positions 5000, 10000, etc. extracted twice). All duplicates have identical climate values.

**Decision:** Deduplicate with `distinct(OBSERVATION_ID, .keep_all = TRUE)` during merge.

---

### Issue #008: NA OBSERVATION_IDs in Region 9, 2024

**Date identified:** 2025-02-03
**Records affected:** 15

**Description:**
15 rows from Region 9, Year 2024 batch have NA OBSERVATION_IDs. The ID column was not passed through GEE correctly for these features.

**Decision:** Filter out during merge.

---

### Issue #009: Join Type Mismatch

**Date identified:** 2025-02-03
**Impact:** 896,929 false NA matches initially

**Description:**
Original merge joined on OBSERVATION_ID, REGION_ID, and SURVEY_YEAR. TerraClimate CSVs stored REGION_ID and SURVEY_YEAR as numeric (double), while IDS geopackage stored them as integer, causing join failures.

**Decision:** Join on OBSERVATION_ID only (unique identifier). Drop REGION_ID and SURVEY_YEAR from TerraClimate data before join.

---

### Issue #010: Missing Climate Data (NoData Pixels)

**Date identified:** 2025-02-03
**Records affected:** 1,235 (0.03%)

**Description:**
1,235 IDS observations have no climate data because their centroids fall in TerraClimate NoData pixels (ocean, dataset edges).

**Distribution by region:**
- Region 10 (Alaska): 694
- Region 6 (Pacific NW): 269
- Region 9 (Eastern): 152
- Region 2 (Rocky Mtn): 44
- Region 8 (Southern): 41
- Region 5 (Pacific SW): 35

**Decision:** Accept as missing. Too few to warrant polygon-mean extraction.

---

## Version History

| Version | Date | Changes |
|---------|------|---------|
| 1.0 | 2025-01-31 | Initial extraction complete |
| 1.1 | 2025-02-03 | Merged with IDS data, scale factors applied |
