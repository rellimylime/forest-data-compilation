# TerraClimate Extraction Log

**Dataset:** TerraClimate extracted at IDS observation locations
**Data Manager:** Emily Miller
**Institution:** UCSB, Bren School, Landscapes of Change Lab
**Log Created:** 2026-01-31
**Last Updated:** 2026-02-10

---

## Dataset Overview

- **Source:** TerraClimate via Google Earth Engine
- **GEE Asset:** IDAHO_EPSCOR/TERRACLIMATE
- **Native Resolution:** ~4km (1/24th degree)
- **Temporal Resolution:** Monthly (all 12 months preserved per year)
- **Variables Extracted:** 14 climate/water balance variables (see `data_dictionary.csv`)
- **Extraction Method:** Pixel decomposition via exactextractr (polygons) and cellFromXY (points)
- **Output Format:** Parquet files (pixel maps + yearly pixel value files)

---

## Current Workflow Issues

Issues relevant to the current pixel decomposition workflow (v2.0).

### Issue #001: Raw Values Require Scaling

**Date identified:** 2025-01-31
**Fields affected:** All 14 climate variables

**Description:**
TerraClimate stores values as integers for storage efficiency. Each variable has a scale factor that must be applied to convert to physical units (e.g., tmmx raw value 254 x 0.1 = 25.4 degrees C).

**Resolution:** Scale factors are applied during extraction (02_extract_terraclimate.R). Output parquet files contain values in physical units. Scale factors are defined in config.yaml.

---

### Issue #004: TerraClimate Temporal Lag

**Date identified:** 2025-01-31
**Potential impact:** 2024 data

**Description:**
TerraClimate data release lags by several months to over a year. 2024 data was available and extracted successfully at time of extraction.

**Resolution:** 2024 extraction succeeded. If future analysis reveals data quality issues for 2024, consider using 2023 climate as proxy.

---

### Issue #005: Coastal/Edge NoData Pixels

**Date identified:** 2025-01-31

**Description:**
TerraClimate has NoData values over oceans and at dataset edges. IDS observations near coastlines may overlap NoData pixels.

**Resolution:** Quantified in Issue #010. Accepted as missing. The pixel decomposition workflow reports `n_pixels_with_data` in summaries, making it easy to identify observations with partial coverage.

---

## Historical Issues (v1.0 Centroid Extraction)

Issues from the original centroid-based extraction (v1.0), which sampled a single
point per observation. Many of these are no longer applicable to the current
pixel decomposition workflow (v2.0), which uses exactextractr for polygon-pixel
mapping. Retained here for the historical record.

### Issue #002: Annual Means vs Annual Totals

**Date identified:** 2025-01-31
**Fields affected:** pr, aet, pet, def, ro (flux variables)

**Description:**
The v1.0 extraction calculated the mean of 12 monthly values. For flux variables (precipitation, evapotranspiration, runoff, deficit), the annual total is more scientifically meaningful.

**Status (v2.0):** No longer applicable. The current workflow preserves individual monthly values. Users can sum flux variables across months as needed for their analysis.

---

### Issue #003: Invalid Centroid Coordinates

**Date identified:** 2025-01-31
**Records affected:** 10 out of 4,475,827 (0.0002%)

**Description:**
Ten IDS observations produced NaN coordinates when computing centroids with `st_point_on_surface()`. Likely degenerate geometries (slivers, self-intersecting polygons).

**Status (v2.0):** The pixel decomposition workflow uses exactextractr, which handles degenerate geometries differently. These observations may now produce valid pixel mappings (with very small coverage_fraction) or may still be excluded. To be verified when pixel maps are built.

---

### Issue #006: st_point_on_surface Warning

**Date identified:** 2025-01-31
**Type:** Warning (not error)

**Description:**
R generates a warning when using `st_point_on_surface()` on geodetic (lat/lon) coordinates.

**Status (v2.0):** No longer applicable. The current workflow does not use `st_point_on_surface()`.

---

### Issue #007: Duplicate OBSERVATION_IDs in TerraClimate Output

**Date identified:** 2025-02-03
**Records affected:** 3,499 duplicate pairs (6,998 rows total)

**Description:**
v1.0 extraction produced duplicate rows for ~3,499 OBSERVATION_IDs within the same region-year batch. Caused by off-by-one error at sub-batch boundaries (features at positions 5000, 10000, etc. extracted twice).

**Status (v2.0):** No longer applicable. The current workflow extracts at unique pixel coordinates (not per observation), so this class of duplication cannot occur.

---

### Issue #008: NA OBSERVATION_IDs in Region 9, 2024

**Date identified:** 2025-02-03
**Records affected:** 15

**Description:**
15 rows from Region 9, Year 2024 batch had NA OBSERVATION_IDs. The ID column was not passed through GEE correctly for these features.

**Status (v2.0):** No longer applicable. The current workflow does not pass OBSERVATION_IDs through GEE; it extracts at pixel coordinates and joins back via the pixel map.

---

### Issue #009: Join Type Mismatch

**Date identified:** 2025-02-03
**Impact:** 896,929 false NA matches initially

**Description:**
Original merge joined on OBSERVATION_ID, REGION_ID, and SURVEY_YEAR. TerraClimate CSVs stored REGION_ID and SURVEY_YEAR as numeric (double), while IDS geopackage stored them as integer, causing join failures.

**Status (v2.0):** No longer applicable. The current workflow joins via pixel_id (integer) in both pixel maps and pixel values. No type mismatch possible.

---

### Issue #010: Missing Climate Data (NoData Pixels)

**Date identified:** 2025-02-03
**Records affected (v1.0):** 1,235 (0.03%)

**Description:**
1,235 IDS observations had no climate data because their centroids fell in TerraClimate NoData pixels (ocean, dataset edges).

**Distribution by region:**
- Region 10 (Alaska): 694
- Region 6 (Pacific NW): 269
- Region 9 (Eastern): 152
- Region 2 (Rocky Mtn): 44
- Region 8 (Southern): 41
- Region 5 (Pacific SW): 35

**Status (v2.0):** Partially mitigated. The pixel decomposition workflow maps all overlapping pixels per observation. A polygon whose centroid fell in NoData may still have coverage from valid neighboring pixels. However, very small or coastal observations may still have no valid pixels. The summaries output includes `n_pixels_with_data` to flag these cases.

---

## Version History

| Version | Date | Changes |
|---------|------|---------|
| 1.0 | 2025-01-31 | Initial centroid-based extraction (CSV output, annual means) |
| 1.1 | 2025-02-03 | Merged with IDS data, scale factors applied |
| 2.0 | 2026-02-05 | Replaced with pixel decomposition workflow (parquet output, monthly values, coverage fractions) |
