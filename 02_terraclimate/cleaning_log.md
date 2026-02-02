# TerraClimate Extraction Log

**Dataset:** TerraClimate extracted at IDS observation centroids  
**Data Manager:** Emily Miller  
**Institution:** UCSB, Bren School - MEDS Program  
**Log Created:** 2025-01-31  
**Last Updated:** 2025-01-31  

---

## Dataset Overview

- **Source:** TerraClimate via Google Earth Engine
- **GEE Asset:** IDAHO_EPSCOR/TERRACLIMATE
- **Native Resolution:** ~4km (1/24th degree)
- **Temporal Resolution:** Monthly → aggregated to annual means
- **Variables Extracted:** 14 climate/water balance variables
- **Extraction Method:** Point sampling at IDS polygon centroids
- **Total Observations:** 4,475,817
- **Output Format:** CSV files (one per region-year batch)

---

## Extraction Methodology

### Why Point Extraction (Not Raster Download)?

TerraClimate is a global raster dataset (~4km resolution). Downloading raw tiles for the entire US would require:
- ~500+ GB of data
- Complex raster processing pipeline
- Substantial storage and compute resources

Instead, we used Google Earth Engine to extract values **only at the locations we need** (IDS observation centroids). This is:
- Fast (~25 minutes for 4.5M points)
- Storage-efficient (~500 MB of CSVs vs 500+ GB of rasters)
- Directly joinable to IDS data via OBSERVATION_ID

### Extraction Steps

1. Load IDS polygon for each observation
2. Compute centroid using `st_point_on_surface()` (guarantees point inside polygon)
3. Transform to WGS84 (EPSG:4326) to match TerraClimate CRS
4. Query TerraClimate for SURVEY_YEAR (Jan 1 to Dec 31)
5. Calculate annual mean across 12 monthly images
6. Extract pixel value at centroid using `sampleRegions(scale=4000)`
7. Save to CSV with OBSERVATION_ID as join key

### Batching Strategy

- **Primary batches:** REGION_ID × SURVEY_YEAR (251 combinations)
- **Sub-batches:** 5000 features max per GEE request (API limit)
- **Resumable:** Script checks for existing CSVs and skips completed batches

---

## Data Quality Issues

### Issue #001: Raw Values Require Scaling

**Date identified:** 2025-01-31  
**Fields affected:** All 14 climate variables  

**Description:**  
TerraClimate stores values as integers for storage efficiency. Each variable has a scale factor that must be applied to convert to physical units.

| Variable | Scale Factor | Raw Example | Scaled Value | Units |
|----------|--------------|-------------|--------------|-------|
| tmmx | 0.1 | 254 | 25.4 | °C |
| tmmn | 0.1 | 89 | 8.9 | °C |
| pr | 1 | 45 | 45 | mm |
| pet | 0.1 | 1200 | 120.0 | mm |
| vpd | 0.01 | 850 | 8.5 | kPa |
| pdsi | 0.01 | -150 | -1.50 | unitless |

**Decision:** Keep raw values in extraction CSVs. Apply scale factors during merge/processing step using `apply_terraclimate_scales()` function.

---

### Issue #002: Annual Means vs Annual Totals

**Date identified:** 2025-01-31  
**Fields affected:** pr, aet, pet, def, ro (flux variables)  

**Description:**  
Extraction calculates the **mean** of 12 monthly values. For some variables, the annual **total** is more scientifically meaningful:
- Precipitation (pr): Annual total rainfall is standard
- Evapotranspiration (aet, pet): Often reported as annual flux
- Runoff (ro): Cumulative annual value

For other variables, annual mean is appropriate:
- Temperature (tmmx, tmmn): Mean annual temperature
- Soil moisture (soil): Mean state variable
- Drought index (pdsi): Mean annual conditions

**Decision:** Document this clearly. Processing scripts should multiply flux variables by 12 to get annual totals if needed for analysis.

---

### Issue #003: Invalid Centroid Coordinates

**Date identified:** 2025-01-31  
**Records affected:** 10 out of 4,475,827 (0.0002%)  

**Description:**  
Ten IDS observations produced NaN coordinates when computing centroids with `st_point_on_surface()`. These were likely degenerate geometries (e.g., slivers, self-intersecting polygons) that passed `st_is_valid()` but failed centroid computation.

**Console output:**
```
Removing 10 features with invalid coordinates...
```

**Decision:** Exclude these 10 observations from extraction. They can be identified by missing OBSERVATION_IDs in the TerraClimate output when joined to IDS data.

---

### Issue #004: TerraClimate Temporal Lag

**Date identified:** 2025-01-31  
**Potential impact:** 2024 data  

**Description:**  
TerraClimate data release lags by several months to over a year. As of extraction date, 2024 data appeared to be available and extracted successfully.

**Verification:**
```r
# Test extraction for 2024
tc_2024 <- get_terraclimate_annual(2024, "pr", ee)
# Result: pr = 49.83 (valid value, not NULL)
```

**Decision:** 2024 extraction succeeded. If future analysis reveals data quality issues for 2024, consider using 2023 climate as proxy or checking for data updates.

---

### Issue #005: Coastal/Edge NoData Pixels

**Date identified:** 2025-01-31  
**Potential impact:** Unknown (not quantified)  

**Description:**  
TerraClimate has NoData values over oceans and at dataset edges. IDS observations near coastlines (especially Alaska, Hawaii, Pacific Northwest) may fall in NoData pixels if the centroid lands slightly offshore or in an unmapped area.

**Symptoms:** NULL or NA values for all climate variables for specific observations.

**Decision:** Check for systematic missingness in coastal regions during merge step. If significant, consider using polygon-mean extraction instead of centroid-point extraction for affected regions.

---

### Issue #006: st_point_on_surface Warning

**Date identified:** 2025-01-31  
**Type:** Warning (not error)  

**Description:**  
R generates a warning when using `st_point_on_surface()` on geodetic (lat/lon) coordinates:
```
st_point_on_surface may not give correct results for longitude/latitude data
```

This occurs because the function uses planar geometry algorithms on spherical coordinates. At the scale of individual IDS polygons (typically <1km) and TerraClimate resolution (~4km), this introduces negligible error.

**Decision:** Ignore warning. The extracted climate values are at 4km resolution, so sub-kilometer centroid precision is not meaningful.

---

## Variables Extracted

| Variable | Description | Units | Scale Factor | Notes |
|----------|-------------|-------|--------------|-------|
| tmmx | Maximum temperature | °C | 0.1 | Monthly mean of daily max |
| tmmn | Minimum temperature | °C | 0.1 | Monthly mean of daily min |
| pr | Precipitation | mm | 1 | Monthly accumulation |
| srad | Shortwave radiation | W/m² | 0.1 | Downward surface flux |
| vs | Wind speed at 10m | m/s | 0.01 | Monthly mean |
| vap | Vapor pressure | kPa | 0.001 | Monthly mean |
| vpd | Vapor pressure deficit | kPa | 0.01 | Monthly mean |
| pet | Reference ET (Penman-Monteith) | mm | 0.1 | Monthly accumulation |
| aet | Actual evapotranspiration | mm | 0.1 | Monthly accumulation |
| def | Climate water deficit | mm | 0.1 | pet - aet |
| soil | Soil moisture | mm | 0.1 | Monthly mean |
| swe | Snow water equivalent | mm | 1 | Monthly mean |
| ro | Runoff | mm | 1 | Monthly accumulation |
| pdsi | Palmer Drought Severity Index | unitless | 0.01 | Monthly mean |

---

## Output Files

**Raw extraction CSVs:**
- Location: `02_terraclimate/data/raw/`
- Naming: `tc_r{REGION_ID}_{SURVEY_YEAR}.csv`
- Total files: 251
- Total size: ~500 MB

**Example file structure (tc_r10_2020.csv):**
```
OBSERVATION_ID,aet,def,pdsi,pet,pr,ro,soil,srad,swe,tmmn,tmmx,vap,vpd,vs,REGION_ID,SURVEY_YEAR
{083df988-...},379.67,55.92,-38.83,435.58,80.75,34.00,1091.75,1011.25,92.25,4.42,77.42,657.67,34.75,328.33,10,2020
```

---

## Processing Steps (Next Phase)

1. [ ] Combine all CSVs into single file
2. [ ] Apply scale factors to convert to physical units
3. [ ] Calculate derived variables (annual totals for flux variables)
4. [ ] Check for missing values (coastal NoData issue)
5. [ ] Join to IDS cleaned data on OBSERVATION_ID
6. [ ] Export merged dataset

---

## Performance Summary

| Metric | Value |
|--------|-------|
| Total features extracted | 4,475,817 |
| Features excluded (invalid centroids) | 10 |
| Total extraction time | ~25 minutes |
| Average rate | ~3,000 features/second |
| Output size | ~500 MB (251 CSV files) |
| Errors encountered | 0 |

---

## Version History

| Version | Date | Changes |
|---------|------|---------|
| 1.0 | 2025-01-31 | Initial extraction complete |