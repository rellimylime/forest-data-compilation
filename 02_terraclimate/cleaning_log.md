# TerraClimate Extraction Log

**Dataset:** TerraClimate extracted at IDS observation locations
**Data Manager:** Emily Miller
**Institution:** UCSB, Bren School, Landscapes of Change Lab
**Log Created:** 2026-01-31
**Last Updated:** 2026-02-13

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

## Data Quality Issues

Issues identified during TerraClimate extraction and processing that affect the current workflow (v2.0).

### Issue #001: Raw Values Require Scaling

**Date identified:** 2025-01-31
**Fields affected:** All 14 climate variables

**Description:**
TerraClimate stores values as integers for storage efficiency. Each variable has a scale factor that must be applied to convert to physical units.

**Example:**
- Raw tmmx value: 254
- Scale factor: 0.1
- Physical value: 25.4°C

**Decision:** Scale factors are applied during extraction ([02_extract_terraclimate.R](scripts/02_extract_terraclimate.R)). Output parquet files contain values in physical units. Scale factors are defined in `config.yaml`.

**Impact:** Output data is immediately usable; users do not need to apply scale factors.

---

### Issue #002: Coastal/Edge NoData Pixels

**Date identified:** 2025-01-31
**Records affected:** ~1,200 observations (0.03%)

**Description:**
TerraClimate has NoData values over oceans and at dataset edges. IDS observations near coastlines may overlap NoData pixels, resulting in missing climate data.

**Distribution by region:**
- Region 10 (Alaska): ~700 observations (most affected, dataset edges)
- Region 6 (Pacific NW): ~270 observations (coastal)
- Region 9 (Eastern): ~150 observations (coastal)
- Other regions: <50 observations each

**Decision:** Accepted as missing data. The pixel decomposition workflow maps all overlapping pixels, so coastal observations may have partial coverage (some pixels valid, some NoData).

**Mitigation:** Summaries output includes `n_pixels_with_data` diagnostic. Users can filter observations with insufficient pixel coverage.

**Impact:** Small coastal or edge-proximal observations may have no valid climate data. Check `n_pixels_with_data` column in summaries.

---

### Issue #003: TerraClimate Temporal Lag

**Date identified:** 2025-01-31
**Potential impact:** Recent years (2024+)

**Description:**
TerraClimate data release lags by several months to over a year behind real-time. At extraction time (2026-02), 2024 data was available and extracted successfully.

**Decision:** Use available data as-is. If future analysis reveals data quality issues for the most recent year, consider using prior year as proxy.

**Impact:** Most recent year may be provisional or subject to revision. Check TerraClimate data version notes if using for time-sensitive applications.

---

### Issue #004: Flux Variables Need Annual Summation

**Date identified:** 2025-01-31
**Variables affected:** pr, aet, pet, def, ro, soil (flux variables)

**Description:**
Flux variables (precipitation, evapotranspiration, runoff, deficit, soil moisture) are monthly accumulations. For annual totals, these should be summed across 12 months, not averaged.

**Decision:** The current workflow preserves individual monthly values in long format. Users calculate annual totals as needed:

```r
# Annual precipitation total (correct):
annual_pr <- pixel_values %>%
  filter(variable == "pr") %>%
  group_by(pixel_id, calendar_year) %>%
  summarize(annual_total = sum(value, na.rm = TRUE))

# NOT this (would be mean monthly, not annual total):
annual_pr_wrong <- summarize(annual_mean = mean(value))  # INCORRECT for flux vars
```

**Impact:** Users must be aware of flux vs. state variable distinction when aggregating.

---

## Design Decisions

Key decisions made during workflow development that affect data structure and usage.

| Decision | Rationale | Date |
|----------|-----------|------|
| **Pixel decomposition** (not centroid sampling) | Preserves within-polygon climate variation; critical for large damage areas | 2026-02-05 |
| **Monthly values preserved** (not annual means) | Enables seasonal analysis; users aggregate as needed | 2026-02-05 |
| **Two-table architecture** (pixel maps + pixel values) | Efficient storage; handles pancake features; enables weighted means | 2026-02-05 |
| **GEE extraction** (not NetCDF download) | No local storage needed; direct pixel sampling; free access | 2026-02-05 |
| **Parquet format** | Efficient columnar storage; fast filtering by year/month/variable | 2026-02-05 |
| **Scale factors applied during extraction** | Values immediately usable in physical units | 2026-02-05 |
| **exactextractr for polygon-pixel mapping** | Provides coverage_fraction for proper area weighting | 2026-02-05 |
| **Monthly stacking in GEE** | 12x extraction efficiency improvement (one API call per year vs. per month) | 2026-02-10 |
| **Both calendar and water year retained** | Different analyses need different time bases; no forced conversion | 2026-02-06 |

---

## Performance Notes

### Extraction Efficiency

**Monthly Stacking Optimization:**
- Original approach: 12 separate GEE `sampleRegions()` calls per year (one per month)
- Optimized approach: Stack all 12 months into single 168-band image (14 variables × 12 months)
- **Result:** ~12x speedup, reduces GEE quota consumption

**Batch Size:**
- Default: 2,500 pixels per GEE request
- 168 bands × 2,500 pixels = ~420,000 values per request
- If GEE timeouts occur, reduce batch size to 1,500-2,000

### Data Volume

| Component | Size |
|-----------|------|
| Pixel maps (3 IDS layers) | ~150 MB total |
| Pixel values (yearly parquet, wide) | ~50-100 MB per year |
| Pixel values (long format, all years) | ~2-3 GB |
| Summaries (observation-level means) | ~500 MB |

---

## Known Limitations

1. **Spatial resolution:** ~4km pixels are coarse for small IDS observations (<50 ha). Use PRISM (800m) for finer spatial detail.

2. **Temporal resolution:** Monthly data may miss short-duration climate events. Use ERA5 (daily) for event-based analysis.

3. **NoData at coastlines/edges:** ~1,200 observations lack climate data due to proximity to ocean or dataset boundary.

4. **Degenerate geometries:** IDS observations with invalid geometries (slivers, self-intersections) may produce no pixel mappings or very low coverage_fraction.

5. **Recent year data quality:** Most recent year (2024 at time of extraction) may be provisional. Check TerraClimate release notes.

---

## Version History

| Version | Date | Changes |
|---------|------|---------|
| 1.0 | 2025-01-31 | Initial centroid-based extraction (CSV output, annual means) — deprecated |
| 2.0 | 2026-02-05 | Pixel decomposition workflow (parquet output, monthly values, coverage fractions) |
| 2.1 | 2026-02-10 | Added monthly stacking optimization for GEE extraction efficiency |

**Note:** Version 1.0 (centroid extraction) is fully deprecated. Historical v1.0 issues have been archived. For v1.0 documentation, see git history (commits before 2026-02-05).
