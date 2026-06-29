# FIA Site Climate Extraction Log

**Dataset:** TerraClimate extracted at FIA site locations
**Data Manager:** Emily Miller
**Institution:** UCSB, Bren School, Landscapes of Change Lab
**Log Created:** 2026-03-07
**Last Updated:** 2026-03-07

---

## Dataset Overview

- **Source:** TerraClimate via Google Earth Engine
- **GEE Asset:** IDAHO_EPSCOR/TERRACLIMATE
- **Native Resolution:** ~4km (1/24th degree)
- **Temporal Resolution:** Monthly (1958–present)
- **Variables Extracted:** tmmx, tmmn, pr, def, pet, aet
- **Extraction Method:** Global TC raster pixel snapping (`terra::cellFromXY()`)
- **Output Format:** Parquet files (site pixel map + site climate long format)

---

## Data Quality Issues

### Issue #001: Wrong pixel locations extracted due to region-limited reference raster

**Date identified:** 2026-03-07
**Records affected:** All 6,956 sites (100% of initial extraction)

**Description:**
The initial version of `scripts/site_climate/02_extract_terraclimate.R` used `build_pixel_map()` with a `ref_rast`
reconstructed from the IDS TerraClimate pixel_values parquets. Those parquets only cover the
IDS damage area footprint (predominantly Alaska and Pacific Northwest). FIA sites outside that
extent received garbage cell numbers from `terra::cellFromXY()`. By numeric coincidence, 339
of those garbage IDs matched real IDS pixel IDs, so `pixel_coords` was silently populated with
339 Alaska/PNW locations rather than the actual FIA site locations. GEE then faithfully
extracted climate data for those wrong pixels. No error was thrown at any step.

**Evidence:**
- GEE annual file x range: -165 to -112, y range: 48 to 67 (Alaska/PNW)
- Actual FIA sites include e.g. -107°, 38° (Colorado) — outside GEE output extent entirely
- Only 339 unique pixels extracted vs. 5,652 unique pixels expected

**Decision:**
Replaced `build_pixel_map()` + region-limited `ref_rast` with a global TerraClimate raster
constructed in memory (`rast(-180, 180, -90, 90, res=1/24)`). `pixel_id` is now the global
cell number, which is identical to the ID `extract_climate_from_gee()` embeds in its output,
making the consolidation join unambiguous. All `_gee_annual/` files were deleted and
re-extracted from GEE.

**Final output after fix:** 6,944 / 6,956 sites have climate data. The 12 missing sites are
international locations (Argentina, Australia, UK, Sweden, New Zealand) or coastal US sites
where TerraClimate has ocean/masked pixels — expected, not a bug.

**Impact:** All data from the initial extraction was incorrect and has been replaced.

---

## Design Decisions

| Decision | Rationale | Date |
|----------|-----------|------|
| Global TC raster for pixel snapping | `build_pixel_map()` with a region-limited `ref_rast` silently corrupts results for points outside that region. A global raster guarantees correct snapping for sites anywhere. | 2026-03-07 |
| 6 variables (tmmx, tmmn, pr, def, pet, aet) | Core temperature and water balance variables needed for disturbance risk modeling; subset of the 14 extracted for IDS | 2026-03-07 |
| 1958–present year range | TerraClimate begins in 1958; FIA sites need full historical record for pre-disturbance climate baselines | 2026-03-07 |

---

## Known Limitations

1. **Coordinate fuzz:** FIA coordinates are fuzzed ~1 mile for privacy, well within TerraClimate's ~4km pixel. Multiple nearby plots may map to the same pixel; this is expected.

2. **12 sites with no data:** International sites (Argentina, Australia, UK, Sweden, New Zealand) and a few coastal US sites fall on ocean pixels where TerraClimate returns no data.

3. **Temporal lag:** TerraClimate data lags ~1–2 years. At extraction time (2026-03), 2024 was the most recent complete year. 2025–2026 returned no data from GEE and are excluded.

---

## Version History

| Version | Date | Changes |
|---------|------|---------|
| 1.0 | 2026-03-05 | Initial extraction — incorrect (region-limited ref_rast; see Issue #001) |
| 2.0 | 2026-03-07 | Re-extraction with global TC raster; all sites correctly snapped |
