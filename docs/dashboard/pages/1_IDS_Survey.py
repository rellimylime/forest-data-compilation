# ==============================================================================
# pages/1_IDS_Survey.py
# IDS Aerial Detection Survey — data explorer
# ==============================================================================

import os
import sys
from pathlib import Path

import pandas as pd
import streamlit as st

sys.path.insert(0, str(Path(__file__).parent.parent))
from utils import (
    apply_dark_css, metric_card, dark_fig, scatter_geo_usa,
    load_parquet, parquet_meta, load_csv, repo_path, PLOTLY_AVAILABLE
)

st.set_page_config(page_title="IDS Survey", page_icon="🗺️", layout="wide")
apply_dark_css()

if PLOTLY_AVAILABLE:
    import plotly.express as px

st.title("🗺️ IDS Aerial Detection Survey")
st.markdown(
    "USDA Forest Service **Insect and Disease Detection Survey** (IDS) — "
    "annual aerial detection surveys covering 1997–2024 across 10 National Forest regions. "
    "Source: [USDA Forest Health Protection](https://www.fs.usda.gov/science-technology/data-tools-products/fhp-mapping-reporting/detection-surveys)"
)

# ------------------------------------------------------------------------------
# Paths
# ------------------------------------------------------------------------------

IDS_DIR     = repo_path("01_ids")
LOOKUP_DIR  = IDS_DIR / "lookups"
IDS_GPK     = IDS_DIR / "data" / "processed" / "ids_layers_cleaned.gpkg"
PIXEL_MAP   = repo_path("02_terraclimate", "data", "processed",
                         "pixel_maps", "damage_areas_pixel_map.parquet")

tab_summary, tab_dca, tab_host, tab_schema, tab_map = st.tabs([
    "📊 Summary",
    "🐛 DCA Codes",
    "🌲 Host Codes",
    "📋 Schema",
    "🗺️ Coverage Map",
])

# ==============================================================================
# TAB 1 — SUMMARY
# ==============================================================================
with tab_summary:
    st.subheader("Dataset Overview")

    c1, c2, c3, c4 = st.columns(4)
    c1.markdown(metric_card("Damage Areas",   "4.4M",  "1997–2024"),    unsafe_allow_html=True)
    c2.markdown(metric_card("Damage Points",  "1.2M",  "point observations"), unsafe_allow_html=True)
    c3.markdown(metric_card("Surveyed Areas", "74.5K", "survey footprints"), unsafe_allow_html=True)
    c4.markdown(metric_card("FS Regions",     "10",    "CONUS + AK + HI"), unsafe_allow_html=True)

    st.markdown("---")
    st.markdown(
        "The IDS is the backbone of this pipeline — it provides the **observation units** "
        "(damage polygons and points) for which climate data is extracted.\n\n"
        "All damage areas are stored in a single cleaned GeoPackage with three layers:\n\n"
        "| Layer | Description | Key columns |\n"
        "|-------|-------------|-------------|\n"
        "| `damage_areas` | Mapped disturbance polygons | `DAMAGE_AREA_ID`, `DCA_CODE`, `HOST_CODE`, `YEAR`, `geometry` |\n"
        "| `damage_points` | Point observations (no polygon) | `OBSERVATION_ID`, `DCA_CODE`, `HOST_CODE`, `YEAR`, `geometry` |\n"
        "| `surveyed_areas` | Annual survey footprints | `SURVEY_ID`, `YEAR`, `geometry` |\n"
    )

    gpk_exists = IDS_GPK.is_file()
    st.markdown(
        f"**GeoPackage:** `{IDS_GPK}`  \n"
        f"{'✅ File exists' if gpk_exists else '❌ Not found — run 01_ids scripts to generate'}"
    )

    st.markdown("---")
    st.subheader("Key Codes")
    st.markdown(
        "Each damage record has a **DCA code** (Damage Causal Agent — what caused the damage) "
        "and a **HOST code** (the tree species affected). "
        "Browse the full lookups in the adjacent tabs."
    )
    st.markdown(
        "| Code | Description |\n"
        "|------|-------------|\n"
        "| DCA 11000–11999 | Mountain pine beetle and other bark beetles |\n"
        "| DCA 11006 | Mountain pine beetle (MPB) — the primary study organism |\n"
        "| DCA 10000–10999 | Spruce budworm and other defoliators |\n"
        "| DCA 20000+ | Disease agents |\n"
        "| DCA 30000+ | Abiotic damage |\n"
    )

    st.markdown("---")
    st.subheader("Derived Products")
    dm_path = repo_path("processed", "ids", "damage_area_to_surveyed_area.parquet")
    am_path = repo_path("processed", "ids", "damage_area_area_metrics.parquet")
    for p, label, desc in [
        (dm_path, "damage_area_to_surveyed_area.parquet",
         "Spatial assignment — each damage area matched to its survey footprint"),
        (am_path, "damage_area_area_metrics.parquet",
         "Area metrics — damage area size (ha), pct of surveyed area covered"),
    ]:
        exists = p.is_file()
        m = parquet_meta(str(p)) if exists else {}
        rows_str = f"{m['rows']:,}" if m.get("rows") else "—"
        size_str = f"{m['size_mb']:.1f} MB" if m.get("size_mb") else "—"
        st.markdown(
            f"{'✅' if exists else '❌'} `{label}` — {desc}  \n"
            f"&nbsp;&nbsp;&nbsp;{rows_str} rows · {size_str}"
        )

# ==============================================================================
# TAB 2 — DCA CODE LOOKUP
# ==============================================================================
with tab_dca:
    st.subheader("DCA Code Lookup")
    st.markdown(
        "Damage Causal Agent (DCA) codes identify the insect, disease, or abiotic agent "
        "responsible for each damage area. Stored in `01_ids/lookups/dca_code_lookup.csv`."
    )

    dca_path = str(LOOKUP_DIR / "dca_code_lookup.csv")
    dca_df, dca_err = load_csv(dca_path)

    if dca_df is None:
        st.warning(f"DCA lookup not found: `{dca_path}`")
        st.markdown("Expected columns: `DCA_CODE`, `LABEL`, `CATEGORY`, `DESCRIPTION`")
    else:
        # Search filter
        search = st.text_input("Search DCA codes, labels, or categories", key="dca_search")
        if search:
            mask = dca_df.astype(str).apply(
                lambda col: col.str.contains(search, case=False, na=False)
            ).any(axis=1)
            display_df = dca_df[mask]
        else:
            display_df = dca_df

        st.caption(f"{len(display_df):,} of {len(dca_df):,} records shown")
        st.dataframe(display_df, use_container_width=True, hide_index=True)

        # Category breakdown if column exists
        cat_col = next((c for c in dca_df.columns
                        if c.lower() in ("category", "group", "type")), None)
        if cat_col and PLOTLY_AVAILABLE:
            st.markdown("---")
            cat_counts = dca_df[cat_col].value_counts().reset_index()
            cat_counts.columns = [cat_col, "count"]
            fig = px.bar(
                cat_counts, x=cat_col, y="count",
                labels={cat_col: "Category", "count": "Number of DCA codes"},
                color_discrete_sequence=["#4e79a7"],
            )
            from utils import dark_fig as _dark_fig
            st.plotly_chart(_dark_fig(fig), use_container_width=True)

# ==============================================================================
# TAB 3 — HOST CODE LOOKUP
# ==============================================================================
with tab_host:
    st.subheader("Host Code Lookup")
    st.markdown(
        "Host codes identify the tree species (or genus group) affected by each damage area. "
        "Stored in `01_ids/lookups/host_code_lookup.csv`."
    )

    host_path = str(LOOKUP_DIR / "host_code_lookup.csv")
    host_df, host_err = load_csv(host_path)

    if host_df is None:
        st.warning(f"Host lookup not found: `{host_path}`")
        st.markdown("Expected columns: `HOST_CODE`, `COMMON_NAME`, `SCIENTIFIC_NAME`, `GROUP`")
    else:
        search_h = st.text_input("Search host codes, common names, or scientific names",
                                  key="host_search")
        if search_h:
            mask = host_df.astype(str).apply(
                lambda col: col.str.contains(search_h, case=False, na=False)
            ).any(axis=1)
            display_h = host_df[mask]
        else:
            display_h = host_df

        st.caption(f"{len(display_h):,} of {len(host_df):,} records shown")
        st.dataframe(display_h, use_container_width=True, hide_index=True)

# ==============================================================================
# TAB 4 — SCHEMA
# ==============================================================================
with tab_schema:
    st.subheader("Layer Schema")
    st.markdown(
        "Column descriptions for each GeoPackage layer. These are the raw cleaned "
        "fields from the merged 10-region IDS geodatabases."
    )

    LAYER_SCHEMA = {
        "damage_areas": [
            ("DAMAGE_AREA_ID",  "Unique polygon ID (large_string) — primary key across all pipeline outputs", "large_string"),
            ("OBSERVATION_ID",  "Original observation identifier from source GDB", "str"),
            ("DCA_CODE",        "Damage Causal Agent code (see DCA lookup tab)", "int"),
            ("HOST_CODE",       "Affected host species code (see Host lookup tab)", "int"),
            ("YEAR",            "Survey year (calendar year)", "int"),
            ("ACRES",           "Mapped area in acres (from GDB attribute)", "float"),
            ("REGION",          "USFS region code (1–10)", "int"),
            ("STATE",           "State FIPS or abbreviation", "str"),
            ("geometry",        "Polygon geometry (EPSG:5070 Conus Albers Equal Area)", "geometry"),
        ],
        "damage_points": [
            ("OBSERVATION_ID",  "Unique point ID — primary key", "large_string"),
            ("DCA_CODE",        "Damage Causal Agent code", "int"),
            ("HOST_CODE",       "Affected host species code", "int"),
            ("YEAR",            "Survey year", "int"),
            ("REGION",          "USFS region code", "int"),
            ("STATE",           "State abbreviation", "str"),
            ("geometry",        "Point geometry (EPSG:5070)", "geometry"),
        ],
        "surveyed_areas": [
            ("SURVEY_ID",       "Unique survey footprint ID", "large_string"),
            ("YEAR",            "Survey year", "int"),
            ("REGION",          "USFS region code", "int"),
            ("STATE",           "State abbreviation", "str"),
            ("geometry",        "Polygon geometry — annual survey boundary", "geometry"),
        ],
    }

    layer_sel = st.selectbox("Layer", list(LAYER_SCHEMA.keys()), key="ids_layer_sel")
    cols_data = LAYER_SCHEMA[layer_sel]
    schema_df = pd.DataFrame(cols_data, columns=["Column", "Description", "Type"])
    st.dataframe(schema_df, use_container_width=True, hide_index=True)

    st.markdown("---")
    st.markdown("**Load in R:**")
    st.code(
        'library(sf)\n'
        'gpkg <- "01_ids/data/processed/ids_layers_cleaned.gpkg"\n'
        '\n'
        '# Read damage areas — optionally filter by SQL\n'
        'damage_areas <- st_read(gpkg, layer = "damage_areas",\n'
        '  query = "SELECT * FROM damage_areas WHERE DCA_CODE = 11006 AND YEAR >= 2010")\n'
        '\n'
        'surveyed_areas <- st_read(gpkg, layer = "surveyed_areas")',
        language="r",
    )
    st.markdown("**Load in Python:**")
    st.code(
        'import geopandas as gpd\n'
        'damage_areas = gpd.read_file(\n'
        '    "01_ids/data/processed/ids_layers_cleaned.gpkg",\n'
        '    layer="damage_areas"\n'
        ')',
        language="python",
    )

# ==============================================================================
# TAB 5 — COVERAGE MAP
# ==============================================================================
with tab_map:
    st.subheader("TerraClimate Pixel Grid — IDS Coverage")
    st.markdown(
        "Each damage area is decomposed into the 4km TerraClimate pixels it overlaps. "
        "The map below shows pixel centroids from "
        "`02_terraclimate/data/processed/pixel_maps/damage_areas_pixel_map.parquet` "
        "(sampled for display speed)."
    )

    pm_path = str(PIXEL_MAP)
    pm_exists = os.path.isfile(pm_path)

    if not pm_exists:
        st.info(
            "Pixel map not found. Run `02_terraclimate/scripts/02_build_pixel_maps.R` "
            "to generate it."
        )
        # Fallback: show FIA site pixel map which is tiny
        fia_pm = str(repo_path("05_fia", "data", "processed", "site_climate",
                                "fia_site_pixel_map.parquet"))
        if os.path.isfile(fia_pm):
            st.markdown("**Showing FIA site pixel map as a proxy** (6,956 points):")
            pm_df, pm_err = load_parquet(fia_pm)
            if pm_df is not None and PLOTLY_AVAILABLE:
                fig = px.scatter_mapbox(
                    pm_df.rename(columns={"y": "lat", "x": "lon"}),
                    lat="lat", lon="lon",
                    color_discrete_sequence=["#4e79a7"],
                    mapbox_style="open-street-map",
                    zoom=3, center={"lat": 44, "lon": -105},
                    opacity=0.7,
                    title="FIA site pixel centroids (4km TerraClimate grid)",
                )
                fig.update_traces(marker_size=5)
                fig.update_layout(paper_bgcolor="#0e1117", font_color="#ddd",
                                  margin=dict(l=0, r=0, t=30, b=0))
                st.plotly_chart(fig, use_container_width=True)
    else:
        meta = parquet_meta(pm_path)
        n_total = meta.get("rows", 0) or 0
        sample_n = min(30_000, n_total)

        st.markdown(
            f"Pixel map has **{n_total:,} rows** (pixel × damage area pairs). "
            f"Showing a random sample of **{sample_n:,}** pixel centroids."
        )

        if not PLOTLY_AVAILABLE:
            st.warning("Install `plotly` to view the map.")
        else:
            pm_df, pm_err = load_parquet(pm_path)
            if pm_df is not None:
                # Get unique pixels only for the map
                pixel_cols = [c for c in ["pixel_id", "x", "y"] if c in pm_df.columns]
                if len(pixel_cols) == 3:
                    unique_pixels = pm_df[pixel_cols].drop_duplicates("pixel_id")
                    if len(unique_pixels) > sample_n:
                        unique_pixels = unique_pixels.sample(sample_n, random_state=42)

                    col_x = "x" if "x" in unique_pixels.columns else None
                    col_y = "y" if "y" in unique_pixels.columns else None

                    if col_x and col_y:
                        fig = px.scatter_mapbox(
                            unique_pixels,
                            lat=col_y, lon=col_x,
                            color_discrete_sequence=["#e15759"],
                            mapbox_style="open-street-map",
                            zoom=3, center={"lat": 44, "lon": -105},
                            opacity=0.5,
                            title=f"TerraClimate 4km pixel centroids (IDS coverage, n={len(unique_pixels):,})",
                        )
                        fig.update_traces(marker_size=3)
                        fig.update_layout(paper_bgcolor="#0e1117", font_color="#ddd",
                                          margin=dict(l=0, r=0, t=30, b=0))
                        st.plotly_chart(fig, use_container_width=True)
                        st.caption(
                            "Each dot = one unique 4km TerraClimate pixel that overlaps "
                            "at least one IDS damage area. The pixel decomposition assigns "
                            "climate values to damage areas via these shared pixels."
                        )
                else:
                    st.warning(f"Expected columns `pixel_id, x, y`. Found: {pm_df.columns.tolist()}")
            elif pm_err:
                st.error(pm_err)
