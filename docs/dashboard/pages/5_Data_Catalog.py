# ==============================================================================
# pages/5_Data_Catalog.py
# Data Catalog — every registered product, read from the product registry
# ==============================================================================
#
# This page holds no product list of its own. It renders the master inventory
# built by forest_explorer/catalog/build_inventory.py from the curated registry
# at forest_explorer/registry/products.yaml.
#
# The previous version kept its own hand-edited CATALOG dict. Nothing checked it
# against the data, so it drifted: 35 of its 49 entries pointed at paths that no
# longer existed, and 39 real products were missing entirely. Do not reintroduce
# a literal product list here — add to the registry and rerun the generator.

import json
import sys
from pathlib import Path

import pandas as pd
import streamlit as st

sys.path.insert(0, str(Path(__file__).parent.parent))
from utils import apply_dark_css, color_status, render_top_nav, repo_path

st.set_page_config(page_title="Data Catalog", page_icon="📋", layout="wide")
apply_dark_css()
render_top_nav()

INVENTORY_PATH = repo_path("forest_explorer/catalog/generated/inventory.json")
BUILD_CMD = "python forest_explorer/catalog/build_inventory.py"

ACCESS_LABELS = {
    "catalog_only": "Catalog only",
    "prepared_download": "Prepared download",
    "filtered_extract": "Filtered extract",
    "guided_combined": "Guided combination",
}
REVIEW_LABELS = {
    "certified": "Reviewed",
    "constrained": "Reviewed with limits",
    "domain_review_required": "Needs scientific review",
    "blocked_by_defect": "Blocked by a defect",
    "not_reviewed": "Not reviewed",
}
AVAILABILITY_ICON = {
    "available": "✅",
    "partial": "⚠️",
    "missing": "❌",
    "error": "🛑",
}


@st.cache_data(show_spinner=False)
def load_inventory(path: str, mtime: float) -> dict:
    """mtime is in the signature so the cache drops when the file is rebuilt."""
    with open(path, encoding="utf-8") as f:
        return json.load(f)


def human_bytes(n) -> str:
    if not n:
        return "—"
    n = float(n)
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if n < 1024 or unit == "TB":
            return f"{n:.0f} {unit}" if unit == "B" else f"{n:.1f} {unit}"
        n /= 1024.0
    return "—"


def load_code(product: dict) -> tuple[str, str]:
    """Build read snippets from the product's format, rather than storing them."""
    path, fmt = product["path"], product["format"]
    layer = product.get("gpkg_layer")

    if fmt in ("parquet_file", "parquet_dataset"):
        r = (f'library(arrow)\n'
             f'ds <- open_dataset("{path}")\n'
             f'# Filter before collecting — these products can be large\n'
             f'df <- ds |> dplyr::collect()')
        py = (f'import pyarrow.dataset as ds\n'
              f'dataset = ds.dataset("{path}")\n'
              f'# Project columns and push filters before to_pandas()\n'
              f'df = dataset.to_table().to_pandas()')
    elif fmt == "gpkg_layer":
        r = (f'library(sf)\n'
             f'# Filter at read time — do not load the whole layer\n'
             f'x <- st_read("{path}", layer = "{layer}",\n'
             f'  query = "SELECT * FROM {layer} LIMIT 1000")')
        py = (f'import geopandas as gpd\n'
              f'gdf = gpd.read_file("{path}",\n'
              f'    layer="{layer}", rows=1000)  # filter at read time')
    elif fmt == "csv":
        r = f'df <- read.csv("{path}")'
        py = f'df = pd.read_csv("{path}")'
    else:
        r = f'# Collection of files under {path}'
        py = f'# Collection of files under {path}'
    return r, py


# ------------------------------------------------------------------------------
# Load
# ------------------------------------------------------------------------------

st.title("📋 Data Catalog")

if not INVENTORY_PATH.is_file():
    st.error(
        "No product inventory found. This page renders "
        "`forest_explorer/catalog/generated/inventory.json`, which is built from "
        "the product registry."
    )
    st.code(BUILD_CMD, language="bash")
    st.stop()

inv = load_inventory(str(INVENTORY_PATH), INVENTORY_PATH.stat().st_mtime)
products = inv["products"]
families = inv["families"]

st.markdown(
    "Every registered data product: what one row means, what identifies it, what "
    "the explorer may do with it, and whether it is actually present."
)
st.caption(
    f"Inventory generated {inv['generated_at'][:19].replace('T', ' ')} UTC · "
    f"data root `{inv['environment_label']}` · registry v{inv['registry_version']} · "
    f"rebuild with `{BUILD_CMD}`"
)

# ------------------------------------------------------------------------------
# Summary
# ------------------------------------------------------------------------------

counts = {k: sum(1 for p in products if p["availability"] == k)
          for k in AVAILABILITY_ICON}
c1, c2, c3, c4 = st.columns(4)
c1.metric("Registered", len(products))
c2.metric("Present", counts["available"])
c3.metric("Grain unconfirmed", counts["partial"])
c4.metric("Not built here", counts["missing"] + counts["error"])

# ------------------------------------------------------------------------------
# Filters
# ------------------------------------------------------------------------------

search = st.text_input(
    "🔍 Search products — name, path, row meaning, caveats", key="catalog_search"
)

f1, f2, f3 = st.columns(3)
with f1:
    fam_filter = st.multiselect(
        "Family", options=list(families),
        format_func=lambda k: families[k]["title"],
    )
with f2:
    access_filter = st.multiselect(
        "Access mode", options=list(ACCESS_LABELS),
        format_func=lambda k: ACCESS_LABELS[k],
    )
with f3:
    show_missing = st.checkbox("Include products not built here", value=True)


def matches(p: dict) -> bool:
    if fam_filter and p["family"] not in fam_filter:
        return False
    if access_filter and p["access_mode"] not in access_filter:
        return False
    if not show_missing and p["availability"] in ("missing", "error"):
        return False
    if search:
        blob = " ".join([
            p["title"], p["id"], p["path"], p["one_row_is"],
            " ".join(p.get("caveats") or []), p.get("notes") or "",
            " ".join(p.get("keys") or []),
        ]).lower()
        if search.lower() not in blob:
            return False
    return True


visible = [p for p in products if matches(p)]
st.caption(f"Showing {len(visible)} of {len(products)} products")

# ------------------------------------------------------------------------------
# Product cards, grouped by family
# ------------------------------------------------------------------------------

for fam_id, fam in families.items():
    fam_products = [p for p in visible if p["family"] == fam_id]
    if not fam_products:
        continue

    st.markdown(f"### {fam['title']}")
    st.caption(fam["blurb"])

    for p in fam_products:
        obs = p.get("observed") or {}
        icon = AVAILABILITY_ICON[p["availability"]]
        rows = f"{obs['n_rows']:,} rows" if obs.get("n_rows") else "—"
        header = (
            f"{icon} **{p['title']}** · `{p['path']}` · "
            f"{rows} · {human_bytes(obs.get('bytes'))}"
        )

        with st.expander(header):
            st.markdown(f"**One row is** {p['one_row_is']}.")

            m1, m2, m3 = st.columns(3)
            access = ACCESS_LABELS[p["access_mode"]]
            if p.get("intended_access_mode"):
                access += (f"  \n_held back pending review; intended as "
                           f"{ACCESS_LABELS[p['intended_access_mode']]}_")
            m1.markdown(f"**Access**  \n{access}")
            m2.markdown(f"**Review**  \n{REVIEW_LABELS[p['review_status']]}")
            key_check = p.get("key_check", {})
            verdict = {
                "unique": "✅ key is unique",
                "not_unique": "⚠️ key is NOT unique",
                "failed": "⚠️ key column missing",
                "not_checked": "— not checked",
                "not_applicable": "— no key declared",
            }.get(key_check.get("status"), "—")
            m3.markdown(f"**Grain check**  \n{verdict}")

            if p.get("keys"):
                st.markdown(
                    "**Identified by:** " + ", ".join(f"`{k}`" for k in p["keys"])
                )
            if key_check.get("status") in ("not_unique", "failed", "not_checked"):
                note = key_check.get("note")
                if note:
                    st.caption(note)

            detail = [f"**Product id:** `{p['id']}`", f"**Format:** {p['format']}"]
            if p.get("producer"):
                detail.append(f"**Built by:** `{p['producer']}`")
            if p.get("parent_grain"):
                detail.append(f"**Nests inside:** `{p['parent_grain']}`")
            if obs.get("partitions"):
                detail.append(f"**Partitions:** {len(obs['partitions'])} states")
            if obs.get("n_files"):
                detail.append(f"**Files:** {obs['n_files']}")
            st.markdown(" · ".join(detail))

            if p["availability"] in ("missing", "error"):
                if p.get("lifecycle") == "planned":
                    st.info(
                        "Planned — the pipeline exists but has not produced an "
                        "output in this data root. Listed so it is discoverable, "
                        "not because it can be extracted."
                    )
                else:
                    st.warning(
                        f"Expected here but not present — "
                        f"{p.get('reason', 'unknown')}. This product is marked "
                        f"`{p.get('lifecycle')}`, so its absence is a problem."
                    )

            if p.get("caveats"):
                st.markdown("**Before you use it**")
                for c in p["caveats"]:
                    st.markdown(f"- {c}")

            if p.get("facets"):
                st.markdown(
                    "**Filterable on:** " + ", ".join(f"`{c}`" for c in p["facets"])
                )

            if obs.get("columns"):
                st.markdown(f"**Schema** — {len(obs['columns'])} columns")
                st.dataframe(
                    pd.DataFrame(obs["columns"], columns=["Column", "Type"]),
                    use_container_width=True, hide_index=True,
                    height=min(400, 35 * len(obs["columns"]) + 40),
                )

            if p["availability"] not in ("missing", "error"):
                r_code, py_code = load_code(p)
                st.markdown("**Load code — R:**")
                st.code(r_code, language="r")
                st.markdown("**Load code — Python:**")
                st.code(py_code, language="python")

            if p.get("notes"):
                st.info(f"**Maintainer note.** {p['notes']}")

# ------------------------------------------------------------------------------
# Inventory table
# ------------------------------------------------------------------------------

st.markdown("---")
st.subheader("Full Inventory Table")

table = pd.DataFrame([{
    "Family": families[p["family"]]["title"],
    "Product": p["title"],
    "Status": AVAILABILITY_ICON[p["availability"]],
    "One row is": p["one_row_is"],
    "Rows": f"{(p.get('observed') or {}).get('n_rows'):,}"
            if (p.get("observed") or {}).get("n_rows") else "—",
    "Size": human_bytes((p.get("observed") or {}).get("bytes")),
    "Access": ACCESS_LABELS[p["access_mode"]],
    "Review": REVIEW_LABELS[p["review_status"]],
    "Path": p["path"],
} for p in visible])

st.dataframe(
    table.style.map(color_status, subset=["Status"]),
    use_container_width=True, hide_index=True,
)
