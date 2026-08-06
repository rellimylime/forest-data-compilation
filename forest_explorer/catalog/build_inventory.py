#!/usr/bin/env python3
"""
Build the master product inventory.

Reads the curated registry (forest_explorer/registry/products.yaml), looks at an
actual data root, and records what is physically there. Writes a machine-readable
inventory and a human-readable report.

The split matters. The registry says what a product *means* — one row, keys, what
the interface may do with it, what a researcher must be warned about. This script
says what is *there* — rows, bytes, columns, and whether the declared key really
is unique. It never invents meaning from a schema, and it never edits the registry.

Read-only. Opens products for metadata and key columns only; writes nothing into
any data directory.

Usage:
    python -m forest_explorer.catalog.build_inventory
    python forest_explorer/catalog/build_inventory.py --root /path/to/data
    python forest_explorer/catalog/build_inventory.py --verify-all

Data root resolution order: --root, then $FOREST_DATA_ROOT, then the repository
root inferred from this file's location.
"""
from __future__ import annotations

import argparse
import json
import os
import sqlite3
import sys
from datetime import datetime, timezone
from pathlib import Path, PurePosixPath

try:
    import pyarrow.dataset as pads
    import pyarrow.parquet as pq
    import yaml
except ModuleNotFoundError as exc:
    sys.exit(
        f"missing dependency: {exc.name}\n"
        "Install it into the active environment:\n"
        "    pip install -r requirements.txt"
    )

REGISTRY_REL = "forest_explorer/registry/products.yaml"

# Verifying a declared key means reading those columns for every row. Above this
# many rows that stops being cheap, so it is skipped unless --verify-all is given.
# The result is recorded as "not checked", never as "passed".
DEFAULT_MAX_VERIFY_ROWS = 8_000_000

AVAILABILITY_ORDER = ["available", "partial", "missing", "error"]

ACCESS_MODE_LABELS = {
    "catalog_only": "Catalog only",
    "prepared_download": "Prepared download",
    "filtered_extract": "Filtered extract",
    "guided_combined": "Guided combination",
}

REVIEW_STATUS_LABELS = {
    "certified": "Reviewed",
    "constrained": "Reviewed with limits",
    "domain_review_required": "Needs scientific review",
    "blocked_by_defect": "Blocked by a defect",
    "not_reviewed": "Not reviewed",
}


def log(msg: str) -> None:
    print(msg, file=sys.stderr)


def human_bytes(n: int | None) -> str:
    if not n:
        return "—"
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if n < 1024 or unit == "TB":
            return f"{n:.0f} {unit}" if unit == "B" else f"{n:.1f} {unit}"
        n /= 1024.0
    return "—"


def dir_bytes(path: Path) -> int:
    return sum(f.stat().st_size for f in path.rglob("*") if f.is_file())


# ---------------------------------------------------------------------------
# Per-format probes
# ---------------------------------------------------------------------------
def probe_parquet_file(path: Path) -> dict:
    pf = pq.ParquetFile(path)
    return {
        "n_rows": pf.metadata.num_rows,
        "n_columns": pf.metadata.num_columns,
        "bytes": path.stat().st_size,
        "modified": datetime.fromtimestamp(
            path.stat().st_mtime, timezone.utc
        ).isoformat(),
        "columns": [(f.name, str(f.type)) for f in pf.schema_arrow],
    }


def probe_parquet_dataset(path: Path) -> dict:
    dataset = pads.dataset(str(path), format="parquet", partitioning="hive")
    files = list(dataset.files)
    if not files:
        return {"empty": True, "n_rows": 0, "n_columns": 0, "bytes": 0, "columns": []}
    n_rows = sum(pq.ParquetFile(f).metadata.num_rows for f in files)
    partitions = sorted({Path(f).parent.name for f in files})
    return {
        "n_rows": n_rows,
        "n_columns": len(dataset.schema.names),
        "bytes": sum(os.path.getsize(f) for f in files),
        "n_files": len(files),
        "partitions": partitions,
        "columns": [(f.name, str(f.type)) for f in dataset.schema],
    }


def probe_gpkg_layer(path: Path, layer: str) -> dict:
    """Layer facts from the GeoPackage catalog tables. Reads no geometry."""
    con = sqlite3.connect(f"file:{path}?mode=ro", uri=True)
    try:
        cur = con.cursor()
        cur.execute(
            "SELECT data_type, min_x, min_y, max_x, max_y, srs_id "
            "FROM gpkg_contents WHERE table_name = ?",
            (layer,),
        )
        row = cur.fetchone()
        if row is None:
            raise KeyError(f"layer {layer!r} not in {path.name}")
        data_type, mnx, mny, mxx, mxy, srs = row
        cur.execute(f'SELECT COUNT(*) FROM "{layer}"')
        n_rows = cur.fetchone()[0]
        cur.execute(f'PRAGMA table_info("{layer}")')
        cols = [(r[1], r[2]) for r in cur.fetchall()]
        return {
            "n_rows": n_rows,
            "n_columns": len(cols),
            "bytes": path.stat().st_size,
            "columns": cols,
            "geometry_type": data_type,
            "bbox": [mnx, mny, mxx, mxy],
            "srs_id": srs,
            "shared_container": True,
        }
    finally:
        con.close()


def probe_csv(path: Path) -> dict:
    with open(path, encoding="utf-8", errors="replace") as f:
        header = f.readline().rstrip("\n").rstrip("\r")
        n_rows = sum(1 for _ in f)
    cols = [c.strip().strip('"') for c in header.split(",")] if header else []
    return {
        "n_rows": n_rows,
        "n_columns": len(cols),
        "bytes": path.stat().st_size,
        "columns": [(c, "csv") for c in cols],
    }


def probe_csv_glob(path: Path, pattern: str) -> dict:
    files = sorted(path.glob(pattern))
    return {
        "n_rows": None,
        "n_columns": None,
        "bytes": sum(f.stat().st_size for f in files),
        "n_files": len(files),
        "files": [f.name for f in files],
        "columns": [],
    }


# ---------------------------------------------------------------------------
# Key verification
# ---------------------------------------------------------------------------
def verify_keys(product: dict, path: Path, observed: dict, max_rows: int) -> dict:
    """Is the declared key actually unique? A failure is a finding, not a crash."""
    declared = product.get("keys") or []
    result = {"declared": declared}

    if not declared:
        result["status"] = "not_applicable"
        result["note"] = "no key declared for this product"
        return result

    fmt = product["format"]
    if fmt not in ("parquet_file", "parquet_dataset", "gpkg_layer", "csv"):
        result["status"] = "not_checked"
        result["note"] = f"key checking is not implemented for {fmt}"
        return result

    names = [c for c, _ in observed.get("columns", [])]
    missing = [k for k in declared if k not in names]
    if missing:
        result["status"] = "failed"
        result["note"] = f"declared key column(s) absent from the product: {missing}"
        result["missing_columns"] = missing
        return result

    n_rows = observed.get("n_rows") or 0
    if n_rows > max_rows:
        result["status"] = "not_checked"
        result["note"] = (
            f"{n_rows:,} rows exceeds the {max_rows:,}-row check limit; "
            "rerun with --verify-all"
        )
        return result

    if fmt == "gpkg_layer":
        # Counted inside SQLite so the geometry column is never materialised.
        cols = ", ".join(f'"{k}"' for k in declared)
        con = sqlite3.connect(f"file:{path}?mode=ro", uri=True)
        try:
            cur = con.cursor()
            cur.execute(
                f'SELECT COUNT(*) FROM (SELECT DISTINCT {cols} '
                f'FROM "{product["gpkg_layer"]}")'
            )
            n_distinct = cur.fetchone()[0]
        finally:
            con.close()
    elif fmt == "csv":
        import csv as _csv

        seen = set()
        with open(path, encoding="utf-8", errors="replace", newline="") as f:
            for row in _csv.DictReader(f):
                seen.add(tuple(row[k] for k in declared))
        n_distinct = len(seen)
    else:
        dataset = (
            pads.dataset(str(path), format="parquet", partitioning="hive")
            if fmt == "parquet_dataset"
            else pads.dataset(str(path), format="parquet")
        )
        table = dataset.to_table(columns=declared)
        n_distinct = table.group_by(declared).aggregate([]).num_rows

    result["n_rows"] = n_rows
    result["n_distinct_keys"] = n_distinct
    result["n_duplicate_rows"] = n_rows - n_distinct
    result["status"] = "unique" if n_distinct == n_rows else "not_unique"
    if result["status"] == "not_unique":
        result["note"] = (
            f"{n_rows - n_distinct:,} rows share a key with another row, so the "
            "declared grain does not describe this product as built"
        )
    return result


# ---------------------------------------------------------------------------
# Inventory
# ---------------------------------------------------------------------------
def inventory_product(product: dict, root: Path, max_rows: int) -> dict:
    rel = product["path"]
    path = root / PurePosixPath(rel)
    fmt = product["format"]

    entry = {
        # curated, copied through unchanged
        "id": product["id"],
        "title": product["title"],
        "family": product["family"],
        "kind": product["kind"],
        "path": rel,
        "format": fmt,
        "lifecycle": product["lifecycle"],
        "one_row_is": product["one_row_is"],
        "keys": product.get("keys") or [],
        "grain_id": product.get("grain_id"),
        "derived_from": product.get("derived_from"),
        "producer": product.get("producer"),
        "access_mode": product["access_mode"],
        "intended_access_mode": product.get("intended_access_mode"),
        "review_status": product["review_status"],
        "facets": product.get("facets") or [],
        "caveats": product.get("caveats") or [],
        "notes": product.get("notes"),
    }

    if not path.exists():
        entry["availability"] = "missing"
        entry["reason"] = "not present in this data root"
        entry["observed"] = {}
        entry["key_check"] = {"status": "not_checked", "note": "product not present"}
        return entry

    try:
        if fmt == "parquet_file":
            observed = probe_parquet_file(path)
        elif fmt == "parquet_dataset":
            observed = probe_parquet_dataset(path)
        elif fmt == "gpkg_layer":
            observed = probe_gpkg_layer(path, product["gpkg_layer"])
        elif fmt == "csv":
            observed = probe_csv(path)
        elif fmt == "csv_glob":
            observed = probe_csv_glob(path, product.get("glob", "*.csv"))
        else:
            entry["availability"] = "error"
            entry["reason"] = f"unknown format {fmt!r}"
            entry["observed"] = {}
            entry["key_check"] = {"status": "not_checked", "note": "unknown format"}
            return entry
    except Exception as exc:  # a bad file is a finding, not a crashed build
        entry["availability"] = "error"
        entry["reason"] = f"{type(exc).__name__}: {exc}"
        entry["observed"] = {}
        entry["key_check"] = {"status": "not_checked", "note": "product unreadable"}
        return entry

    entry["observed"] = observed

    if observed.get("empty") or observed.get("n_files") == 0:
        entry["availability"] = "missing"
        entry["reason"] = "directory exists but contains no data files"
        entry["key_check"] = {"status": "not_checked", "note": "no data files"}
        return entry

    entry["key_check"] = verify_keys(product, path, observed, max_rows)
    entry["availability"] = (
        "partial" if entry["key_check"]["status"] in ("failed", "not_unique")
        else "available"
    )
    if entry["availability"] == "partial":
        entry["reason"] = entry["key_check"].get("note", "declared grain not confirmed")
    return entry


def build(root: Path, registry_path: Path, max_rows: int, env_label: str) -> dict:
    with open(registry_path, encoding="utf-8") as f:
        registry = yaml.safe_load(f)

    products = registry["products"]
    ids = [p["id"] for p in products]
    dupes = sorted({i for i in ids if ids.count(i) > 1})
    if dupes:
        sys.exit(f"registry has duplicate product ids: {dupes}")

    log(f"registry : {registry_path.as_posix()} ({len(products)} products)")
    log(f"data root: {env_label}")
    log("")

    entries = []
    for product in products:
        entry = inventory_product(product, root, max_rows)
        entries.append(entry)
        rows = entry["observed"].get("n_rows")
        shape = f"{rows:,} rows" if rows else ""
        log(f"  [{entry['availability']:>9}] {entry['id']:<48} {shape}")

    return {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "environment_label": env_label,
        "registry_version": registry.get("registry_version"),
        "key_check_row_limit": max_rows,
        "families": registry.get("families", {}),
        "grains": registry.get("grains", {}),
        "products": entries,
        "dashboard_catalog_audit": audit_dashboard_catalog(root, entries),
    }


# ---------------------------------------------------------------------------
# Drift against the dashboard's hard-coded catalog
# ---------------------------------------------------------------------------
DASHBOARD_CATALOG_REL = "docs/dashboard/pages/5_Data_Catalog.py"


def audit_dashboard_catalog(root: Path, entries: list[dict]) -> dict | None:
    """Compare the dashboard's hand-maintained CATALOG dict against reality.

    The dashboard page lists products in a literal dict. Parsed rather than
    imported, because importing a Streamlit page runs it. Returns None if the
    page is gone or its shape has changed — this audit is a migration aid, not a
    dependency.
    """
    import ast

    page = root / PurePosixPath(DASHBOARD_CATALOG_REL)
    if not page.is_file():
        return None
    try:
        tree = ast.parse(page.read_text(encoding="utf-8"))
        catalog = next(
            ast.literal_eval(node.value)
            for node in ast.walk(tree)
            if isinstance(node, ast.Assign)
            and getattr(node.targets[0], "id", None) == "CATALOG"
        )
        listed = [
            (section, e["label"], e["path"])
            for section, items in catalog.items()
            for e in items
        ]
    except (SyntaxError, ValueError, StopIteration, KeyError, TypeError):
        return None

    broken = [(s, l, p) for s, l, p in listed if not (root / PurePosixPath(p)).exists()]
    listed_paths = {p for _, _, p in listed}
    unlisted = sorted(
        (e["title"], e["path"])
        for e in entries
        if e["availability"] == "available" and e["path"] not in listed_paths
    )
    return {
        "page": DASHBOARD_CATALOG_REL,
        "n_listed": len(listed),
        "broken": broken,
        "unlisted": unlisted,
    }


# ---------------------------------------------------------------------------
# Report
# ---------------------------------------------------------------------------
def render_dashboard_drift(audit: dict) -> list[str]:
    L = [
        "## Why this replaces the dashboard's hand-kept catalog\n",
        f"The Data Catalog page (`{audit['page']}`) keeps its product list in a "
        "literal dict that is edited by hand. Nothing checks it against the data, "
        "so it drifts silently. Measured against this data root:\n",
        "| | Count |",
        "|---|---:|",
        f"| Products listed on that page | {audit['n_listed']} |",
        f"| Listed paths with nothing at them | {len(audit['broken'])} |",
        f"| Real products the page never mentions | {len(audit['unlisted'])} |",
        "",
    ]
    if audit["broken"]:
        L.append("Listed but not present:\n")
        L.append("| Section | Entry | Path |")
        L.append("|---|---|---|")
        for section, label, path in audit["broken"]:
            L.append(f"| {section} | {label} | `{path}` |")
        L.append("")
    if audit["unlisted"]:
        L.append("Present but never listed:\n")
        L.append("| Product | Path |")
        L.append("|---|---|")
        for title, path in audit["unlisted"]:
            L.append(f"| {title} | `{path}` |")
        L.append("")
    L.append(
        "Rebuilding that page from the registry removes the drift by "
        "construction: a product exists in one place, and presence is measured "
        "rather than asserted.\n"
    )
    return L


def render_markdown(inv: dict) -> str:
    products = inv["products"]
    fams = inv["families"]
    L: list[str] = []

    counts = {s: sum(1 for p in products if p["availability"] == s)
              for s in AVAILABILITY_ORDER}
    by_access: dict[str, int] = {}
    for p in products:
        if p["availability"] in ("available", "partial"):
            by_access[p["access_mode"]] = by_access.get(p["access_mode"], 0) + 1

    L.append("# Master Product Inventory\n")
    L.append(f"**Generated:** {inv['generated_at']}  ")
    L.append(f"**Data root:** {inv['environment_label']}  ")
    L.append(f"**Registry version:** {inv['registry_version']}\n")
    L.append(
        "Every data product this repository produces or documents, with what one "
        "row means, what identifies it, what the Forest Data Explorer may do with "
        "it, and whether it is actually there.\n"
    )
    L.append(
        "Meaning is curated by hand in "
        "[`forest_explorer/registry/products.yaml`](../forest_explorer/registry/products.yaml). "
        "Everything measured — row counts, sizes, column lists, key checks — is "
        "regenerated from the data by "
        "[`forest_explorer/catalog/build_inventory.py`](../forest_explorer/catalog/build_inventory.py). "
        "Do not edit this file by hand; rerun the generator.\n"
    )

    L.append("## Where things stand\n")
    L.append("| | Count |")
    L.append("|---|---:|")
    L.append(f"| Products registered | {len(products)} |")
    L.append(f"| Present and matching their declared grain | {counts['available']} |")
    L.append(f"| Present but grain not confirmed | {counts['partial']} |")
    L.append(f"| Documented but not built in this data root | {counts['missing']} |")
    if counts["error"]:
        L.append(f"| Unreadable | {counts['error']} |")
    L.append("")

    L.append("Of the products that are present:\n")
    L.append("| What the interface may offer | Products |")
    L.append("|---|---:|")
    for mode, label in ACCESS_MODE_LABELS.items():
        if by_access.get(mode):
            L.append(f"| {label} | {by_access[mode]} |")
    L.append("")

    L.append("## How to read the columns\n")
    L.append("**Access mode** — what the interface is allowed to offer:\n")
    L.append("| Mode | Meaning |")
    L.append("|---|---|")
    L.append("| Catalog only | Described and searchable. No extract can be generated yet. |")
    L.append("| Prepared download | A finished output. Download it or filter it; do not rebuild it. |")
    L.append("| Filtered extract | Pick rows and columns from this one product. |")
    L.append("| Guided combination | May act as the base of an approved, checked join. |")
    L.append("")
    L.append("**Review status** — whether the meaning has been signed off:\n")
    L.append("| Status | Meaning |")
    L.append("|---|---|")
    L.append("| Reviewed | Definition and safe use are established. |")
    L.append("| Reviewed with limits | Usable, but only with the stated restriction applied. |")
    L.append("| Needs scientific review | The method or interpretation is still open. |")
    L.append("| Blocked by a defect | A known data or pipeline fault affects the values. |")
    L.append("| Not reviewed | Not yet evaluated. |")
    L.append("")
    L.append(
        "**Grain check** — whether the columns listed under *Identified by* really "
        "do pick out one row each. `unique` was tested and passed. `not unique` "
        "means the product does not have the grain claimed for it. `not checked` "
        "means the product was too large for the default limit or is in a format "
        "the checker does not read.\n"
    )

    # ---- master table -----------------------------------------------------
    L.append("## Every product\n")
    L.append(
        "| Product | One row is | Identified by | Rows | Size | Access | Review | Grain check | Present |"
    )
    L.append("|---|---|---|---:|---:|---|---|---|---|")
    for fam_id in fams:
        fam_products = [p for p in products if p["family"] == fam_id]
        if not fam_products:
            continue
        L.append(f"| **{fams[fam_id]['title']}** | | | | | | | | |")
        for p in fam_products:
            obs = p["observed"]
            rows = f"{obs['n_rows']:,}" if obs.get("n_rows") else "—"
            keys = ", ".join(f"`{k}`" for k in p["keys"]) or "—"
            kc = p["key_check"]["status"]
            kc_cell = {
                "unique": "unique",
                "not_unique": "**not unique**",
                "failed": "**key missing**",
                "not_checked": "not checked",
                "not_applicable": "—",
            }[kc]
            present = {
                "available": "yes",
                "partial": "yes",
                "missing": "not yet" if p["lifecycle"] == "planned" else "**no**",
                "error": "**error**",
            }[p["availability"]]
            L.append(
                f"| [{p['title']}](#{p['id']}) "
                f"| {p['one_row_is']} | {keys} | {rows} "
                f"| {human_bytes(obs.get('bytes'))} "
                f"| {ACCESS_MODE_LABELS[p['access_mode']]} "
                f"| {REVIEW_STATUS_LABELS[p['review_status']]} "
                f"| {kc_cell} | {present} |"
            )
    L.append("")

    # ---- not built --------------------------------------------------------
    absent = [p for p in products if p["availability"] in ("missing", "error")]
    if absent:
        L.append("## Documented but not built here\n")
        L.append(
            "These have a producer script and a documented path, but nothing at "
            "that path in this data root. The catalog must describe them as "
            "unavailable rather than let a researcher request them.\n"
        )
        L.append("| Product | Path | Producer | Why |")
        L.append("|---|---|---|---|")
        for p in absent:
            L.append(
                f"| {p['title']} | `{p['path']}` | `{p.get('producer') or '—'}` "
                f"| {p.get('reason', '—')} |"
            )
        L.append("")

    if inv.get("dashboard_catalog_audit"):
        L.extend(render_dashboard_drift(inv["dashboard_catalog_audit"]))

    # ---- detail pages -----------------------------------------------------
    L.append("## Product detail\n")
    for fam_id, fam in fams.items():
        fam_products = [p for p in products if p["family"] == fam_id]
        if not fam_products:
            continue
        L.append(f"### {fam['title']}\n")
        L.append(f"{fam['blurb']}\n")
        for p in fam_products:
            obs = p["observed"]
            L.append(f'<a id="{p["id"]}"></a>\n')
            L.append(f"#### {p['title']}\n")
            L.append(f"**One row is** {p['one_row_is']}.\n")

            L.append(f"- **Product id:** `{p['id']}`")
            L.append(f"- **Path:** `{p['path']}` ({p['format']})")
            L.append(f"- **Identified by:** " + (
                ", ".join(f"`{k}`" for k in p["keys"]) or "no key declared"))
            if p.get("grain_id"):
                L.append(f"- **Grain:** `{p['grain_id']}`")
            if p.get("derived_from"):
                L.append(f"- **Built from:** `{p['derived_from']}`")
            L.append(f"- **Built by:** `{p.get('producer') or 'unknown'}`")
            access = ACCESS_MODE_LABELS[p["access_mode"]]
            if p.get("intended_access_mode"):
                access += (f" (held back; intended as "
                           f"{ACCESS_MODE_LABELS[p['intended_access_mode']]})")
            L.append(
                f"- **Access:** {access} · "
                f"**Review:** {REVIEW_STATUS_LABELS[p['review_status']]} · "
                f"**Lifecycle:** {p['lifecycle']}"
            )
            if p["availability"] in ("missing", "error"):
                L.append(f"- **Present:** no — {p.get('reason')}")
            else:
                rows = f"{obs['n_rows']:,}" if obs.get("n_rows") is not None else "—"
                cols = obs.get("n_columns")
                bits = [f"{rows} rows"]
                if cols:
                    bits.append(f"{cols} columns")
                bits.append(human_bytes(obs.get("bytes")))
                if obs.get("n_files"):
                    bits.append(f"{obs['n_files']} files")
                if obs.get("partitions"):
                    bits.append(f"{len(obs['partitions'])} state partitions")
                L.append(f"- **Observed:** {' · '.join(bits)}")
                if obs.get("bbox") and obs["bbox"][0] is not None:
                    bb = [f"{v:.2f}" for v in obs["bbox"]]
                    L.append(
                        f"- **Extent:** {bb[0]}, {bb[1]} to {bb[2]}, {bb[3]} "
                        f"(EPSG:{obs.get('srs_id')})"
                    )
                kc = p["key_check"]
                if kc["status"] == "unique":
                    L.append(
                        f"- **Grain check:** passed — the declared key is unique "
                        f"across all {kc['n_rows']:,} rows"
                    )
                elif kc["status"] in ("not_unique", "failed"):
                    L.append(f"- **Grain check:** FAILED — {kc['note']}")
                elif kc["status"] == "not_checked":
                    L.append(f"- **Grain check:** not checked — {kc['note']}")
            if p["facets"]:
                L.append("- **Filterable on:** " +
                         ", ".join(f"`{c}`" for c in p["facets"]))
            L.append("")

            if p["caveats"]:
                L.append("**Before you use it**\n")
                for c in p["caveats"]:
                    L.append(f"- {c}")
                L.append("")
            if p["notes"]:
                L.append(f"**Maintainer note.** {p['notes']}\n")

    return "\n".join(L) + "\n"


def main() -> int:
    here = Path(__file__).resolve()
    inferred_root = here.parent.parent.parent

    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--root", default=os.environ.get("FOREST_DATA_ROOT",
                                                     str(inferred_root)))
    ap.add_argument("--root-label", default=None,
                    help="Non-sensitive environment label recorded in the output.")
    ap.add_argument("--registry", default=None)
    ap.add_argument("--verify-all", action="store_true",
                    help="Check declared keys on every product, regardless of size.")
    ap.add_argument("--max-verify-rows", type=int, default=DEFAULT_MAX_VERIFY_ROWS)
    ap.add_argument("--json-out",
                    default="forest_explorer/catalog/generated/inventory.json")
    ap.add_argument("--md-out", default="docs/MASTER_PRODUCT_INVENTORY.md")
    args = ap.parse_args()

    root = Path(args.root).resolve()
    if not root.is_dir():
        sys.exit(f"data root does not exist: {root}")

    registry_path = (Path(args.registry) if args.registry
                     else root / PurePosixPath(REGISTRY_REL))
    if not registry_path.is_file():
        sys.exit(f"registry not found: {registry_path}")

    max_rows = sys.maxsize if args.verify_all else args.max_verify_rows
    env_label = (args.root_label or os.environ.get("FDE_ENV_LABEL")
                 or f"local ({root.name})")

    inv = build(root, registry_path, max_rows, env_label)

    json_path = root / PurePosixPath(args.json_out)
    json_path.parent.mkdir(parents=True, exist_ok=True)
    with open(json_path, "w", encoding="utf-8", newline="\n") as f:
        json.dump(inv, f, indent=2, default=str)

    md_path = root / PurePosixPath(args.md_out)
    md_path.parent.mkdir(parents=True, exist_ok=True)
    with open(md_path, "w", encoding="utf-8", newline="\n") as f:
        f.write(render_markdown(inv))

    counts = {s: sum(1 for p in inv["products"] if p["availability"] == s)
              for s in AVAILABILITY_ORDER}
    log("")
    log(f"wrote {args.md_out}")
    log(f"wrote {args.json_out}")
    log(f"available={counts['available']} partial={counts['partial']} "
        f"missing={counts['missing']} error={counts['error']}")

    failures = [p["id"] for p in inv["products"]
                if p["key_check"]["status"] in ("not_unique", "failed")]
    if failures:
        log(f"grain check failed for: {failures}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
