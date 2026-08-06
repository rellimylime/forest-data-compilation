#!/usr/bin/env python3
"""
Profile FIA parquet products against the official FIADB Database Description.

Produces a data dictionary that binds, for every column of every profiled
product:

  1. Official  - FIADB descriptive name, guide subsection, PDF page, taken from
                 fiadb_user_guide_index_v94.json (which is pinned to a specific
                 PDF sha256).
  2. Observed  - Arrow type, null rate, distinct count, min/max, and the actual
                 code values present in the data.
  3. Grain     - whether the declared key is actually unique.
  4. Flags     - columns with no official definition (repository-derived),
                 type surprises, suspected sentinels, and out-of-range codes.

Read-only. Never writes into any product directory.

Usage:
    python3 05_fia/scripts/profile_fia_products.py
    python3 05_fia/scripts/profile_fia_products.py --product plot_condition_metadata
    python3 05_fia/scripts/profile_fia_products.py --root /path/to/repo --out docs/

If --root is omitted it uses $FOREST_DATA_ROOT, then the repository root
inferred from this file's location.
"""
from __future__ import annotations

import argparse
import json
import os
import sys
from datetime import datetime, timezone
from pathlib import Path, PurePosixPath

try:
    import pyarrow as pa
    import pyarrow.compute as pc
    import pyarrow.dataset as ds
except ModuleNotFoundError:
    sys.exit(
        "pyarrow is required. Install it into the active environment:\n"
        "    pip install -r requirements.txt"
    )

# --------------------------------------------------------------------------
# Products to profile. Logical paths only: relative, POSIX, no drive letters.
# `source_tables` is the search order used to attribute a column to an official
# FIADB table, since summary products mix COND and PLOT columns.
# --------------------------------------------------------------------------
PRODUCTS = {
    "plot_condition_metadata": {
        "path": "05_fia/data/processed/summaries/plot_condition_metadata.parquet",
        "grain": ["PLT_CN", "INVYR", "CONDID"],
        "grain_prose": "one mapped FIA condition within one plot visit",
        "source_tables": ["COND", "PLOT"],
    },
    "cond_partitions": {
        "path": "05_fia/data/processed/cond",
        "grain": ["PLT_CN", "INVYR", "CONDID"],
        "grain_prose": "one mapped FIA condition within one plot visit",
        "source_tables": ["COND", "PLOT"],
    },
    "fia_condition_disturbance_flags": {
        "path": "05_fia/data/processed/summaries/fia_condition_disturbance_flags.parquet",
        "grain": ["PLT_CN", "INVYR", "CONDID"],
        "grain_prose": "condition within plot visit",
        "source_tables": ["COND", "PLOT"],
    },
    "plot_treatment_history": {
        "path": "05_fia/data/processed/summaries/plot_treatment_history.parquet",
        "grain": ["PLT_CN", "INVYR", "CONDID", "treatment_slot"],
        "grain_prose": "one treatment slot recorded on one condition in one plot visit (long format)",
        "source_tables": ["COND", "PLOT"],
    },
}

GUIDE_INDEX_REL = "05_fia/docs/dashboard/fiadb_user_guide_index_v94.json"

# Columns whose values are FIA code systems; profiled exhaustively so that
# every code actually present in the data is visible.
MAX_ENUMERATED_VALUES = 40

# Values that commonly mean "not collected" / "not applicable" in FIADB.
SENTINEL_CANDIDATES = {-1, 9999, 99999, -9999}


def log(msg: str) -> None:
    print(msg, file=sys.stderr)


# --------------------------------------------------------------------------
# Official definitions
# --------------------------------------------------------------------------
def load_guide_index(root: Path) -> dict:
    p = root / GUIDE_INDEX_REL
    if not p.is_file():
        log(f"WARNING: guide index not found at {GUIDE_INDEX_REL}; "
            "official definitions will be blank.")
        return {"columns_index": [], "table_sections": {}, "source": {}}
    with open(p, encoding="utf-8") as f:
        return json.load(f)


def build_official_lookup(guide: dict) -> dict[tuple[str, str], dict]:
    """(oracle_table, column_name) -> official metadata."""
    out: dict[tuple[str, str], dict] = {}
    for row in guide.get("columns_index", []):
        table = row.get("oracle_table")
        col = row.get("column_name")
        if not table or not col:
            continue
        out[(table.upper(), col.upper())] = {
            "descriptive_name": row.get("descriptive_name"),
            "subsection": row.get("subsection"),
            "pdf_page": row.get("index_pdf_page"),
            "field_guide_section": row.get("field_guide_section"),
        }
    return out


def attribute_column(
    col: str, source_tables: list[str], lookup: dict[tuple[str, str], dict]
) -> tuple[str | None, dict | None]:
    """Find the official definition, searching source_tables in order."""
    key = col.upper()
    for table in source_tables:
        hit = lookup.get((table.upper(), key))
        if hit:
            return table.upper(), hit
    return None, None


# --------------------------------------------------------------------------
# Observed values
# --------------------------------------------------------------------------
def is_numeric(t: pa.DataType) -> bool:
    return pa.types.is_integer(t) or pa.types.is_floating(t)


def profile_column(dataset: ds.Dataset, col: str, n_rows: int) -> dict:
    """Profile one column at a time so memory stays bounded."""
    tbl = dataset.to_table(columns=[col])
    arr = tbl.column(col).combine_chunks()
    dtype = arr.type

    null_count = arr.null_count
    info: dict = {
        "arrow_type": str(dtype),
        "null_count": int(null_count),
        "null_pct": round(100.0 * null_count / n_rows, 3) if n_rows else None,
        "flags": [],
    }

    non_null = arr.drop_null()
    if len(non_null) == 0:
        info["n_distinct"] = 0
        info["flags"].append("all_null")
        return info

    # Boolean columns in a field that should hold a year or a code is exactly
    # the TRTYR3 failure mode; surface it rather than letting it pass silently.
    if pa.types.is_boolean(dtype):
        info["flags"].append("boolean_type")

    try:
        uniq = pc.unique(non_null)
        n_distinct = len(uniq)
    except pa.ArrowNotImplementedError:
        uniq, n_distinct = None, None
    info["n_distinct"] = int(n_distinct) if n_distinct is not None else None

    if is_numeric(dtype):
        info["min"] = pc.min(non_null).as_py()
        info["max"] = pc.max(non_null).as_py()

    # Enumerate the full value set for low-cardinality columns: for code fields
    # this is the list of codes actually present, which is the thing you need
    # in order to certify a variable.
    if n_distinct is not None and n_distinct <= MAX_ENUMERATED_VALUES:
        counts = pc.value_counts(non_null)
        pairs = [
            (counts.field("values")[i].as_py(), counts.field("counts")[i].as_py())
            for i in range(len(counts))
        ]
        pairs.sort(key=lambda kv: kv[1], reverse=True)
        info["values"] = [{"value": v, "count": int(c)} for v, c in pairs]
    elif uniq is not None:
        sample = sorted(
            (v for v in uniq.to_pylist()[:5000] if v is not None),
            key=lambda x: (str(type(x)), x),
        )
        info["sample_values"] = sample[:5] + (["..."] if len(sample) > 5 else [])

    if is_numeric(dtype):
        present = set()
        if n_distinct is not None and n_distinct <= MAX_ENUMERATED_VALUES:
            present = {v["value"] for v in info["values"]}
        else:
            for s in SENTINEL_CANDIDATES:
                if pc.any(pc.equal(non_null, s)).as_py():
                    present.add(s)
        hits = sorted(present & SENTINEL_CANDIDATES)
        if hits:
            info["flags"].append(f"sentinel_values_present:{hits}")

    return info


def check_grain(dataset: ds.Dataset, grain: list[str], n_rows: int) -> dict:
    """Is the declared key actually unique?"""
    present = [c for c in grain if c in dataset.schema.names]
    missing = [c for c in grain if c not in dataset.schema.names]
    result = {
        "declared_key": grain,
        "key_columns_present": present,
        "key_columns_missing": missing,
        "n_rows": n_rows,
    }
    if missing or not present:
        result["unique"] = None
        result["note"] = "cannot verify: key column(s) absent from product"
        return result

    tbl = dataset.to_table(columns=present)
    df = tbl.to_pandas()
    n_unique = len(df.drop_duplicates())
    result["n_distinct_keys"] = int(n_unique)
    result["n_duplicate_rows"] = int(n_rows - n_unique)
    result["unique"] = bool(n_unique == n_rows)
    return result


def profile_product(name: str, spec: dict, root: Path, lookup: dict) -> dict:
    rel = spec["path"]
    path = root / PurePosixPath(rel)
    out: dict = {
        "product": name,
        "logical_path": rel,
        "grain_prose": spec["grain_prose"],
        "source_tables": spec["source_tables"],
    }
    if not path.exists():
        out["present"] = False
        return out

    out["present"] = True
    dataset = ds.dataset(str(path), format="parquet")
    n_rows = dataset.count_rows()
    out["n_rows"] = n_rows
    out["n_columns"] = len(dataset.schema.names)

    log(f"  {name}: {n_rows:,} rows x {out['n_columns']} cols")

    out["grain_check"] = check_grain(dataset, spec["grain"], n_rows)

    columns = {}
    for col in dataset.schema.names:
        table, official = attribute_column(col, spec["source_tables"], lookup)
        entry = profile_column(dataset, col, n_rows)
        entry["official_table"] = table
        entry["official"] = official
        if official is None:
            entry["flags"].append("no_official_definition")
        columns[col] = entry
    out["columns"] = columns

    out["summary"] = {
        "n_official": sum(1 for c in columns.values() if c["official"]),
        "n_derived": sum(1 for c in columns.values() if not c["official"]),
        "n_flagged": sum(
            1 for c in columns.values()
            if [f for f in c["flags"] if f != "no_official_definition"]
        ),
    }
    return out


# --------------------------------------------------------------------------
# Rendering
# --------------------------------------------------------------------------
def fmt_values(entry: dict) -> str:
    if "values" in entry:
        parts = [f"`{v['value']}` ({v['count']:,})" for v in entry["values"][:12]]
        if len(entry["values"]) > 12:
            parts.append(f"… +{len(entry['values']) - 12} more")
        return "<br>".join(parts)
    if "sample_values" in entry:
        return ", ".join(f"`{v}`" for v in entry["sample_values"])
    if "min" in entry:
        return f"range `{entry['min']}` … `{entry['max']}`"
    return "—"


def render_markdown(report: dict) -> str:
    src = report.get("guide_source", {})
    L: list[str] = []
    L.append("# FIA Data Dictionary — observed products vs. official FIADB\n")
    L.append(f"**Generated:** {report['generated_at']}  ")
    L.append(f"**Data root label:** {report['root_label']}  ")
    L.append(f"**Official reference:** {src.get('pdf_file', 'unknown')}  ")
    if src.get("pdf_sha256"):
        L.append(f"**Reference sha256:** `{src['pdf_sha256'][:16]}…`  ")
    L.append("")
    L.append(
        "Every column below carries three things: what FIADB officially says it "
        "is, what is actually in the file, and whether anything about it is "
        "surprising. Columns with no official entry are repository-derived — "
        "they were created by this project's scripts and their meaning lives in "
        "the producer code, not in any FIA guide.\n"
    )

    L.append("## Contents\n")
    for name, prod in report["products"].items():
        status = "" if prod.get("present") else " — **NOT PRESENT**"
        L.append(f"- [{name}](#{name.replace('_', '-')}){status}")
    L.append("")

    L.append("## What to check before trusting a column\n")
    L.append("| Flag | Meaning |")
    L.append("|---|---|")
    L.append("| `no_official_definition` | Repository-derived. Trace it to the producer script before exposing it. |")
    L.append("| `boolean_type` | Stored as true/false. If the field should hold a year or code, this is the `TRTYR3` failure mode. |")
    L.append("| `all_null` | Column exists but holds no data in this build. |")
    L.append("| `sentinel_values_present` | Contains values like -1 or 9999 that usually mean 'not collected'. Filtering arithmetic must exclude them. |")
    L.append("")

    for name, prod in report["products"].items():
        L.append(f"## {name}\n")
        if not prod.get("present"):
            L.append(f"**Not present** at `{prod['logical_path']}` in this data root.\n")
            continue

        s = prod["summary"]
        g = prod["grain_check"]
        L.append(f"- **Logical path:** `{prod['logical_path']}`")
        L.append(f"- **One row is:** {prod['grain_prose']}")
        L.append(f"- **Rows:** {prod['n_rows']:,} across {prod['n_columns']} columns")
        L.append(
            f"- **Columns:** {s['n_official']} official FIADB, "
            f"{s['n_derived']} repository-derived, {s['n_flagged']} flagged"
        )

        key = " × ".join(g["declared_key"])
        if g.get("unique") is True:
            L.append(f"- **Grain verified:** `{key}` is unique across all {g['n_rows']:,} rows ✅")
        elif g.get("unique") is False:
            L.append(
                f"- **Grain NOT unique:** `{key}` yields {g['n_distinct_keys']:,} "
                f"distinct keys for {g['n_rows']:,} rows "
                f"({g['n_duplicate_rows']:,} duplicates) ⚠️"
            )
        else:
            L.append(f"- **Grain unverified:** {g.get('note', 'unknown')} ⚠️")
        L.append("")

        flagged = {
            c: e for c, e in prod["columns"].items()
            if [f for f in e["flags"] if f != "no_official_definition"]
        }
        if flagged:
            L.append("### Flagged columns\n")
            L.append("| Column | Type | Flags |")
            L.append("|---|---|---|")
            for c, e in flagged.items():
                fl = ", ".join(f"`{f}`" for f in e["flags"])
                L.append(f"| `{c}` | `{e['arrow_type']}` | {fl} |")
            L.append("")

        L.append("### All columns\n")
        L.append("| Column | Official name | Type | Null % | Distinct | Values present | Guide |")
        L.append("|---|---|---|---:|---:|---|---|")
        for c, e in prod["columns"].items():
            off = e["official"]
            if off:
                oname = off.get("descriptive_name") or "—"
                sub = off.get("subsection") or ""
                page = off.get("pdf_page")
                gref = f"§{sub} p.{page}" if sub else (f"p.{page}" if page else "—")
            else:
                oname = "_repository-derived_"
                gref = "—"
            nd = f"{e['n_distinct']:,}" if e.get("n_distinct") is not None else "—"
            npct = f"{e['null_pct']:.1f}" if e.get("null_pct") is not None else "—"
            L.append(
                f"| `{c}` | {oname} | `{e['arrow_type']}` | {npct} | {nd} "
                f"| {fmt_values(e)} | {gref} |"
            )
        L.append("")

    return "\n".join(L)


def main() -> int:
    here = Path(__file__).resolve()
    inferred_root = here.parent.parent.parent

    ap = argparse.ArgumentParser()
    ap.add_argument("--root", default=os.environ.get("FOREST_DATA_ROOT", str(inferred_root)))
    ap.add_argument("--root-label", default=None,
                    help="Non-sensitive environment label recorded in the report.")
    ap.add_argument("--product", action="append",
                    help="Profile only this product (repeatable).")
    ap.add_argument("--out", default="docs",
                    help="Directory for FIA_DATA_DICTIONARY.md and .json")
    args = ap.parse_args()

    root = Path(args.root).resolve()
    if not root.is_dir():
        sys.exit(f"data root does not exist: {root}")

    selected = args.product or list(PRODUCTS)
    unknown = [p for p in selected if p not in PRODUCTS]
    if unknown:
        sys.exit(f"unknown product(s): {unknown}\nknown: {list(PRODUCTS)}")

    guide = load_guide_index(root)
    lookup = build_official_lookup(guide)
    log(f"guide index: {len(lookup):,} (table, column) definitions")

    report = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "root_label": args.root_label or os.environ.get("FDE_ENV_LABEL", "local"),
        "guide_source": guide.get("source", {}),
        "products": {},
    }

    log("profiling:")
    for name in selected:
        report["products"][name] = profile_product(name, PRODUCTS[name], root, lookup)

    out_dir = root / PurePosixPath(args.out)
    out_dir.mkdir(parents=True, exist_ok=True)

    json_path = out_dir / "fia_data_dictionary.json"
    with open(json_path, "w", encoding="utf-8", newline="\n") as f:
        json.dump(report, f, indent=2, default=str)

    md_path = out_dir / "FIA_DATA_DICTIONARY.md"
    with open(md_path, "w", encoding="utf-8", newline="\n") as f:
        f.write(render_markdown(report))

    log(f"\nwrote {md_path}")
    log(f"wrote {json_path}")

    for name, prod in report["products"].items():
        if not prod.get("present"):
            log(f"  [MISSING]  {name}")
            continue
        s, g = prod["summary"], prod["grain_check"]
        grain = {True: "unique", False: "NOT UNIQUE", None: "unverified"}[g.get("unique")]
        log(f"  [{name}] rows={prod['n_rows']:,} official={s['n_official']} "
            f"derived={s['n_derived']} flagged={s['n_flagged']} grain={grain}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
