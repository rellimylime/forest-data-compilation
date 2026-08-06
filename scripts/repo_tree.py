#!/usr/bin/env python3
"""
Print a repository tree showing data and code, hiding settings and caches.

Two things make this readable where `tree` is not:

  * Repetitive siblings collapse. Fifty `state=XX/` partitions or 6,000
    `range_climate_batch_####.parquet` files become one line with a count and a
    total size, instead of thousands of lines.
  * Directories carry their total size, so the tree doubles as a map of where
    the data actually is.

Read-only. Works the same on Windows and on the HPC.

Usage:
    python3 scripts/repo_tree.py                  # data + code, collapsed
    python3 scripts/repo_tree.py --depth 3        # shallower
    python3 scripts/repo_tree.py --files          # list every file
    python3 scripts/repo_tree.py 05_fia           # one subtree
    python3 scripts/repo_tree.py --data-only      # only dirs containing data
    python3 scripts/repo_tree.py --ascii > tree.txt
"""
from __future__ import annotations

import argparse
import os
import re
import sys
from pathlib import Path

# Settings, caches, and dependency trees: never interesting, always huge.
EXCLUDE_DIRS = {
    ".git", ".svn", ".hg",
    "renv", ".Rproj.user", ".Rcheck",
    "__pycache__", ".venv", "venv", ".env", "env",
    ".pytest_cache", ".mypy_cache", ".ruff_cache", ".tox",
    ".ipynb_checkpoints", "node_modules",
    ".vscode", ".idea", ".claude", ".devcontainer", ".streamlit",
    ".DS_Store", ".cache", "site-packages",
}

EXCLUDE_FILE_SUFFIXES = {
    ".pyc", ".pyo", ".pyd", ".so", ".o", ".class",
    ".log", ".tmp", ".swp", ".lock",
}

# Extensions that count as "data" for --data-only and for size reporting.
DATA_SUFFIXES = {
    ".parquet", ".csv", ".tsv", ".gpkg", ".shp", ".tif", ".tiff", ".nc",
    ".rds", ".rda", ".feather", ".arrow", ".zip", ".gz", ".h5", ".hdf5",
    ".sqlite", ".db", ".json", ".geojson",
}

# Collapse a group of similar siblings once it reaches this many members.
COLLAPSE_AT = 4

GLYPHS = {
    "unicode": {"tee": "├── ", "elbow": "└── ", "pipe": "│   ", "space": "    "},
    "ascii": {"tee": "|-- ", "elbow": "`-- ", "pipe": "|   ", "space": "    "},
}


def human(n: float) -> str:
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if n < 1024 or unit == "TB":
            return f"{n:.0f}{unit}" if unit == "B" else f"{n:.1f}{unit}"
        n /= 1024
    return f"{n:.1f}TB"


def pattern_of(name: str) -> str:
    """
    Normalize a filename to a shape, so siblings that differ only by an index or
    a state code group together.

        state=AK                     -> state=@@
        cond_AK.parquet              -> cond_@@.parquet
        range_climate_batch_0001.pq  -> range_climate_batch_####.pq
    """
    s = re.sub(r"\d+", "#", name)
    s = re.sub(r"(?<![A-Za-z])[A-Z]{2}(?![A-Za-z])", "@@", s)
    return s


def is_excluded_dir(p: Path) -> bool:
    return p.name in EXCLUDE_DIRS


def is_excluded_file(p: Path) -> bool:
    return p.name in EXCLUDE_DIRS or p.suffix.lower() in EXCLUDE_FILE_SUFFIXES


def is_data(p: Path) -> bool:
    return p.suffix.lower() in DATA_SUFFIXES


class Node:
    __slots__ = ("path", "is_dir", "children", "size", "n_files", "n_data")

    def __init__(self, path: Path, is_dir: bool):
        self.path = path
        self.is_dir = is_dir
        self.children: list[Node] = []
        self.size = 0
        self.n_files = 0
        self.n_data = 0


def build(path: Path) -> Node | None:
    """Walk once, accumulating sizes bottom-up."""
    if path.is_symlink() and path.is_dir():
        node = Node(path, True)
        return node
    if path.is_dir():
        if is_excluded_dir(path):
            return None
        node = Node(path, True)
        try:
            entries = sorted(path.iterdir(), key=lambda p: (not p.is_dir(), p.name.lower()))
        except (PermissionError, OSError):
            return node
        for entry in entries:
            child = build(entry)
            if child is None:
                continue
            node.children.append(child)
            node.size += child.size
            node.n_files += child.n_files
            node.n_data += child.n_data
        return node

    if is_excluded_file(path):
        return None
    node = Node(path, False)
    try:
        node.size = path.stat().st_size
    except OSError:
        node.size = 0
    node.n_files = 1
    node.n_data = 1 if is_data(path) else 0
    return node


def group_children(children: list[Node]) -> list[tuple[str, list[Node]]]:
    """Group siblings by shape, preserving first-appearance order."""
    order: list[str] = []
    groups: dict[str, list[Node]] = {}
    for c in children:
        key = ("D:" if c.is_dir else "F:") + pattern_of(c.path.name)
        if key not in groups:
            groups[key] = []
            order.append(key)
        groups[key].append(c)
    return [(k, groups[k]) for k in order]


def render(
    node: Node,
    out: list[str],
    prefix: str = "",
    depth: int = 0,
    *,
    max_depth: int,
    show_files: bool,
    data_only: bool,
    g: dict,
) -> None:
    # At the depth limit, stop silently: the parent's label already reports the
    # subtree's file count and size, so an ellipsis line would just repeat it.
    if depth >= max_depth:
        return

    kids = node.children
    if data_only:
        kids = [c for c in kids if c.n_data > 0]
    if not show_files:
        # Keep directories, plus files that sit alongside no directories at all
        # (a leaf folder of data would otherwise render empty).
        dirs = [c for c in kids if c.is_dir]
        files = [c for c in kids if not c.is_dir]
        kids = dirs + files

    groups = group_children(kids)
    renderable: list[tuple[str, list[Node]]] = []
    for key, members in groups:
        if len(members) >= COLLAPSE_AT:
            renderable.append((key, members))
        else:
            renderable.extend((key, [m]) for m in members)

    for i, (key, members) in enumerate(renderable):
        last = i == len(renderable) - 1
        branch = g["elbow"] if last else g["tee"]
        nxt = prefix + (g["space"] if last else g["pipe"])

        if len(members) > 1:
            total = sum(m.size for m in members)
            nfiles = sum(m.n_files for m in members)
            first, lastm = members[0].path.name, members[-1].path.name
            kind = "dirs" if members[0].is_dir else "files"
            out.append(
                f"{prefix}{branch}{first} … {lastm}  "
                f"[{len(members)} {kind}, {nfiles:,} files, {human(total)}]"
            )
            continue

        m = members[0]
        if m.is_dir:
            label = f"{m.path.name}/"
            meta = f"  [{m.n_files:,} files, {human(m.size)}]" if m.n_files else "  [empty]"
            out.append(f"{prefix}{branch}{label}{meta}")
            render(
                m, out, nxt, depth + 1,
                max_depth=max_depth, show_files=show_files,
                data_only=data_only, g=g,
            )
        else:
            if not show_files and not is_data(m.path):
                out.append(f"{prefix}{branch}{m.path.name}")
            else:
                out.append(f"{prefix}{branch}{m.path.name}  {human(m.size)}")


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("root", nargs="?", default=".", help="directory to print (default: .)")
    ap.add_argument("--depth", type=int, default=4, help="max depth (default: 4)")
    ap.add_argument("--files", action="store_true", help="list every file, not just data files")
    ap.add_argument("--data-only", action="store_true", help="only show branches containing data")
    ap.add_argument("--collapse-at", type=int, default=COLLAPSE_AT,
                    help=f"collapse sibling groups at this size (default: {COLLAPSE_AT})")
    ap.add_argument("--ascii", action="store_true", help="ASCII glyphs instead of box drawing")
    args = ap.parse_args()

    globals()["COLLAPSE_AT"] = args.collapse_at

    root = Path(args.root).resolve()
    if not root.is_dir():
        sys.exit(f"not a directory: {root}")

    tree = build(root)
    if tree is None:
        sys.exit(f"root is excluded: {root}")

    # Windows consoles often run a codepage that cannot render box-drawing
    # characters; fall back rather than emitting a wall of "?".
    try:
        sys.stdout.reconfigure(encoding="utf-8")
    except (AttributeError, OSError):
        pass
    use_ascii = args.ascii
    if not use_ascii:
        try:
            "├──".encode(sys.stdout.encoding or "ascii")
        except (UnicodeEncodeError, LookupError):
            use_ascii = True

    g = GLYPHS["ascii" if use_ascii else "unicode"]
    out = [f"{root.name}/  [{tree.n_files:,} files, {human(tree.size)}]"]
    render(
        tree, out, "", 0,
        max_depth=args.depth, show_files=args.files,
        data_only=args.data_only, g=g,
    )
    out.append("")
    out.append(f"excluded: {', '.join(sorted(EXCLUDE_DIRS)[:8])}, …")

    text = "\n".join(out)
    try:
        print(text)
    except UnicodeEncodeError:
        print(text.encode("ascii", "replace").decode("ascii"))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
