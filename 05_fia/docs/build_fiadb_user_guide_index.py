"""
Build a cached JSON index from the FIADB User Guide PDF (v9.4).

Extracts:
- PDF table of contents (bookmarks)
- Index of Tables (official table names + descriptions + documented joins)
- Index of Column Names (column/table occurrences + descriptive names + guide refs)

This script is intentionally parser-specific to the FIADB v9.4 PDF layout. It
uses page-coordinate buckets, which is much more reliable than plain text
scraping for the two index sections.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import re
import unicodedata
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Iterable, List, Optional

import fitz


# FIADB v9.4 (Aug 2025) PDF layout: page indexes are 0-based here.
TABLE_INDEX_PAGE_RANGE = range(646, 664)   # "Index of Tables"
COLUMN_INDEX_PAGE_RANGE = range(664, 758)  # "Index of Column Names"

SECTION_RE = re.compile(r"^\d+\.\d+$")
SUBSECTION_RE = re.compile(r"^\d+\.\d+(?:\.\d+)?$")
SECTION_PREFIX_RE = re.compile(r"^(\d+(?:\.\d+)?)\s+(.+)$")


def _normalize_text(text: str) -> str:
    if not text:
        return ""
    # Normalize common PDF extraction artifacts.
    replacements = {
        "\u00a0": " ",
        "\u2013": "-",
        "\u2014": "-",
        "\u2015": "-",
        "\u2212": "-",
        "\u2018": "'",
        "\u2019": "'",
        "\u201c": '"',
        "\u201d": '"',
        "\uf0b3": ">=",  # common extracted glyph in this PDF ("greater-or-equal")
        "•": "-",
    }
    for src, dst in replacements.items():
        text = text.replace(src, dst)
    # Collapse weird spacing.
    text = " ".join(text.split())
    return text.strip()


def _is_dash_token(value: str) -> bool:
    value = value.strip()
    if not value:
        return False
    return all(unicodedata.category(ch) == "Pd" or ch in {"-"} for ch in value)


def _is_letter_token(value: str) -> bool:
    value = value.strip()
    return len(value) == 1 and value.isalpha() and value.upper() == value


def _group_lines(words: Iterable[tuple], y_tol: float = 1.6) -> List[Dict]:
    words = sorted(words, key=lambda w: (w[1], w[0]))
    lines: List[Dict] = []
    for w in words:
        if lines and abs(lines[-1]["y"] - w[1]) <= y_tol:
            lines[-1]["words"].append(w)
        else:
            lines.append({"y": w[1], "words": [w]})
    for line in lines:
        line["words"].sort(key=lambda w: w[0])
    return lines


def _line_text(line: Dict) -> str:
    return " ".join(w[4] for w in line["words"]).strip()


def _bucket_table_index_line(line: Dict) -> Dict[int, str]:
    # Column starts (approx): 60, 123, 227, 323
    buckets = {1: [], 2: [], 3: [], 4: []}
    for w in line["words"]:
        x0 = w[0]
        token = w[4]
        if x0 < 110:
            buckets[1].append(token)
        elif x0 < 215:
            buckets[2].append(token)
        elif x0 < 305:
            buckets[3].append(token)
        else:
            buckets[4].append(token)
    return {i: " ".join(buckets[i]).strip() for i in buckets}


def _bucket_column_index_line(line: Dict) -> Dict[int, str]:
    # Column starts (approx): 60, 118, 161, 312, 445
    buckets = {1: [], 2: [], 3: [], 4: [], 5: []}
    for w in line["words"]:
        x0 = w[0]
        token = w[4]
        if x0 < 100:
            buckets[1].append(token)
        elif x0 < 150:
            buckets[2].append(token)
        elif x0 < 300:
            buckets[3].append(token)
        elif x0 < 430:
            buckets[4].append(token)
        else:
            buckets[5].append(token)
    return {i: " ".join(buckets[i]).strip() for i in buckets}


def _skip_common_index_line(line: Dict, kind: str, buckets: Dict[int, str]) -> bool:
    text = _line_text(line)
    if not text:
        return True

    if "FIADB User Guides - Volume:" in text:
        return True

    if kind == "tables":
        if "Index of Tables" in text or "Section revision:" in text or "Index - Quick Link:" in text:
            return True
        if re.fullmatch(r"Index\s+1(?:-\d+)?", text):
            return True
        if (
            "Oracle table name" in text
            or "Table name" in text
            or text in {"Description", "Section"}
        ):
            return True
        if line["y"] < 140 and any(h in text for h in ["Section", "Oracle table name", "Table name", "Description"]):
            return True
        vals = [buckets[1], buckets[2], buckets[3], buckets[4]]
    else:
        if "Index of Column Names" in text or "Section revision:" in text or "Index - Quick Link:" in text:
            return True
        if re.fullmatch(r"Index\s+2(?:-\d+)?", text):
            return True
        if (
            "Subsection" in text
            or "Column name" in text
            or "Oracle table name" in text
            or "Descriptive name" in text
            or "Field Guide section" in text
        ):
            return True
        if line["y"] < 520 and (
            text.startswith("The following table contains an alphabetized list")
            or text.startswith("tables included within this user guide.")
            or text.startswith("attribute are also listed.")
            or text.startswith('The "Subsection" number indicates')
            or text.startswith('guide. The "Field Guide section" number indicates')
            or text.startswith("Core Field Guide.")
            or text.startswith("A dash means there is no field guide section")
        ):
            return True
        vals = [buckets[1], buckets[2], buckets[3], buckets[4], buckets[5]]

    nonempty = [v for v in vals if v]
    if nonempty and all(_is_dash_token(v) or _is_letter_token(v) for v in nonempty):
        return True
    return False


def _parse_table_index(doc: fitz.Document) -> List[Dict]:
    records: List[Dict] = []
    current: Optional[Dict] = None

    for page_index in TABLE_INDEX_PAGE_RANGE:
        page = doc[page_index]
        for line in _group_lines(page.get_text("words")):
            b = _bucket_table_index_line(line)
            if _skip_common_index_line(line, "tables", b):
                continue

            s1, s2, s3, s4 = b[1], b[2], b[3], b[4]

            is_new = bool(SECTION_RE.fullmatch(s1)) and bool(s2)
            if is_new:
                if current:
                    records.append(current)
                current = {
                    "section": s1,
                    "oracle_table": "".join(s2.split()),
                    "table_name": _normalize_text(s3),
                    "description_lines": [],
                    "index_pdf_page": page_index + 1,
                }
                if s4:
                    current["description_lines"].append(_normalize_text(s4))
                continue

            if not current:
                continue

            # Continuations: table names / descriptions may wrap across lines.
            if s2 and not _is_dash_token(s2):
                token = "".join(s2.split())
                # Skip isolated quick-link letters if they slip through.
                if not (_is_letter_token(token) and not s3 and not s4):
                    current["oracle_table"] += token

            if s3 and not _is_dash_token(s3):
                current["table_name"] = _normalize_text(f"{current['table_name']} {s3}")

            if s4 and not _is_dash_token(s4):
                current["description_lines"].append(_normalize_text(s4))

    if current:
        records.append(current)

    # Collapse wrapped description lines. Preserve bullet boundaries.
    for rec in records:
        desc_out: List[str] = []
        for line in rec.pop("description_lines", []):
            if line.startswith("- "):
                desc_out.append(line)
            else:
                if desc_out and not desc_out[-1].startswith("- "):
                    desc_out[-1] = _normalize_text(f"{desc_out[-1]} {line}")
                else:
                    desc_out.append(line)
        rec["description"] = "\n".join(desc_out).strip()
        rec["oracle_table"] = rec["oracle_table"].replace(" ", "")
    return records


def _parse_column_index(
    doc: fitz.Document,
    table_by_section: Optional[Dict[str, str]] = None,
) -> List[Dict]:
    records: List[Dict] = []
    current: Optional[Dict] = None

    for page_index in COLUMN_INDEX_PAGE_RANGE:
        page = doc[page_index]
        for line in _group_lines(page.get_text("words")):
            b = _bucket_column_index_line(line)
            if _skip_common_index_line(line, "columns", b):
                continue

            s1, s2, s3, s4, s5 = b[1], b[2], b[3], b[4], b[5]
            is_new = bool(SUBSECTION_RE.fullmatch(s1)) and bool(s3) and bool(s4)

            if is_new:
                if current:
                    records.append(current)
                current = {
                    "subsection": s1,
                    "field_guide_section": None if s2 in {"", "-", "—"} else _normalize_text(s2),
                    "column_name": "".join(s3.split()),
                    "oracle_table": "".join(s4.split()),
                    "descriptive_name": _normalize_text(s5),
                    "index_pdf_page": page_index + 1,
                }
                continue

            if not current:
                continue

            # Wrapped identifiers (column_name / oracle_table) and descriptions.
            if s3 and not _is_dash_token(s3):
                current["column_name"] += "".join(s3.split())
            if s4 and not _is_dash_token(s4):
                current["oracle_table"] += "".join(s4.split())
            if s5 and not _is_dash_token(s5):
                current["descriptive_name"] = _normalize_text(
                    f"{current['descriptive_name']} {s5}"
                )
            if s2 and current["field_guide_section"] is None and s2 not in {"", "-", "—"}:
                current["field_guide_section"] = _normalize_text(s2)

    if current:
        records.append(current)

    # Final cleanup and validation-friendly normalization.
    out: List[Dict] = []
    for rec in records:
        rec["column_name"] = rec["column_name"].replace(" ", "")
        rec["oracle_table"] = rec["oracle_table"].replace(" ", "")
        rec["descriptive_name"] = _normalize_text(rec["descriptive_name"])
        section_prefix = ".".join(rec["subsection"].split(".")[:2])
        if table_by_section and section_prefix in table_by_section:
            rec["oracle_table"] = table_by_section[section_prefix]
        if not SUBSECTION_RE.fullmatch(rec["subsection"]):
            continue
        if not rec["column_name"] or not rec["oracle_table"]:
            continue
        out.append(rec)
    return out


def _compute_toc_page_ranges(doc: fitz.Document) -> List[Dict]:
    toc = doc.get_toc()  # [level, title, page]
    out: List[Dict] = []
    for i, row in enumerate(toc):
        level, title, start_page = row
        end_page = doc.page_count
        for j in range(i + 1, len(toc)):
            next_level, _, next_page = toc[j]
            if next_level <= level:
                end_page = next_page - 1
                break
        rec = {
            "level": int(level),
            "title": _normalize_text(title),
            "start_page": int(start_page),
            "end_page": int(end_page),
        }
        m = SECTION_PREFIX_RE.match(rec["title"])
        if m:
            rec["section"] = m.group(1)
            rec["section_title"] = _normalize_text(m.group(2))
        out.append(rec)
    return out


def _merge_table_sections(toc_entries: List[Dict], table_index: List[Dict]) -> Dict[str, Dict]:
    toc_by_section = {
        e.get("section"): e
        for e in toc_entries
        if e.get("section") and SECTION_RE.fullmatch(e.get("section", "")) and e["level"] == 2
    }
    merged: Dict[str, Dict] = {}
    for rec in table_index:
        toc_rec = toc_by_section.get(rec["section"], {})
        merged[rec["oracle_table"]] = {
            "oracle_table": rec["oracle_table"],
            "section": rec["section"],
            "official_table_name": rec["table_name"],
            "official_index_description": rec["description"],
            "table_index_pdf_page": rec["index_pdf_page"],
            "guide_toc_title": toc_rec.get("title"),
            "guide_section_title": toc_rec.get("section_title"),
            "guide_start_page": toc_rec.get("start_page"),
            "guide_end_page": toc_rec.get("end_page"),
        }
    return merged


def _hash_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def build_index(pdf_path: Path) -> Dict:
    doc = fitz.open(pdf_path)
    toc_entries = _compute_toc_page_ranges(doc)
    table_index = _parse_table_index(doc)
    table_by_section = {r["section"]: r["oracle_table"] for r in table_index}
    column_index = _parse_column_index(doc, table_by_section=table_by_section)
    table_sections = _merge_table_sections(toc_entries, table_index)

    unique_tables_in_columns = sorted({r["oracle_table"] for r in column_index})
    unique_columns = sorted({r["column_name"] for r in column_index})

    return {
        "source": {
            "pdf_file": pdf_path.name,
            "pdf_path": str(pdf_path),
            "pdf_sha256": _hash_file(pdf_path),
            "fiadb_version": "9.4",
            "guide_revision": "08.2025",
            "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        },
        "summary": {
            "toc_entries": len(toc_entries),
            "tables_index_rows": len(table_index),
            "column_index_rows": len(column_index),
            "unique_tables_in_column_index": len(unique_tables_in_columns),
            "unique_column_names": len(unique_columns),
            "pdf_page_count": doc.page_count,
        },
        "toc": toc_entries,
        "tables_index": table_index,
        "table_sections": table_sections,
        "columns_index": column_index,
    }


def main() -> None:
    here = Path(__file__).resolve().parent
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--pdf",
        type=Path,
        default=here / "wo-v9-4_Aug2025_UG_FIADB_database_description_NFI.pdf",
        help="Path to FIADB User Guide PDF",
    )
    parser.add_argument(
        "--out",
        type=Path,
        default=here / "fiadb_user_guide_index_v94.json",
        help="Output JSON path",
    )
    args = parser.parse_args()

    data = build_index(args.pdf)
    args.out.write_text(json.dumps(data, indent=2, ensure_ascii=True), encoding="utf-8")

    print(f"Wrote {args.out}")
    print(
        "Summary:",
        json.dumps(data["summary"], indent=2),
    )


if __name__ == "__main__":
    main()
