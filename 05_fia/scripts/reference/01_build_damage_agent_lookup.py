"""Build a reviewed FIA damage-agent lookup from the official v9.4 guide.

The FIADB reference CSV bundle does not currently include REF_DAMAGE_AGENT or
REF_DAMAGE_AGENT_GROUP. Appendix H of the repository-cached official guide does
contain the complete national code, name, threshold, and region table.
"""

from __future__ import annotations

import argparse
import bisect
import csv
import re
from pathlib import Path

import fitz


APPENDIX_CONTENTS_PAGE = 938  # PDF index, displayed page 939 / Appendix H-1.
APPENDIX_TABLE_PAGES = range(939, 986)  # PDF indexes, displayed pages 940-986.
CODE_RE = re.compile(r"^(\d{5})(?:\s+(RETIRED))?$")
INSECT_GROUP_CODES = {
    10000,
    11000,
    12000,
    13000,
    14000,
    15000,
    16000,
    17000,
    18000,
}


def normalize_text(value: str | None, *, empty_dash: bool = False) -> str:
    # Normalize PDF-specific characters before values are written to CSV.
    if value is None:
        return ""
    replacements = {
        "\u00a0": " ",
        "\u00ad": "",
        "\u2013": "-",
        "\u2014": "-",
        "\u2212": "-",
        "\uf0b3": ">=",
    }
    for source, replacement in replacements.items():
        value = value.replace(source, replacement)
    value = " ".join(value.split()).strip()
    if empty_dash and value in {"-", "—"}:
        return ""
    return value


def extract_group_lookup(document: fitz.Document) -> dict[int, str]:
    # Appendix H starts with the broad code ranges used to group exact agents.
    tables = document[APPENDIX_CONTENTS_PAGE].find_tables().tables
    if len(tables) != 1:
        raise RuntimeError("Expected one damage-agent group table on Appendix H-1.")
    groups: dict[int, str] = {}
    for row in tables[0].extract()[1:]:
        if not row or not row[0]:
            continue
        code = normalize_text(row[0])
        match = CODE_RE.fullmatch(code)
        if match:
            groups[int(match.group(1))] = normalize_text(row[1])
    if not groups:
        raise RuntimeError("No damage-agent groups were extracted.")
    return groups


def extract_agent_rows(
    document: fitz.Document,
    groups: dict[int, str],
) -> list[dict[str, object]]:
    group_codes = sorted(groups)
    rows: list[dict[str, object]] = []
    observed_codes: set[int] = set()

    # Extract the exact agent table from each page of Appendix H.
    for page_index in APPENDIX_TABLE_PAGES:
        tables = document[page_index].find_tables().tables
        if len(tables) != 1:
            raise RuntimeError(
                f"Expected one damage-agent table on PDF page {page_index + 1}."
            )
        for raw in tables[0].extract()[1:]:
            if not raw or not raw[0]:
                continue
            code_text = normalize_text(raw[0])
            code_match = CODE_RE.fullmatch(code_text)
            if not code_match:
                continue
            code = int(code_match.group(1))
            is_retired = code_match.group(2) == "RETIRED"
            if code in observed_codes:
                raise RuntimeError(f"Duplicate damage-agent code extracted: {code}.")
            observed_codes.add(code)

            # FIA groups each exact code under the nearest preceding group code.
            group_position = bisect.bisect_right(group_codes, code) - 1
            if group_position < 0:
                raise RuntimeError(f"No official group found for code {code}.")
            group_code = group_codes[group_position]
            # Preserve current definitions without claiming they apply historically.
            rows.append(
                {
                    "DAMAGE_AGENT_CD": code,
                    "official_label": normalize_text(raw[3]),
                    "scientific_name_or_other": normalize_text(
                        raw[4],
                        empty_dash=True,
                    ),
                    "official_group_code": group_code,
                    "agent_group": groups[group_code],
                    "is_insect_agent": (
                        "TRUE" if group_code in INSECT_GROUP_CODES else "FALSE"
                    ),
                    "threshold": normalize_text(raw[5], empty_dash=True),
                    "region": normalize_text(raw[6], empty_dash=True),
                    "source": (
                        "USDA Forest Service FIADB User Guides, Database "
                        "Description v9.4, Appendix H"
                    ),
                    "source_pdf_page": page_index + 1,
                    "definition_source_version": (
                        "FIADB Database Description 9.4 (2025-08)"
                    ),
                    "manual_version_applicability": (
                        "not_established_by_source_appendix"
                    ),
                    "code_status": (
                        "retired_as_of_manual_9.4" if is_retired else "current_in_v9.4"
                    ),
                    "review_status": "official_v9.4_definition_only",
                }
            )
    return rows


def main() -> None:
    # Use the repository copy of the official guide unless another PDF is supplied.
    repository_root = Path(__file__).resolve().parents[3]
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--pdf",
        type=Path,
        default=repository_root
        / "05_fia"
        / "docs"
        / "wo-v9-4_Aug2025_UG_FIADB_database_description_NFI.pdf",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=repository_root
        / "05_fia"
        / "lookups"
        / "fia_damage_agent_lookup.csv",
    )
    args = parser.parse_args()

    # Build broad groups first because every exact code needs its official group.
    document = fitz.open(args.pdf)
    groups = extract_group_lookup(document)
    rows = extract_agent_rows(document, groups)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    fields = list(rows[0])
    # Write a stable column order based on the reviewed row definition above.
    with args.output.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        writer.writerows(rows)
    print(f"Wrote {len(rows):,} official FIA damage-agent codes -> {args.output}")


if __name__ == "__main__":
    main()
