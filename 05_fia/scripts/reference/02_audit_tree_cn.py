"""Audit whether TREE.CN is globally unique across physical raw FIA files.

The scan is exact but memory-bounded. CN values are streamed into temporary
hash buckets, then each bucket is sorted independently to detect duplicates.
Temporary buckets are removed automatically after the QA files are written.
"""

from __future__ import annotations

import argparse
import csv
import tempfile
from pathlib import Path

import numpy as np
import pyarrow as pa
import pyarrow.csv as pacsv
import pyarrow.parquet as pq


N_BUCKETS = 64


def main() -> None:
    # Resolve paths from the repository so the audit can run from any directory.
    repository_root = Path(__file__).resolve().parents[3]
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--raw-root",
        type=Path,
        default=repository_root / "05_fia" / "data" / "raw",
    )
    parser.add_argument(
        "--qa-dir",
        type=Path,
        default=repository_root / "05_fia" / "qa" / "outputs",
    )
    args = parser.parse_args()

    # Audit every physical state TREE file rather than relying on processed products.
    tree_files = sorted(args.raw_root.glob("[A-Z][A-Z]/[A-Z][A-Z]_TREE.csv"))
    if not tree_files:
        raise FileNotFoundError(f"No raw state TREE files found under {args.raw_root}")

    args.qa_dir.mkdir(parents=True, exist_ok=True)
    scratch_parent = repository_root / "scratch_output"
    scratch_parent.mkdir(parents=True, exist_ok=True)
    state_rows: list[dict[str, object]] = []
    duplicate_examples: list[dict[str, object]] = []
    total_rows = 0

    # Temporary hash buckets keep the national audit within available memory.
    with tempfile.TemporaryDirectory(
        prefix="fia_tree_cn_audit_",
        dir=scratch_parent,
    ) as temporary:
        bucket_dir = Path(temporary)
        writers: dict[int, pq.ParquetWriter] = {}
        try:
            # Stream one state at a time so large TREE files are never fully loaded.
            for index, path in enumerate(tree_files, start=1):
                state = path.name[:2]
                state_count = 0
                state_min: int | None = None
                state_max: int | None = None
                reader = pacsv.open_csv(
                    path,
                    read_options=pacsv.ReadOptions(block_size=64 * 1024 * 1024),
                    convert_options=pacsv.ConvertOptions(
                        include_columns=["CN"],
                        column_types={"CN": pa.int64()},
                    ),
                )
                # Arrow supplies bounded batches of the single identifier column.
                for batch in reader:
                    cn = batch.column(0).to_numpy(zero_copy_only=False)
                    if cn.size == 0:
                        continue
                    state_count += int(cn.size)
                    total_rows += int(cn.size)
                    batch_min = int(cn.min())
                    batch_max = int(cn.max())
                    state_min = (
                        batch_min if state_min is None else min(state_min, batch_min)
                    )
                    state_max = (
                        batch_max if state_max is None else max(state_max, batch_max)
                    )
                    # Equal identifiers always enter the same bucket for exact comparison.
                    bucket_ids = np.bitwise_and(cn, N_BUCKETS - 1)
                    for bucket_id in np.unique(bucket_ids):
                        values = cn[bucket_ids == bucket_id]
                        table = pa.table(
                            {
                                "CN": pa.array(values, type=pa.int64()),
                                "state": pa.array([state] * len(values)),
                            }
                        )
                        bucket = int(bucket_id)
                        if bucket not in writers:
                            writers[bucket] = pq.ParquetWriter(
                                bucket_dir / f"bucket_{bucket:02d}.parquet",
                                table.schema,
                                compression="snappy",
                            )
                        writers[bucket].write_table(table)
                state_rows.append(
                    {
                        "state": state,
                        "raw_tree_rows": state_count,
                        "min_TREE_CN": state_min,
                        "max_TREE_CN": state_max,
                    }
                )
                print(
                    f"[{index}/{len(tree_files)}] {state}: "
                    f"{state_count:,} TREE rows"
                )
        finally:
            for writer in writers.values():
                writer.close()

        # Sort each bucket and detect duplicate identifiers as adjacent values.
        duplicate_cn_count = 0
        duplicate_row_count = 0
        for bucket_id in range(N_BUCKETS):
            bucket_path = bucket_dir / f"bucket_{bucket_id:02d}.parquet"
            if not bucket_path.exists():
                continue
            table = pq.read_table(bucket_path, columns=["CN", "state"])
            cn = table["CN"].to_numpy(zero_copy_only=False)
            order = np.argsort(cn)
            sorted_cn = cn[order]
            duplicated_adjacent = sorted_cn[1:] == sorted_cn[:-1]
            if not duplicated_adjacent.any():
                continue
            # Count duplicate values and retain a small example set for review.
            values, counts = np.unique(sorted_cn, return_counts=True)
            values = values[counts > 1]
            counts = counts[counts > 1]
            duplicate_cn_count += int(len(values))
            duplicate_row_count += int(counts.sum())
            states = table["state"].to_numpy(zero_copy_only=False)
            for value, count in zip(values[:20], counts[:20]):
                matching_states = sorted(set(states[cn == value].tolist()))
                duplicate_examples.append(
                    {
                        "TREE_CN": int(value),
                        "n_rows": int(count),
                        "states": ";".join(matching_states),
                    }
                )

    # Write per-state row ranges separately from the national uniqueness result.
    state_path = args.qa_dir / "fia_tree_cn_global_audit_by_state.csv"
    with state_path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(state_rows[0]))
        writer.writeheader()
        writer.writerows(state_rows)

    summary_path = args.qa_dir / "fia_tree_cn_global_audit_summary.csv"
    summary = [
        {"metric": "raw_tree_files", "value": len(tree_files)},
        {"metric": "raw_tree_rows", "value": total_rows},
        {"metric": "duplicate_TREE_CN_values", "value": duplicate_cn_count},
        {"metric": "rows_with_duplicated_TREE_CN", "value": duplicate_row_count},
        {
            "metric": "TREE_CN_globally_unique",
            "value": str(duplicate_cn_count == 0).upper(),
        },
    ]
    with summary_path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=["metric", "value"])
        writer.writeheader()
        writer.writerows(summary)

    # Keep example duplicates empty when the identifier is globally unique.
    examples_path = args.qa_dir / "fia_tree_cn_duplicate_examples.csv"
    with examples_path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=["TREE_CN", "n_rows", "states"],
        )
        writer.writeheader()
        writer.writerows(duplicate_examples)

    print(f"Summary -> {summary_path}")
    print(f"TREE.CN globally unique: {duplicate_cn_count == 0}")


if __name__ == "__main__":
    main()
