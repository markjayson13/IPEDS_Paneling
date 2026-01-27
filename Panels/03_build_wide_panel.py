#!/usr/bin/env python3
"""
Build a wide institution–year panel from the stitched long panel.

Design choices:
- Observed spine: only UNITID–year pairs present in the long data are included.
- Accepted-only optional filter to keep “winning” matches.
- Year-by-year processing to stay RAM‑friendly.
"""

from __future__ import annotations

import argparse
import os
from typing import Iterable

import pandas as pd
import pyarrow as pa
import pyarrow.dataset as ds
import pyarrow.parquet as pq


def parse_years(spec: str) -> list[int]:
    start, end = spec.split(":")
    return list(range(int(start), int(end) + 1))


def pick_col(schema: pa.Schema, candidates: Iterable[str]) -> str:
    for c in candidates:
        if c in schema.names:
            return c
    raise ValueError(f"None of {candidates} found in schema. Columns: {schema.names}")


def ensure_all_target_cols(df: pd.DataFrame, targets: list[str]) -> pd.DataFrame:
    # Reindex instead of per-column insert to avoid fragmentation warnings
    cols = ["year", "UNITID"] + targets
    return df.reindex(columns=cols)


def coerce_types(df: pd.DataFrame, targets: list[str]) -> pd.DataFrame:
    df["year"] = pd.to_numeric(df["year"], errors="coerce").astype("Int32")
    df["UNITID"] = pd.to_numeric(df["UNITID"], errors="coerce").astype("Int64")
    if targets:
        df[targets] = df[targets].apply(pd.to_numeric, errors="coerce")
    return df


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--input", required=True, help="Stitched LONG panel parquet")
    ap.add_argument("--out_dir", required=True, help="Output dir for year-partitioned wide parquet")
    ap.add_argument("--years", required=True, help='Year span, e.g. "1987:2024"')
    ap.add_argument("--accepted_only", action="store_true", help="Use only accepted rows if column exists")
    ap.add_argument("--write_single", default=None, help="Optional single wide parquet path")
    args = ap.parse_args()

    os.makedirs(args.out_dir, exist_ok=True)
    years = parse_years(args.years)

    dataset = ds.dataset(args.input, format="parquet")
    schema = dataset.schema

    unitid_col = pick_col(schema, ["UNITID", "unitid"])
    year_col = pick_col(schema, ["year", "academicyear"])
    target_col = pick_col(schema, ["target_var", "concept", "target"])
    value_col = pick_col(schema, ["value", "val"])
    accepted_col = "accepted" if "accepted" in schema.names else None

    def rename_cols(df: pd.DataFrame) -> pd.DataFrame:
        mapping = {
            unitid_col: "UNITID",
            year_col: "year",
            target_col: "target_var",
            value_col: "value",
        }
        if accepted_col:
            mapping[accepted_col] = "accepted"
        return df.rename(columns=mapping)

    # Collect universe of target_vars across requested years (independent of accepted flag)
    targets = set()
    for y in years:
        filt = (ds.field(year_col) == y) & ds.field(target_col).is_valid()
        cols = [target_col]
        if accepted_col:
            cols.append(accepted_col)
        tbl = dataset.to_table(columns=cols, filter=filt)
        df = rename_cols(tbl.to_pandas())
        targets.update(df["target_var"].dropna().unique().tolist())

    all_targets = sorted(targets)
    schema_wide = pa.schema(
        [pa.field("year", pa.int32()), pa.field("UNITID", pa.int64())]
        + [pa.field(t, pa.float64()) for t in all_targets]
    )
    year_part_paths: list[str] = []

    for y in years:
        # Observed spine (UNITID-year rows present in long data)
        spine_tbl = dataset.to_table(
            columns=[unitid_col, year_col],
            filter=(ds.field(year_col) == y),
        )
        spine = rename_cols(spine_tbl.to_pandas()).dropna(subset=["UNITID", "year"])
        spine = spine.drop_duplicates(subset=["UNITID", "year"])

        # Concept rows for pivot
        concept_cols = [unitid_col, year_col, target_col, value_col]
        if accepted_col:
            concept_cols.append(accepted_col)

        concept_tbl = dataset.to_table(
            columns=concept_cols,
            filter=(ds.field(year_col) == y) & ds.field(target_col).is_valid(),
        )
        concept = rename_cols(concept_tbl.to_pandas())
        if args.accepted_only and "accepted" in concept.columns:
            concept = concept[concept["accepted"] == True]  # noqa: E712
        concept = concept.dropna(subset=["UNITID", "year", "target_var"])

        # log duplicates if any, then keep first
        dup_mask = concept.duplicated(subset=["UNITID", "year", "target_var"], keep=False)
        if dup_mask.any():
            dup_path = os.path.join(args.out_dir, f"dups_{y}.csv")
            concept.loc[dup_mask].to_csv(dup_path, index=False)
        concept = concept.drop_duplicates(subset=["UNITID", "year", "target_var"], keep="first")

        if len(concept) > 0:
            wide = concept.pivot(index=["year", "UNITID"], columns="target_var", values="value").reset_index()
        else:
            wide = spine.copy()

        # Merge to keep spine rows even if no concept values
        wide = spine.merge(wide, on=["year", "UNITID"], how="left")
        wide = ensure_all_target_cols(wide, all_targets)
        wide = coerce_types(wide, all_targets)

        out_path = os.path.join(args.out_dir, f"year={y}", "part.parquet")
        os.makedirs(os.path.dirname(out_path), exist_ok=True)
        tbl = pa.Table.from_pandas(wide, preserve_index=False).cast(schema_wide)
        pq.write_table(tbl, out_path)
        year_part_paths.append(out_path)

    # single-file write
    if args.write_single:
        writer = None
        for p in year_part_paths:
            # Read each file directly (no dataset merge) to avoid dictionary/int conflicts
            t = pq.ParquetFile(p).read().cast(schema_wide, safe=False)
            if writer is None:
                writer = pq.ParquetWriter(args.write_single, schema_wide)
            writer.write_table(t)
        writer.close()


if __name__ == "__main__":
    main()
