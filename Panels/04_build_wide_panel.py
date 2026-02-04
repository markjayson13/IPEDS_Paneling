#!/usr/bin/env python3
"""
Build a wide institution–year panel from the stitched long panel.

Design choices:
- Observed spine: only UNITID–year pairs present in the long data are included.
- Year-by-year processing to stay RAM‑friendly.
- Columns: varname becomes the wide columns; values are kept as strings.
- Optional discrete-category collapse (LEVEL1..LEVELn -> LEVEL_CAT) with QC outputs.
"""

from __future__ import annotations

import argparse
import os
import re
from typing import Iterable, List
import sys
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.dataset as ds
import pyarrow.parquet as pq


def setup_logging(log_path: str | None) -> None:
    if not log_path:
        return
    log_file = Path(log_path)
    log_file.parent.mkdir(parents=True, exist_ok=True)
    f = log_file.open("a", buffering=1)

    class Tee:
        def __init__(self, *streams):
            self.streams = streams

        def write(self, data):
            for s in self.streams:
                s.write(data)

        def flush(self):
            for s in self.streams:
                s.flush()

    sys.stdout = Tee(sys.stdout, f)
    sys.stderr = Tee(sys.stderr, f)


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


def order_targets(targets: Iterable[str]) -> list[str]:
    """
    Order columns so each imputation variable (X<var>) follows its base var.
    Remaining vars are appended alphabetically.
    """
    target_set = set(targets)
    non_x = sorted([t for t in target_set if not t.startswith("X")])
    ordered: list[str] = []
    for base in non_x:
        ordered.append(base)
        xvar = f"X{base}"
        if xvar in target_set:
            ordered.append(xvar)
    remaining = sorted([t for t in target_set if t not in ordered])
    ordered.extend(remaining)
    return ordered


def build_disc_groups(dict_path: str) -> tuple[dict[str, tuple[str, str]], dict[str, list[str]]]:
    """
    Build discrete var groups using dictionary metadata.
    Groups are based on a shared base name with trailing digits, e.g. LEVEL1..LEVEL9 -> LEVEL.
    Returns:
      - var_to_group: varname -> (base, suffix)
      - group_to_vars: base -> [varnames...]
    """
    if not dict_path:
        return {}, {}
    ddf = pd.read_parquet(dict_path)
    ddf.columns = [c.strip() for c in ddf.columns]
    name_col = "varname" if "varname" in ddf.columns else None
    dtype_col = "DataType" if "DataType" in ddf.columns else None
    fmt_col = "format" if "format" in ddf.columns else None
    if not name_col:
        return {}, {}

    def is_disc(row) -> bool:
        dt = str(row.get(dtype_col, "") or "").strip().lower() if dtype_col else ""
        fmt = str(row.get(fmt_col, "") or "").strip().lower() if fmt_col else ""
        return dt == "disc" or fmt == "disc"

    disc_names = ddf[ddf.apply(is_disc, axis=1)][name_col].dropna().astype(str).str.upper().unique()
    var_to_group: dict[str, tuple[str, str]] = {}
    group_to_vars: dict[str, list[str]] = {}
    for v in disc_names:
        m = re.match(r"^(.*?)(\d+)$", v)
        if not m:
            continue
        base, suffix = m.group(1), m.group(2)
        if not base:
            continue
        var_to_group[v] = (base, suffix)
        group_to_vars.setdefault(base, []).append(v)
    # only keep groups with 2+ vars
    group_to_vars = {k: sorted(vs) for k, vs in group_to_vars.items() if len(vs) >= 2}
    var_to_group = {v: grp for v, grp in var_to_group.items() if grp[0] in group_to_vars}
    return var_to_group, group_to_vars


def resolve_disc_names(
    group_to_vars: dict[str, list[str]],
    existing: set[str],
    suffix: str = "_CAT",
) -> dict[str, str]:
    """
    For each disc group base, pick a unique output name.
    If base already exists as an independent var, append a numeric suffix (base1, base2, ...).
    """
    mapping: dict[str, str] = {}
    taken = set(existing)
    for base in sorted(group_to_vars):
        if base not in taken:
            mapping[base] = base
            taken.add(base)
            continue
        base_suffix = f"{base}{suffix}"
        if base_suffix not in taken:
            mapping[base] = base_suffix
            taken.add(base_suffix)
            continue
        i = 1
        while True:
            cand = f"{base}{suffix}{i}"
            if cand not in taken:
                mapping[base] = cand
                taken.add(cand)
                break
            i += 1
    return mapping


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--input", required=True, help="Stitched LONG panel parquet")
    ap.add_argument("--out_dir", required=True, help="Output dir for year-partitioned wide parquet")
    ap.add_argument("--years", required=True, help='Year span, e.g. "1987:2024"')
    ap.add_argument("--write_single", default=None, help="Optional single wide parquet path")
    ap.add_argument("--dictionary", default=None, help="Optional dictionary_lake.parquet for disc grouping")
    ap.add_argument("--collapse-disc", action="store_true", help="Collapse discrete (disc) groups into a base var")
    ap.add_argument("--drop-disc-components", action="store_true", help="Drop component vars after collapse")
    ap.add_argument("--disc-qc-dir", default=None, help="Optional dir to write disc conflict reports")
    ap.add_argument("--disc-exclude", default=None, help="Comma-separated base names to skip collapsing (e.g., LEVEL,ADMCON)")
    ap.add_argument("--disc-suffix", default="_CAT", help="Suffix used when base name collides with an existing variable")
    ap.add_argument("--dups-qc-dir", default=None, help="Optional dir to write duplicate key samples")
    ap.add_argument("--dups-max-rows", type=int, default=10000, help="Max rows to write for duplicate samples (0 disables)")
    ap.add_argument("--dups-qc-gzip", action="store_true", help="Write dup samples as .csv.gz")
    ap.add_argument("--qc-dir", default=None, help="Optional dir to write QC summary CSV")
    repo_root = pathlib.Path(os.environ.get("IPEDS_ROOT", pathlib.Path(__file__).resolve().parents[1]))
    ap.add_argument("--log-file", default=str(repo_root / "Checks" / "logs" / "04_build_wide_panel.log"), help="Optional log file path")
    args = ap.parse_args()

    setup_logging(args.log_file)

    os.makedirs(args.out_dir, exist_ok=True)
    years = parse_years(args.years)
    var_to_group, group_to_vars = ({}, {})
    if args.collapse_disc:
        var_to_group, group_to_vars = build_disc_groups(args.dictionary)
        if args.disc_exclude:
            excludes = {x.strip().upper() for x in args.disc_exclude.split(",") if x.strip()}
            if excludes:
                group_to_vars = {k: v for k, v in group_to_vars.items() if k.upper() not in excludes}
                var_to_group = {v: grp for v, grp in var_to_group.items() if grp[0].upper() not in excludes}
        if args.disc_qc_dir:
            os.makedirs(args.disc_qc_dir, exist_ok=True)

    dataset = ds.dataset(args.input, format="parquet")
    schema = dataset.schema

    unitid_col = pick_col(schema, ["UNITID", "unitid"])
    year_col = pick_col(schema, ["year", "academicyear"])
    target_col = pick_col(schema, ["varname", "target_var", "concept", "target"])
    value_col = pick_col(schema, ["value", "val"])

    def rename_cols(df: pd.DataFrame) -> pd.DataFrame:
        mapping = {
            unitid_col: "UNITID",
            year_col: "year",
            target_col: "varname",
            value_col: "value",
        }
        return df.rename(columns=mapping)

    # Collect universe of varnames across requested years
    targets = set()
    for y in years:
        filt = (ds.field(year_col) == y) & ds.field(target_col).is_valid()
        tbl = dataset.to_table(columns=[target_col], filter=filt)
        df = rename_cols(tbl.to_pandas())
        targets.update(df["varname"].dropna().unique().tolist())

    all_targets = order_targets(targets)
    disc_name_map = {}
    if args.collapse_disc and group_to_vars:
        disc_name_map = resolve_disc_names(group_to_vars, set(all_targets), suffix=args.disc_suffix)
        for base, new_name in disc_name_map.items():
            if new_name not in all_targets:
                all_targets.append(new_name)
        if args.drop_disc_components:
            # remove component vars from output columns
            components = {v for vs in group_to_vars.values() for v in vs}
            all_targets = [t for t in all_targets if t not in components]
    schema_wide = pa.schema(
        [pa.field("year", pa.int32()), pa.field("UNITID", pa.int64())]
        + [pa.field(t, pa.string()) for t in all_targets]
    )
    year_part_paths: list[str] = []
    qc_rows: list[dict] = []

    print(f"[info] years: {years[0]}–{years[-1]} ({len(years)} total)")
    print(f"[info] wide columns (varname): {len(all_targets)}")

    for y in years:
        print(f"[info] building wide for year={y}")
        # Observed spine (UNITID-year rows present in long data)
        spine_tbl = dataset.to_table(
            columns=[unitid_col, year_col],
            filter=(ds.field(year_col) == y),
        )
        spine = rename_cols(spine_tbl.to_pandas()).dropna(subset=["UNITID", "year"])
        spine = spine.drop_duplicates(subset=["UNITID", "year"])
        print(f"[info] year={y} spine rows: {len(spine)}")

        # Concept rows for pivot
        concept_cols = [unitid_col, year_col, target_col, value_col]
        concept_tbl = dataset.to_table(
            columns=concept_cols,
            filter=(ds.field(year_col) == y) & ds.field(target_col).is_valid(),
        )
        concept = rename_cols(concept_tbl.to_pandas())
        concept = concept.dropna(subset=["UNITID", "year", "varname"])
        print(f"[info] year={y} concept rows: {len(concept)}")

        # Collapse discrete groups into a single base variable (optional)
        if args.collapse_disc and var_to_group:
            disc_rows = concept[concept["varname"].isin(var_to_group)]
            if not disc_rows.empty:
                disc_rows = disc_rows.copy()
                disc_rows["base"] = disc_rows["varname"].map(lambda v: var_to_group.get(v, ("", ""))[0])
                disc_rows["suffix"] = disc_rows["varname"].map(lambda v: var_to_group.get(v, ("", ""))[1])
                # only keep rows with a value
                disc_rows = disc_rows[disc_rows["value"].notna() & (disc_rows["value"] != "")]
                # detect conflicts (more than one active per group)
                counts = disc_rows.groupby(["UNITID", "year", "base"]).size().reset_index(name="n")
                conflict_bases = set(counts.loc[counts["n"] > 1, "base"])
                if conflict_bases and args.disc_qc_dir:
                    conflict_rows = disc_rows[disc_rows["base"].isin(conflict_bases)]
                    conflict_rows.to_csv(os.path.join(args.disc_qc_dir, f"disc_conflicts_{y}.csv"), index=False)
                # keep only non-conflicting groups
                ok = disc_rows[~disc_rows["base"].isin(conflict_bases)]
                if not ok.empty:
                    combined = ok.groupby(["UNITID", "year", "base"], as_index=False).first()
                    combined["varname"] = combined["base"].map(lambda b: disc_name_map.get(b, b))
                    # store the suffix code (e.g., LEVEL1 -> 1)
                    combined["value"] = combined["suffix"]
                    combined = combined[["UNITID", "year", "varname", "value"]]
                    if args.drop_disc_components:
                        concept = concept[~concept["varname"].isin(ok["varname"].unique())]
                    concept = pd.concat([concept, combined], ignore_index=True)

        # log duplicates if any, then keep first
        dup_mask = concept.duplicated(subset=["UNITID", "year", "varname"], keep=False)
        dup_count = int(dup_mask.sum())
        if dup_mask.any() and args.dups_qc_dir and args.dups_max_rows > 0:
            os.makedirs(args.dups_qc_dir, exist_ok=True)
            ext = ".csv.gz" if args.dups_qc_gzip else ".csv"
            dup_path = os.path.join(args.dups_qc_dir, f"dups_{y}{ext}")
            dup_sample = concept.loc[dup_mask].head(args.dups_max_rows)
            dup_sample.to_csv(dup_path, index=False)
        concept = concept.drop_duplicates(subset=["UNITID", "year", "varname"], keep="first")

        if len(concept) > 0:
            wide = concept.pivot(index=["year", "UNITID"], columns="varname", values="value").reset_index()
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
        print(f"[info] wrote {out_path}")

        if args.qc_dir:
            os.makedirs(args.qc_dir, exist_ok=True)
            n_spine = int(len(spine))
            n_vars = int(len(all_targets))
            non_empty = int(((concept["value"].notna()) & (concept["value"] != "")).sum())
            possible = n_spine * n_vars if n_spine and n_vars else 0
            fill_rate = (non_empty / possible) if possible else 0.0
            qc_rows.append(
                {
                    "year": y,
                    "rows": n_spine,
                    "vars": n_vars,
                    "non_empty_values": non_empty,
                    "fill_rate": fill_rate,
                    "dup_rows": dup_count,
                }
            )

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

    if args.qc_dir and qc_rows:
        qc_path = os.path.join(args.qc_dir, "wide_panel_qc_summary.csv")
        pd.DataFrame(qc_rows).to_csv(qc_path, index=False)


if __name__ == "__main__":
    main()
