#!/usr/bin/env python3
"""
Parent/Child cleaning for the stitched wide panel.

Safest policy (Option A):
- Keep all UNITID-year rows.
- If a PRCH_* flag indicates CHILD, null out only that component's columns.
"""
from __future__ import annotations

import argparse
from pathlib import Path
from typing import Callable

import pandas as pd
import pyarrow as pa
import pyarrow.dataset as ds
import pyarrow.parquet as pq


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--input", required=True, help="Input stitched wide parquet")
    p.add_argument("--output", required=True, help="Output cleaned parquet")
    p.add_argument("--dictionary", required=True, help="dictionary_lake.parquet")
    p.add_argument("--qc-dir", default=None, help="Write QC summaries here")
    p.add_argument("--batch-rows", type=int, default=100_000, help="Batch size for streaming")
    return p.parse_args()


def mode(series: pd.Series) -> str:
    s = series.dropna()
    if s.empty:
        return ""
    return s.mode().iat[0]


def build_var_source_map(dictionary_path: Path) -> dict[str, str]:
    df = pd.read_parquet(dictionary_path, columns=["varname", "source_file"])
    df["varname"] = df["varname"].fillna("").astype(str).str.strip().str.upper()
    df["source_file"] = df["source_file"].fillna("").astype(str).str.strip()
    df = df[df["varname"] != ""]
    return df.groupby("varname")["source_file"].agg(mode).to_dict()


def main() -> None:
    args = parse_args()
    in_path = Path(args.input)
    out_path = Path(args.output)
    out_path.parent.mkdir(parents=True, exist_ok=True)

    var_to_source = build_var_source_map(Path(args.dictionary))

    dataset = ds.dataset(str(in_path), format="parquet")
    all_cols = dataset.schema.names
    prch_flags = [c for c in all_cols if c.upper().startswith("PRCH")]

    # Component source_file groups
    group_sets: dict[str, set[str]] = {
        "PRCH_ADM": {"ADM"},
        "PRCH_AL": {"AL"},
        "PRCH_C": {"C_A", "C_B", "C_C", "CDEP"},
        "PRCH_COS": {"COST"},
        "PRCH_E12": {"E12"},
        "PRCH_EAP": {"EAP"},
        "PRCH_EF": {"EFA", "EFA_DIST", "EFB", "EFC", "EFCP", "EFFY", "EFFY_DIST", "EFIA"},
        "PRCH_F": {"F_F", "F_FA", "F_FA_F", "F_FA_G"},
        "PRCH_GR": {"GR", "GR_PELL_SSL"},
        "PRCH_GR2": {"GR200"},
        "PRCH_HR": {"EAP", "SAL_A", "SAL_A_LT", "SAL_B", "SAL_FACULTY", "SAL_IS"},
        "PRCH_OM": {"OM"},
        "PRCH_S": {"S_ABD", "S_CN", "S_F", "S_G", "S_IS", "S_NH", "S_OC", "S_SIS"},
        "PRCH_SA": {"SFA", "SFAV"},
        "PRCH_SFA": {"SFA", "SFAV"},
    }

    # Extra predicates for fuzzy matching
    group_pred: dict[str, Callable[[str], bool]] = {
        "PRCH_F": lambda sf: sf.startswith("F"),
        "PRCH_EF": lambda sf: sf.startswith("EF") or sf.startswith("EFFY"),
        "PRCH_S": lambda sf: sf.startswith("S_"),
    }

    # Build column lists per flag
    flag_cols: dict[str, list[str]] = {f: [] for f in group_sets}
    for col in all_cols:
        if col in prch_flags:
            continue
        sf = var_to_source.get(col.upper(), "")
        if not sf:
            continue
        for flag, sset in group_sets.items():
            if sf in sset:
                flag_cols[flag].append(col)
        # predicate-based adds
        for flag, pred in group_pred.items():
            if pred(sf):
                flag_cols[flag].append(col)

    # de-duplicate columns per flag
    flag_cols = {k: sorted(set(v)) for k, v in flag_cols.items() if v}

    qc_counts: dict[tuple[int, str], int] = {}

    writer = None
    batches = dataset.to_batches(batch_size=args.batch_rows)
    for batch in batches:
        df = batch.to_pandas()
        for flag, cols in flag_cols.items():
            if flag not in df.columns:
                continue
            # child logic
            flag_num = pd.to_numeric(df[flag], errors="coerce")
            if flag == "PRCH_F":
                child_mask = flag_num.isin([2, 3, 5])
            else:
                child_mask = flag_num.isin([2])
            if not child_mask.any():
                continue
            df.loc[child_mask, cols] = pd.NA
            # QC counts by year
            if "year" in df.columns:
                counts = df.loc[child_mask, "year"].value_counts()
                for y, cnt in counts.items():
                    key = (int(y), flag)
                    qc_counts[key] = qc_counts.get(key, 0) + int(cnt)

        table = pa.Table.from_pandas(df, preserve_index=False)
        if writer is None:
            writer = pq.ParquetWriter(out_path, table.schema, compression="snappy")
        writer.write_table(table)

    if writer:
        writer.close()
        print(f"Wrote cleaned panel: {out_path}")

    if args.qc_dir:
        qc_dir = Path(args.qc_dir)
        qc_dir.mkdir(parents=True, exist_ok=True)
        rows = [
            {"year": y, "flag": flag, "child_rows": cnt}
            for (y, flag), cnt in sorted(qc_counts.items())
        ]
        pd.DataFrame(rows).to_csv(qc_dir / "prch_clean_summary.csv", index=False)
        # also record which columns were cleaned per flag
        col_rows = []
        for flag, cols in flag_cols.items():
            for c in cols:
                col_rows.append({"flag": flag, "column": c})
        pd.DataFrame(col_rows).to_csv(qc_dir / "prch_clean_columns.csv", index=False)
        print(f"QC written to {qc_dir}")


if __name__ == "__main__":
    main()
