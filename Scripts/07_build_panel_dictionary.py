#!/usr/bin/env python3
"""
Build a panel-specific variable dictionary from a stitched wide parquet.

The output is a CSV keyed to the actual columns present in a panel and combines:
- panel column order
- actual parquet data types
- dictionary-lake metadata such as varTitle, longDescription, and DataType
"""
from __future__ import annotations

import argparse
import os
from pathlib import Path

import pandas as pd
import pyarrow.parquet as pq


REPO_ROOT = Path(os.environ.get("IPEDS_ROOT", Path(__file__).resolve().parents[1]))


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--input", required=True, help="Input stitched wide parquet")
    p.add_argument("--dictionary", required=True, help="dictionary_lake.parquet")
    p.add_argument("--output", required=True, help="Output CSV path")
    return p.parse_args()


def best_text(series: pd.Series) -> str:
    vals = [str(v).strip() for v in series.dropna().tolist()]
    vals = [v for v in vals if v and v.lower() not in {"nan", "none", "<na>", "nat"}]
    if not vals:
        return ""
    vals = sorted(set(vals), key=lambda x: (-len(x), x))
    return vals[0]


def summarize_dictionary(path: Path) -> pd.DataFrame:
    cols = ["varname", "varTitle", "longDescription", "DataType"]
    df = pd.read_parquet(path, columns=cols)
    for col in cols:
        if col not in df.columns:
            df[col] = ""
    df["varname"] = df["varname"].fillna("").astype(str).str.upper().str.strip()
    df = df[df["varname"] != ""].copy()
    out = (
        df.groupby("varname", as_index=False)
        .agg(
            varTitle=("varTitle", best_text),
            longDescription=("longDescription", best_text),
            dictionaryDataType=("DataType", best_text),
        )
    )
    return out


def panel_schema_df(path: Path) -> pd.DataFrame:
    schema = pq.read_schema(path)
    rows = []
    for idx, name in enumerate(schema.names):
        field = schema.field(name)
        rows.append(
            {
                "column_order": idx,
                "varname": str(name).upper().strip(),
                "panelDataType": str(field.type),
            }
        )
    return pd.DataFrame(rows)


def main() -> None:
    args = parse_args()
    input_path = Path(args.input)
    dictionary_path = Path(args.dictionary)
    out_path = Path(args.output)
    out_path.parent.mkdir(parents=True, exist_ok=True)

    schema_df = panel_schema_df(input_path)
    dict_df = summarize_dictionary(dictionary_path)
    ref = schema_df.merge(dict_df, on="varname", how="left")

    # Controlled metadata for stitched-wide panel keys.
    ref.loc[ref["varname"] == "YEAR", "varTitle"] = ref.loc[ref["varname"] == "YEAR", "varTitle"].fillna("").replace("", "IPEDS reporting year")
    ref.loc[ref["varname"] == "YEAR", "longDescription"] = (
        ref.loc[ref["varname"] == "YEAR", "longDescription"]
        .fillna("")
        .replace("", "IPEDS reporting year carried by the stitched institution-year panel.")
    )

    for col in ["varTitle", "longDescription", "dictionaryDataType"]:
        ref[col] = ref[col].fillna("").astype(str)

    ref = ref[["column_order", "varname", "varTitle", "longDescription", "panelDataType", "dictionaryDataType"]]
    ref.to_csv(out_path, index=False)
    print(f"Wrote {len(ref):,} rows to {out_path}")


if __name__ == "__main__":
    main()
