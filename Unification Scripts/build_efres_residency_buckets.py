#!/usr/bin/env python3
"""Build EF residence buckets for FTFT undergraduate students."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

import pandas as pd

US_CODES = {
    "AL",
    "AK",
    "AZ",
    "AR",
    "CA",
    "CO",
    "CT",
    "DE",
    "DC",
    "FL",
    "GA",
    "HI",
    "ID",
    "IL",
    "IN",
    "IA",
    "KS",
    "KY",
    "LA",
    "ME",
    "MD",
    "MA",
    "MI",
    "MN",
    "MS",
    "MO",
    "MT",
    "NE",
    "NV",
    "NH",
    "NJ",
    "NM",
    "NY",
    "NC",
    "ND",
    "OH",
    "OK",
    "OR",
    "PA",
    "RI",
    "SC",
    "SD",
    "TN",
    "TX",
    "UT",
    "VT",
    "VA",
    "WA",
    "WV",
    "WI",
    "WY",
    "PR",
    "VI",
    "GU",
    "AS",
    "MP",
}

BUCKET_COLS = {
    "INSTATE": "EF_RES_FTFT_UG_INSTATE",
    "OUTSTATE": "EF_RES_FTFT_UG_OUTSTATE",
    "FOREIGN": "EF_RES_FTFT_UG_FOREIGN",
    "UNKNOWN": "EF_RES_FTFT_UG_RES_UNKNOWN",
}

DEFAULT_EFRES = Path(
    "/Users/markjaysonfarol13/Higher Ed research/IPEDS/Parquets/Unify/Enrolllong/efres_long.parquet"
)
DEFAULT_HD = Path(
    "/Users/markjaysonfarol13/Higher Ed research/IPEDS/Parquets/Unify/HD/hd_state_panel.parquet"
)
DEFAULT_OUTPUT = Path(
    "/Users/markjaysonfarol13/Higher Ed research/IPEDS/Parquets/Unify/Enrolllong/efres_residency_buckets.parquet"
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--efres",
        type=Path,
        default=DEFAULT_EFRES,
        help=f"EF residence long file (csv/parquet). Default: {DEFAULT_EFRES}",
    )
    parser.add_argument(
        "--hd",
        type=Path,
        default=DEFAULT_HD,
        help=f"HD file with institution state codes. Default: {DEFAULT_HD}",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=DEFAULT_OUTPUT,
        help=f"Output parquet path. Default: {DEFAULT_OUTPUT}",
    )
    parser.add_argument("--year-col", default="YEAR")
    parser.add_argument("--unitid-col", default="UNITID")
    parser.add_argument("--res-col", default="RES_STATE")
    parser.add_argument("--count-col", default="FTFT_UG_COUNT")
    return parser.parse_args()


def _read_table(path: Path) -> pd.DataFrame:
    if not path.exists():
        raise RuntimeError(f"Input file not found: {path}")
    if path.suffix.lower() == ".parquet":
        return pd.read_parquet(path)
    if path.suffix.lower() == ".csv":
        return pd.read_csv(path)
    raise RuntimeError(f"Unsupported file type for {path}")


def load_efres(path: Path, year_col: str, unitid_col: str, res_col: str, count_col: str) -> pd.DataFrame:
    df = _read_table(path)
    needed = {year_col, unitid_col, res_col, count_col}
    missing = needed - set(df.columns)
    if missing:
        raise RuntimeError(f"EFRES file missing columns: {sorted(missing)}")
    df = df[[year_col, unitid_col, res_col, count_col]].copy()
    df.rename(
        columns={
            year_col: "YEAR",
            unitid_col: "UNITID",
            res_col: "RES_STATE",
            count_col: "FTFT_UG_COUNT",
        },
        inplace=True,
    )
    df["YEAR"] = pd.to_numeric(df["YEAR"], errors="coerce").astype("Int64")
    df["UNITID"] = pd.to_numeric(df["UNITID"], errors="coerce").astype("Int64")
    df["RES_STATE"] = df["RES_STATE"].astype(str).str.strip().str.upper()
    df.loc[df["RES_STATE"].isin({"", "nan", "none"}), "RES_STATE"] = ""
    df["FTFT_UG_COUNT"] = pd.to_numeric(df["FTFT_UG_COUNT"], errors="coerce").fillna(0)
    if df.empty:
        raise RuntimeError("EFRES file has no rows after filtering required columns.")
    return df


def load_hd(path: Path, year_col: str, unitid_col: str) -> pd.DataFrame:
    df = _read_table(path)
    needed = {year_col, unitid_col, "INST_STATE"}
    missing = needed - set(df.columns)
    if missing:
        raise RuntimeError(f"HD file missing columns: {sorted(missing)}")
    df = df[[year_col, unitid_col, "INST_STATE"]].copy()
    df.rename(columns={year_col: "YEAR", unitid_col: "UNITID"}, inplace=True)
    df["YEAR"] = pd.to_numeric(df["YEAR"], errors="coerce").astype("Int64")
    df["UNITID"] = pd.to_numeric(df["UNITID"], errors="coerce").astype("Int64")
    df["INST_STATE"] = df["INST_STATE"].astype(str).str.strip().str.upper()
    df.loc[df["INST_STATE"].isin({"", "nan", "none"}), "INST_STATE"] = ""
    return df


def classify_residency(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    inst_known = df["INST_STATE"].isin(US_CODES)
    res_nonempty = df["RES_STATE"] != ""
    res_in_us = df["RES_STATE"].isin(US_CODES)

    df["bucket"] = "UNKNOWN"
    mask_instate = res_nonempty & inst_known & df["RES_STATE"].eq(df["INST_STATE"])
    df.loc[mask_instate, "bucket"] = "INSTATE"

    mask_foreign = res_nonempty & ~res_in_us
    df.loc[mask_foreign, "bucket"] = "FOREIGN"

    mask_outstate = res_nonempty & res_in_us & inst_known & df["RES_STATE"].ne(df["INST_STATE"])
    df.loc[mask_outstate, "bucket"] = "OUTSTATE"

    return df


def aggregate_buckets(df: pd.DataFrame) -> pd.DataFrame:
    pivot = (
        df.pivot_table(
            index=["YEAR", "UNITID"],
            columns="bucket",
            values="FTFT_UG_COUNT",
            aggfunc="sum",
            fill_value=0,
        )
        .reset_index()
    )
    for bucket, col in BUCKET_COLS.items():
        if bucket not in pivot.columns:
            pivot[bucket] = 0
    pivot.rename(columns=BUCKET_COLS, inplace=True)
    return pivot[
        ["YEAR", "UNITID"]
        + [
            "EF_RES_FTFT_UG_INSTATE",
            "EF_RES_FTFT_UG_OUTSTATE",
            "EF_RES_FTFT_UG_FOREIGN",
            "EF_RES_FTFT_UG_RES_UNKNOWN",
        ]
    ]


def main() -> None:
    args = parse_args()
    efres = load_efres(args.efres, args.year_col, args.unitid_col, args.res_col, args.count_col)
    hd = load_hd(args.hd, args.year_col, args.unitid_col)
    merged = efres.merge(hd, on=["YEAR", "UNITID"], how="left")
    merged = classify_residency(merged)
    buckets = aggregate_buckets(merged)

    if buckets.empty:
        raise RuntimeError("No residency buckets produced.")

    total_pairs = buckets[["YEAR", "UNITID"]].drop_duplicates().shape[0]
    total_count_input = merged["FTFT_UG_COUNT"].sum()
    total_count_output = buckets[
        [
            "EF_RES_FTFT_UG_INSTATE",
            "EF_RES_FTFT_UG_OUTSTATE",
            "EF_RES_FTFT_UG_FOREIGN",
            "EF_RES_FTFT_UG_RES_UNKNOWN",
        ]
    ].sum(axis=1).sum()

    args.output.parent.mkdir(parents=True, exist_ok=True)
    buckets.to_parquet(args.output, index=False, compression="snappy")

    print(f"Wrote {len(buckets)} rows covering {total_pairs} UNITID-year pairs to {args.output}")
    print(f"Total FTFT count input: {total_count_input:,.0f}")
    print(f"Total FTFT count output (bucket sums): {total_count_output:,.0f}")


if __name__ == "__main__":
    main()

