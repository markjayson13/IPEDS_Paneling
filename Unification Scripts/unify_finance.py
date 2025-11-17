#!/usr/bin/env python3
"""Finance Step 0 – Form-level coalescer for IPEDS F1/F2/F3 data."""

from __future__ import annotations

import argparse
import logging
import re
from pathlib import Path
from typing import Sequence

import pandas as pd

ID_COLS = ["YEAR", "UNITID"]
OPTIONAL_ID_COLS = ["REPORTING_UNITID"]
VAL_RE = re.compile(r"^(F[123])([A-Z]{1,2})(\d+[A-Z]?)$", re.IGNORECASE)
FLAG_RE = re.compile(r"^X(F[123])([A-Z]{1,2})(\d+[A-Z]?)$", re.IGNORECASE)

DEFAULT_INPUT = Path("/Users/markjaysonfarol13/Higher Ed research/IPEDS/Paneled Datasets/panel_wide_raw.csv")
DEFAULT_LONG = Path(
    "/Users/markjaysonfarol13/Higher Ed research/IPEDS/Parquets/Unify/Step0Finlong/finance_step0_long.parquet"
)
DEFAULT_WIDE = Path(
    "/Users/markjaysonfarol13/Higher Ed research/IPEDS/Paneled Datasets/Raw panel/Finance/Step0wide/finance_step0_wide.csv"
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Extract raw IPEDS finance variables (F1/F2/F3) into a canonical long form. "
            "This script does NOT harmonize concepts."
        )
    )
    parser.add_argument("--input", type=Path, default=DEFAULT_INPUT, help="panel_wide_raw CSV/Parquet path")
    parser.add_argument("--output-long", type=Path, default=DEFAULT_LONG, help="finance_step0_long.parquet path")
    parser.add_argument(
        "--output-wide",
        type=Path,
        default=DEFAULT_WIDE,
        help="Optional debug wide output with columns per form/base_key",
    )
    parser.add_argument("--log-level", default="INFO")
    return parser.parse_args()


def _ensure_id_cols(df: pd.DataFrame) -> Sequence[str]:
    for col in ID_COLS:
        if col not in df.columns:
            df[col] = pd.NA
    optional = [col for col in OPTIONAL_ID_COLS if col in df.columns]
    return list(ID_COLS) + optional


def _classify_columns(columns: Sequence[str]) -> tuple[list[str], list[str]]:
    value_cols: list[str] = []
    flag_cols: list[str] = []
    for col in columns:
        name = str(col).strip().upper()
        if VAL_RE.match(name):
            value_cols.append(name)
        elif FLAG_RE.match(name):
            flag_cols.append(name)
    return value_cols, flag_cols


def melt_finance(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df.columns = pd.Index(str(c).strip().upper() for c in df.columns)
    id_cols = _ensure_id_cols(df)

    value_cols, flag_cols = _classify_columns(df.columns)
    if not value_cols:
        raise SystemExit("No finance columns detected (look for F1*/F2*/F3* headers).")

    long = (
        df[id_cols + value_cols]
        .melt(id_vars=id_cols, var_name="source_var", value_name="value")
        .dropna(subset=["source_var"], how="any")
    )
    parsed = long["source_var"].str.extract(VAL_RE)
    form_family = parsed[0].str.upper()
    sec_comp = parsed[1].str.upper()
    line_code = parsed[2].str.upper()
    long["section"] = sec_comp.str[0]
    long["line_code"] = line_code
    long["base_key"] = long["section"] + long["line_code"]
    component_suffix = sec_comp.str[1:].fillna("")
    long["form_family"] = form_family
    mask = component_suffix.ne("")
    long.loc[mask, "form_family"] = (
        long.loc[mask, "form_family"] + "_COMP_" + component_suffix[mask]
    )

    if flag_cols:
        flag_long = (
            df[id_cols + flag_cols]
            .melt(id_vars=id_cols, var_name="flag_var", value_name="flag")
            .dropna(subset=["flag_var"], how="any")
        )
        fparsed = flag_long["flag_var"].str.extract(FLAG_RE)
        ff = fparsed[0].str.upper()
        sec_comp = fparsed[1].str.upper()
        line_code = fparsed[2].str.upper()
        flag_long["section"] = sec_comp.str[0]
        flag_long["line_code"] = line_code
        flag_long["base_key"] = flag_long["section"] + flag_long["line_code"]
        comp_suffix = sec_comp.str[1:].fillna("")
        flag_long["form_family"] = ff
        mask = comp_suffix.ne("")
        flag_long.loc[mask, "form_family"] = (
            flag_long.loc[mask, "form_family"] + "_COMP_" + comp_suffix[mask]
        )
        flag_long["flag"] = flag_long["flag"].apply(
            lambda x: 1 if pd.notna(x) and str(x).strip() not in {"", "0"} else 0
        )
        flag_long = (
            flag_long.groupby(id_cols + ["form_family", "base_key"], as_index=False)["flag"].max()
        )
        long = long.merge(
            flag_long[id_cols + ["form_family", "base_key", "flag"]],
            on=id_cols + ["form_family", "base_key"],
            how="left",
        )
        long.rename(columns={"flag": "imputed_flag"}, inplace=True)
    else:
        long["imputed_flag"] = 0

    long["value"] = pd.to_numeric(long["value"], errors="coerce")
    long = long.dropna(subset=["value"], how="all")

    sort_cols = id_cols + ["form_family", "base_key", "source_var"]
    long = (
        long.sort_values(sort_cols)
        .drop_duplicates(id_cols + ["form_family", "base_key"], keep="first")
        .reset_index(drop=True)
    )
    return long


def write_long(long: pd.DataFrame, path: Path) -> None:
    cols = [
        *[col for col in ID_COLS if col in long.columns],
        *[col for col in OPTIONAL_ID_COLS if col in long.columns],
        "form_family",
        "section",
        "line_code",
        "base_key",
        "source_var",
        "value",
        "imputed_flag",
    ]
    path.parent.mkdir(parents=True, exist_ok=True)
    long[cols].to_parquet(path, index=False)


def write_wide(long: pd.DataFrame, path: Path) -> None:
    id_cols = [col for col in ID_COLS if col in long.columns]
    id_cols += [col for col in OPTIONAL_ID_COLS if col in long.columns if col not in id_cols]
    pivot = (
        long.pivot_table(
            index=id_cols,
            columns=["form_family", "base_key"],
            values="value",
            aggfunc="first",
        )
        .sort_index(axis=1)
    )
    pivot.columns = [f"{fam}_{key}" for fam, key in pivot.columns]
    pivot = pivot.reset_index()
    path.parent.mkdir(parents=True, exist_ok=True)
    pivot.to_csv(path, index=False)


def main() -> None:
    args = parse_args()
    logging.basicConfig(level=getattr(logging, args.log_level.upper(), logging.INFO))

    src = args.input
    if not src.exists():
        raise SystemExit(f"Input file not found: {src}")

    logging.info("Reading %s", src)
    if src.suffix.lower() == ".parquet":
        wide = pd.read_parquet(src)
    else:
        wide = pd.read_csv(src, dtype=str)
    wide.columns = pd.Index(str(c).strip().upper() for c in wide.columns)

    long = melt_finance(wide)
    if "YEAR" in long.columns:
        long["YEAR"] = pd.to_numeric(long["YEAR"], errors="coerce").astype("Int64")
    logging.info("Extracted %s finance rows", len(long))

    write_long(long, args.output_long)
    logging.info("Wrote step0 long parquet to %s", args.output_long)

    if args.output_wide:
        write_wide(long, args.output_wide)
        logging.info("Wrote debug wide CSV to %s", args.output_wide)


if __name__ == "__main__":
    main()
