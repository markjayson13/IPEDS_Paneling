#!/usr/bin/env python3
"""Unify IPEDS finance data across F1/F2/F3 form families."""

from __future__ import annotations

import re
import sys
from pathlib import Path

import pandas as pd

ID_COLS = ["YEAR", "UNITID"]
OPTIONAL_ID_COLS = ["REPORTING_UNITID"]

VAL_RE = re.compile(r"^(F[123])([A-Z])(\d+[A-Z]?)$")
FLAG_RE = re.compile(r"^X(F[123])([A-Z])(\d+[A-Z]?)$")

DEFAULT_INPUT = Path(
    "/Users/markjaysonfarol13/Higher Ed research/IPEDS/Paneled Datasets/Crosssections/panel_wide_raw_2004_2024_merged.csv"
)
OUT_DIR = Path("/Users/markjaysonfarol13/Higher Ed research/IPEDS/Paneled Datasets/Raw panel")
CONFLICT_DIR = Path("/Users/markjaysonfarol13/Higher Ed research/IPEDS/Checks/Conflicts")
PARQUET_DIR = Path("/Users/markjaysonfarol13/Higher Ed research/IPEDS/Parquets/UnifyingParquets")


def classify_columns(cols: list[str]) -> tuple[dict[str, tuple[str, str, str]], dict[str, tuple[str, str, str]]]:
    values: dict[str, tuple[str, str, str]] = {}
    flags: dict[str, tuple[str, str, str]] = {}
    for col in cols:
        name = str(col).strip().upper()
        if match := VAL_RE.match(name):
            values[name] = match.groups()
        elif match := FLAG_RE.match(name):
            flags[name] = match.groups()
    return values, flags


def melt_finance(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df.columns = pd.Index([str(c).strip().upper() for c in df.columns])
    for col in ID_COLS:
        if col not in df.columns:
            df[col] = pd.NA
    id_like = [col for col in OPTIONAL_ID_COLS if col in df.columns]
    id_cols = ID_COLS + id_like

    value_cols, flag_cols = classify_columns(df.columns.tolist())
    if not value_cols:
        raise SystemExit("No finance columns detected (F1/F2/F3).")

    long = (
        df[id_cols + list(value_cols.keys())]
        .melt(id_vars=id_cols, var_name="raw_code", value_name="value")
        .dropna(subset=["raw_code"], how="any")
    )
    parsed = long["raw_code"].str.extract(VAL_RE)
    long["form_family"] = parsed[0]
    long["section"] = parsed[1]
    long["base_code"] = parsed[2]
    long["base_key"] = long["section"] + long["base_code"]

    if flag_cols:
        flag_long = (
            df[id_cols + list(flag_cols.keys())]
            .melt(id_vars=id_cols, var_name="raw_flag", value_name="flag")
            .dropna(subset=["raw_flag"], how="any")
        )
        fparsed = flag_long["raw_flag"].str.extract(FLAG_RE)
        flag_long["form_family"] = fparsed[0]
        flag_long["section"] = fparsed[1]
        flag_long["base_code"] = fparsed[2]
        flag_long["base_key"] = flag_long["section"] + flag_long["base_code"]
        flag_long["flag"] = flag_long["flag"].apply(
            lambda x: 1 if pd.notna(x) and str(x).strip() not in {"", "0"} else pd.NA
        )
        long = long.merge(
            flag_long[id_cols + ["form_family", "base_key", "flag"]],
            on=id_cols + ["form_family", "base_key"],
            how="left",
        )
        long.rename(columns={"flag": "imputed_flag"}, inplace=True)
    else:
        long["imputed_flag"] = pd.NA

    long["value"] = pd.to_numeric(long["value"], errors="coerce")
    return long


def coalesce_finance(long: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    id_cols = [col for col in ["YEAR", "UNITID", "REPORTING_UNITID"] if col in long.columns]
    counts = (
        long.dropna(subset=["value"])
        .groupby(id_cols + ["form_family"])
        .size()
        .reset_index(name="nnz")
    )
    merged = long.merge(counts, on=id_cols + ["form_family"], how="left")
    merged["nnz"] = merged["nnz"].fillna(0)
    form_rank = {"F1": 3, "F2": 2, "F3": 1}
    merged["pref_rank"] = merged["form_family"].map(form_rank).fillna(0)

    def pick(group: pd.DataFrame) -> pd.Series:
        g = group.sort_values(
            by=["value", "nnz", "pref_rank"], ascending=[False, False, False]
        )
        return g.iloc[0][["value", "form_family", "imputed_flag"]]

    chosen = (
        merged.groupby(id_cols + ["base_key"], as_index=False)
        .apply(pick)
        .reset_index()
    )
    chosen.rename(
        columns={
            "value": "coalesced_value",
            "form_family": "finance_form_used",
            "imputed_flag": "X_base",
        },
        inplace=True,
    )

    nonnull_counts = (
        merged.groupby(id_cols + ["base_key"]) ["value"].apply(lambda s: s.notna().sum()).reset_index(name="nnz_forms")
    )
    conflicts = nonnull_counts[nonnull_counts["nnz_forms"] > 1]
    return chosen, conflicts


def pivot_finance(chosen: pd.DataFrame) -> pd.DataFrame:
    id_cols = [col for col in ["YEAR", "UNITID", "REPORTING_UNITID"] if col in chosen.columns]
    values = (
        chosen[id_cols + ["base_key", "coalesced_value"]]
        .pivot_table(index=id_cols, columns="base_key", values="coalesced_value", aggfunc="first")
        .reset_index()
    )
    flags = (
        chosen[id_cols + ["base_key", "X_base"]]
        .pivot_table(index=id_cols, columns="base_key", values="X_base", aggfunc="max")
        .reset_index()
    )
    flags.columns = [col if col in id_cols else f"X_{col}" for col in flags.columns]
    wide = values.merge(flags, on=id_cols, how="left")
    dominant = (
        chosen.groupby(id_cols + ["finance_form_used"]).size().reset_index(name="n")
        .sort_values(id_cols + ["n"], ascending=[True, True, False])
        .drop_duplicates(subset=id_cols)
        .drop(columns=["n"])
    )
    return wide.merge(dominant, on=id_cols, how="left")


def main(input_path: str | Path = DEFAULT_INPUT) -> None:
    src = Path(input_path)
    if not src.exists():
        print(f"Input file not found: {src}")
        sys.exit(2)

    print(f"Reading merged wide file: {src}")
    wide = pd.read_csv(src, dtype=str)
    wide.columns = [str(c).strip().upper() for c in wide.columns]
    if "YEAR" in wide.columns:
        wide["YEAR"] = pd.to_numeric(wide["YEAR"], errors="coerce").astype("Int64")
    if "UNITID" in wide.columns:
        try:
            wide["UNITID"] = pd.to_numeric(wide["UNITID"], errors="raise").astype("Int64")
        except Exception:
            wide["UNITID"] = wide["UNITID"].astype("string")

    vlong = melt_finance(wide)
    chosen, conflicts = coalesce_finance(vlong)

    conflict_path = CONFLICT_DIR / "finance_form_conflicts.csv"
    conflict_path.parent.mkdir(parents=True, exist_ok=True)
    conflicts.to_csv(conflict_path, index=False)
    print(f"Wrote conflicts: {conflict_path} ({len(conflicts):,} rows)")

    fin_wide = pivot_finance(chosen)

    long_path = PARQUET_DIR / "finance_unified_long.parquet"
    long_path.parent.mkdir(parents=True, exist_ok=True)
    wide_path = OUT_DIR / "finance_unified_wide.csv"
    chosen.to_parquet(long_path, index=False)
    fin_wide.to_csv(wide_path, index=False)

    print(f"Wrote finance long: {long_path} ({len(chosen):,} rows)")
    print(f"Wrote finance wide: {wide_path} ({len(fin_wide):,} rows, {len(fin_wide.columns):,} cols)")


if __name__ == "__main__":
    inp = sys.argv[1] if len(sys.argv) > 1 else DEFAULT_INPUT
    main(inp)
