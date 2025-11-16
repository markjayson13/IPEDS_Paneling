"""Validate the HD/IC master panel produced by stabilize_hd.py."""
from __future__ import annotations

import argparse
from pathlib import Path
from typing import List

import numpy as np
import pandas as pd

DATA_ROOT = Path("/Users/markjaysonfarol13/Higher Ed research/IPEDS")
DEFAULT_RAW_HDIC = DATA_ROOT / "Parquets" / "Unify" / "HDIClong" / "hd_ic_long.parquet"
DEFAULT_MASTER = DATA_ROOT / "Parquets" / "Unify" / "HDICwide" / "hd_master_panel.parquet"

CARNEGIE_COLS: List[str] = [
    "CARNEGIE_2005",
    "CARNEGIE_2010",
    "CARNEGIE_2015",
    "CARNEGIE_2018",
    "CARNEGIE_2021",
]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--raw",
        type=Path,
        default=DEFAULT_RAW_HDIC,
        help="Path to long-form HD/IC panel (unitid, year, survey, varname, value).",
    )
    parser.add_argument(
        "--master",
        type=Path,
        default=DEFAULT_MASTER,
        help="Path to hd_master_panel.parquet.",
    )
    return parser.parse_args()


def load_raw_hd(raw_path: Path) -> pd.DataFrame:
    if not raw_path.exists():
        raise SystemExit(f"Raw HD/IC file not found: {raw_path}")
    df = pd.read_parquet(raw_path)
    df.columns = [c.lower() for c in df.columns]
    required = {"unitid", "year", "survey", "varname", "value"}
    missing = required - set(df.columns)
    if missing:
        raise SystemExit(f"Raw HD/IC missing required columns: {sorted(missing)}")
    df["survey"] = df["survey"].astype(str).str.upper()
    mask = df["survey"].isin(["HD", "IC"])
    return df.loc[mask].copy()


def load_master(master_path: Path) -> pd.DataFrame:
    if not master_path.exists():
        raise SystemExit(f"Master spine not found: {master_path}")
    df = pd.read_parquet(master_path)
    df.columns = [c.upper() for c in df.columns]
    required = {"UNITID", "YEAR"}
    missing = required - set(df.columns)
    if missing:
        raise SystemExit(f"Master panel missing required columns: {sorted(missing)}")
    return df


def check_row_counts(raw_hd: pd.DataFrame, master: pd.DataFrame) -> None:
    print("\n=== Row counts by YEAR (raw HD vs master) ===")
    raw_counts = raw_hd.groupby("year")["unitid"].nunique().rename("raw_hd_n_uni")
    master_counts = master.groupby("YEAR")["UNITID"].nunique().rename("master_n_uni")
    counts = (
        pd.concat([raw_counts, master_counts], axis=1)
        .fillna(0)
        .astype(int)
        .reset_index()
        .rename(columns={"index": "YEAR"})
    )
    counts["diff"] = counts["master_n_uni"] - counts["raw_hd_n_uni"]
    print(counts.to_string(index=False))


def check_name_uniqueness(master: pd.DataFrame) -> None:
    if "STABLE_INSTITUTION_NAME" not in master.columns:
        print("\n[WARN] STABLE_INSTITUTION_NAME not in master; skipping name uniqueness check.")
        return
    grp = master.groupby("UNITID")["STABLE_INSTITUTION_NAME"].nunique()
    max_n = grp.max()
    print("\n=== STABLE_INSTITUTION_NAME uniqueness per UNITID ===")
    print(f"Max distinct names per UNITID: {max_n}")
    if max_n > 1:
        bad_ids = grp[grp > 1].head(20).index.tolist()
        print(f"[WARN] Found {(grp > 1).sum()} UNITIDs with multiple names. Example IDs: {bad_ids}")
    else:
        print("OK: each UNITID has exactly one stable name.")


def _ever_one_from_raw(raw_hd: pd.DataFrame, varname: str) -> pd.Series:
    mask = raw_hd["varname"].str.upper().eq(varname.upper())
    sub = raw_hd.loc[mask, ["unitid", "value"]].copy()
    if sub.empty:
        return pd.Series(dtype="float64")
    sub["num"] = pd.to_numeric(sub["value"], errors="coerce")
    sub["yes"] = sub["num"] == 1
    return sub.groupby("unitid")["yes"].max().astype(float)


def check_flags(raw_hd: pd.DataFrame, master: pd.DataFrame) -> None:
    print("\n=== HBCU/TRIBAL consistency (raw ever=1 vs STABLE flags) ===")
    for raw_var, stable_col in [("HBCU", "STABLE_HBCU"), ("TRIBAL", "STABLE_TRIBAL")]:
        if stable_col not in master.columns:
            print(f"[WARN] {stable_col} not in master; skipping.")
            continue

        raw_ever = _ever_one_from_raw(raw_hd, raw_var)
        if raw_ever.empty:
            print(f"[WARN] No raw {raw_var} variable found in HD/IC; skipping.")
            continue

        stable = master.drop_duplicates("UNITID").set_index("UNITID")[stable_col].astype("Int64")
        joined = pd.concat([raw_ever.rename("raw_ever1"), stable.rename("stable")], axis=1)

        bad = joined[(joined["raw_ever1"] == 1.0) & (joined["stable"] != 1)]
        print(f"\n{raw_var}:")
        print(f"  Units with ever=1 in raw: {int((raw_ever == 1.0).sum())}")
        print(f"  Units with {stable_col}=1: {int((stable == 1).sum())}")
        if not bad.empty:
            print(f"  [WARN] {len(bad)} units with raw ever=1 but stable flag != 1. Example:")
            print(bad.head().to_string())
        else:
            print("  OK: all raw ever=1 units have stable flag = 1.")


def check_control_sector_transitions(master: pd.DataFrame) -> None:
    print("\n=== CONTROL/SECTOR transitions over time ===")
    for col in ["STABLE_CONTROL", "STABLE_SECTOR"]:
        if col not in master.columns:
            print(f"[WARN] {col} not in master; skipping.")
            continue
        grp = master.groupby("UNITID")[col].nunique(dropna=True)
        n_changers = (grp > 1).sum()
        total = grp.shape[0]
        print(f"\n{col}:")
        print(f"  Units with any data: {total}")
        print(f"  Units with a change over time: {n_changers}")
        if n_changers:
            example_ids = grp[grp > 1].head(10).index.tolist()
            print(f"  Example UNITIDs with changes: {example_ids}")


def check_carnegie_coverage(master: pd.DataFrame) -> None:
    print("\n=== Carnegie coverage ===")
    total = len(master)
    for col in CARNEGIE_COLS:
        if col not in master.columns:
            print(f"[WARN] {col} not in master.")
            continue
        non_null = master[col].notna().sum()
        pct = (non_null / total * 100) if total else 0.0
        print(f"  {col}: {non_null:,} non-missing of {total:,} rows ({pct:.1f}%)")


def main() -> None:
    args = parse_args()
    raw_hd = load_raw_hd(args.raw)
    master = load_master(args.master)

    print(f"Loaded raw HD/IC: {len(raw_hd):,} rows")
    print(f"Loaded master spine: {len(master):,} rows")

    check_row_counts(raw_hd, master)
    check_name_uniqueness(master)
    check_flags(raw_hd, master)
    check_control_sector_transitions(master)
    check_carnegie_coverage(master)


if __name__ == "__main__":
    main()
