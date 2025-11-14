#!/usr/bin/env python3
"""Basic validation utilities for the finance concept panel."""

from __future__ import annotations

import argparse
from pathlib import Path

import pandas as pd

DEFAULT_PANEL = Path(
    "/Users/markjaysonfarol13/Higher Ed research/IPEDS/Parquets/finance_concepts_wide.parquet"
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--panel", type=Path, default=DEFAULT_PANEL)
    parser.add_argument("--tolerance", type=float, default=1e5, help="allowed absolute error when comparing flows")
    return parser.parse_args()


def check_income_statement(df: pd.DataFrame, tol: float) -> pd.DataFrame:
    required = ["IS_REVENUES_TOTAL", "IS_EXPENSES_TOTAL", "IS_NET_INCOME"]
    if not set(required).issubset(df.columns):
        return pd.DataFrame()
    sample = df.dropna(subset=required).copy()
    sample["diff"] = sample["IS_REVENUES_TOTAL"] - sample["IS_EXPENSES_TOTAL"] - sample["IS_NET_INCOME"]
    outliers = sample.loc[sample["diff"].abs() > tol, ["YEAR", "UNITID", "diff"]]
    return outliers


def check_net_assets(df: pd.DataFrame, tol: float) -> pd.DataFrame:
    cols = ["YEAR", "UNITID", "BS_NET_ASSETS_TOTAL", "IS_NET_INCOME"]
    if not set(cols[1:]).issubset(df.columns):
        return pd.DataFrame()
    df = df.sort_values(["UNITID", "YEAR"])
    df["bs_lag"] = df.groupby("UNITID")["BS_NET_ASSETS_TOTAL"].shift(1)
    df["delta_bs"] = df["BS_NET_ASSETS_TOTAL"] - df["bs_lag"]
    sample = df.dropna(subset=["delta_bs", "IS_NET_INCOME"])
    sample["gap"] = sample["delta_bs"] - sample["IS_NET_INCOME"]
    return sample.loc[sample["gap"].abs() > tol, ["YEAR", "UNITID", "gap"]]


def year_totals(df: pd.DataFrame, cols: list[str]) -> pd.DataFrame:
    existing = [c for c in cols if c in df.columns]
    if not existing:
        return pd.DataFrame()
    totals = df.groupby("YEAR")[existing].sum().reset_index()
    return totals


def main() -> None:
    args = parse_args()
    panel_path = args.panel
    if not panel_path.exists():
        raise SystemExit(f"Panel not found: {panel_path}")
    if panel_path.suffix.lower() == ".parquet":
        df = pd.read_parquet(panel_path)
    else:
        df = pd.read_csv(panel_path)

    income_outliers = check_income_statement(df, args.tolerance)
    print(f"Income statement mismatches (> {args.tolerance}): {len(income_outliers)} rows")
    if not income_outliers.empty:
        print(income_outliers.head())

    net_outliers = check_net_assets(df, args.tolerance)
    print(f"Net asset change mismatches (> {args.tolerance}): {len(net_outliers)} rows")
    if not net_outliers.empty:
        print(net_outliers.head())

    totals = year_totals(
        df,
        [
            "REV_TUITION_NET",
            "IS_REVENUES_TOTAL",
            "IS_EXPENSES_TOTAL",
            "BS_ASSETS_TOTAL",
            "BS_NET_ASSETS_TOTAL",
        ],
    )
    if not totals.empty:
        print("\nYearly aggregates (selected columns):")
        print(totals.head())


if __name__ == "__main__":
    main()
