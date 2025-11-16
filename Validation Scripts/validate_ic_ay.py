#!/usr/bin/env python3
"""Validate harmonized IC_AY student charge concepts for consistency."""
from __future__ import annotations

import argparse
from pathlib import Path
from typing import Iterable, List

import numpy as np
import pandas as pd

DATA_ROOT = Path("/Users/markjaysonfarol13/Higher Ed research/IPEDS")
DEFAULT_IC_AY_PANEL = DATA_ROOT / "Parquets" / "Unify" / "ICAYwide" / "icay_concepts_wide.parquet"
DEFAULT_HD_PANEL = DATA_ROOT / "Parquets" / "Unify" / "HDICwide" / "hd_master_panel.parquet"
DEFAULT_VALIDATION_DIR = DATA_ROOT / "Parquets" / "Validation"
DEFAULT_CROSSWALK_PATH = DATA_ROOT / "Paneled Datasets" / "Crosswalks" / "Filled" / "ic_ay_crosswalk_all.csv"

UNITID_CANDIDATES = ["UNITID", "unitid", "UNIT_ID", "unit_id"]
YEAR_CANDIDATES = ["YEAR", "year", "SURVEY_YEAR", "survey_year"]
PRICE_COLS = [
    "PRICE_TUITFEE_IN_DISTRICT_FTFTUG",
    "PRICE_TUITFEE_IN_STATE_FTFTUG",
    "PRICE_TUITFEE_OUT_STATE_FTFTUG",
    "PRICE_BOOK_SUPPLY_FTFTUG",
    "PRICE_RMBD_ON_CAMPUS_FTFTUG",
]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--ic-ay", type=Path, default=DEFAULT_IC_AY_PANEL, help="Path to icay_concepts_wide parquet")
    parser.add_argument("--hd", type=Path, default=DEFAULT_HD_PANEL, help="hd_master_panel path for attributes")
    parser.add_argument("--out-dir", type=Path, default=DEFAULT_VALIDATION_DIR, help="Output directory for reports")
    parser.add_argument(
        "--crosswalk",
        type=Path,
        default=DEFAULT_CROSSWALK_PATH,
        help="Path to the filled IC_AY crosswalk (ic_ay_crosswalk_all.csv)",
    )
    return parser.parse_args()


def _resolve_column(df: pd.DataFrame, preferred: str, fallbacks: Iterable[str]) -> str:
    candidates = [preferred, *fallbacks]
    for candidate in candidates:
        if candidate in df.columns:
            return candidate
    raise KeyError(f"Could not find any of the requested columns: {candidates}")


def _load_panel(path: Path) -> pd.DataFrame:
    if not path.exists():
        raise FileNotFoundError(f"Panel not found: {path}")
    return pd.read_parquet(path)


def _ensure_unitid_year(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    unitid_col = _resolve_column(df, "UNITID", UNITID_CANDIDATES)
    year_col = _resolve_column(df, "YEAR", YEAR_CANDIDATES)
    df = df.rename(columns={unitid_col: "UNITID", year_col: "YEAR"})
    df["UNITID"] = pd.to_numeric(df["UNITID"], errors="raise").astype("int64")
    df["YEAR"] = pd.to_numeric(df["YEAR"], errors="raise").astype("int64")
    return df


def _write_csv(df: pd.DataFrame, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(path, index=False)


def summarize_price_ranges(df: pd.DataFrame, price_cols: List[str], out_dir: Path) -> pd.DataFrame:
    records = []
    for col in price_cols:
        if col not in df.columns:
            continue
        values = pd.to_numeric(df[col], errors="coerce")
        non_null = values.dropna()
        entry = {
            "column": col,
            "non_null": int(non_null.shape[0]),
            "min": non_null.min() if not non_null.empty else np.nan,
            "p01": non_null.quantile(0.01) if not non_null.empty else np.nan,
            "median": non_null.median() if not non_null.empty else np.nan,
            "p99": non_null.quantile(0.99) if not non_null.empty else np.nan,
            "max": non_null.max() if not non_null.empty else np.nan,
            "negatives": int((values < 0).sum()),
            "zeros": int((values == 0).sum()),
        }
        records.append(entry)
    result = pd.DataFrame(records)
    if not result.empty:
        out_path = out_dir / "ic_ay_price_ranges.csv"
        _write_csv(result, out_path)
        print(f"Saved price range summary to {out_path}")
    return result


def check_residency_order(df: pd.DataFrame, out_dir: Path) -> pd.DataFrame:
    required = PRICE_COLS[:3]
    missing = [col for col in required if col not in df.columns]
    if missing:
        print(f"Skipping residency ordering check; missing columns: {', '.join(missing)}")
        return pd.DataFrame()
    pub = df[df["CONTROL"] == 1].copy()
    subset = pub[required].apply(pd.to_numeric, errors="coerce")
    mask = subset.gt(0).all(axis=1)
    viol_mask = mask & (
        (subset[required[0]] > subset[required[1]]) | (subset[required[1]] > subset[required[2]])
    )
    violations = pub.loc[viol_mask, ["UNITID", "YEAR", "CONTROL", "SECTOR", *required]].copy()
    if not violations.empty:
        out_path = out_dir / "ic_ay_residency_violations.csv"
        _write_csv(violations, out_path)
        summary = (
            violations.groupby("YEAR", as_index=False).agg(n_viol=("UNITID", "nunique")).sort_values("YEAR")
        )
        summary_path = out_dir / "ic_ay_residency_violations_by_year.csv"
        _write_csv(summary, summary_path)
        print(
            f"Residency ordering check: {len(violations)} violating rows "
            f"({summary['n_viol'].sum()} total public institutions). "
            f"Details saved to {out_path}"
        )
    else:
        print("Residency ordering check: no violations detected.")
    return violations


def mean_price_timeseries(df: pd.DataFrame, price_cols: List[str], out_dir: Path) -> pd.DataFrame:
    available = [col for col in price_cols if col in df.columns]
    if not available:
        print("No price columns available for time-series mean checks.")
        return pd.DataFrame()
    means = df.groupby(["SECTOR", "YEAR"], as_index=False)[available].mean()
    out_path = out_dir / "ic_ay_mean_prices_by_sector_year.csv"
    _write_csv(means, out_path)
    print(f"Saved mean price trends to {out_path}")
    return means


def flag_large_growth(means: pd.DataFrame, price_cols: List[str], out_dir: Path) -> pd.DataFrame:
    if means.empty:
        return pd.DataFrame()
    records = []
    for sector, grp in means.groupby("SECTOR"):
        grp_sorted = grp.sort_values("YEAR").reset_index(drop=True)
        for col in price_cols:
            if col not in grp_sorted.columns:
                continue
            values = grp_sorted[col]
            positive = values.where(values > 0)
            log_vals = np.log(positive)
            diff = log_vals.diff()
            for idx in range(1, len(grp_sorted)):
                change = diff.iloc[idx]
                if pd.isna(change) or abs(change) <= 0.5:
                    continue
                records.append(
                    {
                        "SECTOR": sector,
                        "YEAR": int(grp_sorted.loc[idx, "YEAR"]),
                        "concept_key": col,
                        "growth": float(change),
                        "level_before": float(values.iloc[idx - 1]),
                        "level_after": float(values.iloc[idx]),
                    }
                )
    flags = pd.DataFrame(records)
    if not flags.empty:
        out_path = out_dir / "ic_ay_large_growth_flags.csv"
        _write_csv(flags, out_path)
        print(f"Flagged {len(flags)} large year-over-year growth events (>65%). Details: {out_path}")
    else:
        print("No large growth events detected.")
    return flags


def main() -> None:
    args = parse_args()
    crosswalk_path = args.crosswalk
    if crosswalk_path and crosswalk_path.exists():
        print(f"Using filled IC_AY crosswalk from {crosswalk_path}")
    else:
        print(
            f"Warning: expected filled IC_AY crosswalk at {crosswalk_path} "
            "was not found. Proceeding with validation of the master panel."
        )
    ic = _ensure_unitid_year(_load_panel(args.ic_ay))
    hd = _ensure_unitid_year(_load_panel(args.hd))
    if not {"CONTROL", "SECTOR"}.issubset(hd.columns):
        missing = {"CONTROL", "SECTOR"} - set(hd.columns)
        raise KeyError(f"HD master panel is missing required columns: {sorted(missing)}")
    df = ic.merge(hd[["UNITID", "YEAR", "CONTROL", "SECTOR"]], on=["UNITID", "YEAR"], how="left")

    available_price_cols = [col for col in PRICE_COLS if col in df.columns]
    if not available_price_cols:
        print("Warning: no IC_AY price columns found in the master panel.")

    range_summary = summarize_price_ranges(df, PRICE_COLS, args.out_dir)
    residency = check_residency_order(df, args.out_dir)
    mean_trends = mean_price_timeseries(df, PRICE_COLS, args.out_dir)
    growth_flags = flag_large_growth(mean_trends, PRICE_COLS, args.out_dir)

    n_inst = df["UNITID"].nunique()
    year_min = df["YEAR"].min()
    year_max = df["YEAR"].max()
    print(
        f"Validated IC_AY panel covering {n_inst:,} institutions across {year_min}-{year_max}. "
        f"Residency violations: {len(residency)} rows. Large growth flags: {len(growth_flags)} rows. "
        f"Price columns analyzed: {', '.join(available_price_cols) if available_price_cols else 'none'}."
    )


if __name__ == "__main__":
    main()
