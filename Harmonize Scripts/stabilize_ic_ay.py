"""Stabilize IC_AY student charge variables into a master panel."""
from __future__ import annotations

import argparse
from pathlib import Path
from typing import List

import numpy as np
import pandas as pd

DATA_ROOT = Path("/Users/markjaysonfarol13/Higher Ed research/IPEDS")
DEFAULT_LONG_PANEL_PATH = DATA_ROOT / "Parquets" / "Unify" / "panel_long_raw.parquet"
DEFAULT_CROSSWALK_PATH = DATA_ROOT / "Paneled Datasets" / "Crosswalks" / "Filled" / "ic_ay_crosswalk.csv"
DEFAULT_OUTPUT_PATH = DATA_ROOT / "Parquets" / "Unify" / "HDICwide" / "ic_ay_master_panel.parquet"


def _prepare_crosswalk(path: Path) -> pd.DataFrame:
    if not path.exists():
        raise FileNotFoundError(f"Crosswalk file not found: {path}")
    df = pd.read_csv(path)
    df.columns = [c.lower() for c in df.columns]
    required = {"concept_key", "survey", "source_var"}
    missing = required - set(df.columns)
    if missing:
        raise ValueError(f"Crosswalk missing required columns: {sorted(missing)}")
    df["concept_key"] = df["concept_key"].astype(str).str.strip()
    df = df[df["concept_key"] != ""].copy()
    if df.empty:
        raise ValueError("Crosswalk has no populated concept_key rows.")
    df["survey"] = df["survey"].astype(str).str.upper()
    df["source_var"] = df["source_var"].astype(str).str.upper()
    return df[["concept_key", "survey", "source_var"]]


def _prepare_long_panel(path: Path) -> pd.DataFrame:
    if not path.exists():
        raise FileNotFoundError(f"Long panel parquet not found: {path}")
    df = pd.read_parquet(path)
    df.columns = [c.lower() for c in df.columns]
    required = {"unitid", "year", "survey", "varname", "value"}
    missing = required - set(df.columns)
    if missing:
        raise ValueError(f"Long panel missing required columns: {sorted(missing)}")
    df["unitid"] = pd.to_numeric(df["unitid"], errors="raise")
    df["year"] = pd.to_numeric(df["year"], errors="raise")
    df["survey"] = df["survey"].astype(str).str.upper()
    df["varname"] = df["varname"].astype(str).str.upper()
    df["value"] = pd.to_numeric(df["value"], errors="coerce")
    return df


def _pivot_wide(df: pd.DataFrame) -> pd.DataFrame:
    wide = (
        df.pivot_table(index=["unitid", "year"], columns="concept_key", values="value", aggfunc="first")
        .reset_index()
    )
    if isinstance(wide.columns, pd.MultiIndex):
        wide.columns = ["_".join(part for part in col if part).rstrip("_") for col in wide.columns]
    wide = wide.rename(columns={"unitid": "UNITID", "year": "YEAR"})
    return wide


def _limited_gap_fill(group: pd.DataFrame, price_cols: List[str]) -> pd.DataFrame:
    group = group.copy()
    for col in price_cols:
        if col not in group.columns:
            continue
        series = group[col]
        if not series.isna().any():
            continue
        isna = series.isna()
        run_id = isna.ne(isna.shift(fill_value=False)).cumsum()
        run_len = isna.groupby(run_id).transform("sum")
        single_gap = isna & (run_len == 1)
        if not single_gap.any():
            continue
        arr = series.to_numpy(dtype="float64")
        idx_list = list(series.index)
        pos_map = {idx: pos for pos, idx in enumerate(idx_list)}
        for idx in series.index[single_gap]:
            pos = pos_map[idx]
            prev_val = next((arr[i] for i in range(pos - 1, -1, -1) if not np.isnan(arr[i])), None)
            next_val = next((arr[i] for i in range(pos + 1, len(arr)) if not np.isnan(arr[i])), None)
            if prev_val is not None and next_val is not None:
                arr[pos] = (prev_val + next_val) / 2.0
            elif prev_val is not None:
                arr[pos] = prev_val
            elif next_val is not None:
                arr[pos] = next_val
        group[col] = pd.Series(arr, index=series.index)
    return group


def stabilize_ic_ay(long_panel: Path, crosswalk: Path, output_path: Path, overwrite: bool = False) -> pd.DataFrame:
    if output_path.exists() and not overwrite:
        raise FileExistsError(f"Output file already exists: {output_path}. Use --overwrite to replace it.")

    cw = _prepare_crosswalk(crosswalk)
    panel = _prepare_long_panel(long_panel)

    merged = panel.merge(cw, left_on=["survey", "varname"], right_on=["survey", "source_var"], how="inner")
    if merged.empty:
        raise ValueError("Merged IC_AY data is empty. Check crosswalk entries and panel coverage.")

    merged = merged[["unitid", "year", "concept_key", "value"]]
    wide = _pivot_wide(merged)
    wide = wide.sort_values(["UNITID", "YEAR"]).reset_index(drop=True)

    price_cols = [col for col in wide.columns if col.startswith("PRICE_")]
    if price_cols:
        wide = (
            wide.groupby("UNITID", group_keys=False)
            .apply(lambda g: _limited_gap_fill(g, price_cols))
            .reset_index(drop=True)
        )

    wide["UNITID"] = wide["UNITID"].astype("int64")
    wide["YEAR"] = wide["YEAR"].astype("int64")

    output_path.parent.mkdir(parents=True, exist_ok=True)
    wide.to_parquet(output_path, index=False)

    print(f"Wrote IC_AY master panel to {output_path}")
    print(f"Shape: {wide.shape[0]:,} rows x {wide.shape[1]:,} columns")
    print(f"Price columns gap-filled: {len(price_cols)}")
    return wide


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--long-panel", type=Path, default=DEFAULT_LONG_PANEL_PATH, help="panel_long_raw path")
    parser.add_argument(
        "--crosswalk",
        type=Path,
        default=DEFAULT_CROSSWALK_PATH,
        help="Filled IC_AY crosswalk CSV path",
    )
    parser.add_argument("--out", type=Path, default=DEFAULT_OUTPUT_PATH, help="Output master panel parquet path")
    parser.add_argument("--overwrite", action="store_true", help="Allow overwriting an existing output file")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    stabilize_ic_ay(args.long_panel, args.crosswalk, args.out, overwrite=args.overwrite)


if __name__ == "__main__":
    main()
