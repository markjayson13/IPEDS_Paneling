#!/usr/bin/env python3
"""Apply the Admissions crosswalk to produce harmonized concept panels."""

from __future__ import annotations

import argparse
import logging
from pathlib import Path
from typing import Iterable

import pandas as pd

DEFAULT_STEP0 = Path("data/adm_step0_long.parquet")
DEFAULT_CROSSWALK = Path("data/adm_crosswalk.csv")
DEFAULT_OUT_LONG = Path("data/adm_concepts_long.parquet")
DEFAULT_OUT_WIDE = Path("data/adm_concepts_wide.parquet")

UNITID_CANDIDATES = ["UNITID", "unitid", "UNIT_ID", "unit_id"]
YEAR_CANDIDATES = ["YEAR", "year", "SURVEY_YEAR", "survey_year", "panel_year"]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--step0", type=Path, default=DEFAULT_STEP0, help="Admissions step0 long parquet path")
    parser.add_argument("--crosswalk", type=Path, default=DEFAULT_CROSSWALK, help="Admissions crosswalk CSV path")
    parser.add_argument("--out-long", type=Path, default=DEFAULT_OUT_LONG, help="Destination Admissions concept long parquet")
    parser.add_argument("--out-wide", type=Path, default=DEFAULT_OUT_WIDE, help="Destination Admissions concept wide parquet")
    parser.add_argument("--log-level", default="INFO")
    return parser.parse_args()


def resolve_column(df: pd.DataFrame, preferred: str, fallbacks: Iterable[str]) -> str:
    candidates = [preferred, *fallbacks]
    for candidate in candidates:
        if candidate in df.columns:
            return candidate
    raise KeyError(f"None of the requested columns are present: {candidates}")


def load_step0(path: Path) -> pd.DataFrame:
    if not path.exists():
        raise FileNotFoundError(f"Admissions step0 file not found: {path}")
    df = pd.read_parquet(path)
    required = {"source_var", "value"}
    missing = required - set(df.columns)
    if missing:
        raise ValueError(f"Step0 long parquet missing columns: {', '.join(sorted(missing))}")
    df = df.copy()
    df["source_var"] = df["source_var"].astype(str).str.upper()
    df["value"] = pd.to_numeric(df["value"], errors="coerce")
    df.dropna(subset=["source_var", "value"], inplace=True)
    return df


def load_crosswalk(path: Path) -> pd.DataFrame:
    if not path.exists():
        raise FileNotFoundError(f"Admissions crosswalk not found: {path}")
    cw = pd.read_csv(path)
    if "source_var" not in cw.columns or "concept_key" not in cw.columns:
        raise ValueError("Crosswalk must include source_var and concept_key columns")
    cw = cw.copy()
    cw["concept_key"] = cw["concept_key"].astype(str).str.strip()
    cw = cw[cw["concept_key"].ne("")]
    cw["source_var"] = cw["source_var"].astype(str).str.upper()
    cw["weight"] = pd.to_numeric(cw.get("weight", 1.0), errors="coerce").fillna(1.0)
    cw["year_start"] = pd.to_numeric(cw.get("year_start"), errors="coerce")
    cw["year_end"] = pd.to_numeric(cw.get("year_end"), errors="coerce")
    cw.dropna(subset=["year_start", "year_end"], inplace=True)
    cw["year_start"] = cw["year_start"].astype(int)
    cw["year_end"] = cw["year_end"].astype(int)
    if cw.empty:
        logging.warning("Crosswalk has no concept assignments after filtering empty concept_key rows")
    return cw


def harmonize(long_df: pd.DataFrame, crosswalk: pd.DataFrame, unitid_col: str, year_col: str) -> tuple[pd.DataFrame, pd.DataFrame]:
    merged = long_df.merge(crosswalk, how="inner", on="source_var")
    merged[year_col] = pd.to_numeric(merged[year_col], errors="coerce")
    merged.dropna(subset=[unitid_col, year_col], inplace=True)
    mask = (merged[year_col] >= merged["year_start"]) & (merged[year_col] <= merged["year_end"])
    merged = merged.loc[mask].copy()
    if merged.empty:
        logging.warning("Joined Admissions crosswalk produced zero rows. Check concept year ranges.")
        return pd.DataFrame(columns=["UNITID", "YEAR", "concept_key", "value"]), pd.DataFrame()

    merged[unitid_col] = pd.to_numeric(merged[unitid_col], errors="coerce").astype("int64")
    merged[year_col] = merged[year_col].astype("int64")
    merged["weighted_value"] = merged["value"] * merged["weight"]

    grouped = (
        merged.groupby([unitid_col, year_col, "concept_key"], as_index=False)["weighted_value"].sum()
    )
    grouped.rename(columns={unitid_col: "UNITID", year_col: "YEAR", "weighted_value": "value"}, inplace=True)

    wide = grouped.pivot_table(index=["UNITID", "YEAR"], columns="concept_key", values="value")
    wide.sort_index(axis=1, inplace=True)
    wide.reset_index(inplace=True)
    return grouped, wide


def main() -> None:
    args = parse_args()
    logging.basicConfig(level=getattr(logging, args.log_level.upper(), logging.INFO), format="%(levelname)s %(message)s")

    long_df = load_step0(args.step0)
    crosswalk = load_crosswalk(args.crosswalk)

    if long_df.empty or crosswalk.empty:
        logging.warning("Admissions harmonization skipped because inputs are empty")
        return

    unitid_col = resolve_column(long_df, "UNITID", UNITID_CANDIDATES)
    year_col = resolve_column(long_df, "YEAR", YEAR_CANDIDATES)

    long_out, wide_out = harmonize(long_df, crosswalk, unitid_col, year_col)
    if long_out.empty:
        logging.warning("Admissions harmonization produced no rows; skipping writes.")
        return

    args.out_long.parent.mkdir(parents=True, exist_ok=True)
    args.out_wide.parent.mkdir(parents=True, exist_ok=True)
    long_out.to_parquet(args.out_long, index=False)
    wide_out.to_parquet(args.out_wide, index=False)
    logging.info(
        "Saved Admissions concept long (%s rows, %s concepts) and wide panels to %s / %s",
        len(long_out),
        long_out["concept_key"].nunique(),
        args.out_long,
        args.out_wide,
    )


if __name__ == "__main__":
    main()
