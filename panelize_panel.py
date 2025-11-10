#!/usr/bin/env python3
"""
Convert the long-form harmonized panel parquet into a wide CSV with UNITID-year as the panel keys.

The script keeps the highest-scoring observation per UNITID/year/target_var, pivots targets to columns,
and writes the result to the requested CSV path.
"""

from __future__ import annotations

import argparse
import logging
from pathlib import Path

import pandas as pd

DEFAULT_PANEL_PATH = Path("/Users/markjaysonfarol13/Higher Ed research/IPEDS/Parquets/panel_long.parquet")
DEFAULT_OUTPUT_PATH = Path("/Users/markjaysonfarol13/Higher Ed research/IPEDS/Paneled Datasets/panel_wide.csv")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Pivot the harmonized long panel into a wide CSV.")
    parser.add_argument(
        "--source",
        type=Path,
        default=DEFAULT_PANEL_PATH,
        help=f"Long-form panel parquet (default: {DEFAULT_PANEL_PATH})",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=DEFAULT_OUTPUT_PATH,
        help=f"Destination CSV path (default: {DEFAULT_OUTPUT_PATH})",
    )
    parser.add_argument("--log-level", type=str, default="INFO", help="Logging level (e.g., INFO, DEBUG)")
    return parser.parse_args()


def configure_logging(level: str) -> None:
    logging.basicConfig(
        level=getattr(logging, level.upper(), logging.INFO),
        format="%(asctime)s %(levelname)s %(message)s",
    )


def dedupe_panel(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df
    work = df.copy()
    if "release" in work.columns:
        work["release_rank"] = work["release"].astype(str).str.lower().eq("revised").astype(int)
    else:
        work["release_rank"] = 0
    score = pd.to_numeric(work.get("decision_score"), errors="coerce").fillna(-9e9)
    work["score_rank"] = score
    sort_cols = ["UNITID", "year", "target_var", "score_rank", "release_rank"]
    ascending = [True, True, True, False, False]
    if "form_family" in work.columns:
        sort_cols.append("form_family")
        ascending.append(True)
    if "source_file" in work.columns:
        sort_cols.append("source_file")
        ascending.append(True)
    work = work.sort_values(sort_cols, ascending=ascending)
    deduped = work.drop_duplicates(["UNITID", "year", "target_var"], keep="first").copy()
    deduped.drop(columns=["score_rank", "release_rank"], inplace=True, errors="ignore")
    return deduped


def pivot_panel(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame(columns=["UNITID", "year"])
    wide = (
        df.pivot(index=["UNITID", "year"], columns="target_var", values="value")
        .sort_index()
        .reset_index()
    )
    wide.columns = [col if isinstance(col, str) else str(col) for col in wide.columns]
    wide = wide.sort_values(["UNITID", "year"]).reset_index(drop=True)
    return wide


def main() -> int:
    args = parse_args()
    configure_logging(args.log_level)
    if not args.source.exists():
        logging.error("Source parquet %s does not exist", args.source)
        return 1
    logging.info("Loading panel parquet from %s", args.source)
    df = pd.read_parquet(args.source)
    df["value"] = pd.to_numeric(df["value"], errors="coerce")
    logging.info("Loaded %d long-form rows", len(df))
    deduped = dedupe_panel(df)
    logging.info("After deduplication: %d rows", len(deduped))
    wide = pivot_panel(deduped)
    logging.info("Wide panel shape: %s rows x %s columns", wide.shape[0], wide.shape[1])
    args.output.parent.mkdir(parents=True, exist_ok=True)
    wide.to_csv(args.output, index=False)
    logging.info("Panel CSV written to %s", args.output)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
