#!/usr/bin/env python3
"""Merge multiple per-year panel_wide_raw CSVs into a single dataset."""

from __future__ import annotations

import argparse
import logging
import re
from pathlib import Path
from typing import Iterable, List, Sequence

import pandas as pd


PANEL_FILENAME_RE = re.compile(r"panel_wide_raw_(?P<year>\d{4})\.csv$", re.IGNORECASE)


def parse_years(expr: str | None) -> set[int] | None:
    if not expr:
        return None
    years: set[int] = set()
    for part in expr.split(","):
        token = part.strip()
        if not token:
            continue
        if "-" in token:
            start, end = token.split("-", 1)
            lo, hi = sorted((int(start), int(end)))
            years.update(range(lo, hi + 1))
        else:
            years.add(int(token))
    return years or None


def discover_inputs(input_dir: Path, years: set[int] | None) -> List[tuple[int, Path]]:
    inputs: List[tuple[int, Path]] = []
    for path in sorted(input_dir.glob("panel_wide_raw_*.csv")):
        match = PANEL_FILENAME_RE.search(path.name)
        if not match:
            continue
        year = int(match.group("year"))
        if years is not None and year not in years:
            continue
        inputs.append((year, path))
    return inputs


def load_panel_csv(path: Path, id_cols: Sequence[str]) -> pd.DataFrame:
    logging.info("Reading %s", path)
    df = pd.read_csv(path, dtype=str, low_memory=False)
    missing = [col for col in id_cols if col not in df.columns]
    if missing:
        raise ValueError(f"{path} missing required columns: {missing}")
    for col in id_cols:
        df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")
    if "reporting_unitid" in df.columns:
        df["reporting_unitid"].fillna(df["UNITID"], inplace=True)
    else:
        df["reporting_unitid"] = df["UNITID"]
    for col in df.columns:
        if col in id_cols:
            continue
        df[col] = df[col].astype(pd.StringDtype())
    return df


def merge_panels(paths: Iterable[Path]) -> pd.DataFrame:
    id_cols = ["year", "UNITID", "reporting_unitid"]
    frames = [load_panel_csv(path, id_cols) for path in paths]
    if not frames:
        raise RuntimeError("No input CSVs were provided")
    merged = pd.concat(frames, ignore_index=True, sort=False)
    merged["reporting_unitid"].fillna(merged["UNITID"], inplace=True)
    return merged


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Merge panel_wide_raw CSV exports")
    parser.add_argument(
        "--input-dir",
        type=Path,
        required=True,
        help="Directory containing panel_wide_raw_<YEAR>.csv files",
    )
    parser.add_argument(
        "--years",
        type=str,
        default=None,
        help="Comma list or ranges (e.g., 2004,2006-2008). Default: all years present",
    )
    parser.add_argument("--output", type=Path, required=True, help="Path to the merged CSV output")
    parser.add_argument("--log-level", default="INFO", help="Logging level")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    logging.basicConfig(
        level=getattr(logging, args.log_level.upper(), logging.INFO),
        format="%(levelname)s %(message)s",
    )
    if not args.input_dir.exists():
        logging.error("Input directory %s does not exist", args.input_dir)
        return 1
    years = parse_years(args.years)
    inputs = discover_inputs(args.input_dir, years)
    if not inputs:
        logging.error("No panel_wide_raw CSV files found under %s", args.input_dir)
        return 1
    logging.info("Merging %s files", len(inputs))
    merged = merge_panels(path for _, path in inputs)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    merged.to_csv(args.output, index=False)
    logging.info("Wrote %s rows and %s columns to %s", len(merged), len(merged.columns), args.output)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
