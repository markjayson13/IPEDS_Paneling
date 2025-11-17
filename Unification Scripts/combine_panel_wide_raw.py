#!/usr/bin/env python3
"""Combine yearly panel_wide_raw_*.csv files into a single panel_wide_raw.csv."""

from __future__ import annotations

import argparse
import logging
import re
from pathlib import Path
from typing import Iterable

import pandas as pd

DEFAULT_INPUT_DIR = Path("/Users/markjaysonfarol13/Higher Ed research/IPEDS/Paneled Datasets/Crosssections")
DEFAULT_PATTERN = "panel_wide_raw_*.csv"
DEFAULT_OUTPUT = Path("/Users/markjaysonfarol13/Higher Ed research/IPEDS/Paneled Datasets/panel_wide_raw.csv")
YEAR_RX = re.compile(r"(\d{4})")
YEAR_COL_CANDIDATES = ["YEAR", "year", "SURVEY_YEAR", "survey_year", "SURVEYYEAR", "panel_year", "ACADYR"]


def extract_year_from_name(path: Path) -> int | None:
    match = YEAR_RX.search(path.stem)
    if match:
        try:
            return int(match.group(1))
        except ValueError:
            return None
    return None


def read_and_standardize(file: Path, year_from_name: int | None) -> pd.DataFrame:
    logging.info("Reading %s", file)
    df = pd.read_csv(file)
    df.columns = df.columns.astype(str).str.strip().str.upper()
    if df.columns.duplicated().any():
        logging.warning("Detected duplicate columns in %s; assigning unique suffixes.", file.name)
        counts: dict[str, int] = {}
        new_cols: list[str] = []
        for col in df.columns:
            cnt = counts.get(col, 0)
            if cnt == 0:
                new_cols.append(col)
            else:
                new_cols.append(f"{col}__DUP{cnt}")
            counts[col] = cnt + 1
        df.columns = new_cols

    year_col = None
    for candidate in YEAR_COL_CANDIDATES:
        candidate_upper = candidate.upper()
        if candidate_upper in df.columns:
            year_col = candidate_upper
            break

    if year_col is None:
        if year_from_name is None:
            raise ValueError(f"Unable to determine YEAR for {file}")
        df["YEAR"] = year_from_name
    elif year_col != "YEAR":
        df.rename(columns={year_col: "YEAR"}, inplace=True)

    if year_from_name is not None:
        # Overwrite with extracted year when files may aggregate multiple forms.
        df["YEAR"] = year_from_name

    return df


def combine_raw_panels(input_dir: Path, pattern: str, output: Path) -> None:
    files = sorted(input_dir.glob(pattern))
    if not files:
        raise SystemExit(f"No files matching {pattern} found in {input_dir}")
    logging.info("Found %d files to combine.", len(files))

    frames = []
    for file in files:
        year = extract_year_from_name(file)
        frames.append(read_and_standardize(file, year))

    combined = pd.concat(frames, ignore_index=True, sort=True)
    if {"UNITID", "YEAR"} <= set(combined.columns):
        combined.sort_values(["UNITID", "YEAR"], inplace=True)
    logging.info("Combined panel shape: %s rows x %s columns", len(combined), len(combined.columns))

    output.parent.mkdir(parents=True, exist_ok=True)
    combined.to_csv(output, index=False)
    logging.info("Wrote combined raw panel to %s", output)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input-dir", type=Path, default=DEFAULT_INPUT_DIR, help="Directory containing panel_wide_raw_*.csv files")
    parser.add_argument("--pattern", type=str, default=DEFAULT_PATTERN, help="Glob pattern for raw panel files")
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT, help="Output CSV path for combined panel")
    parser.add_argument("--log-level", default="INFO", help="Logging level (e.g., INFO, DEBUG)")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    logging.basicConfig(level=getattr(logging, args.log_level.upper(), logging.INFO), format="%(levelname)s %(message)s")
    combine_raw_panels(args.input_dir, args.pattern, args.output)


if __name__ == "__main__":
    main()
