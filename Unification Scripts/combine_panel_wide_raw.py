#!/usr/bin/env python3
"""
Combine panel_wide_raw_YYYY cross-sections into a single all-years raw panel.

This script assumes you already have one "raw" cross-section per year, with
full IPEDS varnames as columns, e.g.:

  /Users/.../Paneled Datasets/Crosssections/panel_wide_raw_2004.csv
  /Users/.../Paneled Datasets/Crosssections/panel_wide_raw_2005.csv
  ...
  /Users/.../Paneled Datasets/Crosssections/panel_wide_raw_2023.csv

It will:
  1. Find all matching files (panel_wide_raw_*.csv) in the input directory.
  2. Extract the year from the filename.
  3. Read each CSV, standardize column names (strip + uppercase).
  4. Ensure YEAR column exists (from file name if needed).
  5. Concatenate all years with an outer join on columns.
  6. Write a single wide raw panel to:

     /Users/.../Paneled Datasets/panel_wide_raw.csv

You should then call unify_sfa.py with:

  python3 "Unification Scripts/unify_sfa.py" \
    --input-wide "/Users/.../Paneled Datasets/panel_wide_raw.csv"

so SFA/NPT varnames match dictionary_lake and the SFA pipeline works.
"""

from __future__ import annotations

import argparse
import logging
import re
from pathlib import Path
from typing import List

import pandas as pd


DEFAULT_INPUT_DIR = Path(
    "/Users/markjaysonfarol13/Higher Ed research/IPEDS/Paneled Datasets/Crosssections"
)
DEFAULT_OUTPUT = Path(
    "/Users/markjaysonfarol13/Higher Ed research/IPEDS/Paneled Datasets/panel_wide_raw.csv"
)
DEFAULT_PATTERN = "panel_wide_raw_*.csv"

YEAR_RE = re.compile(r"(\d{4})")


def extract_year_from_name(path: Path) -> int | None:
    match = YEAR_RE.search(path.name)
    if match:
        return int(match.group(1))
    return None


def read_and_standardize(file: Path, year_from_name: int | None) -> pd.DataFrame:
    logging.info("Reading %s", file)
    df = pd.read_csv(file)
    df.columns = df.columns.astype(str).str.strip().str.upper()

    year_col_candidates = ["YEAR", "SURVEY_YEAR", "SURVEYYEAR", "ACADYR"]
    found_year_col = None
    for candidate in year_col_candidates:
        if candidate in df.columns:
            found_year_col = candidate
            break

    if found_year_col is not None:
        if found_year_col != "YEAR":
            df.rename(columns={found_year_col: "YEAR"}, inplace=True)
    else:
        if year_from_name is None:
            logging.warning(
                "No YEAR-like column found in %s and no year in file name; YEAR will be missing.",
                file,
            )
        else:
            logging.info("Setting YEAR from file name (%d) for %s", year_from_name, file)
            df["YEAR"] = year_from_name

    return df


def combine_raw_panels(input_dir: Path, pattern: str, output: Path) -> None:
    files: List[Path] = sorted(input_dir.glob(pattern))
    if not files:
        raise FileNotFoundError(
            f"No files matching pattern '{pattern}' found in {input_dir}"
        )

    logging.info("Found %d panel_wide_raw_* cross-section files.", len(files))
    frames: List[pd.DataFrame] = []

    for file in files:
        year_from_name = extract_year_from_name(file)
        df = read_and_standardize(file, year_from_name)
        frames.append(df)

    logging.info("Concatenating %d dataframes...", len(frames))
    combined = pd.concat(frames, axis=0, ignore_index=True, sort=True)

    sort_cols: List[str] = []
    if "UNITID" in combined.columns:
        sort_cols.append("UNITID")
    if "YEAR" in combined.columns:
        sort_cols.append("YEAR")

    if sort_cols:
        combined.sort_values(sort_cols, inplace=True)

    logging.info(
        "Combined raw panel shape: %s rows x %s columns",
        combined.shape[0],
        combined.shape[1],
    )

    output.parent.mkdir(parents=True, exist_ok=True)
    combined.to_csv(output, index=False)
    logging.info("Wrote combined raw panel to %s", output)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Combine panel_wide_raw_YYYY cross-sections into panel_wide_raw.csv"
    )
    parser.add_argument(
        "--input-dir",
        type=Path,
        default=DEFAULT_INPUT_DIR,
        help="Directory with panel_wide_raw_*.csv cross-section files.",
    )
    parser.add_argument(
        "--pattern",
        type=str,
        default=DEFAULT_PATTERN,
        help="Glob pattern for raw cross-section files.",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=DEFAULT_OUTPUT,
        help="Output path for combined raw panel CSV.",
    )
    parser.add_argument(
        "--log-level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        help="Logging level.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    logging.basicConfig(
        level=getattr(logging, args.log_level),
        format="%(levelname)s:%(name)s:%(message)s",
    )
    combine_raw_panels(args.input_dir, args.pattern, args.output)


if __name__ == "__main__":
    main()
