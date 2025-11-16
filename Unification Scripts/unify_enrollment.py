#!/usr/bin/env python3
"""Step 0 – Melt per-year wide enrollment panels (EF/E12) into a long format."""

from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path
from typing import Iterable, List

import pandas as pd

DEFAULT_DICTIONARY = Path(
    "/Users/markjaysonfarol13/Higher Ed research/IPEDS/Parquets/dictionary_lake.parquet"
)
DEFAULT_PANEL_ROOT = Path(
    "/Users/markjaysonfarol13/Higher Ed research/IPEDS/Paneled Datasets/Crosssections"
)
DEFAULT_YEARS = "2004-2024"
DEFAULT_OUTPUT = Path(
    "/Users/markjaysonfarol13/Higher Ed research/IPEDS/Parquets/Unify/Enrolllong/enrollment_step0_long.parquet"
)


def parse_years(expr: str) -> List[int]:
    tokens: List[str] = []
    if not expr:
        return []
    tokens.extend(expr.replace(",", " ").split())
    years: set[int] = set()
    for token in tokens:
        if not token:
            continue
        if "-" in token:
            left, right = token.split("-", 1)
            try:
                start = int(left)
                end = int(right)
            except ValueError:
                raise ValueError(f"Invalid year range token: {token}") from None
            lo, hi = sorted((start, end))
            years.update(range(lo, hi + 1))
        else:
            try:
                years.add(int(token))
            except ValueError:
                raise ValueError(f"Invalid year token: {token}") from None
    return sorted(years)


def load_lake(path: Path) -> pd.DataFrame:
    if not path.exists():
        raise FileNotFoundError(f"Dictionary lake not found: {path}")
    return pd.read_parquet(path)


def select_enrollment_vars(lake: pd.DataFrame, years: Iterable[int]) -> pd.DataFrame:
    year_set = set(years)
    mask = (
        lake["year"].isin(year_set)
        & (
            lake["survey"].str.upper().isin({"EF", "E12", "12MONTHENROLLMENT"})
            | lake["survey_hint"].isin({"FallEnrollment", "12MonthEnrollment"})
        )
    )
    subset = lake.loc[mask].copy()
    if subset.empty:
        raise RuntimeError("No enrollment variables found in dictionary_lake for requested years.")
    if "source_var" in subset.columns:
        subset = subset.drop_duplicates(subset=["year", "source_var"])
    return subset


def find_panel_file(panel_root: Path, year: int) -> Path | None:
    candidates = [
        panel_root / f"panel_wide_raw_{year}.parquet",
        panel_root / f"panel_wide_raw_{year}.csv",
    ]
    for path in candidates:
        if path.exists():
            return path
    return None


def read_panel(path: Path) -> pd.DataFrame:
    if path.suffix.lower() == ".parquet":
        return pd.read_parquet(path)
    if path.suffix.lower() == ".csv":
        return pd.read_csv(path, dtype=str, low_memory=False)
    raise ValueError(f"Unsupported panel file format: {path}")


def build_long_for_year(
    df: pd.DataFrame,
    year: int,
    vars_year: set[str],
) -> pd.DataFrame | None:
    df = df.copy()
    df.columns = pd.Index(str(c).upper() for c in df.columns)
    if "UNITID" not in df.columns:
        raise RuntimeError(f"UNITID column missing in panel for year {year}")
    if "YEAR" not in df.columns:
        df["YEAR"] = year
    else:
        df["YEAR"] = pd.to_numeric(df["YEAR"], errors="coerce").fillna(year).astype(int)

    value_vars = sorted(vars_year.intersection(df.columns))
    if not value_vars:
        logging.warning("No enrollment columns found in panel for YEAR=%s", year)
        return None

    long_df = df.melt(
        id_vars=["UNITID", "YEAR"],
        value_vars=value_vars,
        var_name="source_var",
        value_name="value",
    )
    long_df["value"] = pd.to_numeric(long_df["value"], errors="coerce")
    return long_df


def attach_metadata(long_df: pd.DataFrame, lake_year: pd.DataFrame) -> pd.DataFrame:
    merged = long_df.copy()
    merged["source_var_upper"] = merged["source_var"].str.upper()

    meta = lake_year.copy()
    meta["source_var_upper"] = meta["source_var"].str.upper()

    merged = merged.merge(
        meta[
            [
                "year",
                "source_var_upper",
                "survey",
                "survey_hint",
                "subsurvey",
                "source_label",
                "table_name",
                "data_filename",
            ]
        ],
        left_on=["YEAR", "source_var_upper"],
        right_on=["year", "source_var_upper"],
        how="left",
    )
    merged.drop(columns=["source_var_upper", "year"], inplace=True)
    return merged[
        [
            "YEAR",
            "UNITID",
            "source_var",
            "survey",
            "survey_hint",
            "subsurvey",
            "source_label",
            "table_name",
            "data_filename",
            "value",
        ]
    ]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--dictionary",
        type=Path,
        default=DEFAULT_DICTIONARY,
        help=f"Path to dictionary_lake.parquet. Default: {DEFAULT_DICTIONARY}",
    )
    parser.add_argument(
        "--panel-root",
        type=Path,
        default=DEFAULT_PANEL_ROOT,
        help=f"Directory containing panel_wide_raw files. Default: {DEFAULT_PANEL_ROOT}",
    )
    parser.add_argument(
        "--years",
        type=str,
        default=DEFAULT_YEARS,
        help='Year range (e.g. "2004-2024" or "2010 2011"). Default: %(default)s',
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=DEFAULT_OUTPUT,
        help=f"Output parquet path for step0 long data. Default: {DEFAULT_OUTPUT}",
    )
    parser.add_argument("--log-level", default="INFO")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    logging.basicConfig(level=getattr(logging, args.log_level.upper(), logging.INFO), format="%(levelname)s %(message)s")

    years = parse_years(args.years)
    if not years:
        raise SystemExit("No valid years parsed from --years argument.")

    lake = load_lake(args.dictionary)
    lake_enroll = select_enrollment_vars(lake, years)

    long_frames: list[pd.DataFrame] = []
    for year in years:
        panel_file = find_panel_file(args.panel_root, year)
        if not panel_file:
            logging.warning("Panel file not found for YEAR=%s under %s", year, args.panel_root)
            continue
        logging.info("Processing YEAR=%s from %s", year, panel_file)
        panel_df = read_panel(panel_file)

        vars_year = set(
            lake_enroll.loc[lake_enroll["year"] == year, "source_var"].dropna().str.upper().unique()
        )
        if not vars_year:
            logging.warning("Dictionary lake has no enrollment variables for YEAR=%s", year)
            continue
        long_year = build_long_for_year(panel_df, year, vars_year)
        if long_year is None or long_year.empty:
            continue
        meta_year = lake_enroll.loc[lake_enroll["year"] == year]
        long_year = attach_metadata(long_year, meta_year)
        long_frames.append(long_year)

    if not long_frames:
        raise SystemExit("No enrollment data collected; check dictionaries and panel files.")

    long_all = pd.concat(long_frames, ignore_index=True)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    long_all.to_parquet(args.output, index=False, compression="snappy")

    logging.info(
        "Wrote %s rows (%s years, %s UNITIDs, %s variables) to %s",
        len(long_all),
        long_all["YEAR"].nunique(),
        long_all["UNITID"].nunique(),
        long_all["source_var"].nunique(),
        args.output,
    )


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:  # noqa: BLE001
        logging.error("unify_enrollment failed: %s", exc)
        sys.exit(1)
