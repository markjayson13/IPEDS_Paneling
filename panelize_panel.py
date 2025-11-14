#!/usr/bin/env python3
"""
Build a single consolidated wide panel (panel_wide.csv) from the harmonized long-form parquet.

The script:
  * normalizes reporting_unitid (fallback to UNITID when blank),
  * pivots all target variables into columns keyed by (UNITID, year),
  * honors an optional column template to lock the output schema, and
  * emits a conflict report whenever multiple reporting_unitid values appear for the same UNITID-year.

Outputs default to the user's external IPEDS folders so we do not store large CSVs inside the repo.
"""

from __future__ import annotations

import argparse
import logging
from pathlib import Path
from typing import List

import pandas as pd

DEFAULT_OUTPUT = Path("/Users/markjaysonfarol13/Higher Ed research/IPEDS/Artifacts/panel_wide.csv")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Generate a consolidated panel_wide.csv")
    parser.add_argument("--input", type=Path, required=True, help="Path to panel_long.parquet")
    parser.add_argument(
        "--output",
        type=Path,
        default=DEFAULT_OUTPUT,
        help=f"Output CSV path (default: {DEFAULT_OUTPUT})",
    )
    parser.add_argument(
        "--column-template",
        type=Path,
        default=None,
        help="Optional CSV file with a 'column' header specifying the desired column order",
    )
    parser.add_argument("--log-level", default="INFO", help="Logging level (e.g., INFO, DEBUG)")
    return parser.parse_args()


def ensure_reporting_unitid(df: pd.DataFrame) -> pd.DataFrame:
    if "reporting_unitid" not in df.columns:
        df["reporting_unitid"] = pd.NA
    df["reporting_unitid"] = df["reporting_unitid"].replace("", pd.NA)
    if "UNITID" in df.columns:
        df["reporting_unitid"] = df["reporting_unitid"].fillna(df["UNITID"])
    return df


def load_column_template(path: Path | None) -> List[str] | None:
    if path is None:
        return None
    if not path.exists():
        logging.warning("Column template %s not found; ignoring", path)
        return None
    frame = pd.read_csv(path)
    column_field = next((c for c in frame.columns if c.lower() == "column"), None)
    if column_field is None:
        logging.warning("Column template %s missing 'column' header; ignoring", path)
        return None
    template = frame[column_field].dropna().astype(str).tolist()
    # ensure keys always lead
    for key in ["year", "UNITID", "reporting_unitid"]:
        if key not in template:
            template.insert(0, key)
    return template


def build_wide(df: pd.DataFrame, template: List[str] | None) -> pd.DataFrame:
    if df.empty:
        cols = ["year", "UNITID", "reporting_unitid"]
        if template:
            cols = list(dict.fromkeys(template))  # preserve order, drop dupes
        return pd.DataFrame(columns=cols)

    pivot = (
        df.pivot_table(
            index=["UNITID", "year", "reporting_unitid"],
            columns="target_var",
            values="value",
            aggfunc="first",
        )
        .reset_index()
        .copy()
    )

    if template:
        for col in template:
            if col not in pivot.columns:
                pivot[col] = pd.NA
        ordered = [c for c in template if c in pivot.columns]
        extras = [c for c in pivot.columns if c not in ordered]
        return pivot[ordered + extras]

    # no template — keep keys first, then sort remaining columns for stability
    base_cols = ["year", "UNITID", "reporting_unitid"]
    for key in base_cols:
        if key not in pivot.columns:
            pivot[key] = pd.NA
    other_cols = sorted(c for c in pivot.columns if c not in base_cols)
    return pivot[base_cols + other_cols]


def write_conflict_report(df: pd.DataFrame, output_path: Path) -> None:
    conflicts = (
        df.groupby(["UNITID", "year"])["reporting_unitid"]
        .nunique(dropna=True)
        .reset_index(name="n_reporters")
    )
    offenders = conflicts[conflicts["n_reporters"] > 1]
    if offenders.empty:
        logging.info("No reporting conflicts detected.")
        return
    conflict_rows = (
        df.merge(offenders[["UNITID", "year"]], on=["UNITID", "year"], how="inner")[
            ["UNITID", "year", "reporting_unitid"]
        ]
        .drop_duplicates()
        .sort_values(["UNITID", "year"])
    )
    conflict_path = output_path.with_name(output_path.stem + ".reporting_conflicts.csv")
    conflict_rows.to_csv(conflict_path, index=False)
    logging.warning(
        "Detected %d UNITID-year rows with multiple reporters; see %s",
        len(offenders),
        conflict_path,
    )


def main() -> None:
    args = parse_args()
    logging.basicConfig(
        level=getattr(logging, args.log_level.upper(), logging.INFO),
        format="%(levelname)s %(message)s",
    )

    if not args.input.exists():
        raise FileNotFoundError(f"Input parquet not found: {args.input}")

    logging.info("Loading long-form panel from %s", args.input)
    df = pd.read_parquet(args.input)
    df = ensure_reporting_unitid(df)

    template = load_column_template(args.column_template)
    wide = build_wide(df, template)

    args.output.parent.mkdir(parents=True, exist_ok=True)
    wide.sort_values(["year", "UNITID"]).to_csv(args.output, index=False)
    logging.info("Wrote %s with %d rows and %d columns", args.output, len(wide), len(wide.columns))

    write_conflict_report(df, args.output)


if __name__ == "__main__":
    main()

