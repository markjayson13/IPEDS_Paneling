"""Build a long Student Financial Aid panel from the raw wide IPEDS panel."""
from __future__ import annotations

import argparse
import logging
import re
from pathlib import Path
from typing import Iterable, List

import pandas as pd

SFA_VAR_RX = re.compile(r"^(SFA|NPT)", re.IGNORECASE)
UNITID_CANDIDATES = ["UNITID", "unitid", "UNIT_ID", "unit_id"]
YEAR_CANDIDATES = ["YEAR", "year", "SURVEY_YEAR", "survey_year", "panel_year", "SURVYEAR", "survyear"]


def resolve_column(df: pd.DataFrame, preferred: str, fallbacks: Iterable[str]) -> str:
    """Return the first column present in df from preferred + fallbacks."""
    candidates = [preferred, *fallbacks]
    for candidate in candidates:
        if candidate in df.columns:
            return candidate
    raise KeyError(f"None of the requested columns are present: {candidates}")


def identify_sfa_columns(columns: Iterable[str], id_cols: Iterable[str]) -> List[str]:
    id_set = set(id_cols)
    selected: List[str] = []
    for column in columns:
        if column in id_set:
            continue
        name = str(column)
        if SFA_VAR_RX.match(name):
            selected.append(column)
            continue
        # Handle canonical names like "sfa_" or "npt_" (already harmonized columns).
        lowered = name.lower()
        if lowered.startswith("sfa") or lowered.startswith("npt"):
            selected.append(column)
    return selected


def build_long_panel(df: pd.DataFrame, unitid_col: str, year_col: str) -> pd.DataFrame:
    sfa_cols = identify_sfa_columns(df.columns, [unitid_col, year_col])
    if not sfa_cols:
        logging.warning("No SFA/NPT columns were detected in the wide panel.")
    subset = df[[unitid_col, year_col] + sfa_cols].copy()
    long_df = subset.melt(id_vars=[unitid_col, year_col], var_name="source_var", value_name="value")
    long_df["value"] = pd.to_numeric(long_df["value"], errors="coerce")
    long_df.dropna(subset=["value"], inplace=True)
    long_df.rename(columns={unitid_col: "UNITID", year_col: "YEAR"}, inplace=True)
    long_df.sort_values(["UNITID", "YEAR", "source_var"], inplace=True)
    return long_df.reset_index(drop=True)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--input-wide",
        type=Path,
        default=Path("data/derived/ipeds_panel_wide.parquet"),
        help="Path to the raw wide IPEDS panel (parquet).",
    )
    parser.add_argument(
        "--output-long",
        type=Path,
        default=Path("data/derived/sfa_step0_long.parquet"),
        help="Destination for the long SFA parquet.",
    )
    parser.add_argument("--unitid-col", type=str, default="UNITID", help="UNITID column name in the wide panel.")
    parser.add_argument("--year-col", type=str, default="YEAR", help="Year column name in the wide panel.")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")

    if not args.input_wide.exists():
        raise FileNotFoundError(f"Wide panel not found: {args.input_wide}")

    logging.info("Loading wide panel: %s", args.input_wide)
    panel_df = pd.read_parquet(args.input_wide)

    try:
        unitid_col = resolve_column(panel_df, args.unitid_col, UNITID_CANDIDATES)
    except KeyError as exc:
        raise KeyError("Unable to determine UNITID column in the wide panel") from exc
    try:
        year_col = resolve_column(panel_df, args.year_col, YEAR_CANDIDATES)
    except KeyError as exc:
        raise KeyError("Unable to determine YEAR column in the wide panel") from exc

    logging.info("Detected UNITID column: %s", unitid_col)
    logging.info("Detected YEAR column: %s", year_col)

    long_df = build_long_panel(panel_df, unitid_col, year_col)
    logging.info("Long SFA rows: %d", len(long_df))

    args.output_long.parent.mkdir(parents=True, exist_ok=True)
    long_df.to_parquet(args.output_long, index=False)
    logging.info("Saved long SFA panel to %s", args.output_long)


if __name__ == "__main__":
    main()
