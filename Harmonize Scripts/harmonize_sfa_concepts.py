"""Apply the SFA crosswalk to the long SFA panel and produce concept-level data."""
from __future__ import annotations

import argparse
import logging
from pathlib import Path
from typing import Iterable

import pandas as pd

UNITID_CANDIDATES = ["UNITID", "unitid", "UNIT_ID", "unit_id"]
YEAR_CANDIDATES = ["YEAR", "year", "SURVEY_YEAR", "survey_year", "panel_year", "SURVYEAR", "survyear"]


def resolve_column(df: pd.DataFrame, preferred: str, fallbacks: Iterable[str]) -> str:
    candidates = [preferred, *fallbacks]
    for candidate in candidates:
        if candidate in df.columns:
            return candidate
    raise KeyError(f"None of the requested columns are present: {candidates}")


def expand_crosswalk(crosswalk: pd.DataFrame) -> pd.DataFrame:
    cw = crosswalk.copy()
    cw = cw.dropna(subset=["source_var"])
    cw["weight"] = pd.to_numeric(cw.get("weight", 1.0), errors="coerce").fillna(1.0)
    cw["year_start"] = pd.to_numeric(cw.get("year_start"), errors="coerce")
    cw["year_end"] = pd.to_numeric(cw.get("year_end"), errors="coerce")
    cw.loc[cw["year_start"].isna(), "year_start"] = cw.loc[cw["year_start"].isna(), "year_end"]
    cw.loc[cw["year_end"].isna(), "year_end"] = cw.loc[cw["year_end"].isna(), "year_start"]
    cw.dropna(subset=["year_start", "year_end"], inplace=True)
    cw["year_start"] = cw["year_start"].astype(int)
    cw["year_end"] = cw["year_end"].astype(int)

    records = []
    for row in cw.itertuples():
        concept = getattr(row, "concept_key", None)
        if not concept:
            continue
        start, end = int(row.year_start), int(row.year_end)
        if end < start:
            start, end = end, start
        for year in range(start, end + 1):
            records.append({
                "source_var": row.source_var,
                "concept_key": concept,
                "YEAR": year,
                "weight": row.weight,
            })
    expanded = pd.DataFrame.from_records(records)
    if expanded.empty:
        logging.warning("Expanded crosswalk is empty after filtering by concept keys.")
    return expanded


def harmonize(long_df: pd.DataFrame, crosswalk_df: pd.DataFrame, unitid_col: str, year_col: str) -> pd.DataFrame:
    expanded_cw = expand_crosswalk(crosswalk_df)
    merged = long_df.merge(expanded_cw, how="left", left_on=["source_var", year_col], right_on=["source_var", "YEAR"])
    merged.drop(columns=["YEAR_y"], inplace=True, errors="ignore")
    if "YEAR_x" in merged.columns:
        merged.rename(columns={"YEAR_x": year_col}, inplace=True)
    merged = merged.dropna(subset=["concept_key"])
    if merged.empty:
        logging.warning("No rows matched the crosswalk. Check concept assignments and year ranges.")
        return pd.DataFrame(columns=[unitid_col, year_col])
    merged["weighted_value"] = merged["value"] * merged["weight"]
    grouped = (
        merged.groupby([unitid_col, year_col, "concept_key"], as_index=False)["weighted_value"].sum()
    )
    wide = grouped.pivot_table(index=[unitid_col, year_col], columns="concept_key", values="weighted_value")
    wide.sort_index(axis=1, inplace=True)
    wide.reset_index(inplace=True)
    wide.rename(columns={unitid_col: "UNITID", year_col: "YEAR"}, inplace=True)
    return wide


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--input-long",
        type=Path,
        default=Path("data/derived/sfa_step0_long.parquet"),
        help="Long SFA parquet from unify_sfa.py",
    )
    parser.add_argument(
        "--crosswalk",
        type=Path,
        default=Path("data/derived/meta/sfa_crosswalk.csv"),
        help="Edited crosswalk CSV",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=Path("data/derived/sfa_concepts_wide.parquet"),
        help="Destination concept-level parquet",
    )
    parser.add_argument("--unitid-col", type=str, default="UNITID", help="UNITID column name in the long file.")
    parser.add_argument("--year-col", type=str, default="YEAR", help="Year column name in the long file.")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")

    if not args.input_long.exists():
        raise FileNotFoundError(f"Long SFA file not found: {args.input_long}")
    if not args.crosswalk.exists():
        raise FileNotFoundError(f"Crosswalk CSV not found: {args.crosswalk}")

    logging.info("Loading long SFA file: %s", args.input_long)
    long_df = pd.read_parquet(args.input_long)

    logging.info("Loading crosswalk: %s", args.crosswalk)
    crosswalk_df = pd.read_csv(args.crosswalk)

    try:
        unitid_col = resolve_column(long_df, args.unitid_col, UNITID_CANDIDATES)
    except KeyError as exc:
        raise KeyError("Unable to determine UNITID column in the long file") from exc
    try:
        year_col = resolve_column(long_df, args.year_col, YEAR_CANDIDATES)
    except KeyError as exc:
        raise KeyError("Unable to determine YEAR column in the long file") from exc

    logging.info("Detected UNITID column: %s", unitid_col)
    logging.info("Detected YEAR column: %s", year_col)

    concept_wide = harmonize(long_df, crosswalk_df, unitid_col, year_col)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    concept_wide.to_parquet(args.output, index=False)
    logging.info("Saved SFA concepts panel to %s", args.output)


if __name__ == "__main__":
    main()
