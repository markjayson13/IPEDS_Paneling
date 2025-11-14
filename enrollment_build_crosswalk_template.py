#!/usr/bin/env python3
"""Build a template crosswalk for enrollment (EF/E12) variables."""

from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path
from typing import Iterable, List

import pandas as pd


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


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dictionary", type=Path, required=True, help="Path to dictionary_lake.parquet")
    parser.add_argument("--years", type=str, required=True, help='Year range (e.g. "2004-2024" or "2010 2011")')
    parser.add_argument("--output", type=Path, required=True, help="CSV output path for the template")
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

    template = pd.DataFrame(
        {
            "concept_key": "",
            "source_var": lake_enroll["source_var"],
            "year_start": lake_enroll["year"],
            "year_end": lake_enroll["year"],
            "weight": 1.0,
            "survey": lake_enroll["survey"],
            "survey_hint": lake_enroll["survey_hint"],
            "subsurvey": lake_enroll["subsurvey"],
            "source_label": lake_enroll["source_label"],
            "label_norm": lake_enroll["label_norm"],
            "table_name": lake_enroll["table_name"],
            "data_filename": lake_enroll["data_filename"],
            "note": "",
        }
    )

    template = template.sort_values(["year_start", "survey", "subsurvey", "source_var"]).reset_index(drop=True)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    template.to_csv(args.output, index=False)

    logging.info("Wrote %s rows to %s", len(template), args.output)
    logging.info("Reminder: edit this template to create your final enrollment_crosswalk.csv.")


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:  # noqa: BLE001
        logging.error("enrollment_build_crosswalk_template failed: %s", exc)
        sys.exit(1)
