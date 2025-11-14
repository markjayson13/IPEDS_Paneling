#!/usr/bin/env python3
"""Pivot a raw long-form IPEDS panel (source_var/value) into a wide CSV."""

from __future__ import annotations

import argparse
import logging
from pathlib import Path

import pandas as pd


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Convert raw long-form IPEDS data to wide format")
    parser.add_argument("--input", type=Path, required=True, help="panel_long_raw parquet path")
    parser.add_argument("--output", type=Path, required=True, help="panel_wide_raw CSV path")
    parser.add_argument("--column-field", default="source_var", help="Column name to pivot (default: source_var)")
    parser.add_argument("--id-cols", default="year,UNITID,reporting_unitid", help="Comma list of identifier columns")
    parser.add_argument(
        "--survey-order",
        default=None,
        help="Comma list giving the preferred survey sort order (e.g., HD,IC,IC_AY,EF,…)",
    )
    parser.add_argument("--log-level", default="INFO", help="Logging level")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    logging.basicConfig(level=getattr(logging, args.log_level.upper(), logging.INFO), format="%(levelname)s %(message)s")
    if not args.input.exists():
        logging.error("Input parquet %s not found", args.input)
        return 1
    df = pd.read_parquet(args.input)
    id_cols = [col.strip() for col in args.id_cols.split(",") if col.strip()]
    for col in id_cols:
        if col not in df.columns:
            logging.error("ID column %s missing from input", col)
            return 1
    value_col = "value"
    pivot_col = args.column_field
    if pivot_col not in df.columns:
        logging.error("Column field %s not found in input", pivot_col)
        return 1
    if "reporting_unitid" in df.columns:
        df["reporting_unitid"] = df["reporting_unitid"].fillna(df.get("UNITID"))
    wide = df.pivot_table(index=id_cols, columns=pivot_col, values=value_col, aggfunc="first").reset_index()
    survey_order = [token.strip().upper() for token in (args.survey_order or "").split(",") if token.strip()]
    if survey_order:
        if "survey" not in df.columns:
            logging.warning("Survey column missing from input; cannot apply survey ordering")
        else:
            survey_rank = {survey: idx for idx, survey in enumerate(survey_order)}
            var_to_survey = (
                df[[pivot_col, "survey"]]
                .dropna(subset=[pivot_col, "survey"])
                .drop_duplicates(subset=[pivot_col], keep="first")
                .set_index(pivot_col)["survey"]
                .to_dict()
            )

            def sort_key(col: str) -> tuple[int, str, str]:
                survey = var_to_survey.get(col, "")
                rank = survey_rank.get(survey, len(survey_rank))
                return (rank, survey, col.lower())

            value_cols = [col for col in wide.columns if col not in id_cols]
            ordered_value_cols = sorted(value_cols, key=sort_key)
            wide = wide[id_cols + ordered_value_cols]
    args.output.parent.mkdir(parents=True, exist_ok=True)
    wide.to_csv(args.output, index=False)
    logging.info("Wrote %s rows and %s columns to %s", len(wide), len(wide.columns), args.output)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

