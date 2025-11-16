"""Create a Student Financial Aid crosswalk template from the dictionary lake."""
from __future__ import annotations

import argparse
import logging
import re
from pathlib import Path
from typing import Iterable

import pandas as pd

SFA_VAR_RX = re.compile(r"^(SFA|NPT)", re.IGNORECASE)
SURVEY_HINTS = ("SFA", "STUDENT FINANCIAL AID", "NET PRICE", "NET-PRICE")
CROSSWALK_DIR = Path("/Users/markjaysonfarol13/Higher Ed research/IPEDS/Paneled Datasets/Crosswalks")
DICTIONARY_LAKE_PATH = Path("/Users/markjaysonfarol13/Higher Ed research/IPEDS/Parquets/Dictionary/dictionary_lake.parquet")


VAR_COL_CANDIDATES = ["varname", "var_name", "var", "variable"]
SURVEY_YEAR_CANDIDATES = ["survey_year", "SURVEY_YEAR", "year", "YEAR", "panel_year"]
SURVEY_COL_CANDIDATES = ["survey", "SURVEY", "component", "COMPONENT", "survey_label", "component_name"]
TABLE_COL_CANDIDATES = ["table_name", "table", "tableName"]
LABEL_COL_CANDIDATES = ["label", "varLabel", "label_text"]


def resolve_column(df: pd.DataFrame, candidates: Iterable[str], required: bool = True) -> str | None:
    for candidate in candidates:
        if candidate in df.columns:
            return candidate
    if required:
        raise KeyError(f"None of the expected columns are present: {candidates}")
    return None


def filter_sfa_rows(df: pd.DataFrame, var_col: str, survey_col: str | None) -> pd.DataFrame:
    mask = df[var_col].astype(str).str.match(SFA_VAR_RX, na=False)
    if survey_col:
        survey_values = df[survey_col].astype(str).str.upper()
        survey_mask = survey_values.apply(lambda text: any(hint in text for hint in SURVEY_HINTS))
        mask |= survey_mask
    filtered = df.loc[mask].copy()
    return filtered


def build_template(df: pd.DataFrame, year_col: str, var_col: str, survey_col: str | None, table_col: str | None, label_col: str | None) -> pd.DataFrame:
    filtered = filter_sfa_rows(df, var_col, survey_col)
    if filtered.empty:
        logging.warning("No SFA rows found in the dictionary lake.")
    filtered = filtered.drop_duplicates(subset=[var_col, year_col])
    template = pd.DataFrame({
        "concept_key": "",
        "source_var": filtered[var_col].astype(str).str.strip().str.upper(),
        "year_start": filtered[year_col],
        "year_end": filtered[year_col],
        "weight": 1.0,
        "notes": "",
    })
    template["survey_year"] = filtered[year_col]
    if survey_col:
        template["survey"] = filtered[survey_col]
    if table_col:
        template["table_name"] = filtered[table_col]
    if label_col:
        template["label"] = filtered[label_col]
    return template


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--dictionary",
        type=Path,
        default=DICTIONARY_LAKE_PATH,
        help="dictionary_lake.parquet path",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=CROSSWALK_DIR / "sfa_crosswalk_template.csv",
        help="Path for the SFA crosswalk template CSV (outside the repo).",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")

    if not args.dictionary.exists():
        raise FileNotFoundError(f"Dictionary lake not found: {args.dictionary}")

    logging.info("Loading dictionary lake: %s", args.dictionary)
    dictionary_df = pd.read_parquet(args.dictionary)

    var_col = resolve_column(dictionary_df, VAR_COL_CANDIDATES)
    year_col = resolve_column(dictionary_df, SURVEY_YEAR_CANDIDATES)
    survey_col = resolve_column(dictionary_df, SURVEY_COL_CANDIDATES, required=False)
    table_col = resolve_column(dictionary_df, TABLE_COL_CANDIDATES, required=False)
    label_col = resolve_column(dictionary_df, LABEL_COL_CANDIDATES, required=False)

    template = build_template(dictionary_df, year_col, var_col, survey_col, table_col, label_col)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    template.sort_values(["source_var", "year_start"], inplace=True)
    template.to_csv(args.output, index=False)
    logging.info("Saved SFA crosswalk template to %s (%d rows)", args.output, len(template))


if __name__ == "__main__":
    main()
