#!/usr/bin/env python3
"""Build a starter Admissions crosswalk for manual concept mapping.

The resulting CSV lists every Admissions source variable detected in the step0
long file along with the YEAR coverage observed in the panel and lightweight
dictionary metadata. A human editor should fill in ``concept_key`` plus edit
year ranges to capture the SAT (2016 redesign) and ACT writing breaks described
in the project notes.
"""

from __future__ import annotations

import argparse
import logging
from pathlib import Path
from typing import Iterable

import pandas as pd

DEFAULT_STEP0 = Path("data/adm_step0_long.parquet")
DEFAULT_DICT = Path("data/dictionary_lake.parquet")
DEFAULT_OUTPUT = Path("data/adm_crosswalk.csv")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--step0", type=Path, default=DEFAULT_STEP0, help="Admissions step0 long parquet path")
    parser.add_argument("--dictionary-lake", type=Path, default=DEFAULT_DICT, help="dictionary_lake.parquet path")
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT, help="Destination CSV for manual editing")
    parser.add_argument("--log-level", default="INFO")
    return parser.parse_args()


def resolve_column(df: pd.DataFrame, candidates: Iterable[str], required: bool = True) -> str | None:
    for candidate in candidates:
        if candidate in df.columns:
            return candidate
    if required:
        raise KeyError(f"None of the expected columns are present: {candidates}")
    return None


def load_step0(path: Path) -> pd.DataFrame:
    if not path.exists():
        raise FileNotFoundError(f"Admissions step0 file not found: {path}")
    df = pd.read_parquet(path)
    if "source_var" not in df.columns or "YEAR" not in df.columns:
        raise ValueError("adm_step0_long.parquet must have source_var and YEAR columns")
    df = df.copy()
    df["source_var"] = df["source_var"].astype(str).str.upper()
    df["YEAR"] = pd.to_numeric(df["YEAR"], errors="coerce").astype("Int64")
    df.dropna(subset=["YEAR", "source_var"], inplace=True)
    return df


def summarize_coverage(step0: pd.DataFrame) -> pd.DataFrame:
    grouped = step0.groupby("source_var")["YEAR"].agg(min_year="min", max_year="max", observations="count")
    summary = grouped.reset_index()
    summary.rename(columns={"source_var": "source_var"}, inplace=True)
    return summary


def load_dictionary(path: Path) -> tuple[pd.DataFrame, str, str | None, str | None, str | None]:
    if not path.exists():
        raise FileNotFoundError(f"Dictionary lake not found: {path}")
    dictionary = pd.read_parquet(path)
    var_col = resolve_column(dictionary, ["varname", "source_var", "VAR_NAME", "variable", "var"])
    label_col = resolve_column(dictionary, ["label", "source_label", "TITLE", "varTitle"], required=False)
    survey_col = resolve_column(dictionary, ["survey", "component", "SURVEY"], required=False)
    survey_hint_col = resolve_column(dictionary, ["survey_hint", "SURVEY_HINT"], required=False)
    year_col = resolve_column(dictionary, ["year", "YEAR", "survey_year", "SURVEYYEAR"], required=False)
    dict_copy = dictionary.copy()
    dict_copy[var_col] = dict_copy[var_col].astype(str).str.strip().str.upper()
    if year_col:
        dict_copy[year_col] = pd.to_numeric(dict_copy[year_col], errors="coerce")
        dict_copy.sort_values([var_col, year_col], inplace=True)
    else:
        dict_copy.sort_values(var_col, inplace=True)
    dict_unique = dict_copy.drop_duplicates(subset=[var_col], keep="first")
    columns = [var_col]
    for col in (label_col, survey_col, survey_hint_col):
        if col:
            columns.append(col)
    return dict_unique[columns], var_col, label_col, survey_col, survey_hint_col


def build_template(summary: pd.DataFrame, dictionary: pd.DataFrame, var_col: str, label_col: str | None, survey_col: str | None, survey_hint_col: str | None) -> pd.DataFrame:
    merged = summary.merge(dictionary, how="left", left_on="source_var", right_on=var_col)
    template = pd.DataFrame(
        {
            "concept_key": "",
            "source_var": merged["source_var"],
            "year_start": merged["min_year"],
            "year_end": merged["max_year"],
            "weight": 1.0,
            "note": "",
        }
    )
    if label_col:
        template["label"] = merged[label_col]
    else:
        template["label"] = ""
    if survey_hint_col and survey_hint_col in merged.columns:
        template["survey_hint"] = merged[survey_hint_col]
    elif survey_col and survey_col in merged.columns:
        template["survey_hint"] = merged[survey_col]
    else:
        template["survey_hint"] = ""
    template.sort_values("source_var", inplace=True)
    return template


def main() -> None:
    args = parse_args()
    logging.basicConfig(level=getattr(logging, args.log_level.upper(), logging.INFO), format="%(levelname)s %(message)s")

    step0 = load_step0(args.step0)
    coverage = summarize_coverage(step0)
    logging.info("Found %d admissions source variables in step0", len(coverage))

    dictionary, var_col, label_col, survey_col, survey_hint_col = load_dictionary(args.dictionary_lake)
    template = build_template(coverage, dictionary, var_col, label_col, survey_col, survey_hint_col)

    args.output.parent.mkdir(parents=True, exist_ok=True)
    template.to_csv(args.output, index=False)
    logging.info("Wrote Admissions crosswalk template (%d rows) to %s", len(template), args.output)
    logging.info("Fill concept_key and adjust SAT/ACT year ranges per manual spec before harmonization.")


if __name__ == "__main__":
    main()
