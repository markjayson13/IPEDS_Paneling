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

DEFAULT_STEP0 = Path(
    "/Users/markjaysonfarol13/Higher Ed research/IPEDS/Parquets/Unify/Step0adm/adm_step0_long.parquet"
)
DEFAULT_DICT = Path("/Users/markjaysonfarol13/Higher Ed research/IPEDS/Parquets/Dictionary/dictionary_lake.parquet")
DEFAULT_OUTPUT = Path(
    "/Users/markjaysonfarol13/Higher Ed research/IPEDS/Paneled Datasets/Crosswalks/adm_crosswalk.csv"
)
DEFAULT_FILLED_OUTPUT = Path(
    "/Users/markjaysonfarol13/Higher Ed research/IPEDS/Paneled Datasets/Crosswalks/Filled/adm_crosswalk_filled.csv"
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--step0", type=Path, default=DEFAULT_STEP0, help="Admissions step0 long parquet path")
    parser.add_argument("--dictionary-lake", type=Path, default=DEFAULT_DICT, help="dictionary_lake.parquet path")
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT, help="Destination CSV for manual editing")
    parser.add_argument(
        "--filled-output",
        type=Path,
        default=DEFAULT_FILLED_OUTPUT,
        help="Optional path for a pre-filled Admissions crosswalk (concept keys + SAT split).",
    )
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
        year_stats = (
            dict_copy.groupby(var_col)[year_col].agg(dict_min_year="min", dict_max_year="max").reset_index()
        )
        dict_copy = dict_copy.merge(year_stats, on=var_col, how="left")
        dict_copy.sort_values([var_col, year_col], inplace=True)
    else:
        dict_copy.sort_values(var_col, inplace=True)
        dict_copy["dict_min_year"] = pd.NA
        dict_copy["dict_max_year"] = pd.NA
    dict_unique = dict_copy.drop_duplicates(subset=[var_col], keep="last")
    columns = [var_col, "dict_min_year", "dict_max_year"]
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
            "dict_min_year": merged.get("dict_min_year"),
            "dict_max_year": merged.get("dict_max_year"),
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


def fill_adm_crosswalk(template: pd.DataFrame) -> pd.DataFrame:
    """Populate concept keys and SAT old/new splits for the Admissions crosswalk."""

    required_cols = {"concept_key", "source_var", "year_start", "year_end"}
    missing = required_cols - set(template.columns)
    if missing:
        raise ValueError(f"Admissions crosswalk template is missing columns: {missing}")

    df = template.copy()
    df["source_var"] = df["source_var"].astype(str).str.strip().str.upper()
    if "concept_key" not in df.columns:
        df["concept_key"] = ""

    mapping_simple = {
        "APPLCN": "ADM_N_APPLICANTS_TOTAL",
        "APPLCNM": "ADM_N_APPLICANTS_MEN",
        "APPLCNW": "ADM_N_APPLICANTS_WOMEN",
        "ADMSSN": "ADM_N_ADMITTED_TOTAL",
        "ADMSSNM": "ADM_N_ADMITTED_MEN",
        "ADMSSNW": "ADM_N_ADMITTED_WOMEN",
        "ENRLT": "ADM_N_ENROLLED_TOTAL",
        "ENRLTM": "ADM_N_ENROLLED_MEN",
        "ENRLTW": "ADM_N_ENROLLED_WOMEN",
        "SATNUM": "ADM_N_SAT_SUBMIT",
        "ACTNUM": "ADM_N_ACT_SUBMIT",
        "ACTCM25": "ADM_ACT_COMP_25_PCT",
        "ACTCM75": "ADM_ACT_COMP_75_PCT",
        "ACTEN25": "ADM_ACT_ENGL_25_PCT",
        "ACTEN75": "ADM_ACT_ENGL_75_PCT",
        "ACTMT25": "ADM_ACT_MATH_25_PCT",
        "ACTMT75": "ADM_ACT_MATH_75_PCT",
        "ACTWR25": "ADM_ACT_WRIT_25_PCT_OLD",
        "ACTWR75": "ADM_ACT_WRIT_75_PCT_OLD",
        "SATWR25": "ADM_SAT_WRIT_25_PCT_OLD",
        "SATWR75": "ADM_SAT_WRIT_75_PCT_OLD",
    }

    sat_split_year = 2016
    sat_vars = {"SATVR25", "SATVR75", "SATMT25", "SATMT75"}
    rows: list[pd.Series] = []

    for _, row in df.iterrows():
        sv = str(row["source_var"]).strip().upper()
        if sv in sat_vars:
            continue
        new_row = row.copy()
        concept = mapping_simple.get(sv)
        if concept:
            new_row["concept_key"] = concept
        else:
            if not str(new_row.get("concept_key", "")).strip():
                logging.warning("No concept_key mapping for source_var=%s; leaving blank in filled crosswalk.", sv)
        rows.append(new_row)

    sat_mapping = {
        "SATVR25": ("ADM_SAT_CR_25_PCT_OLD", "ADM_SAT_EBRW_25_PCT_NEW"),
        "SATVR75": ("ADM_SAT_CR_75_PCT_OLD", "ADM_SAT_EBRW_75_PCT_NEW"),
        "SATMT25": ("ADM_SAT_MATH_25_PCT_OLD", "ADM_SAT_MATH_25_PCT_NEW"),
        "SATMT75": ("ADM_SAT_MATH_75_PCT_OLD", "ADM_SAT_MATH_75_PCT_NEW"),
    }

    for sv, (old_ck, new_ck) in sat_mapping.items():
        subset = df.loc[df["source_var"] == sv]
        if subset.empty:
            logging.warning("Expected SAT variable %s not found in template; skipping split.", sv)
            continue
        for _, base in subset.iterrows():
            year_start = int(base["year_start"])
            year_end = int(base["year_end"])
            if year_start > year_end:
                continue
            old_end = min(year_end, sat_split_year - 1)
            if year_start <= old_end:
                old_row = base.copy()
                old_row["concept_key"] = old_ck
                old_row["year_start"] = year_start
                old_row["year_end"] = old_end
                rows.append(old_row)
            new_start = max(year_start, sat_split_year)
            if new_start <= year_end:
                new_row = base.copy()
                new_row["concept_key"] = new_ck
                new_row["year_start"] = new_start
                new_row["year_end"] = year_end
                rows.append(new_row)

    filled = pd.DataFrame(rows).reset_index(drop=True)
    present = set(filled["concept_key"].astype(str).str.strip().unique())
    expected = {
        "ADM_N_APPLICANTS_TOTAL",
        "ADM_N_APPLICANTS_MEN",
        "ADM_N_APPLICANTS_WOMEN",
        "ADM_N_ADMITTED_TOTAL",
        "ADM_N_ADMITTED_MEN",
        "ADM_N_ADMITTED_WOMEN",
        "ADM_N_ENROLLED_TOTAL",
        "ADM_N_ENROLLED_MEN",
        "ADM_N_ENROLLED_WOMEN",
        "ADM_N_ACT_SUBMIT",
        "ADM_N_SAT_SUBMIT",
        "ADM_ACT_COMP_25_PCT",
        "ADM_ACT_COMP_75_PCT",
        "ADM_ACT_ENGL_25_PCT",
        "ADM_ACT_ENGL_75_PCT",
        "ADM_ACT_MATH_25_PCT",
        "ADM_ACT_MATH_75_PCT",
        "ADM_ACT_WRIT_25_PCT_OLD",
        "ADM_ACT_WRIT_75_PCT_OLD",
        "ADM_SAT_CR_25_PCT_OLD",
        "ADM_SAT_CR_75_PCT_OLD",
        "ADM_SAT_EBRW_25_PCT_NEW",
        "ADM_SAT_EBRW_75_PCT_NEW",
        "ADM_SAT_MATH_25_PCT_OLD",
        "ADM_SAT_MATH_25_PCT_NEW",
        "ADM_SAT_MATH_75_PCT_OLD",
        "ADM_SAT_MATH_75_PCT_NEW",
    }
    missing_expected = expected - {key for key in present if key}
    if missing_expected:
        logging.warning(
            "Filled Admissions crosswalk missing expected concept_keys: %s",
            ", ".join(sorted(missing_expected)),
        )
    return filled


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
    filled = fill_adm_crosswalk(template)
    args.filled_output.parent.mkdir(parents=True, exist_ok=True)
    filled.to_csv(args.filled_output, index=False)
    logging.info("Wrote filled Admissions crosswalk (%d rows) to %s", len(filled), args.filled_output)


if __name__ == "__main__":
    main()
