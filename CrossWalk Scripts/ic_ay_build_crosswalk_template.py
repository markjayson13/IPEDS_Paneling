"""Build a crosswalk template for IC_AY student charge variables."""
from __future__ import annotations

import argparse
from pathlib import Path
from typing import Iterable, Optional

import pandas as pd

DATA_ROOT = Path("/Users/markjaysonfarol13/Higher Ed research/IPEDS")
DEFAULT_DICT_LAKE_PATH = DATA_ROOT / "Parquets" / "Dictionary" / "dictionary_lake.parquet"
DEFAULT_CROSSWALK_DIR = DATA_ROOT / "Paneled Datasets" / "Crosswalks"
DEFAULT_TEMPLATE_PATH = DEFAULT_CROSSWALK_DIR / "ic_ay_crosswalk_template.csv"

CHARGE_KEYWORDS = (
    "tuition",
    "required fee",
    "fees",
    "books",
    "supplies",
    "room and board",
    "food and housing",
    "other expenses",
    "price of attendance",
)

LABEL_CANDIDATES = ["label", "var_label", "varlab", "varname_label"]
SURVEY_HINT_COLS = ["survey_group", "survey_hint", "survey_component", "component"]
FILENAME_HINT_COLS = ["dict_filename", "data_filename", "filename", "dict_file"]


def _resolve_column(df: pd.DataFrame, candidates: Iterable[str], *, required: bool = True) -> Optional[str]:
    for candidate in candidates:
        if candidate in df.columns:
            return candidate
    if required:
        raise KeyError(f"Required column not found among: {', '.join(candidates)}")
    return None


def _first_non_empty(series: pd.Series) -> str:
    for val in series:
        if pd.notna(val):
            text = str(val).strip()
            if text:
                return val
    return ""


def _ic_ay_mask(df: pd.DataFrame, survey_col: str) -> pd.Series:
    survey = df[survey_col].astype(str).str.upper()
    survey_ic = survey.str.contains("IC", na=False)

    file_mask = pd.Series(False, index=df.index)
    for col in FILENAME_HINT_COLS:
        if col in df.columns:
            values = df[col].astype(str).str.lower()
            file_mask |= values.str.contains("ic", na=False) & values.str.contains("ay", na=False)

    table_mask = pd.Series(False, index=df.index)
    if "table_name" in df.columns:
        table_values = df["table_name"].astype(str).str.lower()
        table_mask |= table_values.str.contains("icay", na=False)
        table_mask |= table_values.str.contains("student charges", na=False)

    hint_mask = pd.Series(False, index=df.index)
    for col in SURVEY_HINT_COLS:
        if col in df.columns:
            hints = df[col].astype(str).str.lower()
            hint_mask |= hints.str.contains("ic_ay", na=False)
            hint_mask |= hints.str.contains("student charges", na=False)
            hint_mask |= hints.str.contains("academic year", na=False)

    var_mask = df["source_var"].astype(str).str.upper().str.match(r"^CHG\d+$", na=False)

    combined = (survey_ic & (file_mask | table_mask | hint_mask)) | var_mask
    return combined


def build_crosswalk_template(dict_lake: Path) -> pd.DataFrame:
    """Create the IC_AY crosswalk template from the dictionary lake."""
    if not dict_lake.exists():
        raise FileNotFoundError(f"Dictionary lake not found: {dict_lake}")

    df = pd.read_parquet(dict_lake)
    df.columns = [c.lower() for c in df.columns]

    survey_col = _resolve_column(df, ["survey"])
    year_col = _resolve_column(df, ["year", "collection_year"])
    source_var_col = _resolve_column(df, ["source_var", "varname", "variable"])
    table_col = _resolve_column(df, ["table", "table_name", "tableid"], required=False)
    label_col = _resolve_column(df, LABEL_CANDIDATES, required=False)

    df["year"] = pd.to_numeric(df[year_col], errors="coerce")
    if df["year"].isna().any():
        raise ValueError("Dictionary lake contains non-numeric years for IC_AY rows.")

    df["survey"] = df[survey_col].astype(str).str.upper()
    df["source_var"] = df[source_var_col].astype(str).str.upper()
    if table_col:
        df["table"] = df[table_col]
    else:
        df["table"] = ""
    if label_col:
        df["label"] = df[label_col].astype(str)
    else:
        df["label"] = ""

    ic_ay_mask = _ic_ay_mask(df, "survey")
    label_lower = df["label"].str.lower()
    charge_mask = pd.Series(False, index=df.index)
    if label_lower.notna().any():
        for keyword in CHARGE_KEYWORDS:
            charge_mask |= label_lower.str.contains(keyword, na=False)
    # Always keep canonical CHG* variables even if the label is missing.
    charge_mask |= df["source_var"].str.match(r"^CHG\d+$", na=False)

    df_ic_ay = df[ic_ay_mask & charge_mask].copy()
    if df_ic_ay.empty:
        raise ValueError("IC_AY filter produced zero rows. Check dictionary lake contents.")

    grouped = (
        df_ic_ay.groupby(["survey", "source_var"], as_index=False)
        .agg(
            year_start=("year", "min"),
            year_end=("year", "max"),
            table=("table", _first_non_empty),
            label=("label", _first_non_empty),
        )
        .sort_values(["source_var", "survey", "year_start"], ignore_index=True)
    )

    grouped.insert(0, "concept_key", "")
    grouped["notes"] = ""
    column_order = ["concept_key", "survey", "source_var", "year_start", "year_end", "table", "label", "notes"]
    grouped = grouped[column_order]
    return grouped


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--dict-lake",
        type=Path,
        default=DEFAULT_DICT_LAKE_PATH,
        help="Path to dictionary_lake.parquet.",
    )
    parser.add_argument(
        "--out",
        type=Path,
        default=DEFAULT_TEMPLATE_PATH,
        help="Output CSV path for the IC_AY crosswalk template.",
    )
    args = parser.parse_args()

    template = build_crosswalk_template(args.dict_lake)
    args.out.parent.mkdir(parents=True, exist_ok=True)
    template.to_csv(args.out, index=False)
    unique_vars = template["source_var"].nunique()
    print(f"Wrote {len(template):,} rows covering {unique_vars:,} IC_AY source variables to {args.out}")
    print("Sample rows:")
    print(template.head(10).to_string(index=False))


if __name__ == "__main__":
    main()
