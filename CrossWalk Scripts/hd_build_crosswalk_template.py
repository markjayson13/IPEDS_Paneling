"""Build a crosswalk template for HD/IC variables from the IPEDS dictionary lake."""
from __future__ import annotations

import argparse
from pathlib import Path
from typing import Iterable

import pandas as pd


DATA_ROOT = Path("/Users/markjaysonfarol13/Higher Ed research/IPEDS")
DEFAULT_DICT_LAKE_PATH = DATA_ROOT / "Parquets" / "dictionary_lake.parquet"
DEFAULT_CROSSWALK_DIR = DATA_ROOT / "Paneled Datasets" / "Crosswalks"
DEFAULT_TEMPLATE_PATH = DEFAULT_CROSSWALK_DIR / "hd_crosswalk_template.csv"


def _normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Return a copy of *df* with lowercase column names for uniform access."""
    df = df.copy()
    df.columns = [c.lower() for c in df.columns]
    return df


def _filter_hd_ic(df: pd.DataFrame) -> pd.DataFrame:
    """Filter the dictionary lake to HD/IC related rows."""
    survey_col = df["survey"].astype(str).str.upper()
    mask = survey_col.isin({"HD", "IC"})

    if "survey_hint" in df.columns:
        hint = df["survey_hint"].astype(str).str.lower()
        mask |= hint.str.contains("institutional", na=False)
        mask |= hint.str.contains(r"\bhd\b", na=False, regex=True)

    return df[mask]


def build_crosswalk_template(dict_lake: Path) -> pd.DataFrame:
    """Create the HD/IC crosswalk template from the dictionary lake."""
    if not dict_lake.exists():
        raise FileNotFoundError(f"Dictionary lake not found: {dict_lake}")

    df = pd.read_parquet(dict_lake)
    df = _normalize_columns(df)

    required_cols: Iterable[str] = {"year", "survey", "varname"}
    missing = set(required_cols) - set(df.columns)
    if missing:
        raise ValueError(f"Dictionary lake is missing required columns: {sorted(missing)}")
    df["year"] = pd.to_numeric(df["year"], errors="raise")

    filtered = _filter_hd_ic(df)
    if filtered.empty:
        raise ValueError("No HD/IC variables found in dictionary lake. Check filters.")

    group_cols = ["survey", "varname"]
    agg_dict = {
        "year": ["min", "max"],
    }
    if "varlab" in filtered.columns:
        agg_dict["varlab"] = "first"

    grouped = filtered.groupby(group_cols, as_index=False).agg(agg_dict)
    rename_map = {"year_min": "year_start", "year_max": "year_end"}
    if "varlab_first" in grouped.columns:
        rename_map["varlab_first"] = "varlab"
    grouped = grouped.rename(columns=rename_map)

    grouped["concept_key"] = ""
    grouped["notes"] = ""

    column_order = [
        "concept_key",
        "survey",
        "varname",
        "year_start",
        "year_end",
    ]
    if "varlab" in grouped.columns:
        column_order.append("varlab")
    column_order.append("notes")

    grouped = grouped[column_order]
    grouped = grouped.sort_values(["survey", "varname", "year_start"], ignore_index=True)

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
        help="Output path for the HD/IC crosswalk template CSV.",
    )
    args = parser.parse_args()

    template = build_crosswalk_template(args.dict_lake)
    args.out.parent.mkdir(parents=True, exist_ok=True)
    template.to_csv(args.out, index=False)
    print(f"Wrote {len(template):,} rows to {args.out}")


if __name__ == "__main__":
    main()
