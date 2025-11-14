#!/usr/bin/env python3
"""Generate a finance crosswalk template from the dictionary lake."""

from __future__ import annotations

import argparse
from pathlib import Path

import pandas as pd

DEFAULT_DICT_LAKE = Path(
    "/Users/markjaysonfarol13/Higher Ed research/IPEDS/Parquets/dictionary_lake.parquet"
)
DEFAULT_OUTPUT = Path("finance_crosswalk_template.csv")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dict-lake", type=Path, default=DEFAULT_DICT_LAKE)
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    parser.add_argument("--year-min", type=int, default=2004)
    parser.add_argument("--year-max", type=int, default=2024)
    parser.add_argument("--log-level", default="INFO")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    df = pd.read_parquet(args.dict_lake)
    mask = (
        df.get("is_finance", False)
        & df.get("form_family").notna()
        & df.get("base_key").notna()
        & df.get("year").between(args.year_min, args.year_max)
    )
    subset = df.loc[mask, [
        "year",
        "survey",
        "form_family",
        "section",
        "line_code",
        "base_key",
        "source_var",
        "source_label",
        "source_label_norm",
    ]].copy()

    subset["concept_key"] = ""
    subset["year_start"] = subset["year"]
    subset["year_end"] = subset["year"]
    subset["weight"] = 1.0
    subset["notes"] = ""

    cols = [
        "concept_key",
        "form_family",
        "survey",
        "year_start",
        "year_end",
        "base_key",
        "section",
        "line_code",
        "source_var",
        "source_label",
        "source_label_norm",
        "weight",
        "notes",
    ]
    template = (
        subset[cols]
        .drop_duplicates(
            subset=["form_family", "base_key", "source_var", "year_start"], keep="first"
        )
        .sort_values(["form_family", "base_key", "year_start", "source_var"])
        .reset_index(drop=True)
    )
    args.output.parent.mkdir(parents=True, exist_ok=True)
    template.to_csv(args.output, index=False)
    print(f"Wrote {len(template):,} rows to {args.output}")


if __name__ == "__main__":
    main()
