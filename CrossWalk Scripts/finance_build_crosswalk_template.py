#!/usr/bin/env python3
"""Generate a finance crosswalk template from the dictionary lake."""

from __future__ import annotations

import argparse
from pathlib import Path

import pandas as pd

DEFAULT_DICT_LAKE = Path(
    "/Users/markjaysonfarol13/Higher Ed research/IPEDS/Parquets/Dictionary/dictionary_lake.parquet"
)
DEFAULT_OUTPUT = Path(
    "/Users/markjaysonfarol13/Higher Ed research/IPEDS/Paneled Datasets/Crosswalks/finance_crosswalk_template.csv"
)


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

    # Only keep rows tied to the five finance components / sections we harmonize downstream.
    def _is_relevant_component(fam: str | None, section: str | None) -> bool:
        """Return True when the row belongs to one of the five core finance components."""
        fam_norm = (fam or "").upper()
        section_norm = (section or "").upper()
        if not fam_norm or not section_norm:
            return False
        if fam_norm.startswith("F1"):
            return section_norm in {"B", "C", "D", "E", "H"}
        if fam_norm.startswith("F2"):
            return section_norm in {"B", "C", "D", "E", "H"}
        if fam_norm.startswith("F3"):
            return section_norm in {"B", "C", "D", "E"}
        return False

    subset = subset[
        subset.apply(lambda r: _is_relevant_component(r.get("form_family"), r.get("section")), axis=1)
    ].copy()

    def first_label(series: pd.Series) -> str:
        for val in series:
            if isinstance(val, str) and val.strip():
                return val
        return str(series.iloc[0]) if not series.empty else ""

    grouped = (
        subset.groupby([
            "form_family",
            "base_key",
            "section",
            "line_code",
            "source_label_norm",
            "survey",
        ])
        .agg(
            year_start=("year", "min"),
            year_end=("year", "max"),
            source_var=("source_var", lambda s: ";".join(sorted(set(s.astype(str))))),
            source_label=("source_label", first_label),
        )
        .reset_index()
    )

    grouped["concept_key"] = ""
    grouped["weight"] = 1.0
    grouped["notes"] = ""

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
        grouped[cols]
        .sort_values(["form_family", "base_key", "year_start", "source_label_norm"])
        .reset_index(drop=True)
    )
    args.output.parent.mkdir(parents=True, exist_ok=True)
    template.to_csv(args.output, index=False)
    print(f"Wrote {len(template):,} rows to {args.output}")


if __name__ == "__main__":
    main()
