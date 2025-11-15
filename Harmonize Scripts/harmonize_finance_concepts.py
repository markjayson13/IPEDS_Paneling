#!/usr/bin/env python3
"""Step 2 – Apply finance crosswalk to produce concept-level panels."""

from __future__ import annotations

import argparse
from pathlib import Path

import pandas as pd

DEFAULT_STEP0 = Path(
    "/Users/markjaysonfarol13/Higher Ed research/IPEDS/Parquets/Long/finance_step0_long.parquet"
)
DEFAULT_CROSSWALK = Path(
    "/Users/markjaysonfarol13/Higher Ed research/IPEDS/Paneled Datasets/Crosswalks/finance_crosswalk_template.csv"
)
DEFAULT_LONG_OUT = Path(
    "/Users/markjaysonfarol13/Higher Ed research/IPEDS/Parquets/Long/finance_concepts_long.parquet"
)
DEFAULT_WIDE_OUT = Path(
    "/Users/markjaysonfarol13/Higher Ed research/IPEDS/Parquets/Long/finance_concepts_wide.parquet"
)
DEFAULT_COVERAGE = Path(
    "/Users/markjaysonfarol13/Higher Ed research/IPEDS/Paneled Datasets/Crosswalks/finance_concepts_coverage.csv"
)

ID_COLS = ["YEAR", "UNITID"]
OPTIONAL_ID_COLS = ["REPORTING_UNITID"]

ALLOWED_CONCEPTS = {
    "BS_ASSETS_TOTAL",
    "BS_LIABILITIES_TOTAL",
    "BS_NET_ASSETS_TOTAL",
    "BS_NET_ASSETS_WODR",
    "BS_NET_ASSETS_WDR",
    "BS_ASSETS_CASH",
    "BS_ASSETS_INVESTMENTS_TOTAL",
    "BS_LIAB_DEBT_LONGTERM",
    "BS_ASSETS_CAPITAL_NET",
    "BS_ENDOWMENT_FMV",
    "IS_REVENUES_TOTAL",
    "IS_EXPENSES_TOTAL",
    "IS_NET_INCOME",
    "REV_TUITION_NET",
    "REV_GOV_APPROPS_TOTAL",
    "REV_GRANTS_CONTRACTS_TOTAL",
    "REV_PRIVATE_GIFTS",
    "REV_INVESTMENT_RETURN",
    "REV_AUXILIARY_NET",
    "EXP_INSTRUCTION",
    "EXP_RESEARCH",
    "EXP_PUBLIC_SERVICE",
    "EXP_ACADEMIC_SUPPORT",
    "EXP_STUDENT_SERVICES",
    "EXP_INSTITUTIONAL_SUPPORT",
    "EXP_OPERATIONS_PLANT",
    "EXP_SCHOLARSHIPS_NET",
    "DISCOUNT_TUITION",
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--step0", type=Path, default=DEFAULT_STEP0)
    parser.add_argument("--crosswalk", type=Path, default=DEFAULT_CROSSWALK)
    parser.add_argument("--output-long", type=Path, default=DEFAULT_LONG_OUT)
    parser.add_argument("--output-wide", type=Path, default=DEFAULT_WIDE_OUT)
    parser.add_argument("--coverage", type=Path, default=DEFAULT_COVERAGE)
    return parser.parse_args()


def load_crosswalk(path: Path) -> pd.DataFrame:
    cw = pd.read_csv(path)
    cw.columns = [c.strip() for c in cw.columns]
    cw = cw.dropna(subset=["concept_key", "form_family", "base_key"])
    cw = cw[cw["concept_key"].astype(str).str.strip().ne("")]
    cw["form_family"] = cw["form_family"].str.upper().str.strip()
    cw["base_key"] = cw["base_key"].str.upper().str.strip()
    cw["year_start"] = pd.to_numeric(cw.get("year_start"), errors="coerce").fillna(-10_000).astype(int)
    cw["year_end"] = pd.to_numeric(cw.get("year_end"), errors="coerce").fillna(10_000).astype(int)
    cw["weight"] = pd.to_numeric(cw.get("weight", 1.0), errors="coerce").fillna(0.0)
    unique_concepts = set(cw["concept_key"].unique())
    invalid = unique_concepts - ALLOWED_CONCEPTS
    if invalid:
        raise ValueError(f"Crosswalk contains unknown concept_keys: {sorted(invalid)}")

    overlap = []
    for (fam, key, concept), group in cw.groupby(["form_family", "base_key", "concept_key"], dropna=False):
        sorted_group = group.sort_values("year_start")
        prev_end = None
        for _, row in sorted_group.iterrows():
            if prev_end is not None and row["year_start"] <= prev_end:
                overlap.append((fam, key, concept, int(row["year_start"]), int(prev_end)))
            prev_end = max(prev_end or row["year_end"], row["year_end"])
    if overlap:
        raise ValueError(f"Overlapping year ranges detected in crosswalk: {overlap[:5]}")
    return cw


def apply_crosswalk(step0: pd.DataFrame, crosswalk: pd.DataFrame) -> pd.DataFrame:
    merged = step0.merge(crosswalk, on=["form_family", "base_key"], how="left", suffixes=("", "_cw"))
    merged = merged[merged["concept_key"].notna()]
    merged = merged[
        (merged["YEAR"] >= merged["year_start"]) & (merged["YEAR"] <= merged["year_end"])
    ]
    merged["concept_value"] = merged["value"] * merged["weight"]
    # Ensure imputed_flag exists so downstream aggregations can carry it
    if "imputed_flag" not in merged.columns:
        merged["imputed_flag"] = 0
    merged["imputed_flag"] = merged["imputed_flag"].fillna(0).astype(int)
    return merged


def build_long(merged: pd.DataFrame) -> pd.DataFrame:
    group_cols = ["YEAR", "UNITID", "concept_key"]
    if "REPORTING_UNITID" in merged.columns:
        group_cols.insert(2, "REPORTING_UNITID")
    long = (
        merged.groupby(group_cols, dropna=False)
        .agg(
            value=("concept_value", "sum"),
            imputed_flag=("imputed_flag", "max"),
        )
        .reset_index()
    )
    return long


def build_wide(long: pd.DataFrame) -> pd.DataFrame:
    id_cols = [col for col in ID_COLS if col in long.columns]
    id_cols += [col for col in OPTIONAL_ID_COLS if col in long.columns and col not in id_cols]
    wide_values = (
        long.pivot_table(index=id_cols, columns="concept_key", values="value", aggfunc="first")
        .reset_index()
    )
    if "imputed_flag" in long.columns:
        wide_flags = (
            long.pivot_table(index=id_cols, columns="concept_key", values="imputed_flag", aggfunc="max")
            .add_suffix("_FLAG")
            .reset_index()
        )
        wide = wide_values.merge(wide_flags, on=id_cols, how="left")
    else:
        wide = wide_values
    return wide


def build_coverage(merged: pd.DataFrame) -> pd.DataFrame:
    coverage = (
        merged.groupby(["YEAR", "concept_key"])
        .agg(
            n_inst=("UNITID", "nunique"),
            forms_used=("form_family", lambda s: ",".join(sorted(set(s.dropna())))),
            raw_codes=("source_var", lambda s: ",".join(sorted(set(s.dropna())))),
        )
        .reset_index()
    )
    return coverage


def main() -> None:
    args = parse_args()
    step0 = pd.read_parquet(args.step0)
    if "YEAR" in step0.columns:
        step0["YEAR"] = pd.to_numeric(step0["YEAR"], errors="coerce").astype("Int64")
    crosswalk = load_crosswalk(args.crosswalk)
    merged = apply_crosswalk(step0, crosswalk)

    long = build_long(merged)
    args.output_long.parent.mkdir(parents=True, exist_ok=True)
    long.to_parquet(args.output_long, index=False)

    wide = build_wide(long)
    args.output_wide.parent.mkdir(parents=True, exist_ok=True)
    wide.to_parquet(args.output_wide, index=False)

    coverage = build_coverage(merged)
    args.coverage.parent.mkdir(parents=True, exist_ok=True)
    coverage.to_csv(args.coverage, index=False)

    print(f"Wrote concept long panel to {args.output_long}")
    print(f"Wrote concept wide panel to {args.output_wide}")
    print(f"Coverage summary saved to {args.coverage}")


if __name__ == "__main__":
    main()
