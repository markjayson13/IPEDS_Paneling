"""Debug IC_AY crosswalk and panel mappings."""
from __future__ import annotations

import argparse
from pathlib import Path

import pandas as pd

DATA_ROOT = Path("/Users/markjaysonfarol13/Higher Ed research/IPEDS")
CW_PATH = DATA_ROOT / "Paneled Datasets" / "Crosswalks" / "Filled" / "ic_ay_crosswalk_all.csv"
STEP0_LONG = DATA_ROOT / "Parquets" / "Unify" / "Step0ICAYlong" / "icay_step0_long.parquet"
CONCEPT_LONG = DATA_ROOT / "Parquets" / "Unify" / "ICAYlong" / "icay_concepts_long.parquet"
CONCEPT_WIDE = DATA_ROOT / "Parquets" / "Unify" / "ICAYwide" / "icay_concepts_wide.parquet"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Debug IC_AY crosswalk and panel mappings.")
    parser.add_argument("--crosswalk", type=Path, default=CW_PATH)
    parser.add_argument("--step0-long", type=Path, default=STEP0_LONG)
    parser.add_argument("--concept-long", type=Path, default=CONCEPT_LONG)
    parser.add_argument("--concept-wide", type=Path, default=CONCEPT_WIDE)
    return parser.parse_args()


def main() -> None:
    args = parse_args()

    cw = pd.read_csv(args.crosswalk, dtype=str).fillna("")
    cw.columns = [c.strip().lower() for c in cw.columns]
    cw["source_var"] = cw["source_var"].astype(str).str.upper()
    cw["concept_key"] = cw["concept_key"].astype(str).str.strip()
    print("Crosswalk concept_key counts (top 40):")
    print(cw["concept_key"].value_counts(dropna=False).head(40))

    mask_chg = cw["source_var"].str.startswith(("CHG", "TUITION", "FEE"))
    print("\nCHG/TUITION/FEE rows in crosswalk:")
    print(cw.loc[mask_chg, ["survey", "source_var", "concept_key"]].head(40))

    step0 = pd.read_parquet(args.step0_long)
    step0.columns = [c.lower() for c in step0.columns]
    step0["varname"] = step0["varname"].astype(str).str.upper()
    step0_counts = step0["varname"].value_counts()
    print("\nStep0 varname value_counts (top 40):")
    print(step0_counts.head(40))

    cw_vars = set(cw["source_var"].unique())
    intersect = sorted(cw_vars & set(step0_counts.index))
    print(f"\nNumber of crosswalk source_var present in Step0: {len(intersect)}")
    print("Sample of intersecting vars:", intersect[:40])

    concept_long = pd.read_parquet(args.concept_long)
    concept_long.columns = [c.lower() for c in concept_long.columns]
    print("\nConcept-long concept_key value_counts (top 40):")
    print(concept_long["concept_key"].value_counts().head(40))

    wide = pd.read_parquet(args.concept_wide)
    print("\nIC_AY wide columns:")
    print(list(wide.columns))
    price_like = [c for c in wide.columns if c.startswith("PRICE_")]
    print("\nColumns starting with 'PRICE_':", price_like)


if __name__ == "__main__":
    main()
