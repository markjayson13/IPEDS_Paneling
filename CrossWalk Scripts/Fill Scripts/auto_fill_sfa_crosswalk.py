#!/usr/bin/env python3
"""
Auto-fill concept_key for SFA crosswalk template rows using label-based heuristics.

- Targets IPEDS Net Price (NPT) income bins in the SFA dictionary.
- Maps:
    income 0–30,000         -> NET_PRICE_AVG_INC_0_30K
    income 30,001–48,000    -> NET_PRICE_AVG_INC_30_48K
    income 48,001–75,000    -> NET_PRICE_AVG_INC_48_75K
    income 75,001–110,000   -> NET_PRICE_AVG_INC_75_110K
    income over 110,000     -> NET_PRICE_AVG_INC_110K_PLUS

- Leaves rows like SFAFORM (collection form type) with blank concept_key,
  because they are categorical / not part of your numeric SFA concept panel.

Default input:
  /Users/markjaysonfarol13/Higher Ed research/IPEDS/Paneled Datasets/Crosswalks/sfa_crosswalk_template.csv

Default output:
  /Users/markjaysonfarol13/Higher Ed research/IPEDS/Paneled Datasets/Crosswalks/Filled/sfa_crosswalk_filled.csv
"""

from __future__ import annotations

import argparse
import logging
from pathlib import Path
from typing import Optional

import pandas as pd


# Base paths
CROSSWALK_DIR = Path(
    "/Users/markjaysonfarol13/Higher Ed research/IPEDS/Paneled Datasets/Crosswalks"
)
FILLED_DIR = CROSSWALK_DIR / "Filled"


def infer_concept_key(label: Optional[str], source_var: Optional[str]) -> Optional[str]:
    """
    Infer a concept_key from the label and source_var.

    Right now this is focused on Net Price income bins, using label text,
    so it will work regardless of whether the NPT varnames are NPT410/411/412
    or some future naming (as long as labels keep the income brackets).
    """
    label_u = (label or "").upper()
    var_u = (source_var or "").upper()

    # Net price income bins
    if "AVERAGE NET PRICE" in label_u:
        if "INCOME 0-30,000" in label_u:
            return "NET_PRICE_AVG_INC_0_30K"
        if "INCOME 30,001-48,000" in label_u:
            return "NET_PRICE_AVG_INC_30_48K"
        if "INCOME 48,001-75,000" in label_u:
            return "NET_PRICE_AVG_INC_48_75K"
        if "INCOME 75,001-110,000" in label_u:
            return "NET_PRICE_AVG_INC_75_110K"
        # Some documentation uses "over 110,000", some might say "110,001+"
        if "INCOME OVER 110,000" in label_u or "INCOME 110,001" in label_u:
            return "NET_PRICE_AVG_INC_110K_PLUS"

    # Example: you could add more patterns later, e.g. Pell, loans, FTFT, GI Bill.
    # For now, anything that is not a clearly identified net price bin will fall through.
    return None


def auto_fill_concepts(
    input_csv: Path,
    output_csv: Path,
) -> None:
    logging.info("Loading SFA crosswalk template from %s", input_csv)
    df = pd.read_csv(input_csv)

    # Normalize concept_key and source_var to strings for safety
    if "concept_key" not in df.columns:
        raise KeyError("Expected column 'concept_key' in crosswalk template.")
    if "source_var" not in df.columns:
        raise KeyError("Expected column 'source_var' in crosswalk template.")
    if "label" not in df.columns:
        raise KeyError("Expected column 'label' in crosswalk template.")

    df["concept_key"] = df["concept_key"].astype("object")

    df["source_var"] = df["source_var"].astype(str).str.strip().str.upper()

    # Identify rows that are already filled (do not override)
    raw_ck = df["concept_key"]
    ck_str = raw_ck.astype(str)
    trimmed = ck_str.str.strip()
    empty_mask = raw_ck.isna() | trimmed.eq("") | trimmed.str.lower().eq("nan")
    already_filled = ~empty_mask

    logging.info("Template has %d rows total.", len(df))
    logging.info("Rows with pre-filled concept_key: %d", already_filled.sum())

    # Apply inference only to rows without a concept_key
    to_fill_mask = ~already_filled
    to_fill = df.loc[to_fill_mask].copy()

    logging.info("Attempting to auto-fill concept_key for %d rows.", len(to_fill))

    filled_count = 0
    for idx, row in to_fill.iterrows():
        concept = infer_concept_key(row.get("label"), row.get("source_var"))
        if concept:
            df.at[idx, "concept_key"] = concept
            filled_count += 1

    logging.info("Auto-filled concept_key for %d rows.", filled_count)

    # Basic summary by concept_key
    filled_summary = (
        df["concept_key"]
        .astype(str)
        .str.strip()
        .value_counts(dropna=True)
        .sort_index()
    )
    logging.info("Resulting concept_key distribution:\n%s", filled_summary)

    # Write output
    output_csv.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(output_csv, index=False)
    logging.info("Saved filled SFA crosswalk to %s", output_csv)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Auto-fill concept_key values in the SFA crosswalk template."
    )
    parser.add_argument(
        "--input",
        type=Path,
        default=CROSSWALK_DIR / "sfa_crosswalk_template.csv",
        help="Path to the SFA crosswalk template CSV.",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=FILLED_DIR / "sfa_crosswalk_filled.csv",
        help="Destination for the filled SFA crosswalk CSV.",
    )
    parser.add_argument(
        "--log-level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        help="Logging level.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    logging.basicConfig(
        level=getattr(logging, args.log_level),
        format="%(levelname)s:%(name)s:%(message)s",
    )
    auto_fill_concepts(args.input, args.output)


if __name__ == "__main__":
    main()
