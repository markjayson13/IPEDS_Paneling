#!/usr/bin/env python3
"""Auto-populate core enrollment concepts in the crosswalk template."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

import pandas as pd

DEFAULT_INPUT = Path(
    "/Users/markjaysonfarol13/Higher Ed research/IPEDS/enrollment_crosswalk_template.csv"
)
DEFAULT_OUTPUT = Path(
    "/Users/markjaysonfarol13/Higher Ed research/IPEDS/enrollment_crosswalk_autofilled.csv"
)

E12_HEAD_ALL_TOT_ALL = "E12_HEAD_ALL_TOT_ALL"
EF_HEAD_ALL_TOT_ALL = "EF_HEAD_ALL_TOT_ALL"
EF_HEAD_FTFT_UG_DEGSEEK_TOT = "EF_HEAD_FTFT_UG_DEGSEEK_TOT"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", type=Path, default=DEFAULT_INPUT)
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    if not args.input.exists():
        print(f"Input crosswalk not found: {args.input}", file=sys.stderr)
        raise SystemExit(1)

    cw = pd.read_csv(args.input)
    cw.columns = [c.strip() for c in cw.columns]

    required_cols = ["concept_key", "source_var", "year_start", "survey"]
    missing = [col for col in required_cols if col not in cw.columns]
    if missing:
        raise RuntimeError(f"Crosswalk template missing required columns: {missing}")

    cw["concept_key"] = cw["concept_key"]
    cw["source_var"] = cw["source_var"].astype(str).str.strip()
    cw["survey"] = cw["survey"].astype(str).str.strip()
    if "label_norm" in cw.columns:
        cw["label_norm"] = cw["label_norm"].astype(str).str.strip()
    else:
        cw["label_norm"] = ""
    cw["year_start"] = pd.to_numeric(cw["year_start"], errors="coerce").astype("Int64")

    def is_blank(x: object) -> bool:
        return pd.isna(x) or str(x).strip() == ""

    blank_mask = cw["concept_key"].map(is_blank)

    if "note" not in cw.columns:
        cw["note"] = ""

    # Rule A: 12-month totals
    mask_e12_total = (
        (cw["survey"] == "12MONTHENROLLMENT")
        & cw["source_var"].isin(["FYRACE24", "EFYTOTLT"])
        & blank_mask
    )
    cw.loc[mask_e12_total, "concept_key"] = E12_HEAD_ALL_TOT_ALL
    cw.loc[mask_e12_total & cw["note"].map(is_blank), "note"] = f"auto:{E12_HEAD_ALL_TOT_ALL}"

    # Rule B: Fall totals
    mask_ef_total_old = (
        (cw["survey"] == "FALLENROLLMENT")
        & (cw["source_var"] == "EFRACE24")
        & cw["year_start"].between(2004, 2007, inclusive="both")
        & blank_mask
    )
    mask_ef_total_new = (
        (cw["survey"] == "FALLENROLLMENT")
        & (cw["source_var"] == "EFTOTLT")
        & (cw["year_start"] >= 2008)
        & blank_mask
    )
    mask_ef_total = mask_ef_total_old | mask_ef_total_new
    cw.loc[mask_ef_total, "concept_key"] = EF_HEAD_ALL_TOT_ALL
    cw.loc[mask_ef_total & cw["note"].map(is_blank), "note"] = f"auto:{EF_HEAD_ALL_TOT_ALL}"

    # Rule C: Fall FTFT deg/cert seekers
    mask_ef_ftft_degseek = (
        (cw["survey"] == "FALLENROLLMENT")
        & (cw["source_var"] == "EFRES01")
        & blank_mask
    )
    cw.loc[mask_ef_ftft_degseek, "concept_key"] = EF_HEAD_FTFT_UG_DEGSEEK_TOT
    cw.loc[mask_ef_ftft_degseek & cw["note"].map(is_blank), "note"] = f"auto:{EF_HEAD_FTFT_UG_DEGSEEK_TOT}"

    filled_total = cw["concept_key"].map(lambda x: not is_blank(x)).sum()
    print(f"{E12_HEAD_ALL_TOT_ALL} rows: {mask_e12_total.sum()}")
    print(f"{EF_HEAD_ALL_TOT_ALL} rows: {mask_ef_total.sum()}")
    print(f"{EF_HEAD_FTFT_UG_DEGSEEK_TOT} rows: {mask_ef_ftft_degseek.sum()}")
    print(f"Total rows with concept_key set: {filled_total}")

    args.output.parent.mkdir(parents=True, exist_ok=True)
    cw.to_csv(args.output, index=False)
    print(f"Wrote autofilled crosswalk to {args.output}")


if __name__ == "__main__":
    main()
