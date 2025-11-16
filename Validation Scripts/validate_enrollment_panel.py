#!/usr/bin/env python3
"""Validation checks for the enrollment concept-wide panel."""

from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path
from typing import List

import pandas as pd

# ==============================
# CONFIG: concept column names
# ==============================
GENDER_TOTAL = "EF_HEAD_ALL_UG_TOT_ALL"
GENDER_MEN = "EF_HEAD_ALL_UG_MEN_ALL"
GENDER_WOMEN = "EF_HEAD_ALL_UG_WOMEN_ALL"

RACE_TOTAL = "EF_HEAD_ALL_UG_TOT_ALL"
RACE_PARTS = [
    "EF_HEAD_ALL_UG_TOT_WHITE",
    "EF_HEAD_ALL_UG_TOT_BLACK",
    "EF_HEAD_ALL_UG_TOT_HISP",
    "EF_HEAD_ALL_UG_TOT_ASIANPI",
    "EF_HEAD_ALL_UG_TOT_AIAN",
    # "EF_HEAD_ALL_UG_TOT_MULTI",
    "EF_HEAD_ALL_UG_TOT_NR",
    "EF_HEAD_ALL_UG_TOT_UNK",
]

EF_TOTAL = "EF_HEAD_ALL_UG_TOT_ALL"
E12_TOTAL = "E12_HEAD_ALL_UG_TOT_ALL"

TOLERANCE = 1.0  # headcount tolerance
DEFAULT_INPUT = Path(
    "/Users/markjaysonfarol13/Higher Ed research/IPEDS/Parquets/Unify/Enrollwide/enrollment_concepts_wide.parquet"
)
DEFAULT_OUTPUT_DIR = Path(
    "/Users/markjaysonfarol13/Higher Ed research/IPEDS/Parquets/Checks/Enrollment"
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--input",
        type=Path,
        default=DEFAULT_INPUT,
        help=f"Path to enrollment_concepts_wide.parquet. Default: {DEFAULT_INPUT}",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=DEFAULT_OUTPUT_DIR,
        help=f"Directory to store validation CSV outputs. Default: {DEFAULT_OUTPUT_DIR}",
    )
    parser.add_argument("--log-level", default="INFO")
    return parser.parse_args()


def load_panel(path: Path) -> pd.DataFrame:
    if not path.exists():
        raise FileNotFoundError(f"Input panel not found: {path}")
    return pd.read_parquet(path)


def ensure_output_dir(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)


def check_gender(df: pd.DataFrame, output: Path) -> None:
    required = [GENDER_TOTAL, GENDER_MEN, GENDER_WOMEN]
    if any(col not in df.columns for col in required):
        logging.info("Skipping gender check; missing columns.")
        return

    subset = df.dropna(subset=required)[["YEAR", "UNITID"] + required].copy()
    subset["gender_diff"] = subset[GENDER_TOTAL] - (subset[GENDER_MEN] + subset[GENDER_WOMEN])
    violations = subset[subset["gender_diff"].abs() > TOLERANCE]
    if violations.empty:
        logging.info("Gender totals check passed (within tolerance).")
        return
    violations.to_csv(output / "enrollment_validation_gender_sums.csv", index=False)
    logging.warning("Gender totals mismatch rows written to %s", output / "enrollment_validation_gender_sums.csv")


def check_race(df: pd.DataFrame, output: Path) -> None:
    required = [RACE_TOTAL] + RACE_PARTS
    if any(col not in df.columns for col in required):
        logging.info("Skipping race check; missing columns.")
        return

    subset = df.dropna(subset=required)[["YEAR", "UNITID"] + required].copy()
    subset["race_sum"] = subset[RACE_PARTS].sum(axis=1)
    subset["race_diff"] = subset[RACE_TOTAL] - subset["race_sum"]
    violations = subset[subset["race_diff"].abs() > TOLERANCE]
    if violations.empty:
        logging.info("Race totals check passed (within tolerance).")
        return
    violations.to_csv(output / "enrollment_validation_race_sums.csv", index=False)
    logging.warning("Race totals mismatch rows written to %s", output / "enrollment_validation_race_sums.csv")


def check_e12_gte_ef(df: pd.DataFrame, output: Path) -> None:
    if EF_TOTAL not in df.columns or E12_TOTAL not in df.columns:
        logging.info("Skipping E12 >= EF check; missing columns.")
        return

    subset = df.dropna(subset=[EF_TOTAL, E12_TOTAL])[["YEAR", "UNITID", EF_TOTAL, E12_TOTAL]].copy()
    subset["e12_minus_ef"] = subset[E12_TOTAL] - subset[EF_TOTAL]
    violations = subset[subset["e12_minus_ef"] < -TOLERANCE]
    if violations.empty:
        logging.info("E12 >= EF check passed (within tolerance).")
        return
    violations.to_csv(output / "enrollment_validation_e12_ge_ef.csv", index=False)
    logging.warning("E12 < EF rows written to %s", output / "enrollment_validation_e12_ge_ef.csv")


def totals_by_year(df: pd.DataFrame, output: Path) -> None:
    totals_cols: List[str] = []
    for col in [EF_TOTAL, E12_TOTAL]:
        if col in df.columns:
            totals_cols.append(col)

    if not totals_cols:
        logging.info("Skipping totals-by-year report; no total columns found.")
        return

    totals = df.groupby("YEAR")[totals_cols].sum(min_count=1).reset_index()
    totals.to_csv(output / "enrollment_validation_totals_by_year.csv", index=False)
    logging.info("Totals by year saved to %s", output / "enrollment_validation_totals_by_year.csv")
    logging.info("Sample totals:\n%s", totals.head())


def check_gender_coverage(df: pd.DataFrame, output: Path) -> None:
    required = [GENDER_TOTAL, GENDER_MEN, GENDER_WOMEN]
    missing = [col for col in required if col not in df.columns]
    if missing:
        logging.info("Gender coverage check skipped: missing columns %s", ", ".join(missing))
        return

    subset = df[["YEAR", "UNITID"] + required].copy()
    mask = subset[GENDER_TOTAL].notna() & subset[[GENDER_MEN, GENDER_WOMEN]].isna().any(axis=1)
    issues = subset.loc[mask]
    if issues.empty:
        logging.info("No gender coverage gaps detected.")
        return
    out_path = output / "enrollment_validation_gender_coverage_gaps.csv"
    issues.to_csv(out_path, index=False)
    logging.warning("Gender coverage gaps written to %s", out_path)


def check_race_coverage(df: pd.DataFrame, output: Path) -> None:
    if RACE_TOTAL not in df.columns:
        logging.info("Race coverage check skipped: %s not found", RACE_TOTAL)
        return
    missing_parts = [col for col in RACE_PARTS if col not in df.columns]
    if missing_parts:
        logging.info(
            "Race coverage check skipped: missing detail columns %s",
            ", ".join(missing_parts),
        )
        return
    subset = df[["YEAR", "UNITID", RACE_TOTAL] + RACE_PARTS].copy()
    mask = subset[RACE_TOTAL].notna() & subset[RACE_PARTS].isna().any(axis=1)
    issues = subset.loc[mask]
    if issues.empty:
        logging.info("No race coverage gaps detected.")
        return
    out_path = output / "enrollment_validation_race_coverage_gaps.csv"
    issues.to_csv(out_path, index=False)
    logging.warning("Race coverage gaps written to %s", out_path)


def main() -> None:
    args = parse_args()
    logging.basicConfig(level=getattr(logging, args.log_level.upper(), logging.INFO), format="%(levelname)s %(message)s")

    df = load_panel(args.input)
    ensure_output_dir(args.output_dir)

    if "YEAR" not in df.columns or "UNITID" not in df.columns:
        raise RuntimeError("Panel must include YEAR and UNITID columns.")

    check_gender(df, args.output_dir)
    check_gender_coverage(df, args.output_dir)
    check_race(df, args.output_dir)
    check_race_coverage(df, args.output_dir)
    check_e12_gte_ef(df, args.output_dir)
    totals_by_year(df, args.output_dir)


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:  # noqa: BLE001
        logging.error("validate_enrollment_panel failed: %s", exc)
        sys.exit(1)
def check_gender_coverage(df: pd.DataFrame, output: Path) -> None:
    required = [GENDER_TOTAL, GENDER_MEN, GENDER_WOMEN]
    missing = [col for col in required if col not in df.columns]
    if missing:
        logging.info("Gender coverage check skipped: missing columns %s", ", ".join(missing))
        return

    subset = df[["YEAR", "UNITID"] + required].copy()
    mask = subset[GENDER_TOTAL].notna() & subset[[GENDER_MEN, GENDER_WOMEN]].isna().any(axis=1)
    issues = subset.loc[mask]
    if issues.empty:
        logging.info("No gender coverage gaps detected.")
        return
    out_path = output / "enrollment_validation_gender_coverage_gaps.csv"
    issues.to_csv(out_path, index=False)
    logging.warning("Gender coverage gaps written to %s", out_path)


def check_race_coverage(df: pd.DataFrame, output: Path) -> None:
    if RACE_TOTAL not in df.columns:
        logging.info("Race coverage check skipped: %s not found", RACE_TOTAL)
        return
    missing_parts = [col for col in RACE_PARTS if col not in df.columns]
    if missing_parts:
        logging.info(
            "Race coverage check skipped: missing detail columns %s",
            ", ".join(missing_parts),
        )
        return
    subset = df[["YEAR", "UNITID", RACE_TOTAL] + RACE_PARTS].copy()
    mask = subset[RACE_TOTAL].notna() & subset[RACE_PARTS].isna().any(axis=1)
    issues = subset.loc[mask]
    if issues.empty:
        logging.info("No race coverage gaps detected.")
        return
    out_path = output / "enrollment_validation_race_coverage_gaps.csv"
    issues.to_csv(out_path, index=False)
    logging.warning("Race coverage gaps written to %s", out_path)
