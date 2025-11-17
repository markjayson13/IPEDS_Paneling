#!/usr/bin/env python3
"""Validate that required EF/E12 crosswalk families have concept_key coverage."""

from __future__ import annotations

import argparse
from pathlib import Path
from typing import List

import pandas as pd

DEFAULT_INPUT = Path("/Users/markjaysonfarol13/Higher Ed research/IPEDS/Paneled Datasets/Crosswalks/Filled/enrollment_crosswalk_autofilled.csv")
DEFAULT_MAX_SAMPLE = 15

STATES = [
    "alabama",
    "alaska",
    "arizona",
    "arkansas",
    "california",
    "colorado",
    "connecticut",
    "delaware",
    "district of columbia",
    "florida",
    "georgia",
    "hawaii",
    "idaho",
    "illinois",
    "indiana",
    "iowa",
    "kansas",
    "kentucky",
    "louisiana",
    "maine",
    "maryland",
    "massachusetts",
    "michigan",
    "minnesota",
    "mississippi",
    "missouri",
    "montana",
    "nebraska",
    "nevada",
    "new hampshire",
    "new jersey",
    "new mexico",
    "new york",
    "north carolina",
    "north dakota",
    "ohio",
    "oklahoma",
    "oregon",
    "pennsylvania",
    "rhode island",
    "south carolina",
    "south dakota",
    "tennessee",
    "texas",
    "utah",
    "vermont",
    "virginia",
    "washington",
    "west virginia",
    "wisconsin",
    "wyoming",
    "state unknown",
]


def is_blank(x: object) -> bool:
    return pd.isna(x) or str(x).strip() == ""


def check_family(name: str, mask: pd.Series, cw: pd.DataFrame, max_sample: int) -> List[str]:
    rows = cw.loc[mask].copy()
    if rows.empty:
        print(f"[WARN] Family '{name}' matched 0 rows in crosswalk.")
        return []

    missing = rows[rows["concept_key"].map(is_blank)]
    if missing.empty:
        print(f"[OK] Family '{name}' is fully covered. Rows: {len(rows)}")
        return []

    errors = [
        f"Family '{name}' has {len(missing)} rows with blank concept_key (out of {len(rows)} rows).",
        "Sample missing rows:\n"
        + missing[["survey", "source_var", "year_start", "label_norm"]]
        .head(max_sample)
        .to_string(index=False),
    ]
    return errors


def load_crosswalk(path: Path) -> pd.DataFrame:
    cw = pd.read_csv(path)
    cw.columns = [c.strip() for c in cw.columns]
    required = ["survey", "source_var", "year_start", "concept_key", "label_norm"]
    for col in required:
        if col not in cw.columns:
            raise RuntimeError(f"Crosswalk missing required column: {col}")

    cw["survey"] = cw["survey"].astype(str).str.strip()
    cw["source_var"] = cw["source_var"].astype(str).str.strip()
    cw["label_norm"] = cw["label_norm"].astype(str).str.strip().str.lower()
    cw["concept_key"] = cw["concept_key"].astype("string")
    cw["year_start"] = pd.to_numeric(cw["year_start"], errors="coerce").astype("Int64")
    return cw


def build_masks(cw: pd.DataFrame) -> dict:
    masks = {}
    survey = cw["survey"].str.upper()
    label = cw["label_norm"]

    masks["E12 headcount (unduplicated)"] = (survey == "12MONTHENROLLMENT") & (
        label.str.contains("unduplicated", case=False, na=False)
        | label.str.contains("12-month enrollment", case=False, na=False)
    )

    masks["E12 status/imputation flags"] = (survey == "12MONTHENROLLMENT") & (
        label.str.contains("response status", case=False, na=False)
        | label.str.contains("revision status", case=False, na=False)
        | label.str.contains("parent/child indicator", case=False, na=False)
        | label.str.contains("parent institution", case=False, na=False)
        | label.str.contains("allocation factor", case=False, na=False)
        | label.str.contains("type of imputation method", case=False, na=False)
        | label.str.contains("method used to report", case=False, na=False)
        | label.str.contains("12-month reporting period", case=False, na=False)
    )

    masks["E12 historic 12-month + instructional activity"] = (survey == "12MONTHENROLLMENT") & label.str.contains(
        "12-month enrollment and instructional activity", case=False, na=False
    )

    masks["EF core totals"] = (survey == "FALLENROLLMENT") & (
        label.str.contains("all students total", case=False, na=False)
        | label.str.contains("undergraduate total", case=False, na=False)
        | label.str.contains("undergraduate degree/certificate-seeking total", case=False, na=False)
        | label.str.contains("undergraduate degree/certificate-seeking first time total", case=False, na=False)
        | label.str.contains("graduate and first-professional total", case=False, na=False)
    )

    masks["EF disability indicators"] = (survey == "FALLENROLLMENT") & (
        label.str.contains("undergraduate students with disabilities", case=False, na=False)
        | label.str.contains("percent indicator of undergraduates formally registered as students with disabilities", case=False, na=False)
        | label.str.contains("percent of undergraduates, who are formally registered as students with disabilities", case=False, na=False)
    )

    base_res = (
        (survey == "FALLENROLLMENT")
        & label.str.contains("first-time degree/certificate-seeking undergraduate students", case=False, na=False)
    )
    mask_residence_any = base_res & (
        label.str.contains("state of residence", case=False, na=False)
        | label.str.contains("residence and migration of first-time", case=False, na=False)
    )
    mask_state_name = pd.Series(False, index=cw.index)
    for state in STATES:
        mask_state_name |= label.str.contains(state, case=False, na=False)
    masks["EF residence & migration (states)"] = mask_residence_any & mask_state_name

    masks["EF majors (CIP fields)"] = (survey == "FALLENROLLMENT") & (
        label.str.startswith("13.0000-education")
        | label.str.startswith("14.0000-engineering")
        | label.str.startswith("26.0000-biological sciences/life sciences")
        | label.str.startswith("27.0000-mathematics")
        | label.str.startswith("40.0000-physical sciences")
        | label.str.startswith("52.0000-business management and administrative services")
        | label.str.startswith("22.0101-law")
        | label.str.startswith("51.0401-dentistry")
        | label.str.startswith("51.1201-medicine")
    )

    masks["EF status / response flags"] = (survey == "FALLENROLLMENT") & (
        label.str.contains("response status", case=False, na=False)
        | label.str.contains("revision status", case=False, na=False)
        | label.str.contains("status of enrollment component when data collection closed", case=False, na=False)
        | label.str.contains("parent/child indicator - enrollment", case=False, na=False)
        | label.str.contains("allocation factor - enrollment component", case=False, na=False)
        | label.str.contains("type of imputation method - enrollment", case=False, na=False)
        | label.str.contains("method used to report fall enrollment", case=False, na=False)
        | label.str.contains("method used to report race and ethnicity - fall enrollment", case=False, na=False)
        | label.str.contains("response status enrollment - race/ethnicity", case=False, na=False)
        | label.str.contains("response status enrollment- age", case=False, na=False)
        | label.str.contains("response status - residence of first-time first-year students", case=False, na=False)
        | label.str.contains("response status - total entering class and retention rates", case=False, na=False)
    )

    return masks


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", type=Path, default=DEFAULT_INPUT, help="Final enrollment crosswalk CSV path")
    parser.add_argument("--max-sample", type=int, default=DEFAULT_MAX_SAMPLE, help="Max rows to show in each sample")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    if not args.input.exists():
        raise SystemExit(f"Crosswalk not found: {args.input}")

    cw = load_crosswalk(args.input)
    masks = build_masks(cw)

    errors: list[str] = []
    for name, mask in masks.items():
        errors.extend(check_family(name, mask, cw, args.max_sample))

    if errors:
        print("\n=== COVERAGE ERRORS DETECTED ===")
        for msg in errors:
            print(msg)
            print("-" * 80)
        raise SystemExit(1)

    print("\nAll coverage families are fully mapped (no blank concept_key in required families).")


if __name__ == "__main__":
    main()
