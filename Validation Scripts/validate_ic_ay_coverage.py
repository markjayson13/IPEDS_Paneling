#!/usr/bin/env python3
"""Audit coverage of high-priority IC_AY Student Charges concepts."""

from __future__ import annotations

import argparse
import re
from dataclasses import dataclass
from pathlib import Path
from typing import List, Pattern

import numpy as np
import pandas as pd

DATA_ROOT = Path("/Users/markjaysonfarol13/Higher Ed research/IPEDS")
XWALK_PATH = DATA_ROOT / "Paneled Datasets" / "Crosswalks" / "Filled" / "ic_ay_crosswalk_all.csv"
ICAY_WIDE_PATH = DATA_ROOT / "Parquets" / "Unify" / "ICAYwide" / "icay_concepts_wide.parquet"
REPORT_PATH = DATA_ROOT / "Parquets" / "Validation" / "ic_ay_concept_coverage.csv"


@dataclass
class TargetConcept:
    name: str
    description: str
    pattern: Pattern


def p(regex: str) -> Pattern:
    return re.compile(regex, flags=re.IGNORECASE)


def build_targets() -> List[TargetConcept]:
    targets: List[TargetConcept] = []

    # COA on campus
    targets += [
        TargetConcept("COA_INDIST_ONCAMP", "Total price for in-district students living on campus", p(r"total price.*in[- ]district.*on[- ]campus")),
        TargetConcept("COA_INST_ONCAMP", "Total price for in-state students living on campus", p(r"total price.*in[- ]state.*on[- ]campus")),
        TargetConcept("COA_OUTST_ONCAMP", "Total price for out-of-state/nonresident students living on campus", p(r"total price.*(out[- ]of[- ]state|nonresident).*on[- ]campus")),
        TargetConcept("COA_INDIST_OFF_NOTFAM", "Total price for in-district students living off campus (not with family)", p(r"total price.*in[- ]district.*off[- ]campus.*not with family")),
        TargetConcept("COA_INST_OFF_NOTFAM", "Total price for in-state students living off campus (not with family)", p(r"total price.*in[- ]state.*off[- ]campus.*not with family")),
        TargetConcept("COA_OUTST_OFF_NOTFAM", "Total price for out-of-state students living off campus (not with family)", p(r"total price.*(out[- ]of[- ]state|nonresident).*off[- ]campus.*not with family")),
        TargetConcept("COA_INDIST_OFF_WITHFAM", "Total price for in-district students living off campus (with family)", p(r"total price.*in[- ]district.*off[- ]campus.*with family")),
        TargetConcept("COA_INST_OFF_WITHFAM", "Total price for in-state students living off campus (with family)", p(r"total price.*in[- ]state.*off[- ]campus.*with family")),
        TargetConcept("COA_OUTST_OFF_WITHFAM", "Total price for out-of-state students living off campus (with family)", p(r"total price.*(out[- ]of[- ]state|nonresident).*off[- ]campus.*with family")),
        TargetConcept("COA_COMBINED_COMPONENTS", "Combined tuition and fees, books and supplies, room, board and other expenses", p(r"combined.*tuition.*fees.*books.*supplies.*room.*board.*other expenses")),
    ]

    # Current-year components
    targets += [
        TargetConcept("CURR_TUITFEE_INDIST", "Published in-district tuition and fees", p(r"published.*in[- ]district.*tuition and fees")),
        TargetConcept("CURR_TUITFEE_INST", "Published in-state tuition and fees", p(r"published.*in[- ]state.*tuition and fees")),
        TargetConcept("CURR_TUITFEE_OUTST", "Published out-of-state tuition and fees", p(r"published.*(out[- ]of[- ]state|nonresident).*tuition and fees")),
        TargetConcept("CURR_BOOKS_SUPPLIES", "Books and supplies", p(r"books and supplies")),
        TargetConcept("CURR_ONCAMP_RMBD", "On campus room and board", p(r"on[- ]campus.*room and board")),
        TargetConcept("CURR_ONCAMP_OTHER", "On campus other expenses", p(r"on[- ]campus.*other expenses")),
        TargetConcept("CURR_OFF_NOTFAM_RMBD", "Off campus (not with family) room and board", p(r"off[- ]campus.*not with family.*room and board")),
        TargetConcept("CURR_OFF_NOTFAM_OTHER", "Off campus (not with family) other expenses", p(r"off[- ]campus.*not with family.*other expenses")),
        TargetConcept("CURR_OFF_WITHFAM_OTHER", "Off campus (with family) other expenses", p(r"off[- ]campus.*with family.*other expenses")),
    ]

    # Tuition/fee detail and guarantees
    targets += [
        TargetConcept("TUIT_INDIST", "Published in-district tuition", p(r"published.*in[- ]district.*tuition(?! and fees)")),
        TargetConcept("FEE_INDIST", "Published in-district fees", p(r"published.*in[- ]district.*fees(?!.*tuition)")),
        TargetConcept("TUIT_INDIST_GUAR", "Published in-district tuition guaranteed percent increase", p(r"in[- ]district.*tuition.*guaranteed.*percent increase")),
        TargetConcept("FEE_INDIST_GUAR", "Published in-district fees guaranteed percent increase", p(r"in[- ]district.*fees.*guaranteed.*percent increase")),
        TargetConcept("TUIT_INST", "Published in-state tuition", p(r"published.*in[- ]state.*tuition(?! and fees)")),
        TargetConcept("FEE_INST", "Published in-state fees", p(r"published.*in[- ]state.*fees(?!.*tuition)")),
        TargetConcept("TUIT_INST_GUAR", "Published in-state tuition guaranteed percent increase", p(r"in[- ]state.*tuition.*guaranteed.*percent increase")),
        TargetConcept("FEE_INST_GUAR", "Published in-state fees guaranteed percent increase", p(r"in[- ]state.*fees.*guaranteed.*percent increase")),
        TargetConcept("TUIT_OUTST", "Published out-of-state tuition", p(r"published.*(out[- ]of[- ]state|nonresident).*tuition(?! and fees)")),
        TargetConcept("FEE_OUTST", "Published out-of-state fees", p(r"published.*(out[- ]of[- ]state|nonresident).*fees(?!.*tuition)")),
        TargetConcept("TUIT_OUTST_GUAR", "Published out-of-state tuition guaranteed percent increase", p(r"(out[- ]of[- ]state|nonresident).*tuition.*guaranteed.*percent increase")),
        TargetConcept("FEE_OUTST_GUAR", "Published out-of-state fees guaranteed percent increase", p(r"(out[- ]of[- ]state|nonresident).*fees.*guaranteed.*percent increase")),
        TargetConcept("TUIT_CHARGE_VARIATION", "Tuition charge varies for in-district, in-state, out-of-state students", p(r"tuition charge varies.*in[- ]district.*in[- ]state.*out[- ]of[- ]state")),
    ]

    # Alternative tuition plans
    targets += [
        TargetConcept("ALT_ANY", "Any alternative tuition plans offered", p(r"alternative tuition plans offered")),
        TargetConcept("ALT_GUAR_PLAN", "Tuition guaranteed plan", p(r"tuition guaranteed plan")),
        TargetConcept("ALT_PREPAID", "Prepaid tuition plan", p(r"prepaid tuition plan")),
        TargetConcept("ALT_PAYMENT", "Tuition payment plan", p(r"tuition payment plan")),
        TargetConcept("ALT_OTHER", "Other alternative tuition plan", p(r"other.*alternative tuition plan")),
        TargetConcept("PROMISE_PROGRAM", "Participates in a Promise Program", p(r"participates in a promise program")),
    ]

    # Room and board infrastructure
    targets += [
        TargetConcept("HOUSING_FLAG", "Institution provides on-campus housing", p(r"provides on[- ]campus housing|institution provide on[- ]campus housing")),
        TargetConcept("DORM_CAPACITY", "Total dormitory capacity", p(r"total dormitory capacity")),
        TargetConcept("MEAL_PLAN_FLAG", "Institution provides board or meal plan", p(r"provides board or meal plan|board or meal plan provided")),
        TargetConcept("MEALS_PER_WEEK", "Number of meals per week in board charge", p(r"number of meals per week.*board charge")),
        TargetConcept("TYP_ROOM_CHARGE", "Typical room charge for academic year", p(r"typical room charge")),
        TargetConcept("TYP_BOARD_CHARGE", "Typical board charge for academic year", p(r"typical board charge")),
        TargetConcept("COMBINED_ROOM_BOARD", "Combined charge for room and board", p(r"combined charge for room and board")),
    ]

    return targets


def check_target(target: TargetConcept, cw: pd.DataFrame, wide: pd.DataFrame, wide_cols: set) -> dict:
    mask = cw["label"].str.contains(target.pattern)
    subset = cw.loc[mask].copy()
    n_rows = subset.shape[0]
    concept_keys = sorted(set(subset["concept_key"].astype(str).str.strip()) - {""})
    n_concept_keys = len(concept_keys)
    present_cols = [ck for ck in concept_keys if ck in wide_cols]
    n_present = len(present_cols)

    has_nonmissing = False
    for ck in present_cols:
        vals = pd.to_numeric(wide[ck], errors="coerce")
        if vals.notna().any():
            has_nonmissing = True
            break

    return {
        "target_name": target.name,
        "description": target.description,
        "n_crosswalk_rows": n_rows,
        "n_concept_keys": n_concept_keys,
        "concept_keys": ";".join(concept_keys),
        "n_present_columns": n_present,
        "has_nonmissing_data": bool(has_nonmissing),
    }


def load_crosswalk(path: Path) -> pd.DataFrame:
    cw = pd.read_csv(path, dtype=str).fillna("")
    cw.columns = [c.lower() for c in cw.columns]
    if "label" not in cw.columns:
        for cand in ("varlab", "var_label"):
            if cand in cw.columns:
                cw["label"] = cw[cand]
                break
    if "label" not in cw.columns:
        cw["label"] = ""
    cw["concept_key"] = cw["concept_key"].astype(str).str.strip()
    cw["source_var"] = cw["source_var"].astype(str).str.upper()
    return cw


def main() -> None:
    parser = argparse.ArgumentParser(description="Audit coverage of key IC_AY Student Charges concepts.")
    parser.add_argument("--crosswalk", type=Path, default=XWALK_PATH, help="Filled IC_AY crosswalk CSV path")
    parser.add_argument("--icay-wide", type=Path, default=ICAY_WIDE_PATH, help="ICAYwide parquet path")
    args = parser.parse_args()

    if not args.crosswalk.exists():
        raise SystemExit(f"Crosswalk not found: {args.crosswalk}")
    if not args.icay_wide.exists():
        raise SystemExit(f"ICAY wide panel not found: {args.icay_wide}")

    cw = load_crosswalk(args.crosswalk)
    cw["label"] = cw["label"].astype(str).str.strip()

    wide = pd.read_parquet(args.icay_wide)
    wide_cols = set(wide.columns)

    targets = build_targets()
    records = [check_target(t, cw, wide, wide_cols) for t in targets]
    report = pd.DataFrame(records).sort_values("target_name")

    REPORT_PATH.parent.mkdir(parents=True, exist_ok=True)
    report.to_csv(REPORT_PATH, index=False)
    print(f"Wrote IC_AY concept coverage report to {REPORT_PATH}")

    missing_xwalk = report[report["n_crosswalk_rows"] == 0]
    missing_cols = report[(report["n_crosswalk_rows"] > 0) & (report["n_present_columns"] == 0)]
    missing_data = report[
        (report["n_present_columns"] > 0) & (~report["has_nonmissing_data"])
    ]

    print("\n=== Coverage summary ===")
    print(f"Targets total: {report.shape[0]}")
    print(f"Targets with NO crosswalk match: {missing_xwalk.shape[0]}")
    print(f"Targets with crosswalk match but NO column in ICAYwide: {missing_cols.shape[0]}")
    print(f"Targets with columns but ALL missing data: {missing_data.shape[0]}")

    if not missing_xwalk.empty:
        print("\nTargets with no crosswalk rows (mapping gaps / regex too strict):")
        print(missing_xwalk[["target_name", "description"]].to_string(index=False))

    if not missing_cols.empty:
        print("\nTargets with crosswalk rows but no columns in ICAYwide (panel/stabilizer gaps):")
        print(missing_cols[["target_name", "description", "concept_keys"]].to_string(index=False))

    if not missing_data.empty:
        print("\nTargets whose columns exist but appear to be entirely missing:")
        print(missing_data[["target_name", "description", "concept_keys"]].to_string(index=False))


if __name__ == "__main__":
    main()
