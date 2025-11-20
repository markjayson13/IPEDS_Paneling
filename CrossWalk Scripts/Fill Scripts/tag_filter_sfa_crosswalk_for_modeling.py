#!/usr/bin/env python3
from __future__ import annotations

import argparse
import logging
from pathlib import Path



import pandas as pd


# Use the unified crosswalk (after running unify_sfa_crosswalk_concepts.py) as the default input.
UNIFIED = Path(
    "/Users/markjaysonfarol13/Higher Ed research/IPEDS/Paneled Datasets/Crosswalks/Filled/sfa_crosswalk_unified.csv"
)
TAGGED = Path(
    "/Users/markjaysonfarol13/Higher Ed research/IPEDS/Paneled Datasets/Crosswalks/Filled/sfa_crosswalk_tagged.csv"
)
MODELING = Path(
    "/Users/markjaysonfarol13/Higher Ed research/IPEDS/Paneled Datasets/Crosswalks/Filled/sfa_crosswalk_modeling.csv"
)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Tag and filter the SFA crosswalk for modeling use, with optional unifications." 
    )
    parser.add_argument("--input", type=Path, default=UNIFIED, help="Unified SFA crosswalk CSV")
    parser.add_argument("--tagged-output", type=Path, default=TAGGED, help="Tagged SFA crosswalk CSV")
    parser.add_argument("--modeling-output", type=Path, default=MODELING, help="Filtered modeling SFA crosswalk CSV")
    parser.add_argument("--log-level", default="INFO", choices=["DEBUG", "INFO", "WARNING", "ERROR"])
    return parser


def main() -> None:
    args = build_parser().parse_args()
    logging.basicConfig(level=getattr(logging, args.log_level), format="%(levelname)s %(message)s")

    df = pd.read_csv(args.input)
    required_cols = ["source_var", "concept_key"]
    for col in required_cols:
        if col not in df.columns:
            raise KeyError(f"Missing required column {col} in {args.input}")

    df["source_var"] = df["source_var"].astype(str).str.strip().str.upper()
    df["concept_key"] = df["concept_key"].astype(str).str.strip()
    df["label"] = df.get("label", "").astype(str)

    df["concept_key_raw"] = df["concept_key"]

    # Unify SCFA/SCFY degree vs nondegree cohorts
    SCFA2DG_FAMILY = {"SCFA2DG", "SCFY2DG"}
    SCFA2ND_FAMILY = {"SCFA2ND", "SCFY2ND"}

    mask_deg = df["source_var"].isin(SCFA2DG_FAMILY)
    df.loc[mask_deg, "concept_key"] = "SFA_COHORT_N_DEGREE_UG"

    mask_ndeg = df["source_var"].isin(SCFA2ND_FAMILY)
    df.loc[mask_ndeg, "concept_key"] = "SFA_COHORT_N_NONDEG_UG"

    # Tagging defaults
    df["keep_in_model"] = True
    df["reason_tag"] = "core_or_unspecified"

    # Structural flags and 2022+ undergrad detail vars to drop from modeling panel
    STRUCTURAL_VARS = {
        "SCUGDGSK",
        "SFAFORM",
    }
    UNDERGRAD_DETAIL_VARS = {
        "UDGAGRNTA",
        "UDGAGRNTN",
        "UDGAGRNTP",
        "UDGAGRNTT",
        "UDGFLOANA",
        "UDGFLOANN",
        "UDGFLOANP",
        "UDGFLOANT",
        "UDGPGRNTA",
        "UDGPGRNTN",
        "UDGPGRNTP",
        "UDGPGRNTT",
        "UNDAGRNTA",
        "UNDAGRNTN",
        "UNDAGRNTP",
        "UNDAGRNTT",
        "UNDFLOANA",
        "UNDFLOANN",
        "UNDFLOANP",
        "UNDFLOANT",
        "UNDPGRNTA",
        "UNDPGRNTN",
        "UNDPGRNTP",
        "UNDPGRNTT",
    }

    drop_vars = STRUCTURAL_VARS | UNDERGRAD_DETAIL_VARS
    mask_drop = df["source_var"].isin(drop_vars)
    df.loc[mask_drop, "keep_in_model"] = False
    df.loc[mask_drop, "reason_tag"] = "drop_2022plus_detail_or_structural"

    # Tag unified SCFA2DG/SCFA2ND concepts for clarity
    df.loc[df["concept_key"] == "SFA_COHORT_N_DEGREE_UG", "reason_tag"] = "degree_ug_cohort_2022plus"
    df.loc[df["concept_key"] == "SFA_COHORT_N_NONDEG_UG", "reason_tag"] = "nondegree_ug_cohort_2022plus"

    # Write tagged crosswalk
    args.tagged_output.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(args.tagged_output, index=False)
    logging.info("Wrote tagged SFA crosswalk to %s", args.tagged_output)

    # Modeling subset
    df_model = df[df["keep_in_model"]].copy()
    args.modeling_output.parent.mkdir(parents=True, exist_ok=True)
    df_model.to_csv(args.modeling_output, index=False)
    logging.info("Wrote modeling SFA crosswalk (keep_in_model=True) to %s, rows=%d", args.modeling_output, len(df_model))

    total_vars = df["source_var"].nunique()
    kept_vars = df_model["source_var"].nunique()
    dropped_vars = total_vars - kept_vars
    logging.info("Total distinct SFA source_var: %d", total_vars)
    logging.info("Distinct SFA source_var kept for modeling: %d", kept_vars)
    logging.info("Distinct SFA source_var dropped from modeling: %d", dropped_vars)
    logging.info("Reason_tag distribution:\n%s", df["reason_tag"].value_counts())


if __name__ == "__main__":
    main()
