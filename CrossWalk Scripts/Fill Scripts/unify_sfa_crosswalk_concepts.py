#!/usr/bin/env python3
from __future__ import annotations

import argparse
import logging
import re
from pathlib import Path

import pandas as pd


CROSSWALK_FILLED = Path(
    "/Users/markjaysonfarol13/Higher Ed research/IPEDS/Paneled Datasets/Crosswalks/Filled/sfa_crosswalk_filled.csv"
)
CROSSWALK_UNIFIED = Path(
    "/Users/markjaysonfarol13/Higher Ed research/IPEDS/Paneled Datasets/Crosswalks/Filled/sfa_crosswalk_unified.csv"
)

FORM_FAMILY_PREFIXES = ("GIS4", "GRN4", "GIST", "GRNT", "NPGRN", "NPIST")


def make_label_slug(label: str) -> str:
    s = str(label or "").upper()
    s = re.sub(r",\s*\d{4}-\d{2}\s*$", "", s)
    s = re.sub(r"\s+", " ", s)
    return s.strip()


def make_concept_slug(label_slug: str) -> str | None:
    base = re.sub(r"[^A-Z0-9 ]", "", label_slug)
    tokens = base.split()
    tokens = tokens[:7]
    if not tokens:
        return None
    return "SFA_" + "_".join(tokens)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Unify SFA crosswalk concepts across reporter types and income bands using labels."
    )
    parser.add_argument("--input", type=Path, default=CROSSWALK_FILLED, help="Filled SFA crosswalk CSV")
    parser.add_argument("--output", type=Path, default=CROSSWALK_UNIFIED, help="Unified SFA crosswalk CSV")
    parser.add_argument("--log-level", default="INFO", choices=["DEBUG", "INFO", "WARNING", "ERROR"])
    return parser


def main() -> None:
    args = build_parser().parse_args()
    logging.basicConfig(level=getattr(logging, args.log_level), format="%(levelname)s %(message)s")

    df = pd.read_csv(args.input)

    required = {"source_var", "concept_key", "label"}
    missing = required - set(df.columns)
    if missing:
        raise KeyError(f"Missing required columns in SFA crosswalk: {sorted(missing)}")

    df["source_var"] = df["source_var"].astype(str).str.strip().str.upper()
    df["concept_key"] = df["concept_key"].astype("object").astype(str).str.strip()
    df["label"] = df["label"].astype(str)

    df["concept_key_raw"] = df["concept_key"]
    df["concept_key_unified"] = df["concept_key"]
    df["label_slug"] = df["label"].apply(make_label_slug)

    # Layer 1: AIDFS vs ANYAID (any aid)
    anyaid_n = {"AIDFSIN", "ANYAIDN"}
    anyaid_p = {"AIDFSIP", "ANYAIDP"}
    df.loc[df["source_var"].isin(anyaid_n), "concept_key_unified"] = "SFA_FTFT_N_ANY_AID"
    df.loc[df["source_var"].isin(anyaid_p), "concept_key_unified"] = "SFA_FTFT_PCT_ANY_AID"

    # Layer 2: SCFA vs SCFY (academic vs program reporters)
    scfa_scfy_map = {
        "SFA_COHORT_N_ALL_UG": {"SCFA1N", "SCFY1N"},
        "SFA_COHORT_N_FTFT": {"SCFA2", "SCFY2"},
        "SFA_COHORT_N_GRANT": {"SCFA11N", "SCFY11N"},
        "SFA_COHORT_N_TIV": {"SCFA12N", "SCFY12N"},
    }
    for concept_name, varset in scfa_scfy_map.items():
        mask = df["source_var"].isin(varset)
        df.loc[mask, "concept_key_unified"] = concept_name

    # Layer 3: label-based unification for sector/form variants (GIS4/GRN4/GIST/GRNT/NPGRN/NPIST)
    family_mask = df["source_var"].str.startswith(FORM_FAMILY_PREFIXES)
    raw_like_mask = df["concept_key_unified"].str.startswith("SFA_VAR_")
    family_df = df[family_mask & raw_like_mask].copy()

    for label_slug, grp in family_df.groupby("label_slug"):
        idx = grp.index
        current = grp["concept_key_unified"].unique()
        nonraw = [c for c in current if not c.startswith("SFA_VAR_") and c.strip()]
        if len(nonraw) == 1:
            canonical = nonraw[0]
        elif len(nonraw) > 1:
            canonical = sorted(nonraw)[0]
        else:
            canonical = make_concept_slug(label_slug)
        if canonical:
            df.loc[idx, "concept_key_unified"] = canonical

    df["concept_key"] = df["concept_key_unified"]
    df.drop(columns=["concept_key_unified"], inplace=True)

    args.output.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(args.output, index=False)

    nonempty = df[df["concept_key"].astype(str).str.strip() != ""]
    print(f"Unified rows: {len(df)}")
    print(f"Distinct source_var: {nonempty['source_var'].nunique()}")
    print(f"Distinct concept_key: {nonempty['concept_key'].nunique()}")
    print("Distinct concept_key per source_var (nunique distribution):")
    print(nonempty.groupby("source_var")["concept_key"].nunique().value_counts().head(10))


if __name__ == "__main__":
    main()
