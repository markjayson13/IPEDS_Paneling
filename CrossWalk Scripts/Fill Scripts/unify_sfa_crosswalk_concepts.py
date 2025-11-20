#!/usr/bin/env python3
from __future__ import annotations

import argparse
import logging
from pathlib import Path

import pandas as pd


CROSSWALK_FILLED = Path(
    "/Users/markjaysonfarol13/Higher Ed research/IPEDS/Paneled Datasets/Crosswalks/Filled/sfa_crosswalk_filled.csv"
)
CROSSWALK_UNIFIED = Path(
    "/Users/markjaysonfarol13/Higher Ed research/IPEDS/Paneled Datasets/Crosswalks/Filled/sfa_crosswalk_unified.csv"
)


def starts_with_any(text: str, prefixes: set[str]) -> bool:
    return any(text.startswith(p) for p in prefixes)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Unify SFA crosswalk concepts across reporter types and income bands."
    )
    parser.add_argument("--input", type=Path, default=CROSSWALK_FILLED, help="Filled SFA crosswalk CSV")
    parser.add_argument("--output", type=Path, default=CROSSWALK_UNIFIED, help="Unified SFA crosswalk CSV")
    parser.add_argument("--log-level", default="INFO", choices=["DEBUG", "INFO", "WARNING", "ERROR"])
    return parser


def main() -> None:
    args = build_parser().parse_args()
    logging.basicConfig(level=getattr(logging, args.log_level), format="%(levelname)s %(message)s")

    df = pd.read_csv(args.input)
    # Normalize
    df["source_var"] = df["source_var"].astype(str).str.strip().str.upper()
    df["concept_key"] = df["concept_key"].astype(str).str.strip()

    df["concept_key_raw"] = df["concept_key"]
    df["concept_key_unified"] = df["concept_key"]

    # Layer 1: Any aid (count and percent)
    anyaid_n = {"AIDFSIN", "ANYAIDN"}
    anyaid_p = {"AIDFSIP", "ANYAIDP"}
    mask_n = df["source_var"].isin(anyaid_n)
    df.loc[mask_n, "concept_key_unified"] = "SFA_FTFT_N_ANY_AID"
    mask_p = df["source_var"].isin(anyaid_p)
    df.loc[mask_p, "concept_key_unified"] = "SFA_FTFT_PCT_ANY_AID"

    # Layer 2: SCFA vs SCFY cohorts
    scfa_scfy_groups = {
        "SFA_COHORT_N_GROUP1": {"SCFA1N", "SCFY1N"},
        "SFA_COHORT_N_GROUP2": {"SCFA2", "SCFY2"},
        "SFA_COHORT_N_GROUP3": {"SCFA11N", "SCFY11N"},
        "SFA_COHORT_N_GROUP4": {"SCFA12N", "SCFY12N"},
    }
    for concept_name, varset in scfa_scfy_groups.items():
        mask = df["source_var"].isin(varset)
        df.loc[mask, "concept_key_unified"] = concept_name

    # Layer 3.1: Counts by income band
    band_counts = {
        "SFA_N_STUDENTS_INC_0_30K": {"GRN4N1", "GRN4N10", "GRN4N11", "GRN4N12"},
        "SFA_N_STUDENTS_INC_30_48K": {"GRN4N2", "GRN4N20", "GRN4N21", "GRN4N22"},
        "SFA_N_STUDENTS_INC_48_75K": {"GRN4N3", "GRN4N30", "GRN4N31", "GRN4N32"},
        "SFA_N_STUDENTS_INC_75_110K": {"GRN4N4", "GRN4N40", "GRN4N41", "GRN4N42"},
        "SFA_N_STUDENTS_INC_110K_PLUS": {"GRN4N5", "GRN4N50", "GRN4N51", "GRN4N52"},
    }
    for concept_name, varset in band_counts.items():
        mask = df["source_var"].isin(varset)
        df.loc[mask, "concept_key_unified"] = concept_name

    # Layer 3.2: Average grant by income band
    band_avg_grant = {
        "SFA_AVG_GRANT_INC_0_30K": {"GIS4A1", "GIS4A10", "GIS4A11", "GIS4A12"},
        "SFA_AVG_GRANT_INC_30_48K": {"GIS4A2", "GIS4A20", "GIS4A21", "GIS4A22"},
        "SFA_AVG_GRANT_INC_48_75K": {"GIS4A3", "GIS4A30", "GIS4A31", "GIS4A32"},
        "SFA_AVG_GRANT_INC_75_110K": {"GIS4A4", "GIS4A40", "GIS4A41", "GIS4A42"},
        "SFA_AVG_GRANT_INC_110K_PLUS": {"GIS4A5", "GIS4A50", "GIS4A51", "GIS4A52"},
    }
    for concept_name, varset in band_avg_grant.items():
        mask = df["source_var"].isin(varset)
        df.loc[mask, "concept_key_unified"] = concept_name

    # Layer 3.3: Net price by income band (pattern-based)
    band_net_price_prefixes = {
        "SFA_AVG_NET_PRICE_INC_0_30K": {"NPT41"},
        "SFA_AVG_NET_PRICE_INC_30_48K": {"NPT42"},
        "SFA_AVG_NET_PRICE_INC_48_75K": {"NPT43"},
        "SFA_AVG_NET_PRICE_INC_75_110K": {"NPT44"},
        "SFA_AVG_NET_PRICE_INC_110K_PLUS": {"NPT45"},
    }
    src_series = df["source_var"].astype(str)
    for concept_name, prefixes in band_net_price_prefixes.items():
        mask = src_series.apply(lambda v: starts_with_any(v, prefixes))
        df.loc[mask, "concept_key_unified"] = concept_name

    # Unify legacy NET_PRICE_AVG_INC_* concept names into the SFA_AVG_NET_PRICE_* equivalents
    net_price_suffixes = [
        "0_30K",
        "30_48K",
        "48_75K",
        "75_110K",
        "110K_PLUS",
    ]
    key_remap = {
        f"NET_PRICE_AVG_INC_{suf}": f"SFA_AVG_NET_PRICE_INC_{suf}"
        for suf in net_price_suffixes
    }
    df["concept_key_unified"] = df["concept_key_unified"].replace(key_remap)

    df["concept_key"] = df["concept_key_unified"]
    df = df.drop(columns=["concept_key_unified"])

    args.output.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(args.output, index=False)

    nonempty = df[df["concept_key"].astype(str).str.strip() != ""]
    print(f"Unified rows: {len(df)}")
    print(f"Distinct source_var: {nonempty['source_var'].nunique()}")
    print(f"Distinct concept_key: {nonempty['concept_key'].nunique()}")
    print(nonempty.groupby("source_var")["concept_key"].nunique().value_counts().head(10))


if __name__ == "__main__":
    main()
