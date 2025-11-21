#!/usr/bin/env python3
from __future__ import annotations

import argparse
import logging
from pathlib import Path
from typing import Dict, Iterable, List

import pandas as pd

# Defaults
TEMPLATE_PATH = Path("/Users/markjaysonfarol13/Higher Ed research/IPEDS/Paneled Datasets/Crosswalks/sfa_crosswalk_template.csv")
FILLED_PATH = Path("/Users/markjaysonfarol13/Higher Ed research/IPEDS/Paneled Datasets/Crosswalks/Filled/sfa_crosswalk_filled.csv")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Auto-fill SFA crosswalk template using explicit unification mappings; others default to their source_var."
    )
    parser.add_argument("--input", type=Path, default=TEMPLATE_PATH, help="SFA crosswalk template CSV")
    parser.add_argument("--output", type=Path, default=FILLED_PATH, help="Destination filled SFA crosswalk CSV")
    parser.add_argument("--log-level", default="INFO", choices=["DEBUG", "INFO", "WARNING", "ERROR"])
    return parser


def invert_mapping(mapping: Dict[str, Iterable[str]]) -> Dict[str, str]:
    """Create source_var -> concept_key map."""
    out: Dict[str, str] = {}
    for concept, vars_ in mapping.items():
        for v in vars_:
            key = str(v).strip().upper()
            out[key] = concept
    return out


def main() -> None:
    args = build_parser().parse_args()
    logging.basicConfig(level=getattr(logging, args.log_level), format="%(levelname)s %(message)s")

    if not args.input.exists():
        raise FileNotFoundError(f"Template not found: {args.input}")

    df = pd.read_csv(args.input)
    if "source_var" not in df.columns:
        raise KeyError("Template missing required column 'source_var'")

    df["source_var"] = df["source_var"].astype(str).str.strip().str.upper()
    # Start fresh; ignore any pre-existing concept_key in template
    df["concept_key"] = pd.NA

    # Explicit unification mappings
    mapping: Dict[str, List[str]] = {
        # 1. Residency and student counts
        "SFA_ALL_N_UG": ["SCFA2", "SCFY2", "SCUGRAD"],
        "SFA_COHORT_N_FTFT": ["SCFA1N", "SCFY1N", "SCUGFFN"],
        "SFA_COHORT_N_DS_UG": ["SCFA2DG", "SCFY2DG", "SCUGDGSK"],
        "SFA_RESIDENCY_INDT_N": ["SCFA11N", "SCFY11N"],
        "SFA_RESIDENCY_INST_N": ["SCFA12N", "SCFY12N"],
        "SFA_RESIDENCY_OUTST_N": ["SCFA13N", "SCFY13N"],
        "SFA_RESIDENCY_UNKWN_N": ["SCFA14N", "SCFY14N"],
        # FTFT any aid
        "SFA_FTFT_N_ANY_AID": ["AIDFSIN", "ANYAIDN"],
        # 2. Living status – Title IV (GIS4/GIST only)
        "SFA_LIVING_ONC_T4ALL_N": ["GIS4ON0", "GIS4ON1", "GIS4ON2", "GISTON0", "GISTON1", "GISTON2"],
        "SFA_LIVING_OFFCWF_T4ALL_N": ["GIS4WF0", "GIS4WF1", "GIS4WF2", "GISTWF0", "GISTWF1", "GISTWF2"],
        "SFA_LIVING_OFFCNWF_T4ALL_N": ["GIS4OF0", "GIS4OF1", "GIS4OF2", "GISTOF0", "GISTOF1", "GISTOF2"],
        "SFA_LIVING_UNKWN_T4ALL_N": ["GIS4UN0", "GIS4UN1", "GIS4UN2", "GISTUN0", "GISTUN1", "GISTUN2"],
        # Grant/scholarship living status (GRN/GRNT only)
        "SFA_LIVING_ONC_GRANTALL_N": ["GRN4ON0", "GRN4ON1", "GRN4ON2", "GRNTON0", "GRNTON1", "GRNTON2"],
        "SFA_LIVING_OFFCWF_GRANTALL_N": ["GRN4WF0", "GRN4WF1", "GRN4WF2", "GRNTWF0", "GRNTWF1", "GRNTWF2"],
        "SFA_LIVING_OFFCNWF_GRANTALL_N": ["GRN4OF0", "GRN4OF1", "GRN4OF2", "GRNTOF0", "GRNTOF1", "GRNTOF2"],
        "SFA_LIVING_UNKWN_GRANTALL_N": ["GRN4UN0", "GRN4UN1", "GRN4UN2", "GRNTUN0", "GRNTUN1", "GRNTUN2"],
        # Title IV all income (non-band) — counts
        "SFA_T4N_N": ["GIS4N0", "GIS4N1", "GIS4N2", "GISTN0", "GISTN1", "GISTN2"],
        # Title IV income bands (counts) — GIS4N only
        "SFA_T4N1_N": ["GIS4N10", "GIS4N11", "GIS4N12"],
        "SFA_T4N2_N": ["GIS4N20", "GIS4N21", "GIS4N22"],
        "SFA_T4N3_N": ["GIS4N30", "GIS4N31", "GIS4N32"],
        "SFA_T4N4_N": ["GIS4N40", "GIS4N41", "GIS4N42"],
        "SFA_T4N5_N": ["GIS4N50", "GIS4N51", "GIS4N52"],
        # Title IV G-family (kept separate)
        "SFA_T4N_G": ["GIS4G0", "GIS4G1", "GIS4G2"],
        "SFA_T4N1_G": ["GIS4G10", "GIS4G11", "GIS4G12"],
        "SFA_T4N2_G": ["GIS4G20", "GIS4G21", "GIS4G22"],
        "SFA_T4N3_G": ["GIS4G30", "GIS4G31", "GIS4G32"],
        "SFA_T4N4_G": ["GIS4G40", "GIS4G41", "GIS4G42"],
        "SFA_T4N5_G": ["GIS4G50", "GIS4G51", "GIS4G52"],
        # Grant/scholarship counts (N family)
        "SFA_GRN_N": ["GRN4N0", "GRN4N1", "GRN4N2", "GRNTN0", "GRNTN1", "GRNTN2"],
        "SFA_GRN1_N": ["GRN4N10", "GRN4N11", "GRN4N12"],
        "SFA_GRN2_N": ["GRN4N20", "GRN4N21", "GRN4N22"],
        "SFA_GRN3_N": ["GRN4N30", "GRN4N31", "GRN4N32"],
        "SFA_GRN4_N": ["GRN4N40", "GRN4N41", "GRN4N42"],
        "SFA_GRN5_N": ["GRN4N50", "GRN4N51", "GRN4N52"],
        # Grant/scholarship counts (G family kept separate)
        "SFA_GRN_G": ["GRN4G0", "GRN4G1", "GRN4G2"],
        "SFA_GRN1_G": ["GRN4G10", "GRN4G11", "GRN4G12"],
        "SFA_GRN2_G": ["GRN4G20", "GRN4G21", "GRN4G22"],
        "SFA_GRN3_G": ["GRN4G30", "GRN4G31", "GRN4G32"],
        "SFA_GRN4_G": ["GRN4G40", "GRN4G41", "GRN4G42"],
        "SFA_GRN5_G": ["GRN4G50", "GRN4G51", "GRN4G52"],
        # 3. Net price (Title IV)
        "SFA_NP_T4N_A": ["NPIST0", "NPIST1", "NPIST2", "NPGRN0", "NPGRN1", "NPGRN2"],
        "SFA_NP_T4N1_A": ["NPIS410", "NPIS411", "NPIS412", "NPT410", "NPT411", "NPT412"],
        "SFA_NP_T4N2_A": ["NPIS420", "NPIS421", "NPIS422", "NPT420", "NPT421", "NPT422"],
        "SFA_NP_T4N3_A": ["NPIS430", "NPIS431", "NPIS432", "NPT430", "NPT431", "NPT432"],
        "SFA_NP_T4N4_A": ["NPIS440", "NPIS441", "NPIS442", "NPT440", "NPT441", "NPT442"],
        "SFA_NP_T4N5_A": ["NPIS450", "NPIS451", "NPIS452", "NPT450", "NPT451", "NPT452"],
    }

    concept_map = invert_mapping(mapping)

    # Apply explicit mappings
    df.loc[df["source_var"].isin(concept_map.keys()), "concept_key"] = df["source_var"].map(concept_map)

    # Map specific financial aid variables to themselves (explicitly kept list)
    self_map_vars = [
        "PGRNT_T", "PGRNT_N", "PGRNT_A",
        "OFGRT_T", "OFGRT_N", "OFGRT_A",
        "SGRNT_T", "SGRNT_N", "SGRNT_A",
        "IGRNT_T", "IGRNT_N", "IGRNT_A",
        "FLOAN_T", "FLOAN_N", "FLOAN_A",
        "OLOAN_T", "OLOAN_N", "OLOAN_A",
        "UPGRNTT", "UPGRNTN", "UPGRNTA",
        "UAGRNTT", "UAGRNTN", "UAGRNTA",
        "UFLOANT", "UFLOANN", "UFLOANA",
        "AGRNT_N",
    ]

    remap = {
        "UDGPGRNTT": "UPGRNTT",
        "UDGPGRNTN": "UPGRNTN",
        "UDGPGRNTA": "UPGRNTA",
        "UDGAGRNTT": "UAGRNTT",
        "UDGAGRNTA": "UAGRNTA",
        "UDGAGRNTN": "UAGRNTN",
        "UDGFLOANT": "UFLOANT",
        "UDGFLOANN": "UFLOANN",
        "UDGFLOANA": "UFLOANA",
    }

    keep_mask = df["source_var"].isin([v.strip().upper() for v in self_map_vars])
    df.loc[keep_mask, "concept_key"] = df.loc[keep_mask, "source_var"]

    # Apply explicit remaps (detail -> aggregate concepts)
    for src, concept in remap.items():
        df.loc[df["source_var"] == src, "concept_key"] = concept

    # Leave all other concept_key cells as missing (no autofill beyond the lists)

    # Save
    args.output.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(args.output, index=False)

    logging.info("Saved filled SFA crosswalk to %s", args.output)
    logging.info("Total rows: %d", len(df))
    logging.info("Distinct source_var: %d", df['source_var'].nunique())
    logging.info("Distinct concept_key: %d", df['concept_key'].nunique())
    # Show any source_vars that were explicitly mapped
    logging.info("Explicitly unified vars: %d", len(concept_map))


if __name__ == "__main__":
    main()
