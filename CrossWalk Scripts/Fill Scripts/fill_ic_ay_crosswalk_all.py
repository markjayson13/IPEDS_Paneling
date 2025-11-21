#!/usr/bin/env python3
from __future__ import annotations

import argparse
import logging
from pathlib import Path
from typing import Dict, Iterable, List

import pandas as pd

CROSSWALK_DIR = Path("/Users/markjaysonfarol13/Higher Ed research/IPEDS/Paneled Datasets/Crosswalks")
TEMPLATE = CROSSWALK_DIR / "ic_ay_crosswalk_template.csv"
FILLED = CROSSWALK_DIR / "Filled" / "ic_ay_crosswalk_all.csv"


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Auto-fill IC_AY crosswalk template with explicit concept mappings; leave others blank."
    )
    parser.add_argument("--input", type=Path, default=TEMPLATE, help="IC_AY crosswalk template CSV")
    parser.add_argument("--output", type=Path, default=FILLED, help="Filled IC_AY crosswalk CSV")
    parser.add_argument("--log-level", default="INFO", choices=["DEBUG", "INFO", "WARNING", "ERROR"])
    return parser


def invert_mapping(mapping: Dict[str, Iterable[str]]) -> Dict[str, str]:
    out: Dict[str, str] = {}
    for concept, vars_ in mapping.items():
        for v in vars_:
            out[str(v).strip().upper()] = concept
    return out


def main() -> None:
    args = build_parser().parse_args()
    logging.basicConfig(level=getattr(logging, args.log_level), format="%(levelname)s %(message)s")

    df = pd.read_csv(args.input)
    if "source_var" not in df.columns:
        raise KeyError("Template missing required column 'source_var'")

    df["source_var"] = df["source_var"].astype(str).str.strip().str.upper()
    df["concept_key"] = pd.NA

    # Explicit mappings
    mapping_raw: Dict[str, List[str]] = {
        # COA in-district
        "ICAY_COA_INDONC": ["CINDON"],
        "ICAY_COA_INDFAM": ["CINDFAM"],
        "ICAY_COA_INDOFFC": ["CINDOFF"],
        "ICAY_COA_COMPIND": ["CMP1AY3"],
        # COA in-state
        "ICAY_COA_INSTC": ["CINSON"],
        "ICAY_COA_INSTFAM": ["CINSFAM"],
        "ICAY_COA_INSTOFF": ["CINSOFF"],
        "ICAY_COA_COMPSTATE": ["CMP2AY3"],
        # COA out-of-state
        "ICAY_COA_OUTSON": ["COTSON"],
        "ICAY_COA_OUTSFAM": ["COTSFAM"],
        "ICAY_COA_OUTSOFF": ["COTSOFF"],
        "ICAY_COA_COMPOUTST": ["CMP3AY3"],
        # COA program/year total price
        "ICAY_COA_PY": ["CMP1PY3"],
        # Tuition
        "ICAY_T_IND": ["PCCHG1AT3", "TUITION1", "CHG1AT3"],
        "ICAY_T_STATE": ["PCCHG2AT3", "TUITION2", "CHG2AT3"],
        "ICAY_T_OUTST": ["PCCHG3AT3", "TUITION3", "CHG3AT3"],
        # Fees
        "ICAY_F_IND": ["PCCHG1AF3", "FEE1", "CHG1AF3"],
        "ICAY_F_STATE": ["PCCHG2AF3", "FEE2", "CHG2AF3"],
        "ICAY_F_OUTST": ["PCCHG3AF3", "FEE3", "CHG3AF3"],
        # Tuition + fees
        "ICAY_TF_IND": ["PCCHG1AY3", "CHG1AY3"],
        "ICAY_TF_STATE": ["PCCHG2AY3", "CHG2AY3"],
        "ICAY_TF_OUTST": ["PCCHG3AY3", "CHG3AY3"],
        "ICAY_TOT_PY": ["PCCHG1PY3", "CHG1PY3"],
        # Books and supplies
        "ICAY_BOOKSUPP": ["CHG4AY3", "CHG4PY3", "PCCHG4AY3", "PCCHG4PY3"],
        # Room/board on campus
        "ICAY_ONCRMBRD": ["CHG5AY3", "CHG5PY3", "PCCHG5AY3", "PCCHG5PY3", "RMBRDAMT", "BOARDAMT"],
        # Other on campus
        "ICAY_ONCOTHEXP": ["CHG6AY3", "CHG6PY3", "PCCHG6AY3", "PCCHG6PY3"],
        # Room/board off campus not with family
        "ICAY_OFFCRMBRD": ["CHG7AY3", "CHG7PY3", "PCCHG7AY3", "PCCHG7PY3"],
        # Other off campus not with family
        "ICAY_OFFCOTHEXP": ["CHG8AY3", "CHG8PY3", "PCCHG8AY3", "PCCHG8PY3"],
        # Other off campus with family
        "ICAY_OFFCFOTHEXP": ["CHG9AY3", "CHG9PY3", "PCCHG9AY3", "PCCHG9PY3"],
        # Flags
        "ICAY_TUITVARY": ["TUITVARY"],
    }

    concept_map = invert_mapping(mapping_raw)
    df.loc[df["source_var"].isin(concept_map.keys()), "concept_key"] = df["source_var"].map(concept_map)

    # Leave all other concept_key cells as missing (template blanks)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(args.output, index=False)

    filled = df["concept_key"].notna() & (df["concept_key"].astype(str).str.strip() != "")
    logging.info("Saved filled IC_AY crosswalk to %s", args.output)
    logging.info("Total rows: %d", len(df))
    logging.info("Distinct source_var: %d", df['source_var'].nunique())
    logging.info("Distinct concept_key: %d", df.loc[filled, 'concept_key'].nunique())
    logging.info("Mapped rows: %d; left blank: %d", filled.sum(), len(df) - filled.sum())


if __name__ == "__main__":
    main()
