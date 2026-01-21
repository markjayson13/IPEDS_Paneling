#!/usr/bin/env python3
from __future__ import annotations

import argparse
import logging
from pathlib import Path
from typing import Dict, Iterable, List

import pandas as pd

CROSSWALK_DIR = Path("/Users/markjaysonfarol13/IPEDS_Paneling/Panels/Crosswalks")
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


def expand_vars(prefixes: Iterable[str], suffixes: Iterable[str] = ("0", "1", "2", "3")) -> list[str]:
    """Return list of prefix+suffix combinations for convenience."""
    return [f"{p}{s}" for p in prefixes for s in suffixes]


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
        "ICAY_T_IND": expand_vars(["PCCHG1AT", "CHG1AT"]) + ["TUITION1"],
        "ICAY_T_STATE": expand_vars(["PCCHG2AT", "CHG2AT"]) + ["TUITION2"],
        "ICAY_T_OUTST": expand_vars(["PCCHG3AT", "CHG3AT"]) + ["TUITION3"],
        # Fees
        "ICAY_F_IND": expand_vars(["PCCHG1AF", "CHG1AF"]) + ["FEE1"],
        "ICAY_F_STATE": expand_vars(["PCCHG2AF", "CHG2AF"]) + ["FEE2"],
        "ICAY_F_OUTST": expand_vars(["PCCHG3AF", "CHG3AF"]) + ["FEE3"],
        # Tuition + fees
        "ICAY_TF_IND": expand_vars(["PCCHG1AY", "CHG1AY"]),
        "ICAY_TF_STATE": expand_vars(["PCCHG2AY", "CHG2AY"]),
        "ICAY_TF_OUTST": expand_vars(["PCCHG3AY", "CHG3AY"]),
        "ICAY_TOT_PY": expand_vars(["PCCHG1PY", "CHG1PY"]),
        # Books and supplies
        "ICAY_BOOKSUPP": expand_vars(["CHG4AY", "CHG4PY", "PCCHG4AY", "PCCHG4PY"]),
        # Room/board on campus
        "ICAY_ONCRMBRD": expand_vars(["CHG5AY", "CHG5PY", "PCCHG5AY", "PCCHG5PY"]) + ["RMBRDAMT", "BOARDAMT"],
        # Other on campus
        "ICAY_ONCOTHEXP": expand_vars(["CHG6AY", "CHG6PY", "PCCHG6AY", "PCCHG6PY"]),
        # Room/board off campus not with family
        "ICAY_OFFCRMBRD": expand_vars(["CHG7AY", "CHG7PY", "PCCHG7AY", "PCCHG7PY"]),
        # Other off campus not with family
        "ICAY_OFFCOTHEXP": expand_vars(["CHG8AY", "CHG8PY", "PCCHG8AY", "PCCHG8PY"]),
        # Other off campus with family
        "ICAY_OFFCFOTHEXP": expand_vars(["CHG9AY", "CHG9PY", "PCCHG9AY", "PCCHG9PY"]),
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
