#!/usr/bin/env python3
"""Check SFA coverage across dictionary, crosswalk, Step0, and harmonized panels."""

from __future__ import annotations

from pathlib import Path
from typing import Iterable

import pandas as pd
import re

SFA_VAR_RX = re.compile(r"^(SFA|NPT)", re.IGNORECASE)
SURVEY_HINTS = ("SFA", "STUDENT FINANCIAL AID", "NET PRICE", "NET-PRICE")
VAR_COL_CANDIDATES = ["varname", "var_name", "var", "variable"]
SURVEY_COL_CANDIDATES = ["survey", "SURVEY", "component", "COMPONENT", "survey_label", "component_name"]

DICT_PATH = Path("/Users/markjaysonfarol13/Higher Ed research/IPEDS/Parquets/Dictionary/dictionary_lake.parquet")
SFA_STEP0_PATH = Path("/Users/markjaysonfarol13/Higher Ed research/IPEDS/Parquets/Unify/Step0sfa/sfa_step0_long.parquet")
SFA_CROSSWALK_FILLED = Path("/Users/markjaysonfarol13/Higher Ed research/IPEDS/Paneled Datasets/Crosswalks/Filled/sfa_crosswalk_filled.csv")
SFA_WIDE_PATH = Path("/Users/markjaysonfarol13/Higher Ed research/IPEDS/Parquets/Unify/SFAwide/sfa_concepts_wide.parquet")


def _resolve_column(df: pd.DataFrame, candidates: Iterable[str]) -> str | None:
    for col in candidates:
        if col in df.columns:
            return col
    return None


def normalize(series: pd.Series) -> pd.Series:
    return series.astype(str).str.strip().str.upper()


def main() -> None:
    if not DICT_PATH.exists():
        raise SystemExit(f"Dictionary lake not found: {DICT_PATH}")
    dict_df = pd.read_parquet(DICT_PATH)
    var_col = _resolve_column(dict_df, VAR_COL_CANDIDATES)
    if var_col is None:
        raise SystemExit("Dictionary lake missing varname column.")
    survey_col = _resolve_column(dict_df, SURVEY_COL_CANDIDATES)
    var_series = dict_df[var_col].astype(str)
    mask = var_series.str.match(SFA_VAR_RX, na=False)
    if survey_col:
        survey_values = dict_df[survey_col].astype(str).str.upper()
        mask |= survey_values.apply(lambda text: any(hint in text for hint in SURVEY_HINTS))
    dict_vars = set(normalize(var_series[mask]))
    print(f"SFA vars in dictionary: {len(dict_vars):,}")

    if not SFA_CROSSWALK_FILLED.exists():
        raise SystemExit(f"SFA crosswalk not found: {SFA_CROSSWALK_FILLED}")
    cw_df = pd.read_csv(SFA_CROSSWALK_FILLED)
    source_var_col = _resolve_column(cw_df, ["source_var", "SOURCE_VAR"])
    if source_var_col is None:
        raise SystemExit("SFA crosswalk missing source_var column.")
    cw_df["concept_key"] = cw_df["concept_key"].astype(str).str.strip()
    cw_df = cw_df[cw_df["concept_key"].ne("")]
    cw_vars = set(normalize(cw_df[source_var_col]))
    print(f"SFA vars with concept_key in crosswalk: {len(cw_vars):,}")
    missing_cw = sorted(dict_vars - cw_vars)
    print(f"SFA vars missing in crosswalk: {len(missing_cw):,}")
    if missing_cw:
        print("Sample missing in crosswalk:", missing_cw[:20])

    if not SFA_STEP0_PATH.exists():
        raise SystemExit(f"SFA Step0 long parquet not found: {SFA_STEP0_PATH}")
    step0_df = pd.read_parquet(SFA_STEP0_PATH, columns=["source_var"])
    step0_vars = set(normalize(step0_df["source_var"]))
    print(f"SFA vars present in Step0 long: {len(step0_vars):,}")
    missing_step0 = sorted(dict_vars - step0_vars)
    print(f"SFA vars missing in Step0 long: {len(missing_step0):,}")
    if missing_step0:
        print("Sample missing in Step0:", missing_step0[:20])

    if not SFA_WIDE_PATH.exists():
        raise SystemExit(f"SFA wide parquet not found: {SFA_WIDE_PATH}")
    wide_df = pd.read_parquet(SFA_WIDE_PATH)
    wide_cols = {col.strip().upper() for col in wide_df.columns}
    concept_keys = set(cw_df["concept_key"].astype(str).str.strip().str.upper())
    missing_wide = sorted(concept_keys - wide_cols)
    print(f"Concept keys defined in crosswalk: {len(concept_keys):,}")
    print(f"Concept keys missing as columns in SFAwide: {len(missing_wide):,}")
    if missing_wide:
        print("Sample missing in SFAwide:", missing_wide[:20])


if __name__ == "__main__":
    main()
