#!/usr/bin/env python3
"""
Auto-fill concept_key for HD/IC crosswalk based on varname and year ranges.

Usage (default paths):
    python auto_fill_hd_crosswalk.py

Or with explicit paths:
    python auto_fill_hd_crosswalk.py \
        --template "/Users/markjaysonfarol13/Higher Ed research/IPEDS/Paneled Datasets/Crosswalks/hd_crosswalk_template.csv" \
        --out "/Users/markjaysonfarol13/Higher Ed research/IPEDS/Paneled Datasets/Crosswalks/Filled/hd_crosswalk.csv"

This script:
  - Reads the template (with columns: concept_key, survey, varname, year_start, year_end, notes)
  - Fills concept_key where missing using:
      * Explicit mappings for core stable vars (INSTNM, SECTOR, STABBR, HBCU, TRIBAL, CONTROL, etc.)
      * Versioned Carnegie mappings (2005/2010/2015/2018/2021) when the relevant vars exist
      * Fallback concept_key = varname for everything else
  - Renames varname -> source_var for compatibility with stabilize_hd.py
  - Drops the obvious junk row "(Provisional release)"
  - Writes a clean hd_crosswalk.csv with non-empty concept_key for every row
"""

from __future__ import annotations

import argparse
from pathlib import Path
from typing import Dict, List, Tuple

import numpy as np
import pandas as pd

# --------------------------------------------------------------------
# Canonical paths (same base you use elsewhere)
# --------------------------------------------------------------------
DATA_ROOT = Path("/Users/markjaysonfarol13/Higher Ed research/IPEDS")
DEFAULT_CROSSWALK_DIR = DATA_ROOT / "Paneled Datasets" / "Crosswalks"
DEFAULT_TEMPLATE_PATH = DEFAULT_CROSSWALK_DIR / "hd_crosswalk_template.csv"
DEFAULT_FILLED_DIR = DEFAULT_CROSSWALK_DIR / "Filled"
DEFAULT_OUTPUT_PATH = DEFAULT_FILLED_DIR / "hd_crosswalk.csv"

# --------------------------------------------------------------------
# Explicit mappings for "stable" concepts
#   - Keys: raw varname (as in dictionary), case-insensitive
#   - Values: concept_key you want in the master panel
# --------------------------------------------------------------------
STABLE_INSTITUTION_NAME = "STABLE_INSTITUTION_NAME"
STABLE_CONTROL = "STABLE_CONTROL"
STABLE_SECTOR = "STABLE_SECTOR"
STABLE_STFIPS = "STABLE_STFIPS"
STABLE_HBCU = "STABLE_HBCU"
STABLE_TRIBAL = "STABLE_TRIBAL"

CARNEGIE_2005 = "CARNEGIE_2005"
CARNEGIE_2010 = "CARNEGIE_2010"
CARNEGIE_2015 = "CARNEGIE_2015"
CARNEGIE_2018 = "CARNEGIE_2018"
CARNEGIE_2021 = "CARNEGIE_2021"

EXACT_VAR_TO_CONCEPT: Dict[str, str] = {
    # Core stable vars used by the HD stabilizer
    "INSTNM": STABLE_INSTITUTION_NAME,
    "SECTOR": STABLE_SECTOR,
    "STABBR": STABLE_STFIPS,
    "HBCU": STABLE_HBCU,
    "TRIBAL": STABLE_TRIBAL,
    "CONTROL": STABLE_CONTROL,  # may not be present in your current template yet

    # You can add more here later, e.g.:
    # "LOCALE": "STABLE_LOCALE",
    # "DEGGRANT": "STABLE_DEGREE_GRANTING_STATUS",
}

# --------------------------------------------------------------------
# Carnegie mappings (for when you add these vars to the template)
# --------------------------------------------------------------------
CARNEGIE_SOURCE_VARS = {
    # When your dictionary lake actually surfaces these for HD/IC, they’ll hit here:
    "CARNEGIE",
    "CCBASIC",
    "CCBASIC15",
    "CCBASIC21",
}

# Year windows mapped to conceptual Carnegie versions.
# These ranges are inclusive and based on IPEDS / AIR documentation for Basic Classification:
#   - 2005/2010 basic: 2005-06 to 2014-15
#   - 2015 basic: starting 2015-16
#   - 2018 basic: starting 2018-19
#   - 2021 basic: starting 2021 update
CARNEGIE_VERSION_WINDOWS: List[Tuple[str, int, int]] = [
    (CARNEGIE_2005, 2005, 2010),  # Treat early years as 2005 basic
    (CARNEGIE_2010, 2011, 2014),
    (CARNEGIE_2015, 2015, 2017),
    (CARNEGIE_2018, 2018, 2020),
    (CARNEGIE_2021, 2021, 2100),
]


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Auto-fill HD/IC concept keys from crosswalk template.")
    p.add_argument(
        "--template",
        type=Path,
        default=DEFAULT_TEMPLATE_PATH,
        help="Path to hd_crosswalk_template.csv",
    )
    p.add_argument(
        "--out",
        type=Path,
        default=DEFAULT_OUTPUT_PATH,
        help="Output path for filled hd_crosswalk.csv (in the Filled directory).",
    )
    return p.parse_args()


def _load_template(path: Path) -> pd.DataFrame:
    if not path.exists():
        raise SystemExit(f"Template not found: {path}")
    df = pd.read_csv(path)
    # Normalize columns
    df.columns = [c.lower() for c in df.columns]
    required = {"survey", "varname", "year_start", "year_end"}
    missing = required - set(df.columns)
    if missing:
        raise SystemExit(f"Template missing required columns: {sorted(missing)}")

    # Cast years to numeric (Int64)
    df["year_start"] = pd.to_numeric(df["year_start"], errors="raise").astype("Int64")
    df["year_end"] = pd.to_numeric(df["year_end"], errors="raise").astype("Int64")

    # Normalize text fields
    df["survey"] = df["survey"].astype(str).str.upper()
    df["varname"] = df["varname"].astype(str).str.strip()

    # Ensure concept_key/notes exist
    if "concept_key" not in df.columns:
        df["concept_key"] = np.nan
    if "notes" not in df.columns:
        df["notes"] = np.nan

    # Drop obvious junk row: "(Provisional release)"
    mask_junk = df["varname"].eq("(Provisional release)")
    if mask_junk.any():
        df = df.loc[~mask_junk].copy()

    return df


def _split_carnegie_row(row: pd.Series) -> List[dict]:
    """
    Given a template row for a Carnegie source var (e.g. CARNEGIE),
    split its year range into separate rows for each version window,
    intersecting the row's [year_start, year_end] with each version window.
    """
    results: List[dict] = []
    y0 = int(row["year_start"])
    y1 = int(row["year_end"])
    for version_key, v_start, v_end in CARNEGIE_VERSION_WINDOWS:
        start = max(y0, v_start)
        end = min(y1, v_end)
        if start <= end:
            results.append(
                {
                    "concept_key": version_key,
                    "survey": row["survey"],
                    "source_var": row["varname"],
                    "year_start": start,
                    "year_end": end,
                    "notes": row.get("notes", np.nan),
                }
            )
    return results


def _auto_fill_concepts(df: pd.DataFrame) -> pd.DataFrame:
    """
    Build a new crosswalk DataFrame with:
      - concept_key set for every row
      - varname renamed to source_var
      - possible splitting of Carnegie rows into multiple versioned rows
    """
    rows: List[dict] = []

    for _, row in df.iterrows():
        var = str(row["varname"]).strip()
        survey = row["survey"]
        year_start = int(row["year_start"])
        year_end = int(row["year_end"])
        notes = row.get("notes", np.nan)

        # Respect any pre-filled concept_key (if you manually edit some in the template)
        existing_ck = str(row.get("concept_key", "")).strip()
        if existing_ck not in ("", "nan", "None"):
            rows.append(
                {
                    "concept_key": existing_ck,
                    "survey": survey,
                    "source_var": var,
                    "year_start": year_start,
                    "year_end": year_end,
                    "notes": notes,
                }
            )
            continue

        var_upper = var.upper()

        # Special handling for Carnegie variables (split into version windows)
        if var_upper in CARNEGIE_SOURCE_VARS:
            rows.extend(_split_carnegie_row(row))
            continue

        # Explicit stable variable mappings
        if var_upper in EXACT_VAR_TO_CONCEPT:
            concept_key = EXACT_VAR_TO_CONCEPT[var_upper]
        else:
            # Fallback: use the raw varname as the concept key
            # This makes every column available in the wide HD panel,
            # while the stabilizer only applies special propagation rules to STABLE_* and CARNEGIE_*.
            concept_key = var_upper

        rows.append(
            {
                "concept_key": concept_key,
                "survey": survey,
                "source_var": var,  # keep original spelling as source_var
                "year_start": year_start,
                "year_end": year_end,
                "notes": notes,
            }
        )

    out = pd.DataFrame(rows)

    # Basic sanity checks
    if out["concept_key"].isna().any() or (out["concept_key"].astype(str).str.strip() == "").any():
        bad = out.loc[out["concept_key"].astype(str).str.strip().isin(["", "nan", "None"])].head()
        raise ValueError(
            "Auto-filled crosswalk still has empty concept_key rows; this should not happen.\n"
            f"Example rows:\n{bad.to_string(index=False)}"
        )

    # No need to enforce uniqueness here; stabilize_hd.py will check that after expansion.
    # Just ensure dtypes are reasonable
    out["year_start"] = pd.to_numeric(out["year_start"], errors="raise").astype("Int64")
    out["year_end"] = pd.to_numeric(out["year_end"], errors="raise").astype("Int64")

    # Order columns for compatibility with stabilize_hd.py
    col_order = ["concept_key", "survey", "source_var", "year_start", "year_end", "notes"]
    out = out[col_order]

    # Sort for readability
    out = out.sort_values(["survey", "source_var", "year_start", "concept_key"], ignore_index=True)

    return out


def main() -> None:
    args = parse_args()
    template = _load_template(args.template)

    print(f"Loaded template from {args.template} with {len(template):,} rows.")

    crosswalk = _auto_fill_concepts(template)

    args.out.parent.mkdir(parents=True, exist_ok=True)
    crosswalk.to_csv(args.out, index=False)
    print(f"Wrote auto-filled HD crosswalk to {args.out}")
    print(f"Rows: {len(crosswalk):,}")


if __name__ == "__main__":
    main()
