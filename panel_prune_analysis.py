#!/usr/bin/env python3
import argparse
from pathlib import Path

import pandas as pd

PANEL_WIDE = Path("/Users/markjaysonfarol13/Higher Ed research/IPEDS/Paneled Datasets/Final/panel_wide.csv")
PANEL_WIDE_CLEANROBUST = Path(
    "/Users/markjaysonfarol13/Higher Ed research/IPEDS/Paneled Datasets/Final/panel_wide_cleanrobust.csv"
)


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Prune HD variables (and later other components) for analysis panel.")
    p.add_argument(
        "--input",
        type=Path,
        default=PANEL_WIDE,
        help="Path to full wide panel CSV (panel_wide.csv).",
    )
    p.add_argument(
        "--output",
        type=Path,
        default=PANEL_WIDE_CLEANROBUST,
        help="Path to write pruned analysis panel CSV (panel_wide_cleanrobust.csv).",
    )
    return p.parse_args()


DROP_LATE_INTRO_COLS = {
    # ICAY: alternative tuition payment plan fields introduced mid-period
    "ICAY__ICAY_ALTPLAN_ANY",
    "ICAY__ICAY_ALTPLAN_GUARANTEE",
    "ICAY__ICAY_ALTPLAN_OTHER",
    "ICAY__ICAY_ALTPLAN_PAYMENT",
    "ICAY__ICAY_ALTPLAN_PREPAID",
    # ICAY: professional tuition variance (late intro)
    "ICAY__ICAY_TUIT_PROF_FT_VAR_DOCPPSP",
    # SFA: late-introduced grant/loan variants
    "SFA__SFA_VAR_AGRNT_A",
    "SFA__SFA_VAR_AGRNT_N",
    "SFA__SFA_VAR_AGRNT_P",
    "SFA__SFA_VAR_AGRNT_T",
    "SFA__SFA_VAR_FGRNT_T",
    "SFA__SFA_VAR_FLOAN_A",
    "SFA__SFA_VAR_FLOAN_N",
    "SFA__SFA_VAR_FLOAN_P",
    "SFA__SFA_VAR_FLOAN_T",
    # SFA: GIS4A series introduced mid-period
    "SFA__SFA_VAR_GIS4A0",
    "SFA__SFA_VAR_GIS4A10",
}

def _is_sfa_flag(col: str) -> bool:
    """Return True for SFA flag/indicator columns we want to drop."""
    if not isinstance(col, str):
        return False
    if not col.startswith("SFA__"):
        return False
    upper = col.upper()
    return "_FLAG" in upper or upper.endswith("_IND") or upper.endswith("_STATUS")


# HD variables to retain in the analysis panel
KEEP_HD_COLS = [
    # Stable identity/grouping
    "HD__STABLE_INSTITUTION_NAME",
    "HD__STABLE_CONTROL",
    "HD__STABLE_SECTOR",
    "HD__STABLE_STFIPS",
    "HD__STABLE_HBCU",
    "HD__STABLE_TRIBAL",
    "HD__STABLE_PRNTCHLD_STATUS",
    # Carnegie/classification
    "HD__CARNEGIE_2005",
    "HD__C15BASIC",
    "HD__C18BASIC",
    "HD__C21BASIC",
    "HD__INSTCAT",
    "HD__INSTSIZE",
    # Degree-granting / postsecondary status
    "HD__DEGGRANT",
    "HD__POSTSEC",
    "HD__PSEFLAG",
    "HD__PSET4FLG",
    # Locale & geography
    "HD__LOCALE",
    "HD__FIPS",
    "HD__CBSA",
    "HD__CBSATYPE",
    "HD__CSA",
    "HD__COUNTYCD",
    "HD__COUNTYNM",
    "HD__NECTA",
    # Structural institutional flags
    "HD__LANDGRNT",
    "HD__HOSPITAL",
    "HD__MEDICAL",
    # OPEID / eligibility fields
    "HD__OPEID",
    "HD__OPEFLAG",
]

INSTITUTION_NAME_CANDIDATES = [
    "HD__STABLE_INSTITUTION_NAME",
    "HD__INSTNM",
    "INSTNM",
]

UNITID_FLAG_CANDIDATES = [
    "REPORTING_UNITID",
    "STABLE_PRNTCHLD_STATUS",
    "HD__STABLE_PRNTCHLD_STATUS",
    "HD__MERGESTAT",
    "HD__MERGFLAG",
    "HD__ACTIVE",
    "HD__PSEFLAG",
    "HD__PSET4FLG",
    "HD__POSTSEC",
]

OPEID_CANDIDATES = [
    "HD__OPEID",
    "OPEID",
]

OPEID_FLAG_CANDIDATES = [
    "HD__OPEFLAG",
    "HD__T4ELIG",
]

COMPONENT_PREFIX_ORDER = [
    "HD__",
    "ICAY__",
    "SFA__",
    "ADM__",
    "ENROLL__",
    "EF__",
    "E12__",
    "FIN__",
]


def main() -> None:
    args = parse_args()

    if not args.input.exists():
        raise SystemExit(f"Input file not found: {args.input}")

    print(f"[INFO] Loading full panel from {args.input}")
    df = pd.read_csv(args.input)
    cols = list(df.columns)

    drop_cols = set(DROP_LATE_INTRO_COLS)
    sfa_flag_drops = [c for c in cols if _is_sfa_flag(c)]
    drop_cols.update(sfa_flag_drops)

    present_drops = [c for c in drop_cols if c in cols]
    if present_drops:
        print(f"[INFO] Dropping {len(present_drops)} columns for balance/flags: {present_drops}")

    # Always retain key identifiers so downstream grouping logic keeps working
    base_keep = []
    for key in ["UNITID", "YEAR"]:
        if key in cols:
            base_keep.append(key)
    for key in ["REPORTING_UNITID", "STABLE_PRNTCHLD_STATUS"]:
        if key in cols:
            base_keep.append(key)

    keep_cols = set(base_keep)

    # Keep non-prefixed columns (anything without "__")
    for c in cols:
        if "__" not in c and c not in drop_cols:
            keep_cols.add(c)

    # HD pruning: only keep curated HD columns
    for c in cols:
        if c.startswith("HD__") and c in KEEP_HD_COLS and c not in drop_cols:
            keep_cols.add(c)

    # Keep all other prefixed components untouched for now
    for c in cols:
        if c in drop_cols:
            continue
        if c.startswith(("EF__", "SFA__", "FIN__", "ADM__", "ICAY__")):
            keep_cols.add(c)
        elif "__" in c and not c.startswith("HD__"):
            keep_cols.add(c)

    def add_if_present(target: list[str], column: str) -> None:
        if column in keep_cols and column not in target:
            target.append(column)

    priority: list[str] = []
    add_if_present(priority, "UNITID")
    add_if_present(priority, "YEAR")
    for name_col in INSTITUTION_NAME_CANDIDATES:
        add_if_present(priority, name_col)
    for unit_flag in UNITID_FLAG_CANDIDATES:
        add_if_present(priority, unit_flag)
    for ope_col in OPEID_CANDIDATES:
        add_if_present(priority, ope_col)
    for ope_flag in OPEID_FLAG_CANDIDATES:
        add_if_present(priority, ope_flag)

    ordered_keep: list[str] = []
    ordered_keep.extend(priority)

    for prefix in COMPONENT_PREFIX_ORDER:
        for c in cols:
            if c in keep_cols and c not in ordered_keep and c.startswith(prefix):
                ordered_keep.append(c)

    for c in cols:
        if c in keep_cols and c not in ordered_keep:
            ordered_keep.append(c)

    pruned = df[ordered_keep].copy()

    args.output.parent.mkdir(parents=True, exist_ok=True)
    pruned.to_csv(args.output, index=False)

    print(f"[INFO] Wrote pruned analysis panel to {args.output}")
    print(f"[INFO] Kept {len(ordered_keep)} columns out of {len(cols)} total.")
    print(f"[INFO] First 12 columns in analysis panel: {ordered_keep[:12]}")

    def prefix_stats(prefix: str) -> str:
        total = sum(c.startswith(prefix) for c in cols)
        kept = sum(c.startswith(prefix) for c in ordered_keep)
        return f"{kept}/{total}"

    print("[INFO] Per-prefix kept/total:")
    for prefix in ["HD__", "EF__", "SFA__", "FIN__", "ADM__", "ICAY__"]:
        print(f"  {prefix}: {prefix_stats(prefix)}")


if __name__ == "__main__":
    main()
