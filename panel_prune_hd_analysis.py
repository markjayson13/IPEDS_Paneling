#!/usr/bin/env python3
import argparse
from pathlib import Path

import pandas as pd

PANEL_WIDE = Path("/Users/markjaysonfarol13/Higher Ed research/IPEDS/Paneled Datasets/Final/panel_wide.csv")
PANEL_WIDE_CLEANROBUST = Path(
    "/Users/markjaysonfarol13/Higher Ed research/IPEDS/Paneled Datasets/Final/panel_wide_cleanrobust.csv"
)

KEEP_HD_COLS = [
    "HD__STABLE_INSTITUTION_NAME",
    "HD__STABLE_CONTROL",
    "HD__STABLE_SECTOR",
    "HD__STABLE_STFIPS",
    "HD__STABLE_HBCU",
    "HD__STABLE_TRIBAL",
    "HD__STABLE_PRNTCHLD_STATUS",
    "HD__CARNEGIE_2005",
    "HD__INSTCAT",
    "HD__INSTSIZE",
    "HD__DEGGRANT",
    "HD__POSTSEC",
    "HD__PSEFLAG",
    "HD__PSET4FLG",
    "HD__LOCALE",
    "HD__FIPS",
    "HD__CBSA",
    "HD__CBSATYPE",
    "HD__CSA",
    "HD__COUNTYCD",
    "HD__COUNTYNM",
    "HD__NECTA",
    "HD__C15BASIC",
    "HD__C18BASIC",
    "HD__C21BASIC",
]


def main() -> None:
    parser = argparse.ArgumentParser(description="Prune HD variables for analysis panel.")
    parser.add_argument("--input", type=Path, default=PANEL_WIDE, help="Full wide panel CSV path.")
    parser.add_argument(
        "--output",
        type=Path,
        default=PANEL_WIDE_CLEANROBUST,
        help="Pruned analysis panel CSV path.",
    )
    args = parser.parse_args()

    if not args.input.exists():
        raise SystemExit(f"Input file not found: {args.input}")

    print(f"[INFO] Loading full panel from {args.input}")
    df = pd.read_csv(args.input)
    cols = list(df.columns)

    base_keep = [c for c in ["UNITID", "YEAR", "REPORTING_UNITID", "STABLE_PRNTCHLD_STATUS"] if c in cols]
    keep_cols = set(base_keep)
    for c in cols:
        if not c.startswith("HD__"):
            keep_cols.add(c)
    for c in KEEP_HD_COLS:
        if c in cols:
            keep_cols.add(c)

    ordered_keep = [c for c in cols if c in keep_cols]
    pruned = df[ordered_keep].copy()

    args.output.parent.mkdir(parents=True, exist_ok=True)
    pruned.to_csv(args.output, index=False)
    print(f"[INFO] Wrote pruned analysis panel to {args.output}")
    print(f"[INFO] Kept {len(ordered_keep)} columns out of {len(cols)} total.")


if __name__ == "__main__":
    main()
