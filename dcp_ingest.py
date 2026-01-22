#!/usr/bin/env python3
"""
Ingest the Delta Cost Project IPEDS Analytics CSVs into a single Parquet file.

Input:
  /Users/markjaysonfarol13/IPEDS_Paneling/dcp_database/IPEDS_Analytics_DCP_87_12_CSV
    - delta_public_87_99.csv
    - delta_public_00_12.csv

Output:
  /Users/markjaysonfarol13/IPEDS_Paneling/Panels/panel_dcp_1987_2012.parquet
  /Users/markjaysonfarol13/IPEDS_Paneling/Checks/dcp_profile.csv

Notes:
  - This does not rename columns; it preserves the DCP variable names as-is.
  - Downstream harmonization should map these DCP columns to your target vars.
"""

from __future__ import annotations

import argparse
from pathlib import Path

import pandas as pd


BASE_ROOT = Path("/Users/markjaysonfarol13/IPEDS_Paneling")
DCP_DIR = BASE_ROOT / "dcp_database" / "IPEDS_Analytics_DCP_87_12_CSV"
OUTPUT_PARQUET = BASE_ROOT / "Panels" / "panel_dcp_1987_2012.parquet"
PROFILE_CSV = BASE_ROOT / "Checks" / "dcp_profile.csv"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--dcp-dir",
        type=Path,
        default=DCP_DIR,
        help="Directory containing delta_public_*.csv (default: %(default)s)",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=OUTPUT_PARQUET,
        help="Output parquet path (default: %(default)s)",
    )
    parser.add_argument(
        "--profile",
        type=Path,
        default=PROFILE_CSV,
        help="Output CSV with row/column counts (default: %(default)s)",
    )
    return parser.parse_args()


def load_dcp_frames(dcp_dir: Path) -> pd.DataFrame:
    paths = [
        dcp_dir / "delta_public_87_99.csv",
        dcp_dir / "delta_public_00_12.csv",
    ]
    frames = []
    for path in paths:
        if not path.exists():
            raise FileNotFoundError(f"Missing DCP file: {path}")
        # Files use mixed encodings; fall back to latin-1 and ignore errors to keep all rows.
        for encoding in ("utf-8", "latin-1"):
            try:
                df = pd.read_csv(path, dtype=str, low_memory=False, encoding=encoding, encoding_errors="ignore")
                break
            except UnicodeDecodeError:
                continue
        else:
            raise UnicodeDecodeError(f"Could not decode {path} with utf-8 or latin-1")
        frames.append(df)
    return pd.concat(frames, ignore_index=True)


def main() -> None:
    args = parse_args()
    dcp_dir = args.dcp_dir
    output = args.output
    profile_path = args.profile

    df = load_dcp_frames(dcp_dir)
    output.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(output, index=False, compression="snappy")
    print(f"Wrote {len(df):,} rows to {output}")

    profile_path.parent.mkdir(parents=True, exist_ok=True)
    profile = pd.DataFrame(
        {
            "rows": [len(df)],
            "cols": [len(df.columns)],
            "source": ["DCP 1987-2012"],
        }
    )
    profile.to_csv(profile_path, index=False)
    print(f"Profile written to {profile_path}")


if __name__ == "__main__":
    main()
