#!/usr/bin/env python3
"""
Combine yearly Step 0 finance parquet files into a single long file.
"""

from __future__ import annotations

import argparse
import logging
from pathlib import Path

import pandas as pd

DEFAULT_INPUT_DIR = Path("/Users/markjaysonfarol13/Higher Ed research/IPEDS/Parquets/Unify/Step0Finlong")
DEFAULT_PATTERN = "Step0Finlong_*.parquet"
DEFAULT_OUTPUT = DEFAULT_INPUT_DIR / "finance_step0_long.parquet"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input-dir", type=Path, default=DEFAULT_INPUT_DIR, help="Directory containing per-year Step0 parquets")
    parser.add_argument("--pattern", default=DEFAULT_PATTERN, help="Glob pattern of per-year files within input-dir")
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT, help="Destination combined parquet path")
    parser.add_argument("--log-level", default="INFO")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    logging.basicConfig(level=getattr(logging, args.log_level.upper(), logging.INFO))

    input_dir = args.input_dir
    if not input_dir.exists():
        raise SystemExit(f"Input directory not found: {input_dir}")

    files = sorted(input_dir.glob(args.pattern))
    if not files:
        raise SystemExit(f"No files matching pattern {args.pattern} found in {input_dir}")

    logging.info("Combining %s files from %s", len(files), input_dir)
    frames = []
    for path in files:
        logging.info("Reading %s", path)
        frames.append(pd.read_parquet(path))

    combined = pd.concat(frames, ignore_index=True)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    combined.to_parquet(args.output, index=False)
    logging.info("Wrote combined parquet to %s", args.output)


if __name__ == "__main__":
    main()
