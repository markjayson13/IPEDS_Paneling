#!/usr/bin/env python3
"""
Build wide institution-year panels from the stitched long panel.

DuckDB is the relational execution layer for target discovery, lane-split
planning, scalar-conflict QC, discrete collapse, typed casting, partitioned
exports, and stitched outputs. Python remains responsible for CLI parsing,
target ordering, runtime setup, and artifact orchestration.
"""

from __future__ import annotations

from wide_build_common import build_arg_parser, setup_logging
from wide_build_duckdb import run


def main() -> None:
    parser = build_arg_parser()
    args = parser.parse_args()
    setup_logging(args.log_file)
    run(args)


if __name__ == "__main__":
    main()
