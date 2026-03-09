#!/usr/bin/env python3
"""
Build a wide institution-year panel from the stitched long panel.

DuckDB is the relational execution layer. Python remains responsible for
CLI parsing, target ordering, and artifact orchestration.
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
