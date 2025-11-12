#!/usr/bin/env python3
"""Assemble raw IPEDS CSV/XLS files into a long-form panel without concept mapping."""

from __future__ import annotations

import argparse
import logging
import re
import sys
from pathlib import Path
from typing import Iterator, Optional, Set

import pandas as pd

SUPPORTED_SUFFIXES = {".csv", ".tsv", ".txt", ".xlsx", ".xls"}
SURVEY_PATTERNS: list[tuple[str, list[str]]] = [
    ("IC_AY", [r"IC\d{4}_AY", r"ICAY", r"IC[A-Z0-9]*AY"]),
    ("IC_PY", [r"IC\d{4}_PY", r"ICPY", r"IC[A-Z0-9]*PY"]),
    ("EFIA", [r"EFIA"]),
    ("E1D", [r"E1D"]),
    ("EFFY", [r"EFFY"]),
    ("GR200", [r"GR200"]),
    ("F1A", [r"F\d{4}_F1A", r"_F1A"]),
    ("F2A", [r"F\d{4}_F2A", r"_F2A"]),
    ("F3A", [r"F\d{4}_F3A", r"_F3A"]),
    ("HD", [r"HD\d{4}"]),
    ("IC", [r"IC\d{4}"]),
    ("EF", [r"EF\d{4}[ABCD]", r"EF\d{4}(RET|DIST)", r"EFDIST", r"EFRET"]),
    ("E12", [r"E12\d{4}"]),
    ("SFA", [r"SFA\d{4}"]),
    ("ADM", [r"ADM\d{4}"]),
    ("GR", [r"GR\d{4}"]),
]


def infer_year(path: Path) -> int | None:
    for parent in path.parents:
        if parent.name.isdigit():
            return int(parent.name)
    return None


def infer_survey(path: Path) -> str:
    stem = path.stem.upper()
    for survey, patterns in SURVEY_PATTERNS:
        for pattern in patterns:
            if re.search(pattern, stem):
                return survey
    return "UNK"


def find_unitid_column(df: pd.DataFrame) -> str | None:
    candidates = {
        col
        for col in df.columns
        if isinstance(col, str) and col.strip().upper().startswith("UNITID")
    }
    if not candidates:
        return None
    return sorted(candidates, key=lambda c: (len(c), c))[0]


def parse_years(expr: Optional[str]) -> Optional[Set[int]]:
    if not expr:
        return None
    years: Set[int] = set()
    for part in expr.split(","):
        token = part.strip()
        if not token:
            continue
        if "-" in token:
            start, end = token.split("-", 1)
            start_i, end_i = int(start), int(end)
            lo, hi = sorted((start_i, end_i))
            years.update(range(lo, hi + 1))
        else:
            years.add(int(token))
    return years if years else None


def parse_surveys(expr: Optional[str]) -> Optional[Set[str]]:
    if not expr:
        return None
    surveys = {token.strip().upper() for token in expr.split(",") if token.strip()}
    return surveys if surveys else None


def iter_data_files(root: Path, years: Optional[Set[int]], surveys: Optional[Set[str]]) -> Iterator[tuple[Path, int, str]]:
    for path in root.rglob("*"):
        if not path.is_file():
            continue
        if path.suffix.lower() not in SUPPORTED_SUFFIXES:
            continue
        # Skip dictionaries/documentation directories
        if any("dict" in part.lower() for part in path.parts):
            continue
        year = infer_year(path)
        if years is not None and year not in years:
            continue
        survey = infer_survey(path)
        if surveys is not None and survey not in surveys:
            continue
        yield path, year, survey


def read_table(path: Path) -> pd.DataFrame | None:
    suffix = path.suffix.lower()
    try:
        if suffix == ".csv":
            try:
                return pd.read_csv(path, dtype=str, encoding_errors="ignore", on_bad_lines="skip")
            except Exception:
                return pd.read_csv(path, dtype=str, engine="python", encoding_errors="ignore", on_bad_lines="skip")
        if suffix == ".tsv":
            return pd.read_csv(path, dtype=str, sep="\t", encoding_errors="ignore", on_bad_lines="skip")
        if suffix == ".txt":
            try:
                return pd.read_csv(path, dtype=str, sep=None, engine="python", encoding_errors="ignore", on_bad_lines="skip")
            except Exception:
                return pd.read_csv(path, dtype=str, delim_whitespace=True, encoding_errors="ignore", on_bad_lines="skip")
        if suffix in {".xlsx", ".xls"}:
            return pd.read_excel(path, dtype=str)
    except Exception as exc:  # noqa: BLE001
        logging.warning("Failed to read %s (%s)", path, exc)
        return None
    return None


def build_raw_panel(root: Path, years: Optional[Set[int]], surveys: Optional[Set[str]]) -> pd.DataFrame:
    frames: list[pd.DataFrame] = []
    for file_path, year, survey in iter_data_files(root, years, surveys):
        if year is None:
            logging.debug("Skipping %s (no year folder)", file_path)
            continue
        df = read_table(file_path)
        if df is None or df.empty:
            continue
        unitid_col = find_unitid_column(df)
        if not unitid_col:
            logging.debug("Skipping %s (UNITID column missing)", file_path)
            continue
        df = df.copy()
        df[unitid_col] = pd.to_numeric(df[unitid_col], errors="coerce")
        df.dropna(subset=[unitid_col], inplace=True)
        if df.empty:
            continue
        value_cols = [c for c in df.columns if c != unitid_col]
        if not value_cols:
            continue
        melted = df.melt(id_vars=[unitid_col], value_vars=value_cols, var_name="source_var", value_name="value")
        melted.dropna(subset=["source_var"], inplace=True)
        melted.rename(columns={unitid_col: "UNITID"}, inplace=True)
        melted["year"] = year
        melted["survey"] = survey
        melted["source_file"] = str(file_path)
        frames.append(melted)
    if not frames:
        raise RuntimeError("No usable data files found under root.")
    panel = pd.concat(frames, ignore_index=True)
    panel["UNITID"] = panel["UNITID"].astype("Int64")
    panel["reporting_unitid"] = panel["UNITID"]
    return panel


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Assemble raw IPEDS files into a long-form panel")
    parser.add_argument("--root", type=Path, required=True, help="Root directory containing year folders")
    parser.add_argument("--output", type=Path, required=True, help="Parquet output path")
    parser.add_argument("--years", type=str, default=None, help="Comma list or ranges (e.g., 2004,2006-2008)")
    parser.add_argument("--surveys", type=str, default=None, help="Comma list of survey tokens (HD,IC,IC_AY,EF,E12,ADM,SFA,FIN,GR)")
    parser.add_argument("--log-level", default="INFO", help="Logging level")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    logging.basicConfig(level=getattr(logging, args.log_level.upper(), logging.INFO), format="%(levelname)s %(message)s")
    if not args.root.exists():
        logging.error("Root directory %s not found", args.root)
        return 1
    years = parse_years(args.years)
    surveys = parse_surveys(args.surveys)
    if surveys:
        logging.info("Restricting to surveys: %s", ",".join(sorted(surveys)))
    if years:
        logging.info("Restricting to years: %s", ",".join(str(y) for y in sorted(years)))
    panel = build_raw_panel(args.root, years, surveys)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    panel.to_parquet(args.output, index=False)
    logging.info("Wrote %s rows to %s", len(panel), args.output)
    return 0


if __name__ == "__main__":
    sys.exit(main())
