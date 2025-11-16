#!/usr/bin/env python3
"""Melt Admissions variables from panel-wide files into a canonical long format."""

from __future__ import annotations

import argparse
import logging
import re
from pathlib import Path
from typing import Iterable

import pandas as pd

ADMISSIONS_VAR_SEEDS = {
    "APPLCN",
    "APPLCNM",
    "APPLCNW",
    "ADMSSN",
    "ADMSSNM",
    "ADMSSNW",
    "ENRLT",
    "ENRLTM",
    "ENRLTW",
    "SATNUM",
    "ACTNUM",
    "SATVR25",
    "SATVR75",
    "SATMT25",
    "SATMT75",
    "SATWR25",
    "SATWR75",
    "ACTCM25",
    "ACTCM75",
    "ACTEN25",
    "ACTEN75",
    "ACTMT25",
    "ACTMT75",
    "ACTWR25",
    "ACTWR75",
}

SURVEY_FILTER = {"IC", "ADM"}

PREFIX_YEAR_RE = re.compile(r"^(?P<survey>[A-Z]{2,5})(?P<year>\d{4})[_-]?(?P<var>[A-Z0-9_]+)$")
SUFFIX_YEAR_RE = re.compile(r"^(?P<var>[A-Z0-9_]+)[_-](?P<survey>[A-Z]{2,5})(?P<year>\d{4})$")
SURVEY_VAR_RE = re.compile(r"^(?P<survey>[A-Z]{2,5})[_-](?P<var>[A-Z0-9_]+)$")

DEFAULT_DICT = Path("/Users/markjaysonfarol13/Higher Ed research/IPEDS/Parquets/Dictionary/dictionary_lake.parquet")
DEFAULT_OUTPUT = Path(
    "/Users/markjaysonfarol13/Higher Ed research/IPEDS/Parquets/Unify/Step0adm/adm_step0_long.parquet"
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Extract Admissions variables from panel-wide files and output a unified long parquet. "
            "The script is agnostic to the IC→ADM migration and simply searches for canonical varnames."
        )
    )
    parser.add_argument("--panel-dir", type=Path, required=True, help="Directory with panel_wide_raw_{YEAR}_merged files")
    parser.add_argument("--year-start", type=int, default=2004)
    parser.add_argument("--year-end", type=int, default=2024)
    parser.add_argument("--dictionary-lake", type=Path, default=DEFAULT_DICT)
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    parser.add_argument("--log-level", default="INFO")
    return parser.parse_args()


def resolve_column(df: pd.DataFrame, candidates: Iterable[str]) -> str:
    for candidate in candidates:
        if candidate in df.columns:
            return candidate
    raise KeyError(f"None of the expected columns are present: {candidates}")


def load_dictionary(path: Path) -> tuple[pd.DataFrame, str, str | None]:
    if not path.exists():
        raise FileNotFoundError(f"Dictionary lake not found: {path}")
    lake = pd.read_parquet(path)
    var_col = resolve_column(lake, ["varname", "source_var", "VAR_NAME", "variable", "var"])
    survey_col = None
    for candidate in ("survey", "SURVEY", "component", "survey_component"):
        if candidate in lake.columns:
            survey_col = candidate
            break
    return lake, var_col, survey_col


def select_admissions_varcodes(lake: pd.DataFrame, var_col: str, survey_col: str | None) -> set[str]:
    df = lake.copy()
    df[var_col] = df[var_col].astype(str).str.strip().str.upper()
    if survey_col:
        df[survey_col] = df[survey_col].astype(str).str.strip().str.upper()
    mask = df[var_col].isin(ADMISSIONS_VAR_SEEDS)
    if survey_col:
        mask &= df[survey_col].isin(SURVEY_FILTER)
    subset = df.loc[mask, var_col]
    varcodes = set(subset.dropna().unique())
    if not varcodes and survey_col:
        logging.warning(
            "No admissions varcodes found with SURVEY_FILTER=%s. Relaxing survey filter.",
            ",".join(sorted(SURVEY_FILTER)),
        )
        mask = df[var_col].isin(ADMISSIONS_VAR_SEEDS)
        subset = df.loc[mask, var_col]
        varcodes = set(subset.dropna().unique())
    if not varcodes:
        raise SystemExit("No admissions varcodes found in dictionary_lake. Check dictionary filters.")
    return varcodes


def locate_panel_file(panel_dir: Path, year: int) -> Path | None:
    candidates = [
        panel_dir / f"panel_wide_raw_{year}_merged.parquet",
        panel_dir / f"panel_wide_raw_{year}_merged.csv",
        panel_dir / f"panel_wide_raw_{year}.parquet",
        panel_dir / f"panel_wide_raw_{year}.csv",
    ]
    for path in candidates:
        if path.exists():
            return path
    return None


def read_panel(path: Path) -> pd.DataFrame:
    if path.suffix.lower() == ".parquet":
        return pd.read_parquet(path)
    if path.suffix.lower() == ".csv":
        return pd.read_csv(path, dtype=str, low_memory=False)
    raise ValueError(f"Unsupported panel format: {path}")


def match_column_to_var(column: str, varcodes: set[str]) -> str | None:
    name = str(column).strip().upper()
    if name in varcodes:
        return name
    for pattern in (PREFIX_YEAR_RE, SUFFIX_YEAR_RE, SURVEY_VAR_RE):
        match = pattern.match(name)
        if not match:
            continue
        survey = match.group("survey").strip("_").upper()
        candidate = match.group("var").strip("_").upper()
        if survey in SURVEY_FILTER and candidate in varcodes:
            return candidate
    return None


def build_long_for_year(df: pd.DataFrame, year: int, varcodes: set[str]) -> pd.DataFrame | None:
    data = df.copy()
    data.columns = pd.Index(str(c).strip().upper() for c in data.columns)
    if "UNITID" not in data.columns:
        raise RuntimeError(f"UNITID column missing for YEAR={year}")
    if "YEAR" not in data.columns:
        data["YEAR"] = year
    else:
        data["YEAR"] = pd.to_numeric(data["YEAR"], errors="coerce")
        non_missing = data["YEAR"].dropna().unique()
        if len(non_missing) == 0:
            logging.warning("All YEAR values missing in panel for nominal YEAR=%s; overwriting with nominal year.", year)
            data["YEAR"] = year
        elif len(non_missing) > 1:
            raise RuntimeError(f"Multiple YEAR values found in panel for nominal YEAR={year}: {non_missing}")
        else:
            canonical = int(non_missing[0])
            if canonical != year:
                logging.error("Panel YEAR=%s does not match nominal YEAR=%s", canonical, year)
                raise RuntimeError(f"YEAR mismatch for panel file {year}: found {canonical}")
            data["YEAR"] = canonical

    col_map: dict[str, str] = {}
    for col in data.columns:
        if col in {"UNITID", "YEAR"}:
            continue
        var = match_column_to_var(col, varcodes)
        if var:
            col_map[col] = var

    if not col_map:
        logging.warning("No admissions columns found for YEAR=%s", year)
        return None

    logging.info(
        "YEAR=%s: mapped %d admissions columns (%s)",
        year,
        len(col_map),
        ", ".join(sorted(set(col_map.values()))),
    )

    subset = data[["UNITID", "YEAR", *col_map.keys()]].copy()
    subset["UNITID"] = pd.to_numeric(subset["UNITID"], errors="coerce")
    subset.dropna(subset=["UNITID"], inplace=True)
    subset["UNITID"] = subset["UNITID"].astype("int64")

    long_df = subset.melt(id_vars=["UNITID", "YEAR"], var_name="source_col", value_name="value")
    long_df.dropna(subset=["value"], inplace=True)
    long_df["value"] = pd.to_numeric(long_df["value"], errors="coerce")
    long_df.dropna(subset=["value"], inplace=True)
    long_df["source_var"] = long_df["source_col"].map(col_map)
    long_df.dropna(subset=["source_var"], inplace=True)
    result = long_df[["UNITID", "YEAR", "source_var", "value"]].copy()
    result["source_var"] = result["source_var"].astype(str).str.upper()
    return result


def combine_years(frames: list[pd.DataFrame]) -> pd.DataFrame:
    long_all = pd.concat(frames, ignore_index=True)
    long_all["UNITID"] = pd.to_numeric(long_all["UNITID"], errors="coerce").astype("int64")
    long_all["YEAR"] = pd.to_numeric(long_all["YEAR"], errors="coerce").astype("int64")
    long_all["source_var"] = long_all["source_var"].astype(str).str.upper()
    long_all["value"] = pd.to_numeric(long_all["value"], errors="coerce")
    long_all.dropna(subset=["value"], inplace=True)

    duplicate_mask = long_all.duplicated(subset=["UNITID", "YEAR", "source_var", "value"])
    if duplicate_mask.any():
        long_all = long_all.loc[~duplicate_mask]

    conflict_counts = long_all.groupby(["UNITID", "YEAR", "source_var"])["value"].nunique()
    conflicts = conflict_counts[conflict_counts > 1]
    if not conflicts.empty:
        sample = conflicts.head(10)
        logging.error("Found %s conflicting UNITID/YEAR/source_var combos. Sample:\n%s", len(conflicts), sample)
        raise RuntimeError(
            "Admissions unify step found multiple distinct values for the same UNITID/YEAR/source_var."
        )

    return long_all


def main() -> None:
    args = parse_args()
    logging.basicConfig(level=getattr(logging, args.log_level.upper(), logging.INFO), format="%(levelname)s %(message)s")

    dictionary, var_col, survey_col = load_dictionary(args.dictionary_lake)
    admissions_varcodes = select_admissions_varcodes(dictionary, var_col, survey_col)
    logging.info("Detected %d admissions varcodes from dictionary_lake", len(admissions_varcodes))

    frames: list[pd.DataFrame] = []
    for year in range(args.year_start, args.year_end + 1):
        panel_file = locate_panel_file(args.panel_dir, year)
        if not panel_file:
            logging.warning("panel_wide_raw file missing for YEAR=%s under %s", year, args.panel_dir)
            continue
        logging.info("Processing YEAR=%s from %s", year, panel_file)
        panel_df = read_panel(panel_file)
        long_year = build_long_for_year(panel_df, year, admissions_varcodes)
        if long_year is not None and not long_year.empty:
            frames.append(long_year)

    if not frames:
        raise SystemExit("No admissions observations collected. Verify panel directory and years.")

    long_all = combine_years(frames)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    long_all.to_parquet(args.output, index=False)
    logging.info(
        "Wrote %s admissions rows spanning %s UNITIDs and %s years to %s",
        len(long_all),
        long_all["UNITID"].nunique(),
        long_all["YEAR"].nunique(),
        args.output,
    )


if __name__ == "__main__":
    main()
