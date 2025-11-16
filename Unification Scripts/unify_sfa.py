"""Build a long Student Financial Aid panel from the raw wide IPEDS panel."""
from __future__ import annotations

import argparse
import logging
import re
from pathlib import Path
from typing import Iterable, List

import pandas as pd

SFA_VAR_RX = re.compile(r"^(SFA|NPT)", re.IGNORECASE)
UNITID_CANDIDATES = ["UNITID", "unitid", "UNIT_ID", "unit_id"]
YEAR_CANDIDATES = ["YEAR", "year", "SURVEY_YEAR", "survey_year", "panel_year", "SURVYEAR", "survyear"]
VAR_COL_CANDIDATES = ["varname", "var_name", "var", "variable"]
SURVEY_COL_CANDIDATES = ["survey", "SURVEY", "component", "COMPONENT", "survey_label", "component_name"]
SURVEY_YEAR_CANDIDATES = ["survey_year", "SURVEY_YEAR", "year", "YEAR", "panel_year"]
SURVEY_HINTS = ("SFA", "STUDENT FINANCIAL AID", "NET PRICE", "NET-PRICE")
BASE_STEP0_SFA_DIR = Path("/Users/markjaysonfarol13/Higher Ed research/IPEDS/Parquets/Unify/Step0sfa")
BASE_SFA_LONG_DIR = Path("/Users/markjaysonfarol13/Higher Ed research/IPEDS/Parquets/Unify/SFAlong")


def _resolve_dict_columns(df: pd.DataFrame, candidates: Iterable[str], required: bool = True) -> str | None:
    for col in candidates:
        if col in df.columns:
            return col
    if required:
        raise KeyError(f"None of the requested columns are present in dictionary: {candidates}")
    return None


def build_sfa_var_whitelist_from_dictionary(dictionary_df: pd.DataFrame, panel_columns: Iterable[str]) -> list[str]:
    """Return panel column names flagged as SFA/NPT in the dictionary, case-insensitive."""

    var_col = _resolve_dict_columns(dictionary_df, VAR_COL_CANDIDATES, required=True)
    survey_col = _resolve_dict_columns(dictionary_df, SURVEY_COL_CANDIDATES, required=False)

    var_series = dictionary_df[var_col].astype(str)
    mask = var_series.str.match(SFA_VAR_RX, na=False)

    if survey_col is not None:
        survey_values = dictionary_df[survey_col].astype(str).str.upper()
        survey_mask = survey_values.apply(lambda text: any(hint in text for hint in SURVEY_HINTS))
        mask |= survey_mask

    filtered = dictionary_df.loc[mask]
    if filtered.empty:
        logging.warning("Dictionary lake has no SFA/NPT rows; whitelist will be empty.")
        return []

    dict_vars_upper = set(filtered[var_col].astype(str).str.upper())
    panel_map = {str(col).upper(): col for col in panel_columns}

    matched = [panel_map[name] for name in dict_vars_upper if name in panel_map]
    if not matched:
        logging.warning("No overlap between SFA dictionary varnames and panel columns; whitelist will be empty.")
        return []

    unique_cols = sorted(set(matched))
    logging.info("Dictionary whitelist: %d SFA/NPT columns resolved in panel.", len(unique_cols))
    return unique_cols


def resolve_column(df: pd.DataFrame, preferred: str, fallbacks: Iterable[str]) -> str:
    """Return the first column present in df from preferred + fallbacks."""
    candidates = [preferred, *fallbacks]
    for candidate in candidates:
        if candidate in df.columns:
            return candidate
    raise KeyError(f"None of the requested columns are present: {candidates}")


def identify_sfa_columns(columns: Iterable[str], id_cols: Iterable[str]) -> List[str]:
    id_set = set(id_cols)
    selected: List[str] = []
    for column in columns:
        if column in id_set:
            continue
        name = str(column)
        if SFA_VAR_RX.match(name):
            selected.append(column)
            continue
        # Handle canonical names like "sfa_" or "npt_" (already harmonized columns).
        lowered = name.lower()
        if lowered.startswith("sfa") or lowered.startswith("npt"):
            selected.append(column)
    return selected


def build_long_panel(
    df: pd.DataFrame,
    unitid_col: str,
    year_col: str,
    sfa_var_whitelist: list[str] | None = None,
) -> pd.DataFrame:
    id_cols = [unitid_col, year_col]
    if sfa_var_whitelist:
        sfa_cols = [col for col in sfa_var_whitelist if col not in id_cols and col in df.columns]
        logging.info("Using dictionary whitelist for SFA cols (%d columns).", len(sfa_cols))
    else:
        sfa_cols = identify_sfa_columns(df.columns, id_cols)
        logging.info("Using regex-based SFA detection for SFA cols (%d columns).", len(sfa_cols))
    if not sfa_cols:
        logging.warning("No SFA columns remain after applying filters; returning empty panel.")
        empty = pd.DataFrame(columns=[unitid_col, year_col, "source_var", "value"])
        empty.rename(columns={unitid_col: "UNITID", year_col: "YEAR"}, inplace=True)
        return empty
    subset = df[[unitid_col, year_col] + sfa_cols].copy()
    long_df = subset.melt(id_vars=[unitid_col, year_col], var_name="source_var", value_name="value")
    long_df["source_var"] = long_df["source_var"].astype(str).str.upper()
    long_df["value"] = pd.to_numeric(long_df["value"], errors="coerce")
    long_df.dropna(subset=["value"], inplace=True)
    long_df.rename(columns={unitid_col: "UNITID", year_col: "YEAR"}, inplace=True)
    long_df.sort_values(["UNITID", "YEAR", "source_var"], inplace=True)
    return long_df.reset_index(drop=True)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--input-wide",
        type=Path,
        default=Path("data/derived/ipeds_panel_wide.parquet"),
        help="Path to the raw wide IPEDS panel (parquet).",
    )
    parser.add_argument(
        "--output-long",
        type=Path,
        default=BASE_STEP0_SFA_DIR / "sfa_step0_long.parquet",
        help="Destination for the long SFA parquet.",
    )
    parser.add_argument(
        "--dictionary-lake",
        type=Path,
        default=None,
        help="Optional path to dictionary_lake.parquet to refine SFA variable selection.",
    )
    parser.add_argument("--unitid-col", type=str, default="UNITID", help="UNITID column name in the wide panel.")
    parser.add_argument("--year-col", type=str, default="YEAR", help="Year column name in the wide panel.")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")

    if not args.input_wide.exists():
        raise FileNotFoundError(f"Wide panel not found: {args.input_wide}")

    logging.info("Loading wide panel: %s", args.input_wide)
    panel_df = pd.read_parquet(args.input_wide)

    try:
        unitid_col = resolve_column(panel_df, args.unitid_col, UNITID_CANDIDATES)
    except KeyError as exc:
        raise KeyError("Unable to determine UNITID column in the wide panel") from exc
    try:
        year_col = resolve_column(panel_df, args.year_col, YEAR_CANDIDATES)
    except KeyError as exc:
        raise KeyError("Unable to determine YEAR column in the wide panel") from exc

    logging.info("Detected UNITID column: %s", unitid_col)
    logging.info("Detected YEAR column: %s", year_col)

    sfa_var_whitelist: list[str] | None = None
    if args.dictionary_lake is not None and args.dictionary_lake.exists():
        logging.info("Loading dictionary lake from %s", args.dictionary_lake)
        dict_df = pd.read_parquet(args.dictionary_lake)
        try:
            sfa_var_whitelist = build_sfa_var_whitelist_from_dictionary(dict_df, panel_df.columns)
        except KeyError as exc:
            logging.warning("Failed to resolve dictionary columns: %s", exc)
            sfa_var_whitelist = None
    elif args.dictionary_lake is not None:
        logging.warning("Dictionary lake path does not exist: %s", args.dictionary_lake)
    else:
        logging.info("No dictionary lake provided; using regex-only SFA detection.")

    long_df = build_long_panel(
        panel_df,
        unitid_col,
        year_col,
        sfa_var_whitelist=sfa_var_whitelist,
    )
    logging.info("Long SFA rows: %d", len(long_df))

    args.output_long.parent.mkdir(parents=True, exist_ok=True)
    long_df.to_parquet(args.output_long, index=False)
    logging.info("Saved long SFA panel to %s", args.output_long)


if __name__ == "__main__":
    main()
