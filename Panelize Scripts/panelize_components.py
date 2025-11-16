#!/usr/bin/env python3
"""Merge multiple survey-specific wide panels into a single institution-year panel with optional parent/child filtering."""

from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path
from typing import Dict, List, Tuple

import pandas as pd

ID_COLS = ["UNITID", "YEAR"]
DEFAULT_OUTPUT = Path("/Users/markjaysonfarol13/Higher Ed research/IPEDS/Paneled Datasets/Final/panel_wide.csv")


def parse_component(spec: str) -> Tuple[str, Path]:
    if "=" not in spec:
        raise argparse.ArgumentTypeError(f"Component '{spec}' must be in the form label=/path/to/file.parquet")
    label, path_str = spec.split("=", 1)
    label = label.strip()
    if not label:
        raise argparse.ArgumentTypeError(f"Component label missing in '{spec}'")
    path = Path(path_str.strip())
    return label, path


def read_table(path: Path) -> pd.DataFrame:
    if not path.exists():
        raise FileNotFoundError(f"Component file not found: {path}")
    suffix = path.suffix.lower()
    if suffix in {".parquet", ".pq"}:
        return pd.read_parquet(path)
    if suffix in {".csv", ".tsv"}:
        sep = "," if suffix == ".csv" else "\t"
        return pd.read_csv(path, sep=sep)
    raise ValueError(f"Unsupported component file type: {path}")


def standardize_ids(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df.columns = [str(c).strip().upper() for c in df.columns]
    for col in ID_COLS:
        if col not in df.columns:
            raise ValueError(f"Component missing required column '{col}'")
    df["UNITID"] = pd.to_numeric(df["UNITID"], errors="coerce").astype("Int64")
    df["YEAR"] = pd.to_numeric(df["YEAR"], errors="coerce").astype("Int64")
    return df


def rename_component_columns(df: pd.DataFrame, label: str, drop_reporters: list[str]) -> pd.DataFrame:
    label_prefix = label.upper()
    rename_map: Dict[str, str] = {}
    for col in df.columns:
        if col in ID_COLS:
            continue
        new_name = f"{label_prefix}__{col}"
        rename_map[col] = new_name
    renamed = df.rename(columns=rename_map)
    for col in drop_reporters:
        prefixed = f"{label_prefix}__{col}"
        if prefixed in renamed.columns:
            renamed[prefixed] = renamed[prefixed]
    return renamed


def combine_components(components: List[Tuple[str, Path]], join: str) -> pd.DataFrame:
    reporter_candidates = {"REPORTING_UNITID"}
    combined: pd.DataFrame | None = None
    for label, path in components:
        logging.info("Loading component '%s' from %s", label, path)
        df = read_table(path)
        df = standardize_ids(df)
        df = rename_component_columns(df, label, list(reporter_candidates))
        if combined is None:
            combined = df
        else:
            combined = combined.merge(df, on=ID_COLS, how=join)
    assert combined is not None

    # Build unified REPORTING_UNITID column if possible
    rep_cols = [col for col in combined.columns if col.endswith("__REPORTING_UNITID")]
    if rep_cols:
        combined["REPORTING_UNITID"] = pd.NA
        for col in rep_cols:
            combined["REPORTING_UNITID"] = combined["REPORTING_UNITID"].fillna(combined[col])
        combined["REPORTING_UNITID"] = pd.to_numeric(combined["REPORTING_UNITID"], errors="ignore")

    status_cols = [col for col in combined.columns if col.endswith("__STABLE_PRNTCHLD_STATUS")]
    if status_cols:
        logging.info("Found parent/child status columns: %s", status_cols)
        combined["STABLE_PRNTCHLD_STATUS"] = pd.NA
        for col in status_cols:
            combined["STABLE_PRNTCHLD_STATUS"] = combined["STABLE_PRNTCHLD_STATUS"].fillna(combined[col])
        combined["STABLE_PRNTCHLD_STATUS"] = pd.to_numeric(
            combined["STABLE_PRNTCHLD_STATUS"], errors="coerce"
        ).astype("Int64")

    return combined


def apply_parent_child_filter(df: pd.DataFrame, mode: str) -> pd.DataFrame:
    if mode == "none":
        logging.info("Parent/child filter mode: none (no filtering applied).")
        return df

    if "STABLE_PRNTCHLD_STATUS" not in df.columns:
        logging.warning(
            "Parent/child filter '%s' requested but STABLE_PRNTCHLD_STATUS column is missing; skipping filter.",
            mode,
        )
        return df

    status = df["STABLE_PRNTCHLD_STATUS"]

    if mode == "campus":
        mask_drop = status.isin([2, 3])
        n_total = len(df)
        n_drop = int(mask_drop.sum())
        counts = status[mask_drop].value_counts(dropna=False).to_dict()
        logging.info(
            "Applying campus-level parent/child filter: dropping %d of %d rows (%.3f%%). Counts by status: %s",
            n_drop,
            n_total,
            (n_drop / n_total * 100.0) if n_total else 0.0,
            counts,
        )
        return df.loc[~mask_drop].copy()

    logging.warning("Unknown parent/child filter mode '%s'; returning unfiltered DataFrame.", mode)
    return df


def write_output(df: pd.DataFrame, output: Path) -> None:
    output.parent.mkdir(parents=True, exist_ok=True)
    if output.suffix.lower() in {".parquet", ".pq"}:
        df.to_parquet(output, index=False)
    else:
        df.to_csv(output, index=False)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--component",
        action="append",
        metavar="LABEL=PATH",
        help="Component file to merge (e.g. hd=/path/hd_master_panel.parquet). Can be passed multiple times.",
    )
    parser.add_argument("--join", choices=["outer", "inner"], default="outer", help="Join strategy across components.")
    parser.add_argument(
        "--parent-child-filter",
        choices=["none", "campus"],
        default="none",
        help=(
            "How to handle parent/child reporting. "
            "'none' = keep all UNITIDs. "
            "'campus' = drop parent and child UNITIDs based on STABLE_PRNTCHLD_STATUS."
        ),
    )
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT, help="Destination wide panel path.")
    parser.add_argument("--log-level", default="INFO", help="Logging level.")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    logging.basicConfig(level=getattr(logging, args.log_level.upper(), logging.INFO), format="%(levelname)s %(message)s")
    if not args.component:
        raise SystemExit("At least one --component LABEL=PATH is required.")
    parsed_components = [parse_component(spec) for spec in args.component]
    wide = combine_components(parsed_components, args.join)
    logging.info("Combined panel shape: %s rows x %s columns", len(wide), len(wide.columns))
    wide = apply_parent_child_filter(wide, args.parent_child_filter)
    logging.info(
        "After parent/child filter '%s': %s rows x %s columns",
        args.parent_child_filter,
        len(wide),
        len(wide.columns),
    )
    write_output(wide, args.output)
    logging.info("Wrote combined panel to %s", args.output)


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:  # noqa: BLE001
        logging.error("panelize_components failed: %s", exc)
        sys.exit(1)
