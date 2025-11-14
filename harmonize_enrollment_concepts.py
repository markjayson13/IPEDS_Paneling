#!/usr/bin/env python3
"""Apply enrollment crosswalk to Step 0 long data and produce a wide panel."""

from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

import pandas as pd


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--step0", type=Path, required=True, help="Path to enrollment_step0_long.parquet")
    parser.add_argument("--crosswalk", type=Path, required=True, help="Path to final enrollment crosswalk CSV")
    parser.add_argument("--output", type=Path, required=True, help="Output parquet path for wide concepts")
    parser.add_argument("--log-level", default="INFO")
    return parser.parse_args()


def load_step0(path: Path) -> pd.DataFrame:
    if not path.exists():
        raise FileNotFoundError(f"Step0 file not found: {path}")
    return pd.read_parquet(path)


def load_crosswalk(path: Path) -> pd.DataFrame:
    if not path.exists():
        raise FileNotFoundError(f"Crosswalk file not found: {path}")
    cw = pd.read_csv(path)
    cw.columns = [c.strip() for c in cw.columns]
    return cw


def apply_crosswalk(step0: pd.DataFrame, cw: pd.DataFrame) -> pd.DataFrame:
    cw = cw.copy()
    cw = cw[cw["concept_key"].astype(str).str.strip().ne("")]
    if cw.empty:
        raise RuntimeError("Crosswalk has no rows with non-empty concept_key.")

    step0 = step0.copy()
    step0["source_var_upper"] = step0["source_var"].str.upper()
    cw["source_var_upper"] = cw["source_var"].str.upper()

    if "weight" not in cw.columns:
        cw["weight"] = 1.0
    cw["weight"] = pd.to_numeric(cw["weight"], errors="coerce").fillna(1.0)
    cw["year_start"] = pd.to_numeric(cw["year_start"], errors="coerce").fillna(-10_000).astype(int)
    cw["year_end"] = pd.to_numeric(cw["year_end"], errors="coerce").fillna(10_000).astype(int)

    merged = step0.merge(
        cw[["concept_key", "source_var_upper", "year_start", "year_end", "weight"]],
        on="source_var_upper",
        how="inner",
    )
    merged = merged.loc[
        (merged["YEAR"] >= merged["year_start"]) & (merged["YEAR"] <= merged["year_end"])
    ].copy()
    merged["value_weighted"] = merged["value"] * merged["weight"]
    return merged


def main() -> None:
    args = parse_args()
    logging.basicConfig(level=getattr(logging, args.log_level.upper(), logging.INFO), format="%(levelname)s %(message)s")

    step0 = load_step0(args.step0)
    cw = load_crosswalk(args.crosswalk)
    merged = apply_crosswalk(step0, cw)

    if merged.empty:
        raise RuntimeError("No rows after applying crosswalk; check mappings and year ranges.")

    agg = (
        merged.groupby(["YEAR", "UNITID", "concept_key"], dropna=False)["value_weighted"]
        .sum()
        .reset_index()
    )

    wide = (
        agg.pivot_table(index=["YEAR", "UNITID"], columns="concept_key", values="value_weighted", aggfunc="sum")
        .reset_index()
    )
    wide.columns = [str(c) for c in wide.columns]
    cols_ordered = ["YEAR", "UNITID"] + [c for c in wide.columns if c not in {"YEAR", "UNITID"}]
    wide = wide[cols_ordered]

    args.output.parent.mkdir(parents=True, exist_ok=True)
    wide.to_parquet(args.output, index=False, compression="snappy")

    logging.info("Wrote %s rows with %s concept columns to %s", len(wide), len(wide.columns) - 2, args.output)
    sample_keys = wide.columns[2:12]
    logging.info("Sample concept keys: %s", ", ".join(sample_keys))


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:  # noqa: BLE001
        logging.error("harmonize_enrollment_concepts failed: %s", exc)
        sys.exit(1)
