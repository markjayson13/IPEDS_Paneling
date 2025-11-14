#!/usr/bin/env python3
"""Aggregate early F1A finance parent and component files into a single total per UNITID."""

from __future__ import annotations

import argparse
import logging
import re
from pathlib import Path
from typing import List

import pandas as pd

ID_COL = "UNITID"
NUMERIC_EXCLUDE = {"UNITID", "SURVEY", "LINE"}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Aggregate F1A parent/component finance files")
    parser.add_argument("--root", type=Path, required=True, help="Path to IPEDS Cross sectional Datas root")
    parser.add_argument("--years", type=str, required=True, help="Comma list or ranges, e.g. 2004-2007,2009")
    parser.add_argument("--log-level", default="INFO")
    return parser.parse_args()


def parse_years(expr: str) -> List[int]:
    years: set[int] = set()
    for token in expr.split(","):
        token = token.strip()
        if not token:
            continue
        if "-" in token:
            start, end = token.split("-", 1)
            for year in range(int(start), int(end) + 1):
                years.add(year)
        else:
            years.add(int(token))
    return sorted(years)


def find_dir(year_dir: Path, suffix: str) -> Path | None:
    pattern = re.compile(rf"F\d{{4}}_F1A{suffix}$", re.IGNORECASE)
    for child in year_dir.iterdir():
        if child.is_dir() and pattern.search(child.name):
            return child
    return None


def pick_csv(folder: Path, label: str) -> Path:
    candidates = sorted(p for p in folder.glob("*.csv") if "rv" in p.name.lower())
    if not candidates:
        raise FileNotFoundError(f"No revised CSV found in {folder} ({label})")
    return candidates[0]


def coerce_numeric(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    for col in out.columns:
        if col.upper() in NUMERIC_EXCLUDE:
            continue
        out[col] = pd.to_numeric(out[col], errors="coerce")
    return out


def aggregate_year(root: Path, year: int) -> Path:
    year_dir = root / str(year)
    if not year_dir.exists():
        raise FileNotFoundError(year_dir)

    parent_dir = find_dir(year_dir, r"(_F1A)?") or find_dir(year_dir, "")
    comp_f_dir = find_dir(year_dir, "_F")
    comp_g_dir = find_dir(year_dir, "_G")
    if parent_dir is None:
        raise RuntimeError(f"Parent F1A directory missing for {year}")

    parent_path = pick_csv(parent_dir, "parent")
    comp_paths = [pick_csv(d, label) for d, label in ((comp_f_dir, "component F"), (comp_g_dir, "component G")) if d]

    parent = pd.read_csv(parent_path, dtype=str)
    if ID_COL not in parent.columns:
        raise RuntimeError(f"UNITID missing in {parent_path}")
    parent = coerce_numeric(parent)

    if comp_paths:
        comps = [coerce_numeric(pd.read_csv(p, dtype=str)) for p in comp_paths]
        components = pd.concat(comps, ignore_index=True)
        components = components.dropna(subset=[ID_COL])
        value_cols = sorted({c for c in components.columns if c not in {ID_COL}})
        grouped = components.groupby(ID_COL)[value_cols].sum()

        parent = parent.set_index(ID_COL)
        grouped = grouped.reindex(parent.index, fill_value=0)
        for col in grouped.columns:
            if col in parent.columns:
                parent[col] = parent[col].fillna(0) + grouped[col]
            else:
                parent[col] = grouped[col]
        parent = parent.reset_index()
    backup_path = parent_path.with_suffix(parent_path.suffix + ".bak")
    if not backup_path.exists():
        parent_path.replace(backup_path)
    else:
        parent_path.unlink()
    parent.to_csv(parent_path, index=False)
    return parent_path


def main() -> None:
    args = parse_args()
    logging.basicConfig(level=getattr(logging, args.log_level.upper(), logging.INFO))
    years = parse_years(args.years)
    for year in years:
        try:
            out = aggregate_year(args.root, year)
            logging.info("Aggregated F1A for %s -> %s", year, out)
        except Exception as exc:  # noqa: BLE001
            logging.error("Failed for %s: %s", year, exc)


if __name__ == "__main__":
    main()
