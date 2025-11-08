#!/usr/bin/env python3
"""
Build tidy, long-form IPEDS panels using the harmonization_maps.csv crosswalk.
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path
from typing import Iterable

import numpy as np
import pandas as pd

ROOT = Path("/Users/markjaysonfarol13/Higher Ed research/IPEDS/Cross sectional Datas")
MAP_PATH = Path("harmonization_maps.csv")
OUTPUT_PATH = Path("panel_long.parquet")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--root",
        type=Path,
        default=ROOT,
        help=f"Directory containing yearly IPEDS files (default: {ROOT})",
    )
    parser.add_argument(
        "--map",
        type=Path,
        default=MAP_PATH,
        help="CSV crosswalk describing the harmonization schema",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=OUTPUT_PATH,
        help="Output parquet path for the long-form panel",
    )
    parser.add_argument(
        "--manifest-suffix",
        default="_manifest.csv",
        help="Filename suffix for per-year manifests (default: _manifest.csv)",
    )
    return parser.parse_args()


def load_map(path: Path) -> pd.DataFrame:
    if not path.exists():
        raise SystemExit(f"Harmonization map not found: {path}")
    df = pd.read_csv(path).fillna("")
    required = [
        "target_var",
        "concept",
        "units",
        "year_from",
        "year_to",
        "survey",
        "prefix",
        "file_glob",
        "source_var",
        "transform",
        "severity",
    ]
    missing = [col for col in required if col not in df.columns]
    if missing:
        raise SystemExit(f"Crosswalk missing columns: {missing}")
    return df


def find_data_file(year_dir: Path, pattern: str) -> Path | None:
    matches = sorted(year_dir.glob(pattern))
    if not matches:
        return None
    if len(matches) > 1:
        print(
            f"WARNING: Multiple matches for {pattern} in {year_dir}. "
            f"Using {matches[0].name}"
        )
    return matches[0]


def read_data(path: Path) -> pd.DataFrame:
    suffix = path.suffix.lower()
    if suffix == ".csv":
        return pd.read_csv(path, low_memory=False)
    if suffix in {".xlsx", ".xls"}:
        engine = "openpyxl" if suffix == ".xlsx" else None
        return pd.read_excel(path, engine=engine)
    raise ValueError(f"Unsupported file type: {path}")


def find_unitid_column(columns: Iterable[str]) -> str | None:
    for col in columns:
        if col.upper() == "UNITID":
            return col
    # Allow variations like UNITID_P
    for col in columns:
        if "UNITID" in col.upper():
            return col
    return None


def apply_transform(series: pd.Series, expression: str) -> pd.Series:
    if not expression or expression == "identity":
        return series
    local_env = {"x": series.copy()}
    allowed_globals = {"pd": pd, "np": np}
    try:
        return eval(expression, {"__builtins__": {}}, {**allowed_globals, **local_env})
    except Exception as exc:  # noqa: BLE001
        print(f"WARNING: Failed to apply transform '{expression}': {exc}")
        return series


def load_manifest(year_dir: Path, year: int, suffix: str) -> pd.DataFrame | None:
    manifest_path = year_dir / f"{year}{suffix}"
    if manifest_path.exists():
        try:
            return pd.read_csv(manifest_path)
        except Exception as exc:  # noqa: BLE001
            print(f"WARNING: Unable to read manifest {manifest_path}: {exc}")
    return None


def lookup_release(manifest: pd.DataFrame | None, filename: str) -> str:
    if manifest is None or "filename" not in manifest.columns:
        return "revised" if "_RV" in filename.upper() else ""
    match = manifest[manifest["filename"] == filename]
    if match.empty:
        return "revised" if "_RV" in filename.upper() else ""
    release_col = "release" if "release" in match.columns else None
    if release_col:
        release_val = match.iloc[0][release_col]
        return release_val if isinstance(release_val, str) else ""
    return "revised" if "_RV" in filename.upper() else ""


def main() -> None:
    args = parse_args()
    crosswalk = load_map(args.map)
    outputs: list[pd.DataFrame] = []

    for _, row in crosswalk.iterrows():
        try:
            year_start = int(row["year_from"])
            year_end = int(row["year_to"])
        except ValueError as exc:
            print(f"Skipping row with invalid year range: {exc}")
            continue

        for year in range(year_start, year_end + 1):
            year_dir = args.root / str(year)
            if not year_dir.exists():
                print(f"WARNING: Year directory missing: {year_dir}")
                continue

            manifest = load_manifest(year_dir, year, args.manifest_suffix)

            data_path = find_data_file(year_dir, row["file_glob"])
            if data_path is None:
                print(
                    f"WARNING: No file matching {row['file_glob']} for year {year} "
                    f"(target_var={row['target_var']})"
                )
                continue

            try:
                df = read_data(data_path)
            except Exception as exc:  # noqa: BLE001
                print(f"WARNING: Failed to read {data_path}: {exc}")
                continue

            unitid_col = find_unitid_column(df.columns)
            if not unitid_col:
                print(f"WARNING: UNITID column not found in {data_path}")
                continue

            if row["source_var"] not in df.columns:
                print(
                    f"WARNING: Column {row['source_var']} not found in "
                    f"{data_path.name} for year {year}"
                )
                continue

            subset = df[[unitid_col, row["source_var"]]].copy()
            subset.columns = ["UNITID", "value"]
            subset["UNITID"] = pd.to_numeric(subset["UNITID"], errors="coerce").astype("Int64")
            subset["value"] = pd.to_numeric(subset["value"], errors="coerce")
            subset["value"] = apply_transform(subset["value"], row["transform"])
            subset["year"] = year
            subset["target_var"] = row["target_var"]
            subset["concept"] = row["concept"]
            subset["units"] = row["units"]
            subset["survey"] = row["survey"]
            subset["prefix"] = row["prefix"]
            subset["source_file"] = data_path.name
            subset["severity"] = row["severity"]
            subset["notes"] = row.get("notes", "")
            subset["release"] = lookup_release(manifest, data_path.name)

            outputs.append(subset)

    if not outputs:
        print("No harmonized data produced.")
        sys.exit(0)

    panel = pd.concat(outputs, ignore_index=True)
    panel.to_parquet(args.output, index=False)
    print(f"Wrote {len(panel):,} rows to {args.output}")


if __name__ == "__main__":
    main()
