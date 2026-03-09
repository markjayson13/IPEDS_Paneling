#!/usr/bin/env python3
"""
Compute reproducible validation metrics for the IPEDS paper and QC appendix.

Outputs (CSV) in `--out-dir` include release-stage summaries, mapping coverage,
long-panel integrity metrics, discrete-conflict summaries, PRCH summaries, and
a compact validation table for manuscript use. Long-panel and panel-level
aggregations use DuckDB for out-of-core execution.
"""
from __future__ import annotations

import argparse
import csv
import re
from pathlib import Path

import duckdb
import pandas as pd


def parse_years(spec: str) -> list[int]:
    if ":" in spec:
        start, end = spec.split(":")
        return list(range(int(start), int(end) + 1))
    return [int(x) for x in spec.split(",") if x.strip()]


def summarize_release_qc(release_qc_dir: Path, years: list[int]) -> pd.DataFrame:
    rows = []
    for y in years:
        detail = release_qc_dir / f"release_details_{y}.csv"
        if not detail.exists():
            rows.append({"year": y, "included": None, "excluded": None, "missing_manifest": True})
            continue
        df = pd.read_csv(detail, dtype=str).fillna("")
        status = df.get("status")
        allowed = df.get("allowed")
        if allowed is None:
            # fall back to release_norm if needed
            status = df.get("release_norm", pd.Series([""] * len(df)))
            allowed = status.isin(["revised", "final"]) | (status == "")
        included = int(allowed.sum())
        excluded = int((~allowed).sum())
        rows.append({"year": y, "included": included, "excluded": excluded, "missing_manifest": False})
    return pd.DataFrame(rows)


def _discover_raw_files(year_root: Path) -> list[Path]:
    exts = {".csv", ".tsv", ".txt", ".gz"}
    out = []
    for fp in year_root.rglob("*"):
        if not fp.is_file():
            continue
        if fp.suffix.lower() not in exts:
            continue
        if any("_dict" in part.lower() for part in fp.parts):
            continue
        out.append(fp)
    return out


def mapping_coverage(
    dictionary_lake: Path,
    raw_root: Path,
    years: list[int],
    scan_raw: bool,
) -> pd.DataFrame:
    df = pd.read_parquet(dictionary_lake)
    df["varname"] = df["varname"].astype(str).str.upper()
    out_rows = []
    total_vars = df["varname"].nunique(dropna=True)
    for y in years:
        dict_year = df[df["year"] == y]
        dict_vars = set(dict_year["varname"].dropna().unique())
        mapped_cols = None
        unmapped_cols = None
        raw_cols = None
        if scan_raw:
            headers = set()
            year_root = raw_root / str(y)
            for fp in _discover_raw_files(year_root):
                try:
                    # read just header
                    h = pd.read_csv(fp, nrows=0).columns
                    headers.update([str(c).strip().upper() for c in h])
                except Exception:
                    continue
            raw_cols = len(headers)
            mapped_cols = len(headers & dict_vars)
            unmapped_cols = len(headers - dict_vars)
        out_rows.append(
            {
                "year": y,
                "dict_rows": int(len(dict_year)),
                "dict_unique_vars": int(len(dict_vars)),
                "total_unique_vars_all_years": int(total_vars),
                "raw_cols": raw_cols,
                "mapped_cols": mapped_cols,
                "unmapped_cols": unmapped_cols,
                "mapped_pct": (mapped_cols / raw_cols) if (mapped_cols and raw_cols) else None,
            }
        )
    return pd.DataFrame(out_rows)


def long_panel_integrity(long_panel: Path, years: list[int]) -> pd.DataFrame:
    con = duckdb.connect()
    con.execute("PRAGMA threads=4;")
    years_list = ",".join(str(y) for y in years)
    query = f"""
        SELECT
          year,
          COUNT(*) AS rows,
          COUNT(DISTINCT (UNITID, varname)) AS distinct_keys,
          COUNT(*) - COUNT(DISTINCT (UNITID, varname)) AS dup_key_rows,
          COUNT(DISTINCT varname) AS vars_with_data
        FROM read_parquet('{long_panel}')
        WHERE year IN ({years_list})
        GROUP BY year
        ORDER BY year
    """
    df = con.execute(query).fetchdf()
    total_vars = con.execute(f"SELECT COUNT(DISTINCT varname) FROM read_parquet('{long_panel}')").fetchone()[0]
    df["vars_total"] = int(total_vars)
    df["coverage_share"] = df["vars_with_data"] / df["vars_total"]
    df["dup_key_rate"] = df["dup_key_rows"] / df["rows"]
    return df


def disc_conflict_summary(disc_qc_dir: Path, wide_panel: Path | None) -> pd.DataFrame:
    # expects CSVs in disc_qc_dir with columns: year, family, conflicts, universe_rows
    rows = []
    for fp in sorted(disc_qc_dir.glob("*.csv")):
        try:
            df = pd.read_csv(fp)
        except Exception:
            continue
        if not {"year", "family", "conflicts"}.issubset(set(df.columns)):
            continue
        rows.append(df)
    if not rows:
        return pd.DataFrame(columns=["year", "family", "conflicts", "universe_rows", "conflict_rate"])
    all_df = pd.concat(rows, ignore_index=True)
    # If universe_rows missing, optionally derive from wide panel row counts
    if "universe_rows" not in all_df.columns and wide_panel:
        con = duckdb.connect()
        yrs = con.execute(f"SELECT year, COUNT(*) AS n FROM read_parquet('{wide_panel}') GROUP BY year").fetchdf()
        all_df = all_df.merge(yrs, on="year", how="left")
        all_df["universe_rows"] = all_df["n"]
        all_df = all_df.drop(columns=["n"])
    all_df["conflict_rate"] = all_df["conflicts"] / all_df["universe_rows"]
    return all_df


def write_table3(
    out_path: Path,
    release_df: pd.DataFrame,
    mapping_df: pd.DataFrame,
    long_df: pd.DataFrame,
    disc_df: pd.DataFrame,
    prch_summary_path: Path | None,
) -> None:
    # Create a compact metrics table with representative stats
    rows = []
    # Release QC
    if not release_df.empty:
        rows.append(
            {
                "section": "5.1 Release-stage validation",
                "metric": "Included component files (median per year)",
                "value": int(release_df["included"].median(skipna=True)),
                "units": "files/year",
                "source": "Checks/release_qc/release_details_YYYY.csv",
                "notes": "Median across selected years",
            }
        )
        rows.append(
            {
                "section": "5.1 Release-stage validation",
                "metric": "Excluded files (median per year)",
                "value": int(release_df["excluded"].median(skipna=True)),
                "units": "files/year",
                "source": "Checks/release_qc/release_details_YYYY.csv",
                "notes": "Non‑revised/final when revised exists",
            }
        )
    # Mapping coverage
    if not mapping_df.empty:
        rows.append(
            {
                "section": "5.2 Mapping coverage",
                "metric": "Dictionary rows (total)",
                "value": int(mapping_df["dict_rows"].sum()),
                "units": "rows",
                "source": "Dictionary/dictionary_lake.parquet",
                "notes": "Variable-year definitions",
            }
        )
        rows.append(
            {
                "section": "5.2 Mapping coverage",
                "metric": "Unique canonical variables",
                "value": int(mapping_df["dict_unique_vars"].max()),
                "units": "vars",
                "source": "Dictionary/dictionary_lake.parquet",
                "notes": "Max across years",
            }
        )
    # Long panel integrity
    if not long_df.empty:
        rows.append(
            {
                "section": "5.3 Long-panel integrity",
                "metric": "Duplicate key rate (median)",
                "value": float(long_df["dup_key_rate"].median()),
                "units": "share",
                "source": "Long panel parquet",
                "notes": "Duplicates on (UNITID, year, varname)",
            }
        )
        rows.append(
            {
                "section": "5.3 Long-panel integrity",
                "metric": "Coverage share (median)",
                "value": float(long_df["coverage_share"].median()),
                "units": "share",
                "source": "Long panel parquet",
                "notes": "Vars with any data / total vars",
            }
        )
    # Disc conflicts
    if not disc_df.empty:
        rows.append(
            {
                "section": "5.4 Discrete collapse conflicts",
                "metric": "Total conflicts (all years)",
                "value": int(disc_df["conflicts"].sum()),
                "units": "rows",
                "source": "Checks/disc_qc/*.csv",
                "notes": "More than one active category",
            }
        )
    # PRCH summary
    if prch_summary_path and prch_summary_path.exists():
        try:
            prch = pd.read_csv(prch_summary_path)
            rows.append(
                {
                    "section": "5.5 Parent/child validation",
                    "metric": "Child institution-years identified",
                    "value": int(prch["child_rows"].sum()),
                    "units": "rows",
                    "source": str(prch_summary_path),
                    "notes": "Sum across all PRCH flags",
                }
            )
        except Exception:
            pass

    pd.DataFrame(rows).to_csv(out_path, index=False)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--dictionary", required=True)
    ap.add_argument("--long-panel", required=True)
    ap.add_argument("--raw-root", required=True)
    ap.add_argument("--release-qc-dir", required=True)
    ap.add_argument("--disc-qc-dir", required=True)
    ap.add_argument("--prch-qc-summary", required=True)
    ap.add_argument("--wide-panel", default=None)
    ap.add_argument("--years", default="2004:2024")
    ap.add_argument("--out-dir", required=True)
    ap.add_argument("--scan-raw", action=argparse.BooleanOptionalAction, default=False)
    args = ap.parse_args()

    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    years = parse_years(args.years)
    release_df = summarize_release_qc(Path(args.release_qc_dir), years)
    release_df.to_csv(out_dir / "release_qc_summary.csv", index=False)

    mapping_df = mapping_coverage(Path(args.dictionary), Path(args.raw_root), years, args.scan_raw)
    mapping_df.to_csv(out_dir / "mapping_coverage.csv", index=False)

    long_df = long_panel_integrity(Path(args.long_panel), years)
    long_df.to_csv(out_dir / "long_panel_integrity.csv", index=False)

    disc_df = disc_conflict_summary(Path(args.disc_qc_dir), Path(args.wide_panel) if args.wide_panel else None)
    disc_df.to_csv(out_dir / "disc_conflicts_summary.csv", index=False)

    # passthrough PRCH summary if present
    prch_summary = Path(args.prch_qc_summary)
    if prch_summary.exists():
        prch_df = pd.read_csv(prch_summary)
        prch_df.to_csv(out_dir / "prch_clean_summary.csv", index=False)

    write_table3(
        out_dir / "table3_validation_metrics_filled.csv",
        release_df,
        mapping_df,
        long_df,
        disc_df,
        prch_summary if prch_summary.exists() else None,
    )

    print(f"Wrote metrics to {out_dir}")


if __name__ == "__main__":
    main()
