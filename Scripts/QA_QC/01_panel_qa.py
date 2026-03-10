#!/usr/bin/env python3
"""
QA checks for PRCH cleaning versus the raw stitched wide panel.

The script verifies year coverage, row preservation, and selected non-null
comparisons, then writes a compact summary to `Checks/panel_qc/` for the full
release cleaning workflow.
"""
from __future__ import annotations

import argparse
import os
from pathlib import Path
import sys

import pandas as pd
import pyarrow.compute as pc
import pyarrow.dataset as ds


def setup_logging(log_path: str | None) -> None:
    if not log_path:
        return
    log_file = Path(log_path)
    log_file.parent.mkdir(parents=True, exist_ok=True)
    f = log_file.open("a", buffering=1)

    class Tee:
        def __init__(self, *streams):
            self.streams = streams

        def write(self, data):
            for s in self.streams:
                s.write(data)

        def flush(self):
            for s in self.streams:
                s.flush()

    sys.stdout = Tee(sys.stdout, f)
    sys.stderr = Tee(sys.stderr, f)


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--raw", required=True, help="Raw stitched wide parquet")
    p.add_argument("--clean", required=True, help="PRCH cleaned wide parquet")
    repo_root = Path(os.environ.get("IPEDS_ROOT", str(Path(__file__).resolve().parents[2])))
    checks_root = repo_root / "Checks"
    p.add_argument("--out-dir", default=str(checks_root / "panel_qc"), help="QA output directory")
    p.add_argument("--prch-qc-dir", default=str(checks_root / "prch_qc"), help="PRCH QC dir (for prch_clean_columns.csv)")
    p.add_argument("--sample-rows", type=int, default=1000, help="Sample size for flag/column check")
    p.add_argument("--probe-col", default="AUTO", help="Column to compare non-null counts (or AUTO)")
    p.add_argument("--auto-max-cols", type=int, default=200, help="Max candidate columns to scan when AUTO")
    p.add_argument("--flag", default="PRCH_F", help="PRCH flag to validate")
    p.add_argument("--flag-child", default="2,3,5", help="Child codes for the flag (comma-separated)")
    p.add_argument("--year-sep", default="|", help="Separator for year lists in CSV")
    p.add_argument("--excel-text", action=argparse.BooleanOptionalAction, default=True, help="Prefix year lists with apostrophe for Excel text")
    p.add_argument("--log-file", default=str(checks_root / "logs" / "01_panel_qa.log"), help="Optional log file path")
    return p.parse_args()


def format_years(years, sep: str, excel_text: bool) -> str:
    years_sorted = sorted(int(y) for y in years if pd.notna(y))
    s = sep.join(str(y) for y in years_sorted)
    if excel_text:
        return "'" + s
    return s


def auto_select_probe(raw: ds.Dataset, clean: ds.Dataset, flag: str, child_codes: set[int], prch_qc_dir: Path, max_cols: int) -> tuple[str | None, int]:
    # Prefer candidate list from PRCH QC if available
    candidates = []
    qc_path = prch_qc_dir / "prch_clean_columns.csv"
    if qc_path.exists():
        qc = pd.read_csv(qc_path)
        candidates = qc.loc[qc["flag"] == flag, "column"].dropna().astype(str).tolist()
    # Fallback: use schema columns (excluding identifiers and flags)
    if not candidates:
        exclude = {"year", "UNITID", flag}
        candidates = [c for c in raw.schema.names if c not in exclude and not c.upper().startswith("PRCH")]
    candidates = candidates[:max_cols]
    if not candidates:
        return None, 0

    cols = [flag] + candidates
    raw_df = raw.to_table(columns=cols).to_pandas()
    clean_df = clean.to_table(columns=cols).to_pandas()
    raw_df[flag] = pd.to_numeric(raw_df[flag], errors="coerce")
    clean_df[flag] = pd.to_numeric(clean_df[flag], errors="coerce")
    raw_child = raw_df[raw_df[flag].isin(child_codes)]
    clean_child = clean_df[clean_df[flag].isin(child_codes)]

    best = None
    best_diff = -1
    for c in candidates:
        raw_nonnull = raw_child[c].notna().sum()
        clean_nonnull = clean_child[c].notna().sum()
        diff = raw_nonnull - clean_nonnull
        if raw_nonnull > 0 and diff >= 0 and diff > best_diff:
            best = c
            best_diff = diff
    return best, len(candidates)


def main() -> None:
    args = parse_args()
    setup_logging(args.log_file)
    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    raw = ds.dataset(args.raw, format="parquet")
    cln = ds.dataset(args.clean, format="parquet")

    # Year coverage
    raw_years = raw.to_table(columns=["year"]).to_pandas()["year"].unique()
    cln_years = cln.to_table(columns=["year"]).to_pandas()["year"].unique()

    # Row counts
    raw_rows = raw.count_rows()
    cln_rows = cln.count_rows()

    # Size
    raw_size = os.path.getsize(args.raw) if os.path.exists(args.raw) else 0
    cln_size = os.path.getsize(args.clean) if os.path.exists(args.clean) else 0

    # Non-null comparison for probe column
    probe = args.probe_col
    auto_probe_used = False
    auto_candidates = 0
    raw_valid = 0
    cln_valid = 0
    # Flag parsing must happen BEFORE AUTO probe selection
    flag = args.flag
    child_codes = {int(x) for x in args.flag_child.split(",") if x.strip()}
    if probe == "AUTO":
        auto_probe_used = True
        probe, auto_candidates = auto_select_probe(raw, cln, flag, child_codes, Path(args.prch_qc_dir), args.auto_max_cols)
    if probe and probe in raw.schema.names and probe in cln.schema.names:
        for b in raw.to_batches(columns=[probe]):
            raw_valid += pc.sum(pc.is_valid(b[probe])).as_py()
        for b in cln.to_batches(columns=[probe]):
            cln_valid += pc.sum(pc.is_valid(b[probe])).as_py()

    # Flag check on sample
    child_raw_nonnull = None
    child_cln_nonnull = None
    if probe and all(c in raw.schema.names for c in ["year", "UNITID", flag, probe]):
        raw_df = raw.to_table(columns=["year", "UNITID", flag, probe]).to_pandas()
        raw_df = raw_df.sample(min(args.sample_rows, len(raw_df)), random_state=1)
        cln_df = cln.to_table(columns=["year", "UNITID", flag, probe]).to_pandas()
        merged = raw_df.merge(cln_df, on=["year", "UNITID", flag], suffixes=("_raw", "_clean"))
        child = merged[merged[flag].isin(child_codes)]
        child_raw_nonnull = child[f"{probe}_raw"].notna().sum()
        child_cln_nonnull = child[f"{probe}_clean"].notna().sum()

    # Print summary
    print("raw years:", sorted(raw_years))
    print("clean years:", sorted(cln_years))
    print("raw rows:", raw_rows)
    print("clean rows:", cln_rows)
    print("raw size (bytes):", raw_size)
    print("clean size (bytes):", cln_size)
    if probe and probe in raw.schema.names:
        print(f"{probe} raw non-null:", raw_valid)
        print(f"{probe} clean non-null:", cln_valid)
    if child_raw_nonnull is not None:
        print(f"child rows {flag} raw non-null {probe}:", child_raw_nonnull)
        print(f"child rows {flag} clean non-null {probe}:", child_cln_nonnull)

    # Write CSV summary
    summary = pd.DataFrame([
        {
            "raw_years": format_years(raw_years, args.year_sep, args.excel_text),
            "clean_years": format_years(cln_years, args.year_sep, args.excel_text),
            "raw_years_min": min([int(y) for y in raw_years if pd.notna(y)], default=None),
            "raw_years_max": max([int(y) for y in raw_years if pd.notna(y)], default=None),
            "raw_years_count": len([y for y in raw_years if pd.notna(y)]),
            "clean_years_min": min([int(y) for y in cln_years if pd.notna(y)], default=None),
            "clean_years_max": max([int(y) for y in cln_years if pd.notna(y)], default=None),
            "clean_years_count": len([y for y in cln_years if pd.notna(y)]),
            "raw_rows": raw_rows,
            "clean_rows": cln_rows,
            "raw_size_bytes": raw_size,
            "clean_size_bytes": cln_size,
            "probe_col": probe,
            "auto_probe_used": auto_probe_used,
            "auto_probe_candidates": auto_candidates,
            "raw_nonnull": raw_valid,
            "clean_nonnull": cln_valid,
            "flag": flag,
            "child_codes": ",".join(map(str, sorted(child_codes))),
            "child_raw_nonnull": child_raw_nonnull,
            "child_clean_nonnull": child_cln_nonnull,
        }
    ])
    summary.to_csv(out_dir / "panel_qa_summary.csv", index=False)
    print("Wrote", out_dir / "panel_qa_summary.csv")


if __name__ == "__main__":
    main()
