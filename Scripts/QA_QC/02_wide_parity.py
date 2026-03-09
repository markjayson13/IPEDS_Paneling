#!/usr/bin/env python3
from __future__ import annotations

import argparse
import os
import shutil
import sys
from pathlib import Path

import duckdb
import pandas as pd
import pyarrow.parquet as pq

SCRIPTS_DIR = Path(__file__).resolve().parents[1]
if str(SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPTS_DIR))

from duckdb_build_utils import sql_quote
from wide_build_common import build_arg_parser, setup_logging
from wide_build_duckdb import run as run_duckdb
from wide_build_legacy import run as run_legacy


def parse_args() -> argparse.Namespace:
    repo_root = Path(os.environ.get("IPEDS_ROOT", str(Path(__file__).resolve().parents[2])))
    checks_root = repo_root / "Checks"
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--input", required=True, help="Stitched long panel parquet")
    p.add_argument("--dictionary", default=None, help="dictionary_lake.parquet")
    p.add_argument("--years", default="2004:2023", help='Year span, e.g. "2004:2023"')
    p.add_argument("--work-dir", default=str(checks_root / "wide_parity"), help="Working directory for legacy/new outputs")
    p.add_argument("--lane-split", action=argparse.BooleanOptionalAction, default=False, help="Mirror 04_build_wide_panel.py lane-split mode")
    p.add_argument("--dim-sources", default="IC_CAMPUSES,IC_PCCAMPUSES,F_FA_F,F_FA_G", help="Exact source_file names treated as dimensioned")
    p.add_argument("--dim-prefixes", default="C_,EF,GR,GR200,SAL,S_,OM,DRV", help="Comma-separated source_file prefixes treated as dimensioned")
    p.add_argument("--exclude-vars", default=None, help="Comma-separated varnames to exclude")
    p.add_argument("--typed-output", action=argparse.BooleanOptionalAction, default=False, help="Coerce numeric variables using dictionary metadata")
    p.add_argument("--drop-empty-cols", action=argparse.BooleanOptionalAction, default=False, help="Drop vars empty across selected years")
    p.add_argument("--drop-globally-null-post", action=argparse.BooleanOptionalAction, default=True, help="Drop globally-null columns in final stitched output")
    p.add_argument("--anti-garbage-ids", default="CIPCODE,LINE,FORMID,FUNCTCD,MAJORNUM", help="Dimension identifier names that must not appear as scalar wide columns")
    p.add_argument("--drop-anti-garbage-cols", action=argparse.BooleanOptionalAction, default=True, help="Drop blocked anti-garbage identifier columns from wide targets before fail gate")
    p.add_argument("--fail-on-anti-garbage", action=argparse.BooleanOptionalAction, default=True, help="Fail if anti-garbage blocked identifiers appear as wide columns")
    p.add_argument("--fail-on-scalar-conflicts", action=argparse.BooleanOptionalAction, default=True, help="Fail if scalar lane has conflicting values on canonical scalar key")
    p.add_argument("--scalar-conflicts-max-rows", type=int, default=100000, help="Max rows to write to scalar conflicts QC file")
    p.add_argument("--collapse-disc", action="store_true", help="Collapse discrete groups")
    p.add_argument("--drop-disc-components", action="store_true", help="Drop disc component vars after collapse")
    p.add_argument("--disc-exclude", default=None, help="Comma-separated base names to skip collapsing")
    p.add_argument("--disc-suffix", default="_CAT", help="Suffix used when base name collides with an existing variable")
    p.add_argument("--parity-contract", choices=["legacy_schema", "semantic_window"], default="legacy_schema", help="Compare either the legacy-compatible schema surface or the semantic-year-window surface")
    p.add_argument("--legacy-schema-seed-manifest", default=None, help="Optional override for the legacy compatibility seed manifest")
    p.add_argument("--scan-batch-rows", type=int, default=200_000, help="Batch size for scanning long rows")
    p.add_argument("--keep-work", action="store_true", help="Do not delete an existing work-dir before running")
    p.add_argument("--log-file", default=str(checks_root / "logs" / "02_wide_parity.log"), help="Optional harness log file")
    return p.parse_args()


def build_engine_args(base: argparse.Namespace, engine_dir: Path, *, persist_duckdb: bool) -> argparse.Namespace:
    parser = build_arg_parser()
    argv = [
        "--input",
        base.input,
        "--out_dir",
        str(engine_dir / "parts"),
        "--years",
        base.years,
        "--write_single",
        str(engine_dir / "panel.parquet"),
        "--log-file",
        str(engine_dir / "build.log"),
        "--dim-sources",
        base.dim_sources,
        "--dim-prefixes",
        base.dim_prefixes,
        "--anti-garbage-ids",
        base.anti_garbage_ids,
        "--scalar-conflicts-max-rows",
        str(base.scalar_conflicts_max_rows),
        "--scan-batch-rows",
        str(base.scan_batch_rows),
        "--duckdb-path",
        str(engine_dir / "build" / "ipeds_build.duckdb"),
        "--duckdb-temp-dir",
        str(engine_dir / "build" / "duckdb_tmp"),
        "--qc-dir",
        str(engine_dir / "wide_qc"),
        "--disc-qc-dir",
        str(engine_dir / "disc_qc"),
        "--scalar-long-out",
        str(engine_dir / "panel_long_scalar.parquet"),
        "--dim-long-out",
        str(engine_dir / "panel_long_dim.parquet"),
    ]
    if base.dictionary:
        argv += ["--dictionary", base.dictionary]
    if base.exclude_vars:
        argv += ["--exclude-vars", base.exclude_vars]
    argv += ["--lane-split" if base.lane_split else "--no-lane-split"]
    argv += ["--typed-output" if base.typed_output else "--no-typed-output"]
    argv += ["--drop-empty-cols" if base.drop_empty_cols else "--no-drop-empty-cols"]
    argv += ["--drop-globally-null-post" if base.drop_globally_null_post else "--no-drop-globally-null-post"]
    argv += ["--drop-anti-garbage-cols" if base.drop_anti_garbage_cols else "--no-drop-anti-garbage-cols"]
    argv += ["--fail-on-anti-garbage" if base.fail_on_anti_garbage else "--no-fail-on-anti-garbage"]
    argv += ["--fail-on-scalar-conflicts" if base.fail_on_scalar_conflicts else "--no-fail-on-scalar-conflicts"]
    argv += ["--legacy-analysis-schema" if base.parity_contract == "legacy_schema" else "--no-legacy-analysis-schema"]
    argv += ["--persist-duckdb" if persist_duckdb else "--no-persist-duckdb"]
    if base.collapse_disc:
        argv.append("--collapse-disc")
    if base.drop_disc_components:
        argv.append("--drop-disc-components")
    if base.disc_exclude:
        argv += ["--disc-exclude", base.disc_exclude]
    if base.disc_suffix:
        argv += ["--disc-suffix", base.disc_suffix]
    if base.legacy_schema_seed_manifest:
        argv += ["--legacy-schema-seed-manifest", base.legacy_schema_seed_manifest]
    return parser.parse_args(argv)


def query_df(sql: str) -> pd.DataFrame:
    con = duckdb.connect()
    try:
        return con.execute(sql).fetchdf()
    finally:
        con.close()


def compare_df(left: pd.DataFrame, right: pd.DataFrame, sort_cols: list[str] | None = None) -> bool:
    if list(left.columns) != list(right.columns):
        return False
    left_norm = left.copy()
    right_norm = right.copy()
    if sort_cols:
        left_norm = left_norm.sort_values(sort_cols).reset_index(drop=True)
        right_norm = right_norm.sort_values(sort_cols).reset_index(drop=True)
    left_norm = left_norm.fillna("<NA>").astype(str)
    right_norm = right_norm.fillna("<NA>").astype(str)
    return left_norm.equals(right_norm)


def csv_df(path: Path, sort_cols: list[str] | None = None) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame()
    df = pd.read_csv(path)
    if sort_cols and not df.empty:
        df = df.sort_values(sort_cols).reset_index(drop=True)
    return df


def aggregate_disc_conflicts(disc_dir: Path) -> pd.DataFrame:
    rows = []
    for fp in sorted(disc_dir.glob("disc_conflicts_*.csv")):
        df = pd.read_csv(fp)
        if df.empty or "base" not in df.columns:
            continue
        agg = df.groupby(["year", "base"]).size().reset_index(name="conflict_rows")
        rows.append(agg)
    if not rows:
        return pd.DataFrame(columns=["year", "base", "conflict_rows"])
    return pd.concat(rows, ignore_index=True).sort_values(["year", "base"]).reset_index(drop=True)


def content_diff_counts(left_path: Path, right_path: Path) -> tuple[int, int]:
    con = duckdb.connect()
    try:
        left_sql = sql_quote(str(left_path))
        right_sql = sql_quote(str(right_path))
        left_only = int(con.execute(f"SELECT COUNT(*) FROM (SELECT * FROM read_parquet({left_sql}) EXCEPT ALL SELECT * FROM read_parquet({right_sql}))").fetchone()[0])
        right_only = int(con.execute(f"SELECT COUNT(*) FROM (SELECT * FROM read_parquet({right_sql}) EXCEPT ALL SELECT * FROM read_parquet({left_sql}))").fetchone()[0])
        return left_only, right_only
    finally:
        con.close()


def main() -> None:
    args = parse_args()
    setup_logging(args.log_file)

    work_dir = Path(args.work_dir)
    if work_dir.exists() and not args.keep_work:
        shutil.rmtree(work_dir)
    work_dir.mkdir(parents=True, exist_ok=True)
    legacy_dir = work_dir / "legacy"
    duckdb_dir = work_dir / "duckdb"
    legacy_dir.mkdir(parents=True, exist_ok=True)
    duckdb_dir.mkdir(parents=True, exist_ok=True)

    legacy_args = build_engine_args(args, legacy_dir, persist_duckdb=False)
    duckdb_args = build_engine_args(args, duckdb_dir, persist_duckdb=True)

    run_legacy(legacy_args)
    run_duckdb(duckdb_args)

    legacy_panel = legacy_dir / "panel.parquet"
    duckdb_panel = duckdb_dir / "panel.parquet"
    legacy_cols = pq.ParquetFile(legacy_panel).schema.names
    duckdb_cols = pq.ParquetFile(duckdb_panel).schema.names

    summary_rows: list[dict] = []

    def add_check(name: str, legacy_value, duckdb_value, match: bool) -> None:
        summary_rows.append({"check": name, "legacy": legacy_value, "duckdb": duckdb_value, "match": bool(match)})

    add_check("column_order", "|".join(legacy_cols), "|".join(duckdb_cols), legacy_cols == duckdb_cols)

    row_counts_legacy = query_df(
        f"""
        SELECT year, COUNT(*) AS rows
        FROM read_parquet({sql_quote(str(legacy_panel))})
        GROUP BY year
        ORDER BY year
        """
    )
    row_counts_duckdb = query_df(
        f"""
        SELECT year, COUNT(*) AS rows
        FROM read_parquet({sql_quote(str(duckdb_panel))})
        GROUP BY year
        ORDER BY year
        """
    )
    add_check("row_counts_by_year", row_counts_legacy.to_json(orient="records"), row_counts_duckdb.to_json(orient="records"), compare_df(row_counts_legacy, row_counts_duckdb, ["year"]))

    unitid_counts_legacy = query_df(
        f"""
        SELECT year, COUNT(DISTINCT UNITID) AS distinct_unitids
        FROM read_parquet({sql_quote(str(legacy_panel))})
        GROUP BY year
        ORDER BY year
        """
    )
    unitid_counts_duckdb = query_df(
        f"""
        SELECT year, COUNT(DISTINCT UNITID) AS distinct_unitids
        FROM read_parquet({sql_quote(str(duckdb_panel))})
        GROUP BY year
        ORDER BY year
        """
    )
    add_check(
        "distinct_unitid_by_year",
        unitid_counts_legacy.to_json(orient="records"),
        unitid_counts_duckdb.to_json(orient="records"),
        compare_df(unitid_counts_legacy, unitid_counts_duckdb, ["year"]),
    )

    legacy_anti = csv_df(legacy_dir / "wide_qc" / "qc_anti_garbage_failures.csv", ["blocked_identifier_column"])
    duckdb_anti = csv_df(duckdb_dir / "wide_qc" / "qc_anti_garbage_failures.csv", ["blocked_identifier_column"])
    add_check("anti_garbage_hits", legacy_anti.to_json(orient="records"), duckdb_anti.to_json(orient="records"), compare_df(legacy_anti, duckdb_anti, ["blocked_identifier_column"]) if not legacy_anti.empty or not duckdb_anti.empty else True)

    legacy_cast = csv_df(legacy_dir / "wide_qc" / "qc_cast_report.csv", ["year", "column"])
    duckdb_cast = csv_df(duckdb_dir / "wide_qc" / "qc_cast_report.csv", ["year", "column"])
    add_check("cast_report", legacy_cast.to_json(orient="records"), duckdb_cast.to_json(orient="records"), compare_df(legacy_cast, duckdb_cast, ["year", "column"]) if not legacy_cast.empty or not duckdb_cast.empty else True)

    legacy_scalar = csv_df(legacy_dir / "wide_qc" / "qc_scalar_conflicts.csv")
    duckdb_scalar = csv_df(duckdb_dir / "wide_qc" / "qc_scalar_conflicts.csv")
    legacy_scalar_cmp = legacy_scalar.sort_values(sorted(legacy_scalar.columns)).reset_index(drop=True) if not legacy_scalar.empty else legacy_scalar
    duckdb_scalar_cmp = duckdb_scalar.sort_values(sorted(duckdb_scalar.columns)).reset_index(drop=True) if not duckdb_scalar.empty else duckdb_scalar
    add_check("scalar_conflicts", legacy_scalar.shape[0], duckdb_scalar.shape[0], compare_df(legacy_scalar_cmp, duckdb_scalar_cmp))

    legacy_disc = aggregate_disc_conflicts(legacy_dir / "disc_qc")
    duckdb_disc = aggregate_disc_conflicts(duckdb_dir / "disc_qc")
    add_check("disc_conflicts", legacy_disc.to_json(orient="records"), duckdb_disc.to_json(orient="records"), compare_df(legacy_disc, duckdb_disc, ["year", "base"]))

    legacy_nulls = csv_df(legacy_dir / "wide_qc" / "qc_globally_null_columns_dropped.csv", ["column"])
    duckdb_nulls = csv_df(duckdb_dir / "wide_qc" / "qc_globally_null_columns_dropped.csv", ["column"])
    add_check("globally_null_drop", legacy_nulls.to_json(orient="records"), duckdb_nulls.to_json(orient="records"), compare_df(legacy_nulls, duckdb_nulls, ["column"]) if not legacy_nulls.empty or not duckdb_nulls.empty else True)

    legacy_seeded = csv_df(legacy_dir / "wide_qc" / "qc_seeded_legacy_columns.csv", ["column_name"])
    duckdb_seeded = csv_df(duckdb_dir / "wide_qc" / "qc_seeded_legacy_columns.csv", ["column_name"])
    add_check("seeded_legacy_columns", legacy_seeded.to_json(orient="records"), duckdb_seeded.to_json(orient="records"), compare_df(legacy_seeded, duckdb_seeded, ["column_name"]) if not legacy_seeded.empty or not duckdb_seeded.empty else True)

    legacy_qc = csv_df(legacy_dir / "wide_qc" / "wide_panel_qc_summary.csv", ["year"])
    duckdb_qc = csv_df(duckdb_dir / "wide_qc" / "wide_panel_qc_summary.csv", ["year"])
    add_check("wide_qc_summary", legacy_qc.to_json(orient="records"), duckdb_qc.to_json(orient="records"), compare_df(legacy_qc, duckdb_qc, ["year"]))

    if legacy_cols == duckdb_cols:
        left_only, right_only = content_diff_counts(legacy_panel, duckdb_panel)
    else:
        left_only, right_only = (-1, -1)
    add_check("content_parity", left_only, right_only, left_only == 0 and right_only == 0)

    summary = pd.DataFrame(summary_rows)
    summary_path = work_dir / "parity_summary.csv"
    summary.to_csv(summary_path, index=False)
    print(f"Wrote {summary_path}")
    mismatches = summary.loc[~summary["match"]]
    if not mismatches.empty:
        print(mismatches.to_string(index=False))
        raise SystemExit(1)


if __name__ == "__main__":
    main()
