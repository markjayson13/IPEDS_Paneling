#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path

import duckdb
import pandas as pd

SCRIPTS_DIR = Path(__file__).resolve().parents[1]
if str(SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPTS_DIR))

from duckdb_build_utils import quote_ident, sql_quote
from wide_build_common import setup_logging


def default_repo_root() -> Path:
    return Path(os.environ.get("IPEDS_ROOT", Path(__file__).resolve().parents[2]))


def default_baseline_root() -> Path:
    return Path(os.environ.get("IPEDS_BASELINE_ROOT", "/Users/markjaysonfarol13/Projects/IPEDS_Paneling"))


def parse_years_spec(spec: str) -> list[int]:
    if ":" in spec:
        start, end = spec.split(":", 1)
        return list(range(int(start), int(end) + 1))
    return [int(part.strip()) for part in spec.split(",") if part.strip()]


def read_json(path: Path) -> dict:
    if not path.exists():
        return {}
    return json.loads(path.read_text())


def part_glob(parts_dir: Path) -> str:
    return str(parts_dir / "year=*" / "part.parquet")


def part_path(parts_dir: Path, year: int) -> Path:
    return parts_dir / f"year={int(year)}" / "part.parquet"


def parquet_exists(path_or_glob: str) -> bool:
    return any(Path().glob(path_or_glob)) if "*" in path_or_glob else Path(path_or_glob).exists()


def query_df(sql: str, params: list[str] | None = None) -> pd.DataFrame:
    con = duckdb.connect()
    try:
        return con.execute(sql, params or []).fetchdf()
    finally:
        con.close()


def parquet_count(path_or_glob: str) -> int:
    con = duckdb.connect()
    try:
        return int(con.execute(f"SELECT COUNT(*) FROM read_parquet({sql_quote(path_or_glob)})").fetchone()[0])
    finally:
        con.close()


def existing_years(parts_dir: Path) -> list[int]:
    years: list[int] = []
    for fp in sorted(parts_dir.glob("year=*/part.parquet")):
        try:
            years.append(int(fp.parent.name.split("=", 1)[1]))
        except (IndexError, ValueError):
            continue
    return years


def describe_parquet(path_or_glob: str) -> pd.DataFrame:
    con = duckdb.connect()
    try:
        return con.execute(f"DESCRIBE SELECT * FROM read_parquet({sql_quote(path_or_glob)})").fetchdf()
    finally:
        con.close()


def csv_df(path: Path, sort_cols: list[str] | None = None) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame()
    df = pd.read_csv(path)
    if sort_cols and not df.empty:
        df = df.sort_values(sort_cols, kind="mergesort").reset_index(drop=True)
    return df


def compare_df(left: pd.DataFrame, right: pd.DataFrame, sort_cols: list[str] | None = None) -> bool:
    if list(left.columns) != list(right.columns):
        return False
    left_norm = left.copy()
    right_norm = right.copy()
    if sort_cols and not left_norm.empty and not right_norm.empty:
        left_norm = left_norm.sort_values(sort_cols, kind="mergesort").reset_index(drop=True)
        right_norm = right_norm.sort_values(sort_cols, kind="mergesort").reset_index(drop=True)
    left_norm = left_norm.fillna("<NA>").astype(str)
    right_norm = right_norm.fillna("<NA>").astype(str)
    return left_norm.equals(right_norm)


def aggregate_disc_conflicts(disc_dir: Path) -> pd.DataFrame:
    rows: list[pd.DataFrame] = []
    for fp in sorted(disc_dir.glob("disc_conflicts_*.csv")):
        df = pd.read_csv(fp)
        if df.empty or "base" not in df.columns:
            continue
        agg = df.groupby(["year", "base"]).size().reset_index(name="conflict_rows")
        rows.append(agg)
    if not rows:
        return pd.DataFrame(columns=["year", "base", "conflict_rows"])
    return pd.concat(rows, ignore_index=True).sort_values(["year", "base"], kind="mergesort").reset_index(drop=True)


def column_list_sql(columns: list[str]) -> str:
    return ", ".join(quote_ident(col) for col in columns)


def content_diff_counts(left_path_or_glob: str, right_path_or_glob: str, columns: list[str]) -> tuple[int, int]:
    select_sql = column_list_sql(columns)
    con = duckdb.connect()
    try:
        left_only = int(
            con.execute(
                f"""
                SELECT COUNT(*)
                FROM (
                    SELECT {select_sql} FROM read_parquet({sql_quote(left_path_or_glob)})
                    EXCEPT ALL
                    SELECT {select_sql} FROM read_parquet({sql_quote(right_path_or_glob)})
                )
                """
            ).fetchone()[0]
        )
        right_only = int(
            con.execute(
                f"""
                SELECT COUNT(*)
                FROM (
                    SELECT {select_sql} FROM read_parquet({sql_quote(right_path_or_glob)})
                    EXCEPT ALL
                    SELECT {select_sql} FROM read_parquet({sql_quote(left_path_or_glob)})
                )
                """
            ).fetchone()[0]
        )
        return left_only, right_only
    finally:
        con.close()


def parse_args() -> argparse.Namespace:
    repo_root = default_repo_root()
    checks_root = repo_root / "Checks"
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--run-dir", default=None, help="Monitored run directory containing build_telemetry.json and run_meta.json")
    p.add_argument("--out-root", default=None, help="Build output root if run-dir is unavailable")
    p.add_argument("--baseline-root", default=str(default_baseline_root()), help="Trusted baseline root with Panels/ and Checks/")
    p.add_argument("--years", default="2004:2023", help='Year span, e.g. "2004:2023"')
    p.add_argument("--summary-csv", default=None, help="Optional output CSV path for certification summary")
    p.add_argument("--summary-md", default=None, help="Optional output markdown path for certification summary")
    p.add_argument("--log-file", default=str(checks_root / "logs" / "04_certify_analysis_build.log"), help="Optional log file path")
    return p.parse_args()


def main() -> None:
    args = parse_args()
    setup_logging(args.log_file)

    if not args.run_dir and not args.out_root:
        raise SystemExit("provide --run-dir or --out-root")

    run_dir = Path(args.run_dir).resolve() if args.run_dir else None
    telemetry = read_json(run_dir / "build_telemetry.json") if run_dir else {}
    meta = read_json(run_dir / "run_meta.json") if run_dir else {}
    out_root_value = args.out_root or telemetry.get("out_root") or meta.get("out_root")
    if not out_root_value:
        raise SystemExit("could not resolve output root from --out-root or run metadata")
    out_root = Path(out_root_value).resolve()

    years = parse_years_spec(args.years)
    baseline_root = Path(args.baseline_root).resolve()
    baseline_wide_parts = baseline_root / "Panels" / "wide_analysis_parts"
    baseline_wide_glob = part_glob(baseline_wide_parts)
    baseline_scalar = baseline_root / "Panels" / "panel_long_scalar_unique.parquet"
    baseline_dim = baseline_root / "Panels" / "panel_long_dim.parquet"
    baseline_wide_qc = baseline_root / "Checks" / "wide_qc"
    baseline_disc_qc = baseline_root / "Checks" / "disc_qc"

    run_wide_parts = out_root / "wide_parts"
    run_wide_glob = part_glob(run_wide_parts)
    run_wide_stitched = out_root / "Panels" / "panel_wide_analysis.parquet"
    run_scalar = out_root / "Panels" / "panel_analysis_scalar_long.parquet"
    run_dim = out_root / "Panels" / "panel_analysis_dim_long.parquet"
    run_wide_qc = out_root / "Checks" / "wide_qc"
    run_disc_qc = out_root / "Checks" / "disc_qc"

    if args.summary_csv:
        summary_csv = Path(args.summary_csv)
    elif run_dir:
        summary_csv = run_dir / "certification_summary.csv"
    else:
        summary_csv = out_root / "Checks" / "certification_summary.csv"
    if args.summary_md:
        summary_md = Path(args.summary_md)
    elif run_dir:
        summary_md = run_dir / "certification_summary.md"
    else:
        summary_md = out_root / "Checks" / "certification_summary.md"
    summary_csv.parent.mkdir(parents=True, exist_ok=True)
    summary_md.parent.mkdir(parents=True, exist_ok=True)

    command = meta.get("command", [])

    rows: list[dict] = []

    def add_check(
        section: str,
        check: str,
        *,
        status: str,
        actual="",
        expected="",
        required: bool = True,
        details: str = "",
    ) -> None:
        rows.append(
            {
                "section": section,
                "check": check,
                "status": status,
                "required": bool(required),
                "actual": actual,
                "expected": expected,
                "details": details,
            }
        )

    def add_bool_check(section: str, check: str, actual, expected, match: bool, *, required: bool = True, details: str = "") -> None:
        add_check(
            section,
            check,
            status="passed" if match else "failed",
            actual=actual,
            expected=expected,
            required=required,
            details=details,
        )

    def add_skip(section: str, check: str, details: str, *, required: bool = False) -> None:
        add_check(section, check, status="skipped", required=required, details=details)

    expected_year_set = set(years)
    run_years = existing_years(run_wide_parts)

    add_bool_check(
        "release_gate",
        "build_completed",
        telemetry.get("termination_reason", ""),
        "completed",
        telemetry.get("termination_reason") == "completed" and telemetry.get("returncode") == 0,
    )
    add_bool_check(
        "release_gate",
        "wide_parts_complete",
        ",".join(map(str, run_years)),
        ",".join(map(str, years)),
        set(run_years) == expected_year_set,
    )
    add_bool_check(
        "release_gate",
        "stitched_wide_present",
        run_wide_stitched.exists(),
        True,
        run_wide_stitched.exists(),
    )
    expects_scalar = "--scalar-long-out" in command if command else True
    expects_dim = "--dim-long-out" in command if command else True
    if expects_scalar:
        add_bool_check("release_gate", "scalar_long_present", run_scalar.exists(), True, run_scalar.exists())
    if expects_dim:
        add_bool_check("release_gate", "dim_long_present", run_dim.exists(), True, run_dim.exists())
    add_bool_check("release_gate", "wide_qc_present", run_wide_qc.exists(), True, run_wide_qc.exists())

    if command:
        def cmd_value(flag: str) -> str | None:
            if flag not in command:
                return None
            idx = command.index(flag)
            if idx + 1 >= len(command):
                return None
            return command[idx + 1]

        add_bool_check("metadata_integrity", "years_recorded", cmd_value("--years"), args.years, cmd_value("--years") == args.years)
        add_bool_check("metadata_integrity", "lane_split_recorded", "--lane-split" in command, True, "--lane-split" in command)
        add_bool_check("metadata_integrity", "typed_output_recorded", "--typed-output" in command, True, "--typed-output" in command)
        add_bool_check(
            "metadata_integrity",
            "bucket_count_recorded",
            cmd_value("--scalar-conflict-buckets"),
            "present",
            cmd_value("--scalar-conflict-buckets") is not None,
            details="monitored runs should record explicit bucket settings",
        )
        add_bool_check(
            "metadata_integrity",
            "bucket_min_year_recorded",
            cmd_value("--scalar-conflict-bucket-min-year"),
            "present",
            cmd_value("--scalar-conflict-bucket-min-year") is not None,
            details="monitored runs should record explicit bucket settings",
        )
        add_bool_check(
            "metadata_integrity",
            "memory_limit_recorded",
            cmd_value("--duckdb-memory-limit"),
            "present",
            cmd_value("--duckdb-memory-limit") is not None,
            details="monitored runs should record explicit DuckDB memory policy",
        )
    else:
        add_skip("metadata_integrity", "run_meta_missing", "run_meta.json command not available", required=True)

    if run_wide_stitched.exists() and baseline_wide_parts.exists():
        run_total_rows = parquet_count(str(run_wide_stitched))
        base_total_rows = parquet_count(baseline_wide_glob)
        add_bool_check("whole_window_spine", "total_row_count", run_total_rows, base_total_rows, run_total_rows == base_total_rows)

        run_spine = query_df(
            f"""
            SELECT COUNT(*) AS n
            FROM (
                SELECT DISTINCT year, UNITID
                FROM read_parquet({sql_quote(str(run_wide_stitched))})
            )
            """
        )
        base_spine = query_df(
            f"""
            SELECT COUNT(*) AS n
            FROM (
                SELECT DISTINCT year, UNITID
                FROM read_parquet({sql_quote(baseline_wide_glob)})
            )
            """
        )
        add_bool_check(
            "whole_window_spine",
            "distinct_unitid_year_count",
            int(run_spine.iloc[0]["n"]),
            int(base_spine.iloc[0]["n"]),
            int(run_spine.iloc[0]["n"]) == int(base_spine.iloc[0]["n"]),
        )

        run_rows_by_year = query_df(
            f"""
            SELECT year, COUNT(*) AS rows
            FROM read_parquet({sql_quote(str(run_wide_stitched))})
            GROUP BY year
            ORDER BY year
            """
        )
        base_rows_by_year = query_df(
            f"""
            SELECT year, COUNT(*) AS rows
            FROM read_parquet({sql_quote(baseline_wide_glob)})
            GROUP BY year
            ORDER BY year
            """
        )
        add_bool_check(
            "whole_window_spine",
            "row_counts_by_year",
            run_rows_by_year.to_json(orient="records"),
            base_rows_by_year.to_json(orient="records"),
            compare_df(run_rows_by_year, base_rows_by_year, ["year"]),
        )

        run_spine_by_year = query_df(
            f"""
            SELECT year, COUNT(*) AS distinct_unitid_year
            FROM (
                SELECT DISTINCT year, UNITID
                FROM read_parquet({sql_quote(str(run_wide_stitched))})
            )
            GROUP BY year
            ORDER BY year
            """
        )
        base_spine_by_year = query_df(
            f"""
            SELECT year, COUNT(*) AS distinct_unitid_year
            FROM (
                SELECT DISTINCT year, UNITID
                FROM read_parquet({sql_quote(baseline_wide_glob)})
            )
            GROUP BY year
            ORDER BY year
            """
        )
        add_bool_check(
            "whole_window_spine",
            "distinct_unitid_year_by_year",
            run_spine_by_year.to_json(orient="records"),
            base_spine_by_year.to_json(orient="records"),
            compare_df(run_spine_by_year, base_spine_by_year, ["year"]),
        )

        run_schema = describe_parquet(str(run_wide_stitched))
        base_schema = describe_parquet(baseline_wide_glob)
        run_cols = run_schema["column_name"].tolist()
        base_cols = base_schema["column_name"].tolist()
        add_bool_check("schema_parity", "wide_column_order", "|".join(run_cols), "|".join(base_cols), run_cols == base_cols)
        add_bool_check(
            "schema_parity",
            "wide_column_types",
            run_schema[["column_name", "column_type"]].to_json(orient="records"),
            base_schema[["column_name", "column_type"]].to_json(orient="records"),
            compare_df(run_schema[["column_name", "column_type"]], base_schema[["column_name", "column_type"]], ["column_name"]),
        )

        shared_cols = [col for col in run_cols if col in set(base_cols)]
        if shared_cols:
            left_only, right_only = content_diff_counts(str(run_wide_stitched), baseline_wide_glob, shared_cols)
            add_bool_check(
                "content_parity",
                "wide_shared_columns",
                json.dumps({"left_only": left_only, "right_only": right_only}),
                json.dumps({"left_only": 0, "right_only": 0}),
                left_only == 0 and right_only == 0,
            )
        else:
            add_skip("content_parity", "wide_shared_columns", "no shared columns between run and baseline", required=True)

        per_year_ok = True
        per_year_details: list[dict] = []
        for year in years:
            run_part = part_path(run_wide_parts, year)
            base_part = part_path(baseline_wide_parts, year)
            if not run_part.exists() or not base_part.exists():
                per_year_ok = False
                per_year_details.append({"year": year, "status": "missing_part"})
                continue
            run_count = parquet_count(str(run_part))
            base_count = parquet_count(str(base_part))
            year_ok = run_count == base_count
            shared_left, shared_right = content_diff_counts(str(run_part), str(base_part), shared_cols)
            year_ok = year_ok and shared_left == 0 and shared_right == 0
            per_year_ok = per_year_ok and year_ok
            per_year_details.append(
                {
                    "year": year,
                    "run_rows": run_count,
                    "base_rows": base_count,
                    "left_only": shared_left,
                    "right_only": shared_right,
                }
            )
        add_bool_check(
            "per_year_partition_parity",
            "wide_parts_all_years",
            json.dumps(per_year_details),
            "all years exact on shared columns and row counts",
            per_year_ok,
        )
    else:
        add_skip("whole_window_spine", "baseline_wide_missing", "run stitched wide or baseline wide parts missing", required=True)

    run_anti = csv_df(run_wide_qc / "qc_anti_garbage_failures.csv", ["blocked_identifier_column"])
    base_anti = csv_df(baseline_wide_qc / "qc_anti_garbage_failures.csv", ["blocked_identifier_column"])
    add_bool_check(
        "qc_parity",
        "anti_garbage",
        run_anti.to_json(orient="records"),
        base_anti.to_json(orient="records"),
        compare_df(run_anti, base_anti, ["blocked_identifier_column"]),
    )

    run_cast = csv_df(run_wide_qc / "qc_cast_report.csv", ["year", "column"])
    base_cast = csv_df(baseline_wide_qc / "qc_cast_report.csv", ["year", "column"])
    if not base_cast.empty:
        add_bool_check(
            "qc_parity",
            "cast_report",
            run_cast.to_json(orient="records"),
            base_cast.to_json(orient="records"),
            compare_df(run_cast, base_cast, ["year", "column"]),
        )
    else:
        add_skip("qc_parity", "cast_report", "baseline cast report missing", required=False)

    run_nulls = csv_df(run_wide_qc / "qc_globally_null_columns_dropped.csv", ["column"])
    base_nulls = csv_df(baseline_wide_qc / "qc_globally_null_columns_dropped.csv", ["column"])
    add_bool_check(
        "qc_parity",
        "globally_null_drop",
        run_nulls.to_json(orient="records"),
        base_nulls.to_json(orient="records"),
        compare_df(run_nulls, base_nulls, ["column"]),
    )

    run_disc = aggregate_disc_conflicts(run_disc_qc)
    base_disc = aggregate_disc_conflicts(baseline_disc_qc)
    add_bool_check(
        "qc_parity",
        "disc_conflicts",
        run_disc.to_json(orient="records"),
        base_disc.to_json(orient="records"),
        compare_df(run_disc, base_disc, ["year", "base"]),
    )

    run_qc_summary = csv_df(run_wide_qc / "wide_panel_qc_summary.csv", ["year"])
    base_qc_summary = csv_df(baseline_wide_qc / "wide_panel_qc_summary.csv", ["year"])
    add_bool_check(
        "qc_parity",
        "wide_qc_summary",
        run_qc_summary.to_json(orient="records"),
        base_qc_summary.to_json(orient="records"),
        compare_df(run_qc_summary, base_qc_summary, ["year"]),
    )

    run_seeded = csv_df(run_wide_qc / "qc_seeded_legacy_columns.csv", ["column_name"])
    manifest_path = default_repo_root() / "Artifacts" / "legacy_analysis_schema_seed.csv"
    manifest_df = csv_df(manifest_path, ["column_name"])
    if not run_seeded.empty and not manifest_df.empty:
        manifest_names = manifest_df[["column_name"]].drop_duplicates().sort_values(["column_name"], kind="mergesort").reset_index(drop=True)
        run_seeded_names = run_seeded[["column_name"]].drop_duplicates().sort_values(["column_name"], kind="mergesort").reset_index(drop=True)
        add_bool_check(
            "legacy_compatibility",
            "seeded_legacy_names",
            run_seeded_names.to_json(orient="records"),
            manifest_names.to_json(orient="records"),
            compare_df(run_seeded_names, manifest_names, ["column_name"]),
        )
    else:
        add_skip("legacy_compatibility", "seeded_legacy_names", "run seeded legacy QC or manifest missing", required=False)

    run_scalar_conflicts = csv_df(run_wide_qc / "qc_scalar_conflicts.csv")
    base_scalar_conflicts = csv_df(baseline_wide_qc / "qc_scalar_conflicts.csv")
    if run_scalar_conflicts.empty and base_scalar_conflicts.empty:
        add_bool_check("scalar_conflict_certification", "scalar_conflict_qc_absent", 0, 0, True)
    elif not run_scalar_conflicts.empty and not base_scalar_conflicts.empty:
        sort_cols = [col for col in ["year", "UNITID", "varnumber", "source_file", "value"] if col in run_scalar_conflicts.columns]
        add_bool_check(
            "scalar_conflict_certification",
            "scalar_conflict_qc_content",
            run_scalar_conflicts.to_json(orient="records"),
            base_scalar_conflicts.to_json(orient="records"),
            compare_df(run_scalar_conflicts, base_scalar_conflicts, sort_cols),
        )
    else:
        add_skip("scalar_conflict_certification", "scalar_conflict_qc_partial", "one side has scalar conflict QC while the other does not", required=False)

    target_lineage = csv_df(run_wide_qc / "qc_target_lineage.csv")
    if not target_lineage.empty:
        add_bool_check(
            "target_lineage",
            "target_lineage_present",
            len(target_lineage),
            ">0",
            len(target_lineage) > 0,
            required=False,
        )
    else:
        add_skip("target_lineage", "target_lineage_present", "target lineage QC missing", required=False)

    summary = pd.DataFrame(rows)
    has_failures = bool((summary["status"] == "failed").any())
    has_skips = bool((summary["status"] == "skipped").any())
    if has_failures:
        final_status = "Not certified"
    elif has_skips:
        final_status = "Conditionally certified"
    else:
        final_status = "Certified parity"

    summary.to_csv(summary_csv, index=False)

    passed_count = int((summary["status"] == "passed").sum())
    failed_count = int((summary["status"] == "failed").sum())
    skipped_count = int((summary["status"] == "skipped").sum())
    lines = [
        f"# Analysis Build Certification",
        "",
        f"- Status: {final_status}",
        f"- Years: {args.years}",
        f"- Run Dir: {run_dir if run_dir else '<none>'}",
        f"- Out Root: {out_root}",
        f"- Baseline Root: {baseline_root}",
        f"- Passed Checks: {passed_count}",
        f"- Failed Checks: {failed_count}",
        f"- Skipped Checks: {skipped_count}",
        "",
        "| Section | Check | Status | Required | Actual | Expected | Details |",
        "| --- | --- | --- | --- | --- | --- | --- |",
    ]
    for row in summary.to_dict("records"):
        lines.append(
            "| {section} | {check} | {status} | {required} | {actual} | {expected} | {details} |".format(
                section=row["section"],
                check=row["check"],
                status=row["status"],
                required="yes" if row["required"] else "no",
                actual=str(row["actual"]).replace("|", "/"),
                expected=str(row["expected"]).replace("|", "/"),
                details=str(row["details"]).replace("|", "/"),
            )
        )
    summary_md.write_text("\n".join(lines) + "\n")

    print(f"Wrote {summary_csv}")
    print(f"Wrote {summary_md}")
    print(f"Final status: {final_status}")
    if final_status == "Not certified":
        raise SystemExit(1)


if __name__ == "__main__":
    main()
