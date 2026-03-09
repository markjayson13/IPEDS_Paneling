#!/usr/bin/env python3
"""
Repository orchestrator for the IPEDS panel pipeline.

This wrapper runs per-year harmonization, stitches the long panel, optionally
builds wide panels, optionally runs PRCH cleaning, and can finish with a custom
panel extract. Defaults target the standard IPEDS_ROOT layout
(Raw_Cross_Section_Data, Dictionary, Panels, Checks).
"""
from __future__ import annotations

import argparse
import subprocess
import sys
from pathlib import Path
import os

import pyarrow.parquet as pq

REPO_ROOT = Path(os.environ.get("IPEDS_ROOT", Path(__file__).resolve().parents[1]))
SCRIPTS_DIR = Path(__file__).resolve().parent

DEFAULT_ROOT = REPO_ROOT / "Raw_Cross_Section_Data"
DEFAULT_LAKE = REPO_ROOT / "Dictionary" / "dictionary_lake.parquet"
DEFAULT_CROSS = REPO_ROOT / "Cross_sections"
DEFAULT_PARTS = REPO_ROOT / "Cross_sections"
DEFAULT_STITCH = REPO_ROOT / "Panels" / "2004-2024" / "panel_long_varnum_2004_2024.parquet"
DEFAULT_WIDE_OUT = REPO_ROOT / "Panels" / "wide_analysis_parts"
DEFAULT_DISC_QC = REPO_ROOT / "Checks" / "disc_qc"
DEFAULT_WIDE_QC = REPO_ROOT / "Checks" / "wide_qc"
DEFAULT_WIDE_STITCH = REPO_ROOT / "Panels" / "2004_2024_IPEDS_Raw_Panel_DS.parquet"
DEFAULT_CLEAN_PANEL = REPO_ROOT / "Panels" / "2004_2024_IPEDS_clean_Panel_DS.parquet"
DEFAULT_CUSTOM_OUT = REPO_ROOT / "Panels" / "custom_panel.parquet"
DEFAULT_PRCH_CLEAN = REPO_ROOT / "Panels" / "2004_2024_IPEDS_PRCHclean_Panel_DS.parquet"
DEFAULT_PRCH_QC = REPO_ROOT / "Checks" / "prch_qc"
DEFAULT_RELEASE_QC = REPO_ROOT / "Checks" / "release_qc"
DEFAULT_LOG_DIR = REPO_ROOT / "Checks" / "logs"


def parse_years(spec: str) -> list[int]:
    if ":" in spec:
        start, end = spec.split(":")
        return list(range(int(start), int(end) + 1))
    return [int(x) for x in spec.split(",") if x.strip()]


def run(cmd: list[str], dry_run: bool) -> None:
    print("+", " ".join(cmd))
    if dry_run:
        return
    res = subprocess.run(cmd, check=False)
    if res.returncode != 0:
        sys.exit(res.returncode)


def stitch_years(
    years: list[int],
    base_dir: Path,
    out_path: Path,
    skip_missing: bool = True,
) -> None:
    out_path.parent.mkdir(parents=True, exist_ok=True)
    writer = None
    skipped = []
    for y in years:
        p = base_dir / f"panel_long_varnum_{y}.parquet"
        if not p.exists():
            skipped.append((y, "missing"))
            if skip_missing:
                continue
            raise FileNotFoundError(p)
        try:
            pf = pq.ParquetFile(p)
        except Exception as e:
            skipped.append((y, f"bad: {e}"))
            if skip_missing:
                continue
            raise
        for batch in pf.iter_batches():
            if writer is None:
                writer = pq.ParquetWriter(out_path, batch.schema)
            writer.write_batch(batch)
    if writer:
        writer.close()
        print(f"Wrote {out_path}")
    else:
        print("[warn] no valid inputs found")
    if skipped:
        print("Skipped years:")
        for y, reason in skipped:
            print(y, reason)


def stitch_wide_partitioned(in_dir: Path, out_path: Path) -> None:
    import pyarrow.dataset as ds

    out_path.parent.mkdir(parents=True, exist_ok=True)
    dataset = ds.dataset(str(in_dir), format="parquet", partitioning="hive")
    writer = None
    for i, batch in enumerate(dataset.to_batches(), start=1):
        if writer is None:
            writer = pq.ParquetWriter(out_path, batch.schema)
        writer.write_batch(batch)
        if i % 100 == 0:
            print(f"[wide-stitch] wrote {i} batches...")
    if writer:
        writer.close()
        print(f"Wrote {out_path}")
    else:
        print(f"[warn] no batches found in {in_dir}")


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--root", default=str(DEFAULT_ROOT), help="Raw_Cross_Section_Data root")
    ap.add_argument("--lake", default=str(DEFAULT_LAKE), help="dictionary_lake.parquet path")
    ap.add_argument("--years", default="2004:2024", help="Year span, e.g. 2004:2024")
    ap.add_argument("--cross-sections-dir", default=str(DEFAULT_CROSS), help="Output dir for per-year long panels")
    ap.add_argument("--parts-dir-base", default=str(DEFAULT_PARTS), help="Base dir for per-year parquet parts")
    ap.add_argument("--skip-existing", action=argparse.BooleanOptionalAction, default=True, help="Skip per-year build if output exists")
    ap.add_argument("--stitch", action=argparse.BooleanOptionalAction, default=True, help="Stitch per-year outputs into one file")
    ap.add_argument("--stitch-out", default=str(DEFAULT_STITCH), help="Output path for stitched long panel")
    ap.add_argument("--skip-missing", action=argparse.BooleanOptionalAction, default=True, help="Skip missing/bad years during stitch")
    ap.add_argument("--cleanup-year-longs", action=argparse.BooleanOptionalAction, default=False, help="Delete per-year long parquet files after successful stitch")
    ap.add_argument("--build-wide", action=argparse.BooleanOptionalAction, default=True, help="Build wide panel after stitching")
    ap.add_argument("--wide-out-dir", default=str(DEFAULT_WIDE_OUT), help="Output dir for wide panel (partitioned)")
    ap.add_argument("--wide-years", default=None, help="Years for wide build (default: --years)")
    ap.add_argument("--wide-write-single", default=None, help="Optional single wide parquet path")
    ap.add_argument("--wide-analysis-out", default=None, help="Optional analysis-wide parquet output (lane-split mode)")
    ap.add_argument("--lane-split", action=argparse.BooleanOptionalAction, default=False, help="Build scalar+dimension lanes and analysis-wide panel")
    ap.add_argument("--scalar-long-out", default=None, help="Optional output parquet for scalar long lane")
    ap.add_argument("--dim-long-out", default=None, help="Optional output parquet for dimensioned long lane")
    ap.add_argument("--dim-sources", default="IC_CAMPUSES,IC_PCCAMPUSES,F_FA_F,F_FA_G", help="Exact source_file names treated as dimensioned")
    ap.add_argument("--dim-prefixes", default="C_,EF,GR,GR200,SAL,S_,OM,DRV", help="Comma-separated source_file prefixes treated as dimensioned")
    ap.add_argument("--exclude-vars", default=None, help="Comma-separated varnames to exclude from wide output")
    ap.add_argument("--typed-output", action=argparse.BooleanOptionalAction, default=False, help="Coerce numeric variables using dictionary metadata")
    ap.add_argument("--drop-empty-cols", action=argparse.BooleanOptionalAction, default=False, help="Drop vars empty across selected years")
    ap.add_argument("--drop-globally-null-post", action=argparse.BooleanOptionalAction, default=True, help="Drop globally-null columns in final stitched output")
    ap.add_argument("--legacy-analysis-schema", action=argparse.BooleanOptionalAction, default=True, help="Seed legacy-compatible analysis-wide placeholder columns")
    ap.add_argument("--legacy-schema-seed-manifest", default=None, help="Optional override for the legacy compatibility seed manifest")
    ap.add_argument("--anti-garbage-ids", default="CIPCODE,LINE,FORMID,FUNCTCD,MAJORNUM", help="Dimension identifiers that should not survive as scalar columns")
    ap.add_argument("--drop-anti-garbage-cols", action=argparse.BooleanOptionalAction, default=True, help="Drop anti-garbage blocked identifier columns")
    ap.add_argument("--fail-on-anti-garbage", action=argparse.BooleanOptionalAction, default=True, help="Fail if anti-garbage identifiers remain in wide targets")
    ap.add_argument("--fail-on-scalar-conflicts", action=argparse.BooleanOptionalAction, default=True, help="Fail if scalar lane has conflicting values")
    ap.add_argument("--scan-batch-rows", type=int, default=200_000, help="Batch size for scanning long input in 04_build_wide_panel.py")
    ap.add_argument("--stitch-wide", action=argparse.BooleanOptionalAction, default=False, help="Stitch partitioned wide output into a single file")
    ap.add_argument("--stitch-wide-out", default=str(DEFAULT_WIDE_STITCH), help="Output path for stitched wide panel")
    ap.add_argument("--collapse-disc", action=argparse.BooleanOptionalAction, default=True, help="Collapse discrete groups in wide builder")
    ap.add_argument("--drop-disc-components", action=argparse.BooleanOptionalAction, default=True, help="Drop component vars after collapse")
    ap.add_argument("--disc-qc-dir", default=str(DEFAULT_DISC_QC), help="QC dir for disc conflicts")
    ap.add_argument("--disc-exclude", default=None, help="Comma-separated base names to skip collapsing")
    ap.add_argument("--disc-suffix", default=None, help="Suffix for collapsed disc vars (default in script)")
    ap.add_argument("--qc-dir", default=str(DEFAULT_WIDE_QC), help="QC dir for wide summary")
    ap.add_argument("--dry-run", action="store_true", help="Print commands without executing")
    ap.add_argument("--release-allow", default="revised,final", help="Comma list of allowed release statuses")
    ap.add_argument("--release-strict", action=argparse.BooleanOptionalAction, default=True, help="Fail if manifest is missing or not revised/final")
    ap.add_argument("--release-qc-dir", default=str(DEFAULT_RELEASE_QC), help="QC dir for release validation")
    ap.add_argument("--harmonize-chunksize", type=int, default=50_000, help="Row chunksize for 03_harmonize (lower uses less RAM)")
    ap.add_argument("--harmonize-value-cols-per-chunk", type=int, default=250, help="Max value columns per melt chunk in 03_harmonize")
    ap.add_argument("--dedupe", action=argparse.BooleanOptionalAction, default=True, help="Deterministically drop duplicate (UNITID, year, varname)")
    ap.add_argument("--dedupe-priority", default=None, help="Override source_file priority list for dedupe")
    ap.add_argument("--final-dedupe", action=argparse.BooleanOptionalAction, default=True, help="Run final DuckDB dedupe after write")
    ap.add_argument("--duckdb-temp-dir", default=None, help="Temp directory for DuckDB during dedupe")
    ap.add_argument("--duckdb-path", default=None, help="Optional persistent DuckDB path for 04_build_wide_panel.py")
    ap.add_argument("--persist-duckdb", action=argparse.BooleanOptionalAction, default=True, help="Persist DuckDB build state for 04_build_wide_panel.py")
    ap.add_argument("--log-dir", default=str(DEFAULT_LOG_DIR), help="Directory for per-step logs")
    ap.add_argument("--build-custom", action=argparse.BooleanOptionalAction, default=False, help="Build a custom wide panel from the cleaned panel")
    ap.add_argument("--custom-input", default=str(DEFAULT_CLEAN_PANEL), help="Input wide panel for custom extraction")
    ap.add_argument("--custom-out", default=str(DEFAULT_CUSTOM_OUT), help="Output path for custom panel")
    ap.add_argument("--custom-vars", default=None, help="Comma-separated vars for custom panel")
    ap.add_argument("--custom-vars-file", default=None, help="File of vars for custom panel")
    ap.add_argument("--custom-years", default=None, help="Optional year filter for custom panel")
    ap.add_argument("--custom-format", choices=["parquet", "csv"], default="parquet", help="Custom panel output format")
    ap.add_argument("--custom-batch-rows", type=int, default=100_000, help="Batch size for custom panel export")
    ap.add_argument("--run-cleaning", action=argparse.BooleanOptionalAction, default=False, help="Run PRCH cleaning after wide stitch")
    ap.add_argument("--prch-clean-out", default=str(DEFAULT_PRCH_CLEAN), help="Output path for PRCH cleaned panel")
    ap.add_argument("--clean-out", default=str(DEFAULT_CLEAN_PANEL), help="Output path for research-ready clean panel")
    ap.add_argument("--prch-qc-dir", default=str(DEFAULT_PRCH_QC), help="QC dir for PRCH cleaning")
    ap.add_argument("--drop-imputation-flags", action=argparse.BooleanOptionalAction, default=False, help="Drop X* imputation flags in clean output")
    args = ap.parse_args()

    years = parse_years(args.years)
    cross_dir = Path(args.cross_sections_dir)
    cross_dir.mkdir(parents=True, exist_ok=True)
    log_dir = Path(args.log_dir)
    log_dir.mkdir(parents=True, exist_ok=True)

    # Step 1: harmonize per-year
    for y in years:
        out = cross_dir / f"panel_long_varnum_{y}.parquet"
        if args.skip_existing and out.exists():
            print(f"[skip] {out}")
            continue
        cmd = [
            sys.executable,
            str(SCRIPTS_DIR / "03_harmonize.py"),
            "--root",
            args.root,
            "--lake",
            args.lake,
            "--years",
            f"{y}:{y}",
            "--output",
            str(out),
        ]
        cmd += ["--chunksize", str(args.harmonize_chunksize)]
        cmd += ["--value-cols-per-chunk", str(args.harmonize_value_cols_per_chunk)]
        cmd += ["--dedupe" if args.dedupe else "--no-dedupe"]
        cmd += ["--final-dedupe" if args.final_dedupe else "--no-final-dedupe"]
        if args.dedupe_priority:
            cmd += ["--dedupe-priority", args.dedupe_priority]
        if args.duckdb_temp_dir:
            cmd += ["--duckdb-temp-dir", args.duckdb_temp_dir]
        cmd += ["--log-file", str(log_dir / f"03_harmonize_{y}.log")]
        if args.release_allow:
            cmd += ["--release-allow", args.release_allow]
        cmd += ["--release-strict" if args.release_strict else "--no-release-strict"]
        if args.release_qc_dir:
            cmd += ["--release-qc-dir", args.release_qc_dir]
        if args.parts_dir_base:
            parts_dir = Path(args.parts_dir_base) / f"parts_{y}"
            cmd += ["--parts-dir", str(parts_dir)]
        run(cmd, args.dry_run)

    # Step 2: stitch (optional)
    stitched_path = None
    if args.stitch:
        if not args.stitch_out:
            raise SystemExit("--stitch-out is required when --stitch is set")
        stitched_path = Path(args.stitch_out)
        if not args.dry_run:
            stitch_years(years, cross_dir, stitched_path, skip_missing=args.skip_missing)
            if args.cleanup_year_longs:
                try:
                    pq.ParquetFile(stitched_path)
                except Exception as e:
                    raise SystemExit(f"Refusing to cleanup per-year files; stitched file is not readable: {e}")
                removed = 0
                for y in years:
                    p = cross_dir / f"panel_long_varnum_{y}.parquet"
                    if p.exists():
                        p.unlink()
                        removed += 1
                print(f"[cleanup] removed {removed} per-year long files")
        else:
            print(f"+ stitch {cross_dir} -> {stitched_path}")

    # Step 3: build wide (optional)
    if args.build_wide:
        wide_input = stitched_path if stitched_path else None
        if wide_input is None:
            raise SystemExit("Wide build requires --stitch or an existing stitched file")
        if not args.wide_out_dir:
            raise SystemExit("--wide-out-dir is required when --build-wide is set")
        wide_years = args.wide_years if args.wide_years else args.years
        cmd = [
            sys.executable,
            str(SCRIPTS_DIR / "04_build_wide_panel.py"),
            "--input",
            str(wide_input),
            "--out_dir",
            args.wide_out_dir,
            "--years",
            wide_years,
            "--dictionary",
            args.lake,
        ]
        cmd += ["--lane-split" if args.lane_split else "--no-lane-split"]
        cmd += ["--dim-sources", args.dim_sources]
        cmd += ["--dim-prefixes", args.dim_prefixes]
        if args.exclude_vars:
            cmd += ["--exclude-vars", args.exclude_vars]
        if args.scalar_long_out:
            cmd += ["--scalar-long-out", args.scalar_long_out]
        if args.dim_long_out:
            cmd += ["--dim-long-out", args.dim_long_out]
        if args.wide_analysis_out:
            cmd += ["--wide-analysis-out", args.wide_analysis_out]
        cmd += ["--typed-output" if args.typed_output else "--no-typed-output"]
        cmd += ["--drop-empty-cols" if args.drop_empty_cols else "--no-drop-empty-cols"]
        cmd += ["--drop-globally-null-post" if args.drop_globally_null_post else "--no-drop-globally-null-post"]
        cmd += ["--legacy-analysis-schema" if args.legacy_analysis_schema else "--no-legacy-analysis-schema"]
        if args.legacy_schema_seed_manifest:
            cmd += ["--legacy-schema-seed-manifest", args.legacy_schema_seed_manifest]
        cmd += ["--anti-garbage-ids", args.anti_garbage_ids]
        cmd += ["--drop-anti-garbage-cols" if args.drop_anti_garbage_cols else "--no-drop-anti-garbage-cols"]
        cmd += ["--fail-on-anti-garbage" if args.fail_on_anti_garbage else "--no-fail-on-anti-garbage"]
        cmd += ["--fail-on-scalar-conflicts" if args.fail_on_scalar_conflicts else "--no-fail-on-scalar-conflicts"]
        cmd += ["--scan-batch-rows", str(args.scan_batch_rows)]
        cmd += ["--persist-duckdb" if args.persist_duckdb else "--no-persist-duckdb"]
        if args.duckdb_path:
            cmd += ["--duckdb-path", args.duckdb_path]
        if args.duckdb_temp_dir:
            cmd += ["--duckdb-temp-dir", args.duckdb_temp_dir]
        if args.collapse_disc:
            cmd.append("--collapse-disc")
        if args.drop_disc_components:
            cmd.append("--drop-disc-components")
        if args.disc_qc_dir:
            cmd += ["--disc-qc-dir", args.disc_qc_dir]
        if args.disc_exclude:
            cmd += ["--disc-exclude", args.disc_exclude]
        if args.disc_suffix:
            cmd += ["--disc-suffix", args.disc_suffix]
        if args.qc_dir:
            cmd += ["--qc-dir", args.qc_dir]
        if args.wide_write_single:
            cmd += ["--write_single", args.wide_write_single]
        cmd += ["--log-file", str(log_dir / "04_build_wide_panel.log")]
        run(cmd, args.dry_run)

    # Step 4: stitch wide partitions (optional)
    if args.stitch_wide:
        wide_out_dir = Path(args.wide_out_dir)
        stitch_out = Path(args.stitch_wide_out)
        if not args.dry_run:
            stitch_wide_partitioned(wide_out_dir, stitch_out)
        else:
            print(f"+ stitch wide {wide_out_dir} -> {stitch_out}")

    # Step 4.5: PRCH clean + research-ready clean (optional)
    if args.run_cleaning:
        raw_wide = Path(
            args.stitch_wide_out
            if args.stitch_wide
            else (args.wide_write_single or args.wide_analysis_out or args.stitch_wide_out)
        )
        if not raw_wide.exists() and not args.dry_run:
            raise SystemExit(f"Raw wide not found: {raw_wide}")
        # PRCH clean
        cmd = [
            sys.executable,
            str(SCRIPTS_DIR / "05_clean_panel.py"),
            "--input",
            str(raw_wide),
            "--output",
            args.prch_clean_out,
            "--dictionary",
            args.lake,
        ]
        if args.prch_qc_dir:
            cmd += ["--qc-dir", args.prch_qc_dir]
        cmd += ["--log-file", str(log_dir / "05_cleaning_panel_prch.log")]
        run(cmd, args.dry_run)

        # Research-ready clean (optionally drop X* flags)
        cmd = [
            sys.executable,
            str(SCRIPTS_DIR / "05_clean_panel.py"),
            "--input",
            str(raw_wide),
            "--output",
            args.clean_out,
            "--dictionary",
            args.lake,
        ]
        if args.prch_qc_dir:
            cmd += ["--qc-dir", args.prch_qc_dir]
        if args.drop_imputation_flags:
            cmd.append("--drop-imputation-flags")
        cmd += ["--log-file", str(log_dir / "05_cleaning_panel_clean.log")]
        run(cmd, args.dry_run)

    # Step 6: build a custom panel (optional)
    if args.build_custom:
        if not args.custom_vars and not args.custom_vars_file:
            raise SystemExit("Custom build requires --custom-vars or --custom-vars-file.")
        cmd = [
            sys.executable,
            str(SCRIPTS_DIR / "06_build_custom_panel.py"),
            "--input",
            args.custom_input,
            "--output",
            args.custom_out,
            "--format",
            args.custom_format,
            "--batch-rows",
            str(args.custom_batch_rows),
        ]
        if args.custom_vars:
            cmd += ["--vars", args.custom_vars]
        if args.custom_vars_file:
            cmd += ["--vars-file", args.custom_vars_file]
        if args.custom_years:
            cmd += ["--years", args.custom_years]
        cmd += ["--log-file", str(log_dir / "06_build_custom_panel.log")]
        run(cmd, args.dry_run)


if __name__ == "__main__":
    main()
