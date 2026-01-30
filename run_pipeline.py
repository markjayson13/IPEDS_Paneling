#!/usr/bin/env python3
"""
Wrapper to run harmonize.py per-year, stitch outputs, and optionally build a wide panel.
Defaults are baked in for 2004–2024 and the standard repo layout.
"""
from __future__ import annotations

import argparse
import subprocess
import sys
from pathlib import Path

import pyarrow.parquet as pq

BASE = Path("/Users/markjaysonfarol13/IPEDS_Paneling")
DEFAULT_ROOT = BASE / "Raw_Cross_Section_Data"
DEFAULT_LAKE = BASE / "Dictionary" / "dictionary_lake.parquet"
DEFAULT_CROSS = BASE / "Cross_sections"
DEFAULT_PARTS = BASE / "Cross_sections"
DEFAULT_STITCH = BASE / "Panels" / "2004-2024" / "panel_long_varnum_2004_2024.parquet"
DEFAULT_WIDE_OUT = BASE / "Panels" / "wide_2004_2024"
DEFAULT_DISC_QC = BASE / "Checks" / "disc_qc"
DEFAULT_WIDE_QC = BASE / "Checks" / "wide_qc"


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
    ap.add_argument("--build-wide", action=argparse.BooleanOptionalAction, default=True, help="Build wide panel after stitching")
    ap.add_argument("--wide-out-dir", default=str(DEFAULT_WIDE_OUT), help="Output dir for wide panel (partitioned)")
    ap.add_argument("--wide-years", default=None, help="Years for wide build (default: --years)")
    ap.add_argument("--wide-write-single", default=None, help="Optional single wide parquet path")
    ap.add_argument("--collapse-disc", action=argparse.BooleanOptionalAction, default=True, help="Collapse discrete groups in wide builder")
    ap.add_argument("--drop-disc-components", action=argparse.BooleanOptionalAction, default=True, help="Drop component vars after collapse")
    ap.add_argument("--disc-qc-dir", default=str(DEFAULT_DISC_QC), help="QC dir for disc conflicts")
    ap.add_argument("--disc-exclude", default=None, help="Comma-separated base names to skip collapsing")
    ap.add_argument("--disc-suffix", default=None, help="Suffix for collapsed disc vars (default in script)")
    ap.add_argument("--qc-dir", default=str(DEFAULT_WIDE_QC), help="QC dir for wide summary")
    ap.add_argument("--dry-run", action="store_true", help="Print commands without executing")
    args = ap.parse_args()

    years = parse_years(args.years)
    cross_dir = Path(args.cross_sections_dir)
    cross_dir.mkdir(parents=True, exist_ok=True)

    # Step 1: harmonize per-year
    for y in years:
        out = cross_dir / f"panel_long_varnum_{y}.parquet"
        if args.skip_existing and out.exists():
            print(f"[skip] {out}")
            continue
        cmd = [
            sys.executable,
            "harmonize.py",
            "--root",
            args.root,
            "--lake",
            args.lake,
            "--years",
            f"{y}:{y}",
            "--output",
            str(out),
        ]
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
            "Panels/03_build_wide_panel.py",
            "--input",
            str(wide_input),
            "--out_dir",
            args.wide_out_dir,
            "--years",
            wide_years,
            "--dictionary",
            args.lake,
        ]
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
        run(cmd, args.dry_run)


if __name__ == "__main__":
    main()
