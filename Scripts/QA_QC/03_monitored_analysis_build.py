#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import shutil
import signal
import subprocess
import sys
import time
from datetime import datetime
from pathlib import Path


def default_repo_root() -> Path:
    return Path(os.environ.get("IPEDS_ROOT", Path(__file__).resolve().parents[2]))


def parse_args() -> argparse.Namespace:
    repo_root = default_repo_root()
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--input", required=True, help="Stitched long parquet input")
    p.add_argument("--dictionary", required=True, help="dictionary_lake parquet")
    p.add_argument("--years", default="2004:2023", help='Year span, e.g. "2004:2023"')
    p.add_argument("--run-dir-root", default=str(repo_root / "Checks" / "real_parity_runs"), help="Durable log/telemetry directory")
    p.add_argument("--work-root", default="/tmp", help="Scratch root for build outputs and DuckDB state")
    p.add_argument("--label", default="analysis_2004_2023", help="Run id prefix")
    p.add_argument("--dim-sources", default="C_A,C_B,C_C,CDEP,EAP,IC_CAMPUSES,IC_PCCAMPUSES,F_FA_F,F_FA_G")
    p.add_argument("--dim-prefixes", default="C_,EF,GR,GR200,SAL,S_,OM,DRV")
    p.add_argument("--exclude-vars", default="SPORT1,SPORT2,SPORT3,SPORT4")
    p.add_argument("--scalar-conflict-buckets", type=int, default=16)
    p.add_argument("--scalar-conflict-bucket-min-year", type=int, default=2008)
    p.add_argument("--duckdb-memory-limit", default="8GB")
    p.add_argument("--poll-seconds", type=float, default=1.0)
    p.add_argument("--log-interval-seconds", type=int, default=60)
    p.add_argument("--terminal-heartbeat-seconds", type=float, default=30.0, help="Print a live terminal status line every N seconds; set to 0 to disable.")
    p.add_argument("--timeout-seconds", type=int, default=900)
    p.add_argument("--kill-if-no-partitions", action=argparse.BooleanOptionalAction, default=True)
    return p.parse_args()


def tree_size(path: Path) -> int:
    if not path.exists():
        return 0
    total = 0
    for root, _, files in os.walk(path):
        for name in files:
            fp = Path(root) / name
            try:
                total += fp.stat().st_size
            except FileNotFoundError:
                continue
    return total


def classify_termination(rc: int | None, reason: str) -> str:
    if reason != "completed":
        return reason
    if rc == 0:
        return "completed"
    if rc is None:
        return "unknown"
    if rc < 0:
        try:
            sig_name = signal.Signals(-rc).name.lower()
        except ValueError:
            sig_name = f"sig{-rc}"
        return f"signaled_{sig_name}"
    return f"exit_{rc}"


def last_phase(build_log: Path) -> str | None:
    if not build_log.exists():
        return None
    phase = None
    for line in build_log.read_text(errors="replace").splitlines():
        if line.startswith("[phase "):
            parts = line.split("] ", 1)
            if len(parts) == 2:
                phase = parts[1]
    return phase


def format_bytes(num_bytes: int) -> str:
    value = float(num_bytes)
    units = ["B", "KB", "MB", "GB", "TB"]
    for unit in units:
        if value < 1024.0 or unit == units[-1]:
            return f"{value:.1f}{unit}"
        value /= 1024.0
    return f"{num_bytes}B"


def format_elapsed(seconds: float) -> str:
    whole = max(0, int(seconds))
    hours, rem = divmod(whole, 3600)
    minutes, secs = divmod(rem, 60)
    return f"{hours:02d}:{minutes:02d}:{secs:02d}"


def emit_terminal_status(
    *,
    elapsed: float,
    partition_count: int,
    work_bytes: int,
    db_bytes: int,
    temp_bytes: int,
    phase: str | None,
) -> None:
    phase_text = phase or "phase not yet logged"
    print(
        "[tracker] "
        f"elapsed={format_elapsed(elapsed)} "
        f"partitions={partition_count} "
        f"work={format_bytes(work_bytes)} "
        f"duckdb={format_bytes(db_bytes)} "
        f"temp={format_bytes(temp_bytes)} "
        f"phase={phase_text}",
        flush=True,
    )


def main() -> None:
    args = parse_args()
    repo_root = default_repo_root()
    run_id = datetime.now().strftime(f"{args.label}_%Y%m%d_%H%M%S")
    run_dir = Path(args.run_dir_root) / run_id
    out_root = Path(args.work_root) / f"ipeds_real_parity_{run_id}"
    run_dir.mkdir(parents=True, exist_ok=True)
    if out_root.exists():
        shutil.rmtree(out_root)
    (out_root / "build").mkdir(parents=True, exist_ok=True)
    (out_root / "duckdb_tmp").mkdir(parents=True, exist_ok=True)
    (out_root / "wide_parts").mkdir(parents=True, exist_ok=True)
    (out_root / "Checks" / "wide_qc").mkdir(parents=True, exist_ok=True)
    (out_root / "Checks" / "disc_qc").mkdir(parents=True, exist_ok=True)
    (out_root / "Panels").mkdir(parents=True, exist_ok=True)

    build_log = run_dir / "build.log"
    monitor_log = run_dir / "monitor.log"
    telemetry_path = run_dir / "build_telemetry.json"
    meta_path = run_dir / "run_meta.json"

    cmd = [
        "python3",
        "-u",
        str(repo_root / "Scripts" / "04_build_wide_panel.py"),
        "--input",
        args.input,
        "--out_dir",
        str(out_root / "wide_parts"),
        "--dictionary",
        args.dictionary,
        "--years",
        args.years,
        "--lane-split",
        "--dim-sources",
        args.dim_sources,
        "--dim-prefixes",
        args.dim_prefixes,
        "--exclude-vars",
        args.exclude_vars,
        "--scalar-long-out",
        str(out_root / "Panels" / "panel_analysis_scalar_long.parquet"),
        "--dim-long-out",
        str(out_root / "Panels" / "panel_analysis_dim_long.parquet"),
        "--wide-analysis-out",
        str(out_root / "Panels" / "panel_wide_analysis.parquet"),
        "--typed-output",
        "--drop-empty-cols",
        "--collapse-disc",
        "--drop-disc-components",
        "--qc-dir",
        str(out_root / "Checks" / "wide_qc"),
        "--disc-qc-dir",
        str(out_root / "Checks" / "disc_qc"),
        "--duckdb-path",
        str(out_root / "build" / "ipeds_build.duckdb"),
        "--duckdb-temp-dir",
        str(out_root / "duckdb_tmp"),
        "--duckdb-memory-limit",
        args.duckdb_memory_limit,
        "--scalar-conflict-buckets",
        str(args.scalar_conflict_buckets),
        "--scalar-conflict-bucket-min-year",
        str(args.scalar_conflict_bucket_min_year),
        "--persist-duckdb",
    ]
    meta = {
        "run_id": run_id,
        "run_dir": str(run_dir),
        "out_root": str(out_root),
        "command": cmd,
        "started_at": datetime.now().isoformat(),
        "terminal_heartbeat_seconds": args.terminal_heartbeat_seconds,
    }
    meta_path.write_text(json.dumps(meta, indent=2))

    start = time.time()
    first_partition_seconds = None
    peak_work = peak_db = peak_temp = peak_parts = 0
    raw_reason = "completed"
    next_log_at = 0
    next_terminal_heartbeat_at = 0.0

    print(f"run_dir: {run_dir}")
    print(f"out_root: {out_root}")
    print(f"build_log: {build_log}")
    print(f"monitor_log: {monitor_log}")
    print(f"heartbeat_seconds: {args.terminal_heartbeat_seconds}")
    print("starting monitored analysis build...", flush=True)

    with build_log.open("w", buffering=1) as logf, monitor_log.open("w", buffering=1) as mon:
        mon.write(f"start {datetime.now().isoformat()}\n")
        proc = subprocess.Popen(cmd, cwd=str(repo_root), stdout=logf, stderr=subprocess.STDOUT)
        mon.write(f"pid {proc.pid}\n")
        mon.flush()
        print(f"pid: {proc.pid}", flush=True)

        while True:
            rc = proc.poll()
            elapsed = time.time() - start
            work_bytes = tree_size(out_root)
            db_bytes = tree_size(out_root / "build")
            temp_bytes = tree_size(out_root / "duckdb_tmp")
            partition_count = sum(1 for _ in (out_root / "wide_parts").glob("year=*/part.parquet"))
            phase = last_phase(build_log)
            peak_work = max(peak_work, work_bytes)
            peak_db = max(peak_db, db_bytes)
            peak_temp = max(peak_temp, temp_bytes)
            peak_parts = max(peak_parts, partition_count)
            if partition_count and first_partition_seconds is None:
                first_partition_seconds = elapsed

            if elapsed >= next_log_at:
                mon.write(
                    json.dumps(
                        {
                            "elapsed_seconds": round(elapsed, 3),
                            "work_bytes": work_bytes,
                            "db_bytes": db_bytes,
                            "temp_bytes": temp_bytes,
                            "partition_count": partition_count,
                        }
                    )
                    + "\n"
                )
                mon.flush()
                next_log_at += args.log_interval_seconds

            if args.terminal_heartbeat_seconds > 0 and elapsed >= next_terminal_heartbeat_at:
                emit_terminal_status(
                    elapsed=elapsed,
                    partition_count=partition_count,
                    work_bytes=work_bytes,
                    db_bytes=db_bytes,
                    temp_bytes=temp_bytes,
                    phase=phase,
                )
                next_terminal_heartbeat_at += args.terminal_heartbeat_seconds

            if rc is not None:
                break

            timed_out = elapsed >= args.timeout_seconds
            no_partitions = partition_count == 0
            if timed_out and (not args.kill_if_no_partitions or no_partitions):
                raw_reason = "killed_timeout_no_partitions" if no_partitions else "killed_timeout"
                proc.terminate()
                try:
                    proc.wait(timeout=20)
                except subprocess.TimeoutExpired:
                    raw_reason = f"{raw_reason}_force"
                    proc.kill()
                    proc.wait(timeout=20)
                break

            time.sleep(args.poll_seconds)

        rc = proc.wait()
        elapsed = time.time() - start
        work_bytes = tree_size(out_root)
        db_bytes = tree_size(out_root / "build")
        temp_bytes = tree_size(out_root / "duckdb_tmp")
        partition_count = sum(1 for _ in (out_root / "wide_parts").glob("year=*/part.parquet"))
        reason = classify_termination(rc, raw_reason)
        mon.write(f"end {datetime.now().isoformat()} rc={rc} reason={reason}\n")
        mon.flush()
        emit_terminal_status(
            elapsed=elapsed,
            partition_count=partition_count,
            work_bytes=work_bytes,
            db_bytes=db_bytes,
            temp_bytes=temp_bytes,
            phase=last_phase(build_log),
        )
        print(f"completed rc={rc} reason={reason}", flush=True)

    telemetry = {
        "run_id": run_id,
        "run_dir": str(run_dir),
        "out_root": str(out_root),
        "build_log": str(build_log),
        "monitor_log": str(monitor_log),
        "returncode": rc,
        "termination_reason": reason,
        "wall_clock_seconds": round(elapsed, 3),
        "first_partition_seconds": None if first_partition_seconds is None else round(first_partition_seconds, 3),
        "peak_work_bytes": peak_work,
        "final_work_bytes": work_bytes,
        "peak_duckdb_bytes": peak_db,
        "final_duckdb_bytes": db_bytes,
        "peak_temp_bytes": peak_temp,
        "final_temp_bytes": temp_bytes,
        "peak_partition_count": peak_parts,
        "final_partition_count": partition_count,
        "last_observed_phase": last_phase(build_log),
    }
    telemetry_path.write_text(json.dumps(telemetry, indent=2))
    print(json.dumps(telemetry, indent=2))


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        sys.exit(130)
