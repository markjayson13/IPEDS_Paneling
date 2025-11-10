#!/usr/bin/env python3
"""
Helper script for CI drift detection.

Modes:
    generate-current: runs the downloader in manifest-only mode to refresh manifests.
    compare: compares baseline vs current manifests and exits non-zero on drift.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import os
import subprocess
import sys
from pathlib import Path
from typing import Iterable

DEFAULT_YEARS = "2004:2024"
DEFAULT_FIELDS = [
    "year",
    "survey",
    "prefix",
    "filename",
    "dictionary_filename",
    "release",
    "filesize_bytes",
    "sha256",
]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Manifest drift helper.")
    parser.add_argument(
        "--mode",
        required=True,
        choices=["generate-current", "compare"],
        help="Operation mode.",
    )
    parser.add_argument("--out", help="Output directory for generated manifests.")
    parser.add_argument("--years", default=DEFAULT_YEARS, help="Year expression for downloader.")
    parser.add_argument("--baseline", help="Baseline manifests directory.")
    parser.add_argument("--current", help="Current manifests directory.")
    parser.add_argument(
        "--strategy",
        default=",".join(DEFAULT_FIELDS),
        help="Comma-separated list of manifest fields to compare.",
    )
    parser.add_argument(
        "--diff-report",
        default="manifests/diff_report.txt",
        help="Path to write a human-readable diff summary.",
    )
    parser.add_argument(
        "--allow-missing-baseline",
        action="store_true",
        help="Treat missing baseline manifests as a warning (pass without diff).",
    )
    return parser.parse_args()


def run_generate(out_dir: Path, years: str) -> int:
    out_dir.mkdir(parents=True, exist_ok=True)
    cmd = [
        sys.executable,
        "download_ipeds.py",
        "--manifest-only",
        "--out-root",
        str(out_dir),
        "--years",
        years,
    ]
    result = subprocess.run(cmd, check=False)
    return result.returncode


def manifest_files(root: Path) -> list[Path]:
    if not root.exists():
        return []
    if root.is_file():
        return [root]
    return sorted(root.rglob("*_manifest.csv"))


def load_rows(root: Path) -> list[dict]:
    rows: list[dict] = []
    for path in manifest_files(root):
        with path.open(newline="", encoding="utf-8") as handle:
            reader = csv.DictReader(handle)
            for row in reader:
                rows.append({k: (v or "").strip() for k, v in row.items()})
    return rows


def digest_rows(rows: Iterable[dict], fields: list[str]) -> str:
    hasher = hashlib.sha256()
    for row in sorted(rows, key=lambda r: tuple(row.get(f, "") for f in fields)):
        line = "|".join(row.get(field, "") for field in fields)
        hasher.update(line.encode("utf-8"))
        hasher.update(b"\n")
    return hasher.hexdigest()


def compare_manifests(
    baseline_dir: Path,
    current_dir: Path,
    fields: list[str],
    diff_report: Path,
    allow_missing_baseline: bool = False,
) -> int:
    baseline_rows = load_rows(baseline_dir)
    if not baseline_rows:
        if allow_missing_baseline:
            print(f"WARNING: Baseline manifests missing in {baseline_dir}; skipping drift check.")
            return 0
        print(f"ERROR: Baseline manifests missing in {baseline_dir}", file=sys.stderr)
        return 3
    current_rows = load_rows(current_dir)
    if not current_rows:
        print(f"ERROR: No manifests found in {current_dir}", file=sys.stderr)
        return 2

    baseline_digest = digest_rows(baseline_rows, fields)
    current_digest = digest_rows(current_rows, fields)

    diff_report.parent.mkdir(parents=True, exist_ok=True)
    with diff_report.open("w", encoding="utf-8") as handle:
        handle.write("Baseline digest: " + baseline_digest + "\n")
        handle.write("Current digest : " + current_digest + "\n")

    if baseline_digest == current_digest:
        print("Manifest drift check passed.")
        return 0

    baseline_keys = {
        tuple(row.get(field, "") for field in fields): row for row in baseline_rows
    }
    current_keys = {
        tuple(row.get(field, "") for field in fields): row for row in current_rows
    }

    missing = sorted(set(baseline_keys) - set(current_keys))
    added = sorted(set(current_keys) - set(baseline_keys))

    with diff_report.open("a", encoding="utf-8") as handle:
        handle.write("\n=== Missing from current ===\n")
        if missing:
            for key in missing[:50]:
                handle.write("|".join(key) + "\n")
        else:
            handle.write("(none)\n")
        handle.write("\n=== Added in current ===\n")
        if added:
            for key in added[:50]:
                handle.write("|".join(key) + "\n")
        else:
            handle.write("(none)\n")
        if len(missing) > 50 or len(added) > 50:
            handle.write("\n(Truncated diff; see raw manifests for full details.)\n")

    print("ERROR: Manifest drift detected.", file=sys.stderr)
    return 4


def main() -> int:
    args = parse_args()
    if args.mode == "generate-current":
        if not args.out:
            print("--out is required in generate-current mode", file=sys.stderr)
            return 1
        return run_generate(Path(args.out), args.years or DEFAULT_YEARS)

    fields = [field.strip() for field in args.strategy.split(",") if field.strip()]
    if not fields:
        fields = DEFAULT_FIELDS
    baseline_dir = Path(args.baseline or "")
    current_dir = Path(args.current or "")
    diff_report = Path(args.diff_report)
    return compare_manifests(
        baseline_dir,
        current_dir,
        fields,
        diff_report,
        allow_missing_baseline=args.allow_missing_baseline,
    )


if __name__ == "__main__":
    sys.exit(main())
