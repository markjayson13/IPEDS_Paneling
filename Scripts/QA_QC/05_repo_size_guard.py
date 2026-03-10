#!/usr/bin/env python3
"""
Fail if tracked repository files violate small-repo rules.

This guard scans tracked files, reports the largest committed assets, blocks
forbidden tracked basenames such as `.DS_Store`, and fails when any tracked
file exceeds the configured size threshold.
"""
from __future__ import annotations

import argparse
import subprocess
import sys
from pathlib import Path


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--repo-root", default=str(Path(__file__).resolve().parents[2]), help="Repository root")
    p.add_argument("--max-file-size-mb", type=float, default=5.0, help="Fail if any tracked file exceeds this size")
    p.add_argument("--top-n", type=int, default=25, help="How many largest tracked files to print")
    p.add_argument("--forbid-pattern", action="append", default=[".DS_Store"], help="Tracked basename patterns that should never be committed")
    return p.parse_args()


def git_ls_files(repo_root: Path) -> list[str]:
    out = subprocess.check_output(["git", "ls-files"], cwd=repo_root, text=True)
    return [line for line in out.splitlines() if line.strip()]


def main() -> None:
    args = parse_args()
    repo_root = Path(args.repo_root).resolve()
    threshold_bytes = int(float(args.max_file_size_mb) * 1024 * 1024)

    rows: list[tuple[int, str]] = []
    bad_patterns: list[str] = []
    for rel in git_ls_files(repo_root):
        path = repo_root / rel
        if not path.exists() or not path.is_file():
            continue
        size = path.stat().st_size
        rows.append((size, rel))
        if path.name in set(args.forbid_pattern):
            bad_patterns.append(rel)

    rows.sort(reverse=True)
    too_large = [(size, rel) for size, rel in rows if size > threshold_bytes]

    print(f"Tracked files scanned: {len(rows)}")
    print(f"Max allowed size: {args.max_file_size_mb:.2f} MB")
    print("Largest tracked files:")
    for size, rel in rows[: max(int(args.top_n), 0)]:
        print(f"  {size / (1024 * 1024):8.3f} MB  {rel}")

    if bad_patterns:
        print("\nTracked forbidden patterns:")
        for rel in bad_patterns:
            print(f"  {rel}")

    if too_large:
        print("\nTracked files above threshold:")
        for size, rel in too_large:
            print(f"  {size / (1024 * 1024):8.3f} MB  {rel}")

    if bad_patterns or too_large:
        return 1
    print("\nRepo size guard passed.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
