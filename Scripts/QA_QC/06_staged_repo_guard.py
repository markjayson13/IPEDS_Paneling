#!/usr/bin/env python3
"""
Block staged large-data artifacts before commit or push.

This guard rejects staged files under generated-data directories, staged parquet
or DuckDB artifacts, oversized staged files, and other paths that should never
enter version control in this repo.
"""
from __future__ import annotations

import argparse
import subprocess
import sys
from pathlib import Path


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--repo-root", default=str(Path(__file__).resolve().parents[2]), help="Repository root")
    p.add_argument("--max-file-size-mb", type=float, default=5.0, help="Fail if any staged file exceeds this size")
    p.add_argument("--forbid-pattern", action="append", default=[".DS_Store"], help="Forbidden staged basenames")
    p.add_argument(
        "--forbid-path-prefix",
        action="append",
        default=["Raw_Cross_Section_Data/", "Cross_sections/", "Panels/", "Checks/", "build/", "audit_pack/"],
        help="Forbidden staged path prefixes",
    )
    p.add_argument(
        "--forbid-suffix",
        action="append",
        default=[".parquet", ".duckdb", ".duckdb.wal", ".feather", ".arrow"],
        help="Forbidden staged file suffixes",
    )
    return p.parse_args()


def staged_files(repo_root: Path) -> list[str]:
    out = subprocess.check_output(
        ["git", "diff", "--cached", "--name-only", "--diff-filter=ACMR"],
        cwd=repo_root,
        text=True,
    )
    return [line.strip() for line in out.splitlines() if line.strip()]


def main() -> int:
    args = parse_args()
    repo_root = Path(args.repo_root).resolve()
    threshold_bytes = int(float(args.max_file_size_mb) * 1024 * 1024)
    forbid_patterns = set(args.forbid_pattern)
    forbid_prefixes = tuple(args.forbid_path_prefix)
    forbid_suffixes = tuple(args.forbid_suffix)

    blocked: list[tuple[str, str]] = []
    too_large: list[tuple[int, str]] = []
    symlinks: list[str] = []

    for rel in staged_files(repo_root):
        path = repo_root / rel
        if not path.exists():
            continue
        if path.is_symlink():
            symlinks.append(rel)
        if rel.startswith(forbid_prefixes):
            blocked.append((rel, "forbidden_path_prefix"))
        if path.name in forbid_patterns:
            blocked.append((rel, "forbidden_basename"))
        if rel.endswith(forbid_suffixes):
            blocked.append((rel, "forbidden_suffix"))
        if path.is_file() and path.stat().st_size > threshold_bytes:
            too_large.append((path.stat().st_size, rel))

    if blocked or too_large or symlinks:
        print("Staged repo guard failed.")
        if blocked:
            print("\nBlocked staged paths:")
            for rel, reason in blocked:
                print(f"  {reason}: {rel}")
        if symlinks:
            print("\nBlocked staged symlinks:")
            for rel in symlinks:
                print(f"  {rel}")
        if too_large:
            print(f"\nStaged files above {args.max_file_size_mb:.2f} MB:")
            for size, rel in sorted(too_large, reverse=True):
                print(f"  {size / (1024 * 1024):8.3f} MB  {rel}")
        return 1

    print("Staged repo guard passed.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
