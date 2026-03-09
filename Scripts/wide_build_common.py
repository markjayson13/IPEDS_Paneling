#!/usr/bin/env python3
from __future__ import annotations

import argparse
import os
import pathlib
import re
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable

import pandas as pd
import pyarrow as pa


NULL_LIKE_TOKENS = ("", ".", "nan", "none", "<na>", "na", "nat")


def default_repo_root() -> pathlib.Path:
    return pathlib.Path(os.environ.get("IPEDS_ROOT", pathlib.Path(__file__).resolve().parents[1]))


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


def parse_years(spec: str) -> list[int]:
    start, end = spec.split(":")
    return list(range(int(start), int(end) + 1))


def pick_col(schema: pa.Schema, candidates: Iterable[str]) -> str:
    for c in candidates:
        if c in schema.names:
            return c
    raise ValueError(f"None of {candidates} found in schema. Columns: {schema.names}")


def pick_optional_col(schema: pa.Schema, candidates: Iterable[str]) -> str | None:
    for c in candidates:
        if c in schema.names:
            return c
    return None


def ensure_all_target_cols(df: pd.DataFrame, targets: list[str]) -> pd.DataFrame:
    cols = ["year", "UNITID"] + targets
    return df.reindex(columns=cols)


def coerce_types(df: pd.DataFrame, numeric_targets: set[str] | None = None) -> pd.DataFrame:
    df["year"] = pd.to_numeric(df["year"], errors="coerce").astype("Int32")
    df["UNITID"] = pd.to_numeric(df["UNITID"], errors="coerce").astype("Int64")
    if numeric_targets:
        cols = [c for c in df.columns if c in numeric_targets]
        if cols:
            df[cols] = df[cols].apply(pd.to_numeric, errors="coerce")
    return df


def order_targets(targets: Iterable[str]) -> list[str]:
    target_set = set(targets)
    non_x = sorted([t for t in target_set if not t.startswith("X")])
    ordered: list[str] = []
    for base in non_x:
        ordered.append(base)
        xvar = f"X{base}"
        if xvar in target_set:
            ordered.append(xvar)
    remaining = sorted([t for t in target_set if t not in ordered])
    ordered.extend(remaining)
    return ordered


def normalize_value_tokens(series: pd.Series) -> pd.Series:
    txt = series.astype("string").str.strip()
    low = txt.str.lower()
    txt = txt.mask(low.isin(NULL_LIKE_TOKENS), pd.NA)
    return txt


def is_non_empty_value(series: pd.Series) -> pd.Series:
    normalized = normalize_value_tokens(series)
    return normalized.notna()


def parse_upper_set(spec: str | None) -> set[str]:
    if not spec:
        return set()
    return {x.strip().upper() for x in str(spec).split(",") if x.strip()}


def is_dimensioned_source_file(sf: str, dim_sources: set[str], dim_prefixes: tuple[str, ...]) -> bool:
    s = str(sf or "").strip().upper()
    if not s:
        return False
    if s in dim_sources:
        return True
    return any(s.startswith(p) for p in dim_prefixes)


def find_anti_garbage_hits(targets: Iterable[str], blocked_ids: set[str]) -> list[str]:
    hits: list[str] = []
    for t in targets:
        up = str(t).upper()
        if up in blocked_ids:
            hits.append(t)
            continue
        for b in blocked_ids:
            if re.match(rf"^{re.escape(b)}($|[_0-9])", up):
                hits.append(t)
                break
    return sorted(set(hits))


def build_numeric_targets(dict_path: str | None, targets: Iterable[str]) -> set[str]:
    if not dict_path:
        return set()
    ddf = pd.read_parquet(dict_path)
    if "varname" not in ddf.columns:
        return set()
    for col in ["DataType", "format"]:
        if col not in ddf.columns:
            ddf[col] = ""
    ddf["varname"] = ddf["varname"].fillna("").astype(str).str.upper().str.strip()
    ddf["DataType"] = ddf["DataType"].fillna("").astype(str).str.lower().str.strip()
    ddf["format"] = ddf["format"].fillna("").astype(str).str.lower().str.strip()
    ddf = ddf[ddf["varname"] != ""]
    if ddf.empty:
        return set()

    numeric_markers = {"cont", "continuous", "numeric", "number", "num", "int", "integer", "float", "double", "decimal"}
    string_markers = {"disc", "discrete", "char", "string", "text", "categorical", "category"}
    target_set = {str(t).upper().strip() for t in targets}
    out: set[str] = set()

    for varname, g in ddf.groupby("varname", sort=False):
        if varname not in target_set:
            continue
        vals = set(g["DataType"].tolist() + g["format"].tolist())
        has_numeric = any(v in numeric_markers for v in vals if v)
        has_string = any(v in string_markers for v in vals if v)
        if has_numeric and not has_string:
            out.add(varname)
    return out


def build_disc_groups(dict_path: str | None) -> tuple[dict[str, tuple[str, str]], dict[str, list[str]]]:
    if not dict_path:
        return {}, {}
    ddf = pd.read_parquet(dict_path)
    ddf.columns = [c.strip() for c in ddf.columns]
    name_col = "varname" if "varname" in ddf.columns else None
    dtype_col = "DataType" if "DataType" in ddf.columns else None
    fmt_col = "format" if "format" in ddf.columns else None
    if not name_col:
        return {}, {}

    def is_disc(row) -> bool:
        dt = str(row.get(dtype_col, "") or "").strip().lower() if dtype_col else ""
        fmt = str(row.get(fmt_col, "") or "").strip().lower() if fmt_col else ""
        return dt == "disc" or fmt == "disc"

    disc_names = ddf[ddf.apply(is_disc, axis=1)][name_col].dropna().astype(str).str.upper().unique()
    var_to_group: dict[str, tuple[str, str]] = {}
    group_to_vars: dict[str, list[str]] = {}
    for v in disc_names:
        m = re.match(r"^(.*?)(\d+)$", v)
        if not m:
            continue
        base, suffix = m.group(1), m.group(2)
        if not base:
            continue
        var_to_group[v] = (base, suffix)
        group_to_vars.setdefault(base, []).append(v)
    group_to_vars = {k: sorted(vs) for k, vs in group_to_vars.items() if len(vs) >= 2}
    var_to_group = {v: grp for v, grp in var_to_group.items() if grp[0] in group_to_vars}
    return var_to_group, group_to_vars


def resolve_disc_names(
    group_to_vars: dict[str, list[str]],
    existing: set[str],
    suffix: str = "_CAT",
) -> dict[str, str]:
    mapping: dict[str, str] = {}
    taken = set(existing)
    for base in sorted(group_to_vars):
        if base not in taken:
            mapping[base] = base
            taken.add(base)
            continue
        base_suffix = f"{base}{suffix}"
        if base_suffix not in taken:
            mapping[base] = base_suffix
            taken.add(base_suffix)
            continue
        i = 1
        while True:
            cand = f"{base}{suffix}{i}"
            if cand not in taken:
                mapping[base] = cand
                taken.add(cand)
                break
            i += 1
    return mapping


@dataclass
class WideBuildRuntime:
    repo_root: pathlib.Path
    years: list[int]
    scalar_conflicts_out: str | None
    anti_garbage_out: str | None
    cast_report_out: str | None
    dim_sources: set[str]
    dim_prefixes: tuple[str, ...]
    anti_garbage_ids: set[str]
    exclude_vars: set[str]


def build_arg_parser(repo_root: pathlib.Path | None = None) -> argparse.ArgumentParser:
    repo_root = repo_root or default_repo_root()
    logs_root = repo_root / "Checks" / "logs"
    build_root = repo_root / "build"

    ap = argparse.ArgumentParser()
    ap.add_argument("--input", required=True, help="Stitched LONG panel parquet")
    ap.add_argument("--out_dir", required=True, help="Output dir for year-partitioned wide parquet")
    ap.add_argument("--years", default="2004:2023", help='Year span, default analysis window: "2004:2023"')
    ap.add_argument("--write_single", default=None, help="Optional single wide parquet path")
    ap.add_argument("--dictionary", default=None, help="Optional dictionary_lake.parquet for disc grouping")
    ap.add_argument("--lane-split", action=argparse.BooleanOptionalAction, default=False, help="Split long input into scalar lane and dimensioned lane using source_file rules")
    ap.add_argument("--scalar-long-out", default=None, help="Optional output parquet for scalar long lane")
    ap.add_argument("--dim-long-out", default=None, help="Optional output parquet for dimensioned long lane")
    ap.add_argument("--wide-analysis-out", default=None, help="Alias for --write_single when building analysis-wide output")
    ap.add_argument("--dim-sources", default="IC_CAMPUSES,IC_PCCAMPUSES,F_FA_F,F_FA_G", help="Exact source_file names treated as dimensioned")
    ap.add_argument("--dim-prefixes", default="C_,EF,GR,GR200,SAL,S_,OM,DRV", help="Comma-separated source_file prefixes treated as dimensioned")
    ap.add_argument("--exclude-vars", default=None, help="Comma-separated varnames to exclude from analysis-wide output")
    ap.add_argument("--fail-on-scalar-conflicts", action=argparse.BooleanOptionalAction, default=True, help="Fail if scalar lane has conflicting values on canonical scalar key")
    ap.add_argument("--scalar-conflicts-max-rows", type=int, default=100000, help="Max rows to write to scalar conflicts QC file")
    ap.add_argument("--anti-garbage-ids", default="CIPCODE,LINE,FORMID,FUNCTCD,MAJORNUM", help="Dimension identifier names that must not appear as scalar wide columns")
    ap.add_argument("--drop-anti-garbage-cols", action=argparse.BooleanOptionalAction, default=True, help="Drop blocked anti-garbage identifier columns from wide targets before fail gate")
    ap.add_argument("--fail-on-anti-garbage", action=argparse.BooleanOptionalAction, default=True, help="Fail if anti-garbage blocked identifiers appear as wide columns")
    ap.add_argument("--anti-garbage-out", default=None, help="QC output CSV path for anti-garbage column hits")
    ap.add_argument("--drop-globally-null-post", action=argparse.BooleanOptionalAction, default=True, help="Drop globally-null columns in final stitched single-file output")
    ap.add_argument("--typed-output", action=argparse.BooleanOptionalAction, default=False, help="Coerce numeric variables using dictionary metadata")
    ap.add_argument("--drop-empty-cols", action=argparse.BooleanOptionalAction, default=False, help="Drop vars that are empty across all requested years")
    ap.add_argument("--collapse-disc", action="store_true", help="Collapse discrete (disc) groups into a base var")
    ap.add_argument("--drop-disc-components", action="store_true", help="Drop component vars after collapse")
    ap.add_argument("--disc-qc-dir", default=None, help="Optional dir to write disc conflict reports")
    ap.add_argument("--disc-exclude", default=None, help="Comma-separated base names to skip collapsing (e.g., LEVEL,ADMCON)")
    ap.add_argument("--disc-suffix", default="_CAT", help="Suffix used when base name collides with an existing variable")
    ap.add_argument("--dups-qc-dir", default=None, help="Optional dir to write duplicate key samples")
    ap.add_argument("--dups-max-rows", type=int, default=10000, help="Max rows to write for duplicate samples (0 disables)")
    ap.add_argument("--dups-qc-gzip", action="store_true", help="Write dup samples as .csv.gz")
    ap.add_argument("--qc-dir", default=None, help="Optional dir to write QC summary CSV")
    ap.add_argument("--scalar-conflicts-out", default=None, help="QC CSV path for scalar conflict keys")
    ap.add_argument("--cast-report-out", default=None, help="QC CSV path for typed-cast parse report")
    ap.add_argument("--scan-batch-rows", type=int, default=200_000, help="Batch size for scanning long rows")
    ap.add_argument("--duckdb-path", default=str(build_root / "ipeds_build.duckdb"), help="Persistent DuckDB build path")
    ap.add_argument("--duckdb-temp-dir", default=str(build_root / "duckdb_tmp"), help="DuckDB temp directory for spills")
    ap.add_argument("--persist-duckdb", action=argparse.BooleanOptionalAction, default=True, help="Persist DuckDB build state to --duckdb-path")
    ap.add_argument("--log-file", default=str(logs_root / "04_build_wide_panel.log"), help="Optional log file path")
    return ap


def prepare_runtime(args: argparse.Namespace) -> WideBuildRuntime:
    repo_root = default_repo_root()
    os.makedirs(args.out_dir, exist_ok=True)
    years = parse_years(args.years)
    if args.wide_analysis_out and not args.write_single:
        args.write_single = args.wide_analysis_out
    if args.write_single is None and args.lane_split:
        args.write_single = str(repo_root / "Panels" / f"panel_wide_analysis_{years[0]}_{years[-1]}.parquet")

    if args.qc_dir:
        Path(args.qc_dir).mkdir(parents=True, exist_ok=True)
    if args.scalar_long_out:
        Path(args.scalar_long_out).parent.mkdir(parents=True, exist_ok=True)
    if args.dim_long_out:
        Path(args.dim_long_out).parent.mkdir(parents=True, exist_ok=True)
    if args.disc_qc_dir:
        Path(args.disc_qc_dir).mkdir(parents=True, exist_ok=True)

    scalar_conflicts_out = args.scalar_conflicts_out or (os.path.join(args.qc_dir, "qc_scalar_conflicts.csv") if args.qc_dir else None)
    anti_garbage_out = args.anti_garbage_out or (os.path.join(args.qc_dir, "qc_anti_garbage_failures.csv") if args.qc_dir else None)
    cast_report_out = args.cast_report_out or (os.path.join(args.qc_dir, "qc_cast_report.csv") if args.qc_dir else None)

    return WideBuildRuntime(
        repo_root=repo_root,
        years=years,
        scalar_conflicts_out=scalar_conflicts_out,
        anti_garbage_out=anti_garbage_out,
        cast_report_out=cast_report_out,
        dim_sources=parse_upper_set(args.dim_sources),
        dim_prefixes=tuple([x.strip().upper() for x in str(args.dim_prefixes).split(",") if x.strip()]),
        anti_garbage_ids=parse_upper_set(args.anti_garbage_ids),
        exclude_vars=parse_upper_set(args.exclude_vars),
    )
