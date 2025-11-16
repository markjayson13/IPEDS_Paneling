#!/usr/bin/env python3
"""Validate harmonized Admissions concepts for basic consistency."""

from __future__ import annotations

import argparse
import logging
from pathlib import Path
from typing import Iterable, List

import pandas as pd

DEFAULT_WIDE = Path(
    "/Users/markjaysonfarol13/Higher Ed research/IPEDS/Parquets/Unify/ADMwide/adm_concepts_wide.parquet"
)
DEFAULT_OUT_DIR = Path("/Users/markjaysonfarol13/Higher Ed research/IPEDS/Parquets/Validation")

UNITID_CANDIDATES = ["UNITID", "unitid", "UNIT_ID", "unit_id"]
YEAR_CANDIDATES = ["YEAR", "year", "SURVEY_YEAR", "survey_year", "panel_year"]

FUNNEL_GROUPS = {
    "TOTAL": ("ADM_N_ENROLLED_TOTAL", "ADM_N_ADMITTED_TOTAL", "ADM_N_APPLICANTS_TOTAL"),
    "MEN": ("ADM_N_ENROLLED_MEN", "ADM_N_ADMITTED_MEN", "ADM_N_APPLICANTS_MEN"),
    "WOMEN": ("ADM_N_ENROLLED_WOMEN", "ADM_N_ADMITTED_WOMEN", "ADM_N_APPLICANTS_WOMEN"),
}

PERCENTILE_PAIRS = [
    ("ADM_ACT_COMP_25_PCT", "ADM_ACT_COMP_75_PCT", "ACT composite"),
    ("ADM_ACT_ENGL_25_PCT", "ADM_ACT_ENGL_75_PCT", "ACT English"),
    ("ADM_ACT_MATH_25_PCT", "ADM_ACT_MATH_75_PCT", "ACT Math"),
    ("ADM_ACT_WRIT_25_PCT_OLD", "ADM_ACT_WRIT_75_PCT_OLD", "ACT Writing (old)"),
    ("ADM_SAT_CR_25_PCT_OLD", "ADM_SAT_CR_75_PCT_OLD", "SAT Critical Reading (old)"),
    ("ADM_SAT_MATH_25_PCT_OLD", "ADM_SAT_MATH_75_PCT_OLD", "SAT Math (old)"),
    ("ADM_SAT_EBRW_25_PCT_NEW", "ADM_SAT_EBRW_75_PCT_NEW", "SAT EBRW (new)"),
    ("ADM_SAT_MATH_25_PCT_NEW", "ADM_SAT_MATH_75_PCT_NEW", "SAT Math (new)"),
]

SAT_RANGE_COLS = [
    "ADM_SAT_CR_25_PCT_OLD",
    "ADM_SAT_CR_75_PCT_OLD",
    "ADM_SAT_EBRW_25_PCT_NEW",
    "ADM_SAT_EBRW_75_PCT_NEW",
    "ADM_SAT_MATH_25_PCT_OLD",
    "ADM_SAT_MATH_75_PCT_OLD",
    "ADM_SAT_MATH_25_PCT_NEW",
    "ADM_SAT_MATH_75_PCT_NEW",
]

ACT_RANGE_COLS = [
    "ADM_ACT_COMP_25_PCT",
    "ADM_ACT_COMP_75_PCT",
    "ADM_ACT_ENGL_25_PCT",
    "ADM_ACT_ENGL_75_PCT",
    "ADM_ACT_MATH_25_PCT",
    "ADM_ACT_MATH_75_PCT",
    "ADM_ACT_WRIT_25_PCT_OLD",
    "ADM_ACT_WRIT_75_PCT_OLD",
]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--in-wide", type=Path, default=DEFAULT_WIDE, help="Admissions concepts wide parquet path")
    parser.add_argument("--out-dir", type=Path, default=DEFAULT_OUT_DIR, help="Directory for validation outputs")
    parser.add_argument(
        "--open-admissions",
        type=Path,
        default=None,
        help="Optional Parquet/CSV with UNITID, YEAR, and an open-admissions flag",
    )
    parser.add_argument(
        "--open-flag-col",
        type=str,
        default="OPENADMP",
        help="Column name for the open-admissions indicator when provided",
    )
    parser.add_argument("--log-level", default="INFO")
    return parser.parse_args()


def resolve_column(df: pd.DataFrame, preferred: str, fallbacks: Iterable[str]) -> str:
    candidates = [preferred, *fallbacks]
    for candidate in candidates:
        if candidate in df.columns:
            return candidate
    raise KeyError(f"None of the requested columns are present: {candidates}")


def ensure_numeric(series: pd.Series) -> pd.Series:
    return pd.to_numeric(series, errors="coerce")


def write_dual(df: pd.DataFrame, csv_path: Path) -> None:
    csv_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(csv_path, index=False)
    parquet_path = csv_path.with_suffix(".parquet")
    df.to_parquet(parquet_path, index=False)


def check_funnel(df: pd.DataFrame, unitid_col: str, year_col: str, out_dir: Path) -> List[str]:
    lines: List[str] = []
    violation_frames: list[pd.DataFrame] = []
    for label, (enroll_col, admit_col, app_col) in FUNNEL_GROUPS.items():
        missing = [col for col in (enroll_col, admit_col, app_col) if col not in df.columns]
        if missing:
            lines.append(f"{label}: missing columns {', '.join(missing)}; skipping funnel check.")
            continue
        subset = df[[unitid_col, year_col, enroll_col, admit_col, app_col]].copy()
        for col in (enroll_col, admit_col, app_col):
            subset[col] = ensure_numeric(subset[col])
        mask = subset[[enroll_col, admit_col, app_col]].notna().all(axis=1)
        comparable = subset.loc[mask]
        if comparable.empty:
            lines.append(f"{label}: no rows with complete funnel counts.")
            continue
        viol_enroll = comparable[enroll_col] > comparable[admit_col]
        viol_app = comparable[admit_col] > comparable[app_col]
        viol_any = viol_enroll | viol_app
        count = viol_any.sum()
        share = count / len(comparable)
        lines.append(f"{label}: {count} funnel violations ({share:.2%} of comparable rows)")
        if count:
            sample = comparable.loc[viol_any, [unitid_col, year_col, enroll_col, admit_col, app_col]].copy()
            sample.insert(2, "funnel_group", label)
            violation_frames.append(sample)
    if violation_frames:
        combined = pd.concat(violation_frames, ignore_index=True)
        out_path = out_dir / "adm_funnel_violations.csv"
        write_dual(combined, out_path)
        lines.append(f"Saved funnel violation details to {out_path} and {out_path.with_suffix('.parquet')}")
    return lines


def check_percentiles(df: pd.DataFrame, unitid_col: str, year_col: str, out_dir: Path) -> List[str]:
    lines: List[str] = []
    violation_frames: list[pd.DataFrame] = []
    for low_col, high_col, label in PERCENTILE_PAIRS:
        missing = [col for col in (low_col, high_col) if col not in df.columns]
        if missing:
            lines.append(f"{label}: missing columns {', '.join(missing)}; skipping percentile check.")
            continue
        subset = df[[unitid_col, year_col, low_col, high_col]].copy()
        subset[low_col] = ensure_numeric(subset[low_col])
        subset[high_col] = ensure_numeric(subset[high_col])
        mask = subset[[low_col, high_col]].notna().all(axis=1)
        comparable = subset.loc[mask]
        if comparable.empty:
            lines.append(f"{label}: no comparable rows for percentile ordering check.")
            continue
        violations = comparable[low_col] > comparable[high_col]
        count = violations.sum()
        share = count / len(comparable)
        lines.append(f"{label}: {count} percentile violations ({share:.2%} of comparable rows)")
        if count:
            sample = comparable.loc[violations, [unitid_col, year_col, low_col, high_col]].copy()
            sample.insert(2, "pair", label)
            violation_frames.append(sample)
    if violation_frames:
        combined = pd.concat(violation_frames, ignore_index=True)
        out_path = out_dir / "adm_percentile_order_violations.csv"
        write_dual(combined, out_path)
        lines.append(f"Saved percentile violation details to {out_path} and {out_path.with_suffix('.parquet')}")
    return lines


def check_score_ranges(df: pd.DataFrame, unitid_col: str, year_col: str, out_dir: Path) -> List[str]:
    lines: List[str] = []
    violation_frames: list[pd.DataFrame] = []
    for col in SAT_RANGE_COLS:
        if col not in df.columns:
            continue
        values = ensure_numeric(df[col])
        mask = values.notna()
        if not mask.any():
            continue
        low = (values < 200) & mask
        high = (values > 800) & mask
        count = (low | high).sum()
        if count:
            lines.append(f"{col}: {count} values outside [200, 800]")
            sample = df.loc[low | high, [unitid_col, year_col, col]].copy()
            sample.insert(2, "column", col)
            violation_frames.append(sample)
    for col in ACT_RANGE_COLS:
        if col not in df.columns:
            continue
        values = ensure_numeric(df[col])
        mask = values.notna()
        if not mask.any():
            continue
        low = (values < 1) & mask
        high = (values > 36) & mask
        count = (low | high).sum()
        if count:
            lines.append(f"{col}: {count} values outside [1, 36]")
            sample = df.loc[low | high, [unitid_col, year_col, col]].copy()
            sample.insert(2, "column", col)
            violation_frames.append(sample)
    if violation_frames:
        combined = pd.concat(violation_frames, ignore_index=True)
        out_path = out_dir / "adm_score_range_violations.csv"
        write_dual(combined, out_path)
        lines.append(f"Saved score range violation details to {out_path} and {out_path.with_suffix('.parquet')}")
    if not lines:
        lines.append("No out-of-range SAT/ACT values detected.")
    return lines


def evaluate_open_admissions(
    df: pd.DataFrame,
    unitid_col: str,
    year_col: str,
    open_file: Path | None,
    flag_col: str,
) -> List[str]:
    lines: List[str] = []
    if open_file is None:
        lines.append("Open admissions file not provided; skipping missingness check.")
        return lines
    if "ADM_N_APPLICANTS_TOTAL" not in df.columns:
        lines.append("ADM_N_APPLICANTS_TOTAL missing; cannot run open admissions missingness check.")
        return lines
    if not open_file.exists():
        lines.append(f"Open admissions file not found: {open_file}")
        return lines
    if open_file.suffix.lower() == ".parquet":
        open_df = pd.read_parquet(open_file)
    else:
        open_df = pd.read_csv(open_file)
    try:
        open_unitid = resolve_column(open_df, "UNITID", UNITID_CANDIDATES)
        open_year = resolve_column(open_df, "YEAR", YEAR_CANDIDATES)
    except KeyError as exc:
        lines.append(f"Unable to resolve UNITID/YEAR in open admissions file: {exc}")
        return lines
    if flag_col not in open_df.columns:
        lines.append(f"Open admissions flag column '{flag_col}' missing; skipping check.")
        return lines
    subset = open_df[[open_unitid, open_year, flag_col]].copy()
    numeric_flags = ensure_numeric(subset[flag_col])
    if numeric_flags.notna().any():
        subset[flag_col] = numeric_flags.fillna(0)
    else:
        values = subset[flag_col].astype(str).str.strip().str.upper()
        parsed = pd.Series(0, index=values.index, dtype="float64")
        open_mask = values.isin(["Y", "YES", "1", "TRUE"])
        closed_mask = values.isin(["N", "NO", "0", "FALSE"])
        parsed.loc[open_mask] = 1.0
        parsed.loc[~(open_mask | closed_mask)] = 0.0
        subset[flag_col] = parsed
    collapsed = subset.groupby([open_unitid, open_year], as_index=False)[flag_col].max()
    collapsed.rename(columns={open_unitid: unitid_col, open_year: year_col}, inplace=True)

    applicants = df[[unitid_col, year_col, "ADM_N_APPLICANTS_TOTAL"]].copy()
    applicants["ADM_N_APPLICANTS_TOTAL"] = ensure_numeric(applicants["ADM_N_APPLICANTS_TOTAL"])
    merged = applicants.merge(collapsed, on=[unitid_col, year_col], how="left")
    merged[flag_col] = merged[flag_col].fillna(0)
    open_mask = merged[flag_col] >= 1
    closed_mask = merged[flag_col] < 1
    if open_mask.sum() == 0 or closed_mask.sum() == 0:
        lines.append("Insufficient overlap between Admissions panel and open admissions file.")
        return lines
    open_nonmissing = merged.loc[open_mask, "ADM_N_APPLICANTS_TOTAL"].notna().mean()
    closed_missing = merged.loc[closed_mask, "ADM_N_APPLICANTS_TOTAL"].isna().mean()
    lines.append(f"Open admissions: {open_nonmissing:.2%} have applicant counts present.")
    lines.append(f"Selective institutions: {closed_missing:.2%} missing applicant counts.")
    return lines


def summarize_coverage(df: pd.DataFrame, unitid_col: str, year_col: str, out_dir: Path) -> List[str]:
    lines: List[str] = []
    concepts = [
        "ADM_N_APPLICANTS_TOTAL",
        "ADM_N_ADMITTED_TOTAL",
        "ADM_N_ENROLLED_TOTAL",
    ]
    available = [col for col in concepts if col in df.columns]
    if not available:
        lines.append("Coverage summary skipped; required columns missing.")
        return lines
    records = []
    for col in available:
        series = df[[year_col, col]].copy()
        series[col] = ensure_numeric(series[col])
        grouped = (
            series.groupby(year_col)[col]
            .apply(lambda s: s.notna().sum())
            .rename("non_missing")
            .reset_index()
        )
        grouped["concept_key"] = col
        records.append(grouped)
    if not records:
        lines.append("Coverage summary unavailable.")
        return lines
    coverage_df = pd.concat(records, ignore_index=True)
    csv_path = out_dir / "adm_coverage_by_year.csv"
    write_dual(coverage_df, csv_path)
    lines.append(
        f"Wrote coverage summary (non-missing counts by year) to {csv_path} and {csv_path.with_suffix('.parquet')}"
    )
    return lines


def check_non_negative_counts(df: pd.DataFrame, unitid_col: str, year_col: str, out_dir: Path) -> List[str]:
    lines: List[str] = []
    cols = [
        "ADM_N_APPLICANTS_TOTAL",
        "ADM_N_ADMITTED_TOTAL",
        "ADM_N_ENROLLED_TOTAL",
        "ADM_N_APPLICANTS_MEN",
        "ADM_N_ADMITTED_MEN",
        "ADM_N_ENROLLED_MEN",
        "ADM_N_APPLICANTS_WOMEN",
        "ADM_N_ADMITTED_WOMEN",
        "ADM_N_ENROLLED_WOMEN",
    ]
    cols = [c for c in cols if c in df.columns]
    if not cols:
        lines.append("No Admissions count columns available for non-negativity check.")
        return lines
    violation_frames: list[pd.DataFrame] = []
    for col in cols:
        series = df[[unitid_col, year_col, col]].copy()
        series[col] = ensure_numeric(series[col])
        mask = series[col] < 0
        count = mask.sum()
        if count:
            lines.append(f"{col}: {count} negative values")
            violation_frames.append(series.loc[mask])
    if violation_frames:
        combined = pd.concat(violation_frames, ignore_index=True)
        out_path = out_dir / "adm_negative_counts.csv"
        write_dual(combined, out_path)
        lines.append(f"Saved negative count details to {out_path} and {out_path.with_suffix('.parquet')}")
    if not violation_frames:
        lines.append("No negative Admissions counts detected.")
    return lines


def plot_applicants(df: pd.DataFrame, year_col: str, out_dir: Path) -> List[str]:
    lines: List[str] = []
    if "ADM_N_APPLICANTS_TOTAL" not in df.columns:
        lines.append("ADM_N_APPLICANTS_TOTAL missing; skipping applicant volume plot.")
        return lines
    series = df[[year_col, "ADM_N_APPLICANTS_TOTAL"]].copy()
    series["ADM_N_APPLICANTS_TOTAL"] = ensure_numeric(series["ADM_N_APPLICANTS_TOTAL"])
    series.dropna(inplace=True)
    if series.empty:
        lines.append("Applicant counts are entirely missing; skipping plot.")
        return lines
    grouped = series.groupby(year_col)["ADM_N_APPLICANTS_TOTAL"].sum()
    import matplotlib.pyplot as plt

    fig, ax = plt.subplots(figsize=(8, 4))
    grouped.plot(ax=ax)
    ax.set_title("Total applicants by year (sum across UNITIDs)")
    ax.set_ylabel("Applicants")
    ax.set_xlabel("Year")
    fig.tight_layout()
    out_path = out_dir / "adm_applicants_by_year.png"
    fig.savefig(out_path, dpi=150)
    plt.close(fig)
    lines.append(f"Saved applicant volume plot to {out_path}")
    return lines


def plot_sat_percentiles(df: pd.DataFrame, year_col: str, out_dir: Path) -> List[str]:
    combos = [
        (
            "Reading/EBRW",
            [
                ("ADM_SAT_CR_25_PCT_OLD", "Critical Reading 25th (old)"),
                ("ADM_SAT_EBRW_25_PCT_NEW", "EBRW 25th (new)"),
            ],
        ),
        (
            "Math",
            [
                ("ADM_SAT_MATH_25_PCT_OLD", "Math 25th (old)"),
                ("ADM_SAT_MATH_25_PCT_NEW", "Math 25th (new)"),
            ],
        ),
    ]
    import matplotlib.pyplot as plt

    has_data = False
    fig, axes = plt.subplots(len(combos), 1, figsize=(8, 6), sharex=True)
    for ax, (title, series_meta) in zip(axes, combos):
        plotted = False
        for column, label in series_meta:
            if column not in df.columns:
                continue
            series = df[[year_col, column]].copy()
            series[column] = ensure_numeric(series[column])
            series.dropna(inplace=True)
            if series.empty:
                continue
            grouped = series.groupby(year_col)[column].mean()
            grouped.plot(ax=ax, label=label)
            plotted = True
            has_data = True
        if plotted:
            ax.set_title(f"SAT {title} percentiles (25th)")
            ax.set_ylabel("Score")
            ax.legend()
        else:
            ax.set_title(f"SAT {title} percentiles unavailable")
    axes[-1].set_xlabel("Year")
    fig.tight_layout()
    out_path = out_dir / "adm_sat_percentiles.png"
    if has_data:
        fig.savefig(out_path, dpi=150)
        result = [f"Saved SAT percentile plot to {out_path}"]
    else:
        result = ["SAT percentile columns missing; skipped plot."]
    plt.close(fig)
    return result


def main() -> None:
    args = parse_args()
    logging.basicConfig(level=getattr(logging, args.log_level.upper(), logging.INFO), format="%(levelname)s %(message)s")

    if not args.in_wide.exists():
        raise FileNotFoundError(f"Admissions concepts wide parquet not found: {args.in_wide}")
    df = pd.read_parquet(args.in_wide)
    unitid_col = resolve_column(df, "UNITID", UNITID_CANDIDATES)
    year_col = resolve_column(df, "YEAR", YEAR_CANDIDATES)

    args.out_dir.mkdir(parents=True, exist_ok=True)

    sections = [
        ("Coverage summary", summarize_coverage(df, unitid_col, year_col, args.out_dir)),
        ("Funnel checks", check_funnel(df, unitid_col, year_col, args.out_dir)),
        ("Non-negative counts", check_non_negative_counts(df, unitid_col, year_col, args.out_dir)),
        ("Percentile ordering", check_percentiles(df, unitid_col, year_col, args.out_dir)),
        ("Score ranges", check_score_ranges(df, unitid_col, year_col, args.out_dir)),
        ("Open admissions", evaluate_open_admissions(df, unitid_col, year_col, args.open_admissions, args.open_flag_col)),
        ("Applicant plot", plot_applicants(df, year_col, args.out_dir)),
        ("SAT plots", plot_sat_percentiles(df, year_col, args.out_dir)),
    ]

    for title, lines in sections:
        print(f"==== {title} ====")
        for line in lines:
            print(line)


if __name__ == "__main__":
    main()
