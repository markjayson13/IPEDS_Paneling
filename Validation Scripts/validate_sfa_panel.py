"""Run validation and diagnostics on the harmonized SFA concept panel."""
from __future__ import annotations

import argparse
import logging
from pathlib import Path
from typing import List, Sequence

import pandas as pd

UNITID_CANDIDATES = ["UNITID", "unitid", "UNIT_ID", "unit_id"]
YEAR_CANDIDATES = ["YEAR", "year", "SURVEY_YEAR", "survey_year", "panel_year", "SURVYEAR", "survyear"]
NET_PRICE_BINS = [
    "NET_PRICE_AVG_INC_0_30K",
    "NET_PRICE_AVG_INC_30_48K",
    "NET_PRICE_AVG_INC_48_75K",
    "NET_PRICE_AVG_INC_75_110K",
    "NET_PRICE_AVG_INC_110K_PLUS",
]
BASE_SFA_LONG_DIR = Path("/Users/markjaysonfarol13/Higher Ed research/IPEDS/Parquets/Unify/SFAlong")
BASE_SFA_WIDE_DIR = Path("/Users/markjaysonfarol13/Higher Ed research/IPEDS/Parquets/Unify/SFAwide")
BASE_VALIDATION_DIR = Path("/Users/markjaysonfarol13/Higher Ed research/IPEDS/Parquets/Validation")


def resolve_column(df: pd.DataFrame, preferred: str, fallbacks: Sequence[str]) -> str:
    candidates = [preferred, *fallbacks]
    for candidate in candidates:
        if candidate in df.columns:
            return candidate
    raise KeyError(f"None of the requested columns are present: {candidates}")


def to_numeric(series: pd.Series) -> pd.Series:
    return pd.to_numeric(series, errors="coerce")


def check_percent_bounds(df: pd.DataFrame) -> List[str]:
    lines: List[str] = []
    percent_cols = [col for col in df.columns if "_PCT_" in col.upper()]
    for col in percent_cols:
        values = to_numeric(df[col])
        nonmissing = values.notna().sum()
        if nonmissing == 0:
            continue
        lt_zero = (values < 0).sum()
        gt_hundred = (values > 100).sum()
        lines.append(
            f"{col}: {lt_zero} (<0) {lt_zero / nonmissing:.2%}; {gt_hundred} (>100) {gt_hundred / nonmissing:.2%}"
        )
    if not lines:
        lines.append("No percent columns found.")
    return lines


def summarize_series(series: pd.Series) -> str:
    clean = series.dropna()
    if clean.empty:
        return "insufficient data"
    stats = clean.describe(percentiles=[0.01, 0.5, 0.99])
    return (
        f"min={stats['min']:.2f}, p1={stats['1%']:.2f}, median={stats['50%']:.2f}, "
        f"mean={stats['mean']:.2f}, p99={stats['99%']:.2f}, max={stats['max']:.2f}"
    )


def check_amount_bounds(df: pd.DataFrame) -> List[str]:
    lines: List[str] = []
    candidates = [col for col in df.columns if col.upper().endswith("_AMT") or col.upper().startswith("NET_PRICE_")]
    for col in candidates:
        values = to_numeric(df[col])
        nonmissing = values.notna().sum()
        if nonmissing == 0:
            continue
        neg = (values < 0).sum()
        neg_large = (values < -1000).sum()
        stats_line = summarize_series(values.dropna()) if nonmissing else ""
        lines.append(
            f"{col}: negatives={neg} ({neg / nonmissing:.2%}), < -1000={neg_large} ({neg_large / nonmissing:.2%}); {stats_line}"
        )
    if not lines:
        lines.append("No amount/net price columns found.")
    return lines


def check_nested_counts(df: pd.DataFrame, unitid_col: str, year_col: str) -> List[str]:
    lines: List[str] = []
    required = [
        "SFA_FTFT_N",
        "SFA_FTFT_N_AID",
        "SFA_FTFT_N_PELL",
        "SFA_FTFT_N_FED_LOAN",
    ]
    missing = [col for col in required if col not in df.columns]
    if missing:
        lines.append(f"Skipping nested-count check; missing columns: {', '.join(missing)}")
        return lines

    checks = [
        ("SFA_FTFT_N_AID", "<=", "SFA_FTFT_N"),
        ("SFA_FTFT_N_PELL", "<=", "SFA_FTFT_N_AID"),
        ("SFA_FTFT_N_FED_LOAN", "<=", "SFA_FTFT_N_AID"),
    ]
    for left, _, right in checks:
        left_vals = to_numeric(df[left])
        right_vals = to_numeric(df[right])
        mask = left_vals.notna() & right_vals.notna()
        if not mask.any():
            continue
        violations = (left_vals > right_vals) & mask
        count = violations.sum()
        share = count / mask.sum()
        lines.append(f"{left} <= {right}: {count} violations ({share:.2%} of comparable rows)")
        if count:
            sample = df.loc[violations, [unitid_col, year_col, left, right]].head(5)
            lines.append(f"Sample violations for {left} <= {right}:")
            lines.append(sample.to_string(index=False))
    return lines


def check_cross_component(
    sfa_df: pd.DataFrame,
    ef_df: pd.DataFrame,
    unitid_col: str,
    year_col: str,
    ef_unitid_col: str,
    ef_year_col: str,
    ef_ftft_col: str,
) -> List[str]:
    lines: List[str] = []
    if "SFA_FTFT_N" not in sfa_df.columns:
        lines.append("SFA panel missing SFA_FTFT_N; skipping cross-component check.")
        return lines
    if ef_ftft_col not in ef_df.columns:
        lines.append(f"EF panel missing {ef_ftft_col}; skipping cross-component check.")
        return lines
    merge_cols = {unitid_col: "sfa_unitid", year_col: "sfa_year"}
    sfa_tmp = sfa_df[[unitid_col, year_col, "SFA_FTFT_N"]].dropna(subset=["SFA_FTFT_N"]).copy()
    ef_tmp = ef_df[[ef_unitid_col, ef_year_col, ef_ftft_col]].dropna(subset=[ef_ftft_col]).copy()
    sfa_tmp.rename(columns=merge_cols, inplace=True)
    ef_tmp.rename(columns={ef_unitid_col: "sfa_unitid", ef_year_col: "sfa_year"}, inplace=True)
    merged = sfa_tmp.merge(ef_tmp, on=["sfa_unitid", "sfa_year"], how="inner")
    if merged.empty:
        lines.append("No overlapping UNITID/YEAR between SFA and EF panels.")
        return lines
    sfa_counts = to_numeric(merged["SFA_FTFT_N"])
    ef_counts = to_numeric(merged[ef_ftft_col])
    mask = sfa_counts.notna() & ef_counts.notna()
    violations = (sfa_counts > ef_counts) & mask
    count = violations.sum()
    share = count / mask.sum()
    lines.append(f"SFA_FTFT_N <= {ef_ftft_col}: {count} violations ({share:.2%})")
    if count:
        sample = merged.loc[violations, ["sfa_unitid", "sfa_year", "SFA_FTFT_N", ef_ftft_col]].head(5)
        lines.append("Sample cross-component violations:")
        lines.append(sample.to_string(index=False))
    return lines


def check_net_price_monotonicity(df: pd.DataFrame) -> List[str]:
    lines: List[str] = []
    available = [col for col in NET_PRICE_BINS if col in df.columns]
    if len(available) < 2:
        lines.append("Not enough net price bins to evaluate monotonicity.")
        return lines
    subset = df[available].dropna()
    if subset.empty:
        lines.append("No rows have complete net price bins.")
        return lines
    low = to_numeric(subset[NET_PRICE_BINS[0]])
    high = to_numeric(subset[NET_PRICE_BINS[-1]])
    violations = low > high
    count = violations.sum()
    share = count / len(subset)
    lines.append(
        f"NET_PRICE_AVG_INC_0_30K <= NET_PRICE_AVG_INC_110K_PLUS: {count} violations ({share:.2%} of complete rows)"
    )
    return lines


def plot_time_series(df: pd.DataFrame, year_col: str, output_dir: Path) -> List[str]:
    import matplotlib.pyplot as plt

    output_dir.mkdir(parents=True, exist_ok=True)
    plots = [
        ("SFA_FTFT_AVG_PELL_AMT", "sfa_ftft_avg_pell_amt_by_year.png"),
        ("NET_PRICE_AVG_TITLEIV", "net_price_avg_titleiv_by_year.png"),
        ("SFA_FTFT_PCT_PELL", "sfa_ftft_pct_pell_by_year.png"),
    ]
    lines: List[str] = []
    for column, filename in plots:
        if column not in df.columns:
            lines.append(f"Skipping plot for {column}; column missing.")
            continue
        series = df[[year_col, column]].dropna()
        if series.empty:
            lines.append(f"Skipping plot for {column}; no data.")
            continue
        grouped = series.groupby(year_col)[column].mean()
        fig, ax = plt.subplots(figsize=(8, 4))
        grouped.plot(ax=ax)
        ax.set_title(column)
        ax.set_xlabel("Year")
        ax.set_ylabel(column)
        fig.tight_layout()
        dest = output_dir / filename
        fig.savefig(dest)
        plt.close(fig)
        lines.append(f"Saved {dest}")
    return lines


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--sfa-panel",
        type=Path,
        default=BASE_SFA_WIDE_DIR / "sfa_concepts_wide.parquet",
        help="Concept-level SFA parquet to validate.",
    )
    parser.add_argument("--ef-panel", type=Path, default=None, help="Optional EF concepts parquet for cross-checks")
    parser.add_argument(
        "--output-summary",
        type=Path,
        default=BASE_VALIDATION_DIR / "sfa_panel_validation_summary.txt",
        help="Destination for validation summary text.",
    )
    parser.add_argument("--no-output-summary", action="store_true", help="Skip writing the summary file.")
    parser.add_argument("--make-plots", action="store_true", help="Generate time-series plots")
    parser.add_argument(
        "--plots-dir",
        type=Path,
        default=BASE_VALIDATION_DIR / "plots",
        help="Directory for diagnostic plots when --make-plots is set.",
    )
    parser.add_argument("--unitid-col", type=str, default="UNITID")
    parser.add_argument("--year-col", type=str, default="YEAR")
    parser.add_argument("--ef-unitid-col", type=str, default="UNITID")
    parser.add_argument("--ef-year-col", type=str, default="YEAR")
    parser.add_argument("--ef-ftft-col", type=str, default="EF_FTFT_TOTAL")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")

    if not args.sfa_panel.exists():
        raise FileNotFoundError(f"SFA panel not found: {args.sfa_panel}")

    logging.info("Loading SFA panel: %s", args.sfa_panel)
    sfa_df = pd.read_parquet(args.sfa_panel)
    try:
        unitid_col = resolve_column(sfa_df, args.unitid_col, UNITID_CANDIDATES)
        year_col = resolve_column(sfa_df, args.year_col, YEAR_CANDIDATES)
    except KeyError as exc:
        raise KeyError("Unable to find UNITID/YEAR columns in SFA panel") from exc

    summary_lines: List[str] = []

    percent_lines = check_percent_bounds(sfa_df)
    logging.info("Percent bound checks complete")
    summary_lines.append("Percent bounds:")
    summary_lines.extend(percent_lines)

    amount_lines = check_amount_bounds(sfa_df)
    logging.info("Amount/net price checks complete")
    summary_lines.append("Amount & net price bounds:")
    summary_lines.extend(amount_lines)

    nested_lines = check_nested_counts(sfa_df, unitid_col, year_col)
    logging.info("Nested FTFT checks complete")
    summary_lines.append("Nested FTFT cohort checks:")
    summary_lines.extend(nested_lines)

    if args.ef_panel:
        if not Path(args.ef_panel).exists():
            raise FileNotFoundError(f"EF panel not found: {args.ef_panel}")
        logging.info("Loading EF panel: %s", args.ef_panel)
        ef_df = pd.read_parquet(args.ef_panel)
        cross_lines = check_cross_component(
            sfa_df,
            ef_df,
            unitid_col,
            year_col,
            resolve_column(ef_df, args.ef_unitid_col, UNITID_CANDIDATES),
            resolve_column(ef_df, args.ef_year_col, YEAR_CANDIDATES),
            args.ef_ftft_col,
        )
        summary_lines.append("Cross-component EF checks:")
        summary_lines.extend(cross_lines)

    monotonic_lines = check_net_price_monotonicity(sfa_df)
    summary_lines.append("Net price monotonicity:")
    summary_lines.extend(monotonic_lines)

    if args.make_plots:
        plot_lines = plot_time_series(sfa_df, year_col, args.plots_dir)
        summary_lines.append("Plots:")
        summary_lines.extend(plot_lines)

    text = "\n".join(summary_lines)
    print(text)
    if not args.no_output_summary and args.output_summary:
        args.output_summary.parent.mkdir(parents=True, exist_ok=True)
        args.output_summary.write_text(text)
        logging.info("Saved summary to %s", args.output_summary)
    elif args.no_output_summary:
        logging.info("Summary file skipped per CLI flag.")


if __name__ == "__main__":
    main()
