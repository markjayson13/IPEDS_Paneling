#!/usr/bin/env python3
"""Validate the final panel_wide CSV for structural and numeric integrity."""

from __future__ import annotations

import argparse
import logging
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Callable, Dict, List, Tuple

import numpy as np
import pandas as pd

HARD_RULES = {
    "net_price_negative",
    "net_price_too_high",
    "finance_revenue_negative",
    "finance_expense_negative",
}

SOFT_RULES = {
    "finance_revenue_too_large",
    "finance_expense_too_large",
    "enrollment_exceeds_admits",
}


def classify_rule(rule: str) -> str:
    if rule in HARD_RULES:
        return "hard"
    if rule in SOFT_RULES:
        return "soft"
    return "soft"

@dataclass
class RuleViolation:
    unitid: int | float | None
    year: int | float | None
    column: str
    value: float | str | None
    rule: str


def load_panel(path: Path) -> pd.DataFrame:
    logging.info("Loading panel_wide from %s", path)
    df = pd.read_csv(path, low_memory=False)

    # Normalize potential lowercase column names
    if "UNITID" not in df.columns and "unitid" in df.columns:
        df = df.rename(columns={"unitid": "UNITID"})
    if "YEAR" not in df.columns and "year" in df.columns:
        df = df.rename(columns={"year": "YEAR"})

    if "UNITID" not in df.columns or "YEAR" not in df.columns:
        raise ValueError("Input panel must contain UNITID and YEAR columns.")

    df["UNITID"] = pd.to_numeric(df["UNITID"], errors="coerce")
    df["YEAR"] = pd.to_numeric(df["YEAR"], errors="coerce")
    missing = df["UNITID"].isna() | df["YEAR"].isna()
    if missing.any():
        raise ValueError(f"Found {missing.sum()} rows with missing UNITID or YEAR.")
    df["UNITID"] = df["UNITID"].astype("Int64")
    df["YEAR"] = df["YEAR"].astype("Int64")

    if "STABLE_PRNTCHLD_STATUS" in df.columns:
        df["STABLE_PRNTCHLD_STATUS"] = pd.to_numeric(
            df["STABLE_PRNTCHLD_STATUS"], errors="coerce"
        ).astype("Int64")
        status_dtype = df["STABLE_PRNTCHLD_STATUS"].dtype
    else:
        status_dtype = None

    logging.info(
        "Column dtypes - UNITID: %s, YEAR: %s%s",
        df["UNITID"].dtype,
        df["YEAR"].dtype,
        f", STABLE_PRNTCHLD_STATUS: {status_dtype}" if status_dtype is not None else "",
    )
    return df


def check_no_duplicates(df: pd.DataFrame) -> List[RuleViolation]:
    dup_mask = df.duplicated(subset=["UNITID", "YEAR"])
    violations: List[RuleViolation] = []
    if dup_mask.any():
        logging.warning("Found %s duplicate (UNITID, YEAR) rows.", dup_mask.sum())
        for unitid, year in df.loc[dup_mask, ["UNITID", "YEAR"]].values:
            violations.append(RuleViolation(unitid, year, "*", "duplicate_row", "duplicate_unitid_year"))
    else:
        logging.info("No duplicate (UNITID, YEAR) rows detected.")
    return violations


def check_parent_child_status(df: pd.DataFrame) -> List[RuleViolation]:
    if "STABLE_PRNTCHLD_STATUS" not in df.columns:
        logging.info("STABLE_PRNTCHLD_STATUS not present; skipping parent/child validation.")
        return []
    allowed = {1, 2, 3}
    status = df["STABLE_PRNTCHLD_STATUS"]
    bad_mask = ~status.isin(allowed) & ~status.isna()
    violations: List[RuleViolation] = []
    if bad_mask.any():
        logging.warning("Found %s rows with invalid STABLE_PRNTCHLD_STATUS.", bad_mask.sum())
        for unitid, year, value in df.loc[bad_mask, ["UNITID", "YEAR", "STABLE_PRNTCHLD_STATUS"]].values:
            violations.append(
                RuleViolation(unitid, year, "STABLE_PRNTCHLD_STATUS", value, "invalid_parent_child_status")
            )
    else:
        status_counts = status.value_counts(dropna=True).to_dict()
        logging.info("Parent/child status counts: %s", status_counts)
    return violations


def check_net_price(df: pd.DataFrame) -> List[RuleViolation]:
    cols = [c for c in df.columns if c.startswith("SFA__NET_PRICE")]
    violations: List[RuleViolation] = []
    for col in cols:
        series = pd.to_numeric(df[col], errors="coerce")
        neg_mask = series < 0
        high_mask = series > 200_000
        for mask, rule in [(neg_mask, "net_price_negative"), (high_mask, "net_price_too_high")]:
            if mask.any():
                logging.warning("%s: %s violations", col, rule)
                for unitid, year, value in df.loc[mask, ["UNITID", "YEAR", col]].values:
                    violations.append(RuleViolation(unitid, year, col, value, rule))
    return violations


def check_finance(df: pd.DataFrame) -> List[RuleViolation]:
    violations: List[RuleViolation] = []
    finance_checks = [
        ("FINANCE__IS_REVENUES_TOTAL", lambda s: s < 0, "finance_revenue_negative"),
        ("FINANCE__IS_EXPENSES_TOTAL", lambda s: s < 0, "finance_expense_negative"),
        ("FINANCE__REV_TUITION_NET", lambda s: s < -10_000_000, "finance_tuition_extreme_negative"),
        ("FINANCE__IS_REVENUES_TOTAL", lambda s: s > 5e10, "finance_revenue_too_large"),
        ("FINANCE__IS_EXPENSES_TOTAL", lambda s: s > 5e10, "finance_expense_too_large"),
    ]
    for col, condition, rule in finance_checks:
        if col not in df.columns:
            logging.warning("Finance check skipped; column %s missing.", col)
            continue
        series = pd.to_numeric(df[col], errors="coerce")
        mask = condition(series)
        mask = mask.fillna(False)
        if mask.any():
            logging.warning("%s: %s violations", col, rule)
            for unitid, year, value in df.loc[mask, ["UNITID", "YEAR", col]].values:
                violations.append(RuleViolation(unitid, year, col, value, rule))

    # Difference check
    if {"FINANCE__IS_REVENUES_TOTAL", "FINANCE__IS_EXPENSES_TOTAL"} <= set(df.columns):
        rev = pd.to_numeric(df["FINANCE__IS_REVENUES_TOTAL"], errors="coerce")
        exp = pd.to_numeric(df["FINANCE__IS_EXPENSES_TOTAL"], errors="coerce")
        valid = (rev > 0) & (exp > 0)
        denom = rev.where(rev.abs() >= exp.abs(), exp.abs())
        denom = denom.where(denom != 0)
        diff_ratio = pd.Series(np.nan, index=df.index)
        diff_ratio[valid] = (rev[valid] - exp[valid]).abs() / denom[valid]
        mask = valid & (diff_ratio > 10.0)
        if mask.any():
            logging.warning("Finance difference check: %s suspicious rows.", mask.sum())
            for unitid, year, r, e in df.loc[mask, ["UNITID", "YEAR", "FINANCE__IS_REVENUES_TOTAL", "FINANCE__IS_EXPENSES_TOTAL"]].values:
                violations.append(
                    RuleViolation(unitid, year, "FINANCE__IS_REVENUES_TOTAL", r, "finance_rev_exp_mismatch")
                )
                violations.append(
                    RuleViolation(unitid, year, "FINANCE__IS_EXPENSES_TOTAL", e, "finance_rev_exp_mismatch")
                )
    return violations


def check_admissions(df: pd.DataFrame) -> List[RuleViolation]:
    req = [
        "ADM__ADM_N_APPLICANTS_TOTAL",
        "ADM__ADM_N_ADMITTED_TOTAL",
        "ADM__ADM_N_ENROLLED_TOTAL",
    ]
    if not set(req).issubset(df.columns):
        logging.info("Admissions columns missing; skipping funnel check.")
        return []
    apps = pd.to_numeric(df[req[0]], errors="coerce")
    admits = pd.to_numeric(df[req[1]], errors="coerce")
    enroll = pd.to_numeric(df[req[2]], errors="coerce")
    violations: List[RuleViolation] = []
    masks = [
        (apps < 0, req[0], "admissions_negative_apps"),
        (admits < 0, req[1], "admissions_negative_admits"),
        (enroll < 0, req[2], "admissions_negative_enroll"),
        (admits > apps, req[1], "admissions_exceeds_applicants"),
        (enroll > admits, req[2], "enrollment_exceeds_admits"),
    ]
    for mask, col, rule in masks:
        if mask.any():
            logging.warning("Admissions check %s triggered %s rows.", rule, mask.sum())
            for unitid, year, value in df.loc[mask, ["UNITID", "YEAR", col]].values:
                violations.append(RuleViolation(unitid, year, col, value, rule))
    return violations


def check_percentages(df: pd.DataFrame) -> List[RuleViolation]:
    cols = [c for c in df.columns if "_PCT" in c or "_RATE" in c]
    skip_tokens = ("SAT_", "ACT_", "SAT", "ACT")
    violations: List[RuleViolation] = []
    skip_cols = [c for c in cols if any(token in c for token in skip_tokens)]
    if skip_cols:
        logging.info(
            "Skipping SAT/ACT percentile columns in percentage check: %s",
            skip_cols,
        )
    check_cols = [c for c in cols if c not in skip_cols]
    for col in check_cols:
        series = pd.to_numeric(df[col], errors="coerce")
        mask = (series < 0) | (series > 100)
        if mask.any():
            logging.warning("%s has %s percentage violations.", col, mask.sum())
            for unitid, year, value in df.loc[mask, ["UNITID", "YEAR", col]].values:
                violations.append(RuleViolation(unitid, year, col, value, "pct_out_of_range"))
    return violations


def check_enrollment(df: pd.DataFrame) -> List[RuleViolation]:
    cols = [c for c in df.columns if c.startswith("ENROLL__") and "HEAD" in c]
    violations: List[RuleViolation] = []
    for col in cols:
        series = pd.to_numeric(df[col], errors="coerce")
        mask = series < 0
        if mask.any():
            logging.warning("%s has %s negative enrollment counts.", col, mask.sum())
            for unitid, year, value in df.loc[mask, ["UNITID", "YEAR", col]].values:
                violations.append(RuleViolation(unitid, year, col, value, "enrollment_negative"))
    return violations


def violations_to_dataframe(violations: List[RuleViolation]) -> pd.DataFrame:
    base_columns = ["unitid", "year", "column", "value", "rule"]
    if not violations:
        return pd.DataFrame(columns=base_columns)
    return pd.DataFrame(
        [
            {
                "unitid": v.unitid,
                "year": v.year,
                "column": v.column,
                "value": v.value,
                "rule": v.rule,
            }
            for v in violations
        ],
        columns=base_columns,
    )


HARD_CLEANABLE_RULES = {
    "net_price_negative",
    "net_price_too_high",
    "finance_revenue_negative",
    "finance_expense_negative",
}


def apply_cleaning(
    df: pd.DataFrame, violations_df: pd.DataFrame
) -> Tuple[pd.DataFrame, pd.DataFrame, Dict[str, int]]:
    cleaned = df.copy()
    rule_counts: Dict[str, int] = {}
    if violations_df.empty:
        return cleaned, violations_df, rule_counts

    for idx, row in violations_df.iterrows():
        rule = row["rule"]
        if classify_rule(rule) != "hard" or rule not in HARD_CLEANABLE_RULES:
            continue
        column = row["column"]
        if column not in cleaned.columns:
            continue
        mask = (cleaned["UNITID"] == row["unitid"]) & (cleaned["YEAR"] == row["year"])
        affected = int(mask.sum())
        if affected:
            cleaned.loc[mask, column] = pd.NA
            violations_df.at[idx, "cleaned"] = True
            rule_counts[rule] = rule_counts.get(rule, 0) + affected

    for rule, count in rule_counts.items():
        logging.info("Cleaning applied for %s: set %s cells to NaN.", rule, count)

    return cleaned, violations_df, rule_counts


def unresolved_hard_violations(violations_df: pd.DataFrame) -> pd.DataFrame:
    if violations_df.empty or "severity" not in violations_df.columns or "cleaned" not in violations_df.columns:
        return violations_df.iloc[0:0]
    mask = (violations_df["severity"] == "hard") & (~violations_df["cleaned"])
    return violations_df.loc[mask]


def write_outputs(
    cleaned: pd.DataFrame,
    violations_df: pd.DataFrame,
    cleaning_counts: Dict[str, int],
    summary_path: Path,
    violations_path: Path,
    output_clean: Path,
) -> None:
    cleaned.to_csv(output_clean, index=False)
    base_columns = ["unitid", "year", "column", "value", "rule", "severity", "cleaned"]
    if violations_df.empty:
        violations_to_write = pd.DataFrame(columns=base_columns)
    else:
        violations_to_write = violations_df.copy()
    violations_to_write.to_csv(violations_path, index=False)

    total_violations = len(violations_to_write)

    with summary_path.open("w", encoding="utf-8") as fh:
        fh.write(f"Cleaned rows: {len(cleaned):,}, columns: {len(cleaned.columns):,}\n")
        fh.write(f"Total violations: {total_violations}\n")
        if total_violations:
            hard_total = int((violations_to_write["severity"] == "hard").sum())
            soft_total = int((violations_to_write["severity"] == "soft").sum())
        else:
            hard_total = soft_total = 0

        fh.write("Violations by rule type:\n")
        fh.write(f"  HARD: {hard_total}\n")
        fh.write(f"  SOFT: {soft_total}\n")

        if total_violations:
            fh.write("\nViolations by rule:\n")
            for rule, group in violations_to_write.groupby("rule"):
                severity = group["severity"].iloc[0].upper()
                cleaned_count = int(group["cleaned"].sum())
                total_count = len(group)
                fh.write(f"{rule} ({severity}): total={total_count}, cleaned={cleaned_count}\n")
        fh.write("\n")

        hard_cleaned = int(
            ((violations_to_write["severity"] == "hard") & (violations_to_write["cleaned"])).sum()
        )
        hard_remaining = int(
            ((violations_to_write["severity"] == "hard") & (~violations_to_write["cleaned"])).sum()
        )
        fh.write(f"Hard violations cleaned: {hard_cleaned}\n")
        fh.write(f"Remaining hard violations after cleaning: {hard_remaining}\n\n")

        key_cols = [
            "FINANCE__IS_REVENUES_TOTAL",
            "FINANCE__IS_EXPENSES_TOTAL",
            "FINANCE__REV_TUITION_NET",
        ] + [c for c in cleaned.columns if c.startswith("SFA__NET_PRICE")]
        fh.write("\nSummary statistics:\n")

        def describe(series: pd.Series) -> str:
            series = pd.to_numeric(series, errors="coerce").dropna()
            if series.empty:
                return "No data"
            stats = series.describe(percentiles=[0.01, 0.5, 0.99])
            return (
                f"count={int(stats['count'])}, "
                f"min={stats['min']:.2f}, "
                f"p1={stats['1%']:.2f}, "
                f"median={stats['50%']:.2f}, "
                f"mean={stats['mean']:.2f}, "
                f"p99={stats['99%']:.2f}, "
                f"max={stats['max']:.2f}"
            )

        for col in key_cols:
            if col in cleaned.columns:
                fh.write(f"{col}: {describe(cleaned[col])}\n")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", required=True, type=Path, help="Input panel_wide CSV.")
    parser.add_argument("--output-clean", required=True, type=Path, help="Path to write cleaned CSV.")
    parser.add_argument("--violations", required=True, type=Path, help="Path to write violations CSV.")
    parser.add_argument("--summary", required=True, type=Path, help="Path to write summary text.")
    parser.add_argument(
        "--fail-on-soft",
        action="store_true",
        help=(
            "If set, exit with code 1 even when only soft violations remain after cleaning. "
            "By default, the script exits 0 when only soft violations are present."
        ),
    )
    parser.add_argument("--log-level", default="INFO", help="Logging level (INFO, DEBUG, ...)")
    return parser.parse_args()


def run_checks(df: pd.DataFrame) -> List[RuleViolation]:
    checks: List[Callable[[pd.DataFrame], List[RuleViolation]]] = [
        check_no_duplicates,
        check_parent_child_status,
        check_net_price,
        check_finance,
        check_admissions,
        check_enrollment,
        check_percentages,
    ]
    violations: List[RuleViolation] = []
    for check in checks:
        try:
            violations.extend(check(df))
        except Exception as exc:  # pragma: no cover - defensive
            logging.exception("Validation check %s failed: %s", check.__name__, exc)
    return violations


def main() -> None:
    args = parse_args()
    logging.basicConfig(
        level=getattr(logging, args.log_level.upper(), logging.INFO),
        format="%(levelname)s %(message)s",
    )

    try:
        df = load_panel(args.input)
    except Exception as exc:
        logging.error("Failed to load panel: %s", exc)
        sys.exit(1)

    violations = run_checks(df)
    violations_df = violations_to_dataframe(violations)
    if violations_df.empty:
        violations_df = pd.DataFrame(
            columns=["unitid", "year", "column", "value", "rule", "severity", "cleaned"]
        )
    else:
        violations_df["severity"] = violations_df["rule"].map(classify_rule)
        violations_df["cleaned"] = False

    cleaned, violations_df, cleaning_counts = apply_cleaning(df, violations_df)
    args.output_clean.parent.mkdir(parents=True, exist_ok=True)
    args.violations.parent.mkdir(parents=True, exist_ok=True)
    args.summary.parent.mkdir(parents=True, exist_ok=True)
    write_outputs(
        cleaned,
        violations_df,
        cleaning_counts,
        args.summary,
        args.violations,
        args.output_clean,
    )

    has_any_violations = not violations_df.empty
    unresolved_hard = unresolved_hard_violations(violations_df)
    has_hard_violations = not unresolved_hard.empty

    if has_hard_violations:
        logging.error(
            "Validation completed with %s unresolved HARD violations.",
            len(unresolved_hard),
        )
        sys.exit(1)

    if has_any_violations:
        if args.fail_on_soft:
            logging.error(
                "Validation found %s non-fatal violations but --fail-on-soft requested.",
                len(violations_df),
            )
            sys.exit(1)
        soft_total = int((violations_df["severity"] == "soft").sum())
        cleaned_hard_total = int(
            ((violations_df["severity"] == "hard") & (violations_df["cleaned"])).sum()
        )
        logging.warning(
            "Validation found %s violations (%s soft, %s cleaned hard) but no unresolved hard errors; exiting with code 0.",
            len(violations_df),
            soft_total,
            cleaned_hard_total,
        )
        sys.exit(0)

    logging.info("Validation completed with no violations.")
    sys.exit(0)


if __name__ == "__main__":
    main()
