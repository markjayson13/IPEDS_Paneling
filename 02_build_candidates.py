#!/usr/bin/env python3
"""
Auto-propose harmonization candidates by mining the dictionary lake.

This script should be run after 01_ingest_dictionaries.py has created
dictionary_lake.parquet.
"""

from __future__ import annotations

import argparse
import re
from pathlib import Path
from typing import Iterable

import pandas as pd

LAKE_PATH = Path("dictionary_lake.parquet")
OUTPUT_PATH = Path("harmonization_candidates.csv")
FIN_PREFIX_SEARCH = re.compile(r"(F[123][A-Z])", re.IGNORECASE)
GENERAL_PREFIX_RE = re.compile(
    r"^(?:EFFY|EFIA?|EFIB|EFIC|EFID|E1D|OM|HR|IC|SFA|GRS?|PE|AL|ADM|HD|C)",
    re.IGNORECASE,
)


def ensure_noncapturing(pattern: str) -> str:
    """Convert capturing groups to non-capturing to silence warnings."""
    return re.sub(r"(?<!\\)\((?!\?)", r"(?:", pattern)

PATTERN_DEFS = [
    dict(
        target_var="totrev_all",
        concept="Total revenues and other additions",
        units="USD",
        survey="Finance",
        prefix_re=r"F[123]A",
        label_re=r"\btotal revenues?(?: and)? (?:other additions|investment return)\b",
        notes="Includes other additions; ensure correct form (F1/F2/F3).",
    ),
    dict(
        target_var="tuition_rev",
        concept="Tuition and fees revenue",
        units="USD",
        survey="Finance",
        prefix_re=r"F[123]A",
        label_re=r"\btuition(?: and)? fees\b",
        exclude_re=r"(discount|allowance|after)",
        notes="Exclude discounts; treat form families separately.",
    ),
    dict(
        target_var="state_app",
        concept="State appropriations",
        units="USD",
        survey="Finance",
        prefix_re=r"F1A|F2A",
        label_re=r"state appropriations",
        notes="Applies to publics (F1A/F2A).",
    ),
    dict(
        target_var="fte_fall",
        concept="Fall FTE",
        units="FTE",
        survey="FallEnrollment",
        prefix_re=r"EF",
        label_re=r"full[- ]time equivalent",
        notes="IPEDS-defined FTE using EF survey.",
    ),
    dict(
        target_var="enr_ug_all",
        concept="Undergraduate headcount",
        units="count",
        survey="FallEnrollment",
        prefix_re=r"EF",
        label_re=r"undergraduate.*total",
        notes="Total undergraduate enrollment.",
    ),
    dict(
        target_var="aux_rev",
        concept="Auxiliary enterprises revenue",
        units="USD",
        survey="Finance",
        prefix_re=r"F[123]A",
        label_re=r"auxiliary (enterprises?|enterprise) (revenue|revenues)",
        notes="Revenues only; excludes auxiliary expenses.",
    ),
    dict(
        target_var="hospital_rev",
        concept="Hospital revenue",
        units="USD",
        survey="Finance",
        prefix_re=r"F1A|F2A",
        label_re=r"hospital (revenue|revenues)",
        notes="Hospital operations revenue for publics/nonprofits.",
    ),
    dict(
        target_var="federal_grants_contracts",
        concept="Federal grants and contracts",
        units="USD",
        survey="Finance",
        prefix_re=r"F[123]A",
        label_re=r"federal grants?( and)? contracts?",
        notes="Gross federal grants and contracts revenue.",
    ),
    dict(
        target_var="state_grants_contracts",
        concept="State grants and contracts",
        units="USD",
        survey="Finance",
        prefix_re=r"F[123]A",
        label_re=r"state grants?( and)? contracts?",
        notes="Gross state grants and contracts revenue.",
    ),
    dict(
        target_var="local_grants_contracts",
        concept="Local grants and contracts",
        units="USD",
        survey="Finance",
        prefix_re=r"F[123]A",
        label_re=r"local grants?( and)? contracts?",
        notes="Gross local grants and contracts revenue.",
    ),
    dict(
        target_var="investment_income",
        concept="Investment income",
        units="USD",
        survey="Finance",
        prefix_re=r"F[123]A",
        label_re=r"investment (income|return)",
        notes="Investment income (exclude endowment net gains if separate).",
    ),
    dict(
        target_var="total_expenses",
        concept="Total expenses",
        units="USD",
        survey="Finance",
        prefix_re=r"F[123]A",
        label_re=r"total expenses?( and deductions)?",
        notes="Comprehensive expense line.",
    ),
    dict(
        target_var="instruction_exp",
        concept="Instruction expenses",
        units="USD",
        survey="Finance",
        prefix_re=r"F[123]A",
        label_re=r"(^| )instruction($|[^a-z])",
        notes="Instruction functional expense.",
    ),
    dict(
        target_var="research_exp",
        concept="Research expenses",
        units="USD",
        survey="Finance",
        prefix_re=r"F[123]A",
        label_re=r"(^| )research($|[^a-z])",
        notes="Research functional expense.",
    ),
    dict(
        target_var="public_service_exp",
        concept="Public service expenses",
        units="USD",
        survey="Finance",
        prefix_re=r"F[123]A",
        label_re=r"public service",
        notes="Public service functional expense.",
    ),
    dict(
        target_var="academic_support_exp",
        concept="Academic support expenses",
        units="USD",
        survey="Finance",
        prefix_re=r"F[123]A",
        label_re=r"academic support",
        notes="Academic support functional expense.",
    ),
    dict(
        target_var="student_services_exp",
        concept="Student services expenses",
        units="USD",
        survey="Finance",
        prefix_re=r"F[123]A",
        label_re=r"student services?",
        notes="Student services functional expense.",
    ),
    dict(
        target_var="institutional_support_exp",
        concept="Institutional support expenses",
        units="USD",
        survey="Finance",
        prefix_re=r"F[123]A",
        label_re=r"institutional support",
        notes="Institutional support functional expense.",
    ),
    dict(
        target_var="operation_maint_exp",
        concept="Operations and maintenance of plant expenses",
        units="USD",
        survey="Finance",
        prefix_re=r"F[123]A",
        label_re=r"(operations? and )?maintenance of plant",
        notes="O&M of plant functional expense.",
    ),
    dict(
        target_var="scholarships_fellowships_exp",
        concept="Scholarships and fellowships expenses",
        units="USD",
        survey="Finance",
        prefix_re=r"F[123]A",
        label_re=r"scholarships? (and )?fellowships?",
        notes="Scholarship/fellowship expense line.",
    ),
    dict(
        target_var="interest_expense",
        concept="Interest expense",
        units="USD",
        survey="Finance",
        prefix_re=r"F[123]A",
        label_re=r"interest expense",
        notes="Interest expense line.",
    ),
    dict(
        target_var="depreciation_expense",
        concept="Depreciation expense",
        units="USD",
        survey="Finance",
        prefix_re=r"F[123]A",
        label_re=r"depreciation( and amortization)?",
        notes="Depreciation (and amortization) expense line.",
    ),
    dict(
        target_var="enr_grad_all",
        concept="Graduate headcount",
        units="count",
        survey="FallEnrollment",
        prefix_re=r"EF",
        label_re=r"graduate.*total",
        notes="Total graduate headcount.",
    ),
    dict(
        target_var="enr_total_all",
        concept="Total headcount",
        units="count",
        survey="FallEnrollment",
        prefix_re=r"EF",
        label_re=r"(grand total|enrollment.*total|all students total)",
        notes="Total fall headcount.",
    ),
    dict(
        target_var="pell_amount",
        concept="Pell Grants amount",
        units="USD",
        survey="StudentFinancialAid",
        prefix_re=r"SFA",
        label_re=r"pell grants?.*(amount|total)",
        notes="Total Pell Grant dollars awarded.",
    ),
    dict(
        target_var="pell_recip_count",
        concept="Pell recipients count",
        units="count",
        survey="StudentFinancialAid",
        prefix_re=r"SFA",
        label_re=r"pell (recipients?|awards?)",
        notes="Number of Pell recipients.",
    ),
    dict(
        target_var="gr_150_4yr_all",
        concept="Graduation rate 150% time (4-year)",
        units="percent",
        survey="GraduationRates",
        prefix_re=r"GRS?|PE",
        label_re=r"150%.*(graduation rate|completed)",
        notes="150%% completion for 4-year cohorts.",
    ),
    dict(
        target_var="gr_150_2yr_all",
        concept="Graduation rate 150% time (2-year)",
        units="percent",
        survey="GraduationRates",
        prefix_re=r"GRS?|PE",
        label_re=r"150%.*(graduation rate|completed)",
        notes="150%% completion for 2-year cohorts.",
    ),
    # Outcome Measures subcohorts (Pell vs Non-Pell; FTFT/FTPT/NFTFT/NFTPT at 4/6/8 years)
    dict(
        target_var="om_ftft_pell_award_4yr",
        concept="OM FTFT Pell — award by 4 years",
        units="percent",
        survey="OutcomeMeasures",
        prefix_re=r"OM",
        label_re=r"(?:full[- ]?time.*first[- ]?time).*pell.*(?:4[- ]?year|4[- ]?yr).*(?:award|completed)",
        notes="First-time full-time Pell recipients completing within 4 years.",
    ),
    dict(
        target_var="om_ftft_pell_award_6yr",
        concept="OM FTFT Pell — award by 6 years",
        units="percent",
        survey="OutcomeMeasures",
        prefix_re=r"OM",
        label_re=r"(?:full[- ]?time.*first[- ]?time).*pell.*(?:6[- ]?year|6[- ]?yr).*(?:award|completed)",
        notes="First-time full-time Pell recipients completing within 6 years.",
    ),
    dict(
        target_var="om_ftft_pell_award_8yr",
        concept="OM FTFT Pell — award by 8 years",
        units="percent",
        survey="OutcomeMeasures",
        prefix_re=r"OM",
        label_re=r"(?:full[- ]?time.*first[- ]?time).*pell.*(?:8[- ]?year|8[- ]?yr).*(?:award|completed)",
        notes="First-time full-time Pell recipients completing within 8 years.",
    ),
    dict(
        target_var="om_ftft_nonpell_award_4yr",
        concept="OM FTFT Non-Pell — award by 4 years",
        units="percent",
        survey="OutcomeMeasures",
        prefix_re=r"OM",
        label_re=r"(?:full[- ]?time.*first[- ]?time).*(?:non[- ]?pell|not pell|no pell).*?(?:4[- ]?year|4[- ]?yr).*(?:award|completed)",
        notes="First-time full-time Non-Pell completing within 4 years.",
    ),
    dict(
        target_var="om_ftft_nonpell_award_6yr",
        concept="OM FTFT Non-Pell — award by 6 years",
        units="percent",
        survey="OutcomeMeasures",
        prefix_re=r"OM",
        label_re=r"(?:full[- ]?time.*first[- ]?time).*(?:non[- ]?pell|not pell|no pell).*?(?:6[- ]?year|6[- ]?yr).*(?:award|completed)",
        notes="First-time full-time Non-Pell completing within 6 years.",
    ),
    dict(
        target_var="om_ftft_nonpell_award_8yr",
        concept="OM FTFT Non-Pell — award by 8 years",
        units="percent",
        survey="OutcomeMeasures",
        prefix_re=r"OM",
        label_re=r"(?:full[- ]?time.*first[- ]?time).*(?:non[- ]?pell|not pell|no pell).*?(?:8[- ]?year|8[- ]?yr).*(?:award|completed)",
        notes="First-time full-time Non-Pell completing within 8 years.",
    ),
    dict(
        target_var="om_ftpt_pell_award_4yr",
        concept="OM FTPT Pell — award by 4 years",
        units="percent",
        survey="OutcomeMeasures",
        prefix_re=r"OM",
        label_re=r"(?:part[- ]?time.*first[- ]?time).*pell.*(?:4[- ]?year|4[- ]?yr).*(?:award|completed)",
        notes="First-time part-time Pell recipients completing within 4 years.",
    ),
    dict(
        target_var="om_ftpt_pell_award_6yr",
        concept="OM FTPT Pell — award by 6 years",
        units="percent",
        survey="OutcomeMeasures",
        prefix_re=r"OM",
        label_re=r"(?:part[- ]?time.*first[- ]?time).*pell.*(?:6[- ]?year|6[- ]?yr).*(?:award|completed)",
        notes="First-time part-time Pell recipients completing within 6 years.",
    ),
    dict(
        target_var="om_ftpt_pell_award_8yr",
        concept="OM FTPT Pell — award by 8 years",
        units="percent",
        survey="OutcomeMeasures",
        prefix_re=r"OM",
        label_re=r"(?:part[- ]?time.*first[- ]?time).*pell.*(?:8[- ]?year|8[- ]?yr).*(?:award|completed)",
        notes="First-time part-time Pell recipients completing within 8 years.",
    ),
    dict(
        target_var="om_ftpt_nonpell_award_4yr",
        concept="OM FTPT Non-Pell — award by 4 years",
        units="percent",
        survey="OutcomeMeasures",
        prefix_re=r"OM",
        label_re=r"(?:part[- ]?time.*first[- ]?time).*(?:non[- ]?pell|not pell|no pell).*?(?:4[- ]?year|4[- ]?yr).*(?:award|completed)",
        notes="First-time part-time Non-Pell completing within 4 years.",
    ),
    dict(
        target_var="om_ftpt_nonpell_award_6yr",
        concept="OM FTPT Non-Pell — award by 6 years",
        units="percent",
        survey="OutcomeMeasures",
        prefix_re=r"OM",
        label_re=r"(?:part[- ]?time.*first[- ]?time).*(?:non[- ]?pell|not pell|no pell).*?(?:6[- ]?year|6[- ]?yr).*(?:award|completed)",
        notes="First-time part-time Non-Pell completing within 6 years.",
    ),
    dict(
        target_var="om_ftpt_nonpell_award_8yr",
        concept="OM FTPT Non-Pell — award by 8 years",
        units="percent",
        survey="OutcomeMeasures",
        prefix_re=r"OM",
        label_re=r"(?:part[- ]?time.*first[- ]?time).*(?:non[- ]?pell|not pell|no pell).*?(?:8[- ]?year|8[- ]?yr).*(?:award|completed)",
        notes="First-time part-time Non-Pell completing within 8 years.",
    ),
    dict(
        target_var="om_nftft_pell_award_4yr",
        concept="OM Non-FTFT Pell — award by 4 years (full-time)",
        units="percent",
        survey="OutcomeMeasures",
        prefix_re=r"OM",
        label_re=r"(?:full[- ]?time.*non[- ]?first[- ]?time).*pell.*(?:4[- ]?year|4[- ]?yr).*(?:award|completed)",
        notes="Full-time non-first-time Pell recipients completing within 4 years.",
    ),
    dict(
        target_var="om_nftft_pell_award_6yr",
        concept="OM Non-FTFT Pell — award by 6 years (full-time)",
        units="percent",
        survey="OutcomeMeasures",
        prefix_re=r"OM",
        label_re=r"(?:full[- ]?time.*non[- ]?first[- ]?time).*pell.*(?:6[- ]?year|6[- ]?yr).*(?:award|completed)",
        notes="Full-time non-first-time Pell recipients completing within 6 years.",
    ),
    dict(
        target_var="om_nftft_pell_award_8yr",
        concept="OM Non-FTFT Pell — award by 8 years (full-time)",
        units="percent",
        survey="OutcomeMeasures",
        prefix_re=r"OM",
        label_re=r"(?:full[- ]?time.*non[- ]?first[- ]?time).*pell.*(?:8[- ]?year|8[- ]?yr).*(?:award|completed)",
        notes="Full-time non-first-time Pell recipients completing within 8 years.",
    ),
    dict(
        target_var="om_nftft_nonpell_award_4yr",
        concept="OM Non-FTFT Non-Pell — award by 4 years (full-time)",
        units="percent",
        survey="OutcomeMeasures",
        prefix_re=r"OM",
        label_re=r"(?:full[- ]?time.*non[- ]?first[- ]?time).*(?:non[- ]?pell|not pell|no pell).*?(?:4[- ]?year|4[- ]?yr).*(?:award|completed)",
        notes="Full-time non-first-time Non-Pell completing within 4 years.",
    ),
    dict(
        target_var="om_nftft_nonpell_award_6yr",
        concept="OM Non-FTFT Non-Pell — award by 6 years (full-time)",
        units="percent",
        survey="OutcomeMeasures",
        prefix_re=r"OM",
        label_re=r"(?:full[- ]?time.*non[- ]?first[- ]?time).*(?:non[- ]?pell|not pell|no pell).*?(?:6[- ]?year|6[- ]?yr).*(?:award|completed)",
        notes="Full-time non-first-time Non-Pell completing within 6 years.",
    ),
    dict(
        target_var="om_nftft_nonpell_award_8yr",
        concept="OM Non-FTFT Non-Pell — award by 8 years (full-time)",
        units="percent",
        survey="OutcomeMeasures",
        prefix_re=r"OM",
        label_re=r"(?:full[- ]?time.*non[- ]?first[- ]?time).*(?:non[- ]?pell|not pell|no pell).*?(?:8[- ]?year|8[- ]?yr).*(?:award|completed)",
        notes="Full-time non-first-time Non-Pell completing within 8 years.",
    ),
    dict(
        target_var="om_nftpt_pell_award_4yr",
        concept="OM Non-FTFT Pell — award by 4 years (part-time)",
        units="percent",
        survey="OutcomeMeasures",
        prefix_re=r"OM",
        label_re=r"(?:part[- ]?time.*non[- ]?first[- ]?time).*pell.*(?:4[- ]?year|4[- ]?yr).*(?:award|completed)",
        notes="Part-time non-first-time Pell recipients completing within 4 years.",
    ),
    dict(
        target_var="om_nftpt_pell_award_6yr",
        concept="OM Non-FTFT Pell — award by 6 years (part-time)",
        units="percent",
        survey="OutcomeMeasures",
        prefix_re=r"OM",
        label_re=r"(?:part[- ]?time.*non[- ]?first[- ]?time).*pell.*(?:6[- ]?year|6[- ]?yr).*(?:award|completed)",
        notes="Part-time non-first-time Pell recipients completing within 6 years.",
    ),
    dict(
        target_var="om_nftpt_pell_award_8yr",
        concept="OM Non-FTFT Pell — award by 8 years (part-time)",
        units="percent",
        survey="OutcomeMeasures",
        prefix_re=r"OM",
        label_re=r"(?:part[- ]?time.*non[- ]?first[- ]?time).*pell.*(?:8[- ]?year|8[- ]?yr).*(?:award|completed)",
        notes="Part-time non-first-time Pell recipients completing within 8 years.",
    ),
    dict(
        target_var="om_nftpt_nonpell_award_4yr",
        concept="OM Non-FTFT Non-Pell — award by 4 years (part-time)",
        units="percent",
        survey="OutcomeMeasures",
        prefix_re=r"OM",
        label_re=r"(?:part[- ]?time.*non[- ]?first[- ]?time).*(?:non[- ]?pell|not pell|no pell).*?(?:4[- ]?year|4[- ]?yr).*(?:award|completed)",
        notes="Part-time non-first-time Non-Pell completing within 4 years.",
    ),
    dict(
        target_var="om_nftpt_nonpell_award_6yr",
        concept="OM Non-FTFT Non-Pell — award by 6 years (part-time)",
        units="percent",
        survey="OutcomeMeasures",
        prefix_re=r"OM",
        label_re=r"(?:part[- ]?time.*non[- ]?first[- ]?time).*(?:non[- ]?pell|not pell|no pell).*?(?:6[- ]?year|6[- ]?yr).*(?:award|completed)",
        notes="Part-time non-first-time Non-Pell completing within 6 years.",
    ),
    dict(
        target_var="om_nftpt_nonpell_award_8yr",
        concept="OM Non-FTFT Non-Pell — award by 8 years (part-time)",
        units="percent",
        survey="OutcomeMeasures",
        prefix_re=r"OM",
        label_re=r"(?:part[- ]?time.*non[- ]?first[- ]?time).*(?:non[- ]?pell|not pell|no pell).*?(?:8[- ]?year|8[- ]?yr).*(?:award|completed)",
        notes="Part-time non-first-time Non-Pell completing within 8 years.",
    ),
    # Graduation rate 150% detail by Pell status and level
    dict(
        target_var="gr_150_4yr_pell",
        concept="GR 150% — 4-year, Pell recipients",
        units="percent",
        survey="GraduationRates",
        prefix_re=r"GRS?|PE",
        label_re=r"(?:4[- ]?year|four[- ]?year).*(?:150%|150 percent).*(?:graduation rate|completed).*pell",
        notes="150% completion for 4-year Pell recipients.",
    ),
    dict(
        target_var="gr_150_4yr_nonpell",
        concept="GR 150% — 4-year, Non-Pell",
        units="percent",
        survey="GraduationRates",
        prefix_re=r"GRS?|PE",
        label_re=r"(?:4[- ]?year|four[- ]?year).*(?:150%|150 percent).*(?:graduation rate|completed).*(?:non[- ]?pell|not pell|no pell)",
        notes="150% completion for 4-year Non-Pell students.",
    ),
    dict(
        target_var="gr_150_2yr_pell",
        concept="GR 150% — 2-year, Pell recipients",
        units="percent",
        survey="GraduationRates",
        prefix_re=r"GRS?|PE",
        label_re=r"(?:2[- ]?year|two[- ]?year|less[- ]?than[- ]?2[- ]?year).*(?:150%|150 percent).*(?:graduation rate|completed).*pell",
        notes="150% completion for 2-year Pell recipients.",
    ),
    dict(
        target_var="gr_150_2yr_nonpell",
        concept="GR 150% — 2-year, Non-Pell",
        units="percent",
        survey="GraduationRates",
        prefix_re=r"GRS?|PE",
        label_re=r"(?:2[- ]?year|two[- ]?year|less[- ]?than[- ]?2[- ]?year).*(?:150%|150 percent).*(?:graduation rate|completed).*(?:non[- ]?pell|not pell|no pell)",
        notes="150% completion for 2-year Non-Pell students.",
    ),
]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--lake",
        type=Path,
        default=LAKE_PATH,
        help="dictionary_lake parquet file (default: dictionary_lake.parquet)",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=OUTPUT_PATH,
        help="Output CSV for candidate mappings (default: harmonization_candidates.csv)",
    )
    return parser.parse_args()


def infer_prefix(row: pd.Series) -> str:
    source_var = str(row.get("source_var", "")).strip()
    fin_match = FIN_PREFIX_SEARCH.search(source_var)
    if fin_match:
        raw = fin_match.group(1).upper()
        if raw in {"F1A", "F2A", "F3A"}:
            return raw
        if raw.startswith("F1"):
            return "F1A"
        if raw.startswith("F2"):
            return "F2A"
        if raw.startswith("F3"):
            return "F3A"
    match = GENERAL_PREFIX_RE.match(source_var)
    if match:
        return match.group(0).upper()
    hint = row.get("prefix_hint", "")
    if isinstance(hint, str) and hint:
        return hint
    path_str = str(row.get("dict_file", ""))
    match2 = re.search(
        r"(F[123]A|EFFY|EFIA?|EFIB|EFIC|EFID|E1D|OM|HR|IC|SFA|GRS?|PE|AL|ADM|HD|C)",
        path_str.upper(),
    )
    return match2.group(1) if match2 else ""


def main() -> None:
    args = parse_args()
    if not args.lake.exists():
        raise SystemExit(
            f"{args.lake} not found. Run 01_ingest_dictionaries.py first."
        )
    lake = pd.read_parquet(args.lake)
    if "source_label_norm" not in lake.columns:
        raise SystemExit("dictionary_lake is missing 'source_label_norm'; rebuild it.")

    candidates: list[pd.DataFrame] = []
    for pattern in PATTERN_DEFS:
        label_pattern = ensure_noncapturing(pattern["label_re"])
        label_regex = re.compile(label_pattern, flags=re.IGNORECASE)
        subset = lake[
            lake["source_label_norm"].str.contains(label_regex, na=False)
        ].copy()
        if subset.empty:
            continue

        exclude_re = pattern.get("exclude_re")
        if exclude_re:
            exclude_pattern = re.compile(ensure_noncapturing(exclude_re), flags=re.IGNORECASE)
            subset = subset[~subset["source_label_norm"].str.contains(exclude_pattern, na=False)]
            if subset.empty:
                continue

        subset["survey"] = pattern["survey"]
        subset["target_var"] = pattern["target_var"]
        subset["concept"] = pattern["concept"]
        subset["units"] = pattern["units"]
        subset["notes"] = pattern.get("notes", "")
        subset["prefix"] = subset.apply(infer_prefix, axis=1)

        prefix_re = pattern.get("prefix_re")
        if prefix_re:
            subset = subset[
                subset["prefix"].str.contains(prefix_re, regex=True, na=False)
            ]
        if subset.empty:
            continue

        subset["label_match_re"] = pattern["label_re"]
        subset = subset[
            [
                "year",
                "survey",
                "prefix",
                "target_var",
                "concept",
                "units",
                "source_var",
                "source_label",
                "dict_file",
                "filename",
                "notes",
                "release",
                "label_match_re",
            ]
        ]
        candidates.append(subset)

    if candidates:
        proposed = (
            pd.concat(candidates, ignore_index=True)
            .sort_values(["target_var", "year", "prefix"])
            .reset_index(drop=True)
        )
    else:
        proposed = pd.DataFrame(
            columns=[
                "year",
                "survey",
                "prefix",
                "target_var",
                "concept",
                "units",
                "source_var",
                "source_label",
                "dict_file",
                "filename",
                "notes",
                "release",
                "label_match_re",
            ]
        )

    proposed.to_csv(args.output, index=False)
    print(f"Wrote {len(proposed):,} candidate rows to {args.output}")


if __name__ == "__main__":
    main()
