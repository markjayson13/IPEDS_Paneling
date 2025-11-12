#!/usr/bin/env python3
"""
Emit per-component wide panels from the harmonized long-form parquet.

For each supported survey family (IC, EF, E12, SFA, ADM, Finance), the script:
1. normalizes UNITID/reporting_unitid,
2. pivots target variables into a fixed column order (optionally overridden by templates),
3. writes a CSV (panel_<COMP>.csv) under the requested output directory, and
4. reports UNITID-year rows that have multiple reporters.

Example:
    python panelize_panel.py \\
        --input panel_long.parquet \\
        --outdir ./panel_wide_components \\
        --templates schemas/component_columns/ic_columns.csv
"""

from __future__ import annotations

import argparse
import logging
import os
from pathlib import Path
from typing import Dict, List

import pandas as pd

SURVEY_CODE_MAP = {
    "InstitutionalCharacteristics": "IC",
    "FallEnrollment": "EF",
    "12MonthEnrollment": "E12",
    "StudentFinancialAid": "SFA",
    "Admissions": "ADM",
    "Finance": "F",
}

COMPONENT_TARGETS: Dict[str, List[str]] = {
    "IC": [
        "dir_opeid",
        "dir_inst_name",
        "dir_state_abbr",
        "dir_state_fips",
        "bea_region_code",
        "ic_sector",
        "ic_level",
        "ic_control",
        "ic_institutional_category",
        "ic_degree_granting",
        "carnegie_unified_class",
        "dir_zip",
        "dir_city",
        "dir_opeflag",
        "ic_active_in_year",
        "ic_urbanicity",
        "ic_calendar_system",
        "dir_csa_code",
        "dir_cbsa_code",
        "dir_cbsa_type",
        "dir_longitude",
        "dir_latitude",
        "dir_county_fips",
        "dir_county_name",
        "dir_congress_district",
        "dir_necta_code",
        "dir_multi_campus_org",
        "dir_multi_campus_id",
        "dir_ein",
        "ic_open_public",
        "ic_highest_degree_offered",
        "ic_highest_level_offering",
        "ic_ug_offering",
        "ic_gr_offering",
        "ic_affiliation",
        "ic_public_control_primary",
        "ic_public_control_secondary",
        "ic_religious_affiliation",
        "ic_parent_unitid",
        "ic_hbcu_flag",
        "ic_tribal_flag",
        "ic_med_school_flag",
        "ic_distance_programs",
        "ic_open_admissions",
        "ic_promise_program_flag",
        "ic_response_status",
        "ic_revision_status",
        "ic_status_when_migrated",
        "ic_imputation_method",
        "ic_tuition_ug_in_district",
        "ic_tuition_ug_in_state",
        "ic_tuition_ug_out_state",
        "ic_tuition_gr_in_district",
        "ic_tuition_gr_in_state",
        "ic_tuition_gr_out_state",
        "ic_room_charge_on_campus",
        "ic_board_charge_on_campus",
        "ic_room_board_combined_on_campus",
        "ic_alt_tuition_any",
        "ic_alt_tuition_guaranteed",
        "ic_alt_tuition_prepaid",
        "ic_alt_tuition_payment",
        "ic_alt_tuition_other",
    ],
    "EF": [
        "ef_total",
        "ef_ug_total",
        "ef_grad_total",
        "ef_ug_degseek_total",
        "ef_ftft_ug_total",
        "ef_full_time_total",
        "ef_part_time_total",
        "ef_de_exclusive",
        "ef_de_some",
        "ef_de_none",
        "ef_retention_ftft_full_time",
        "ef_retention_ftft_part_time",
        "ef_student_faculty_ratio",
    ],
    "E12": [
        "e12_undup_total",
        "e12_ug_undup",
        "e12_gr_undup",
        "e12_credit_hours_ug",
        "e12_credit_hours_gr",
        "e12_contact_hours_ug",
        "e12_contact_hours_gr",
        "e12_fte",
        "e12_hs_students_for_credit",
    ],
    "SFA": [
        "sfa_ftft_in_district_count",
        "sfa_ftft_in_state_count",
        "sfa_ftft_out_state_count",
        "sfa_ftft_unknown_rate_count",
        "sfa_any_aid_recip_count",
        "sfa_any_aid_amount",
        "sfa_federal_grant_recip_count",
        "sfa_federal_grant_amount",
        "sfa_state_local_grant_recip_count",
        "sfa_state_local_grant_amount",
        "sfa_institutional_grant_recip_count",
        "sfa_institutional_grant_amount",
        "sfa_direct_sub_loan_recip_count",
        "sfa_direct_sub_loan_amount",
        "sfa_direct_unsub_loan_recip_count",
        "sfa_direct_unsub_loan_amount",
        "sfa_parent_plus_recip_count",
        "sfa_parent_plus_amount",
        "sfa_private_loan_recip_count",
        "sfa_private_loan_amount",
        "sfa_veterans_benefits_amount",
        "pell_recip_count",
        "pell_amount",
    ],
    "ADM": [
        "adm_open_admissions",
        "adm_applicants_total",
        "adm_admits_total",
        "adm_enrolled_ftft",
        "adm_sat_submit_count",
        "adm_sat_submit_pct",
        "adm_act_submit_count",
        "adm_act_submit_pct",
    ],
    "F": [
        "fin_total_rev_invest_return",
        "fin_tuition_fees_net",
        "fin_federal_grants_contracts",
        "fin_state_grants_contracts",
        "fin_local_grants_contracts",
        "fin_private_gifts_grants_contracts",
        "fin_sales_ed_activities",
        "fin_auxiliary_rev",
        "fin_hospital_rev",
        "fin_investment_return",
        "fin_instruction_exp",
        "fin_research_exp",
        "fin_public_service_exp",
        "fin_academic_support_exp",
        "fin_student_services_exp",
        "fin_institutional_support_exp",
        "fin_auxiliary_exp",
        "fin_hospital_exp",
        "fin_plant_ops_exp",
        "fin_scholarships_fellowships_exp",
        "fin_depreciation_exp",
        "fin_interest_expense",
        "fin_other_operating_exp",
        "fin_total_exp",
        "fin_endowment_assets_boy_eoy",
        "fin_allowances_tuition",
    ],
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build per-component wide panels")
    parser.add_argument("--input", type=Path, required=True, help="panel_long parquet path")
    parser.add_argument("--outdir", type=Path, required=True, help="Directory for per-component CSVs")
    parser.add_argument(
        "--templates",
        nargs="+",
        default=[],
        help="Optional CSV templates (one column named 'column') to override component orders. "
        "Component inferred from filename prefix before the first '.'.",
    )
    parser.add_argument("--log-level", default="INFO", help="Logging level (e.g., INFO, DEBUG)")
    return parser.parse_args()


def load_templates(paths: List[str]) -> Dict[str, List[str]]:
    templates: Dict[str, List[str]] = {}
    for path in paths:
        tpath = Path(path)
        if not tpath.exists():
            logging.warning("Template %s does not exist; skipping", tpath)
            continue
        frame = pd.read_csv(tpath)
        column_field = None
        for candidate in ("column", "columns", "target_var"):
            if candidate in frame.columns:
                column_field = candidate
                break
        if column_field is None:
            logging.warning("Template %s missing 'column' header; skipping", tpath)
            continue
        comp = tpath.stem.split(".")[0].upper()
        templates[comp] = frame[column_field].dropna().astype(str).tolist()
        logging.info("Loaded template for %s with %d columns", comp, len(templates[comp]))
    return templates


def ensure_reporting_unitid(df: pd.DataFrame) -> pd.DataFrame:
    if "reporting_unitid" not in df.columns:
        df["reporting_unitid"] = pd.NA
    df["reporting_unitid"] = df["reporting_unitid"].replace("", pd.NA)
    if "UNITID" in df.columns:
        df["reporting_unitid"] = df["reporting_unitid"].fillna(df["UNITID"])
    return df


def pivot_component(df: pd.DataFrame, component: str, targets: List[str]) -> pd.DataFrame:
    if df.empty:
        cols = ["year", "UNITID", "reporting_unitid"] + targets
        return pd.DataFrame(columns=cols)
    pivot = (
        df.pivot_table(
            index=["UNITID", "year", "reporting_unitid"], columns="target_var", values="value", aggfunc="first"
        )
        .reset_index()
        .copy()
    )
    for target in targets:
        if target not in pivot.columns:
            pivot[target] = pd.NA
    ordered = ["year", "UNITID", "reporting_unitid"] + targets
    extras = [col for col in pivot.columns if col not in ordered]
    return pivot[ordered + extras]


def main() -> None:
    args = parse_args()
    logging.basicConfig(level=getattr(logging, args.log_level.upper(), logging.INFO), format="%(levelname)s %(message)s")

    if not args.input.exists():
        raise FileNotFoundError(f"Input parquet not found: {args.input}")

    df = pd.read_parquet(args.input)
    df = ensure_reporting_unitid(df)
    df["survey_code"] = df["survey"].map(SURVEY_CODE_MAP).fillna(df["survey"]).astype(str)
    args.outdir.mkdir(parents=True, exist_ok=True)

    templates = load_templates(args.templates)

    for comp, default_targets in COMPONENT_TARGETS.items():
        targets = templates.get(comp, default_targets)
        comp_df = df[df["survey_code"].str.upper() == comp]
        wide = pivot_component(comp_df, comp, targets)
        out_path = args.outdir / f"panel_{comp}.csv"
        wide.sort_values(["year", "UNITID"]).to_csv(out_path, index=False)
        logging.info("Wrote %s with %d rows", out_path, len(wide))

    conflicts = (
        df.groupby(["UNITID", "year"])["reporting_unitid"]
        .nunique(dropna=True)
        .reset_index(name="n_reporters")
    )
    conflict_rows = conflicts[conflicts["n_reporters"] > 1]
    if not conflict_rows.empty:
        offenders = (
            df.merge(conflict_rows[["UNITID", "year"]], on=["UNITID", "year"], how="inner")
            [["UNITID", "year", "reporting_unitid"]]
            .drop_duplicates()
        )
        conflict_path = args.outdir / "panel_wide.reporting_conflicts.csv"
        offenders.to_csv(conflict_path, index=False)
        logging.warning("Reporting conflicts detected; see %s", conflict_path)
    else:
        logging.info("No reporting conflicts detected")


if __name__ == "__main__":
    main()
