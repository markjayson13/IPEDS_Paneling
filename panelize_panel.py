#!/usr/bin/env python3
"""
Convert the long-form harmonized panel parquet into a wide CSV with UNITID-year as the panel keys.

The script keeps the highest-scoring observation per UNITID/year/target_var, pivots targets to columns,
and writes the result to the requested CSV path.
"""

from __future__ import annotations

import argparse
import logging
from pathlib import Path

import pandas as pd

DEFAULT_PANEL_PATH = Path("/Users/markjaysonfarol13/Higher Ed research/IPEDS/Parquets/panel_long.parquet")
DEFAULT_OUTPUT_PATH = Path("/Users/markjaysonfarol13/Higher Ed research/IPEDS/Paneled Datasets/panel_wide.csv")

CORE_COLUMNS = ["year", "UNITID", "reporting_unitid"]

INSTITUTIONAL_COLUMNS = [
    "dir_opeid",
    "dir_opeflag",
    "dir_inst_name",
    "dir_city",
    "dir_state_abbr",
    "dir_zip",
    "dir_county_name",
    "dir_county_fips",
    "dir_csa_code",
    "dir_cbsa_code",
    "dir_cbsa_type",
    "dir_congress_district",
    "dir_necta_code",
    "dir_longitude",
    "dir_latitude",
    "dir_multi_campus_org",
    "dir_multi_campus_id",
    "dir_ein",
    "ic_active_in_year",
    "ic_open_public",
    "ic_sector",
    "ic_level",
    "ic_control",
    "ic_institutional_category",
    "ic_degree_granting",
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
    "ic_urbanicity",
    "ic_calendar_system",
    "ic_distance_programs",
    "ic_open_admissions",
    "ic_promise_program_flag",
    "ic_response_status",
    "ic_revision_status",
    "ic_status_when_migrated",
    "ic_imputation_method",
]

FALL_ENROLLMENT_COLUMNS = [
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
]

E12_COLUMNS = [
    "e12_undup_total",
    "e12_ug_undup",
    "e12_gr_undup",
    "e12_credit_hours_ug",
    "e12_credit_hours_gr",
    "e12_contact_hours_ug",
    "e12_contact_hours_gr",
    "e12_fte",
    "e12_hs_students_for_credit",
]

COST_COLUMNS = [
    "ic_tuition_ug_in_district",
    "ic_tuition_ug_in_state",
    "ic_tuition_ug_out_state",
    "ic_tuition_gr_in_district",
    "ic_tuition_gr_in_state",
    "ic_tuition_gr_out_state",
    "ic_comprehensive_fee_ug",
    "ic_comprehensive_fee_gr",
    "ic_coa_on_campus",
    "ic_coa_off_campus_not_family",
    "ic_coa_off_campus_with_family",
    "ic_room_board_on_campus",
    "ic_room_board_off_campus_not_family",
    "ic_room_charge_on_campus",
    "ic_board_charge_on_campus",
    "ic_room_board_combined_on_campus",
    "ic_other_exp_on_campus",
    "ic_other_exp_off_campus_not_family",
    "ic_other_exp_off_campus_with_family",
    "ic_books_supplies",
    "ic_tuition_charge_varies",
    "ic_alt_tuition_any",
    "ic_alt_tuition_guaranteed",
    "ic_alt_tuition_prepaid",
    "ic_alt_tuition_payment",
    "ic_alt_tuition_other",
    "anp_all",
    "anp_0_30",
    "anp_30_48",
    "anp_48_75",
    "anp_75_110",
    "anp_110_plus",
]

SFA_COLUMNS = [
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
    "sfa_ftft_in_district_count",
    "sfa_ftft_in_state_count",
    "sfa_ftft_out_state_count",
    "sfa_ftft_unknown_rate_count",
    "pell_recip_count",
    "pell_amount",
]

FINANCE_COLUMNS = [
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
    "fin_om_plant_exp",
    "fin_scholarships_fellowships_exp",
    "fin_auxiliary_exp",
    "fin_hospital_exp",
    "fin_depreciation_exp",
    "fin_interest_exp",
    "fin_total_exp",
    "fin_endowment_assets_boy_eoy",
]

ADMISSIONS_COLUMNS = [
    "adm_open_admissions",
    "adm_applicants_total",
    "adm_admits_total",
    "adm_enrolled_ftft",
    "adm_sat_submit_count",
    "adm_sat_submit_pct",
    "adm_act_submit_count",
    "adm_act_submit_pct",
    "sat_ebrw_p25",
    "sat_ebrw_p75",
    "sat_math_p25",
    "sat_math_p75",
    "act_eng_p25",
    "act_eng_p75",
    "act_math_p25",
    "act_math_p75",
    "act_comp_p25",
    "act_comp_p75",
]

COLUMN_GROUP_SPECS = [
    {"explicit": CORE_COLUMNS},
    {"explicit": INSTITUTIONAL_COLUMNS, "prefixes": ["dir_"]},
    {"explicit": FALL_ENROLLMENT_COLUMNS, "prefixes": ["ef_"]},
    {"explicit": E12_COLUMNS, "prefixes": ["e12_"]},
    {
        "explicit": COST_COLUMNS,
        "prefixes": [
            "ic_tuition_",
            "ic_comprehensive_",
            "ic_coa_",
            "ic_room_",
            "ic_other_",
            "ic_books",
            "anp_",
        ],
    },
    {"explicit": SFA_COLUMNS, "prefixes": ["sfa_", "pell_"]},
    {"explicit": FINANCE_COLUMNS, "prefixes": ["fin_"]},
    {"explicit": ADMISSIONS_COLUMNS, "prefixes": ["adm_", "sat_", "act_"]},
]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Pivot the harmonized long panel into a wide CSV.")
    parser.add_argument(
        "--source",
        type=Path,
        default=DEFAULT_PANEL_PATH,
        help=f"Long-form panel parquet (default: {DEFAULT_PANEL_PATH})",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=DEFAULT_OUTPUT_PATH,
        help=f"Destination CSV path (default: {DEFAULT_OUTPUT_PATH})",
    )
    parser.add_argument("--log-level", type=str, default="INFO", help="Logging level (e.g., INFO, DEBUG)")
    return parser.parse_args()


def configure_logging(level: str) -> None:
    logging.basicConfig(
        level=getattr(logging, level.upper(), logging.INFO),
        format="%(asctime)s %(levelname)s %(message)s",
    )


def dedupe_panel(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df
    work = df.copy()
    if "release" in work.columns:
        work["release_rank"] = work["release"].astype(str).str.lower().eq("revised").astype(int)
    else:
        work["release_rank"] = 0
    score = pd.to_numeric(work.get("decision_score"), errors="coerce").fillna(-9e9)
    work["score_rank"] = score
    sort_cols = ["UNITID", "year", "target_var", "score_rank", "release_rank"]
    ascending = [True, True, True, False, False]
    if "form_family" in work.columns:
        sort_cols.append("form_family")
        ascending.append(True)
    if "source_file" in work.columns:
        sort_cols.append("source_file")
        ascending.append(True)
    work = work.sort_values(sort_cols, ascending=ascending)
    deduped = work.drop_duplicates(["UNITID", "year", "target_var"], keep="first").copy()
    deduped.drop(columns=["score_rank", "release_rank"], inplace=True, errors="ignore")
    return deduped


def pivot_panel(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame(columns=["UNITID", "year"])
    id_cols = df[["UNITID", "year", "reporting_unitid"]].drop_duplicates()
    dup_mask = id_cols.duplicated(subset=["UNITID", "year"], keep=False)
    if dup_mask.any():
        logging.warning(
            "Multiple reporting_unitid values detected for some UNITID/year pairs; keeping first occurrence."
        )
        id_cols = id_cols.drop_duplicates(subset=["UNITID", "year"], keep="first")
    id_cols = id_cols.sort_values(["UNITID", "year"])
    wide = (
        df.pivot(index=["UNITID", "year"], columns="target_var", values="value")
        .sort_index()
        .reset_index()
    )
    wide = id_cols.merge(wide, on=["UNITID", "year"], how="right")
    wide.columns = [col if isinstance(col, str) else str(col) for col in wide.columns]
    wide = wide.sort_values(["UNITID", "year"]).reset_index(drop=True)
    return wide


def order_columns(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df
    columns = list(df.columns)
    ordered: list[str] = []
    seen: set[str] = set()

    for spec in COLUMN_GROUP_SPECS:
        for col in spec.get("explicit", []):
            if col in columns and col not in seen:
                ordered.append(col)
                seen.add(col)
        for prefix in spec.get("prefixes", []):
            for col in columns:
                if col not in seen and col.startswith(prefix):
                    ordered.append(col)
                    seen.add(col)
    for col in columns:
        if col not in seen:
            ordered.append(col)
            seen.add(col)
    return df[ordered]


def main() -> int:
    args = parse_args()
    configure_logging(args.log_level)
    if not args.source.exists():
        logging.error("Source parquet %s does not exist", args.source)
        return 1
    logging.info("Loading panel parquet from %s", args.source)
    df = pd.read_parquet(args.source)
    df["value"] = pd.to_numeric(df["value"], errors="coerce")
    logging.info("Loaded %d long-form rows", len(df))
    deduped = dedupe_panel(df)
    logging.info("After deduplication: %d rows", len(deduped))
    wide = pivot_panel(deduped)
    logging.info("Wide panel shape: %s rows x %s columns", wide.shape[0], wide.shape[1])
    wide = order_columns(wide)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    wide.to_csv(args.output, index=False)
    logging.info("Panel CSV written to %s", args.output)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
