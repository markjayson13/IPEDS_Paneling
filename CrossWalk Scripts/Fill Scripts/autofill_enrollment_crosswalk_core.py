#!/usr/bin/env python3
"""Auto-populate core enrollment concepts in the crosswalk template."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

import pandas as pd

DEFAULT_INPUT = Path(
    "/Users/markjaysonfarol13/Higher Ed research/IPEDS/Paneled Datasets/Crosswalks/enrollment_crosswalk_template.csv"
)
DEFAULT_OUTPUT = Path(
    "/Users/markjaysonfarol13/Higher Ed research/IPEDS/Paneled Datasets/Crosswalks/Filled/enrollment_crosswalk_autofilled.csv"
)

E12_HEAD_ALL_TOT_ALL = "E12_HEAD_ALL_TOT_ALL"
EF_HEAD_ALL_TOT_ALL = "EF_HEAD_ALL_TOT_ALL"
EF_HEAD_FTFT_UG_DEGSEEK_TOT = "EF_HEAD_FTFT_UG_DEGSEEK_TOT"
EF_HEAD_FT_ALL_TOT_ALL = "EF_HEAD_FT_ALL_TOT_ALL"
EF_HEAD_FT_UG_TOT_ALL = "EF_HEAD_FT_UG_TOT_ALL"
EF_HEAD_FT_GR_TOT_ALL = "EF_HEAD_FT_GR_TOT_ALL"
EF_HEAD_FTFT_UG_RES_INSTATE = "EF_HEAD_FTFT_UG_RES_INSTATE"
EF_HEAD_FTFT_UG_RES_OUTSTATE = "EF_HEAD_FTFT_UG_RES_OUTSTATE"
EF_HEAD_FTFT_UG_RES_FOREIGN = "EF_HEAD_FTFT_UG_RES_FOREIGN"
EF_HEAD_FTFT_UG_RES_UNKNOWN = "EF_HEAD_FTFT_UG_RES_UNKNOWN"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", type=Path, default=DEFAULT_INPUT)
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    if not args.input.exists():
        print(f"Input crosswalk not found: {args.input}", file=sys.stderr)
        raise SystemExit(1)

    cw = pd.read_csv(args.input)
    cw.columns = [c.strip() for c in cw.columns]

    required_cols = ["concept_key", "source_var", "year_start", "survey"]
    missing = [col for col in required_cols if col not in cw.columns]
    if missing:
        raise RuntimeError(f"Crosswalk template missing required columns: {missing}")

    cw["concept_key"] = cw["concept_key"]
    cw["source_var"] = cw["source_var"].astype(str).str.strip()
    cw["survey"] = cw["survey"].astype(str).str.strip().str.upper()
    if "label_norm" in cw.columns:
        cw["label_norm"] = cw["label_norm"].astype(str).str.strip()
    else:
        cw["label_norm"] = ""
    cw["year_start"] = pd.to_numeric(cw["year_start"], errors="raise").astype("Int64")
    min_year = int(cw["year_start"].min())
    max_year = int(cw["year_start"].max())
    surveys = ", ".join(sorted(cw["survey"].dropna().unique()))
    print(f"Enrollment crosswalk rows: {len(cw):,}. Year span: {min_year}-{max_year}. Surveys: {surveys}")

    key_cols = ["survey", "source_var", "year_start"]
    dup_mask = cw.duplicated(key_cols, keep=False)
    if dup_mask.any():
        print(f"ERROR: Found {dup_mask.sum()} duplicate key rows in enrollment crosswalk.")
        print(cw.loc[dup_mask, key_cols + ['concept_key']].head(10).to_string(index=False))
        raise SystemExit(1)

    def is_blank(x: object) -> bool:
        return pd.isna(x) or str(x).strip() == ""

    blank_mask = cw["concept_key"].map(is_blank)
    fill_counts: dict[str, int] = {}

    def _note_is_blank(series: pd.Series) -> pd.Series:
        return series.map(is_blank)

    if "note" not in cw.columns:
        cw["note"] = ""

    # Rule A: 12-month totals
    mask_e12_total = (
        (cw["survey"] == "12MONTHENROLLMENT")
        & cw["source_var"].isin(["FYRACE24", "EFYTOTLT"])
        & blank_mask
    )
    cw.loc[mask_e12_total, "concept_key"] = E12_HEAD_ALL_TOT_ALL
    cw.loc[mask_e12_total & _note_is_blank(cw["note"]), "note"] = f"auto:{E12_HEAD_ALL_TOT_ALL}"
    fill_counts[E12_HEAD_ALL_TOT_ALL] = int(mask_e12_total.sum())
    blank_mask = cw["concept_key"].map(is_blank)

    # Rule B: Fall totals
    mask_ef_total_old = (
        (cw["survey"] == "FALLENROLLMENT")
        & (cw["source_var"] == "EFRACE24")
        & cw["year_start"].between(2004, 2007, inclusive="both")
        & blank_mask
    )
    mask_ef_total_new = (
        (cw["survey"] == "FALLENROLLMENT")
        & (cw["source_var"] == "EFTOTLT")
        & (cw["year_start"] >= 2008)
        & blank_mask
    )
    mask_ef_total = mask_ef_total_old | mask_ef_total_new
    cw.loc[mask_ef_total, "concept_key"] = EF_HEAD_ALL_TOT_ALL
    cw.loc[mask_ef_total & _note_is_blank(cw["note"]), "note"] = f"auto:{EF_HEAD_ALL_TOT_ALL}"
    fill_counts[EF_HEAD_ALL_TOT_ALL] = int(mask_ef_total.sum())
    blank_mask = cw["concept_key"].map(is_blank)

    # Rule C: Fall FTFT deg/cert seekers
    mask_ef_ftft_degseek = (
        (cw["survey"] == "FALLENROLLMENT")
        & (cw["source_var"] == "EFRES01")
        & blank_mask
    )
    cw.loc[mask_ef_ftft_degseek, "concept_key"] = EF_HEAD_FTFT_UG_DEGSEEK_TOT
    cw.loc[mask_ef_ftft_degseek & _note_is_blank(cw["note"]), "note"] = f"auto:{EF_HEAD_FTFT_UG_DEGSEEK_TOT}"
    fill_counts[EF_HEAD_FTFT_UG_DEGSEEK_TOT] = int(mask_ef_ftft_degseek.sum())
    blank_mask = cw["concept_key"].map(is_blank)

    # Rule D: Full-time undergraduates
    mask_ft_ug_name = (
        (cw["survey"] == "FALLENROLLMENT")
        & cw["source_var"].str.upper().eq("EFUGFT")
        & blank_mask
    )
    mask_ft_ug_label = (
        (cw["survey"] == "FALLENROLLMENT")
        & cw["label_norm"].str.contains("full-time", case=False, na=False)
        & cw["label_norm"].str.contains("undergraduate", case=False, na=False)
        & (
            cw["label_norm"].str.contains("enrollment", case=False, na=False)
            | cw["label_norm"].str.contains("students", case=False, na=False)
        )
        & blank_mask
    )
    mask_ft_ug = mask_ft_ug_name | mask_ft_ug_label
    cw.loc[mask_ft_ug, "concept_key"] = EF_HEAD_FT_UG_TOT_ALL
    cw.loc[mask_ft_ug & _note_is_blank(cw["note"]), "note"] = f"auto:{EF_HEAD_FT_UG_TOT_ALL}"
    fill_counts[EF_HEAD_FT_UG_TOT_ALL] = int(mask_ft_ug.sum())
    blank_mask = cw["concept_key"].map(is_blank)

    # Rule E: Full-time graduate
    grad_ft_varnames = {"EFGRFT"}
    mask_ft_gr_name = (
        (cw["survey"] == "FALLENROLLMENT")
        & cw["source_var"].str.upper().isin(grad_ft_varnames)
        & blank_mask
    )
    mask_ft_gr_label = (
        (cw["survey"] == "FALLENROLLMENT")
        & cw["label_norm"].str.contains("full-time", case=False, na=False)
        & cw["label_norm"].str.contains("graduate", case=False, na=False)
        & (
            cw["label_norm"].str.contains("enrollment", case=False, na=False)
            | cw["label_norm"].str.contains("students", case=False, na=False)
        )
        & blank_mask
    )
    mask_ft_gr = mask_ft_gr_name | mask_ft_gr_label
    cw.loc[mask_ft_gr, "concept_key"] = EF_HEAD_FT_GR_TOT_ALL
    cw.loc[mask_ft_gr & _note_is_blank(cw["note"]), "note"] = f"auto:{EF_HEAD_FT_GR_TOT_ALL}"
    fill_counts[EF_HEAD_FT_GR_TOT_ALL] = int(mask_ft_gr.sum())
    blank_mask = cw["concept_key"].map(is_blank)

    # Rule F: Full-time all levels
    mask_ft_all_label = (
        (cw["survey"] == "FALLENROLLMENT")
        & cw["label_norm"].str.contains("full-time", case=False, na=False)
        & (
            cw["label_norm"].str.contains("enrollment", case=False, na=False)
            | cw["label_norm"].str.contains("students", case=False, na=False)
        )
        & ~cw["label_norm"].str.contains("undergraduate", case=False, na=False)
        & ~cw["label_norm"].str.contains("graduate", case=False, na=False)
        & blank_mask
    )
    cw.loc[mask_ft_all_label, "concept_key"] = EF_HEAD_FT_ALL_TOT_ALL
    cw.loc[mask_ft_all_label & _note_is_blank(cw["note"]), "note"] = f"auto:{EF_HEAD_FT_ALL_TOT_ALL}"
    fill_counts[EF_HEAD_FT_ALL_TOT_ALL] = int(mask_ft_all_label.sum())
    blank_mask = cw["concept_key"].map(is_blank)

    # Rule G: FTFT residence buckets
    base_ftft_ug = (
        (cw["survey"] == "FALLENROLLMENT")
        & (
            cw["label_norm"].str.contains("first-time", case=False, na=False)
            | cw["label_norm"].str.contains("first time", case=False, na=False)
        )
        & (
            cw["label_norm"].str.contains("degree/certificate", case=False, na=False)
            | cw["label_norm"].str.contains("degree-seeking", case=False, na=False)
            | cw["label_norm"].str.contains("degree or certificate", case=False, na=False)
            | cw["label_norm"].str.contains("degree", case=False, na=False)
        )
        & cw["label_norm"].str.contains("undergraduate", case=False, na=False)
        & blank_mask
    )
    mask_res_instate = (
        base_ftft_ug
        & (
            cw["label_norm"].str.contains("in same state", case=False, na=False)
            | cw["label_norm"].str.contains("in same jurisdiction", case=False, na=False)
        )
    )
    mask_res_outstate = (
        base_ftft_ug
        & (
            cw["label_norm"].str.contains("in a different state", case=False, na=False)
            | cw["label_norm"].str.contains("in a different jurisdiction", case=False, na=False)
        )
    )
    mask_res_foreign = (
        base_ftft_ug
        & (
            cw["label_norm"].str.contains("outside the united states", case=False, na=False)
            | cw["label_norm"].str.contains("outside the us", case=False, na=False)
        )
    )
    mask_res_unknown = base_ftft_ug & cw["label_norm"].str.contains("unknown", case=False, na=False)

    cw.loc[mask_res_instate, "concept_key"] = EF_HEAD_FTFT_UG_RES_INSTATE
    cw.loc[mask_res_outstate, "concept_key"] = EF_HEAD_FTFT_UG_RES_OUTSTATE
    cw.loc[mask_res_foreign, "concept_key"] = EF_HEAD_FTFT_UG_RES_FOREIGN
    cw.loc[mask_res_unknown, "concept_key"] = EF_HEAD_FTFT_UG_RES_UNKNOWN
    cw.loc[mask_res_instate & _note_is_blank(cw["note"]), "note"] = f"auto:{EF_HEAD_FTFT_UG_RES_INSTATE}"
    cw.loc[mask_res_outstate & _note_is_blank(cw["note"]), "note"] = f"auto:{EF_HEAD_FTFT_UG_RES_OUTSTATE}"
    cw.loc[mask_res_foreign & _note_is_blank(cw["note"]), "note"] = f"auto:{EF_HEAD_FTFT_UG_RES_FOREIGN}"
    cw.loc[mask_res_unknown & _note_is_blank(cw["note"]), "note"] = f"auto:{EF_HEAD_FTFT_UG_RES_UNKNOWN}"
    fill_counts[EF_HEAD_FTFT_UG_RES_INSTATE] = int(mask_res_instate.sum())
    fill_counts[EF_HEAD_FTFT_UG_RES_OUTSTATE] = int(mask_res_outstate.sum())
    fill_counts[EF_HEAD_FTFT_UG_RES_FOREIGN] = int(mask_res_foreign.sum())
    fill_counts[EF_HEAD_FTFT_UG_RES_UNKNOWN] = int(mask_res_unknown.sum())
    blank_mask = cw["concept_key"].map(is_blank)

    ck_series = cw["concept_key"].astype(str).str.strip()
    missing_mask = ck_series.eq("") | ck_series.str.lower().eq("nan")
    if missing_mask.any():
        print("ERROR: Enrollment crosswalk still has blank concept_key rows. Sample offending rows:")
        print(cw.loc[missing_mask, ["survey", "source_var", "year_start", "label_norm"]].head(10).to_string(index=False))
        raise SystemExit(1)

    args.output.parent.mkdir(parents=True, exist_ok=True)
    cw.to_csv(args.output, index=False)
    filled_total = ck_series.ne("").sum()
    print(f"Wrote autofilled crosswalk to {args.output} ({filled_total} of {len(cw)} rows mapped)")
    print("Autofill rule counts:")
    for concept, count in fill_counts.items():
        print(f"  {concept}: {count}")
    top = cw.loc[ck_series.ne(""), "concept_key"].value_counts().head(20)
    print("Top concept_keys:")
    print(top.to_string())


if __name__ == "__main__":
    main()
