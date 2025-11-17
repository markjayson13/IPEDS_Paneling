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
E12_HEAD_UG_TOT_ALL = "E12_HEAD_UG_TOT_ALL"
E12_HEAD_GR_FT_ALL = "E12_HEAD_GR_FT_ALL"
EF_HEAD_ALL_TOT_ALL = "EF_HEAD_ALL_TOT_ALL"
EF_HEAD_UG_TOT_ALL = "EF_HEAD_UG_TOT_ALL"
EF_HEAD_UG_DEGSEEK_TOT = "EF_HEAD_UG_DEGSEEK_TOT"
EF_HEAD_UG_DEGSEEK_FTFT_TOT = "EF_HEAD_UG_DEGSEEK_FTFT_TOT"
EF_HEAD_FTFT_UG_DEGSEEK_TOT = "EF_HEAD_FTFT_UG_DEGSEEK_TOT"
EF_HEAD_FT_ALL_TOT_ALL = "EF_HEAD_FT_ALL_TOT_ALL"
EF_HEAD_FT_UG_TOT_ALL = "EF_HEAD_FT_UG_TOT_ALL"
EF_HEAD_GR_TOT_ALL = "EF_HEAD_GR_TOT_ALL"
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

    # Ensure string dtype for concept_key and note to avoid dtype issues
    if "concept_key" in cw.columns:
        cw["concept_key"] = cw["concept_key"].astype("string")
    else:
        cw["concept_key"] = pd.Series(pd.NA, index=cw.index, dtype="string")

    if "note" in cw.columns:
        cw["note"] = cw["note"].astype("string")
    else:
        cw["note"] = pd.Series("", index=cw.index, dtype="string")

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
        print(cw.loc[dup_mask, key_cols + ["concept_key"]].head(10).to_string(index=False))
        raise SystemExit(1)

    def is_blank(x: object) -> bool:
        return pd.isna(x) or str(x).strip() == ""

    def fresh_blank_mask() -> pd.Series:
        return cw["concept_key"].map(is_blank)

    concepts = [
        E12_HEAD_ALL_TOT_ALL,
        E12_HEAD_UG_TOT_ALL,
        E12_HEAD_GR_FT_ALL,
        EF_HEAD_ALL_TOT_ALL,
        EF_HEAD_UG_TOT_ALL,
        EF_HEAD_UG_DEGSEEK_TOT,
        EF_HEAD_UG_DEGSEEK_FTFT_TOT,
        EF_HEAD_FTFT_UG_DEGSEEK_TOT,
        EF_HEAD_FT_ALL_TOT_ALL,
        EF_HEAD_FT_UG_TOT_ALL,
        EF_HEAD_GR_TOT_ALL,
        EF_HEAD_FT_GR_TOT_ALL,
        EF_HEAD_FTFT_UG_RES_INSTATE,
        EF_HEAD_FTFT_UG_RES_OUTSTATE,
        EF_HEAD_FTFT_UG_RES_FOREIGN,
        EF_HEAD_FTFT_UG_RES_UNKNOWN,
    ]
    fill_counts: dict[str, int] = {key: 0 for key in concepts}

    def _note_is_blank(series: pd.Series) -> pd.Series:
        return series.map(is_blank)

    # E12 graduate FTE (estimated or reported)
    blank_mask = fresh_blank_mask()
    mask_e12_gr_fte = (
        (cw["survey"] == "12MONTHENROLLMENT")
        & (
            cw["label_norm"].str.contains("estimated full-time equivalent fte graduate enrollment", case=False, na=False)
            | cw["label_norm"]
            .str.contains("reported full-time equivalent fte graduate enrollment", case=False, na=False)
        )
        & blank_mask
    )
    if mask_e12_gr_fte.any():
        cw.loc[mask_e12_gr_fte, "concept_key"] = E12_HEAD_GR_FT_ALL
        cw.loc[mask_e12_gr_fte & _note_is_blank(cw["note"]), "note"] = f"auto:{E12_HEAD_GR_FT_ALL}"
    fill_counts[E12_HEAD_GR_FT_ALL] = int(mask_e12_gr_fte.sum())

    # Rule A: 12-month unduplicated totals
    blank_mask = fresh_blank_mask()
    mask_e12_total = (
        (cw["survey"] == "12MONTHENROLLMENT")
        & cw["source_var"].isin(["FYRACE24", "EFYTOTLT"])
        & blank_mask
    )
    cw.loc[mask_e12_total, "concept_key"] = E12_HEAD_ALL_TOT_ALL
    cw.loc[mask_e12_total & _note_is_blank(cw["note"]), "note"] = f"auto:{E12_HEAD_ALL_TOT_ALL}"
    fill_counts[E12_HEAD_ALL_TOT_ALL] = int(mask_e12_total.sum())

    # E12 undergraduate total (only if clearly labeled)
    blank_mask = fresh_blank_mask()
    mask_e12_ug_label = (
        (cw["survey"] == "12MONTHENROLLMENT")
        & cw["label_norm"].str.contains("undergraduate", case=False, na=False)
        & cw["label_norm"].str.contains("total", case=False, na=False)
        & blank_mask
    )
    if mask_e12_ug_label.any():
        cw.loc[mask_e12_ug_label, "concept_key"] = E12_HEAD_UG_TOT_ALL
        cw.loc[mask_e12_ug_label & _note_is_blank(cw["note"]), "note"] = f"auto:{E12_HEAD_UG_TOT_ALL}"
    fill_counts[E12_HEAD_UG_TOT_ALL] = int(mask_e12_ug_label.sum())

    # EF undergraduate deg/cert-seeking FTFT total
    blank_mask = fresh_blank_mask()
    mask_ef_ug_degseek_ftft = (
        (cw["survey"] == "FALLENROLLMENT")
        & cw["label_norm"].str.contains(
            "full-time first-time degree/certificate-seeking undergraduate", case=False, na=False
        )
        & blank_mask
    )
    if mask_ef_ug_degseek_ftft.any():
        cw.loc[mask_ef_ug_degseek_ftft, "concept_key"] = EF_HEAD_UG_DEGSEEK_FTFT_TOT
        cw.loc[mask_ef_ug_degseek_ftft & _note_is_blank(cw["note"]), "note"] = f"auto:{EF_HEAD_UG_DEGSEEK_FTFT_TOT}"
    fill_counts[EF_HEAD_UG_DEGSEEK_FTFT_TOT] = int(mask_ef_ug_degseek_ftft.sum())

    # EF FTFT deg/cert-seeking total when label omits "full-time"
    blank_mask = fresh_blank_mask()
    mask_ef_ftft_ug_degseek = (
        (cw["survey"] == "FALLENROLLMENT")
        & cw["label_norm"].str.contains("first-time degree/certificate-seeking undergraduate students", case=False, na=False)
        & blank_mask
    )
    if mask_ef_ftft_ug_degseek.any():
        cw.loc[mask_ef_ftft_ug_degseek, "concept_key"] = EF_HEAD_FTFT_UG_DEGSEEK_TOT
        cw.loc[mask_ef_ftft_ug_degseek & _note_is_blank(cw["note"]), "note"] = f"auto:{EF_HEAD_FTFT_UG_DEGSEEK_TOT}"
    fill_counts[EF_HEAD_FTFT_UG_DEGSEEK_TOT] = int(mask_ef_ftft_ug_degseek.sum())

    # EF undergraduate deg/cert-seeking total (any load)
    blank_mask = fresh_blank_mask()
    mask_ef_ug_degseek_label = (
        (cw["survey"] == "FALLENROLLMENT")
        & cw["label_norm"].str.contains("degree/certificate-seeking", case=False, na=False)
        & cw["label_norm"].str.contains("undergraduate", case=False, na=False)
        & blank_mask
    )
    if mask_ef_ug_degseek_label.any():
        cw.loc[mask_ef_ug_degseek_label, "concept_key"] = EF_HEAD_UG_DEGSEEK_TOT
        cw.loc[mask_ef_ug_degseek_label & _note_is_blank(cw["note"]), "note"] = f"auto:{EF_HEAD_UG_DEGSEEK_TOT}"
    fill_counts[EF_HEAD_UG_DEGSEEK_TOT] = int(mask_ef_ug_degseek_label.sum())

    # EF undergraduate entering total
    blank_mask = fresh_blank_mask()
    mask_ef_ug_total_entering = (
        (cw["survey"] == "FALLENROLLMENT")
        & cw["label_norm"].str.contains("total entering students at the undergraduate level", case=False, na=False)
        & blank_mask
    )
    if mask_ef_ug_total_entering.any():
        cw.loc[mask_ef_ug_total_entering, "concept_key"] = EF_HEAD_UG_TOT_ALL
        cw.loc[mask_ef_ug_total_entering & _note_is_blank(cw["note"]), "note"] = f"auto:{EF_HEAD_UG_TOT_ALL}"
    fill_counts[EF_HEAD_UG_TOT_ALL] = int(mask_ef_ug_total_entering.sum())

    # EF graduate entering total
    blank_mask = fresh_blank_mask()
    mask_ef_gr_total_entering = (
        (cw["survey"] == "FALLENROLLMENT")
        & cw["label_norm"].str.contains("total entering students at the graduate level", case=False, na=False)
        & blank_mask
    )
    if mask_ef_gr_total_entering.any():
        cw.loc[mask_ef_gr_total_entering, "concept_key"] = EF_HEAD_GR_TOT_ALL
        cw.loc[mask_ef_gr_total_entering & _note_is_blank(cw["note"]), "note"] = f"auto:{EF_HEAD_GR_TOT_ALL}"
    fill_counts[EF_HEAD_GR_TOT_ALL] = int(mask_ef_gr_total_entering.sum())

    # Rule B: Fall grand totals (EFRACE24/EFTOTLT)
    blank_mask = fresh_blank_mask()
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
    if mask_ef_total.any():
        cw.loc[mask_ef_total, "concept_key"] = EF_HEAD_ALL_TOT_ALL
        cw.loc[mask_ef_total & _note_is_blank(cw["note"]), "note"] = f"auto:{EF_HEAD_ALL_TOT_ALL}"
    fill_counts[EF_HEAD_ALL_TOT_ALL] = int(mask_ef_total.sum())

    # Rule D: Full-time undergraduates
    blank_mask = fresh_blank_mask()
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
    if mask_ft_ug.any():
        cw.loc[mask_ft_ug, "concept_key"] = EF_HEAD_FT_UG_TOT_ALL
        cw.loc[mask_ft_ug & _note_is_blank(cw["note"]), "note"] = f"auto:{EF_HEAD_FT_UG_TOT_ALL}"
    fill_counts[EF_HEAD_FT_UG_TOT_ALL] = int(mask_ft_ug.sum())

    # Rule E: Full-time graduate
    blank_mask = fresh_blank_mask()
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
    if mask_ft_gr.any():
        cw.loc[mask_ft_gr, "concept_key"] = EF_HEAD_FT_GR_TOT_ALL
        cw.loc[mask_ft_gr & _note_is_blank(cw["note"]), "note"] = f"auto:{EF_HEAD_FT_GR_TOT_ALL}"
    fill_counts[EF_HEAD_FT_GR_TOT_ALL] = int(mask_ft_gr.sum())

    # Rule F: Full-time all levels
    blank_mask = fresh_blank_mask()
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
    if mask_ft_all_label.any():
        cw.loc[mask_ft_all_label, "concept_key"] = EF_HEAD_FT_ALL_TOT_ALL
        cw.loc[mask_ft_all_label & _note_is_blank(cw["note"]), "note"] = f"auto:{EF_HEAD_FT_ALL_TOT_ALL}"
    fill_counts[EF_HEAD_FT_ALL_TOT_ALL] = int(mask_ft_all_label.sum())

    # Rule G: FTFT residence buckets
    blank_mask = fresh_blank_mask()
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

    if mask_res_instate.any():
        cw.loc[mask_res_instate, "concept_key"] = EF_HEAD_FTFT_UG_RES_INSTATE
        cw.loc[mask_res_instate & _note_is_blank(cw["note"]), "note"] = f"auto:{EF_HEAD_FTFT_UG_RES_INSTATE}"
    if mask_res_outstate.any():
        cw.loc[mask_res_outstate, "concept_key"] = EF_HEAD_FTFT_UG_RES_OUTSTATE
        cw.loc[mask_res_outstate & _note_is_blank(cw["note"]), "note"] = f"auto:{EF_HEAD_FTFT_UG_RES_OUTSTATE}"
    if mask_res_foreign.any():
        cw.loc[mask_res_foreign, "concept_key"] = EF_HEAD_FTFT_UG_RES_FOREIGN
        cw.loc[mask_res_foreign & _note_is_blank(cw["note"]), "note"] = f"auto:{EF_HEAD_FTFT_UG_RES_FOREIGN}"
    if mask_res_unknown.any():
        cw.loc[mask_res_unknown, "concept_key"] = EF_HEAD_FTFT_UG_RES_UNKNOWN
        cw.loc[mask_res_unknown & _note_is_blank(cw["note"]), "note"] = f"auto:{EF_HEAD_FTFT_UG_RES_UNKNOWN}"
    fill_counts[EF_HEAD_FTFT_UG_RES_INSTATE] = int(mask_res_instate.sum())
    fill_counts[EF_HEAD_FTFT_UG_RES_OUTSTATE] = int(mask_res_outstate.sum())
    fill_counts[EF_HEAD_FTFT_UG_RES_FOREIGN] = int(mask_res_foreign.sum())
    fill_counts[EF_HEAD_FTFT_UG_RES_UNKNOWN] = int(mask_res_unknown.sum())

    ck_series = cw["concept_key"].astype(str).str.strip()
    missing_mask = ck_series.eq("") | ck_series.str.lower().eq("nan")
    num_missing = int(missing_mask.sum())
    if num_missing > 0:
        print(f"WARNING: {num_missing} enrollment crosswalk rows still have blank concept_key.")
        print("Sample blank rows:")
        print(
            cw.loc[missing_mask, ["survey", "source_var", "year_start", "label_norm"]]
            .head(10)
            .to_string(index=False)
        )

    args.output.parent.mkdir(parents=True, exist_ok=True)
    cw.to_csv(args.output, index=False)
    num_nonblank = len(cw) - num_missing
    print(f"Wrote autofilled crosswalk to {args.output} ({num_nonblank} of {len(cw)} rows have non-blank concept_key)")
    print("Autofill rule counts:")
    print(f"  {E12_HEAD_GR_FT_ALL}: {mask_e12_gr_fte.sum()}")
    print(f"  {E12_HEAD_ALL_TOT_ALL}: {mask_e12_total.sum()}")
    print(f"  {E12_HEAD_UG_TOT_ALL}: {mask_e12_ug_label.sum()}")
    print(f"  {EF_HEAD_UG_DEGSEEK_FTFT_TOT}: {mask_ef_ug_degseek_ftft.sum()}")
    print(f"  {EF_HEAD_FTFT_UG_DEGSEEK_TOT}: {mask_ef_ftft_ug_degseek.sum()}")
    print(f"  {EF_HEAD_UG_DEGSEEK_TOT}: {mask_ef_ug_degseek_label.sum()}")
    print(f"  {EF_HEAD_UG_TOT_ALL}: {mask_ef_ug_total_entering.sum()}")
    print(f"  {EF_HEAD_GR_TOT_ALL}: {mask_ef_gr_total_entering.sum()}")
    print(f"  {EF_HEAD_ALL_TOT_ALL}: {mask_ef_total.sum()}")
    print(f"  {EF_HEAD_FT_UG_TOT_ALL}: {mask_ft_ug.sum()}")
    print(f"  {EF_HEAD_FT_GR_TOT_ALL}: {mask_ft_gr.sum()}")
    print(f"  {EF_HEAD_FT_ALL_TOT_ALL}: {mask_ft_all_label.sum()}")
    print(f"  {EF_HEAD_FTFT_UG_RES_INSTATE}: {mask_res_instate.sum()}")
    print(f"  {EF_HEAD_FTFT_UG_RES_OUTSTATE}: {mask_res_outstate.sum()}")
    print(f"  {EF_HEAD_FTFT_UG_RES_FOREIGN}: {mask_res_foreign.sum()}")
    print(f"  {EF_HEAD_FTFT_UG_RES_UNKNOWN}: {mask_res_unknown.sum()}")
    top = cw.loc[ck_series.ne(""), "concept_key"].value_counts().head(20)
    print("Top concept_keys:")
    print(top.to_string())


if __name__ == "__main__":
    main()
