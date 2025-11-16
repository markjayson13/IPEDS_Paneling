"""Auto-fill IC_AY crosswalk template with high-confidence concept keys."""
from __future__ import annotations

import argparse
import math
from pathlib import Path
from typing import Dict, Tuple

import pandas as pd

HERE = Path(__file__).resolve()
PROJECT_ROOT = HERE.parents[2]
DATA_ROOT = Path("/Users/markjaysonfarol13/Higher Ed research/IPEDS")
CROSSWALK_DIR = DATA_ROOT / "Paneled Datasets" / "Crosswalks"
DEFAULT_TEMPLATE = CROSSWALK_DIR / "ic_ay_crosswalk_template.csv"
DEFAULT_OUTPUT_DIR = CROSSWALK_DIR / "Filled"
DEFAULT_OUTPUT_NAME = "ic_ay_crosswalk_autofilled.csv"
SUMMARY_NAME = "ic_ay_crosswalk_autofill_summary.csv"

# Price-of-attendance concepts for FTFT undergrads (CHG1–CHG5)
CHG_CONCEPTS: Dict[str, str] = {
    "CHG1": "PRICE_TUITFEE_IN_DISTRICT_FTFTUG",
    "CHG2": "PRICE_TUITFEE_IN_STATE_FTFTUG",
    "CHG3": "PRICE_TUITFEE_OUT_STATE_FTFTUG",
    "CHG4": "PRICE_BOOK_SUPPLY_FTFTUG",
    "CHG5": "PRICE_RMBD_ON_CAMPUS_FTFTUG",
}

# Average tuition/fee concepts for all FT undergrads (TUITION/FEE 1–3)
TUITION_FEE_CONCEPTS: Dict[str, str] = {
    "TUITION1": "UG_TUIT_IN_DISTRICT_FULLTIME_AVG",
    "TUITION2": "UG_TUIT_IN_STATE_FULLTIME_AVG",
    "TUITION3": "UG_TUIT_OUT_STATE_FULLTIME_AVG",
    "FEE1": "UG_FEE_IN_DISTRICT_FULLTIME_AVG",
    "FEE2": "UG_FEE_IN_STATE_FULLTIME_AVG",
    "FEE3": "UG_FEE_OUT_STATE_FULLTIME_AVG",
}


def _normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df.columns = [c.strip() for c in df.columns]
    return df


def _clean_str(value) -> str:
    if value is None:
        return ""
    if isinstance(value, float):
        if math.isnan(value):
            return ""
    return str(value)


def suggest_concept_key(row: pd.Series) -> str | None:
    """Return a high-confidence concept key suggestion or None."""
    var_raw = _clean_str(row.get("source_var")).strip().upper()
    survey = _clean_str(row.get("survey")).strip().upper()
    label = _clean_str(row.get("label")).strip().lower()

    if not var_raw or not survey:
        return None

    survey_ok = ("IC" in survey) or ("INSTITUTION" in survey)
    if not survey_ok:
        return None

    var_base = var_raw
    for base in list(CHG_CONCEPTS.keys()) + list(TUITION_FEE_CONCEPTS.keys()):
        if var_raw.startswith(base):
            var_base = base
            break

    if var_base == "CHG1":
        has_tuition = "tuition" in label
        has_fee = "fee" in label
        has_district = "district" in label
        if has_tuition and has_fee and (has_district or "in-district" in label):
            return CHG_CONCEPTS[var_base]
        if "tuition" in label and "district" in label and ("fee" in label or "required" in label):
            return CHG_CONCEPTS[var_base]

    if var_base == "CHG2":
        if "tuition" in label and ("in-state" in label or "resident" in label) and "fee" in label:
            if "nonresident" not in label and "out-of-state" not in label:
                return CHG_CONCEPTS[var_base]
        if "tuition and fees" in label and ("in state" in label or "instate" in label):
            return CHG_CONCEPTS[var_base]

    if var_base == "CHG3":
        if "tuition" in label and "fee" in label and ("out-of-state" in label or "nonresident" in label):
            return CHG_CONCEPTS[var_base]

    if var_base == "CHG4":
        if "books" in label and ("suppl" in label or "supplies" in label):
            return CHG_CONCEPTS[var_base]

    if var_base == "CHG5":
        if "room" in label and "board" in label and ("on-campus" in label or "on campus" in label):
            return CHG_CONCEPTS[var_base]
        if "room and board" in label and "campus" in label:
            return CHG_CONCEPTS[var_base]

    if var_base == "TUITION1":
        if "tuition" in label and "in-district" in label and "full-time" in label:
            return TUITION_FEE_CONCEPTS[var_base]

    if var_base == "TUITION2":
        if "tuition" in label and "full-time" in label and (("in-state" in label) or ("resident" in label and "nonresident" not in label)):
            return TUITION_FEE_CONCEPTS[var_base]

    if var_base == "TUITION3":
        if "tuition" in label and "full-time" in label and ("out-of-state" in label or "nonresident" in label):
            return TUITION_FEE_CONCEPTS[var_base]

    if var_base == "FEE1":
        if "fee" in label and "in-district" in label and "full-time" in label:
            return TUITION_FEE_CONCEPTS[var_base]

    if var_base == "FEE2":
        if "fee" in label and "full-time" in label and (("in-state" in label) or ("resident" in label and "nonresident" not in label)):
            return TUITION_FEE_CONCEPTS[var_base]

    if var_base == "FEE3":
        if "fee" in label and "full-time" in label and ("out-of-state" in label or "nonresident" in label):
            return TUITION_FEE_CONCEPTS[var_base]

    return None


def load_template(path: Path) -> pd.DataFrame:
    df = pd.read_csv(path, dtype=str).fillna("")
    df = _normalize_columns(df)
    if "concept_key" not in df.columns:
        df["concept_key"] = ""
    return df


def apply_suggestions(df: pd.DataFrame) -> Tuple[pd.DataFrame, Dict[str, int]]:
    df = df.copy()
    df["concept_key_original"] = df["concept_key"]
    df["concept_key_suggested"] = df.apply(suggest_concept_key, axis=1)

    mask_empty_before = df["concept_key"].isna() | (df["concept_key"].astype(str).str.strip() == "")
    mask_suggest = df["concept_key_suggested"].notna()
    to_fill = mask_empty_before & mask_suggest
    df.loc[to_fill, "concept_key"] = df.loc[to_fill, "concept_key_suggested"]

    stats = {
        "n_total": len(df),
        "n_already_filled": int((~mask_empty_before).sum()),
        "n_new_filled": int(to_fill.sum()),
        "n_suggest_only": int(
            (
                (~mask_empty_before)
                & mask_suggest
                & (df["concept_key_original"].astype(str).str.strip() != df["concept_key_suggested"].astype(str))
            ).sum()
        ),
        "n_unfilled": int((df["concept_key"].isna() | (df["concept_key"].astype(str).str.strip() == "")).sum()),
    }
    return df, stats


def write_outputs(df: pd.DataFrame, stats: Dict[str, int], output_dir: Path, output_name: str, overwrite: bool) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)
    output_path = output_dir / output_name
    if output_path.exists() and not overwrite:
        raise SystemExit(f"Refusing to overwrite existing file {output_path}. Use --overwrite to override.")
    df.to_csv(output_path, index=False)

    summary_records = [{"metric": key, "value": value} for key, value in stats.items()]
    summary_records.extend(
        {"metric": f"concept_key::{key}", "value": val}
        for key, val in df["concept_key"].value_counts(dropna=False).items()
    )
    summary_df = pd.DataFrame(summary_records)
    summary_path = output_dir / SUMMARY_NAME
    summary_df.to_csv(summary_path, index=False)


def print_summary(df: pd.DataFrame, stats: Dict[str, int]) -> None:
    print("Autofill Summary:")
    for key, value in stats.items():
        print(f"  {key}: {value}")

    print("\nConcept key counts:")
    print(df["concept_key"].value_counts(dropna=False).head(50))

    unfilled_mask = df["concept_key"].isna() | (df["concept_key"].astype(str).str.strip() == "")
    if unfilled_mask.any():
        print("\nUnfilled by source_var (top 20):")
        print(df.loc[unfilled_mask, "source_var"].value_counts().head(20))
    else:
        print("\nAll rows filled.")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--template", type=Path, default=DEFAULT_TEMPLATE, help="Path to ic_ay_crosswalk_template.csv")
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR, help="Output directory for autofilled CSV")
    parser.add_argument("--output-name", type=str, default=DEFAULT_OUTPUT_NAME, help="Autofilled CSV filename")
    parser.add_argument("--overwrite", action="store_true", help="Overwrite existing output files")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    template_path = args.template
    if not template_path.exists():
        alt = PROJECT_ROOT / "Paneled Datasets" / "Crosswalks" / template_path.name
        if alt.exists():
            print(f"Template not found at {template_path}. Using fallback {alt}")
            template_path = alt
    if not template_path.exists():
        raise SystemExit(f"Template not found: {template_path}")

    df = load_template(template_path)
    df_filled, stats = apply_suggestions(df)
    print_summary(df_filled, stats)
    write_outputs(df_filled, stats, args.output_dir, args.output_name, args.overwrite)
    print(f"\nWrote autofilled crosswalk to {args.output_dir / args.output_name}")
    print(f"Summary saved to {args.output_dir / SUMMARY_NAME}")


if __name__ == "__main__":
    main()
