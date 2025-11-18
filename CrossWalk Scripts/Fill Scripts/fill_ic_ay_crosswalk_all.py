"""Fill all IC_AY crosswalk rows using strict mappings plus deterministic slugs."""
from __future__ import annotations

import argparse
import hashlib
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Tuple

import logging
import pandas as pd

HERE = Path(__file__).resolve()
PROJECT_ROOT = HERE.parents[2]
DATA_ROOT = Path("/Users/markjaysonfarol13/Higher Ed research/IPEDS")
CROSSWALK_DIR = DATA_ROOT / "Paneled Datasets" / "Crosswalks"
FILLED_DIR = CROSSWALK_DIR / "Filled"
DEFAULT_INPUT = CROSSWALK_DIR / "ic_ay_crosswalk_template.csv"
DEFAULT_TEMPLATE_FALLBACK = CROSSWALK_DIR / "ic_ay_crosswalk_template.csv"
DEFAULT_OUTPUT_DIR = FILLED_DIR
DEFAULT_OUTPUT_NAME = "ic_ay_crosswalk_all.csv"

STRICT_CONCEPTS = {
    "CHG5": "PRICE_RMBD_ON_CAMPUS_FTFTUG",
    "TUITION1": "UG_TUIT_IN_DISTRICT_FULLTIME_AVG",
    "TUITION2": "UG_TUIT_IN_STATE_FULLTIME_AVG",
    "TUITION3": "UG_TUIT_OUT_STATE_FULLTIME_AVG",
    "FEE1": "UG_FEE_IN_DISTRICT_FULLTIME_AVG",
    "FEE2": "UG_FEE_IN_STATE_FULLTIME_AVG",
    "FEE3": "UG_FEE_OUT_STATE_FULLTIME_AVG",
}

CANONICAL_MAP = {
    "CHG5": "PRICE_RMBD_ON_CAMPUS_FTFTUG",
    "RMBRDAMT": "PRICE_RMBD_ON_CAMPUS_FTFTUG",
    "ROOMAMT": "PRICE_RMBD_ON_CAMPUS_FTFTUG",
    "BOARDAMT": "PRICE_RMBD_ON_CAMPUS_FTFTUG",
    "TUITION1": "UG_TUIT_IN_DISTRICT_FULLTIME_AVG",
    "TUITION2": "UG_TUIT_IN_STATE_FULLTIME_AVG",
    "TUITION3": "UG_TUIT_OUT_STATE_FULLTIME_AVG",
    "FEE1": "UG_FEE_IN_DISTRICT_FULLTIME_AVG",
    "FEE2": "UG_FEE_IN_STATE_FULLTIME_AVG",
    "FEE3": "UG_FEE_OUT_STATE_FULLTIME_AVG",
}

CANONICAL_CONCEPTS = set(CANONICAL_MAP.values())

RES_STOPWORDS = {"OF", "THE", "FOR", "AND", "IN", "AT", "BY", "WITH", "ON"}


@dataclass
class SchemaRule:
    concept_key: str
    source_prefix: Optional[str] = None
    label_pattern: Optional[re.Pattern] = None


def _schema_rules() -> List[SchemaRule]:
    def patt(regex: str) -> re.Pattern:
        return re.compile(regex, flags=re.IGNORECASE)

    return [
        # 1. Total price of attendance by residency & housing
        SchemaRule(
            concept_key="ICAY_COA_INDIST_ONCAMP",
            label_pattern=patt(r"(total price|price of attendance|cost of attendance).*in[- ]district.*on[- ]campus"),
        ),
        SchemaRule(
            concept_key="ICAY_COA_INST_ONCAMP",
            label_pattern=patt(r"(total price|price of attendance|cost of attendance).*in[- ]state.*on[- ]campus"),
        ),
        SchemaRule(
            concept_key="ICAY_COA_OUTST_ONCAMP",
            label_pattern=patt(r"(total price|price of attendance|cost of attendance).*(out[- ]of[- ]state|nonresident).*on[- ]campus"),
        ),
        SchemaRule(
            concept_key="ICAY_COA_INDIST_OFFNWF",
            label_pattern=patt(r"(total price|price of attendance|cost of attendance).*in[- ]district.*off[- ]campus.*not with family"),
        ),
        SchemaRule(
            concept_key="ICAY_COA_INST_OFFNWF",
            label_pattern=patt(r"(total price|price of attendance|cost of attendance).*in[- ]state.*off[- ]campus.*not with family"),
        ),
        SchemaRule(
            concept_key="ICAY_COA_OUTST_OFFNWF",
            label_pattern=patt(r"(total price|price of attendance|cost of attendance).*(out[- ]of[- ]state|nonresident).*off[- ]campus.*not with family"),
        ),
        SchemaRule(
            concept_key="ICAY_COA_INDIST_OFFFAM",
            label_pattern=patt(r"(total price|price of attendance|cost of attendance).*in[- ]district.*off[- ]campus.*with family"),
        ),
        SchemaRule(
            concept_key="ICAY_COA_INST_OFFFAM",
            label_pattern=patt(r"(total price|price of attendance|cost of attendance).*in[- ]state.*off[- ]campus.*with family"),
        ),
        SchemaRule(
            concept_key="ICAY_COA_OUTST_OFFFAM",
            label_pattern=patt(r"(total price|price of attendance|cost of attendance).*(out[- ]of[- ]state|nonresident).*off[- ]campus.*with family"),
        ),
        SchemaRule(
            concept_key="ICAY_COA_COMBINED_ALL",
            label_pattern=patt(r"combined.*tuition.*fees.*books.*suppl.*room.*board.*other expenses"),
        ),
        # 1a. Comprehensive fee (in-district)
        SchemaRule(
            concept_key="ICAY_COA_COMP_INDIST_CURR",
            source_prefix="CMP1",
            label_pattern=patt(r"comprehensive fee"),
        ),
        SchemaRule(
            concept_key="ICAY_COA_COMP_INST_CURR",
            source_prefix="CMP2",
            label_pattern=patt(r"comprehensive fee"),
        ),
        SchemaRule(
            concept_key="ICAY_COA_COMP_OUTST_CURR",
            source_prefix="CMP3",
            label_pattern=patt(r"comprehensive fee"),
        ),
        # 2. Current-year components (published tuition+fees, books+supplies, RMBD, other)
        SchemaRule(
            concept_key="ICAY_TUITFEE_INDIST_CURR",
            label_pattern=patt(r"published.*in[- ]district.*tuition.*(required fees|tuition and fees)"),
        ),
        SchemaRule(
            concept_key="ICAY_TUITFEE_INST_CURR",
            label_pattern=patt(r"published.*in[- ]state.*tuition.*(required fees|tuition and fees)"),
        ),
        SchemaRule(
            concept_key="ICAY_TUITFEE_OUTST_CURR",
            label_pattern=patt(r"published.*(out[- ]of[- ]state|nonresident).*tuition.*(required fees|tuition and fees)"),
        ),
        SchemaRule(
            concept_key="ICAY_BOOK_SUPPLY_CURR",
            label_pattern=patt(r"books.*supplies"),
        ),
        SchemaRule(
            concept_key="ICAY_RMBD_ONCAMP_CURR",
            label_pattern=patt(r"on[- ]campus.*room and board"),
        ),
        SchemaRule(
            concept_key="ICAY_OTHER_ONCAMP_CURR",
            label_pattern=patt(r"on[- ]campus.*other expenses"),
        ),
        SchemaRule(
            concept_key="ICAY_RMBD_OFFNWF_CURR",
            label_pattern=patt(r"off[- ]campus.*not with family.*room and board"),
        ),
        SchemaRule(
            concept_key="ICAY_OTHER_OFFNWF_CURR",
            label_pattern=patt(r"off[- ]campus.*not with family.*other expenses"),
        ),
        SchemaRule(
            concept_key="ICAY_OTHER_OFFFAM_CURR",
            label_pattern=patt(r"off[- ]campus.*with family.*other expenses"),
        ),
        # 3. Tuition/fees detail & guaranteed pct, by residency
        SchemaRule(
            concept_key="ICAY_TUIT_INDIST_CURR",
            label_pattern=patt(r"published.*in[- ]district.*tuition(?!.*fees)"),
        ),
        SchemaRule(
            concept_key="ICAY_FEE_INDIST_CURR",
            label_pattern=patt(r"published.*in[- ]district.*(required fee|fees)"),
        ),
        SchemaRule(
            concept_key="ICAY_TUIT_INDIST_GUAR_PCT",
            label_pattern=patt(r"in[- ]district.*tuition.*guaranteed.*percent increase"),
        ),
        SchemaRule(
            concept_key="ICAY_FEE_INDIST_GUAR_PCT",
            label_pattern=patt(r"in[- ]district.*fees.*guaranteed.*percent increase"),
        ),
        SchemaRule(
            concept_key="ICAY_TUIT_INST_CURR",
            label_pattern=patt(r"published.*in[- ]state.*tuition(?!.*fees)"),
        ),
        SchemaRule(
            concept_key="ICAY_FEE_INST_CURR",
            label_pattern=patt(r"published.*in[- ]state.*(required fee|fees)"),
        ),
        SchemaRule(
            concept_key="ICAY_TUIT_INST_GUAR_PCT",
            label_pattern=patt(r"in[- ]state.*tuition.*guaranteed.*percent increase"),
        ),
        SchemaRule(
            concept_key="ICAY_FEE_INST_GUAR_PCT",
            label_pattern=patt(r"in[- ]state.*fees.*guaranteed.*percent increase"),
        ),
        SchemaRule(
            concept_key="ICAY_TUIT_OUTST_CURR",
            label_pattern=patt(r"published.*(out[- ]of[- ]state|nonresident).*tuition(?!.*fees)"),
        ),
        SchemaRule(
            concept_key="ICAY_FEE_OUTST_CURR",
            label_pattern=patt(r"published.*(out[- ]of[- ]state|nonresident).*fees"),
        ),
        SchemaRule(
            concept_key="ICAY_TUIT_OUTST_GUAR_PCT",
            label_pattern=patt(r"(out[- ]of[- ]state|nonresident).*tuition.*guaranteed.*percent increase"),
        ),
        SchemaRule(
            concept_key="ICAY_FEE_OUTST_GUAR_PCT",
            label_pattern=patt(r"(out[- ]of[- ]state|nonresident).*fees.*guaranteed.*percent increase"),
        ),
        SchemaRule(
            concept_key="ICAY_TUIT_VARIES_RESIDENCY",
            label_pattern=patt(r"tuition charge varies.*(in[- ]district|in[- ]state|out[- ]of[- ]state|residency)"),
        ),
        # 4. Alternative tuition plans & Promise
        SchemaRule(
            concept_key="ICAY_ALTPLAN_ANY",
            label_pattern=patt(r"any alternative tuition plans"),
        ),
        SchemaRule(
            concept_key="ICAY_ALTPLAN_GUARANTEE",
            label_pattern=patt(r"tuition guaranteed plan"),
        ),
        SchemaRule(
            concept_key="ICAY_ALTPLAN_PREPAID",
            label_pattern=patt(r"prepaid tuition plan"),
        ),
        SchemaRule(
            concept_key="ICAY_ALTPLAN_PAYMENT",
            label_pattern=patt(r"tuition payment plan"),
        ),
        SchemaRule(
            concept_key="ICAY_ALTPLAN_OTHER",
            label_pattern=patt(r"other alternative tuition plan"),
        ),
        SchemaRule(
            concept_key="ICAY_PROMISE_PROGRAM",
            label_pattern=patt(r"promise program"),
        ),
        # 5. Room & board infrastructure
        SchemaRule(
            concept_key="ICAY_ONCAMP_HOUSING_FLAG",
            label_pattern=patt(r"provide on[- ]campus housing"),
        ),
        SchemaRule(
            concept_key="ICAY_DORM_CAPACITY",
            label_pattern=patt(r"total dormitory capacity"),
        ),
        SchemaRule(
            concept_key="ICAY_BOARD_MEAL_PLAN_FLAG",
            label_pattern=patt(r"provides board or meal plan"),
        ),
        SchemaRule(
            concept_key="ICAY_MEALS_PER_WEEK",
            label_pattern=patt(r"number of meals per week.*board charge"),
        ),
        SchemaRule(
            concept_key="ICAY_ROOM_CHARGE_TYPICAL",
            label_pattern=patt(r"typical room charge.*academic year"),
        ),
        SchemaRule(
            concept_key="ICAY_BOARD_CHARGE_TYPICAL",
            label_pattern=patt(r"typical board charge.*academic year"),
        ),
        SchemaRule(
            concept_key="ICAY_RMBD_COMBINED_TYPICAL",
            label_pattern=patt(r"combined charge for room and board"),
        ),
    ]


SCHEMA_CONCEPT_KEYS = {rule.concept_key for rule in _schema_rules()}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Fill ALL IC_AY concept_keys using strict rules + generic label slugs."
    )
    parser.add_argument(
        "--input",
        type=Path,
        default=DEFAULT_INPUT,
        help="Input crosswalk CSV (default: ic_ay_crosswalk_template.csv)",
    )
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR, help="Output directory")
    parser.add_argument("--output-name", type=str, default=DEFAULT_OUTPUT_NAME, help="Output filename")
    parser.add_argument("--overwrite", action="store_true", help="Allow overwriting existing output")
    return parser.parse_args()


def resolve_input(path: Path | None) -> Path:
    candidates = []
    if path:
        candidates.append(path)
    candidates.extend(
        [
            DEFAULT_INPUT,
            DEFAULT_TEMPLATE_FALLBACK,
            PROJECT_ROOT / "Paneled Datasets" / "Crosswalks" / "ic_ay_crosswalk_template.csv",
        ]
    )
    seen = set()
    for candidate in candidates:
        if candidate in seen:
            continue
        seen.add(candidate)
        if candidate.exists():
            if candidate != path:
                print(f"Using input file: {candidate}")
            return candidate
    raise SystemExit("No available IC_AY crosswalk input. Provide --input pointing to a CSV file.")


def load_crosswalk(path: Path) -> pd.DataFrame:
    df = pd.read_csv(path, dtype=str).fillna("")
    df.columns = [c.strip() for c in df.columns]
    if "label" not in df.columns:
        for candidate in ("varlab", "var_label"):
            if candidate in df.columns:
                df["label"] = df[candidate]
                break
    if "label" not in df.columns:
        df["label"] = ""
    required = {"concept_key", "survey", "source_var", "label", "year_start", "year_end"}
    missing = required - set(df.columns)
    if missing:
        raise SystemExit(f"IC_AY crosswalk missing required columns: {sorted(missing)}")
    for optional in ("table", "notes"):
        if optional not in df.columns:
            df[optional] = ""
    df["concept_key_original"] = df["concept_key"].astype(str)
    df["concept_key_source"] = ""
    df["survey"] = df["survey"].astype(str).str.strip().str.upper()
    df["source_var"] = df["source_var"].astype(str).str.strip()
    df["year_start"] = pd.to_numeric(df["year_start"], errors="raise").astype("Int64")
    df["year_end"] = pd.to_numeric(df["year_end"], errors="raise").astype("Int64")
    bad_range = df["year_start"] > df["year_end"]
    if bad_range.any():
        print("ERROR: IC_AY crosswalk has rows with year_start > year_end.")
        print(df.loc[bad_range, ["survey", "source_var", "year_start", "year_end"]].head(10).to_string(index=False))
        raise SystemExit(1)
    key_cols = ["survey", "source_var", "year_start", "year_end"]
    dup_mask = df.duplicated(key_cols, keep=False)
    if dup_mask.any():
        print(f"ERROR: Found {dup_mask.sum()} duplicate key rows in IC_AY crosswalk.")
        print(df.loc[dup_mask, key_cols + ["concept_key"]].head(10).to_string(index=False))
        raise SystemExit(1)
    min_year = int(df["year_start"].min())
    max_year = int(df["year_end"].max())
    surveys = ", ".join(sorted(df["survey"].dropna().unique()))
    print(f"IC_AY template rows: {len(df):,}. Year span: {min_year}-{max_year}. Surveys: {surveys}")
    return df


def _clean(value: str) -> str:
    return (value or "").strip()


def _normalize_var(var: str) -> str:
    for key in STRICT_CONCEPTS:
        if var.startswith(key):
            return key
    return var


def suggest_concept_key_strict(row: pd.Series) -> str | None:
    var = _normalize_var(_clean(row.get("source_var", "")).upper())
    survey = _clean(row.get("survey", "")).upper()
    label = _clean(row.get("label", "")).lower()
    if not var or not survey or "IC" not in survey or "AY" not in survey:
        return None
    if var == "CHG5":
        return STRICT_CONCEPTS[var]
    if var == "TUITION1" and "tuition" in label and "in-district" in label and "full-time" in label:
        return STRICT_CONCEPTS[var]
    if var == "TUITION2" and "tuition" in label and ("in-state" in label or "in state" in label or "resident" in label) and "full-time" in label:
        return STRICT_CONCEPTS[var]
    if var == "TUITION3" and "tuition" in label and ("out-of-state" in label or "out of state" in label or "nonresident" in label) and "full-time" in label:
        return STRICT_CONCEPTS[var]
    if var == "FEE1" and "fee" in label and "in-district" in label and "full-time" in label:
        return STRICT_CONCEPTS[var]
    if var == "FEE2" and "fee" in label and ("in-state" in label or "in state" in label or "resident" in label) and "full-time" in label:
        return STRICT_CONCEPTS[var]
    if var == "FEE3" and "fee" in label and ("out-of-state" in label or "out of state" in label or "nonresident" in label) and "full-time" in label:
        return STRICT_CONCEPTS[var]
    return None


def _tokenize_label(label: str) -> list[str]:
    clean = re.sub(r"[^a-z0-9]+", " ", label.lower())
    tokens = [tok.upper() for tok in clean.split() if tok]
    filtered: list[str] = []
    for tok in tokens:
        if tok in RES_STOPWORDS:
            continue
        if tok.isdigit():
            continue
        if len(tok) == 4 and tok.isdigit():
            continue
        filtered.append(tok)
    return filtered


def slugify_label_to_concept(row: pd.Series, used_keys: set[str]) -> str:
    var = _clean(row.get("source_var", "")).upper()
    survey = _clean(row.get("survey", "")).upper()
    label_raw = _clean(row.get("label", ""))
    label = (
        label_raw.replace("\\n", " ")
        .replace("\n", " ")
        .replace("\r", " ")
        .lower()
    )

    charge_type = ""
    if "cost of attendance" in label or "coa" in label:
        charge_type = "COA"
    elif "tuition and required fees" in label:
        charge_type = "TUITFEE"
    elif "tuition" in label and "fees" in label:
        charge_type = "TUITFEE"
    elif "tuition" in label:
        charge_type = "TUIT"
    elif "required fee" in label or "fees" in label:
        charge_type = "FEE"
    elif "books and supplies" in label or ("books" in label and "suppl" in label):
        charge_type = "BOOKSUP"
    elif "room and board" in label or ("housing" in label and "food" in label):
        charge_type = "RMBD"
    elif "room" in label and "board" not in label:
        charge_type = "ROOM"
    elif "board" in label and "room" not in label:
        charge_type = "BOARD"
    elif "other expense" in label:
        charge_type = "OTHER"
    elif "cost of attendance" in label or "total price" in label:
        charge_type = "PRICE"
    else:
        charge_type = "CHARGE"

    residency = ""
    if "in-district" in label or "in district" in label:
        residency = "INDIST"
    elif "in-state" in label or "in state" in label:
        residency = "INST"
    elif "out-of-state" in label or "out of state" in label or "nonresident" in label:
        residency = "OUTST"
    elif "resident" in label:
        residency = "RES"

    level = ""
    if "undergraduate" in label or "ug" in label:
        level = "UG"
    elif "graduate" in label:
        level = "GR"
    elif "first-professional" in label or "professional practice" in label:
        level = "PROF"
    elif "doctoral" in label:
        level = "DR"

    attendance = ""
    if "full-time" in label or "full time" in label:
        attendance = "FT"
    elif "part-time" in label or "part time" in label:
        attendance = "PT"

    cohort = ""
    if "first-time" in label and "degree" in label:
        cohort = "FTFT"
    elif "first-time" in label:
        cohort = "FIRST"
    elif "degree/certificate-seeking" in label:
        cohort = "DGSEEK"

    living = ""
    if "on-campus" in label or "on campus" in label:
        living = "ONCAMP"
    elif "off-campus" in label and "family" in label:
        living = "OFFFAM"
    elif "off-campus" in label:
        living = "OFFNWF"
    elif "with family" in label:
        living = "WITHFAM"

    calendar = ""
    if "9-month" in label or "9 month" in label:
        calendar = "9M"
    elif "12-month" in label or "12 month" in label:
        calendar = "12M"
    elif "per credit" in label:
        calendar = "PERCRED"
    elif "per term" in label or "per semester" in label:
        calendar = "PERTERM"

    components: list[str] = ["ICAY", charge_type, residency, level, attendance, cohort, living, calendar]
    components = [c for c in components if c]

    if len(components) <= 2:
        fallback_tokens = _tokenize_label(label)
        components.extend(fallback_tokens[:5])

    components.append(f"VAR_{var or 'UNKNOWN'}")
    key = "_".join(filter(None, components))
    key = re.sub(r"_+", "_", key).strip("_")

    if len(key) > 80:
        suffix = ""
        if key.endswith(f"VAR_{var}"):
            suffix = f"VAR_{var}"
            core = key[: -(len(suffix) + 1)]
            core = core[: max(0, 80 - len(suffix) - 1)]
            key = f"{core}_{suffix}".strip("_")
        else:
            key = key[:80].rstrip("_")

    base_key = key
    if key in used_keys:
        fingerprint = hashlib.sha1(f"{survey}|{var}|{label_raw}".encode("utf-8")).hexdigest()[:6].upper()
        key = f"{base_key}_H{fingerprint}"
        while key in used_keys:
            fingerprint = hashlib.sha1((fingerprint + label_raw).encode("utf-8")).hexdigest()[:6].upper()
            key = f"{base_key}_H{fingerprint}"

    used_keys.add(key)
    return key


def apply_canonical_map(df: pd.DataFrame) -> Tuple[pd.DataFrame, int]:
    """
    Fill canonical PRICE_*/UG_* concept_keys based on source_var prefixes.
    Only operates on currently blank concept_key rows.
    """
    df = df.copy()
    filled = 0
    for idx, row in df.iterrows():
        current = _clean(row.get("concept_key", ""))
        if current:
            continue
        source_var = _clean(row.get("source_var", "")).upper()
        if not source_var:
            continue
        for prefix, concept in CANONICAL_MAP.items():
            if source_var.startswith(prefix):
                df.at[idx, "concept_key"] = concept
                df.at[idx, "concept_key_source"] = "canonical_map"
                filled += 1
                break
    return df, filled


def _assert_no_overlaps_icay(df: pd.DataFrame) -> None:
    groups = df.groupby(["survey", "source_var", "concept_key"], dropna=False)
    for key, grp in groups:
        grp_sorted = grp.sort_values("year_start")
        prev_end = None
        for _, row in grp_sorted.iterrows():
            start = int(row["year_start"])
            end = int(row["year_end"])
            if prev_end is not None and start <= prev_end:
                print("ERROR: IC_AY overlap detected for", key)
                print(
                    grp_sorted[["survey", "source_var", "concept_key", "year_start", "year_end"]]
                    .head(10)
                    .to_string(index=False)
                )
                raise SystemExit(1)
            prev_end = max(prev_end or end, end)


def schema_concept_for_row(row: pd.Series, rules: List[SchemaRule]) -> str | None:
    """
    Apply schema-first mapping: if a row's label (and optionally source_var)
    matches one of the SCHEMA_RULES, return the stable concept_key.
    """
    label = _clean(row.get("label", ""))
    label_lower = label.lower()
    source_var = _clean(row.get("source_var", "")).upper()

    if not label:
        return None

    has_price_attendance = ("price of attendance" in label_lower) or ("cost of attendance" in label_lower)
    has_comprehensive_fee = "comprehensive fee" in label_lower
    has_guaranteed_pct = ("guaranteed percent increase" in label_lower) or ("guaranteed percent" in label_lower)

    if has_comprehensive_fee and has_guaranteed_pct:
        # Percent increases for comprehensive fees should not hit schema concepts; fall back to slugging.
        return None

    for rule in rules:
        if has_comprehensive_fee:
            if not rule.concept_key.startswith("ICAY_COA_COMP_"):
                continue
        if has_price_attendance and not has_comprehensive_fee:
            if not rule.concept_key.startswith("ICAY_COA_"):
                continue
        if has_guaranteed_pct:
            if rule.concept_key in {
                "ICAY_TUIT_INDIST_CURR",
                "ICAY_TUIT_INST_CURR",
                "ICAY_TUIT_OUTST_CURR",
                "ICAY_FEE_INDIST_CURR",
                "ICAY_FEE_INST_CURR",
                "ICAY_FEE_OUTST_CURR",
            }:
                continue
        if rule.concept_key == "ICAY_BOOK_SUPPLY_CURR":
            if has_comprehensive_fee or has_price_attendance:
                continue
        if rule.source_prefix is not None and not source_var.startswith(rule.source_prefix):
            continue
        if rule.label_pattern is not None and not rule.label_pattern.search(label):
            continue
        return rule.concept_key

    return None


def fill_concept_keys(df: pd.DataFrame) -> Tuple[pd.DataFrame, Dict[str, int]]:
    df = df.copy()
    df["concept_key"] = df["concept_key"].astype(str).str.strip()
    if "concept_key_source" not in df.columns:
        df["concept_key_source"] = ""
    df["concept_key_source"] = df["concept_key_source"].astype(str)

    # 0. Canonical PRICE_*/UG_* mapping
    df, n_canonical = apply_canonical_map(df)

    concept = df["concept_key"].astype(str).str.strip()
    existing_mask = concept != ""
    empty_source_mask = df["concept_key_source"].astype(str).str.strip() == ""
    df.loc[existing_mask & empty_source_mask, "concept_key_source"] = "existing"
    used_keys = set(concept[existing_mask])

    rules = _schema_rules()

    # 1. Schema-first mapping for high-priority concepts
    schema_mask = concept.eq("")
    schema_results = df.loc[schema_mask].apply(lambda row: schema_concept_for_row(row, rules), axis=1)
    schema_idx = schema_results.index[schema_results.notna()]
    if len(schema_idx) > 0:
        df.loc[schema_idx, "concept_key"] = schema_results.loc[schema_idx]
        df.loc[schema_idx, "concept_key_source"] = "schema_rule"
        used_keys.update(df.loc[schema_idx, "concept_key"].tolist())

    # Recompute mask after schema mapping
    concept = df["concept_key"].astype(str).str.strip()
    existing_mask = concept != ""

    # 2. Strict CHG/TUITION/FEE mapping for remaining empties
    empty_mask = ~existing_mask
    strict_results = df.loc[empty_mask].apply(suggest_concept_key_strict, axis=1)
    strict_idx = strict_results.index[strict_results.notna()]
    if len(strict_idx) > 0:
        df.loc[strict_idx, "concept_key"] = strict_results.loc[strict_idx]
        df.loc[strict_idx, "concept_key_source"] = "strict_rule"
        used_keys.update(df.loc[strict_idx, "concept_key"].tolist())

    # 3. Generic slugs for everything else (except canonical/schema keys)
    concept = df["concept_key"].astype(str).str.strip()
    canonical_mask = df["concept_key"].isin(CANONICAL_CONCEPTS) | df["concept_key"].isin(SCHEMA_CONCEPT_KEYS)
    remaining_mask = (concept == "") & (~canonical_mask)
    generic_idx = df.index[remaining_mask]
    for idx in generic_idx:
        df.at[idx, "concept_key"] = slugify_label_to_concept(df.loc[idx], used_keys)
        df.at[idx, "concept_key_source"] = "generic_slug"

    ck_series = df["concept_key"].astype(str).str.strip()
    missing_mask = ck_series.eq("") | ck_series.str.lower().eq("nan")
    if missing_mask.any():
        print("ERROR: IC_AY autofill left blank concept_key rows. Sample:")
        print(
            df.loc[missing_mask, ["survey", "source_var", "year_start", "year_end", "label"]]
            .head(10)
            .to_string(index=False)
        )
        raise SystemExit(1)
    _assert_no_overlaps_icay(df)
    stats = {
        "n_total": len(df),
        "n_canonical": n_canonical,
        "n_existing": int((df["concept_key_source"] == "existing").sum()),
        "n_schema": int((df["concept_key_source"] == "schema_rule").sum()),
        "n_strict": int((df["concept_key_source"] == "strict_rule").sum()),
        "n_generic": int((df["concept_key_source"] == "generic_slug").sum()),
    }
    return df, stats


def write_outputs(df: pd.DataFrame, stats: Dict[str, int], output_dir: Path, output_name: str, overwrite: bool) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)
    output_path = output_dir / output_name
    if output_path.exists() and not overwrite:
        raise SystemExit(f"Refusing to overwrite {output_path}. Use --overwrite to override.")
    df.to_csv(output_path, index=False)
    logging.info("Saved filled IC_AY crosswalk to %s", output_path)


def print_summary(df: pd.DataFrame, stats: Dict[str, int]) -> None:
    print(f"Total rows: {stats['n_total']}")
    print(f"Filled by canonical map (PRICE_*/UG_*): {stats.get('n_canonical', 0)}")
    print(f"Existing concept_key kept: {stats['n_existing']}")
    print(f"Filled by schema rules: {stats.get('n_schema', 0)}")
    print(f"Filled by strict rules: {stats['n_strict']}")
    print(f"Filled by generic slug: {stats['n_generic']}")
    print(f"Distinct concept_keys: {df['concept_key'].nunique()}")

    print("\nConcept_key frequencies (top 40):")
    print(df["concept_key"].value_counts().head(40))

    mask_schema = df["concept_key_source"] == "schema_rule"
    if mask_schema.any():
        print("\nSchema concept_key frequencies:")
        print(df.loc[mask_schema, "concept_key"].value_counts().to_string())

    mask_generic = df["concept_key_source"] == "generic_slug"
    print("\nSource_var using generic_slug (top 20):")
    print(df.loc[mask_generic, "source_var"].value_counts().head(20))

    schema_filled = set(df.loc[df["concept_key_source"] == "schema_rule", "concept_key"])
    missing_schema = sorted(SCHEMA_CONCEPT_KEYS - schema_filled)
    if missing_schema:
        print("\nSchema concept_keys with NO matching rows in IC_AY crosswalk:")
        for ck in missing_schema:
            print(f"  - {ck}")
    else:
        print("\nAll schema concept_keys have at least one matching crosswalk row.")


def main() -> None:
    args = parse_args()
    input_path = resolve_input(args.input)
    df = load_crosswalk(input_path)
    concept_nonempty = df["concept_key"].astype(str).str.strip().ne("")
    n_nonempty = int(concept_nonempty.sum())
    print(f"Input rows: {len(df):,}. Non-empty concept_key rows: {n_nonempty:,}.")
    if n_nonempty == 0:
        print("Input concept_key column is empty; canonical/schema fill will populate from the template.")
    df_filled, stats = fill_concept_keys(df)
    print_summary(df_filled, stats)
    write_outputs(df_filled, stats, args.output_dir, args.output_name, args.overwrite)
    print(f"\nSaved fully filled crosswalk to {args.output_dir / args.output_name}")


if __name__ == "__main__":
    main()
