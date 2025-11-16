"""Fill all IC_AY crosswalk rows using strict mappings plus deterministic slugs."""
from __future__ import annotations

import argparse
import hashlib
import re
from pathlib import Path
from typing import Iterable, Tuple

import pandas as pd

HERE = Path(__file__).resolve()
PROJECT_ROOT = HERE.parents[2]
DATA_ROOT = Path("/Users/markjaysonfarol13/Higher Ed research/IPEDS")
CROSSWALK_DIR = DATA_ROOT / "Paneled Datasets" / "Crosswalks"
FILLED_DIR = CROSSWALK_DIR / "Filled"
DEFAULT_INPUT = CROSSWALK_DIR / "ic_ay_crosswalk_autofilled.csv"
DEFAULT_TEMPLATE_FALLBACK = CROSSWALK_DIR / "ic_ay_crosswalk_template.csv"
DEFAULT_OUTPUT_DIR = FILLED_DIR
DEFAULT_OUTPUT_NAME = "ic_ay_crosswalk_all.csv"
SUMMARY_NAME = "ic_ay_crosswalk_all_summary.csv"

STRICT_CONCEPTS = {
    "CHG1": "PRICE_TUITFEE_IN_DISTRICT_FTFTUG",
    "CHG2": "PRICE_TUITFEE_IN_STATE_FTFTUG",
    "CHG3": "PRICE_TUITFEE_OUT_STATE_FTFTUG",
    "CHG4": "PRICE_BOOK_SUPPLY_FTFTUG",
    "CHG5": "PRICE_RMBD_ON_CAMPUS_FTFTUG",
    "TUITION1": "UG_TUIT_IN_DISTRICT_FULLTIME_AVG",
    "TUITION2": "UG_TUIT_IN_STATE_FULLTIME_AVG",
    "TUITION3": "UG_TUIT_OUT_STATE_FULLTIME_AVG",
    "FEE1": "UG_FEE_IN_DISTRICT_FULLTIME_AVG",
    "FEE2": "UG_FEE_IN_STATE_FULLTIME_AVG",
    "FEE3": "UG_FEE_OUT_STATE_FULLTIME_AVG",
}

RES_STOPWORDS = {"OF", "THE", "FOR", "AND", "IN", "AT", "BY", "WITH", "ON"}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Fill ALL IC_AY concept_keys using strict rules + generic label slugs."
    )
    parser.add_argument("--input", type=Path, default=DEFAULT_INPUT, help="Input crosswalk CSV path")
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
            FILLED_DIR / "ic_ay_crosswalk_autofilled.csv",
            CROSSWALK_DIR / "ic_ay_crosswalk_autofilled.csv",
            DEFAULT_TEMPLATE_FALLBACK,
            PROJECT_ROOT / "Paneled Datasets" / "Crosswalks" / "ic_ay_crosswalk_autofilled.csv",
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
    required = ["concept_key", "survey", "source_var", "label", "table", "year_start", "year_end", "notes"]
    for col in required:
        if col not in df.columns:
            df[col] = ""
    df["concept_key_original"] = df["concept_key"].astype(str)
    df["concept_key_source"] = ""
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
    if var == "CHG1" and "tuition" in label and "fee" in label and "district" in label:
        return STRICT_CONCEPTS[var]
    if var == "CHG2" and "tuition" in label and "fee" in label and (
        "in-state" in label or "in state" in label or ("resident" in label and "nonresident" not in label)
    ):
        return STRICT_CONCEPTS[var]
    if var == "CHG3" and "tuition" in label and "fee" in label and (
        "out-of-state" in label or "out of state" in label or "nonresident" in label
    ):
        return STRICT_CONCEPTS[var]
    if var == "CHG4" and "books" in label and "suppl" in label:
        return STRICT_CONCEPTS[var]
    if var == "CHG5" and "room" in label and "board" in label and ("on campus" in label or "on-campus" in label):
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
    return [tok for tok in tokens if tok not in RES_STOPWORDS]


def slugify_label_to_concept(row: pd.Series, used_keys: set[str]) -> str:
    var = _clean(row.get("source_var", "")).upper()
    survey = _clean(row.get("survey", "")).upper()
    label_raw = _clean(row.get("label", ""))
    label = label_raw.lower()

    charge_type = ""
    if "tuition and required fees" in label:
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


def fill_concept_keys(df: pd.DataFrame) -> Tuple[pd.DataFrame, dict[str, int]]:
    df = df.copy()
    concept = df["concept_key"].astype(str).str.strip()
    existing_mask = concept != ""
    df.loc[existing_mask, "concept_key_source"] = "existing"
    used_keys = set(concept[existing_mask])

    empty_mask = ~existing_mask
    strict_results = df.loc[empty_mask].apply(suggest_concept_key_strict, axis=1)
    strict_idx = strict_results.index[strict_results.notna()]
    df.loc[strict_idx, "concept_key"] = strict_results.loc[strict_idx]
    df.loc[strict_idx, "concept_key_source"] = "strict_rule"
    used_keys.update(df.loc[strict_idx, "concept_key"].tolist())

    remaining_mask = df["concept_key"].astype(str).str.strip() == ""
    generic_idx = df.index[remaining_mask]
    for idx in generic_idx:
        df.at[idx, "concept_key"] = slugify_label_to_concept(df.loc[idx], used_keys)
        df.at[idx, "concept_key_source"] = "generic_slug"

    assert (df["concept_key"].astype(str).str.strip() == "").sum() == 0
    stats = {
        "n_total": len(df),
        "n_existing": int((df["concept_key_source"] == "existing").sum()),
        "n_strict": int((df["concept_key_source"] == "strict_rule").sum()),
        "n_generic": int((df["concept_key_source"] == "generic_slug").sum()),
    }
    return df, stats


def write_outputs(df: pd.DataFrame, stats: dict[str, int], output_dir: Path, output_name: str, overwrite: bool) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)
    output_path = output_dir / output_name
    if output_path.exists() and not overwrite:
        raise SystemExit(f"Refusing to overwrite {output_path}. Use --overwrite to override.")
    df.to_csv(output_path, index=False)

    summary_df = pd.DataFrame(
        [{"metric": key, "value": value} for key, value in stats.items()]
    )
    summary_df.to_csv(output_dir / SUMMARY_NAME, index=False)


def print_summary(df: pd.DataFrame, stats: dict[str, int]) -> None:
    print(f"Total rows: {stats['n_total']}")
    print(f"Existing concept_key kept: {stats['n_existing']}")
    print(f"Filled by strict rules: {stats['n_strict']}")
    print(f"Filled by generic slug: {stats['n_generic']}")

    print("\nConcept_key frequencies (top 40):")
    print(df["concept_key"].value_counts().head(40))

    mask_generic = df["concept_key_source"] == "generic_slug"
    print("\nSource_var using generic_slug (top 20):")
    print(df.loc[mask_generic, "source_var"].value_counts().head(20))


def main() -> None:
    args = parse_args()
    input_path = resolve_input(args.input)
    df = load_crosswalk(input_path)
    df_filled, stats = fill_concept_keys(df)
    print_summary(df_filled, stats)
    write_outputs(df_filled, stats, args.output_dir, args.output_name, args.overwrite)
    print(f"\nSaved fully filled crosswalk to {args.output_dir / args.output_name}")


if __name__ == "__main__":
    main()
