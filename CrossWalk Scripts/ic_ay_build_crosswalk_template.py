"""Build a crosswalk template for IC_AY student charge variables."""
from __future__ import annotations

import argparse
from pathlib import Path
from typing import Iterable, Optional

import pandas as pd

DATA_ROOT = Path("/Users/markjaysonfarol13/Higher Ed research/IPEDS")
DEFAULT_DICT_LAKE_PATH = DATA_ROOT / "Parquets" / "Dictionary" / "dictionary_lake.parquet"
DEFAULT_CROSSWALK_DIR = DATA_ROOT / "Paneled Datasets" / "Crosswalks"
DEFAULT_TEMPLATE_PATH = DEFAULT_CROSSWALK_DIR / "ic_ay_crosswalk_template.csv"

CHARGE_KEYWORDS = (
    "tuition",
    "required fee",
    "fees",
    "books",
    "supplies",
    "room and board",
    "food and housing",
    "other expenses",
    "price of attendance",
    "total price",
)

PROGRAM_CONTEXT_TOKENS = [
    "largest program",
    "2nd largest program",
    "second largest program",
    "3rd largest program",
    "third largest program",
    "4th largest program",
    "fourth largest program",
    "5th largest program",
    "fifth largest program",
    "6th largest program",
    "sixth largest program",
    "total length of the program",
    "total length of program",
    "entire program",
    "contact hours",
    "clock hours",
    "credit hours",
]

CIP_CONTEXT_TOKENS = [
    "cip code",
    "cip code of",
]

LABEL_CANDIDATES = ["label", "var_label", "varlab", "varname_label"]
SURVEY_HINT_COLS = ["survey_group", "survey_hint", "survey_component", "component"]
FILENAME_HINT_COLS = ["dict_filename", "data_filename", "filename", "dict_file"]

# Explicit variables to force-include if present in the dictionary (AY + PY, including X-prefixed flags).
EXPLICIT_VARS = {
    var.strip().upper()
    for var in """
XCIPTUI1 CIPTUIT1 XCIPSUP1 CIPSUPP1 XCIPLGT1 CIPLGTH1 PRGMSR1 XMTHCMP1 MTHCMP1 XWKCMP1 WKCMP1 XLNAYHR1 LNAYHR1 XLNAYWK1 LNAYWK1
XCHG1PY0 CHG1PY0 XCHG1PY1 CHG1PY1 XCHG1PY2 CHG1PY2 XCHG1PY3 CHG1PY3
XCHG4PY0 CHG4PY0 XCHG4PY1 CHG4PY1 XCHG4PY2 CHG4PY2 XCHG4PY3 CHG4PY3
XCHG5PY0 CHG5PY0 XCHG5PY1 CHG5PY1 XCHG5PY2 CHG5PY2 XCHG5PY3 CHG5PY3
XCHG6PY0 CHG6PY0 XCHG6PY1 CHG6PY1 XCHG6PY2 CHG6PY2 XCHG6PY3 CHG6PY3
XCHG7PY0 CHG7PY0 XCHG7PY1 CHG7PY1 XCHG7PY2 CHG7PY2 XCHG7PY3 CHG7PY3
XCHG8PY0 CHG8PY0 XCHG8PY1 CHG8PY1 XCHG8PY2 CHG8PY2 XCHG8PY3 CHG8PY3
XCHG9PY0 CHG9PY0 XCHG9PY1 CHG9PY1 XCHG9PY2 CHG9PY2 XCHG9PY3 CHG9PY3
CIPCODE2 XCIPTUI2 CIPTUIT2 XCIPSUP2 CIPSUPP2 XCIPLGT2 CIPLGTH2 PRGMSR2 XMTHCMP2 MTHCMP2
CIPCODE3 XCIPTUI3 CIPTUIT3 XCIPSUP3 CIPSUPP3 XCIPLGT3 CIPLGTH3 PRGMSR3 XMTHCMP3 MTHCMP3
CIPCODE4 XCIPTUI4 CIPTUIT4 XCIPSUP4 CIPSUPP4 XCIPLGT4 CIPLGTH4 PRGMSR4 XMTHCMP4 MTHCMP4
CIPCODE5 XCIPTUI5 CIPTUIT5 XCIPSUP5 CIPSUPP5 XCIPLGT5 CIPLGTH5 PRGMSR5 XMTHCMP5 MTHCMP5
CIPCODE6 XCIPTUI6 CIPTUIT6 XCIPSUP6 CIPSUPP6 XCIPLGT6 CIPLGTH6 PRGMSR6 XMTHCMP6 MTHCMP6
XTUIT1 TUITION1 XFEE1 FEE1 XHRCHG1 HRCHG1 XTUIT2 TUITION2 XFEE2 FEE2 XHRCHG2 HRCHG2 XTUIT3 TUITION3 XFEE3 FEE3 XHRCHG3 HRCHG3
XTUIT5 TUITION5 XFEE5 FEE5 XHRCHG5 HRCHG5 XTUIT6 TUITION6 XFEE6 FEE6 XHRCHG6 HRCHG6 XTUIT7 TUITION7 XFEE7 FEE7 XHRCHG7 HRCHG7
XISPRO1 ISPROF1 XISPFE1 ISPFEE1 XOSPRO1 OSPROF1 XOSPFE1 OSPFEE1
XISPRO2 ISPROF2 XISPFE2 ISPFEE2 XOSPRO2 OSPROF2 XOSPFE2 OSPFEE2
XISPRO3 ISPROF3 XISPFE3 ISPFEE3 XOSPRO3 OSPROF3 XOSPFE3 OSPFEE3
XISPRO4 ISPROF4 XISPFE4 ISPFEE4 XOSPRO4 OSPROF4 XOSPFE4 OSPFEE4
XISPRO5 ISPROF5 XISPFE5 ISPFEE5 XOSPRO5 OSPROF5 XOSPFE5 OSPFEE5
XISPRO6 ISPROF6 XISPFE6 ISPFEE6 XOSPRO6 OSPROF6 XOSPFE6 OSPFEE6
XISPRO7 ISPROF7 XISPFE7 ISPFEE7 XOSPRO7 OSPROF7 XOSPFE7 OSPFEE7
XISPRO8 ISPROF8 XISPFE8 ISPFEE8 XOSPRO8 OSPROF8 XOSPFE8 OSPFEE8
XISPRO9 ISPROF9 XISPFE9 ISPFEE9 XOSPRO9 OSPROF9 XOSPFE9 OSPFEE9
XCHG1AT0 CHG1AT0 XCHG1AF0 CHG1AF0 XCHG1AY0 CHG1AY0 XCHG1AT1 CHG1AT1 XCHG1AF1 CHG1AF1 XCHG1AY1 CHG1AY1 XCHG1AT2 CHG1AT2 XCHG1AF2 CHG1AF2 XCHG1AY2 CHG1AY2 XCHG1AT3 CHG1AT3 XCHG1AF3 CHG1AF3 XCHG1AY3 CHG1AY3 CHG1TGTD CHG1FGTD
XCHG2AT0 CHG2AT0 XCHG2AF0 CHG2AF0 XCHG2AY0 CHG2AY0 XCHG2AT1 CHG2AT1 XCHG2AF1 CHG2AF1 XCHG2AY1 CHG2AY1 XCHG2AT2 CHG2AT2 XCHG2AF2 CHG2AF2 XCHG2AY2 CHG2AY2 XCHG2AT3 CHG2AT3 XCHG2AF3 CHG2AF3 XCHG2AY3 CHG2AY3 CHG2TGTD CHG2FGTD
XCHG3AT0 CHG3AT0 XCHG3AF0 CHG3AF0 XCHG3AY0 CHG3AY0 XCHG3AT1 CHG3AT1 XCHG3AF1 CHG3AF1 XCHG3AY1 CHG3AY1 XCHG3AT2 CHG3AT2 XCHG3AF2 CHG3AF2 XCHG3AY2 CHG3AY2 XCHG3AT3 CHG3AT3 XCHG3AF3 CHG3AF3 XCHG3AY3 CHG3AY3 CHG3TGTD CHG3FGTD
XCHG4AY0 CHG4AY0 XCHG4AY1 CHG4AY1 XCHG4AY2 CHG4AY2 XCHG4AY3 CHG4AY3
XCHG5AY0 CHG5AY0 XCHG5AY1 CHG5AY1 XCHG5AY2 CHG5AY2 XCHG5AY3 CHG5AY3
XCHG6AY0 CHG6AY0 XCHG6AY1 CHG6AY1 XCHG6AY2 CHG6AY2 XCHG6AY3 CHG6AY3
XCHG7AY0 CHG7AY0 XCHG7AY1 CHG7AY1 XCHG7AY2 CHG7AY2 XCHG7AY3 CHG7AY3
XCHG8AY0 CHG8AY0 XCHG8AY1 CHG8AY1 XCHG8AY2 CHG8AY2 XCHG8AY3 CHG8AY3
XCHG9AY0 CHG9AY0 XCHG9AY1 CHG9AY1 XCHG9AY2 CHG9AY2 XCHG9AY3 CHG9AY3
"""
    if var.strip()
}
# Alias for readability in downstream logic.
TARGET_VARS = EXPLICIT_VARS


def _resolve_column(df: pd.DataFrame, candidates: Iterable[str], *, required: bool = True) -> Optional[str]:
    for candidate in candidates:
        if candidate in df.columns:
            return candidate
    if required:
        raise KeyError(f"Required column not found among: {', '.join(candidates)}")
    return None


def _first_non_empty(series: pd.Series) -> str:
    for val in series:
        if pd.notna(val):
            text = str(val).strip()
            if text:
                return val
    return ""


def _ic_cost_mask(df: pd.DataFrame, survey_col: str) -> pd.Series:
    survey = df[survey_col].astype(str).str.upper()
    # Any survey that looks like an IC variant (IC, ICyyyy, ICAY, ICPY, INSTITUTIONAL CHARACTERISTICS, etc.).
    survey_ic = survey.str.contains(r"\bIC\b", na=False) | survey.str.contains("INSTITUTIONAL CHARACTERISTICS", na=False)
    # Also catch forms like IC2020, IC2019, etc.
    survey_ic |= survey.str.match(r"^IC\d{4}$", na=False)

    file_mask = pd.Series(False, index=df.index)
    for col in FILENAME_HINT_COLS:
        if col in df.columns:
            values = df[col].astype(str).str.lower()
            ic_hit = values.str.contains("ic", na=False)

            file_mask |= ic_hit & (
                values.str.contains("icay", na=False)
                | values.str.contains("icpy", na=False)
                | values.str.contains("ic20", na=False)  # e.g., ic2020, ic2019, etc.
                | values.str.contains("institutional characteristics", na=False)
                | values.str.contains("student charges", na=False)
            )

            # As a fallback, allow any filename with "ic" that also hits CHARGE_KEYWORDS later.
            file_mask |= ic_hit

    table_mask = pd.Series(False, index=df.index)
    if "table_name" in df.columns:
        table_values = df["table_name"].astype(str).str.lower()
        table_mask |= table_values.str.contains("icay", na=False)
        table_mask |= table_values.str.contains("icpy", na=False)
        table_mask |= table_values.str.contains("student charges", na=False)
        table_mask |= table_values.str.contains("institutional characteristics", na=False)
        table_mask |= table_values.str.contains(r"\bic\d{4}\b", na=False)

    hint_mask = pd.Series(False, index=df.index)
    for col in SURVEY_HINT_COLS:
        if col in df.columns:
            hints = df[col].astype(str).str.lower()
            hint_mask |= hints.str.contains("ic_ay", na=False)
            hint_mask |= hints.str.contains("ic_py", na=False)
            hint_mask |= hints.str.contains("student charges", na=False)
            hint_mask |= hints.str.contains("academic year", na=False)
            hint_mask |= hints.str.contains("program year", na=False)
            hint_mask |= hints.str.contains("institutional characteristics", na=False)
            hint_mask |= hints.str.contains(r"\bic\b", na=False)

    var_upper = df["source_var"].astype(str).str.upper()
    var_mask = var_upper.str.match(r"^CHG\\d+$", na=False)
    var_mask |= var_upper.str.match(r"^PCCHG\\d+AY\\d*", na=False)
    var_mask |= var_upper.str.match(r"^PCCHG\\d+PY\\d*", na=False)
    var_mask |= var_upper.str.match(r"^CMP\\d+AY\\d*", na=False)
    var_mask |= var_upper.str.match(r"^CMP\\d+PY\\d*", na=False)
    var_mask |= var_upper.str.match(r"^TUITION\\d+", na=False)
    var_mask |= var_upper.str.match(r"^FEE\\d+", na=False)
    # X-prefixed edit/provisional versions
    var_mask |= var_upper.str.match(r"^XCHG\\d+AY\\d*", na=False)
    var_mask |= var_upper.str.match(r"^XCHG\\d+PY\\d*", na=False)
    var_mask |= var_upper.str.match(r"^XTUIT\\d+", na=False)
    var_mask |= var_upper.str.match(r"^XFEE\\d+", na=False)
    var_mask |= var_upper.str.match(r"^XHRCHG\\d+", na=False)
    # Program-year CIP/program cost fields
    var_mask |= var_upper.str.match(r"^CIPTUI[T]?[0-9]+", na=False)
    var_mask |= var_upper.str.match(r"^XCIPTUI[0-9]+", na=False)
    var_mask |= var_upper.str.match(r"^CIPSUP[P]?[0-9]+", na=False)
    var_mask |= var_upper.str.match(r"^XCIPSUP[0-9]+", na=False)
    var_mask |= var_upper.str.match(r"^CIPLGTH[0-9]+", na=False)
    var_mask |= var_upper.str.match(r"^XCIPLGT[0-9]+", na=False)
    var_mask |= var_upper.str.match(r"^PRGMSR[0-9]+", na=False)
    var_mask |= var_upper.str.match(r"^XMTHCMP[0-9]+", na=False)
    var_mask |= var_upper.str.match(r"^MTHCMP[0-9]+", na=False)
    var_mask |= var_upper.str.match(r"^LNAYHR[0-9]+", na=False)
    var_mask |= var_upper.str.match(r"^XLNAYHR[0-9]+", na=False)
    var_mask |= var_upper.str.match(r"^LNAYWK[0-9]+", na=False)
    var_mask |= var_upper.str.match(r"^XLNAYWK[0-9]+", na=False)
    var_mask |= var_upper.str.match(r"^CIPCODE[0-9]+", na=False)
    # Room/board etc.
    var_mask |= var_upper.isin(["TUITVARY", "BOARDAMT", "RMBRDAMT"])
    # Force-include explicit targets
    var_mask |= var_upper.isin(TARGET_VARS)

    combined = (survey_ic & (file_mask | table_mask | hint_mask)) | var_mask
    return combined


def build_crosswalk_template(dict_lake: Path) -> pd.DataFrame:
    """Create the IC_AY crosswalk template from the dictionary lake."""
    if not dict_lake.exists():
        raise FileNotFoundError(f"Dictionary lake not found: {dict_lake}")

    df = pd.read_parquet(dict_lake)
    df.columns = [c.lower() for c in df.columns]

    survey_col = _resolve_column(df, ["survey"])
    year_col = _resolve_column(df, ["year", "collection_year"])
    source_var_col = _resolve_column(df, ["source_var", "varname", "variable"])
    table_col = _resolve_column(df, ["table", "table_name", "tableid"], required=False)
    label_col = _resolve_column(df, LABEL_CANDIDATES, required=False)

    df["year"] = pd.to_numeric(df[year_col], errors="coerce")
    if df["year"].isna().any():
        raise ValueError("Dictionary lake contains non-numeric years for IC_AY rows.")

    df["survey"] = df[survey_col].astype(str).str.upper()
    df["source_var"] = df[source_var_col].astype(str).str.upper()
    if table_col:
        df["table"] = df[table_col]
    else:
        df["table"] = ""
    if label_col:
        df["label"] = df[label_col].astype(str)
    else:
        df["label"] = ""

    ic_ay_mask = _ic_cost_mask(df, "survey")
    label_lower = df["label"].str.lower()

    def contains_any(text: str, tokens: list[str]) -> bool:
        text = text or ""
        return any(tok in text for tok in tokens)

    program_context_mask = label_lower.apply(lambda s: contains_any(s, PROGRAM_CONTEXT_TOKENS))
    cip_context_mask = label_lower.apply(lambda s: contains_any(s, CIP_CONTEXT_TOKENS))
    must_include_mask = df["source_var"].astype(str).str.upper().isin(TARGET_VARS)
    exclude_mask = (program_context_mask | cip_context_mask) & ~must_include_mask

    charge_mask = pd.Series(False, index=df.index)
    if label_lower.notna().any():
        for keyword in CHARGE_KEYWORDS:
            charge_mask |= label_lower.str.contains(keyword, na=False)
    # Always keep canonical CHG* variables even if the label is missing.
    charge_mask |= df["source_var"].str.match(r"^CHG\d+$", na=False)
    # Always keep explicit targets regardless of label keywords.
    charge_mask |= must_include_mask

    df_ic_ay = df[ic_ay_mask & ~exclude_mask & charge_mask].copy()
    if df_ic_ay.empty:
        raise ValueError("IC_AY filter produced zero rows. Check dictionary lake contents.")

    grouped = (
        df_ic_ay.groupby(["survey", "source_var"], as_index=False)
        .agg(
            year_start=("year", "min"),
            year_end=("year", "max"),
            table=("table", _first_non_empty),
            label=("label", _first_non_empty),
        )
        .sort_values(["source_var", "survey", "year_start"], ignore_index=True)
    )

    grouped.insert(0, "concept_key", "")
    grouped["notes"] = ""
    column_order = ["concept_key", "survey", "source_var", "year_start", "year_end", "table", "label", "notes"]
    grouped = grouped[column_order]
    return grouped


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--dict-lake",
        type=Path,
        default=DEFAULT_DICT_LAKE_PATH,
        help="Path to dictionary_lake.parquet.",
    )
    parser.add_argument(
        "--out",
        type=Path,
        default=DEFAULT_TEMPLATE_PATH,
        help="Output CSV path for the IC_AY crosswalk template.",
    )
    args = parser.parse_args()

    template = build_crosswalk_template(args.dict_lake)
    args.out.parent.mkdir(parents=True, exist_ok=True)
    template.to_csv(args.out, index=False)
    unique_vars = template["source_var"].nunique()
    print(f"Wrote {len(template):,} rows covering {unique_vars:,} IC_AY source variables to {args.out}")
    print("Sample rows:")
    print(template.head(10).to_string(index=False))


if __name__ == "__main__":
    main()
