#!/usr/bin/env python3
"""
Auto-fill concept_key for finance_crosswalk_template.csv based on source_label_norm.

This is a first-pass mapping into the conceptual schema:
- BS_*  balance sheet
- IS_*  income statement
- REV_* revenues
- EXP_* expenses
- FIN_DISCOUNTS_* tuition/fee discounts

You MUST review the output (finance_crosswalk_filled.csv) before using it in production.
"""

from pathlib import Path
import re

import pandas as pd


def _normalize_form_family(fam: str | None) -> str:
    """
    Normalize finance form_family codes (F1, F1A, F1A_F, F1COMP, etc.) to a common root.
    """
    if not isinstance(fam, str):
        return ""
    fam_norm = fam.strip().upper()
    if fam_norm.startswith("F1"):
        return "F1"
    if fam_norm.startswith("F2"):
        return "F2"
    if fam_norm.startswith("F3"):
        return "F3"
    return fam_norm


# Assumes you run this from the repo root where finance_crosswalk_template.csv lives.
CROSSWALK_IN = Path("/Users/markjaysonfarol13/Higher Ed research/IPEDS/Paneled Datasets/Crosswalks/finance_crosswalk_template.csv")
CROSSWALK_OUT = Path("/Users/markjaysonfarol13/Higher Ed research/IPEDS/Paneled Datasets/Crosswalks/Filled/finance_crosswalk_filled.csv")
OVERRIDES_PATH = Path("/Users/markjaysonfarol13/Higher Ed research/IPEDS/Paneled Datasets/Crosswalks/finance_crosswalk_overrides.csv")
DICTIONARY_LAKE = Path("/Users/markjaysonfarol13/Higher Ed research/IPEDS/Parquets/Dictionary/dictionary_lake.parquet")
STEP0_SAMPLE = Path("/Users/markjaysonfarol13/Higher Ed research/IPEDS/Paneled Datasets/Step0/finance_step0_long_2004.parquet")

# Only treat the component-core F1/F2/F3 sections as required rows (F3 has no H component).
CORE_SECTION_PATTERN = re.compile(r"^(?:F1[BCDEH]|F2[BCDEH]|F3[BCDE])", re.IGNORECASE)

SOURCE_VAR_CONCEPT_OVERRIDES = {
    # --- Revenues (GASB F1B) ---
    "F1B01": "REV_TUITION_NET",
    "F1B02": "REV_GRANTS_FED",
    "F1B03": "REV_GRANTS_STATE",
    "F1B04": "REV_GRANTS_LOCAL_PRIV",
    "F1B05": "REV_AUXILIARY",
    "F1B06": "REV_HOSPITAL",
    "F1B07": "REV_INDEPENDENT_OPS",
    "F1B08": "REV_OTHER_OPERATING",
    "F1B10": "REV_FED_APPROPS",
    "F1B11": "REV_STATE_APPROPS",
    "F1B12": "REV_LOCAL_APPROPS",
    "F1B13": "REV_GRANTS_FED",
    "F1B14": "REV_GRANTS_STATE",
    "F1B15": "REV_GRANTS_LOCAL_PRIV",
    "F1B16": "REV_PRIVATE_GIFTS_GRANTS",
    "F1B17": "REV_INVESTMENT_RETURN",
    "F1B18": "REV_OTHER_NONOPERATING",
    "F1B20": "REV_CAPITAL_APPROPS",
    "F1B21": "REV_CAPITAL_GRANTS_GIFTS",
    "F1B22": "REV_ADD_PERM_ENDOW",
    "F1B23": "REV_OTHER_ADDITIONS",
    "F1B27": "REV_TOTAL_REVENUES",
    # --- Revenues (FASB F2D / F2B) ---
    "F2D01": "REV_TUITION_NET",
    "F2D02": "REV_FED_APPROPS",
    "F2D03": "REV_STATE_APPROPS",
    "F2D04": "REV_LOCAL_APPROPS",
    "F2D05": "REV_GRANTS_FED",
    "F2D06": "REV_GRANTS_STATE",
    "F2D07": "REV_GRANTS_LOCAL_PRIV",
    "F2D08": "REV_PRIVATE_GIFTS_GRANTS",
    "F2D09": "REV_INVESTMENT_RETURN",
    "F2D11": "REV_AUXILIARY",
    "F2D13": "REV_HOSPITAL",
    "F2D14": "REV_INDEPENDENT_OPS",
    "F2D15": "REV_OTHER_OPERATING",
    "F2D16": "REV_TOTAL_REVENUES",
    "F2B01": "REV_TOTAL_REVENUES",
    # --- Revenues (For-profit F3D / F3B) ---
    "F3D01": "REV_TUITION_NET",
    "F3D02": "REV_GRANTS_FED",
    "F3D02A": "REV_FED_APPROPS",
    "F3D02B": "REV_GRANTS_FED",
    "F3D03": "REV_GRANTS_STATE",
    "F3D03A": "REV_STATE_APPROPS",
    "F3D03B": "REV_GRANTS_STATE",
    "F3D03C": "REV_LOCAL_APPROPS",
    "F3D04": "REV_PRIVATE_GIFTS_GRANTS",
    "F3D05": "REV_INVESTMENT_RETURN",
    "F3D07": "REV_AUXILIARY",
    "F3D08": "REV_HOSPITAL",
    "F3D09": "REV_INDEPENDENT_OPS",
    "F3D10": "REV_OTHER_OPERATING",
    "F3D16": "REV_TOTAL_REVENUES",
    "F3B01": "REV_TOTAL_REVENUES",
    # --- Expenses (GASB F1C) ---
    "F1C011": "EXP_INSTRUCTION",
    "F1C021": "EXP_RESEARCH",
    "F1C031": "EXP_PUBLIC_SERVICE",
    "F1C051": "EXP_ACADEMIC_SUPPORT",
    "F1C061": "EXP_STUDENT_SERVICES",
    "F1C071": "EXP_INSTITUTIONAL_SUPPORT",
    "F1C081": "EXP_OPERATIONS_PLANT",
    "F1C101": "EXP_SCHOLARSHIPS_NET",
    "F1C111": "EXP_AUXILIARY",
    "F1C121": "EXP_HOSPITAL",
    "F1C131": "EXP_INDEPENDENT_OPS",
    "F1C141": "EXP_OTHER_FUNCTIONAL",
    "F1C191": "IS_EXPENSES_TOTAL",
    # --- Expenses (FASB F2E / F2B) ---
    "F2E011": "EXP_INSTRUCTION",
    "F2E021": "EXP_RESEARCH",
    "F2E031": "EXP_PUBLIC_SERVICE",
    "F2E041": "EXP_ACADEMIC_SUPPORT",
    "F2E051": "EXP_STUDENT_SERVICES",
    "F2E061": "EXP_INSTITUTIONAL_SUPPORT",
    "F2E081": "EXP_SCHOLARSHIPS_NET",
    "F2E111": "EXP_OPERATIONS_PLANT",
    "F2E071": "EXP_AUXILIARY",
    "F2E091": "EXP_HOSPITAL",
    "F2E101": "EXP_INDEPENDENT_OPS",
    "F2E121": "EXP_OTHER_FUNCTIONAL",
    "F2B02": "IS_EXPENSES_TOTAL",
    # --- Expenses (For-profit F3E) ---
    "F3E011": "EXP_INSTRUCTION",
    "F3E02A1": "EXP_RESEARCH",
    "F3E02B1": "EXP_PUBLIC_SERVICE",
    "F3E03A1": "EXP_ACADEMIC_SUPPORT",
    "F3E03B1": "EXP_STUDENT_SERVICES",
    "F3E03C1": "EXP_INSTITUTIONAL_SUPPORT",
    "F3E041": "EXP_AUXILIARY",
    "F3E061": "EXP_OTHER_FUNCTIONAL",
    "F3E101": "EXP_HOSPITAL",
    # TODO: identify explicit F3E code for operation & maintenance of plant totals / independent operations.
    "F3E051": "EXP_SCHOLARSHIPS_NET",
    "F3E071": "IS_EXPENSES_TOTAL",
    # --- Scholarships / Discounts (GASB F1E) ---
    "F1E01": "FIN_SCHOLARSHIPS_PELL",
    "F1E02": "FIN_SCHOLARSHIPS_OTHER_FED",
    "F1E03": "FIN_SCHOLARSHIPS_STATE",
    "F1E04": "FIN_SCHOLARSHIPS_LOCAL",
    "F1E05": "FIN_SCHOLARSHIPS_INSTITUTIONAL_RESTRICTED",
    "F1E06": "FIN_SCHOLARSHIPS_INSTITUTIONAL_UNRESTRICTED",
    "F1E07": "FIN_SCHOLARSHIPS_TOTAL_GROSS",
    "F1E08": "FIN_DISCOUNTS_TUITION",
    "F1E09": "FIN_DISCOUNTS_AUXILIARY",
    "F1E10": "FIN_DISCOUNTS_TOTAL",
    "F1E11": "FIN_SCHOLARSHIPS_NET",
    # --- Scholarships & discounts by source (GASB F1E, Pell/federal/state/local/endowment/institutional) ---
    "F1E12": "FIN_PELLGROSS_1_0",
    "F1E121": "FIN_PELLTUIT_1_0",
    "F1E122": "FIN_PELLAUX_1_0",
    "F1E13": "FIN_OTHFEDSCH_1_0",
    "F1E131": "FIN_OTHFEDTUIT_1_0",
    "F1E132": "FIN_OTHFEDAUX_1_0",
    "F1E14": "FIN_STGRSCH_1_0",
    "F1E141": "FIN_STGRTUIT_1_0",
    "F1E142": "FIN_STGRAUX_1_0",
    "F1E15": "FIN_LCGRSCH_1_0",
    "F1E151": "FIN_LCGRTUIT_1_0",
    "F1E152": "FIN_LCGRAUX_1_0",
    "F1E16": "FIN_ENDOW1_1_0",
    "F1E161": "FIN_ENDOWTUIT_1_0",
    "F1E162": "FIN_ENDOWAUX_1_0",
    "F1E17": "FIN_INGRRESSCH_1_0",
    "F1E171": "FIN_INGRTUIT_1_0",
    "F1E172": "FIN_INGRAUX_1_0",
    # --- Endowment (GASB F1H) ---
    "F1H01": "FIN_ENDOW_ASSETS_BEGIN",
    "F1H02": "FIN_ENDOW_ASSETS_END",
    "F1H03": "FIN_ENDOW_NET_CHANGE",
# --- Scholarships / Discounts (FASB F2C) ---
    "F2C01": "FIN_SCHOLARSHIPS_PELL",
    "F2C02": "FIN_SCHOLARSHIPS_OTHER_FED",
    "F2C03": "FIN_SCHOLARSHIPS_STATE",
    "F2C04": "FIN_SCHOLARSHIPS_LOCAL",
    "F2C05": "FIN_SCHOLARSHIPS_INSTITUTIONAL_RESTRICTED",
    "F2C06": "FIN_SCHOLARSHIPS_INSTITUTIONAL_UNRESTRICTED",
    "F2C07": "FIN_SCHOLARSHIPS_TOTAL_GROSS",
    "F2C08": "FIN_DISCOUNTS_TUITION",
    "F2C09": "FIN_DISCOUNTS_AUXILIARY",
    "F2C10": "FIN_DISCOUNTS_TOTAL",
    "F2C11": "FIN_SCHOLARSHIPS_NET",
    "F2C12": "FIN_PELLGROSS_1_0",
    "F2C121": "FIN_PELLTUIT_1_0",
    "F2C122": "FIN_PELLAUX_1_0",
    "F2C13": "FIN_OTHFEDSCH_1_0",
    "F2C131": "FIN_OTHFEDTUIT_1_0",
    "F2C132": "FIN_OTHFEDAUX_1_0",
    "F2C14": "FIN_STGRSCH_1_0",
    "F2C141": "FIN_STGRTUIT_1_0",
    "F2C142": "FIN_STGRAUX_1_0",
    "F2C15": "FIN_LCGRSCH_1_0",
    "F2C151": "FIN_LCGRTUIT_1_0",
    "F2C152": "FIN_LCGRAUX_1_0",
    "F2C16": "FIN_ENDOW1_1_0",
    "F2C161": "FIN_ENDOWTUIT_1_0",
    "F2C162": "FIN_ENDOWAUX_1_0",
    "F2C17": "FIN_INGRRESSCH_1_0",
    "F2C171": "FIN_INGRTUIT_1_0",
    "F2C172": "FIN_INGRAUX_1_0",
# --- Scholarships / Discounts (For-profit F3C) ---
    "F3C01": "FIN_SCHOLARSHIPS_PELL",
    "F3C02": "FIN_SCHOLARSHIPS_OTHER_FED",
    "F3C03": "FIN_SCHOLARSHIPS_STATE",
    "F3C03A": "FIN_SCHOLARSHIPS_STATE",
    "F3C03B": "FIN_SCHOLARSHIPS_LOCAL",
    "F3C04": "FIN_SCHOLARSHIPS_INSTITUTIONAL_UNRESTRICTED",
    "F3C05": "FIN_SCHOLARSHIPS_INSTITUTIONAL_RESTRICTED",
    "F3C06": "FIN_DISCOUNTS_TUITION",
    "F3C07": "FIN_DISCOUNTS_AUXILIARY",
    "F3C08": "FIN_DISCOUNTS_TOTAL",
    "F3C12": "FIN_PELLGROSS_1_0",
    "F3C121": "FIN_PELLTUIT_1_0",
    "F3C122": "FIN_PELLAUX_1_0",
    "F3C13": "FIN_OTHFEDSCH_1_0",
    "F3C131": "FIN_OTHFEDTUIT_1_0",
    "F3C132": "FIN_OTHFEDAUX_1_0",
    "F3C14": "FIN_STGRSCH_1_0",
    "F3C141": "FIN_STGRTUIT_1_0",
    "F3C142": "FIN_STGRAUX_1_0",
    "F3C15": "FIN_LCGRSCH_1_0",
    "F3C151": "FIN_LCGRTUIT_1_0",
    "F3C152": "FIN_LCGRAUX_1_0",
    "F3C16": "FIN_ENDOW1_1_0",
    "F3C161": "FIN_ENDOWTUIT_1_0",
    "F3C162": "FIN_ENDOWAUX_1_0",
    "F3C17": "FIN_INGRRESSCH_1_0",
    "F3C171": "FIN_INGRTUIT_1_0",
    "F3C172": "FIN_INGRAUX_1_0",
    # --- Endowment spending distribution for current use ---
    "F1H03C": "FIN_SPENDDIS",
    "F2H03C": "FIN_SPENDDIS",
    "F3H03C": "FIN_SPENDDIS",
    # --- Endowment (FASB / For-profit) ---
    "F2H01": "FIN_ENDOW_ASSETS_BEGIN",
    "F2H02": "FIN_ENDOW_ASSETS_END",
    "F2H03": "FIN_ENDOW_NET_CHANGE",
    "F3H01": "FIN_ENDOW_ASSETS_BEGIN",
    "F3H02": "FIN_ENDOW_ASSETS_END",
    "F3H03": "FIN_ENDOW_NET_CHANGE",
}

CONCEPTS = {
    "IS_REVENUES_TOTAL",
    "IS_EXPENSES_TOTAL",
    "BS_ASSETS_INVESTMENTS_TOTAL",
    "BS_ENDOWMENT_FMV",
    "FIN_ENDOW_ASSETS_BEGIN",
    "FIN_ENDOW_ASSETS_END",
    "FIN_ENDOW_NET_CHANGE",
    "REV_TUITION_NET",
    "REV_GOV_APPROPS_TOTAL",
    "REV_FED_APPROPS",
    "REV_STATE_APPROPS",
    "REV_LOCAL_APPROPS",
    "REV_GRANTS_CONTRACTS_TOTAL",
    "REV_GRANTS_FED",
    "REV_GRANTS_STATE",
    "REV_GRANTS_LOCAL_PRIV",
    "REV_PRIVATE_GIFTS",
    "REV_PRIVATE_GIFTS_GRANTS",
    "REV_INVESTMENT_RETURN",
    "REV_AUXILIARY_NET",
    "REV_AUXILIARY",
    "REV_HOSPITAL",
    "REV_INDEPENDENT_OPS",
    "REV_OTHER_OPERATING",
    "REV_OTHER_NONOPERATING",
    "REV_CAPITAL_APPROPS",
    "REV_CAPITAL_GRANTS_GIFTS",
    "REV_ADD_PERM_ENDOW",
    "REV_OTHER_ADDITIONS",
    "REV_TOTAL_REVENUES",
    "EXP_INSTRUCTION",
    "EXP_RESEARCH",
    "EXP_PUBLIC_SERVICE",
    "EXP_ACADEMIC_SUPPORT",
    "EXP_STUDENT_SERVICES",
    "EXP_INSTITUTIONAL_SUPPORT",
    "EXP_AUXILIARY",
    "EXP_HOSPITAL",
    "EXP_INDEPENDENT_OPS",
    "EXP_OTHER_FUNCTIONAL",
    "EXP_OPERATIONS_PLANT",
    "EXP_SCHOLARSHIPS_NET",
    "FIN_SCHOLARSHIPS_PELL",
    "FIN_SCHOLARSHIPS_OTHER_FED",
    "FIN_SCHOLARSHIPS_STATE",
    "FIN_SCHOLARSHIPS_LOCAL",
    "FIN_SCHOLARSHIPS_INSTITUTIONAL_RESTRICTED",
    "FIN_SCHOLARSHIPS_INSTITUTIONAL_UNRESTRICTED",
    "FIN_SCHOLARSHIPS_TOTAL_GROSS",
    "FIN_DISCOUNTS_TUITION",
    "FIN_DISCOUNTS_AUXILIARY",
    "FIN_DISCOUNTS_TOTAL",
    "FIN_DISCOUNTS_OTHER_INST_TOTAL",
    "FIN_SCHOLARSHIPS_NET",
    "FIN_PELLGROSS_1_0",
    "FIN_PELLTUIT_1_0",
    "FIN_PELLAUX_1_0",
    "FIN_OTHFEDSCH_1_0",
    "FIN_OTHFEDTUIT_1_0",
    "FIN_OTHFEDAUX_1_0",
    "FIN_STGRSCH_1_0",
    "FIN_STGRTUIT_1_0",
    "FIN_STGRAUX_1_0",
    "FIN_LCGRSCH_1_0",
    "FIN_LCGRTUIT_1_0",
    "FIN_LCGRAUX_1_0",
    "FIN_ENDOW1_1_0",
    "FIN_ENDOWTUIT_1_0",
    "FIN_ENDOWAUX_1_0",
    "FIN_INGRRESSCH_1_0",
    "FIN_INGRTUIT_1_0",
    "FIN_INGRAUX_1_0",
    "FIN_OTHTUIT_1_0",
    "FIN_OTHAUX_1_0",
    "FIN_SPENDDIS",
}

IGNORED_CONCEPTS = {
    "BS_ASSETS_TOTAL",
    "BS_LIABILITIES_TOTAL",
    "BS_NET_ASSETS_TOTAL",
    "BS_NET_ASSETS_WODR",
    "BS_NET_ASSETS_WDR",
    "BS_ASSETS_CASH",
    "BS_LIAB_DEBT_LONGTERM",
    "BS_ASSETS_CAPITAL_NET",
    "IS_NET_INCOME",
}


def _is_relevant_component(form_family: str | None, section: str | None) -> bool:
    """
    Return True when (form_family, section) belongs to the five finance components we care about.
    """
    fam = (form_family or "").upper()
    sec = (section or "").upper()
    if not fam or not sec:
        return False
    if fam.startswith("F1"):
        return sec in {"B", "C", "D", "E", "H"}
    if fam.startswith("F2"):
        return sec in {"B", "C", "D", "E", "H"}
    if fam.startswith("F3"):
        return sec in {"B", "C", "D", "E"}
    return False


def _assert_no_overlaps(df: pd.DataFrame, group_cols: tuple[str, ...]) -> None:
    for key, grp in df.groupby(list(group_cols), dropna=False):
        grp_sorted = grp.sort_values("year_start")
        prev_end = None
        for _, row in grp_sorted.iterrows():
            start = int(row["year_start"])
            end = int(row["year_end"])
            if prev_end is not None and start <= prev_end:
                print("ERROR: Overlapping year ranges detected for", key)
                print(
                    grp_sorted[
                        ["form_family", "base_key", "concept_key", "year_start", "year_end"]
                    ]
                    .head(10)
                    .to_string(index=False)
                )
                raise SystemExit(1)
            prev_end = max(prev_end or end, end)


def _contains_any(text: str, *terms: str) -> bool:
    return any(term in text for term in terms)


def _match_function_total(text: str, term: str) -> bool:
    """Return True when `text` clearly represents the total for the requested function."""
    if term not in text:
        return False

    if _contains_any(text, "expenses", "expense", "expenditures"):
        return True

    if "total" in text:
        detail_terms = ("salaries", "wages", "benefits", "fringe")
        if any(detail in text for detail in detail_terms):
            return False

        pattern = rf"(?:{term}\W{{0,12}}total|total\W{{0,12}}{term})"
        if re.search(pattern, text):
            return True

    # Fallback for labels that are literally just the function name.
    if text.strip() == term:
        return True

    return False


def _strip_str_cols(df: pd.DataFrame, cols: tuple[str, ...]) -> None:
    for col in cols:
        if col in df.columns:
            df[col] = df[col].apply(lambda v: v.strip() if isinstance(v, str) else v)


def _build_var_type_map(dict_path: Path | str | None = None) -> dict[tuple[str, str, str], bool]:
    """
    Return a map of (form_family, survey, source_var) -> is_amount flag using dictionary_lake metadata.

    We treat cross_sectional/vartable entries as numeric amount rows and tablesdoc/value-code entries as categorical.
    """
    dict_path = Path(dict_path) if dict_path else Path()
    if not dict_path.exists():
        print(f"WARNING: dictionary lake not found at {dict_path}; cannot infer amount types.")
        return {}

    cols_needed = [
        "form_family",
        "source_var",
        "survey",
        "source",
        "sheet_name",
        "table_name",
        "source_label_norm",
        "label_norm",
        "source_label",
        "code_norm",
        "value",
        "data_filename",
    ]
    try:
        df = pd.read_parquet(dict_path, columns=cols_needed)
    except Exception:
        try:
            df = pd.read_parquet(dict_path)
        except Exception as exc:  # pragma: no cover - diagnostics
            print(f"WARNING: failed to load dictionary lake {dict_path}: {exc}")
            return {}

    if df.empty:
        return {}

    available = [c for c in cols_needed if c in df.columns]
    if not available:
        print("WARNING: dictionary lake missing required columns to infer amount types.")
        return {}
    df = df[available].copy()

    needed = {"form_family", "source_var"}
    df = df.dropna(subset=[col for col in needed if col in df.columns])

    def _norm(val: str | None) -> str:
        return val.strip() if isinstance(val, str) else ""

    df["form_family"] = df["form_family"].apply(lambda v: _normalize_form_family(_norm(v)))
    df["source_var"] = df["source_var"].apply(lambda v: _norm(v).upper())
    if "survey" in df.columns:
        df["survey"] = df["survey"].apply(lambda v: _norm(v).upper())
    else:
        df["survey"] = ""
    if "code_norm" in df.columns:
        df["code_norm"] = df["code_norm"].fillna("").astype(str).str.strip()
    else:
        df["code_norm"] = ""

    amount_terms = (
        "revenue",
        "revenues",
        "expense",
        "expenses",
        "tuition",
        "fees",
        "allowance",
        "allowances",
        "scholarship",
        "scholarships",
        "fellowship",
        "grants",
        "contracts",
        "investment",
        "endowment",
        "assets",
        "liabilities",
        "net",
        "total",
        "capital",
        "operations",
        "plant",
        "hospital",
        "auxiliary",
        "independent",
        "gift",
        "contribution",
        "discount",
        "appropriation",
    )
    categorical_terms = (
        "code",
        "flag",
        "indicator",
        "status",
        "category",
        "categories",
        "type",
        "types",
        "level",
        "levels",
        "percent",
        "percentage",
        "ratio",
        "classification",
        "yes/no",
        "yes or no",
        "imputation",
    )
    amount_sources = {"cross_sectional", "panel", "longitudinal"}
    doc_sheet_terms = ("description", "doc", "note")

    def _looks_amount_label(text: str) -> bool:
        return any(term in text for term in amount_terms)

    def _looks_categorical_label(text: str) -> bool:
        return any(term in text for term in categorical_terms)

    def _looks_categorical_var(var: str) -> bool:
        suffixes = ("CD", "C", "CAT", "TYPE", "TYP", "IND", "INDICATOR", "FLAG", "FLG", "STAT")
        return any(var.endswith(suf) for suf in suffixes)

    mapping: dict[tuple[str, str, str], bool] = {}
    for _, row in df.iterrows():
        fam = row["form_family"]
        var = row["source_var"]
        survey = row.get("survey", "")
        if not fam or not var:
            continue

        source_kind = _norm(row.get("source")).lower()
        sheet_name = _norm(row.get("sheet_name")).lower()
        table_name = _norm(row.get("table_name")).lower()
        label_text = (
            _norm(row.get("source_label_norm"))
            or _norm(row.get("label_norm"))
            or _norm(row.get("source_label"))
        ).lower()
        code_norm = _norm(row.get("code_norm"))
        value_text = _norm(row.get("value")).lower()
        data_filename = _norm(row.get("data_filename")).lower()

        value_table_signal = False
        if source_kind.startswith("value"):
            value_table_signal = True
        if sheet_name and any(term in sheet_name for term in ("valuelabel", "value label")):
            value_table_signal = True
        if data_filename and "valuelabel" in data_filename:
            value_table_signal = True
        if value_text:
            value_table_signal = True
        if code_norm and code_norm.upper() != var:
            value_table_signal = True

        doc_signal = source_kind == "tablesdoc"
        if sheet_name and not doc_signal and source_kind not in amount_sources:
            doc_signal = any(term in sheet_name for term in doc_sheet_terms)
        amount_signal = (
            (source_kind in amount_sources)
            or (sheet_name.startswith("vartable") if sheet_name else False)
            or (table_name.startswith("f") if table_name else False)
            or _looks_amount_label(label_text)
            or survey == "FIN"
        )
        if _looks_categorical_label(label_text) or _looks_categorical_var(var):
            doc_signal = True
        if value_table_signal:
            doc_signal = True

        is_amount = amount_signal and not doc_signal

        key = (fam, survey, var)
        if key not in mapping:
            mapping[key] = is_amount
        else:
            mapping[key] = mapping[key] or is_amount

        generic_key = (fam, "", var)
        if generic_key not in mapping:
            mapping[generic_key] = is_amount
        else:
            mapping[generic_key] = mapping[generic_key] or is_amount

    return mapping


def assign_concept(label: str, form_family: str, base_key: str, source_var: str | None = None) -> str | None:
    """Heuristic mapping from source_label_norm to the conceptual schema."""
    source = (source_var or "").strip().upper()
    if source in SOURCE_VAR_CONCEPT_OVERRIDES:
        return SOURCE_VAR_CONCEPT_OVERRIDES[source]

    if not isinstance(label, str):
        return None
    s = " ".join(label.lower().split())

    # ENDOWMENTS / INVESTMENTS
    if (
        ("long-term investments" in s)
        or ("long term investments" in s)
        or ("investments" in s and "noncurrent" in s)
        or ("investments" in s and "assets" in s)
        or ("investments" in s and "fair value" in s)
    ):
        return "BS_ASSETS_INVESTMENTS_TOTAL"
    if (
        s.startswith("value of endowment assets at the end")
        or ("endowment" in s and ("assets" in s or "funds" in s) and ("end of" in s or "at the end" in s))
    ):
        return "BS_ENDOWMENT_FMV"
    if "spending distribution for current use" in s and "endowment" in s:
        return "FIN_SPENDDIS"

    # INCOME STATEMENT TOTALS
    if (
        "total revenues and other additions" in s
        or "total operating revenues" in s
        or (s.startswith("total revenues") and "other" not in s)
    ):
        return "IS_REVENUES_TOTAL"
    if "total expenses" in s or "total operating expenses" in s:
        detail_disqualifiers = (
            "salaries",
            "wages",
            "benefits",
            "depreciation",
            "all other",
            "interest",
            "operation and maintenance",
            "natural classification",
            "by function",
        )
        if not any(term in s for term in detail_disqualifiers):
            return "IS_EXPENSES_TOTAL"

    # REVENUES: TUITION, DISCOUNTS, AUXILIARY, ETC.
    tuition_phrases = (
        "tuition and fees",
        "tuition & fees",
        "tuition fees",
        "net tuition",
    )
    tuition_mentions = any(p in s for p in tuition_phrases) or ("tuition" in s and "fees" in s)
    net_markers = (
        "after deducting discounts",
        "net of discounts",
        "net of scholarship allowances",
        "net of scholarships",
        "(net)",
        "net of discounts and allowances",
    )
    if tuition_mentions and (
        any(marker in s for marker in net_markers)
        or "net tuition" in s
        or "net of allowance" in s
    ):
        return "REV_TUITION_NET"
    if (
        "tuition and fees, net" in s
        or "tuition and fees net" in s
        or "net tuition and fees" in s
    ):
        return "REV_TUITION_NET"
    # Scholarships / discounts by source: Pell, federal, state, local, endowment, institutional.
    if (
        "pell grants represents the gross amount of pell grants" in s
        or "pell grants includes the amount administered" in s
        or "total discounts and allowances from pell grants" in s
    ):
        return "FIN_PELLGROSS_1_0"
    if "discounts and allowances from pell grants applied to tuition and fees" in s:
        return "FIN_PELLTUIT_1_0"
    if "discounts and allowances from pell grants applied to auxiliary enterprises" in s:
        return "FIN_PELLAUX_1_0"
    if (
        "other federal awards are expenditures for scholarships and fellowships" in s
        and "discounts and allowances" not in s
    ) or "other federal grants includes the amount awarded" in s:
        return "FIN_OTHFEDSCH_1_0"
    if "total discounts and allowances from other federal grants" in s:
        return "FIN_OTHFEDSCH_1_0"
    if "discounts and allowances from other federal grants applied to tuition and fees" in s:
        return "FIN_OTHFEDTUIT_1_0"
    if "discounts and allowances from other federal grants applied to auxiliary enterprises" in s:
        return "FIN_OTHFEDAUX_1_0"
    if (
        "grants by state government includes expenditures for scholarships and fellowships" in s
        and "discounts and allowances" not in s
    ) or "state grants includes the amount awarded" in s:
        return "FIN_STGRSCH_1_0"
    if "total discounts and allowances from state government grants" in s:
        return "FIN_STGRSCH_1_0"
    if "discounts and allowances from state government grants applied to tuition and fees" in s:
        return "FIN_STGRTUIT_1_0"
    if "discounts and allowances from state government grants applied to auxiliary enterprises" in s:
        return "FIN_STGRAUX_1_0"
    if (
        "grants by local government includes expenditures for scholarships and fellowships" in s
        and "discounts and allowances" not in s
    ) or "local grants includes the amount awarded" in s:
        return "FIN_LCGRSCH_1_0"
    if "total discounts and allowances from local government grants" in s:
        return "FIN_LCGRSCH_1_0"
    if "discounts and allowances from local government grants applied to tuition and fees" in s:
        return "FIN_LCGRTUIT_1_0"
    if "discounts and allowances from local government grants applied to auxiliary enterprises" in s:
        return "FIN_LCGRAUX_1_0"
    if (
        "endowments are funds whose principal is nonexpendable" in s
        and "discounts and allowances" in s
    ):
        return "FIN_ENDOW1_1_0"
    if "discounts and allowances from endowments and gifts applied to tuition and fees" in s:
        return "FIN_ENDOWTUIT_1_0"
    if "discounts and allowances from endowments and gifts applied to auxiliary enterprises" in s:
        return "FIN_ENDOWAUX_1_0"
    if "institutional grants from restricted sources are expenditures for scholarships and fellowships" in s:
        return "FIN_INGRRESSCH_1_0"
    if "discounts and allowances from other institutional sources applied to tuition and fees" in s:
        return "FIN_INGRTUIT_1_0"
    if "discounts and allowances from other institutional sources applied to auxiliary enterprises" in s:
        return "FIN_INGRAUX_1_0"
    if (
        "scholarship allowances" in s
        or "discounts and allowances" in s
        or "tuition discounts" in s
        or ("discounts" in s and "tuition" in s)
    ):
        return "FIN_DISCOUNTS_TUITION"
    if "auxiliary enterprises" in s and ("revenue" in s or "revenues" in s or "net" in s):
        return "REV_AUXILIARY"
    if "appropriations" in s and ("federal" in s or "state" in s or "local" in s or "government" in s):
        if "federal" in s:
            return "REV_FED_APPROPS"
        if "state" in s:
            return "REV_STATE_APPROPS"
        if "local" in s:
            return "REV_LOCAL_APPROPS"
        return "REV_GOV_APPROPS_TOTAL"
    if "grants and contracts" in s or ("grants" in s and "contracts" in s):
        if "federal" in s:
            return "REV_GRANTS_FED"
        if "state" in s:
            return "REV_GRANTS_STATE"
        if "local" in s or "private" in s:
            return "REV_GRANTS_LOCAL_PRIV"
        return "REV_GRANTS_CONTRACTS_TOTAL"
    if (
        "private gifts" in s
        or "private grants" in s
        or ("private gifts, grants, and contracts" in s)
        or ("contributions from private sources" in s)
        or ("contributions" in s and "government" not in s and "state" not in s)
    ):
        return "REV_PRIVATE_GIFTS_GRANTS"
    if (
        "investment income" in s
        or "investment return" in s
        or ("income from investments" in s)
        or "investment gain" in s
        or "return on investments" in s
        or "investment income (net of expenses)" in s
    ):
        return "REV_INVESTMENT_RETURN"
    if "hospital" in s and ("revenue" in s or "revenues" in s):
        return "REV_HOSPITAL"
    if "independent operations" in s and ("revenue" in s or "revenues" in s):
        return "REV_INDEPENDENT_OPS"
    if "other" in s and "operating" in s and ("revenue" in s or "revenues" in s):
        return "REV_OTHER_OPERATING"
    if "other" in s and "nonoperating" in s and ("revenue" in s or "revenues" in s):
        return "REV_OTHER_NONOPERATING"
    if "capital appropriations" in s:
        return "REV_CAPITAL_APPROPS"
    if "capital grants" in s or "capital gifts" in s:
        return "REV_CAPITAL_GRANTS_GIFTS"
    if "permanent endow" in s:
        return "REV_ADD_PERM_ENDOW"

    # EXPENSES BY FUNCTION
    if _match_function_total(s, "instruction"):
        return "EXP_INSTRUCTION"
    if _match_function_total(s, "research"):
        return "EXP_RESEARCH"
    if _match_function_total(s, "public service"):
        return "EXP_PUBLIC_SERVICE"
    if _match_function_total(s, "academic support"):
        return "EXP_ACADEMIC_SUPPORT"
    if _match_function_total(s, "student services"):
        return "EXP_STUDENT_SERVICES"
    if _match_function_total(s, "institutional support"):
        return "EXP_INSTITUTIONAL_SUPPORT"
    if "operations and maintenance of plant" in s or "operation and maintenance of plant" in s:
        return "EXP_OPERATIONS_PLANT"
    if (
        "scholarships and fellowships" in s
        and "discounts" not in s
        and "allowances" not in s
    ):
        return "EXP_SCHOLARSHIPS_NET"
    if (
        ("student aid" in s or "grants to students" in s or "grants and scholarships to students" in s)
        and "discount" not in s
        and "allowance" not in s
    ):
        return "EXP_SCHOLARSHIPS_NET"

    return None


def _first_nonempty(series: pd.Series) -> str:
    for val in series:
        if isinstance(val, str) and val.strip():
            return val
    return ""


def _collapse_block(block: pd.DataFrame, year_start: int, year_end: int) -> dict:
    first = block.iloc[0]

    surveys = sorted(set(str(s) for s in block.get("survey", []) if pd.notna(s)))
    survey_val = ";".join(surveys) if surveys else first.get("survey", "")

    vars_raw: list[str] = []
    if "source_var" in block.columns:
        amount_rows = block
        if "is_amount" in block.columns:
            amount_rows = block[block["is_amount"].eq(True)]
        if amount_rows.empty:
            amount_rows = block
        for v in amount_rows["source_var"].dropna():
            vars_raw.extend(str(v).split(";"))
    source_vars = sorted(set(v.strip() for v in vars_raw if v.strip()))
    source_var_val = ";".join(source_vars) if source_vars else first.get("source_var", "")

    is_amount_val = True
    if "is_amount" in block.columns:
        is_amount_val = bool(block["is_amount"].eq(True).any())

    source_label = _first_nonempty(block.get("source_label", pd.Series(dtype=object)))
    source_label_norm = _first_nonempty(block.get("source_label_norm", pd.Series(dtype=object)))

    notes_vals = [str(n) for n in block.get("notes", []) if isinstance(n, str) and n.strip()]
    notes_val = " | ".join(sorted(set(notes_vals))) if notes_vals else first.get("notes", "")

    return {
        "concept_key": first["concept_key"],
        "form_family": first.get("form_family", ""),
        "survey": survey_val,
        "year_start": year_start,
        "year_end": year_end,
        "base_key": first.get("base_key", ""),
        "section": first.get("section", ""),
        "line_code": first.get("line_code", ""),
        "source_var": source_var_val,
        "source_label": source_label,
        "source_label_norm": source_label_norm,
        "weight": first.get("weight", 1.0),
        "notes": notes_val,
        "is_amount": is_amount_val,
    }


def _merge_intervals_for_group(g: pd.DataFrame) -> pd.DataFrame:
    if g.empty:
        return g

    g = g.copy()
    g["year_start"] = pd.to_numeric(g["year_start"], errors="coerce").astype("Int64")
    g["year_end"] = pd.to_numeric(g["year_end"], errors="coerce").astype("Int64")
    g = g.sort_values(["year_start", "year_end"])

    merged_rows = []
    current_start = int(g.iloc[0]["year_start"])
    current_end = int(g.iloc[0]["year_end"])
    idxs = [g.index[0]]

    for idx, row in g.iloc[1:].iterrows():
        ys = int(row["year_start"])
        ye = int(row["year_end"])

        if ys <= current_end + 1:
            current_end = max(current_end, ye)
            idxs.append(idx)
        else:
            block = g.loc[idxs]
            merged_rows.append(_collapse_block(block, current_start, current_end))
            current_start = ys
            current_end = ye
            idxs = [idx]

    block = g.loc[idxs]
    merged_rows.append(_collapse_block(block, current_start, current_end))

    return pd.DataFrame(merged_rows)


def _apply_manual_overrides(cw: pd.DataFrame) -> pd.DataFrame:
    if not OVERRIDES_PATH.exists():
        return cw

    overrides = pd.read_csv(OVERRIDES_PATH)
    overrides.columns = [c.strip() for c in overrides.columns]

    required = {"form_family", "base_key", "year_start", "year_end", "concept_key"}
    missing = required - set(overrides.columns)
    if missing:
        raise ValueError(
            f"finance_crosswalk_overrides.csv is missing required columns: {sorted(missing)}"
        )

    overrides = overrides.copy()
    _strip_str_cols(overrides, ("form_family", "base_key"))
    overrides["concept_key"] = overrides["concept_key"].fillna("").astype(str).str.strip()

    for col in ("year_start", "year_end"):
        overrides[col] = pd.to_numeric(overrides[col], errors="coerce").astype("Int64")

    if "weight" not in overrides.columns:
        overrides["weight"] = 1.0
    overrides["weight"] = pd.to_numeric(overrides["weight"], errors="coerce").fillna(1.0)

    merge_cols = ["form_family", "base_key", "year_start", "year_end"]
    merged = cw.merge(
        overrides[merge_cols + ["concept_key", "weight"]],
        on=merge_cols,
        how="left",
        suffixes=("", "_ov"),
    )

    mask = merged["concept_key_ov"].notna() & merged["concept_key_ov"].astype(str).str.strip().ne("")
    applied = int(mask.sum())
    if applied:
        print(f"Applied {applied} manual overrides from {OVERRIDES_PATH}")
    else:
        print(f"Manual overrides file {OVERRIDES_PATH} loaded but no rows matched template keys.")

    merged.loc[mask, "concept_key"] = merged.loc[mask, "concept_key_ov"]
    merged.loc[mask, "weight"] = merged.loc[mask, "weight_ov"]

    return merged.drop(columns=["concept_key_ov", "weight_ov"])


def _export_suspect_core(cw: pd.DataFrame) -> None:
    if "source_label_norm" not in cw.columns:
        print("\nsource_label_norm column missing; cannot inspect suspect core rows.")
        return

    core_pattern = (
        "instruction|research|public service|academic support|student services|institutional support|"
        "operation and maintenance of plant|operations and maintenance of plant|"
        "tuition|fees|scholarship|scholarships|student aid|grants to students|"
        "endowment|auxiliary|appropriations|grants and contracts|investment income|investment return"
    )

    mask_blank = cw["concept_key"].astype(str).str.strip().eq("")
    label_str = cw["source_label_norm"].fillna("").astype(str)
    amount_mask = (
        cw["is_amount"]
        if "is_amount" in cw.columns
        else pd.Series(True, index=cw.index)
    )
    suspect_core = cw[
        mask_blank
        & amount_mask
        & label_str.str.contains(core_pattern, case=False, regex=True)
    ]

    if not suspect_core.empty:
        print(f"\nWARNING: {len(suspect_core)} suspect core rows still have blank concept_key.")
        cols = [
            "form_family",
            "base_key",
            "year_start",
            "year_end",
            "source_var",
            "source_label",
            "source_label_norm",
        ]
        existing_cols = [c for c in cols if c in suspect_core.columns]
        print(suspect_core[existing_cols].head(10).to_string(index=False))
    else:
        print("\nNo suspect core rows with blank concept_key.")


def inspect_component_endowment_labels(dict_path: Path | str | None = None) -> None:
    """
    Inspect dictionary lake entries for component (F1A_F / F1A_G) lines that mention endowment.

    This allows us to decide whether manual overrides are needed for component files.
    """
    dict_path = Path(dict_path) if dict_path else DICTIONARY_LAKE
    if not dict_path.exists():
        print(f"Dictionary lake not found: {dict_path}")
        return

    cols = [
        "year",
        "form_family",
        "base_key",
        "source_var",
        "source_label_norm",
        "dict_filename",
        "data_filename",
        "survey",
        "is_finance",
    ]
    try:
        df = pd.read_parquet(dict_path, columns=cols)
    except Exception as exc:  # pragma: no cover - debug helper
        print(f"Failed to load dictionary lake {dict_path}: {exc}")
        return

    df = df[df["is_finance"] == True].copy()
    if df.empty:
        print("No finance rows present in dictionary lake sample.")
        return

    dict_names = df["dict_filename"].fillna("")
    data_names = df["data_filename"].fillna("")
    survey_vals = df["survey"].fillna("")
    comp_mask = (
        dict_names.str.contains(r"f1a.*_(?:f|g)", case=False, regex=True)
        | data_names.str.contains(r"f1a.*_(?:f|g)", case=False, regex=True)
        | survey_vals.str.contains("F1A", case=False, regex=True)
    )
    label_mask = df["source_label_norm"].fillna("").str.contains("endowment", case=False, na=False)
    component_endow = df[comp_mask & label_mask]

    if component_endow.empty:
        print("No endowment-related labels found on F1 component dictionaries.")
        return

    cols_out = [
        "year",
        "form_family",
        "base_key",
        "source_var",
        "source_label_norm",
        "dict_filename",
    ]
    cols_present = [c for c in cols_out if c in component_endow.columns]
    summary = (
        component_endow[cols_present]
        .drop_duplicates()
        .sort_values(cols_present)
    )

    print("\nPotential component endowment lines:")
    print(summary.to_string(index=False))
    print(f"\nTotal matching rows: {len(summary)}")


def inspect_endowment_base_keys(
    step0_path: Path | str | None = None,
    filled_crosswalk: Path | str | None = None,
) -> None:
    """
    Helper for manual inspection of endowment base_keys.

    Reads a Step 0 long file (defaulting to STEP0_SAMPLE) and the filled crosswalk to
    summarize which base_keys currently map to the endowment concepts and which base_keys
    appear for the F1 component families. Intended to be invoked manually in REPL.
    """
    step0_path = Path(step0_path) if step0_path else STEP0_SAMPLE
    filled_crosswalk = Path(filled_crosswalk) if filled_crosswalk else CROSSWALK_OUT

    if not step0_path.exists():
        print(f"Step 0 sample not found: {step0_path}")
        return
    if not filled_crosswalk.exists():
        print(f"Filled crosswalk not found: {filled_crosswalk}")
        return

    try:
        step0 = pd.read_parquet(step0_path)
    except Exception as exc:  # pragma: no cover - debug helper
        print(f"Failed to load Step 0 sample {step0_path}: {exc}")
        return

    try:
        crosswalk = pd.read_csv(filled_crosswalk)
    except Exception as exc:  # pragma: no cover - debug helper
        print(f"Failed to load crosswalk {filled_crosswalk}: {exc}")
        return

    if "concept_key" not in crosswalk.columns:
        print("Crosswalk is missing concept_key column; cannot inspect endowments.")
    else:
        endow_concepts = [
            "BS_ENDOWMENT_FMV",
            "FIN_ENDOW_ASSETS_BEGIN",
            "FIN_ENDOW_ASSETS_END",
            "FIN_ENDOW_NET_CHANGE",
        ]
        any_concepts = False
        for concept in endow_concepts:
            endow = crosswalk[crosswalk["concept_key"] == concept]
            if endow.empty:
                continue
            any_concepts = True
            cols = [c for c in ["form_family", "base_key", "year_start", "year_end"] if c in endow.columns]
            print(f"\nCrosswalk {concept} entries:")
            if cols:
                preview = endow[cols].drop_duplicates().sort_values(cols)
                print(preview.to_string(index=False))
            fam_counts = endow["form_family"].value_counts(dropna=False)
            print("\nCounts by form_family:")
            print(fam_counts.to_string())
        if not any_concepts:
            print("No endowment concept rows present in filled crosswalk.")

    required_cols = {"form_family", "base_key"}
    if not required_cols.issubset(step0.columns):
        print(f"Step 0 file lacks required columns: {sorted(required_cols - set(step0.columns))}")
        return

    comp_fams = {"F1_COMP_F", "F1_COMP_G"}
    comp_rows = step0[step0["form_family"].isin(comp_fams)].copy()
    value_col = next((col for col in ("value", "amount", "reported_value") if col in comp_rows.columns), None)
    if value_col:
        comp_rows = comp_rows[comp_rows[value_col].notna()]

    if comp_rows.empty:
        print("\nNo component-family rows found in Step 0 sample.")
        return

    print("\nComponent family base_keys observed in Step 0 sample:")
    base_key_summary = (
        comp_rows[["form_family", "base_key"]]
        .drop_duplicates()
        .sort_values(["form_family", "base_key"])
    )
    for fam, subset in base_key_summary.groupby("form_family"):
        keys = ", ".join(subset["base_key"].astype(str))
        print(f"  {fam}: {keys}")


def main() -> None:
    cw = pd.read_csv(CROSSWALK_IN)
    cw.columns = [c.strip() for c in cw.columns]
    required = {"form_family", "base_key", "year_start", "year_end"}
    missing = required - set(cw.columns)
    if missing:
        raise SystemExit(f"Finance crosswalk template missing columns: {sorted(missing)}")

    if "concept_key" not in cw.columns:
        cw["concept_key"] = ""
    cw["concept_key"] = cw["concept_key"].astype("string")

    # Drop any pre-existing balance sheet concepts; we no longer auto-fill those.
    cw.loc[cw["concept_key"].isin(IGNORED_CONCEPTS), "concept_key"] = ""

    _strip_str_cols(cw, ("form_family", "base_key"))
    for col in ("year_start", "year_end"):
        if col in cw.columns:
            cw[col] = pd.to_numeric(cw[col], errors="raise").astype("Int64")
    core_sections = {"B", "C", "D", "E", "H"}
    if {"section", "form_family"}.issubset(cw.columns):
        # Restrict to the five finance components (revenues, expenses, changes, aid, endowment).
        cw = cw[cw["section"].isin(core_sections)].copy()

        def _is_relevant_component_row(row: pd.Series) -> bool:
            return _is_relevant_component(row.get("form_family"), row.get("section"))

        cw = cw[cw.apply(_is_relevant_component_row, axis=1)].reset_index(drop=True)

    var_type_map = _build_var_type_map(DICTIONARY_LAKE)
    if var_type_map:
        def _row_is_amount(row: pd.Series) -> bool:
            fam = _normalize_form_family(row.get("form_family"))
            survey = (row.get("survey") or "").strip().upper()
            raw = row.get("source_var")
            if not fam or not isinstance(raw, str) or not raw.strip():
                return False
            parts = [p.strip().upper() for p in raw.split(";") if p.strip()]
            if not parts:
                return False
            for part in parts:
                key = (fam, survey, part)
                if key in var_type_map and var_type_map[key]:
                    return True
                generic = (fam, "", part)
                if generic in var_type_map and var_type_map[generic]:
                    return True

            sec = (row.get("section") or "").strip().upper()
            label = (row.get("source_label_norm") or row.get("source_label") or "").lower()
            detail_terms = ("salaries", "wages", "benefits", "depreciation", "interest", "all other")

            if fam.startswith("F1") and sec == "D":
                if "total revenues and other additions" in label:
                    return True
                if "total expenses and other deductions" in label:
                    return True

            if fam.startswith("F2") and sec == "B":
                if "total revenues and investment return" in label:
                    return True
                if "total expenses" in label and not any(term in label for term in detail_terms):
                    return True

            if fam.startswith("F3") and sec == "B":
                if "total revenues and investment return" in label:
                    return True
                if "total expenses" in label and not any(term in label for term in detail_terms):
                    return True

            return False

        cw["is_amount"] = cw.apply(_row_is_amount, axis=1)
    else:
        print("WARNING: Unable to determine amount types; defaulting all rows to is_amount=True.")
        cw["is_amount"] = True
    bad_range = cw["year_start"] > cw["year_end"]
    if bad_range.any():
        print("ERROR: Finance crosswalk has rows with year_start > year_end.")
        print(cw.loc[bad_range, ["form_family", "base_key", "year_start", "year_end"]].head(10).to_string(index=False))
        raise SystemExit(1)
    key_cols = ["form_family", "base_key", "year_start", "year_end"]
    dup_mask = cw.duplicated(key_cols, keep=False)
    if dup_mask.any():
        dup_count = int(dup_mask.sum())
        print(f"[WARN] Finance template has {dup_count} duplicate key rows; keeping first occurrence per {key_cols}.")
        cw = cw.drop_duplicates(subset=key_cols, keep="first").reset_index(drop=True)
    min_year = int(cw["year_start"].min())
    max_year = int(cw["year_end"].max())
    forms = ", ".join(sorted(cw["form_family"].dropna().unique()))
    print(f"Finance template rows: {len(cw):,}. Year span: {min_year}-{max_year}. Form families: {forms}")

    if "weight" not in cw.columns:
        cw["weight"] = 1.0

    cw = _apply_manual_overrides(cw)
    cw["concept_key"] = cw["concept_key"].astype("string")

    mask_blank = cw["concept_key"].isna() | (cw["concept_key"].astype(str).str.strip() == "")
    amount_mask = (
        cw["is_amount"]
        if "is_amount" in cw.columns
        else pd.Series(True, index=cw.index)
    )
    mask_to_fill = mask_blank & amount_mask
    cw.loc[mask_to_fill, "concept_key"] = cw.loc[mask_to_fill].apply(
        lambda r: assign_concept(
            r.get("source_label_norm"),
            r.get("form_family"),
            r.get("base_key"),
            r.get("source_var"),
        ),
        axis=1,
    )

    cw["weight"] = pd.to_numeric(cw["weight"], errors="coerce").fillna(1.0)

    print("concept_key counts after auto-fill:")
    print(cw["concept_key"].value_counts(dropna=True).head(40))

    if "is_amount" in cw.columns:
        non_amount_with_concept = cw[
            (cw["is_amount"] == False)
            & cw["concept_key"].astype(str).str.strip().ne("")
        ]
        if not non_amount_with_concept.empty:
            print(
                f"[WARN] Clearing concept_key on {len(non_amount_with_concept)} non-amount rows to avoid mislabeling categorical variables."
            )
            print("\n[DEBUG] Sample non-amount rows that had concepts before clearing:")
            cols = [
                "form_family",
                "base_key",
                "year_start",
                "year_end",
                "survey",
                "source_var",
                "source_label",
                "source_label_norm",
                "concept_key",
            ]
            sample_cols = [c for c in cols if c in non_amount_with_concept.columns]
            if sample_cols:
                print(non_amount_with_concept[sample_cols].head(12).to_string(index=False))
            cw.loc[non_amount_with_concept.index, "concept_key"] = ""

    filled = sorted(
        c
        for c in cw["concept_key"].dropna().unique()
        if isinstance(c, str) and c.strip()
    )
    if filled:
        print(f"\nConcepts filled ({len(filled)} total):")
        for name in filled:
            print(f"  - {name}")
    else:
        print("\nNo concepts were auto-filled.")

    # Collapse overlapping/contiguous intervals per (form_family, base_key, concept_key)
    has_concept = cw["concept_key"].astype(str).str.strip() != ""
    mapped = cw[has_concept].copy()
    unmapped = cw[~has_concept].copy()

    merged_groups: list[pd.DataFrame] = []
    group_cols = ["form_family", "base_key", "concept_key"]
    for _, grp in mapped.groupby(group_cols, dropna=False):
        merged_groups.append(_merge_intervals_for_group(grp))

    if merged_groups:
        mapped = pd.concat(merged_groups, ignore_index=True)

    cw = pd.concat([mapped, unmapped], ignore_index=True, sort=False)
    cw = cw.sort_values(["form_family", "base_key", "concept_key", "year_start"], na_position="last").reset_index(drop=True)

    unknown = set(cw["concept_key"].dropna().unique()) - CONCEPTS - {""}
    if unknown:
        print("WARNING: crosswalk contains concept_keys not in the schema:", sorted(unknown))

    endow_concepts = [
        "BS_ENDOWMENT_FMV",
        "FIN_ENDOW_ASSETS_BEGIN",
        "FIN_ENDOW_ASSETS_END",
        "FIN_ENDOW_NET_CHANGE",
    ]
    any_endowment = False
    for concept in endow_concepts:
        subset = cw[cw["concept_key"] == concept]
        if subset.empty:
            continue
        any_endowment = True
        print(f"\n{concept} mappings by form_family:")
        print(subset["form_family"].value_counts(dropna=False))
        preview_cols = ["form_family", "base_key", "year_start", "year_end"]
        preview_cols = [c for c in preview_cols if c in subset.columns]
        if preview_cols:
            preview = (
                subset[preview_cols]
                .drop_duplicates()
                .sort_values(preview_cols)
                .head(20)
            )
            print(f"\nSample of {concept} rows:")
            print(preview.to_string(index=False))
    if not any_endowment:
        print("\nWARNING: no endowment mappings found in crosswalk.")

    mapped_nonblank = cw[cw["concept_key"].astype(str).str.strip().ne("")]
    if not mapped_nonblank.empty:
        _assert_no_overlaps(mapped_nonblank, ("form_family", "base_key", "concept_key"))

    ck_series = cw["concept_key"].astype(str).str.strip()
    core_mask = cw["source_var"].astype(str).str.match(CORE_SECTION_PATTERN.pattern, na=False)
    amount_mask = (
        cw["is_amount"]
        if "is_amount" in cw.columns
        else pd.Series(True, index=cw.index)
    )
    core_amount_mask = core_mask & amount_mask
    missing_mask = core_amount_mask & (ck_series.eq("") | ck_series.str.lower().eq("nan"))
    if missing_mask.any():
        print("ERROR: Finance crosswalk still has core B/C/E/H rows without concept_key. Sample:")
        print(cw.loc[missing_mask, ["form_family", "source_var", "base_key", "year_start", "year_end", "source_label_norm"]].head(10).to_string(index=False))
        raise SystemExit(1)

    _export_suspect_core(cw)

    cw.to_csv(CROSSWALK_OUT, index=False)
    print(f"Wrote filled crosswalk to {CROSSWALK_OUT}")
    print(f"Total rows: {len(cw):,}. Concept-keyed rows: {ck_series.ne('').sum():,}")
    top = mapped_nonblank["concept_key"].value_counts().head(20)
    print("Top concept_keys:")
    print(top.to_string())


if __name__ == "__main__":
    main()
