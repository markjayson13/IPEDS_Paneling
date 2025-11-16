#!/usr/bin/env python3
"""
Auto-fill concept_key for finance_crosswalk_template.csv based on source_label_norm.

This is a first-pass mapping into the conceptual schema:
- BS_*  balance sheet
- IS_*  income statement
- REV_* revenues
- EXP_* expenses
- DISCOUNT_TUITION

You MUST review the output (finance_crosswalk_filled.csv) before using it in production.
"""

from pathlib import Path
import re

import pandas as pd


# Assumes you run this from the repo root where finance_crosswalk_template.csv lives.
CROSSWALK_IN = Path("/Users/markjaysonfarol13/Higher Ed research/IPEDS/Paneled Datasets/Crosswalks/finance_crosswalk_template.csv")
CROSSWALK_OUT = Path("/Users/markjaysonfarol13/Higher Ed research/IPEDS/Paneled Datasets/Crosswalks/Filled/finance_crosswalk_filled.csv")
OVERRIDES_PATH = Path("/Users/markjaysonfarol13/Higher Ed research/IPEDS/Paneled Datasets/Crosswalks/finance_crosswalk_overrides.csv")

CONCEPTS = {
    "IS_REVENUES_TOTAL",
    "IS_EXPENSES_TOTAL",
    "BS_ASSETS_INVESTMENTS_TOTAL",
    "BS_ENDOWMENT_FMV",
    "REV_TUITION_NET",
    "REV_GOV_APPROPS_TOTAL",
    "REV_GRANTS_CONTRACTS_TOTAL",
    "REV_PRIVATE_GIFTS",
    "REV_INVESTMENT_RETURN",
    "REV_AUXILIARY_NET",
    "EXP_INSTRUCTION",
    "EXP_RESEARCH",
    "EXP_PUBLIC_SERVICE",
    "EXP_ACADEMIC_SUPPORT",
    "EXP_STUDENT_SERVICES",
    "EXP_INSTITUTIONAL_SUPPORT",
    "EXP_OPERATIONS_PLANT",
    "EXP_SCHOLARSHIPS_NET",
    "DISCOUNT_TUITION",
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


def _contains_any(text: str, *terms: str) -> bool:
    return any(term in text for term in terms)


def _match_function_total(text: str, term: str) -> bool:
    """Return True when `text` clearly represents the total for the requested function."""
    if term not in text:
        return False

    if _contains_any(text, "expenses", "expense", "expenditures"):
        return True

    if "total" not in text:
        return False

    detail_terms = ("salaries", "wages", "benefits", "fringe")
    if any(detail in text for detail in detail_terms):
        return False

    pattern = rf"(?:{term}\W{{0,12}}total|total\W{{0,12}}{term})"
    return bool(re.search(pattern, text))


def _strip_str_cols(df: pd.DataFrame, cols: tuple[str, ...]) -> None:
    for col in cols:
        if col in df.columns:
            df[col] = df[col].apply(lambda v: v.strip() if isinstance(v, str) else v)


def assign_concept(label: str, form_family: str, base_key: str) -> str | None:
    """Heuristic mapping from source_label_norm to the conceptual schema."""
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

    # INCOME STATEMENT TOTALS
    if (
        "total revenues and other additions" in s
        or "total operating revenues" in s
        or (s.startswith("total revenues") and "other" not in s)
    ):
        return "IS_REVENUES_TOTAL"
    if "total expenses" in s or "total operating expenses" in s:
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
        "scholarship allowances" in s
        or "discounts and allowances" in s
        or "tuition discounts" in s
        or ("discounts" in s and "tuition" in s)
    ):
        return "DISCOUNT_TUITION"
    if "auxiliary enterprises" in s and ("revenue" in s or "revenues" in s or "net" in s):
        return "REV_AUXILIARY_NET"
    if "appropriations" in s and ("federal" in s or "state" in s or "local" in s or "government" in s):
        return "REV_GOV_APPROPS_TOTAL"
    if "grants and contracts" in s or ("grants" in s and "contracts" in s):
        return "REV_GRANTS_CONTRACTS_TOTAL"
    if (
        "private gifts" in s
        or "private grants" in s
        or ("private gifts, grants, and contracts" in s)
        or ("contributions from private sources" in s)
        or ("contributions" in s and "government" not in s and "state" not in s)
    ):
        return "REV_PRIVATE_GIFTS"
    if (
        "investment income" in s
        or "investment return" in s
        or ("income from investments" in s)
        or "investment gain" in s
        or "return on investments" in s
        or "investment income (net of expenses)" in s
    ):
        return "REV_INVESTMENT_RETURN"

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
        for v in block["source_var"].dropna():
            vars_raw.extend(str(v).split(";"))
    source_vars = sorted(set(v.strip() for v in vars_raw if v.strip()))
    source_var_val = ";".join(source_vars) if source_vars else first.get("source_var", "")

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
        print("\nsource_label_norm column missing; cannot produce suspect core CSV.")
        return

    core_pattern = (
        "instruction|research|public service|academic support|student services|institutional support|"
        "operation and maintenance of plant|operations and maintenance of plant|"
        "tuition|fees|scholarship|scholarships|student aid|grants to students|"
        "endowment|auxiliary|appropriations|grants and contracts|investment income|investment return"
    )

    mask_blank = cw["concept_key"].astype(str).str.strip().eq("")
    label_str = cw["source_label_norm"].fillna("").astype(str)
    suspect_core = cw[mask_blank & label_str.str.contains(core_pattern, case=False, regex=True)]

    suspect_path = CROSSWALK_OUT.with_name("finance_crosswalk_suspect_core.csv")
    if not suspect_core.empty:
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
        suspect_core[existing_cols].to_csv(suspect_path, index=False)
        print(f"\nWrote {len(suspect_core)} suspect core rows with blank concept_key to {suspect_path}")
    else:
        print("\nNo suspect core rows with blank concept_key.")


def main() -> None:
    cw = pd.read_csv(CROSSWALK_IN)
    cw.columns = [c.strip() for c in cw.columns]

    if "concept_key" not in cw.columns:
        cw["concept_key"] = ""

    # Drop any pre-existing balance sheet concepts; we no longer auto-fill those.
    cw.loc[cw["concept_key"].isin(IGNORED_CONCEPTS), "concept_key"] = ""

    _strip_str_cols(cw, ("form_family", "base_key"))
    for col in ("year_start", "year_end"):
        if col in cw.columns:
            cw[col] = pd.to_numeric(cw[col], errors="coerce").astype("Int64")

    if "weight" not in cw.columns:
        cw["weight"] = 1.0

    cw = _apply_manual_overrides(cw)

    mask_blank = cw["concept_key"].isna() | (cw["concept_key"].astype(str).str.strip() == "")
    cw.loc[mask_blank, "concept_key"] = cw.loc[mask_blank].apply(
        lambda r: assign_concept(
            r.get("source_label_norm"),
            r.get("form_family"),
            r.get("base_key"),
        ),
        axis=1,
    )

    cw["weight"] = pd.to_numeric(cw["weight"], errors="coerce").fillna(1.0)

    print("concept_key counts after auto-fill:")
    print(cw["concept_key"].value_counts(dropna=True).head(40))

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

    _export_suspect_core(cw)

    cw.to_csv(CROSSWALK_OUT, index=False)
    print(f"Wrote filled crosswalk to {CROSSWALK_OUT}")


if __name__ == "__main__":
    main()
