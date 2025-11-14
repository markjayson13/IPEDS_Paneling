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
import pandas as pd


# Assumes you run this from the repo root where finance_crosswalk_template.csv lives.
CROSSWALK_IN = Path("/Users/markjaysonfarol13/Higher Ed research/IPEDS/Paneled Datasets/Crosswalks/finance_crosswalk_template.csv")
CROSSWALK_OUT = Path("/Users/markjaysonfarol13/Higher Ed research/IPEDS/Paneled Datasets/Crosswalks/finance_crosswalk_template.csv")

CONCEPTS = {
    "BS_ASSETS_TOTAL",
    "BS_LIABILITIES_TOTAL",
    "BS_NET_ASSETS_TOTAL",
    "BS_NET_ASSETS_WODR",
    "BS_NET_ASSETS_WDR",
    "BS_ASSETS_CASH",
    "BS_ASSETS_INVESTMENTS_TOTAL",
    "BS_LIAB_DEBT_LONGTERM",
    "BS_ASSETS_CAPITAL_NET",
    "BS_ENDOWMENT_FMV",
    "IS_REVENUES_TOTAL",
    "IS_EXPENSES_TOTAL",
    "IS_NET_INCOME",
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


def assign_concept(label: str, form_family: str, base_key: str) -> str | None:
    """Heuristic mapping from source_label_norm to the conceptual schema."""
    if not isinstance(label, str):
        return None
    s = label.lower().strip()

    # BALANCE SHEET TOTALS
    if "total assets" in s and "net assets" not in s:
        if "current assets" not in s and "noncurrent" not in s:
            return "BS_ASSETS_TOTAL"
    if "total liabilities" in s:
        return "BS_LIABILITIES_TOTAL"
    if "total net assets" in s or ("net position end of year" in s) or ("net assets end of year" in s):
        return "BS_NET_ASSETS_TOTAL"
    if ("unrestricted net" in s) or ("without donor restrictions" in s):
        return "BS_NET_ASSETS_WODR"
    if (("restricted" in s and ("net assets" in s or "net position" in s)) or ("with donor restrictions" in s)):
        return "BS_NET_ASSETS_WDR"

    # CASH (only if explicitly present)
    if "cash and cash equivalents" in s:
        return "BS_ASSETS_CASH"

    # INVESTMENTS & ENDOWMENT
    if "long-term investments" in s or "long term investments" in s:
        return "BS_ASSETS_INVESTMENTS_TOTAL"
    if s.startswith("value of endowment assets at the end"):
        return "BS_ENDOWMENT_FMV"

    # CAPITAL ASSETS NET
    if ("capital assets - net" in s) or ("capital assets net of" in s) or ("net capital assets" in s):
        return "BS_ASSETS_CAPITAL_NET"
    if "depreciable capital assets net of depreciation" in s:
        return "BS_ASSETS_CAPITAL_NET"

    # LONG-TERM DEBT
    if ("bonds payable" in s or "notes payable" in s or "long-term debt" in s or "long term debt" in s) and "current" not in s:
        return "BS_LIAB_DEBT_LONGTERM"

    # INCOME STATEMENT TOTALS
    if (
        "total revenues and other additions" in s
        or "total operating revenues" in s
        or (s.startswith("total revenues") and "other" not in s)
    ):
        return "IS_REVENUES_TOTAL"
    if "total expenses" in s or "total operating expenses" in s:
        return "IS_EXPENSES_TOTAL"
    if "change in net assets" in s or "increase in net assets" in s or "increase in net position" in s:
        return "IS_NET_INCOME"

    # REVENUES: TUITION, DISCOUNTS, AUXILIARY, ETC.
    if ("tuition and fees" in s or "tuition fees" in s) and (
        "after deducting discounts" in s
        or "net of discounts" in s
        or "net of scholarship allowances" in s
        or "net" in s
    ):
        return "REV_TUITION_NET"
    if "scholarship allowances" in s or "discounts and allowances" in s:
        return "DISCOUNT_TUITION"
    if "auxiliary enterprises" in s and ("revenue" in s or "revenues" in s):
        return "REV_AUXILIARY_NET"
    if "appropriations" in s:
        return "REV_GOV_APPROPS_TOTAL"
    if "grants and contracts" in s:
        return "REV_GRANTS_CONTRACTS_TOTAL"
    if (
        "private gifts" in s
        or "private grants" in s
        or ("contributions" in s and "government" not in s and "state" not in s)
    ):
        return "REV_PRIVATE_GIFTS"
    if "investment income" in s or "investment return" in s or ("income from investments" in s):
        return "REV_INVESTMENT_RETURN"

    # EXPENSES BY FUNCTION
    if s.startswith("instruction") or s.startswith("expenses for instruction"):
        return "EXP_INSTRUCTION"
    if s.startswith("research") or s.startswith("expenses for research"):
        return "EXP_RESEARCH"
    if s.startswith("public service") or s.startswith("expenses for public service"):
        return "EXP_PUBLIC_SERVICE"
    if s.startswith("academic support") or s.startswith("expenses for academic support"):
        return "EXP_ACADEMIC_SUPPORT"
    if s.startswith("student services") or s.startswith("expenses for student services"):
        return "EXP_STUDENT_SERVICES"
    if s.startswith("institutional support") or s.startswith("expenses for institutional support"):
        return "EXP_INSTITUTIONAL_SUPPORT"
    if "operations and maintenance of plant" in s or "operation and maintenance of plant" in s:
        return "EXP_OPERATIONS_PLANT"
    if s.startswith("scholarships and fellowships") and "discounts" not in s and "allowances" not in s:
        return "EXP_SCHOLARSHIPS_NET"

    return None


def main() -> None:
    cw = pd.read_csv(CROSSWALK_IN)
    cw.columns = [c.strip() for c in cw.columns]

    if "concept_key" not in cw.columns:
        cw["concept_key"] = ""

    mask_blank = cw["concept_key"].isna() | (cw["concept_key"].astype(str).str.strip() == "")
    cw.loc[mask_blank, "concept_key"] = cw.loc[mask_blank].apply(
        lambda r: assign_concept(
            r.get("source_label_norm"),
            r.get("form_family"),
            r.get("base_key"),
        ),
        axis=1,
    )

    if "weight" not in cw.columns:
        cw["weight"] = 1.0
    cw["weight"] = pd.to_numeric(cw["weight"], errors="coerce").fillna(1.0)

    print("concept_key counts after auto-fill:")
    print(cw["concept_key"].value_counts(dropna=True).head(40))

    unknown = set(cw["concept_key"].dropna().unique()) - CONCEPTS - {""}
    if unknown:
        print("WARNING: crosswalk contains concept_keys not in the schema:", sorted(unknown))

    cw.to_csv(CROSSWALK_OUT, index=False)
    print(f"Wrote filled crosswalk to {CROSSWALK_OUT}")


if __name__ == "__main__":
    main()
