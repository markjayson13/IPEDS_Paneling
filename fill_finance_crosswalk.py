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
CROSSWALK_OUT = Path("/Users/markjaysonfarol13/Higher Ed research/IPEDS/Paneled Datasets/Crosswalks/Filled/finance_crosswalk_filled.csv")

CONCEPTS = {
    "IS_REVENUES_TOTAL",
    "IS_EXPENSES_TOTAL",
    "IS_NET_INCOME",
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
}

def assign_concept(label: str, form_family: str, base_key: str) -> str | None:
    """Heuristic mapping from source_label_norm to the conceptual schema."""
    if not isinstance(label, str):
        return None
    s = label.lower().strip()

    # ENDOWMENTS / INVESTMENTS
    if (
        ("investments" in s and "long-term" in s)
        or ("investments" in s and "long term" in s)
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
    if "change in net assets" in s or "increase in net assets" in s or "increase in net position" in s:
        return "IS_NET_INCOME"

    # REVENUES: TUITION, DISCOUNTS, AUXILIARY, ETC.
    if ("tuition and fees" in s or "tuition fees" in s or "net tuition" in s) and (
        "after deducting discounts" in s
        or "net of discounts" in s
        or "net of scholarship allowances" in s
        or "net of scholarships" in s
        or "net" in s
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
    if ("instruction" in s and "expenses" in s) or s.startswith("instruction"):
        return "EXP_INSTRUCTION"
    if ("research" in s and "expenses" in s) or s.startswith("research"):
        return "EXP_RESEARCH"
    if ("public service" in s and "expenses" in s) or s.startswith("public service"):
        return "EXP_PUBLIC_SERVICE"
    if ("academic support" in s and "expenses" in s) or s.startswith("academic support"):
        return "EXP_ACADEMIC_SUPPORT"
    if ("student services" in s and "expenses" in s) or s.startswith("student services"):
        return "EXP_STUDENT_SERVICES"
    if ("institutional support" in s and "expenses" in s) or s.startswith("institutional support"):
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


def main() -> None:
    cw = pd.read_csv(CROSSWALK_IN)
    cw.columns = [c.strip() for c in cw.columns]

    if "concept_key" not in cw.columns:
        cw["concept_key"] = ""

    # Drop any pre-existing balance sheet concepts; we no longer auto-fill those.
    cw.loc[cw["concept_key"].isin(IGNORED_CONCEPTS), "concept_key"] = ""

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
