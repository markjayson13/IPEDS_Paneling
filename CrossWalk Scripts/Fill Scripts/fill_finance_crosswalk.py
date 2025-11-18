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
DICTIONARY_LAKE = Path("/Users/markjaysonfarol13/Higher Ed research/IPEDS/Parquets/Dictionary/dictionary_lake.parquet")
STEP0_SAMPLE = Path("/Users/markjaysonfarol13/Higher Ed research/IPEDS/Paneled Datasets/Step0/finance_step0_long_2004.parquet")

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
        "tuition and fees, net" in s
        or "tuition and fees net" in s
        or "net tuition and fees" in s
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
    suspect_core = cw[mask_blank & label_str.str.contains(core_pattern, case=False, regex=True)]

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
    summarize which base_keys currently map to BS_ENDOWMENT_FMV and which base_keys
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
        endow = crosswalk[crosswalk["concept_key"] == "BS_ENDOWMENT_FMV"]
        if endow.empty:
            print("No BS_ENDOWMENT_FMV rows present in filled crosswalk.")
        else:
            cols = [c for c in ["form_family", "base_key", "year_start", "year_end"] if c in endow.columns]
            print("\nCrosswalk BS_ENDOWMENT_FMV entries:")
            if cols:
                preview = endow[cols].drop_duplicates().sort_values(cols)
                print(preview.to_string(index=False))
            fam_counts = endow["form_family"].value_counts(dropna=False)
            print("\nCounts by form_family:")
            print(fam_counts.to_string())

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

    endow = cw[cw["concept_key"] == "BS_ENDOWMENT_FMV"]
    if not endow.empty:
        print("\nBS_ENDOWMENT_FMV mappings by form_family:")
        print(endow["form_family"].value_counts(dropna=False))
        preview_cols = ["form_family", "base_key", "year_start", "year_end"]
        preview_cols = [c for c in preview_cols if c in endow.columns]
        if preview_cols:
            preview = (
                endow[preview_cols]
                .drop_duplicates()
                .sort_values(preview_cols)
                .head(20)
            )
            print("\nSample of BS_ENDOWMENT_FMV rows:")
            print(preview.to_string(index=False))
    else:
        print("\nWARNING: no BS_ENDOWMENT_FMV mappings found in crosswalk.")

    mapped_nonblank = cw[cw["concept_key"].astype(str).str.strip().ne("")]
    if not mapped_nonblank.empty:
        _assert_no_overlaps(mapped_nonblank, ("form_family", "base_key", "concept_key"))

    ck_series = cw["concept_key"].astype(str).str.strip()
    missing_mask = ck_series.eq("") | ck_series.str.lower().eq("nan")
    if missing_mask.any():
        print("ERROR: Finance crosswalk still has rows without concept_key. Sample:")
        print(cw.loc[missing_mask, ["form_family", "base_key", "year_start", "year_end", "source_label_norm"]].head(10).to_string(index=False))
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
