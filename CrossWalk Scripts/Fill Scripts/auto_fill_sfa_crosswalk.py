#!/usr/bin/env python3
"""
Auto-fill concept_key for SFA crosswalk template rows using label-based heuristics.

- Targets IPEDS Net Price (NPT) income bins in the SFA dictionary.
- Maps:
    income 0–30,000         -> NET_PRICE_AVG_INC_0_30K
    income 30,001–48,000    -> NET_PRICE_AVG_INC_30_48K
    income 48,001–75,000    -> NET_PRICE_AVG_INC_48_75K
    income 75,001–110,000   -> NET_PRICE_AVG_INC_75_110K
    income over 110,000     -> NET_PRICE_AVG_INC_110K_PLUS

- Leaves rows like SFAFORM (collection form type) with blank concept_key,
  because they are categorical / not part of your numeric SFA concept panel.

Default input:
  /Users/markjaysonfarol13/Higher Ed research/IPEDS/Paneled Datasets/Crosswalks/sfa_crosswalk_template.csv

Default output:
  /Users/markjaysonfarol13/Higher Ed research/IPEDS/Paneled Datasets/Crosswalks/Filled/sfa_crosswalk_filled.csv
"""

from __future__ import annotations

import argparse
import hashlib
import logging
import re
from pathlib import Path
from typing import Optional

import pandas as pd


# Base paths
CROSSWALK_DIR = Path(
    "/Users/markjaysonfarol13/Higher Ed research/IPEDS/Paneled Datasets/Crosswalks"
)
FILLED_DIR = CROSSWALK_DIR / "Filled"

TOKEN_RE = re.compile(r"[A-Z0-9]+")
YEAR_RE = re.compile(r"^(19|20)\d{2}$")
STOPWORDS = {
    "AVERAGE",
    "AMOUNT",
    "NUMBER",
    "PERCENT",
    "PERCENTAGE",
    "OF",
    "FOR",
    "THE",
    "A",
    "AN",
    "AND",
    "OR",
    "WITH",
    "WITHOUT",
    "STUDENTS",
    "UNDERGRADUATES",
    "RECEIVING",
    "RECEIVED",
    "AWARDED",
    "TOTAL",
    "TOTALS",
    "FULLTIME",
    "FULL",
    "TIME",
    "FTFT",
    "FIRSTTIME",
    "FIRST",
    "DEGREE",
    "CERTIFICATE",
    "SEEKING",
    "TITLE",
    "IV",
    "INCOME",
    "COHORT",
    "AMT",
    "FORM",
    "PART",
    "PARTS",
    "NET",
    "PRICE",
    "AVERAGE",
    "MIDPOINT",
    "IN",
    "OUT",
    "LESS",
    "MORE",
    "THAN",
    "TO",
    "BY",
    "INSTITUTIONAL",
    "FEDERAL",
    "STATE",
    "LOCAL",
    "GRANT",
    "GRANTS",
    "AID",
    "ASSISTANCE",
    "LOAN",
    "LOANS",
    "PELL",
    "STAFFORD",
    "PLUS",
    "SCHOLARSHIP",
    "SCHOLARSHIPS",
    "PUBLIC",
    "PRIVATE",
    "MILITARY",
    "VETERAN",
}
MAX_SLUG_TOKENS = 8
MAX_CONCEPT_LEN = 96


def infer_concept_key(label: Optional[str], source_var: Optional[str]) -> Optional[str]:
    """
    Infer a concept_key from the label and source_var.

    Right now this is focused on Net Price income bins, using label text,
    so it will work regardless of whether the NPT varnames are NPT410/411/412
    or some future naming (as long as labels keep the income brackets).
    """
    label_u = (label or "").upper()
    var_u = (source_var or "").upper()

    # Net price income bins
    if "AVERAGE NET PRICE" in label_u:
        if "INCOME 0-30,000" in label_u:
            return "NET_PRICE_AVG_INC_0_30K"
        if "INCOME 30,001-48,000" in label_u:
            return "NET_PRICE_AVG_INC_30_48K"
        if "INCOME 48,001-75,000" in label_u:
            return "NET_PRICE_AVG_INC_48_75K"
        if "INCOME 75,001-110,000" in label_u:
            return "NET_PRICE_AVG_INC_75_110K"
        # Some documentation uses "over 110,000", some might say "110,001+"
        if "INCOME OVER 110,000" in label_u or "INCOME 110,001" in label_u:
            return "NET_PRICE_AVG_INC_110K_PLUS"

    # Example: you could add more patterns later, e.g. Pell, loans, FTFT, GI Bill.
    # For now, anything that is not a clearly identified net price bin will fall through.
    return None


def tokenize_label(label: Optional[str]) -> list[str]:
    if not label:
        return []
    text = str(label).upper().replace("\\n", " ").replace("\n", " ").replace("\r", " ")
    tokens = TOKEN_RE.findall(text)
    cleaned: list[str] = []
    for tok in tokens:
        if not tok or tok in STOPWORDS:
            continue
        if YEAR_RE.match(tok):
            continue
        if tok.isdigit():
            continue
        cleaned.append(tok)
    return cleaned


def slug_from_label(label: Optional[str], source_var: str, used_keys: set[str]) -> str:
    tokens = tokenize_label(label)
    base_tokens = tokens[:MAX_SLUG_TOKENS]
    if base_tokens:
        slug = "SFA_" + "_".join(base_tokens)
    else:
        slug = f"SFA_VAR_{source_var}"
    slug = re.sub(r"_+", "_", slug).strip("_")
    if not slug.startswith("SFA_"):
        slug = f"SFA_{slug}"
    slug = slug[:MAX_CONCEPT_LEN]

    candidate = slug
    suffix = f"_VAR_{source_var}"
    if candidate in used_keys and not candidate.endswith(suffix):
        candidate = f"{candidate}_{suffix}".strip("_")
    while candidate in used_keys:
        fingerprint = hashlib.sha1(f"{candidate}|{source_var}".encode("utf-8")).hexdigest()[:6].upper()
        candidate = f"{slug}_H{fingerprint}"
    return candidate


def build_var_concept_map(df: pd.DataFrame, filled_mask: pd.Series) -> dict[str, str]:
    """Return existing source_var -> concept_key mappings for bootstrap."""
    mapping: dict[str, str] = {}
    if not filled_mask.any():
        return mapping

    filled = df.loc[filled_mask, ["source_var", "concept_key"]]
    for raw_var, ck in filled.itertuples(index=False, name=None):
        source_var = str(raw_var or "").strip().upper()
        concept = str(ck or "").strip()
        if source_var and concept:
            mapping[source_var] = concept
    return mapping


def auto_fill_concepts(
    input_csv: Path,
    output_csv: Path,
) -> None:
    logging.info("Loading SFA crosswalk template from %s", input_csv)
    df = pd.read_csv(input_csv)

    # Normalize concept_key and source_var to strings for safety
    if "concept_key" not in df.columns:
        raise KeyError("Expected column 'concept_key' in crosswalk template.")
    if "source_var" not in df.columns:
        raise KeyError("Expected column 'source_var' in crosswalk template.")
    if "label" not in df.columns:
        raise KeyError("Expected column 'label' in crosswalk template.")
    required = {"concept_key", "source_var", "label"}
    missing = required - set(df.columns)
    if missing:
        raise SystemExit(f"SFA template missing columns: {sorted(missing)}")

    df["concept_key"] = df["concept_key"].astype("object")

    df["source_var"] = df["source_var"].astype(str).str.strip().str.upper()
    if "survey" in df.columns:
        df["survey"] = df["survey"].astype(str).str.strip().str.upper()
    if "year_start" in df.columns:
        df["year_start"] = pd.to_numeric(df["year_start"], errors="coerce").astype("Int64")
    if "year_end" in df.columns:
        df["year_end"] = pd.to_numeric(df["year_end"], errors="coerce").astype("Int64")
    if {"year_start", "year_end"} <= set(df.columns):
        bad_range = df["year_start"] > df["year_end"]
        if bad_range.any():
            print("ERROR: SFA template has year_start > year_end. Fix template.")
            print(df.loc[bad_range, ["source_var", "year_start", "year_end"]].head(10).to_string(index=False))
            raise SystemExit(1)

    key_cols = ["source_var"]
    if "survey" in df.columns:
        key_cols.insert(0, "survey")
    if "year_start" in df.columns:
        key_cols.append("year_start")
    dup_mask = df.duplicated(key_cols, keep=False)
    if dup_mask.any():
        print(f"ERROR: Found {dup_mask.sum()} duplicate key rows in SFA template.")
        print(df.loc[dup_mask, key_cols + ["concept_key"]].head(10).to_string(index=False))
        raise SystemExit(1)
    print(f"SFA template rows: {len(df):,}")

    # Identify rows that are already filled (do not override)
    raw_ck = df["concept_key"]
    ck_str = raw_ck.astype(str)
    trimmed = ck_str.str.strip()
    empty_mask = raw_ck.isna() | trimmed.eq("") | trimmed.str.lower().eq("nan")
    already_filled = ~empty_mask

    df["concept_key_source"] = ""
    df.loc[already_filled, "concept_key_source"] = "existing"

    logging.info("Template has %d rows total.", len(df))
    logging.info("Rows with pre-filled concept_key: %d", already_filled.sum())

    # Apply inference only to rows without a concept_key
    to_fill_mask = ~already_filled
    to_fill = df.loc[to_fill_mask].copy()

    logging.info("Attempting to auto-fill concept_key for %d rows.", len(to_fill))

    # Map from source_var -> concept_key so each IPEDS var keeps one concept.
    var_to_concept = build_var_concept_map(df, already_filled)

    filled_counts = {"net_price": 0, "varname": 0}
    for idx, row in to_fill.iterrows():
        raw_var = row.get("source_var", "")
        source_var = str(raw_var or "").strip().upper() or f"ROW_{idx}"
        concept = infer_concept_key(row.get("label"), source_var)
        if concept:
            if source_var not in var_to_concept:
                var_to_concept[source_var] = concept
            df.at[idx, "concept_key"] = concept
            df.at[idx, "concept_key_source"] = "net_price"
            filled_counts["net_price"] += 1
            continue

        if source_var in var_to_concept:
            concept = var_to_concept[source_var]
        else:
            concept = f"SFA_VAR_{source_var}"
            var_to_concept[source_var] = concept

        df.at[idx, "concept_key"] = concept
        df.at[idx, "concept_key_source"] = "varname"
        filled_counts["varname"] += 1

    total_autofilled = filled_counts["net_price"] + filled_counts["varname"]
    logging.info(
        "Auto-filled concept_key for %d rows (net_price=%d, varname=%d).",
        total_autofilled,
        filled_counts["net_price"],
        filled_counts["varname"],
    )

    # Basic summary by concept_key
    filled_summary = df["concept_key"].astype(str).str.strip().value_counts(dropna=True).sort_index()
    logging.info("Resulting concept_key distribution:\n%s", filled_summary)

    ck_series = df["concept_key"].astype(str).str.strip()
    missing_mask = ck_series.eq("") | ck_series.str.lower().eq("nan")
    if missing_mask.any():
        print("ERROR: SFA autofill left blank concept_key rows. Showing sample:")
        print(df.loc[missing_mask, ["source_var", "label"]].head(10).to_string(index=False))
        raise SystemExit(1)

    if (df["concept_key"].astype(str).str.strip() == "").any():
        raise RuntimeError("Some SFA rows still lack concept_key assignments after auto-fill.")

    # Write output
    output_csv.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(output_csv, index=False)
    logging.info("Saved filled SFA crosswalk to %s", output_csv)
    total_rows = len(df)
    existing = int((df["concept_key_source"] == "existing").sum())
    net_price = filled_counts["net_price"]
    varname = filled_counts["varname"]
    print(f"SFA crosswalk rows: {total_rows:,}")
    print(f"Existing concept_key rows: {existing:,}")
    print(f"Autofilled rows: {total_rows - existing:,} (net_price={net_price}, varname={varname})")
    top_keys = df["concept_key"].value_counts().head(10)
    print("Top concept_keys:")
    for key, count in top_keys.items():
        print(f"  {key}: {count}")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Auto-fill concept_key values in the SFA crosswalk template."
    )
    parser.add_argument(
        "--input",
        type=Path,
        default=CROSSWALK_DIR / "sfa_crosswalk_template.csv",
        help="Path to the SFA crosswalk template CSV.",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=FILLED_DIR / "sfa_crosswalk_filled.csv",
        help="Destination for the filled SFA crosswalk CSV.",
    )
    parser.add_argument(
        "--log-level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        help="Logging level.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    logging.basicConfig(
        level=getattr(logging, args.log_level),
        format="%(levelname)s:%(name)s:%(message)s",
    )
    auto_fill_concepts(args.input, args.output)


if __name__ == "__main__":
    main()
