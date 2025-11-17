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

    df["concept_key"] = df["concept_key"].astype("object")

    df["source_var"] = df["source_var"].astype(str).str.strip().str.upper()

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

    used_keys = set(df.loc[already_filled, "concept_key"].astype(str).str.strip().tolist())
    used_keys.discard("")

    filled_counts = {"net_price": 0, "label_slug": 0}
    for idx, row in to_fill.iterrows():
        raw_var = row.get("source_var", "")
        source_var = str(raw_var or "").strip().upper() or f"ROW_{idx}"
        concept = infer_concept_key(row.get("label"), source_var)
        source = "net_price"
        if not concept:
            concept = slug_from_label(row.get("label"), source_var, used_keys)
            source = "label_slug"
        if concept in used_keys and source == "net_price":
            # Ensure uniqueness for heuristics that return static names
            concept = slug_from_label(row.get("label"), source_var, used_keys)
            source = "label_slug"
        df.at[idx, "concept_key"] = concept
        df.at[idx, "concept_key_source"] = source
        used_keys.add(concept)
        filled_counts[source] += 1

    if filled_counts["net_price"] or filled_counts["label_slug"]:
        logging.info(
            "Auto-filled concept_key for %d rows (net_price patterns=%d, label slugs=%d).",
            filled_counts["net_price"] + filled_counts["label_slug"],
            filled_counts["net_price"],
            filled_counts["label_slug"],
        )
    else:
        logging.info("No additional concept_key rows were filled.")

    # Basic summary by concept_key
    filled_summary = df["concept_key"].astype(str).str.strip().value_counts(dropna=True).sort_index()
    logging.info("Resulting concept_key distribution:\n%s", filled_summary)

    if (df["concept_key"].astype(str).str.strip() == "").any():
        raise RuntimeError("Some SFA rows still lack concept_key assignments after auto-fill.")

    # Write output
    output_csv.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(output_csv, index=False)
    logging.info("Saved filled SFA crosswalk to %s", output_csv)


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
