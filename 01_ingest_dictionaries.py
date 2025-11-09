#!/usr/bin/env python3
"""
Build a consolidated "dictionary lake" from every IPEDS data dictionary.

The resulting dictionary_lake.parquet makes it easy to search for variables
across years, survey components, and accounting forms.
"""

from __future__ import annotations

import argparse
import re
import sys
from pathlib import Path
from typing import Iterable

try:  # pylint: disable=wrong-import-position
    import pandas as pd
except ImportError as exc:  # pragma: no cover - startup guard
    sys.stderr.write(
        "pandas/openpyxl/xlrd missing. Run: source .venv/bin/activate && pip install -r requirements.txt\n"
    )
    raise

ROOT = Path("/Users/markjaysonfarol13/Higher Ed research/IPEDS/Cross sectional Datas")
DEFAULT_OUTPUT = Path("dictionary_lake.parquet")
DICT_NAME_PATTERN = re.compile(
    r"(?:^|[/_-])(dict|dictionary|varlist|variables?|layout|codebook)(?:$|[_-])",
    re.IGNORECASE,
)
SUPPORTED_SUFFIXES = {".xlsx", ".xls", ".csv", ".txt"}
VAR_PREFIX_RE = re.compile(
    r"^(F[123]A|EFFY|EFIA?|EFIB|EFIC|EFID|E1D|OM|HR|IC|SFA|GRS?|PE|AL|ADM|HD|C)",
    re.IGNORECASE,
)

VAR_CANDIDATES = (
    "varname",
    "variable",
    "var",
    "var_name",
    "name",
    "column",
)
LABEL_CANDIDATES = (
    "varlabel",
    "variable label",
    "variable_label",
    "label",
    "description",
    "vartitle",
    "var_title",
    "var title",
    "longdescription",
    "long description",
    "valuelabel",
)


def report_duplicate_modules() -> None:
    repo_root = Path(__file__).resolve().parent
    targets = {
        "01_ingest_dictionaries.py": Path(__file__).resolve(),
        "harmonize_new.py": repo_root / "harmonize_new.py",
        "concept_catalog.py": repo_root / "concept_catalog.py",
    }
    for name, canonical in targets.items():
        canonical_path = canonical.resolve()
        matches = [p.resolve() for p in repo_root.rglob(name)]
        duplicates = [
            p
            for p in matches
            if p != canonical_path and ".venv" not in p.parts and "__pycache__" not in p.parts
        ]
        for dup in duplicates:
            print(f"REMOVE_AFTER_REVIEW duplicate module found: {dup}")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--root",
        type=Path,
        default=ROOT,
        help="Root directory containing yearly IPEDS downloads "
        f"(default: {ROOT})",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=DEFAULT_OUTPUT,
        help="Output parquet file (default: dictionary_lake.parquet)",
    )
    return parser.parse_args()


def first_match(columns: Iterable[str], candidates: Iterable[str]) -> str | None:
    normalized = {c.lower().strip(): c for c in columns}
    for key in candidates:
        if key in normalized:
            return normalized[key]
    return None


def read_any_dictionary(path: Path) -> pd.DataFrame:
    suffix = path.suffix.lower()

    def extract_columns(df: pd.DataFrame) -> pd.DataFrame | None:
        if df is None or df.empty:
            return None
        var_col = first_match(df.columns, VAR_CANDIDATES)
        label_col = first_match(df.columns, LABEL_CANDIDATES)
        if var_col and label_col:
            out = df[[var_col, label_col]].copy()
            out.columns = ["source_var", "source_label"]
            return out
        return None

    if suffix in {".xlsx", ".xls"}:
        engine = "openpyxl" if suffix == ".xlsx" else "xlrd"
        xls = pd.ExcelFile(path, engine=engine)
        extracted = None
        for sheet in xls.sheet_names:
            candidate = extract_columns(xls.parse(sheet_name=sheet, dtype=str))
            if candidate is not None:
                extracted = candidate
                break
        if extracted is None:
            df = xls.parse(sheet_name=0, dtype=str)
            extracted = extract_columns(df) or df.iloc[:, :2].copy()
            extracted.columns = ["source_var", "source_label"]
        return extracted

    if suffix == ".csv":
        df = pd.read_csv(path, dtype=str, encoding_errors="ignore")
        extracted = extract_columns(df)
        if extracted is None:
            extracted = df.iloc[:, :2].copy()
            extracted.columns = ["source_var", "source_label"]
        return extracted
    if suffix == ".txt":
        try:
            df = pd.read_csv(path, dtype=str, sep=None, engine="python", encoding_errors="ignore")
        except Exception:
            try:
                df = pd.read_csv(path, dtype=str, sep="|", encoding_errors="ignore")
            except Exception:
                df = pd.read_csv(path, dtype=str, delim_whitespace=True, encoding_errors="ignore")
        extracted = extract_columns(df)
        if extracted is None:
            extracted = df.iloc[:, :2].copy()
            extracted.columns = ["source_var", "source_label"]
        return extracted

    # Fallback: let pandas attempt to read anything unknown
    engine = "openpyxl" if suffix == ".xlsx" else "xlrd"
    df = pd.read_excel(path, sheet_name=0, dtype=str, engine=engine)
    extracted = extract_columns(df)
    if extracted is None:
        extracted = df.iloc[:, :2].copy()
        extracted.columns = ["source_var", "source_label"]
    return extracted


def derive_prefix(path: Path) -> str:
    match = re.search(
        r"(F[123]A|EFFY|EF|E1D|OM|HR|IC|SFA|GRS?|PE|AL|ADM|HD|C)[_-]?",
        path.stem.upper(),
    )
    return match.group(1) if match else ""


def derive_release(metadata: str) -> str:
    text = metadata.lower()
    if "revised" in text or "_rv" in text:
        return "revised"
    if "provisional" in text:
        return "provisional"
    if "final" in text:
        return "final"
    return ""


def looks_like_dictionary(path: Path) -> bool:
    """Return True if the file or its parent folder appears to be a dictionary."""
    return bool(
        DICT_NAME_PATTERN.search(path.name)
        or DICT_NAME_PATTERN.search(path.parent.name)
    )


def iter_dictionary_files(year_dir: Path) -> Iterable[Path]:
    """Yield candidate dictionary files under a given year directory."""
    for path in year_dir.rglob("*"):
        if not path.is_file():
            continue
        if path.suffix.lower() not in SUPPORTED_SUFFIXES:
            continue
        if looks_like_dictionary(path):
            yield path


def main() -> None:
    args = parse_args()
    report_duplicate_modules()
    root = args.root
    if not root.exists():
        sys.exit(f"Root directory not found: {root}")

    rows: list[pd.DataFrame] = []
    for year_dir in sorted(p for p in root.iterdir() if p.is_dir() and p.name.isdigit()):
        year = int(year_dir.name)
        for path in iter_dictionary_files(year_dir):
            try:
                df = read_any_dictionary(path)
            except Exception as exc:  # noqa: BLE001
                print(f"SKIP {path} ({exc})")
                continue

            df = df.dropna(how="all", subset=["source_var", "source_label"])
            if df.empty:
                continue

            df["year"] = year
            df["dict_file"] = str(path)
            df["filename"] = path.name
            df["prefix_hint"] = (
                df["source_var"]
                .astype(str)
                .str.extract(VAR_PREFIX_RE, expand=False)
                .str.upper()
                .fillna("")
            )
            fallback = derive_prefix(path)
            if fallback:
                df.loc[df["prefix_hint"].eq(""), "prefix_hint"] = fallback
            df["release"] = derive_release(path.name)
            survey_hint = re.findall(r"/([A-Z]{1,4})[_-]", "/" + path.stem.upper() + "_")
            df["survey_hint"] = survey_hint[0] if survey_hint else ""
            rows.append(df)

    if not rows:
        sys.exit("No dictionary files found. Did you run the downloader?")

    lake = pd.concat(rows, ignore_index=True)
    lake["source_label_norm"] = (
        lake["source_label"]
        .fillna("")
        .str.strip()
        .str.lower()
        .str.replace(r"[\u2010\u2011\u2012\u2013\u2014\u2212]", "-", regex=True)
        .str.replace(r"[•·]", " ", regex=True)
        .str.replace(r"[&]", " and ", regex=True)
        .str.replace(r"[^\w\s\(\)/%-]", "", regex=True)
        .str.replace(r"\s+", " ", regex=True)
        .str.strip()
    )

    lake.to_parquet(args.output, index=False)
    print(f"Wrote {len(lake):,} rows to {args.output}")


if __name__ == "__main__":
    main()
