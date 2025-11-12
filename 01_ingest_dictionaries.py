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
import json
import hashlib

try:  # pylint: disable=wrong-import-position
    import pandas as pd
except ImportError as exc:  # pragma: no cover - startup guard
    sys.stderr.write(
        "pandas/openpyxl/xlrd missing. Run: source .venv/bin/activate && pip install -r requirements.txt\n"
    )
    raise

ROOT = Path("/Users/markjaysonfarol13/Higher Ed research/IPEDS/Cross sectional Datas")
DEFAULT_OUTPUT = Path("/Users/markjaysonfarol13/Higher Ed research/IPEDS/Parquets/dictionary_lake.parquet")
DICT_NAME_PATTERN = re.compile(
    r"(?:^|[/_-])(dict|dictionary|varlist|variables?|layout|codebook)(?:$|[_-])",
    re.IGNORECASE,
)
SUPPORTED_SUFFIXES = {".xlsx", ".xls", ".csv", ".txt"}
VAR_PREFIX_RE = re.compile(
    r"^(F[123]A|EFFY|EFIA?|EFIB|EFIC|EFID|E1D|OM|HR|IC|SFA|GRS?|PE|AL|ADM|HD|C)",
    re.IGNORECASE,
)
RE_FILE = re.compile(
    r"(?i)(?P<prefix>[a-z]{1,4})?"
    r"(?P<y1>\d{2})(?P<y2>\d{2})?"
    r"(?:[_-])?(?P<suffix>[a-z0-9]+)?"
)
SURVEY_HINT_TOKENS = {
    "ef": "EF",
    "efia": "E12",
    "e12": "E12",
    "effy": "E12",
    "e1d": "E12",
    "f1a": "F1A",
    "f2a": "F2A",
    "f3a": "F3A",
    "f": "F",
    "ic": "IC",
    "hd": "HD",
    "adm": "ADM",
    "gr": "GR",
    "grs": "GRS",
    "pe": "PE",
    "om": "OM",
    "sfa": "SFA",
    "al": "AL",
    "c": "C",
}
UNICODE_HYPHENS = r"[\u2010\u2011\u2012\u2013\u2014\u2015\u2212]"

VAR_CANDIDATES = (
    "varname",
    "variable",
    "var",
    "var_name",
    "name",
    "unitid",
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
    "unique identification number of the institution",
)
TABLE_CANDIDATES = (
    "table",
    "tablenm",
    "worksheet",
    "sheet",
    "tab",
    "section",
)
DATAFILE_CANDIDATES = (
    "data_file",
    "datafile",
    "data filename",
    "filename",
    "file",
    "dataset",
)

SURVEY_HINT_BY_PREFIX = {
    "F1A": "Finance",
    "F2A": "Finance",
    "F3A": "Finance",
    "F1": "Finance",
    "F2": "Finance",
    "F3": "Finance",
    "EF": "FallEnrollment",
    "EFIA": "12MonthEnrollment",
    "EFIB": "12MonthEnrollment",
    "EFIC": "12MonthEnrollment",
    "EFID": "12MonthEnrollment",
    "E1D": "12MonthEnrollment",
    "EFFY": "12MonthEnrollment",
    "IC": "InstitutionalCharacteristics",
    "HD": "InstitutionalCharacteristics",
    "SFA": "StudentFinancialAid",
    "OM": "OutcomeMeasures",
    "HR": "HumanResources",
    "GR": "GraduationRates",
    "GRS": "GraduationRates",
    "PE": "GraduationRates",
    "ADM": "Admissions",
    "AL": "AcademicLibraries",
    "C": "Completions",
}


def _normalize_label_text(value: object) -> str:
    if value is None:
        return ""
    text = str(value)
    text = re.sub(UNICODE_HYPHENS, "-", text)
    text = text.replace("•", " ").replace("&", " and ")
    text = re.sub(r"[“”\"'`]", "", text)
    text = re.sub(r"[(){}\[\]]", " ", text)
    text = re.sub(r"[;:.,]", " ", text)
    text = re.sub(r"\s+", " ", text).strip().lower()
    return text


def normalize_label(series: pd.Series) -> pd.Series:
    return series.map(_normalize_label_text)


def extract_columns(df: pd.DataFrame) -> pd.DataFrame | None:
    if df is None or df.empty:
        return None
    var_col = first_match(df.columns, VAR_CANDIDATES)
    label_col = first_match(df.columns, LABEL_CANDIDATES)
    if var_col and label_col:
        table_col = first_match(df.columns, TABLE_CANDIDATES)
        data_col = first_match(df.columns, DATAFILE_CANDIDATES)
        out = pd.DataFrame(
            {
                "source_var": df[var_col],
                "source_label": df[label_col],
                "table_name": df[table_col] if table_col else pd.NA,
                "data_filename": df[data_col] if data_col else pd.NA,
            }
        )
        return out
    return None


def read_txt(path: Path) -> pd.DataFrame | None:
    attempts = (
        dict(sep=None, engine="python"),
        dict(sep="|"),
        dict(delim_whitespace=True),
    )
    for kwargs in attempts:
        try:
            return pd.read_csv(path, dtype=str, encoding_errors="ignore", low_memory=False, **kwargs)
        except Exception:
            continue
    return None


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


def parse_file_meta(path: Path) -> dict | None:
    match = RE_FILE.search(path.stem)
    if not match:
        return None
    gd = match.groupdict()
    year_token = gd.get("y2") or gd.get("y1")
    if not year_token:
        return None
    year = int(f"20{year_token}")
    prefix_token = (gd.get("prefix") or "").lower()
    suffix = (gd.get("suffix") or "").lower()
    hint = SURVEY_HINT_TOKENS.get(prefix_token) or SURVEY_HINT_TOKENS.get(suffix) or prefix_token.upper()
    return {"year": year, "prefix_token": prefix_token.upper(), "survey_hint": hint}


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
        df = pd.read_csv(path, dtype=str, encoding_errors="ignore", low_memory=False)
        extracted = extract_columns(df)
        if extracted is None:
            extracted = df.iloc[:, :2].copy()
            extracted.columns = ["source_var", "source_label"]
        return extracted
    if suffix == ".txt":
        df = read_txt(path)
        if df is None:
            raise ValueError(f"Unable to parse TXT dictionary {path}")
        extracted = extract_columns(df)
        if extracted is None:
            extracted = df.iloc[:, :2].copy()
            extracted.columns = ["source_var", "source_label"]
        return extracted
    # Fallback to Excel reader for anything else
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


def map_survey_hint(prefix_hint: str, fallback: str) -> str:
    prefix_upper = (prefix_hint or "").upper()
    if prefix_upper in SURVEY_HINT_BY_PREFIX:
        return SURVEY_HINT_BY_PREFIX[prefix_upper]
    for key, value in SURVEY_HINT_BY_PREFIX.items():
        if prefix_upper.startswith(key):
            return value
    return fallback


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

            meta = parse_file_meta(path) or {}
            df["year"] = meta.get("year", year)
            df["dict_file"] = str(path)
            df["dict_filename"] = path.name
            df["filename"] = path.name
            for col in ("table_name", "data_filename"):
                if col not in df.columns:
                    df[col] = pd.NA
            df["source_var"] = df["source_var"].astype(str).str.strip()
            df["source_label"] = df["source_label"].astype(str)
            df["source_label_norm"] = normalize_label(df["source_label"])
            df["source_var_norm"] = df["source_var"].str.lower()
            df["table_name"] = df["table_name"].astype(str)
            df["table_name_norm"] = df["table_name"].str.strip().str.lower()
            df["data_filename"] = df["data_filename"].astype(str)
            df["prefix_hint"] = (
                df["source_var"]
                .astype(str)
                .str.extract(VAR_PREFIX_RE, expand=False)
                .str.upper()
                .fillna("")
            )
            meta_prefix = meta.get("prefix_token", "")
            if meta_prefix:
                df.loc[df["prefix_hint"].eq(""), "prefix_hint"] = meta_prefix.upper()
            fallback = derive_prefix(path)
            if fallback:
                df.loc[df["prefix_hint"].eq(""), "prefix_hint"] = fallback
            df["prefix_token"] = df["prefix_hint"]
            df["release"] = derive_release(path.name)
            path_hint = re.findall(r"/([A-Z]{1,4})[_-]", "/" + path.stem.upper() + "_")
            fallback_hint = meta.get("survey_hint") or (path_hint[0] if path_hint else "")
            df["survey_hint"] = df["prefix_hint"].apply(lambda p: map_survey_hint(p, fallback_hint))
            df["dict_row_sha256"] = (
                df["source_var_norm"].fillna("")
                + "|"
                + df["source_label_norm"].fillna("")
                + "|"
                + df["table_name_norm"].fillna("")
            ).map(lambda s: hashlib.sha256(s.encode("utf-8")).hexdigest())
            rows.append(df)

    if not rows:
        sys.exit("No dictionary files found. Did you run the downloader?")

    lake = pd.concat(rows, ignore_index=True)
    lake["source_label_norm"] = normalize_label(lake.get("source_label"))
    lake["source_var"] = lake["source_var"].astype(str).str.strip()
    lake["source_var_norm"] = lake["source_var"].str.lower()
    lake["dict_file"] = lake["dict_file"].astype(str)
    if "dict_filename" not in lake.columns:
        lake["dict_filename"] = lake["filename"]
    lake["dict_filename"] = lake["dict_filename"].astype(str)
    lake["filename"] = lake["filename"].astype(str)
    lake["prefix_hint"] = lake["prefix_hint"].fillna("").astype(str).str.upper()
    lake["survey_hint"] = lake["survey_hint"].fillna("").astype(str)
    lake["release"] = lake["release"].fillna("").astype(str)
    lake["year"] = pd.to_numeric(lake["year"], errors="coerce").astype("Int64")
    if "table_name" not in lake.columns:
        lake["table_name"] = pd.NA
    lake["table_name"] = lake["table_name"].astype(str)
    lake["table_name_norm"] = lake["table_name"].str.strip().str.lower()
    if "data_filename" not in lake.columns:
        lake["data_filename"] = pd.NA
    lake["data_filename"] = lake["data_filename"].astype(str)
    if "dict_row_sha256" not in lake.columns:
        lake["dict_row_sha256"] = (
            lake["source_var_norm"].fillna("")
            + "|"
            + lake["source_label_norm"].fillna("")
            + "|"
            + lake["table_name_norm"].fillna("")
        ).map(lambda s: hashlib.sha256(s.encode("utf-8")).hexdigest())

    required_cols = [
        "year",
        "source_var",
        "source_label",
        "source_label_norm",
        "source_var_norm",
        "table_name",
        "table_name_norm",
        "data_filename",
        "dict_file",
        "dict_filename",
        "filename",
        "dict_row_sha256",
        "release",
        "prefix_hint",
        "survey_hint",
    ]
    missing = [col for col in required_cols if col not in lake.columns]
    if missing:
        raise RuntimeError(f"Dictionary lake missing required columns: {missing}")

    dedup_key = ["year", "dict_row_sha256"]
    dup_rows = lake[lake.duplicated(dedup_key, keep=False)].copy()
    lake = (
        lake.sort_values(["year", "survey_hint", "prefix_hint", "dict_file", "source_var"])
        .drop_duplicates(dedup_key, keep="first")
        .reset_index(drop=True)
    )

    args.output.parent.mkdir(parents=True, exist_ok=True)
    lake.to_parquet(args.output, index=False, compression="snappy")
    print(f"Wrote {len(lake):,} rows to {args.output}")

    if not dup_rows.empty:
        dup_path = args.output.with_name("dictionary_lake_duplicates.csv")
        dup_rows.sort_values(dedup_key).to_csv(dup_path, index=False)
        print(f"Duplicate rows written to {dup_path}")
        dup_rows.to_csv(args.output.with_name("dictionary_duplicates.csv"), index=False)

    top_labels = (
        lake.groupby(["year", "survey_hint", "source_label_norm"], dropna=False)
        .size()
        .reset_index(name="count")
    )
    top_labels = (
        top_labels.sort_values(["year", "survey_hint", "count"], ascending=[True, True, False])
        .groupby(["year", "survey_hint"], as_index=False)
        .head(25)
    )
    top_path = args.output.with_name("dictionary_lake_top_labels.csv")
    top_labels.to_csv(top_path, index=False)
    print(f"Top label summary written to {top_path}")

    profile = (
        lake.groupby(["year", "survey_hint", "prefix_hint", "dict_file"], dropna=False)
        .size()
        .reset_index(name="row_count")
    )
    profile_csv_path = args.output.with_name("dictionary_profile.csv")
    profile.to_csv(profile_csv_path, index=False)
    profile_path = args.output.with_name("dictionary_lake_columns_profile.json")
    profile.to_json(profile_path, orient="records", indent=2)
    print(f"Dictionary profile written to {profile_path}")


if __name__ == "__main__":
    main()
