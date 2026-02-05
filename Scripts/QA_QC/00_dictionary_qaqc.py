#!/usr/bin/env python3
"""
Dictionary QA/QC helper:
- Builds a collapsed code-label table across years (varnumber + codevalue).
- Writes source_file -> years coverage summary.
- Prints key counts (unique varname/varnumber, blanks, source_file count).
"""
from __future__ import annotations

import argparse
import os
from pathlib import Path

import pandas as pd

BASE = Path(os.environ.get("IPEDS_ROOT", Path(__file__).resolve().parents[2]))
ARTIFACTS = BASE / "Artifacts"
DEFAULT_CODES = ARTIFACTS / "Dictionary" / "dictionary_codes.parquet"
DEFAULT_COLLAPSED = ARTIFACTS / "Dictionary" / "dictionary_codes_collapsed.csv"
DEFAULT_SOURCE_YEARS = ARTIFACTS / "Dictionary" / "source_file_years.csv"
DEFAULT_QAQC = ARTIFACTS / "Dictionary" / "dictionary_codes_qaqc.csv"


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--codes", type=Path, default=DEFAULT_CODES)
    p.add_argument("--collapsed-out", type=Path, default=DEFAULT_COLLAPSED)
    p.add_argument("--source-years-out", type=Path, default=DEFAULT_SOURCE_YEARS)
    p.add_argument("--qaqc-out", type=Path, default=DEFAULT_QAQC)
    p.add_argument("--year-sep", default="|", help="Separator for year lists")
    p.add_argument(
        "--excel-text",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Prefix year list with apostrophe for Excel",
    )
    return p.parse_args()


def pick_mode(s: pd.Series) -> str:
    s = s[s != ""]
    return s.mode().iat[0] if not s.empty else ""


def years_text(series: pd.Series, sep: str, excel_text: bool) -> str:
    yrs = sorted({str(int(y)) for y in series.dropna()})
    joined = sep.join(yrs)
    return f"'{joined}" if excel_text else joined


def normalize_source_label(label: str) -> str:
    """Remove year spans from source labels to keep a stable label."""
    if not label:
        return ""
    txt = str(label)
    # remove boilerplate "File Documentation for (the) "
    txt = pd.Series([txt]).str.replace(r"(?i)\bfile documentation for (the )?\b", "", regex=True).iat[0]
    # remove common year spans like 2014-15, 2023-24
    txt = pd.Series([txt]).str.replace(r"\b20\d{2}\s*[-/]\s*\d{2}\b", "", regex=True).iat[0]
    # remove trailing commas/extra whitespace
    txt = pd.Series([txt]).str.replace(r"\s+,", ",", regex=True).iat[0]
    txt = pd.Series([txt]).str.replace(r"\s+", " ", regex=True).iat[0].strip(" ,")
    return txt


def main() -> None:
    args = parse_args()
    if not args.codes.exists():
        raise SystemExit(f"Missing codes file: {args.codes}")

    df = pd.read_parquet(args.codes)
    # normalize columns
    for col in ("varnumber", "varname", "varTitle", "codevalue", "valuelabel", "source_file", "source_file_label"):
        if col not in df.columns:
            df[col] = ""
        df[col] = df[col].fillna("").astype(str).str.strip()
    df["varname"] = df["varname"].str.upper()

    # QA/QC counts
    total_rows = len(df)
    unique_varname = df["varname"].replace("", pd.NA).nunique(dropna=True)
    unique_varnumber = df["varnumber"].replace("", pd.NA).nunique(dropna=True)
    blank_varname = int((df["varname"] == "").sum())
    unique_source_file = df["source_file"].replace("", pd.NA).nunique(dropna=True)
    unique_source_file_label = df["source_file_label"].replace("", pd.NA).nunique(dropna=True)

    multi_varname = (
        df[df["varnumber"] != ""]
        .groupby("varnumber")["varname"]
        .nunique(dropna=True)
    )
    varnumber_multi_varname = int((multi_varname > 1).sum())

    qaqc = pd.DataFrame(
        [
            {
                "total_rows": total_rows,
                "unique_varname": unique_varname,
                "unique_varnumber": unique_varnumber,
                "blank_varname_rows": blank_varname,
                "unique_source_file": unique_source_file,
                "unique_source_file_label": unique_source_file_label,
                "varnumber_multi_varname": varnumber_multi_varname,
            }
        ]
    )
    args.qaqc_out.parent.mkdir(parents=True, exist_ok=True)
    qaqc.to_csv(args.qaqc_out, index=False)

    # collapsed codes by varnumber + codevalue
    cdf = df[df["varnumber"] != ""].copy()
    collapsed = (
        cdf.groupby(["varnumber", "codevalue"], dropna=False)
        .agg(
            varname=("varname", pick_mode),
            varTitle=("varTitle", pick_mode),
            valuelabel=("valuelabel", pick_mode),
            source_file=("source_file", pick_mode),
            source_file_label=("source_file_label", pick_mode),
            years=("year", lambda x: years_text(x, args.year_sep, args.excel_text)),
        )
        .reset_index()[["varnumber", "varname", "varTitle", "codevalue", "valuelabel", "source_file", "source_file_label", "years"]]
    )
    args.collapsed_out.parent.mkdir(parents=True, exist_ok=True)
    collapsed.to_csv(args.collapsed_out, index=False)

    # source_file years coverage
    sf_df = df[df["source_file"] != ""].copy()
    sf_df["source_file_label_norm"] = sf_df["source_file_label"].apply(normalize_source_label)
    sf_years = (
        sf_df.groupby("source_file")
        .agg(
            source_file_label=("source_file_label_norm", pick_mode),
            years=("year", lambda x: years_text(x, args.year_sep, args.excel_text)),
        )
        .reset_index()
    )
    args.source_years_out.parent.mkdir(parents=True, exist_ok=True)
    sf_years.to_csv(args.source_years_out, index=False)

    print("QA/QC written:", args.qaqc_out)
    print("Collapsed codes:", args.collapsed_out)
    print("Source file years:", args.source_years_out)


if __name__ == "__main__":
    main()
