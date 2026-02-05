#!/usr/bin/env python3
"""
Build a lean IPEDS dictionary lake (2004–2024) with core metadata only.

Outputs:
- dictionary_lake.parquet: year, varnumber, varname, varTitle, longDescription,
  DataType, format, Fieldwidth, imputationvar
- dictionary_codes.parquet/csv: value labels from Frequencies/FrequenciesRV/Imputation sheets
"""

from __future__ import annotations

import argparse
import re
import sys
import os
from pathlib import Path
from typing import Tuple

import pandas as pd

BASE_ROOT = Path(os.environ.get("IPEDS_ROOT", Path(__file__).resolve().parents[1]))
ARTIFACTS = BASE_ROOT / "Artifacts"
ROOT = BASE_ROOT / "Raw_Cross_Section_Data"
DICT_PARQUET_PATH = ARTIFACTS / "Dictionary" / "dictionary_lake.parquet"
DICT_CODES_PARQUET_PATH = ARTIFACTS / "Dictionary" / "dictionary_codes.parquet"
DICT_CODES_CSV_PATH = ARTIFACTS / "Dictionary" / "dictionary_codes.csv"


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--root", type=Path, default=ROOT)
    p.add_argument("--min-year", type=int, default=2004)
    p.add_argument("--output", type=Path, default=DICT_PARQUET_PATH)
    p.add_argument("--output-csv", type=Path, default=ARTIFACTS / "Dictionary" / "dictionary_lake.csv")
    p.add_argument("--codes-output", type=Path, default=DICT_CODES_PARQUET_PATH)
    p.add_argument("--codes-output-csv", type=Path, default=DICT_CODES_CSV_PATH)
    return p.parse_args()


def normalize_varnumber(val: object) -> str:
    if val is None:
        return ""
    txt = re.sub(r"\s+", "", str(val))
    if txt.isdigit():
        return txt.zfill(8)
    return txt


def normalize_source_file(path: Path) -> str:
    """Normalize source filename for dictionary rows.
    - Strip years and digits from names (except GR200).
    - Return uppercase underscore-separated token.
    """
    stem = path.stem.upper()
    # Collapse separators to underscore
    stem = re.sub(r"[^A-Z0-9]+", "_", stem)
    # Keep GR200 as-is (only allowed numeric token)
    if "GR200" in stem:
        return "GR200"
    # Remove all digits
    stem = re.sub(r"\d+", "", stem)
    # Collapse multiple underscores and trim
    stem = re.sub(r"_+", "_", stem).strip("_")
    return stem


def extract_source_label(xls: pd.ExcelFile | None) -> str:
    """Extract a human-readable source label from the Introduction sheet (row 1)."""
    if xls is None:
        return ""
    intro_sheet = next((s for s in xls.sheet_names if s.strip().lower() == "introduction"), None)
    if not intro_sheet:
        intro_sheet = xls.sheet_names[0] if xls.sheet_names else None
    if not intro_sheet:
        return ""
    try:
        intro_df = xls.parse(sheet_name=intro_sheet, header=None, nrows=1)
        if intro_df.empty:
            return ""
        for val in intro_df.iloc[0].tolist():
            if val is None:
                continue
            txt = str(val).strip()
            if txt:
                return clean_source_label(txt)
    except Exception:
        return ""
    return ""


def clean_source_label(label: str) -> str:
    """Normalize source labels by removing years and boilerplate phrases."""
    if not label:
        return ""
    txt = str(label)
    # remove boilerplate "File Documentation for (the) "
    txt = re.sub(r"(?i)\bfile documentation for (the )?\b", "", txt)
    # remove year spans like 2014-15 or 2023-24
    txt = re.sub(r"\b20\d{2}\s*[-/]\s*\d{2}\b", "", txt)
    # collapse whitespace/punctuation
    txt = re.sub(r"\s+", " ", txt).strip(" ,")
    return txt


def find_varlist_sheet(xls: pd.ExcelFile) -> str | None:
    for s in xls.sheet_names:
        if s.lower() == "varlist":
            return s
    for s in xls.sheet_names:
        if "varlist" in s.lower():
            return s
    # heuristic: first sheet that has varname & vartitle-ish columns
    for s in xls.sheet_names:
        try:
            preview = xls.parse(sheet_name=s, nrows=5, dtype=str)
        except Exception:
            continue
        cols = {c.strip().lower(): c for c in preview.columns}
        has_var = any(k in cols for k in ("varname", "var_name", "name"))
        has_title = any(k in cols for k in ("vartitle", "var_title", "var title", "title"))
        if has_var and has_title:
            return s
    return None


def col(df: pd.DataFrame, *names: str) -> str | None:
    cols = {c.lower(): c for c in df.columns}
    for n in names:
        if n.lower() in cols:
            return cols[n.lower()]
    return None


def ingest_year(year_dir: Path, min_year: int) -> Tuple[list[dict], list[dict]]:
    records = []
    codes = []
    year = int(year_dir.name)
    if year < min_year:
        return records, codes

    for dict_path in year_dir.rglob("*"):
        if dict_path.suffix.lower() not in {".xlsx", ".xls", ".csv"}:
            continue
        name = dict_path.name.lower()
        parent = dict_path.parent.name.lower()
        if "_dict" not in name and "_dict" not in parent:
            continue

        try:
            if dict_path.suffix.lower() == ".csv":
                xls = None
                var_sheet = "__csv__"
                var_df_raw = pd.read_csv(dict_path, dtype=str, encoding_errors="ignore")
            else:
                xls = pd.ExcelFile(dict_path)
                var_sheet = find_varlist_sheet(xls)
                if not var_sheet:
                    continue
                var_df_raw = xls.parse(sheet_name=var_sheet, dtype=str)
        except Exception:
            continue

        var_df_raw.columns = [str(c).strip() for c in var_df_raw.columns]
        varnum_col = col(var_df_raw, "varnumber", "var_num", "number")
        varname_col = col(var_df_raw, "varname", "var_name", "name")
        vartitle_col = col(var_df_raw, "vartitle", "var_title", "var title", "title")
        dtype_col = col(var_df_raw, "datatype", "data type", "data_type")
        format_col = col(var_df_raw, "format")
        width_col = col(var_df_raw, "fieldwidth", "field width", "field_width", "width")
        imp_col = col(var_df_raw, "imputationvar", "imputation var", "impvar")

        # Description sheet
        desc_map = {}
        if xls:
            desc_sheet = next((s for s in xls.sheet_names if s.lower().startswith("description")), None)
            if desc_sheet:
                try:
                    ddf = xls.parse(sheet_name=desc_sheet, dtype=str)
                    ddf.columns = [str(c).strip().lower() for c in ddf.columns]
                    dn = col(ddf, "varname", "var_name", "name")
                    dn_num = col(ddf, "varnumber", "var_num", "number")
                    long_col = col(ddf, "longdescription", "long description", "description")
                    if long_col and (dn or dn_num):
                        keys = []
                        if dn:
                            keys.append(ddf[dn].fillna("").astype(str).str.lower())
                        if dn_num:
                            keys.append(ddf[dn_num].fillna("").astype(str).str.lower())
                        if keys:
                            ddf["key"] = keys[0]
                            for k in keys[1:]:
                                ddf.loc[ddf["key"].eq(""), "key"] = k
                            desc_map = ddf.set_index("key")[long_col].astype(str).to_dict()
                except Exception:
                    pass

        source_file = normalize_source_file(dict_path)
        source_file_label = extract_source_label(xls)
        # build quick lookup for varTitle by varname/varnumber
        title_by_varname: dict[str, str] = {}
        title_by_varnumber: dict[str, str] = {}
        if varname_col and vartitle_col:
            for _, r in var_df_raw.iterrows():
                vn = str(r.get(varname_col, "") or "").strip().upper()
                vt = str(r.get(vartitle_col, "") or "").strip()
                if vn and vt:
                    title_by_varname[vn] = vt
        if varnum_col and vartitle_col:
            for _, r in var_df_raw.iterrows():
                vnum = normalize_varnumber(r.get(varnum_col, "") if varnum_col else "")
                vt = str(r.get(vartitle_col, "") or "").strip()
                if vnum and vt:
                    title_by_varnumber[vnum] = vt

        for _, row in var_df_raw.iterrows():
            varname = str(row.get(varname_col, "") or "").strip().upper() if varname_col else ""
            if not varname:
                continue
            varnum = normalize_varnumber(row.get(varnum_col, "") if varnum_col else "")
            vartitle = str(row.get(vartitle_col, "") or "").strip() if vartitle_col else ""
            longdesc = desc_map.get(varname.lower(), "") or desc_map.get(varnum.lower(), "")
            datatype = str(row.get(dtype_col, "") or "").strip() if dtype_col else ""
            fmt = str(row.get(format_col, "") or "").strip() if format_col else ""
            width = str(row.get(width_col, "") or "").strip() if width_col else ""
            impvar = str(row.get(imp_col, "") or "").strip().upper() if imp_col else ""
            if not impvar and varname:
                impvar = f"X{varname}"
            records.append(
                {
                    "year": year,
                    "varnumber": varnum,
                    "varname": varname,
                    "varTitle": vartitle,
                    "longDescription": longdesc,
                    "DataType": datatype,
                    "format": fmt,
                    "Fieldwidth": width,
                    "imputationvar": impvar,
                    "source_file": source_file,
                    "source_file_label": source_file_label,
                }
            )
            # Also expose the imputation variable itself as a synthetic varname so it can appear in the final panel.
            # Keep varnumber untouched (no X-prefix); only the varname is synthetic.
            if impvar and impvar != varname:
                if impvar:  # guard against blank synthetic names
                    records.append(
                        {
                            "year": year,
                            "varnumber": varnum,
                            "varname": impvar.upper(),
                            "varTitle": f"Imputation flag for {varname}" if vartitle else f"Imputation flag for {varname}",
                            "longDescription": longdesc,
                            "DataType": "",
                            "format": "",
                            "Fieldwidth": "",
                            "imputationvar": "",
                            "source_file": source_file,
                            "source_file_label": source_file_label,
                        }
                    )

        # Frequencies / Imputation labels
        if xls:
            freq_sheet = next((s for s in xls.sheet_names if "frequenciesrv" in s.lower()), None)
            if not freq_sheet:
                freq_sheet = next((s for s in xls.sheet_names if s.strip().lower() == "frequencies"), None)
            if not freq_sheet:
                freq_sheet = next((s for s in xls.sheet_names if "frequencies" in s.lower()), None)
            if freq_sheet:
                try:
                    fdf = xls.parse(sheet_name=freq_sheet, dtype=str)
                    fdf.columns = [str(c).strip().lower() for c in fdf.columns]
                    fn = col(fdf, "varname", "var_name", "name")
                    fn_num = col(fdf, "varnumber", "var_num", "number")
                    code_col = col(fdf, "codevalue", "code value", "code", "value")
                    label_col = col(fdf, "valuelabel", "value label", "label")
                    if code_col and label_col:
                        for _, r in fdf.iterrows():
                            vt = ""
                            if fn:
                                vt = title_by_varname.get(str(r.get(fn, "") or "").strip().upper(), "")
                            if not vt and fn_num:
                                vt = title_by_varnumber.get(normalize_varnumber(r.get(fn_num, "")), "")
                            codes.append(
                                {
                                    "year": year,
                                    "varnumber": normalize_varnumber(r.get(fn_num, "")) if fn_num else "",
                                    "varname": str(r.get(fn, "") or "").strip().upper() if fn else "",
                                    "codevalue": str(r.get(code_col, "") or "").strip(),
                                    "valuelabel": str(r.get(label_col, "") or "").strip(),
                                    "varTitle": vt,
                                    "source_file": source_file,
                                    "source_file_label": source_file_label,
                                    "dict_file": str(dict_path),
                                    "sheet_name": freq_sheet,
                                    "source": "frequencies",
                                    "is_imputation_label": False,
                                    "label_scope": "regular",
                                }
                            )
                except Exception:
                    pass

            imp_sheet = next((s for s in xls.sheet_names if "imputation" in s.lower()), None)
            if imp_sheet:
                try:
                    idf = xls.parse(sheet_name=imp_sheet, dtype=str, header=1)
                    idf.columns = [str(c).strip().lower() for c in idf.columns]
                    code_col = col(idf, "codevalue", "code value", "code", "value")
                    label_col = col(idf, "valuelabel", "value label", "label")
                    if code_col and label_col:
                        for _, r in idf.iterrows():
                            codes.append(
                                {
                                    "year": year,
                                    "varnumber": "",
                                    "varname": "",
                                    "codevalue": str(r.get(code_col, "") or "").strip(),
                                    "valuelabel": str(r.get(label_col, "") or "").strip(),
                                    "source_file": source_file,
                                    "source_file_label": source_file_label,
                                    "dict_file": str(dict_path),
                                    "sheet_name": imp_sheet,
                                    "source": "imputation_values",
                                    "is_imputation_label": True,
                                    "label_scope": "imputation_variable",
                                }
                            )
                except Exception:
                    pass

    return records, codes


def main() -> None:
    args = parse_args()
    root = args.root
    if not root.exists():
        sys.exit(f"Root directory not found: {root}")

    all_rows: list[dict] = []
    all_codes: list[dict] = []
    for year_dir in sorted(p for p in root.iterdir() if p.is_dir() and p.name.isdigit()):
        recs, codes = ingest_year(year_dir, args.min_year)
        all_rows.extend(recs)
        all_codes.extend(codes)

    if not all_rows:
        sys.exit(
            f"No Varlist sheets found at {root} for years >= {args.min_year}. "
            "Confirm dictionaries are downloaded and min-year is not excluding them."
        )

    lake = pd.DataFrame(all_rows)
    lake["varnumber"] = lake["varnumber"].map(normalize_varnumber)
    lake["year"] = pd.to_numeric(lake["year"], errors="coerce").astype("Int64")
    lake = lake.drop_duplicates(subset=["year", "varnumber", "varname"]).reset_index(drop=True)
    # enforce column order
    desired_cols = [
        "year",
        "varnumber",
        "varname",
        "varTitle",
        "longDescription",
        "DataType",
        "format",
        "Fieldwidth",
        "imputationvar",
        "source_file",
        "source_file_label",
    ]
    for c in desired_cols:
        if c not in lake.columns:
            lake[c] = ""
    lake = lake[desired_cols]

    args.output.parent.mkdir(parents=True, exist_ok=True)
    lake.to_parquet(args.output, index=False, compression="snappy")
    print(f"Wrote {len(lake):,} rows to {args.output}")

    # Always regenerate CSV for inspection
    if args.output_csv:
        args.output_csv.parent.mkdir(parents=True, exist_ok=True)
        lake.to_csv(args.output_csv, index=False)
        print(f"Wrote {len(lake):,} rows to {args.output_csv}")

    if all_codes:
        codes_df = pd.DataFrame(all_codes)
        codes_df["year"] = pd.to_numeric(codes_df["year"], errors="coerce").astype("Int64")
        if "varTitle" not in codes_df.columns:
            codes_df["varTitle"] = ""
        if "source_file_label" not in codes_df.columns:
            codes_df["source_file_label"] = ""
        # Ensure flag columns exist
        if "is_imputation_label" not in codes_df.columns:
            codes_df["is_imputation_label"] = False
        if "label_scope" not in codes_df.columns:
            codes_df["label_scope"] = "regular"
        codes_df.to_parquet(args.codes_output, index=False, compression="snappy")
        codes_df.to_csv(args.codes_output_csv, index=False)
        print(f"Wrote {len(codes_df):,} rows to {args.codes_output} and {args.codes_output_csv}")


if __name__ == "__main__":
    main()
