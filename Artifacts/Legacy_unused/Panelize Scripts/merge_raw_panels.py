#!/usr/bin/env python3
import os
import re
import sys
from pathlib import Path
import pandas as pd
from collections import OrderedDict

# Paths: edit only if you moved anything
OUT_DIR = Path("/Users/markjaysonfarol13/Higher Ed research/IPEDS/Paneled Datasets/Crosssections")
FILES = [
    OUT_DIR / f"panel_wide_raw_{y}.csv"
    for y in range(2004, 2025)
]

BUILD_LONG = os.environ.get("MERGE_BUILD_LONG", "0") not in {"0", "false", "False", ""}

# 1) Known HD bare-code whitelist (not prefixed with HD/IC)
HD_BARE = {
    # identity + contact
    "UNITID","INSTNM","ADDR","CITY","STABBR","ZIP","FIPS","OBEREG","CHFNM","CHFTITLE",
    "GENTELE","FINTELE","ADMTELE","EIN","DUNS","OPEID","OPEFLAG","WEBADDR",
    # classification
    "SECTOR","ICLEVEL","CONTROL","HLOFFER","UGOFFER","GROFFER","FPOFFER","HDEGOFFR",
    "DEGGRANT","HBCU","HOSPITAL","MEDICAL","TRIBAL","CARNEGIE","LOCALE","OPENPUBL",
    # status/admin
    "ACT","NEWID","DEATHYR","CLOSEDAT","CYACTIVE","POSTSEC","PSEFLAG","PSET4FLG",
    "RPTMTH","INSTCAT","TENURSYS"
}

# 2) Component regex map (strongest first)
COMPONENT_ORDER = ["HD","IC","IC_AY","EF","E12","EFIA","E1D","EFFY","SFA","F1A","F2A","F3A","ADM","GR","GR200","OTHER"]
COMPONENT_PATTERN = {
    # IC_AY costs and IC codes
    "IC_AY": re.compile(r"^(ICAY|IC_AY|COA|TUITION|FEE|ROOM|RMBRD|BOOKS|OTHEREXP)", re.I),
    "IC":    re.compile(r"^IC", re.I),
    # EF families
    "EFIA":  re.compile(r"^EFIA|^EFFY|^E1D", re.I),
    "E12":   re.compile(r"^E12", re.I),
    "EF":    re.compile(r"^EF(?!FIA|FY|FY_|FIA_)", re.I),
    # SFA
    "SFA":   re.compile(r"^(NPIS|PGRNT|AIDF|AGRNT|GIS|UPGRNT|SFA|DL|PLUS|PRIV|PELL)", re.I),
    # Finance forms
    "F1A":   re.compile(r"^F1[A-Z0-9]", re.I),
    "F2A":   re.compile(r"^F2[A-Z0-9]", re.I),
    "F3A":   re.compile(r"^F3[A-Z0-9]", re.I),
    # Admissions
    "ADM":   re.compile(r"^(APPL|ADMSS|ENRL|SAT|ACT|ADM)", re.I),
    # Graduation
    "GR200": re.compile(r"^(GR2|G200)", re.I),
    "GR":    re.compile(r"^(GRS|GRT|GRADRATE|GR)", re.I),
}

ID_COLS = ["YEAR", "UNITID"]
OPTIONAL_ID_COLS = ["REPORTING_UNITID"]  # include if present

def classify(col: str) -> str:
    c = col.upper()
    if c in ID_COLS or c in (x.upper() for x in OPTIONAL_ID_COLS):
        return "ID"
    if c in HD_BARE:
        return "HD"
    # test patterns in priority order: first hit wins
    for comp in ["IC_AY","IC","EFIA","E12","EF","SFA","F1A","F2A","F3A","ADM","GR200","GR"]:
        if COMPONENT_PATTERN[comp].match(c):
            return comp
    return "OTHER"

def column_sort_key(col: str):
    comp = classify(col)
    if comp == "ID":
        return (-1, col)  # IDs first
    return (COMPONENT_ORDER.index(comp), col)

def read_panel(path: Path) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame()
    df = pd.read_csv(path, dtype=str)
    df.columns = pd.Index(str(c).strip().upper() for c in df.columns)
    df = df.loc[:, ~df.columns.duplicated()]
    if "YEAR" in df.columns:
        df["YEAR"] = pd.to_numeric(df["YEAR"], errors="coerce").astype("Int64")
    return df


def coalesce_duplicate_columns(df: pd.DataFrame) -> pd.DataFrame:
    groups: "OrderedDict[str, list[str]]" = OrderedDict()
    for col in df.columns:
        groups.setdefault(col, []).append(col)

    result_frames = []
    new_cols = []
    for col, duplicates in groups.items():
        if len(duplicates) == 1:
            series = df[duplicates[0]]
        else:
            series = df[duplicates].bfill(axis=1).iloc[:, 0]
        result_frames.append(series)
        new_cols.append(col)

    merged = pd.concat(result_frames, axis=1)
    merged.columns = new_cols
    return merged

def main():
    frames = []
    for f in FILES:
        df = read_panel(f)
        if df.empty:
            continue
        frames.append(df)

    if not frames:
        print("No input files found or all empty.")
        sys.exit(0)

    # union-all columns, align types
    wide = pd.concat(frames, ignore_index=True, sort=False)
    # ensure IDs exist
    for c in ID_COLS:
        if c not in wide.columns:
            wide[c] = pd.NA

    wide = coalesce_duplicate_columns(wide)
    # order columns by component priority
    cols = list(wide.columns)
    # keep optional id cols up front if present
    id_like = [c for c in OPTIONAL_ID_COLS if c in cols]
    ordered = ID_COLS + id_like + sorted([c for c in cols if c not in ID_COLS + id_like], key=column_sort_key)
    wide = wide[ordered]

    # coerce numeric for purely numeric columns (best-effort)
    for c in wide.columns:
        if c in ID_COLS + id_like:
            continue
        # try fast numeric coerce where safe (won't break strings)
        ser = pd.to_numeric(wide[c], errors="ignore")
        wide[c] = ser

    out_wide = OUT_DIR / "panel_wide_raw_2004_2024_merged.csv"
    wide.to_csv(out_wide, index=False)
    print(f"Wrote merged wide: {out_wide} with {wide.shape[0]:,} rows and {wide.shape[1]:,} cols.")

    if BUILD_LONG:
        long = (
            wide.set_index(ID_COLS + id_like)
            .stack(dropna=True)
            .reset_index()
            .rename(columns={"level_" + str(len(ID_COLS + id_like)): "source_var", 0: "value"})
        )

        if len(long) > 100_000_000:
            raise SystemExit(
                f"Refusing to write long output with {len(long):,} rows. Restrict years or columns and retry."
            )

        long["value"] = long["value"].astype("string")
        out_long = OUT_DIR / "panel_long_raw_2004_2024_merged.parquet"
        long.to_parquet(out_long, index=False)
        print(f"Wrote merged long (parquet): {out_long} with {len(long):,} rows.")
    else:
        print("Skipping long output (set MERGE_BUILD_LONG=1 to enable).")

if __name__ == "__main__":
    main()
