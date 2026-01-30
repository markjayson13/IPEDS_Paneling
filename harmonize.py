#!/usr/bin/env python3
"""
Harmonizer that builds a long panel keyed by (year, UNITID, varnumber).

Inputs
------
- root:   /path/to/Raw_Cross_Section_Data
- lake:   dictionary_lake.parquet produced by 01_ingest_dictionaries.py
- years:  "YYYY:YYYY" or comma list "2018,2019,2020"
- output: destination parquet (long format)

Behavior
--------
- Scans all data files under root/<year> recursively with extensions .csv/.tsv/.txt/.gz.
- Skips obvious dictionary folders (name contains "_dict" case-insensitive).
- Reads each file as text (dtype=str), requires UNITID column.
- Melts to long, merges with dictionary on (year, varname) to obtain varnumber + metadata.
- Writes a single parquet with columns:
    year, UNITID, varnumber, varname, value,
    varTitle, longDescription, DataType, format, Fieldwidth, imputationvar, source_file
"""

import argparse
import pathlib
from typing import Iterable, List

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq


def parse_years(arg: str) -> List[int]:
    if ":" in arg:
        start, end = map(int, arg.split(":"))
        return list(range(start, end + 1))
    return [int(x) for x in arg.split(",") if x.strip()]


def discover_files(year_root: pathlib.Path) -> Iterable[pathlib.Path]:
    exts = {".csv", ".tsv", ".txt", ".gz"}
    for fp in year_root.rglob("*"):
        if not fp.is_file():
            continue
        if fp.suffix.lower() not in exts:
            continue
        # skip dictionary folders
        if any("_dict" in part.lower() for part in fp.parts):
            continue
        yield fp


def read_table(fp: pathlib.Path) -> pd.DataFrame:
    suffix = fp.suffix.lower()
    sep = "\t" if suffix == ".tsv" else ","
    compression = "gzip" if suffix == ".gz" else None
    # Try UTF-8 first, then fall back to latin1 and skipping bad lines if needed.
    attempts = (
        dict(dtype=str, sep=sep, compression=compression, low_memory=False),
        dict(dtype=str, sep=sep, compression=compression, engine="python"),
        dict(dtype=str, sep=sep, compression=compression, engine="python", encoding="latin1"),
        dict(dtype=str, sep=sep, compression=compression, engine="python", encoding="latin1", on_bad_lines="skip"),
    )
    last_err = None
    for kwargs in attempts:
        try:
            return pd.read_csv(fp, **kwargs)
        except Exception as e:
            last_err = e
            continue
    print(f"[warn] failed to read {fp}: {last_err}")
    return pd.DataFrame()


def melt_file(fp: pathlib.Path, year: int, dict_year: pd.DataFrame) -> pd.DataFrame:
    df = read_table(fp)
    if df.empty:
        return pd.DataFrame()
    if "UNITID" not in df.columns:
        return pd.DataFrame()
    df["UNITID"] = pd.to_numeric(df["UNITID"], errors="coerce").astype("Int64")
    id_cols = ["UNITID"]
    value_cols = [c for c in df.columns if c not in id_cols]
    long = df.melt(id_vars=id_cols, value_vars=value_cols, var_name="varname", value_name="value")
    long["year"] = year
    long["source_file"] = fp.name
    merged = long.merge(dict_year, on=["year", "varname"], how="left")
    merged = merged.dropna(subset=["varnumber"])
    # Return full long rows; we'll pivot to wide in a separate step
    cols = [
        "year",
        "UNITID",
        "varname",
        "varnumber",
        "value",
        "varTitle",
        "longDescription",
        "DataType",
        "format",
        "Fieldwidth",
        "imputationvar",
        "source_file",
    ]
    return merged[cols]


def write_parquet_stream(out_path: pathlib.Path, frames: Iterable[pd.DataFrame]) -> None:
    writer = None
    for chunk in frames:
        if chunk.empty:
            continue
        table = pa.Table.from_pandas(chunk, preserve_index=False)
        if writer is None:
            writer = pq.ParquetWriter(out_path, table.schema, compression="snappy")
        writer.write_table(table)
    if writer:
        writer.close()


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--root", required=True)
    ap.add_argument("--lake", required=True)
    ap.add_argument("--years", required=True)
    ap.add_argument("--output", required=True)
    args = ap.parse_args()

    years = parse_years(args.years)
    dict_df = pd.read_parquet(args.lake)
    dict_df["varnumber"] = dict_df["varnumber"].astype(str).str.zfill(8)

    out_path = pathlib.Path(args.output)
    out_path.parent.mkdir(parents=True, exist_ok=True)

    # Ensure intermediate per-year directory exists when writing year-by-year runs
    # (useful if caller passes outputs like .../Cross_sections/panel_long_varnum_<year>.parquet)

    all_chunks = []
    for year in years:
        print(f"[info] processing year {year}")
        dict_year = dict_df[dict_df["year"] == year]
        year_root = pathlib.Path(args.root) / str(year)
        for fp in discover_files(year_root):
            chunk = melt_file(fp, year, dict_year)
            if not chunk.empty:
                all_chunks.append(chunk)

    write_parquet_stream(out_path, all_chunks)
    print(f"[info] wrote {out_path}")


if __name__ == "__main__":
    main()
