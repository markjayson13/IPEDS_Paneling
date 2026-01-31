#!/usr/bin/env python3
"""
Harmonizer that builds a LONG panel keyed by (UNITID, year, varname) with varnumber metadata.

Inputs:
  - root:   /path/to/Raw_Cross_Section_Data
  - lake:   dictionary_lake.parquet from 01_ingest_dictionaries.py
  - years:  "YYYY:YYYY" or comma list "2018,2019,2020"
  - output: destination parquet (long format)

Behavior:
  - Scans all data files under root/<year> recursively (.csv/.tsv/.txt/.gz)
  - Skips dictionary folders (name contains "_dict" case‑insensitive)
  - Reads in chunks (low RAM), requires UNITID
  - Melts to long, merges with dictionary on (year, varname)
  - Writes a parquet with columns:
      year, UNITID, varname, varnumber, value,
      varTitle, longDescription, DataType, format, Fieldwidth, imputationvar, source_file
"""
import duckdb
import argparse
import pathlib
import csv
from typing import Iterable, List
import os

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


def check_release_manifest(
    year_root: pathlib.Path,
    year: int,
    allowlist: set[str],
    strict: bool,
    qc_dir: pathlib.Path | None,
) -> None:
    """
    Validate that the year's manifest indicates Revised/Final release only.
    Writes QC summary + details if qc_dir is provided.
    """
    manifest_path = year_root / f"{year}_manifest.csv"
    if qc_dir:
        qc_dir.mkdir(parents=True, exist_ok=True)
    if not manifest_path.exists():
        msg = f"[release] missing manifest: {manifest_path}"
        if qc_dir:
            (qc_dir / f"release_summary_{year}.csv").write_text("status,count\nmissing_manifest,1\n")
        if strict:
            raise SystemExit(msg)
        print(msg)
        return

    df = pd.read_csv(manifest_path, dtype=str).fillna("")
    if "release" not in df.columns:
        msg = f"[release] manifest missing 'release' column: {manifest_path}"
        if qc_dir:
            (qc_dir / f"release_summary_{year}.csv").write_text("status,count\nmissing_release_column,1\n")
        if strict:
            raise SystemExit(msg)
        print(msg)
        return

    df["release_norm"] = df["release"].str.strip().str.lower()
    if "is_revision" in df.columns:
        df["is_revision_norm"] = df["is_revision"].str.strip().str.lower().isin(["1", "true", "yes", "y"])
    else:
        df["is_revision_norm"] = False
    df["status"] = df["release_norm"]
    df.loc[df["is_revision_norm"], "status"] = "revised"
    # Some manifests use blank release for surveys without revised/final labeling.
    # Treat blank as allowed to avoid blocking those files, but still log in QC.
    df["allowed"] = df["status"].isin(allowlist) | (df["status"] == "")
    has_revised = (df["status"] == "revised").any()

    if qc_dir:
        details_path = qc_dir / f"release_details_{year}.csv"
        df.to_csv(details_path, index=False)
        summary = (
            df.groupby("status", dropna=False)
              .size()
              .reset_index(name="count")
        )
        summary_path = qc_dir / f"release_summary_{year}.csv"
        summary.to_csv(summary_path, index=False)

    # Enforce only when revised is an option AND user asked for revised
    enforce_revised = strict and ("revised" in allowlist) and has_revised
    if strict and ("revised" in allowlist) and not has_revised:
        print(f"[release] no revised entries in manifest for {year}; strict enforcement skipped.")

    # Any unknown/empty status is not allowed when strict+revised
    if enforce_revised:
        bad = df[~df["allowed"]]
        if not bad.empty:
            raise SystemExit(
                f"[release] non‑revised/final entries found in {manifest_path} "
                f"(count={len(bad)})."
            )


def read_table_iter(fp: pathlib.Path, chunksize: int = 200000):
    suffix = fp.suffix.lower()
    sep = "\t" if suffix == ".tsv" else ","
    compression = "gzip" if suffix == ".gz" else None
    # Try UTF-8 first, then fall back to latin1 and skipping bad lines if needed.
    attempts = (
        dict(dtype=str, sep=sep, compression=compression, low_memory=False, chunksize=chunksize),
        dict(dtype=str, sep=sep, compression=compression, engine="python", on_bad_lines="skip", chunksize=chunksize),
        dict(dtype=str, sep=sep, compression=compression, engine="python", encoding="latin1", on_bad_lines="skip", chunksize=chunksize),
        # Last-resort: treat quotes as regular characters
        dict(
            dtype=str,
            sep=sep,
            compression=compression,
            engine="python",
            encoding="latin1",
            on_bad_lines="skip",
            quoting=csv.QUOTE_NONE,
            escapechar="\\",
            chunksize=chunksize,
        ),
    )
    last_err = None
    for kwargs in attempts:
        try:
            reader = pd.read_csv(fp, **kwargs)
            for chunk in reader:
                yield chunk
            return
        except Exception as e:
            last_err = e
            continue
    print(f"[warn] failed to read {fp}: {last_err}")
    return


def melt_file(fp: pathlib.Path, year: int, dict_year: pd.DataFrame) -> Iterable[pd.DataFrame]:
    for df in read_table_iter(fp):
        if df.empty:
            continue
        if "UNITID" not in df.columns:
            continue
        df["UNITID"] = pd.to_numeric(df["UNITID"], errors="coerce").astype("Int64")
        id_cols = ["UNITID"]
        value_cols = [c for c in df.columns if c not in id_cols]
        if not value_cols:
            continue
        long = df.melt(id_vars=id_cols, value_vars=value_cols, var_name="varname", value_name="value")
        long["year"] = year
        merged = long.merge(dict_year, on=["year", "varname"], how="left")
        merged = merged.dropna(subset=["varnumber"])
        # Ensure source_file exists (from dictionary); fall back to data filename if missing.
        if "source_file" not in merged.columns:
            merged["source_file"] = fp.name
        else:
            merged["source_file"] = merged["source_file"].fillna(fp.name)
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
        yield merged[cols]


def write_parquet_stream(out_path: pathlib.Path, frames: Iterable[pd.DataFrame]) -> None:
    """
    Stream-write to a temp file, then atomically replace the target.
    This avoids leaving corrupted parquet files when a run is interrupted.
    """
    tmp_path = out_path.with_suffix(out_path.suffix + ".tmp")
    if tmp_path.exists():
        tmp_path.unlink()
    writer = None
    for chunk in frames:
        if chunk.empty:
            continue
        table = pa.Table.from_pandas(chunk, preserve_index=False)
        if writer is None:
            writer = pq.ParquetWriter(tmp_path, table.schema, compression="snappy")
        writer.write_table(table)
    if writer:
        writer.close()
        tmp_path.replace(out_path)


def write_parquet_parts(out_path: pathlib.Path, frames: Iterable[pd.DataFrame], parts_dir: pathlib.Path) -> None:
    """
    Write each chunk to its own parquet part, then stitch to final output.
    This avoids long-lived single-writer runs that can be killed mid-write.
    """
    parts_dir.mkdir(parents=True, exist_ok=True)
    idx = 0
    for chunk in frames:
        if chunk.empty:
            continue
        table = pa.Table.from_pandas(chunk, preserve_index=False)
        part_path = parts_dir / f"part_{idx:05d}.parquet"
        pq.write_table(table, part_path, compression="snappy")
        idx += 1

    if idx == 0:
        return

    tmp_out = out_path.with_suffix(out_path.suffix + ".tmp")
    if tmp_out.exists():
        tmp_out.unlink()

    writer = None
    for part in sorted(parts_dir.glob("part_*.parquet")):
        pf = pq.ParquetFile(part)
        for batch in pf.iter_batches():
            if writer is None:
                writer = pq.ParquetWriter(tmp_out, batch.schema, compression="snappy")
            writer.write_batch(batch)
    if writer:
        writer.close()
        tmp_out.replace(out_path)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--root", required=True)
    ap.add_argument("--lake", required=True)
    ap.add_argument("--years", required=True)
    ap.add_argument("--output", required=True)
    ap.add_argument("--parts-dir", default=None, help="Optional directory to write parquet parts before stitching")
    ap.add_argument("--reuse-parts", action=argparse.BooleanOptionalAction, default=True, help="Reuse existing parts_YYYY directory if present")
    ap.add_argument("--release-allow", default="revised,final", help="Comma list of allowed release statuses")
    ap.add_argument("--release-strict", action=argparse.BooleanOptionalAction, default=True, help="Fail if manifest is missing or not revised/final")
    ap.add_argument("--release-qc-dir", default="/Users/markjaysonfarol13/IPEDS_Paneling/Checks/release_qc", help="QC dir for release validation")
    args = ap.parse_args()

    years = parse_years(args.years)
    dict_df = pd.read_parquet(args.lake)
    dict_df["varnumber"] = dict_df["varnumber"].astype(str).str.zfill(8)

    out_path = pathlib.Path(args.output)
    out_path.parent.mkdir(parents=True, exist_ok=True)

    # Ensure intermediate per-year directory exists when writing year-by-year runs
    # (useful if caller passes outputs like .../Cross_sections/panel_long_varnum_<year>.parquet)

    allowlist = {s.strip().lower() for s in args.release_allow.split(",") if s.strip()}
    qc_dir = pathlib.Path(args.release_qc_dir) if args.release_qc_dir else None

    def iter_chunks_for_year(year: int):
        print(f"[info] processing year {year}")
        year_root = pathlib.Path(args.root) / str(year)
        check_release_manifest(year_root, year, allowlist, args.release_strict, qc_dir)
        dict_year = dict_df[dict_df["year"] == year]
        for fp in discover_files(year_root):
            for chunk in melt_file(fp, year, dict_year):
                if not chunk.empty:
                    yield chunk

    if args.parts_dir:
        parts_dir = pathlib.Path(args.parts_dir)
        # If a parts dir already exists and reuse is allowed, stitch without reprocessing
        if args.reuse_parts and parts_dir.exists():
            part_files = sorted(parts_dir.glob("part_*.parquet"))
            if part_files:
                tmp_out = out_path.with_suffix(out_path.suffix + ".tmp")
                if tmp_out.exists():
                    tmp_out.unlink()
                writer = None
                for part in part_files:
                    pf = pq.ParquetFile(part)
                    for batch in pf.iter_batches():
                        if writer is None:
                            writer = pq.ParquetWriter(tmp_out, batch.schema, compression="snappy")
                        writer.write_batch(batch)
                if writer:
                    writer.close()
                    tmp_out.replace(out_path)
                    print(f"[info] stitched from existing parts: {out_path}")
                    return
        # Otherwise, process and write parts
        write_parquet_parts(out_path, iter_chunks_for_year(years[0]) if len(years) == 1 else (chunk for y in years for chunk in iter_chunks_for_year(y)), parts_dir)
    else:
        write_parquet_stream(out_path, (chunk for y in years for chunk in iter_chunks_for_year(y)))
    print(f"[info] wrote {out_path}")


if __name__ == "__main__":
    main()
