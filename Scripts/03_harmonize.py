#!/usr/bin/env python3
"""
Harmonizer that builds a LONG panel with provenance-preserving grain:
  canonical key = (UNITID, year, varnumber, source_file)

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
      NOTE: dictionary is reduced to a single preferred row per (year, varname) before merge
            to prevent cartesian expansion.
  - Drops rows with missing UNITID to avoid <NA> cross-product explosions.
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
import sys

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq


def setup_logging(log_path: str | None) -> None:
    if not log_path:
        return
    log_file = pathlib.Path(log_path)
    log_file.parent.mkdir(parents=True, exist_ok=True)
    f = log_file.open("a", buffering=1)

    class Tee:
        def __init__(self, *streams):
            self.streams = streams

        def write(self, data):
            for s in self.streams:
                s.write(data)

        def flush(self):
            for s in self.streams:
                s.flush()

    sys.stdout = Tee(sys.stdout, f)
    sys.stderr = Tee(sys.stderr, f)


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


def prefer_rv_files(files: Iterable[pathlib.Path]) -> list[pathlib.Path]:
    """
    If both base and *_rv files exist in the same folder, keep *_rv only.
    Otherwise keep the available file.
    """
    groups: dict[tuple[pathlib.Path, str], list[tuple[bool, pathlib.Path]]] = {}
    for fp in files:
        stem = fp.stem.lower()
        is_rv = stem.endswith("_rv")
        key = stem[:-3] if is_rv else stem
        gkey = (fp.parent, key)
        groups.setdefault(gkey, []).append((is_rv, fp))
    kept: list[pathlib.Path] = []
    for entries in groups.values():
        has_rv = any(is_rv for is_rv, _ in entries)
        if has_rv:
            kept.extend([fp for is_rv, fp in entries if is_rv])
        else:
            kept.extend([fp for _, fp in entries])
    return kept


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


def read_table_iter(fp: pathlib.Path, chunksize: int = 50000):
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


def _chunk_cols(cols: list[str], size: int) -> Iterable[list[str]]:
    for i in range(0, len(cols), size):
        yield cols[i : i + size]


def melt_file(
    fp: pathlib.Path,
    year: int,
    dict_year: pd.DataFrame,
    dict_vars: set[str],
    pref_df: pd.DataFrame | None,
    strict: bool,
    value_cols_per_chunk: int,
    chunksize: int,
    log_prefix: str = "",
) -> Iterable[pd.DataFrame]:
    logged = False
    for df in read_table_iter(fp, chunksize=chunksize):
        if df.empty:
            continue
        # Normalize header case/spacing
        df.columns = [str(c).strip().upper() for c in df.columns]
        if "UNITID" not in df.columns:
            continue
        df["UNITID"] = pd.to_numeric(df["UNITID"], errors="coerce").astype("Int64")
        before_rows = len(df)
        df = df.dropna(subset=["UNITID"])
        dropped = before_rows - len(df)
        if dropped > 0:
            print(f"{log_prefix}[warn] {fp.name} dropped_rows_missing_UNITID={dropped}")
            if strict:
                raise SystemExit(
                    f"[fatal] missing UNITID rows detected in {fp.name} (dropped={dropped})."
                )
        id_cols = ["UNITID"]
        # Keep only vars we can match in the dictionary for this year
        value_cols = [c for c in df.columns if c not in id_cols and c in dict_vars]
        if not value_cols:
            continue
        if not logged:
            print(f"{log_prefix}[file] {fp.name} matched_cols={len(value_cols)}")
            logged = True
        for col_chunk in _chunk_cols(value_cols, value_cols_per_chunk):
            long = df.melt(id_vars=id_cols, value_vars=col_chunk, var_name="varname", value_name="value")
            long["year"] = year
            if strict:
                merged = long.merge(dict_year, on=["year", "varname"], how="left", validate="m:1")
                if len(merged) != len(long):
                    raise SystemExit(
                        f"[fatal] dictionary merge expanded rows for {fp.name} year={year}: "
                        f"{len(long)} -> {len(merged)}"
                    )
            else:
                merged = long.merge(dict_year, on=["year", "varname"], how="left")
            merged = merged.dropna(subset=["varnumber"])
            # Ensure source_file exists (from dictionary); fall back to data filename if missing.
            if "source_file" not in merged.columns:
                merged["source_file"] = fp.name
            else:
                merged["source_file"] = merged["source_file"].fillna(fp.name)
            # NOTE: preferred_source filtering is applied to dict_year before merge.
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


def dedupe_long_panel(out_path: pathlib.Path, priority_list: list[str], temp_dir: pathlib.Path | None = None) -> None:
    """
    Deterministically drop duplicate canonical-key rows by source_file priority.
    Canonical key = (UNITID, year, varnumber, source_file).
    Uses DuckDB to avoid loading the full parquet into memory.
    """
    if not out_path.exists():
        return
    # Build CASE expression for priority ranking
    case = "CASE"
    for i, src in enumerate(priority_list):
        src_esc = src.replace("'", "''")
        case += f" WHEN source_file = '{src_esc}' THEN {i}"
    case += " ELSE 999 END"

    tmp_out = out_path.with_suffix(out_path.suffix + ".dedupe.tmp")
    if tmp_out.exists():
        tmp_out.unlink()

    con = duckdb.connect()
    if temp_dir is not None:
        temp_dir.mkdir(parents=True, exist_ok=True)
        safe_temp = str(temp_dir).replace("'", "''")
        con.execute(f"PRAGMA temp_directory='{safe_temp}'")
    # If source_file column is missing, fall back to arbitrary deterministic order.
    cols = pq.ParquetFile(out_path).schema.names
    if "source_file" not in cols:
        order_expr = "varname"
        case_expr = "0"
    else:
        order_expr = f"{case}, source_file"
        case_expr = case

    q = f"""
        COPY (
            SELECT * EXCLUDE(_src_rank, _rn)
            FROM (
                SELECT *,
                       {case_expr} AS _src_rank,
                       ROW_NUMBER() OVER (
                           PARTITION BY UNITID, year, varnumber, source_file
                           ORDER BY {order_expr}
                       ) AS _rn
                FROM read_parquet('{out_path}')
            )
            WHERE _rn = 1
        ) TO '{tmp_out}' (FORMAT PARQUET);
    """
    con.execute(q)
    tmp_out.replace(out_path)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--root", required=True)
    ap.add_argument("--lake", required=True)
    ap.add_argument("--years", required=True)
    ap.add_argument("--output", required=True)
    ap.add_argument("--parts-dir", default=None, help="Optional directory to write parquet parts before stitching")
    ap.add_argument("--reuse-parts", action=argparse.BooleanOptionalAction, default=True, help="Reuse existing parts_YYYY directory if present")
    ap.add_argument("--cleanup-parts", action=argparse.BooleanOptionalAction, default=True, help="Remove parts directory after successful stitch")
    ap.add_argument("--chunksize", type=int, default=50000, help="Row chunksize for CSV reading (lower uses less RAM)")
    ap.add_argument("--value-cols-per-chunk", type=int, default=250, help="Max number of value columns to melt per chunk")
    ap.add_argument("--dedupe", action=argparse.BooleanOptionalAction, default=True, help="Deterministically drop duplicate canonical-key rows by preferred source_file")
    ap.add_argument("--dedupe-priority", default="HD,IC,IC_AY,IC_PY,ADM,AL,C_A,C_B,C_C,CDEP,COST,EAP,EFA,EFA_DIST,EFB,EFC,EFCP,EFFY,EFFY_DIST,EFIA,FLAGS,F_F,F_FA,F_FA_F,F_FA_G,GR,GR200,GR_PELL_SSL,OM,SAL_A,SAL_A_LT,SAL_B,SAL_FACULTY,SAL_IS,S_ABD,S_CN,S_F,S_G,S_IS,S_NH,S_OC,S_SIS,SFA,SFAV", help="Comma-separated source_file priority list for dedupe")
    ap.add_argument("--final-dedupe", action=argparse.BooleanOptionalAction, default=True, help="Apply a final deterministic de-duplication on the output parquet")
    ap.add_argument("--duckdb-temp-dir", default=None, help="Optional temp directory for DuckDB (used during final dedupe)")
    ap.add_argument("--release-allow", default="revised,final", help="Comma list of allowed release statuses")
    ap.add_argument("--release-strict", action=argparse.BooleanOptionalAction, default=True, help="Fail if manifest is missing or not revised/final")
    ap.add_argument("--strict", action=argparse.BooleanOptionalAction, default=False, help="Fail fast on missing UNITID or dictionary merge expansion")
    ap.add_argument("--qc-only", action="store_true", help="Only run release QC and exit; no output is written")
    repo_root = pathlib.Path(os.environ.get("IPEDS_ROOT", pathlib.Path(__file__).resolve().parents[1]))
    artifacts_root = repo_root / "Artifacts"
    ap.add_argument("--release-qc-dir", default=str(artifacts_root / "Checks" / "release_qc"), help="QC dir for release validation")
    ap.add_argument("--log-file", default=str(artifacts_root / "Checks" / "logs" / "03_harmonize.log"), help="Optional log file path")
    args = ap.parse_args()

    setup_logging(args.log_file)

    years = parse_years(args.years)
    allowlist = {s.strip().lower() for s in args.release_allow.split(",") if s.strip()}
    qc_dir = pathlib.Path(args.release_qc_dir) if args.release_qc_dir else None

    if args.qc_only:
        for year in years:
            year_root = pathlib.Path(args.root) / str(year)
            check_release_manifest(year_root, year, allowlist, args.release_strict, qc_dir)
        print("[info] release QC complete (qc-only)")
        return

    dict_df = pd.read_parquet(args.lake)
    dict_df["varnumber"] = dict_df["varnumber"].astype(str).str.zfill(8)
    dict_df["varname"] = dict_df["varname"].astype(str).str.upper()
    dict_df = dict_df[
        [
            "year",
            "varname",
            "varnumber",
            "varTitle",
            "longDescription",
            "DataType",
            "format",
            "Fieldwidth",
            "imputationvar",
            "source_file",
        ]
    ]

    out_path = pathlib.Path(args.output)
    out_path.parent.mkdir(parents=True, exist_ok=True)

    # Ensure intermediate per-year directory exists when writing year-by-year runs
    # (useful if caller passes outputs like .../Cross_sections/panel_long_varnum_<year>.parquet)

    def iter_chunks_for_year(year: int):
        print(f"[info] processing year {year}")
        year_root = pathlib.Path(args.root) / str(year)
        check_release_manifest(year_root, year, allowlist, args.release_strict, qc_dir)
        dict_year = dict_df[dict_df["year"] == year].copy()
        # Deduplicate dictionary rows so each (varname, source_file) is unique.
        # This prevents cartesian expansion during merge.
        if not dict_year.empty:
            dict_year = dict_year.sort_values(["varname", "source_file", "varnumber"]).drop_duplicates(
                ["varname", "source_file"], keep="first"
            )
        dict_vars = set(dict_year["varname"].dropna().unique())
        pref_df = None
        if args.dedupe:
            prio = {k.strip(): i for i, k in enumerate(args.dedupe_priority.split(","))}
            pref = dict_year[["year", "varname", "source_file"]].dropna().copy()
            if not pref.empty:
                pref["rank"] = pref["source_file"].map(prio).fillna(999).astype(int)
                pref = pref.sort_values(["year", "varname", "rank", "source_file"]).drop_duplicates(["year", "varname"], keep="first")
                pref_df = pref[["year", "varname", "source_file"]].rename(columns={"source_file": "preferred_source"})
        if pref_df is not None and not pref_df.empty and not dict_year.empty:
            dict_year = dict_year.merge(pref_df, on=["year", "varname"], how="left")
            dict_year = dict_year[(dict_year["preferred_source"].isna()) | (dict_year["source_file"] == dict_year["preferred_source"])]
            dict_year = dict_year.drop(columns=["preferred_source"])
        if not dict_year.empty:
            dup = dict_year.duplicated(["year", "varname"]).sum()
            if dup:
                msg = f"[warn] dict_year not unique on (year,varname) for year={year} (dups={dup})"
                if args.strict:
                    raise SystemExit(f"[fatal] {msg}")
                print(msg)
                dict_year = dict_year.drop_duplicates(["year", "varname"], keep="first")
            dict_vars = set(dict_year["varname"].dropna().unique())
        all_files = list(discover_files(year_root))
        files = prefer_rv_files(all_files)
        skipped_rv = len(all_files) - len(files)
        if skipped_rv > 0:
            print(f"[year {year}] skipped {skipped_rv} non-_rv files where _rv exists")
        for fp in files:
            for chunk in melt_file(
                fp,
                year,
                dict_year,
                dict_vars,
                pref_df,
                args.strict,
                args.value_cols_per_chunk,
                args.chunksize,
                log_prefix=f"[year {year}] ",
            ):
                if not chunk.empty:
                    yield chunk

    temp_dir = None
    if args.duckdb_temp_dir:
        temp_dir = pathlib.Path(args.duckdb_temp_dir)
    elif os.environ.get("DUCKDB_TEMP_DIRECTORY"):
        temp_dir = pathlib.Path(os.environ["DUCKDB_TEMP_DIRECTORY"])
    else:
        temp_dir = out_path.parent / ".duckdb_tmp"

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
                    if args.dedupe and args.final_dedupe:
                        prio_list = [k.strip() for k in args.dedupe_priority.split(",") if k.strip()]
                        dedupe_long_panel(out_path, prio_list, temp_dir)
                    if args.cleanup_parts:
                        try:
                            import shutil
                            shutil.rmtree(parts_dir)
                            print(f"[info] removed parts dir: {parts_dir}")
                        except Exception as e:
                            print(f"[warn] failed to remove parts dir {parts_dir}: {e}")
                    return
        # Otherwise, process and write parts
        write_parquet_parts(out_path, iter_chunks_for_year(years[0]) if len(years) == 1 else (chunk for y in years for chunk in iter_chunks_for_year(y)), parts_dir)
        if args.dedupe and args.final_dedupe:
            prio_list = [k.strip() for k in args.dedupe_priority.split(",") if k.strip()]
            dedupe_long_panel(out_path, prio_list, temp_dir)
        if args.cleanup_parts:
            try:
                import shutil
                shutil.rmtree(parts_dir)
                print(f"[info] removed parts dir: {parts_dir}")
            except Exception as e:
                print(f"[warn] failed to remove parts dir {parts_dir}: {e}")
    else:
        write_parquet_stream(out_path, (chunk for y in years for chunk in iter_chunks_for_year(y)))
        if args.dedupe and args.final_dedupe:
            prio_list = [k.strip() for k in args.dedupe_priority.split(",") if k.strip()]
            dedupe_long_panel(out_path, prio_list, temp_dir)
    print(f"[info] wrote {out_path}")


if __name__ == "__main__":
    main()
