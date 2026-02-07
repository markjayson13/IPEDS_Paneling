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
      varTitle, longDescription, DataType, format, Fieldwidth, imputationvar, imputation_value, source_file
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

REPO_ROOT = pathlib.Path(os.environ.get("IPEDS_ROOT", pathlib.Path(__file__).resolve().parents[1]))


def normalize_varnumber(val: object) -> str:
    """
    Normalize varnumber as a string ID.
    Zero-pad only numeric-looking IDs to width 8.
    """
    if pd.isna(val):
        return ""
    txt = str(val).strip()
    if txt.lower() in {"", "nan", "none", "<na>", "na", "nat"}:
        return ""
    return txt.zfill(8) if txt.isdigit() else txt


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
        # skip mission folders entirely (requested exclusion)
        if any("mission" in part.lower() for part in fp.parts):
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


def _manifest_filename_column(df: pd.DataFrame) -> str | None:
    """
    Best-effort detection of a filename/path column in a year manifest.
    """
    candidates = ["filename", "file", "filepath", "path", "data_file", "datafile"]
    lower = {c.lower(): c for c in df.columns}
    for c in candidates:
        if c in lower:
            return lower[c]
    return None


def _normalized_name_key(name: str) -> str:
    """
    Normalize filenames across manifest archives and extracted data files.
    Example:
      - IC2004.zip -> ic2004
      - ic2004.csv -> ic2004
      - c2004_a_rv.csv -> c2004_a
    """
    stem = pathlib.Path(str(name)).stem.lower().strip()
    if stem.endswith("_rv"):
        return stem[:-3]
    return stem


def build_manifest_allowlist(
    year_root: pathlib.Path,
    year: int,
    allowlist_status: set[str],
    strict: bool,
    qc_dir: pathlib.Path | None,
) -> set[str] | None:
    """
    Build an allowlist of file basenames from {year}_manifest.csv.
    """
    manifest_path = year_root / f"{year}_manifest.csv"
    if not manifest_path.exists():
        if strict:
            raise SystemExit(f"[release] missing manifest: {manifest_path}")
        print(f"[release] missing manifest (allowlist disabled): {manifest_path}")
        return None

    df = pd.read_csv(manifest_path, dtype=str).fillna("")
    fname_col = _manifest_filename_column(df)
    if fname_col is None:
        msg = f"[release] manifest has no filename/path column; allowlist disabled: {manifest_path}"
        if strict:
            raise SystemExit(msg)
        print(msg)
        return None

    if "release" not in df.columns:
        msg = f"[release] manifest missing 'release' column; allowlist disabled: {manifest_path}"
        if strict:
            raise SystemExit(msg)
        print(msg)
        return None

    # Keep release handling consistent with check_release_manifest.
    df["release_norm"] = df["release"].astype(str).str.strip().str.lower()
    if "is_revision" in df.columns:
        df["is_revision_norm"] = df["is_revision"].astype(str).str.strip().str.lower().isin(["1", "true", "yes", "y"])
    else:
        df["is_revision_norm"] = False
    df["status"] = df["release_norm"]
    df.loc[df["is_revision_norm"], "status"] = "revised"
    df["allowed"] = df["status"].isin(allowlist_status) | (df["status"] == "")

    # Normalize to basenames in lower case; manifests may contain full paths.
    df["_base"] = df[fname_col].astype(str).apply(lambda x: pathlib.Path(x).name.lower().strip())
    df = df[df["_base"] != ""]
    if df.empty:
        msg = f"[release] manifest filename column is empty; allowlist disabled: {manifest_path}"
        if strict:
            raise SystemExit(msg)
        print(msg)
        return None

    allowed_df = df[df["allowed"]].copy()
    if allowed_df.empty:
        msg = f"[release] manifest has no allowed files under statuses={sorted(allowlist_status)} for year={year}"
        if strict:
            raise SystemExit(msg)
        print(msg)
        return None

    # Prefer revised variants at manifest-selection stage, same policy as filesystem selection.
    groups: dict[str, list[tuple[bool, str]]] = {}
    for _, r in allowed_df.iterrows():
        base = r["_base"]
        stem = pathlib.Path(base).stem
        is_rv = stem.endswith("_rv")
        key = stem[:-3] if is_rv else stem
        groups.setdefault(key, []).append((is_rv, base))

    selected: set[str] = set()
    for items in groups.values():
        has_rv = any(is_rv for is_rv, _ in items)
        if has_rv:
            for is_rv, base in items:
                if is_rv:
                    selected.add(base)
                    selected.add(_normalized_name_key(base))
        else:
            for _, base in items:
                selected.add(base)
                selected.add(_normalized_name_key(base))
    return selected


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
    # Try UTF-8 first, then latin1. Keep last-resort parsing guarded by UNITID sanity checks.
    attempts = (
        ("chunked", dict(dtype=str, sep=sep, compression=compression, low_memory=False, index_col=False, chunksize=chunksize)),
        ("chunked", dict(dtype=str, sep=sep, compression=compression, engine="python", on_bad_lines="skip", index_col=False, chunksize=chunksize)),
        ("chunked", dict(dtype=str, sep=sep, compression=compression, engine="python", encoding="latin1", on_bad_lines="skip", index_col=False, chunksize=chunksize)),
        # Some files fail only in chunked python mode; parse full then yield slices.
        ("full", dict(dtype=str, sep=sep, compression=compression, engine="python", encoding="latin1", on_bad_lines="skip", index_col=False)),
        # Last-resort: treat quotes as regular characters.
        ("chunked", dict(
            dtype=str,
            sep=sep,
            compression=compression,
            engine="python",
            encoding="latin1",
            on_bad_lines="skip",
            index_col=False,
            quoting=csv.QUOTE_NONE,
            escapechar="\\",
            chunksize=chunksize,
        )),
    )
    last_err = None
    for mode, kwargs in attempts:
        try:
            if mode == "chunked":
                reader = pd.read_csv(fp, **kwargs)
                first = next(reader, None)
                if first is None:
                    return
                # Guard against malformed parses where UNITID exists but all values are non-numeric.
                cols = [str(c).strip().upper() for c in first.columns]
                if "UNITID" in cols:
                    unitid_col = first.columns[cols.index("UNITID")]
                    if pd.to_numeric(first[unitid_col], errors="coerce").notna().sum() == 0:
                        raise ValueError("suspicious parse: UNITID present but no numeric values in first chunk")
                yield first
                for chunk in reader:
                    yield chunk
                return

            full_df = pd.read_csv(fp, **kwargs)
            if full_df.empty:
                return
            cols = [str(c).strip().upper() for c in full_df.columns]
            if "UNITID" in cols:
                unitid_col = full_df.columns[cols.index("UNITID")]
                if pd.to_numeric(full_df[unitid_col], errors="coerce").notna().sum() == 0:
                    raise ValueError("suspicious full parse: UNITID present but no numeric values")
            for i in range(0, len(full_df), chunksize):
                yield full_df.iloc[i : i + chunksize].copy()
            return
        except Exception as e:
            last_err = e
            continue
    print(f"[warn] failed to read {fp}: {last_err}")
    return


def _chunk_cols(cols: list[str], size: int) -> Iterable[list[str]]:
    for i in range(0, len(cols), size):
        yield cols[i : i + size]


def write_na_drop_log(na_drop_log: list[dict], harmonize_qc_dir: pathlib.Path) -> tuple[pathlib.Path | None, dict]:
    """
    Persist NA-UNITID drop events and return a compact summary.
    """
    harmonize_qc_dir.mkdir(parents=True, exist_ok=True)
    summary = {"events": 0, "files": 0, "rows_dropped": 0}
    if not na_drop_log:
        return None, summary
    df = pd.DataFrame(na_drop_log)
    out_path = harmonize_qc_dir / "dropped_missing_unitid_by_file.csv"
    df.to_csv(out_path, index=False)
    summary["events"] = int(len(df))
    if {"year", "file"}.issubset(df.columns):
        summary["files"] = int(df[["year", "file"]].drop_duplicates().shape[0])
    if "dropped_rows_missing_UNITID" in df.columns:
        summary["rows_dropped"] = int(
            pd.to_numeric(df["dropped_rows_missing_UNITID"], errors="coerce").fillna(0).sum()
        )
    return out_path, summary


def print_end_summary(
    *,
    out_path: pathlib.Path,
    na_log_path: pathlib.Path | None,
    na_summary: dict,
    strict: bool,
) -> None:
    print("")
    print("=== Harmonize Summary ===")
    print(f"output_parquet: {out_path}")
    print(f"strict_mode: {strict}")
    if na_log_path is None:
        print("missing_UNITID_drops: none")
    else:
        print(f"missing_UNITID_drops_csv: {na_log_path}")
        print(f"missing_UNITID_drop_events: {na_summary.get('events', 0)}")
        print(f"missing_UNITID_files_affected: {na_summary.get('files', 0)}")
        print(f"missing_UNITID_rows_dropped_total: {na_summary.get('rows_dropped', 0)}")
    print("========================")
    print("")


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
    na_drop_log: list[dict] | None = None,
    harmonize_qc_dir: pathlib.Path | None = None,
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
        if dropped > 0 and not logged:
            print(f"{log_prefix}[warn] {fp.name} dropped_rows_missing_UNITID={dropped}")
        if dropped > 0 and na_drop_log is not None:
            na_drop_log.append(
                {
                    "year": year,
                    "file": fp.name,
                    "stage": "pre_melt",
                    "dropped_rows_missing_UNITID": int(dropped),
                    "rows_before": int(before_rows),
                    "rows_after": int(len(df)),
                }
            )
        if dropped > 0 and strict:
            if harmonize_qc_dir is not None and na_drop_log is not None:
                na_log_path, _ = write_na_drop_log(na_drop_log, harmonize_qc_dir)
                if na_log_path is not None:
                    print(f"[info] wrote {na_log_path}")
            raise SystemExit(
                f"[fatal] missing UNITID rows detected in {fp.name} (dropped={dropped})."
            )
        if df.empty:
            continue
        if strict and df["UNITID"].isna().any():
            raise SystemExit(f"[fatal] NA UNITID remained after pre-melt cleanup for {fp.name} year={year}")
        # Row index lets us map the imputation flag value for each melted variable row.
        df = df.reset_index(drop=True)
        df["_rowid"] = df.index.astype("int64")
        id_cols = ["UNITID", "_rowid"]
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
            before_long = len(long)
            long = long.dropna(subset=["UNITID"])
            dropped_post = before_long - len(long)
            if dropped_post > 0:
                print(f"{log_prefix}[warn] {fp.name} post_melt_dropped_rows_missing_UNITID={dropped_post}")
            if dropped_post > 0 and na_drop_log is not None:
                na_drop_log.append(
                    {
                        "year": year,
                        "file": fp.name,
                        "stage": "post_melt",
                        "dropped_rows_missing_UNITID": int(dropped_post),
                        "rows_before": int(before_long),
                        "rows_after": int(len(long)),
                    }
                )
            if dropped_post > 0 and strict:
                if harmonize_qc_dir is not None and na_drop_log is not None:
                    na_log_path, _ = write_na_drop_log(na_drop_log, harmonize_qc_dir)
                    if na_log_path is not None:
                        print(f"[info] wrote {na_log_path}")
                raise SystemExit(
                    f"[fatal] missing UNITID rows detected after melt in {fp.name} (dropped={dropped_post})."
                )
            if strict and long["UNITID"].isna().any():
                raise SystemExit(f"[fatal] NA UNITID remained after post-melt cleanup for {fp.name} year={year}")

            dups = dict_year.duplicated(["year", "varname"]).sum() if not dict_year.empty else 0
            if dups:
                raise SystemExit(
                    f"[fatal] dict_year not unique on (year,varname) for year={year} (dups={dups}). "
                    "This would multiply rows; fix preferred-source reduction."
                )

            before_merge = len(long)
            merged = long.merge(dict_year, on=["year", "varname"], how="left", validate="m:1")
            if len(merged) != before_merge:
                raise SystemExit(
                    f"[fatal] dictionary merge expanded rows for {fp.name} year={year}: "
                    f"{before_merge} -> {len(merged)}"
                )
            if strict and merged["UNITID"].isna().any():
                raise SystemExit(f"[fatal] NA UNITID survived into merged output for {fp.name} year={year}")

            missing_varnumber = int(merged["varnumber"].isna().sum())
            if missing_varnumber > 0 and strict:
                raise SystemExit(
                    f"[fatal] missing varnumber after merge for {fp.name} year={year} "
                    f"(rows={missing_varnumber})"
                )
            merged = merged.dropna(subset=["varnumber"])
            merged["imputationvar"] = merged["imputationvar"].fillna("").astype(str).str.upper()
            merged.loc[merged["imputationvar"].isin({"NAN", "NONE", "<NA>", "NAT"}), "imputationvar"] = ""
            imp_cols = sorted({c for c in merged["imputationvar"].unique() if c and c in df.columns})
            if imp_cols:
                imp_long = df[["_rowid"] + imp_cols].melt(
                    id_vars=["_rowid"],
                    value_vars=imp_cols,
                    var_name="imputationvar",
                    value_name="imputation_value",
                )
                merged = merged.merge(imp_long, on=["_rowid", "imputationvar"], how="left")
            else:
                merged["imputation_value"] = ""
            merged["imputation_value"] = merged["imputation_value"].fillna("")
            # Ensure source_file exists (from dictionary); fall back to data filename if missing.
            if "source_file" not in merged.columns:
                if strict:
                    raise SystemExit(f"[fatal] missing source_file column after merge for {fp.name} year={year}")
                merged["source_file"] = fp.name
            else:
                missing_source_file = int(merged["source_file"].isna().sum())
                if missing_source_file > 0 and strict:
                    raise SystemExit(
                        f"[fatal] missing source_file after merge for {fp.name} year={year} "
                        f"(rows={missing_source_file})"
                    )
                merged["source_file"] = merged["source_file"].fillna(fp.name)
            if strict and merged["source_file"].isna().any():
                raise SystemExit(f"[fatal] NA source_file in merged output for {fp.name} year={year}")
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
                "imputation_value",
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
    ap.add_argument("--checks-dir", default=None, help="QC output directory (default: $IPEDS_ROOT/Checks)")
    ap.add_argument(
        "--strict",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Fail fast on schema and merge anomalies (default: True for debug runs)",
    )
    ap.add_argument("--qc-only", action="store_true", help="Only run release QC and exit; no output is written")
    ap.add_argument("--release-qc-dir", default=None, help="QC dir for release validation")
    ap.add_argument("--log-file", default=None, help="Optional log file path")
    args = ap.parse_args()

    checks_dir = pathlib.Path(args.checks_dir) if args.checks_dir else (REPO_ROOT / "Checks")
    checks_dir.mkdir(parents=True, exist_ok=True)
    log_file = args.log_file if args.log_file else str(checks_dir / "logs" / "03_harmonize.log")
    setup_logging(log_file)

    years = parse_years(args.years)
    allowlist = {s.strip().lower() for s in args.release_allow.split(",") if s.strip()}
    qc_dir = pathlib.Path(args.release_qc_dir) if args.release_qc_dir else (checks_dir / "release_qc")
    harmonize_qc_dir = checks_dir / "harmonize_qc"
    harmonize_qc_dir.mkdir(parents=True, exist_ok=True)
    na_drop_log: list[dict] = []

    if args.qc_only:
        for year in years:
            year_root = pathlib.Path(args.root) / str(year)
            check_release_manifest(year_root, year, allowlist, args.release_strict, qc_dir)
        print("[info] release QC complete (qc-only)")
        return

    dict_df = pd.read_parquet(args.lake)
    dict_df["varnumber"] = dict_df["varnumber"].map(normalize_varnumber)
    dict_df["varname"] = dict_df["varname"].astype(str).str.upper()
    dict_df["imputationvar"] = dict_df["imputationvar"].fillna("").astype(str).str.upper()
    dict_df.loc[dict_df["imputationvar"].isin({"NAN", "NONE", "<NA>", "NAT"}), "imputationvar"] = ""
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
        manifest_allow = build_manifest_allowlist(year_root, year, allowlist, args.release_strict, qc_dir)
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
        files_after_rv = prefer_rv_files(all_files)
        skipped_rv = len(all_files) - len(files_after_rv)
        files = files_after_rv
        excluded_manifest = 0
        if manifest_allow is not None:
            before_allow = len(files)
            files = [
                fp
                for fp in files
                if fp.name.lower() in manifest_allow or _normalized_name_key(fp.name) in manifest_allow
            ]
            excluded_manifest = before_allow - len(files)
            if excluded_manifest > 0:
                print(f"[year {year}] manifest allowlist excluded {excluded_manifest} files (kept {len(files)}/{before_allow})")
            if args.release_strict and len(files) == 0:
                raise SystemExit(f"[fatal] manifest allowlist resulted in zero files for year={year}")
        if skipped_rv > 0:
            print(f"[year {year}] skipped {skipped_rv} non-_rv files where _rv exists")
        if qc_dir is not None:
            qc_dir.mkdir(parents=True, exist_ok=True)
            selected_set = {fp.name.lower() for fp in files}
            rv_set = {fp.name.lower() for fp in files_after_rv}
            qc_rows = []
            for fp in files_after_rv:
                name = fp.name.lower()
                qc_rows.append(
                    {
                        "year": year,
                        "file": fp.name,
                        "normalized_key": _normalized_name_key(fp.name),
                        "selected": int(name in selected_set),
                        "excluded_by_manifest": int(manifest_allow is not None and name in rv_set and name not in selected_set),
                    }
                )
            pd.DataFrame(qc_rows).to_csv(qc_dir / f"release_selected_files_{year}.csv", index=False)
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
                na_drop_log=na_drop_log,
                harmonize_qc_dir=harmonize_qc_dir,
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

    na_log_path: pathlib.Path | None = None
    na_summary: dict = {"events": 0, "files": 0, "rows_dropped": 0}
    try:
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
                        na_log_path, na_summary = write_na_drop_log(na_drop_log, harmonize_qc_dir)
                        if na_log_path is not None:
                            print(f"[info] wrote {na_log_path}")
                        print_end_summary(out_path=out_path, na_log_path=na_log_path, na_summary=na_summary, strict=args.strict)
                        return
            # Otherwise, process and write parts
            write_parquet_parts(
                out_path,
                iter_chunks_for_year(years[0]) if len(years) == 1 else (chunk for y in years for chunk in iter_chunks_for_year(y)),
                parts_dir,
            )
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
    finally:
        if na_log_path is None:
            na_log_path, na_summary = write_na_drop_log(na_drop_log, harmonize_qc_dir)
            if na_log_path is not None:
                print(f"[info] wrote {na_log_path}")
            print_end_summary(out_path=out_path, na_log_path=na_log_path, na_summary=na_summary, strict=args.strict)
    print(f"[info] wrote {out_path}")


if __name__ == "__main__":
    main()
