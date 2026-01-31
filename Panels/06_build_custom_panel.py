#!/usr/bin/env python3
"""
Build a custom wide panel by selecting specific variables from a wide panel.

Always keeps UNITID and year. Users specify additional varnames via --vars or --vars-file.
Supports parquet or CSV output (streamed).
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

import pyarrow as pa
import pyarrow.csv as pcsv
import pyarrow.dataset as ds
import pyarrow.parquet as pq


def parse_years(spec: str) -> list[int]:
    if ":" in spec:
        start, end = spec.split(":")
        return list(range(int(start), int(end) + 1))
    return [int(x.strip()) for x in spec.split(",") if x.strip()]


def load_vars(vars_arg: str | None, vars_file: str | None) -> list[str]:
    out: list[str] = []
    if vars_arg:
        out.extend([v.strip() for v in vars_arg.split(",") if v.strip()])
    if vars_file:
        p = Path(vars_file)
        if not p.exists():
            raise FileNotFoundError(p)
        for line in p.read_text().splitlines():
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            out.extend([v.strip() for v in line.split(",") if v.strip()])
    # de-dup while preserving order
    seen: set[str] = set()
    deduped: list[str] = []
    for v in out:
        key = v.strip()
        if not key or key in seen:
            continue
        seen.add(key)
        deduped.append(key)
    return deduped


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--input", required=True, help="Input wide parquet (raw/clean)")
    ap.add_argument("--output", required=True, help="Output file path (.parquet or .csv)")
    ap.add_argument("--vars", default=None, help="Comma-separated list of varnames")
    ap.add_argument("--vars-file", default=None, help="File with varnames (one per line or comma-separated)")
    ap.add_argument("--years", default=None, help='Optional year filter, e.g. "2004:2024" or "2004,2006"')
    ap.add_argument("--format", choices=["parquet", "csv"], default="parquet", help="Output format")
    ap.add_argument("--batch-rows", type=int, default=100_000, help="Batch size for streaming output")
    ap.add_argument("--strict", action="store_true", help="Fail if any requested varname is missing")
    args = ap.parse_args()

    vars_requested = load_vars(args.vars, args.vars_file)
    if not vars_requested:
        raise SystemExit("Provide --vars or --vars-file with at least one variable.")

    dataset = ds.dataset(args.input, format="parquet")
    schema = dataset.schema

    # Resolve column names case-insensitively.
    name_map = {name.upper(): name for name in schema.names}
    year_col = name_map.get("YEAR", "year" if "year" in schema.names else None)
    unitid_col = name_map.get("UNITID", "UNITID" if "UNITID" in schema.names else None)
    if not year_col or not unitid_col:
        raise SystemExit("Input must include UNITID and year columns.")

    requested_upper = [v.upper() for v in vars_requested]
    resolved = []
    missing = []
    for v in requested_upper:
        if v in name_map:
            resolved.append(name_map[v])
        else:
            missing.append(v)
    if missing:
        msg = f"Missing {len(missing)} vars: {', '.join(missing[:20])}"
        if args.strict:
            raise SystemExit(msg)
        print("[warn]", msg)

    cols = [year_col, unitid_col] + resolved
    # De-dup while preserving order
    seen: set[str] = set()
    cols = [c for c in cols if not (c in seen or seen.add(c))]

    filt = None
    if args.years:
        years = parse_years(args.years)
        if len(years) == 1:
            filt = ds.field(year_col) == years[0]
        elif ":" in args.years:
            start, end = years[0], years[-1]
            filt = (ds.field(year_col) >= start) & (ds.field(year_col) <= end)
        else:
            filt = ds.field(year_col).isin(years)

    out_path = Path(args.output)
    out_path.parent.mkdir(parents=True, exist_ok=True)

    if args.format == "parquet":
        writer = None
        rows = 0
        for batch in dataset.to_batches(columns=cols, filter=filt, batch_size=args.batch_rows):
            rows += batch.num_rows
            if writer is None:
                writer = pq.ParquetWriter(out_path, batch.schema, compression="snappy")
            writer.write_batch(batch)
            if rows % (args.batch_rows * 10) == 0:
                print(f"[progress] rows={rows:,}")
        if writer:
            writer.close()
        print(f"Wrote {out_path} rows={rows:,} cols={len(cols)}")
    else:
        # Stream CSV
        sink = pa.OSFile(str(out_path), "wb")
        writer = None
        rows = 0
        for batch in dataset.to_batches(columns=cols, filter=filt, batch_size=args.batch_rows):
            table = pa.Table.from_batches([batch])
            if writer is None:
                writer = pcsv.CSVWriter(sink, table.schema)
            writer.write_table(table)
            rows += batch.num_rows
            if rows % (args.batch_rows * 10) == 0:
                print(f"[progress] rows={rows:,}")
        if writer:
            writer.close()
        sink.close()
        print(f"Wrote {out_path} rows={rows:,} cols={len(cols)}")


if __name__ == "__main__":
    main()
