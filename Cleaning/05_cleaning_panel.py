#!/usr/bin/env python3
"""
Parent/Child cleaning for the stitched wide panel.

Safest policy (Option A):
- Keep all UNITID-year rows.
- If a PRCH_* flag indicates CHILD, null out only that component's columns.
"""
from __future__ import annotations

import argparse
import sys
import time
from pathlib import Path
from typing import Callable

import pandas as pd
import pyarrow as pa
import pyarrow.dataset as ds
import pyarrow.compute as pc
import pyarrow.parquet as pq


def setup_logging(log_path: str | None) -> None:
    if not log_path:
        return
    log_file = Path(log_path)
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


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--input", required=True, help="Input stitched wide parquet")
    p.add_argument("--output", required=True, help="Output cleaned parquet")
    p.add_argument("--dictionary", required=True, help="dictionary_lake.parquet")
    p.add_argument("--qc-dir", default=None, help="Write QC summaries here")
    p.add_argument("--batch-rows", type=int, default=100_000, help="Batch size for streaming")
    p.add_argument("--log-every", type=int, default=50, help="Log progress every N batches")
    p.add_argument("--drop-imputation-flags", action=argparse.BooleanOptionalAction, default=False, help="Drop X* imputation columns")
    repo_root = pathlib.Path(os.environ.get("IPEDS_ROOT", pathlib.Path(__file__).resolve().parents[1]))
    p.add_argument("--log-file", default=str(repo_root / "Checks" / "logs" / "05_cleaning_panel.log"), help="Optional log file path")
    return p.parse_args()


def mode(series: pd.Series) -> str:
    s = series.dropna()
    if s.empty:
        return ""
    return s.mode().iat[0]


def build_var_source_map(dictionary_path: Path) -> dict[str, str]:
    df = pd.read_parquet(dictionary_path, columns=["varname", "source_file"])
    df["varname"] = df["varname"].fillna("").astype(str).str.strip().str.upper()
    df["source_file"] = df["source_file"].fillna("").astype(str).str.strip()
    df = df[df["varname"] != ""]
    return df.groupby("varname")["source_file"].agg(mode).to_dict()


def main() -> None:
    args = parse_args()
    setup_logging(args.log_file)
    in_path = Path(args.input)
    out_path = Path(args.output)
    out_path.parent.mkdir(parents=True, exist_ok=True)

    var_to_source = build_var_source_map(Path(args.dictionary))

    dataset = ds.dataset(str(in_path), format="parquet")
    if "year" not in dataset.schema.names:
        raise SystemExit("Input must contain a 'year' column for PRCH cleaning.")

    # Guard: refuse to run if only a single year is detected
    years: set[int] = set()
    for batch in dataset.to_batches(columns=["year"], batch_size=min(args.batch_rows, 200_000)):
        uniq = pc.unique(batch.column(0)).to_pylist()
        for v in uniq:
            if v is None:
                continue
            years.add(int(v))
    if len(years) == 1:
        yr = next(iter(years))
        raise SystemExit(
            f"Refusing to run: input appears to contain only one year ({yr}). "
            "Provide the full stitched panel to avoid a single-year cleaned output."
        )
    years_sorted = sorted(years)
    all_cols = dataset.schema.names
    prch_flags = [c for c in all_cols if c.upper().startswith("PRCH")]

    # Component source_file groups
    group_sets: dict[str, set[str]] = {
        "PRCH_ADM": {"ADM"},
        "PRCH_AL": {"AL"},
        "PRCH_C": {"C_A", "C_B", "C_C", "CDEP"},
        "PRCH_COS": {"COST"},
        "PRCH_E12": {"E12"},
        "PRCH_EAP": {"EAP"},
        "PRCH_EF": {"EFA", "EFA_DIST", "EFB", "EFC", "EFCP", "EFFY", "EFFY_DIST", "EFIA"},
        "PRCH_F": {"F_F", "F_FA", "F_FA_F", "F_FA_G"},
        "PRCH_GR": {"GR", "GR_PELL_SSL"},
        "PRCH_GR2": {"GR200"},
        "PRCH_HR": {"EAP", "SAL_A", "SAL_A_LT", "SAL_B", "SAL_FACULTY", "SAL_IS"},
        "PRCH_OM": {"OM"},
        "PRCH_S": {"S_ABD", "S_CN", "S_F", "S_G", "S_IS", "S_NH", "S_OC", "S_SIS"},
        "PRCH_SA": {"SFA", "SFAV"},
        "PRCH_SFA": {"SFA", "SFAV"},
    }

    # Extra predicates for fuzzy matching
    group_pred: dict[str, Callable[[str], bool]] = {
        "PRCH_F": lambda sf: sf.startswith("F"),
        "PRCH_EF": lambda sf: sf.startswith("EF") or sf.startswith("EFFY"),
        "PRCH_S": lambda sf: sf.startswith("S_"),
    }

    # Build column lists per flag
    flag_cols: dict[str, list[str]] = {f: [] for f in group_sets}
    for col in all_cols:
        if col in prch_flags:
            continue
        sf = var_to_source.get(col.upper(), "")
        if not sf:
            continue
        for flag, sset in group_sets.items():
            if sf in sset:
                flag_cols[flag].append(col)
        # predicate-based adds
        for flag, pred in group_pred.items():
            if pred(sf):
                flag_cols[flag].append(col)

    # de-duplicate columns per flag
    flag_cols = {k: sorted(set(v)) for k, v in flag_cols.items() if v}

    qc_counts: dict[tuple[int, str], int] = {}

    writer = None
    batch_idx = 0
    rows_processed = 0
    years_seen: set[int] = set()
    last_log = time.time()
    target_schema = dataset.schema
    for y in years_sorted:
        print(f"[year] start {y}")
        year_rows = 0
        scanner = dataset.scanner(filter=ds.field("year") == y, batch_size=args.batch_rows)
        for batch in scanner.to_batches():
            df = batch.to_pandas()
            batch_idx += 1
            rows_processed += len(df)
            year_rows += len(df)
            for flag, cols in flag_cols.items():
                if flag not in df.columns:
                    continue
                # child logic
                flag_num = pd.to_numeric(df[flag], errors="coerce")
                if flag == "PRCH_F":
                    child_mask = flag_num.isin([2, 3, 5])
                else:
                    child_mask = flag_num.isin([2])
                if not child_mask.any():
                    continue
                df.loc[child_mask, cols] = pd.NA
                # QC counts by year
                counts = df.loc[child_mask, "year"].value_counts()
                for yy, cnt in counts.items():
                    key = (int(yy), flag)
                    qc_counts[key] = qc_counts.get(key, 0) + int(cnt)

            table = pa.Table.from_pandas(df, preserve_index=False)
            # Align schema to avoid mismatches across years/batches
            for field in target_schema:
                if field.name not in table.column_names:
                    table = table.append_column(field.name, pa.nulls(table.num_rows, type=field.type))
            table = table.select([f.name for f in target_schema])
            try:
                table = table.cast(target_schema, safe=False)
            except Exception:
                # If casting fails, still write aligned columns; Parquet will store as-is.
                pass
            if args.drop_imputation_flags:
                drop_cols = [c for c in table.column_names if c.upper().startswith("X")]
                if drop_cols:
                    table = table.drop(drop_cols)
            if writer is None:
                schema_to_write = table.schema if args.drop_imputation_flags else target_schema
                writer = pq.ParquetWriter(out_path, schema_to_write, compression="snappy")
            writer.write_table(table)

            if args.log_every and batch_idx % args.log_every == 0:
                now = time.time()
                if now - last_log >= 1:
                    print(
                        f"[progress] batches={batch_idx} rows={rows_processed:,} "
                        f"current_year={y} year_rows={year_rows:,}"
                    )
                    sys.stdout.flush()
                    last_log = now
        print(f"[year] done {y} rows={year_rows:,}")

    if writer:
        writer.close()
        print(f"Wrote cleaned panel: {out_path}")

    if args.qc_dir:
        qc_dir = Path(args.qc_dir)
        qc_dir.mkdir(parents=True, exist_ok=True)
        rows = [
            {"year": y, "flag": flag, "child_rows": cnt}
            for (y, flag), cnt in sorted(qc_counts.items())
        ]
        pd.DataFrame(rows).to_csv(qc_dir / "prch_clean_summary.csv", index=False)
        # also record which columns were cleaned per flag
        col_rows = []
        for flag, cols in flag_cols.items():
            for c in cols:
                col_rows.append({"flag": flag, "column": c})
        pd.DataFrame(col_rows).to_csv(qc_dir / "prch_clean_columns.csv", index=False)
        print(f"QC written to {qc_dir}")


if __name__ == "__main__":
    main()
