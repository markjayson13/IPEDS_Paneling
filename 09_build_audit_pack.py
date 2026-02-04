
#!/usr/bin/env python3
"""
Build a reviewer-facing audit pack for IPEDS_Paneling.

Creates audit_pack/ with reproducibility artifacts, QC outputs, and summary tables.
Optionally zips the pack.
"""
from __future__ import annotations

import argparse
import csv
import hashlib
import json
import os
import platform
import shutil
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path

import duckdb
import pandas as pd
import pyarrow.parquet as pq

DEFAULT_AUDIT_PACK_README = """# IPEDS_Paneling — Audit Pack (End-to-End Reproducibility & Validation)

**Project:** IPEDS_Paneling  
**Repository:** https://github.com/markjayson13/IPEDS_Paneling  
**Audit Pack Version:** {AUDIT_PACK_VERSION}  
**Code Version:** {GIT_TAG_OR_COMMIT_HASH}  
**Build Date (UTC):** {BUILD_DATETIME_UTC}  
**Built By:** {BUILT_BY}

## What this is
This bundle is designed for reviewers to verify:
1) **Reproducibility** (same inputs + same commit → same outputs)  
2) **Auditability** (transformations produce machine-readable QC artifacts)  
3) **Panel safety** (explicit defenses for schema drift, universe changes, and parent–child reporting)

Raw IPEDS files are not redistributed. Instead, this pack includes **input manifests + hashes**, **output hashes**, and **QC artifacts**.

## Where to start
- **Reproduction metadata:** `00_run/run_metadata.json`, `00_run/run_command.txt`
- **Input manifest + hashes:** `01_inputs/input_manifest.csv`, `01_inputs/input_hashes.csv`
- **Output hashes:** `00_run/output_hashes.csv`
- **QC index:** `06_qc/checks_index.md`

## Key checks (what to inspect)
### Dictionary / mapping
- `02_dictionary/dictionary_coverage_by_year_component.csv`
- `02_dictionary/mapping_collisions.csv` (+ `mapping_collisions_varname.csv`)
- `02_dictionary/drift_summary.csv`

### Long-panel integrity
- `03_long_panel/long_key_integrity.csv`
- `03_long_panel/long_schema.json`

### Wide-panel integrity
- `04_wide_panel/wide_integrity.csv`
- `04_wide_panel/wide_schema_diff.csv` (+ `wide_schema_diff_columns.csv`)

### Parent–child (PRCH)
- `05_prch/prch_rules.md`
- `05_prch/*.csv` (PRCH QC summaries copied from `Checks/prch_qc/` if present)

### Other QC (release / discrete collapse / wide build)
- `06_qc/release_qc/*`
- `06_qc/disc_qc/*`
- `06_qc/wide_qc/*`
- `06_qc/panel_qc/*`

## Performance
- `08_performance/output_sizes.csv`
"""


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


def parse_years(spec: str) -> list[int]:
    if ":" in spec:
        start, end = spec.split(":")
        return list(range(int(start), int(end) + 1))
    return [int(x) for x in spec.split(",") if x.strip()]


def sha256_file(path: Path, chunk_size: int = 1024 * 1024) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(chunk_size), b""):
            h.update(chunk)
    return h.hexdigest()


def ensure_dir(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)


def safe_copy(src: Path, dst: Path, missing: list[str]) -> None:
    if src and src.exists():
        ensure_dir(dst.parent)
        shutil.copy2(src, dst)
    else:
        missing.append(str(src))


def git_info() -> dict:
    info = {"commit": None, "describe": None}
    try:
        info["commit"] = subprocess.check_output(["git", "rev-parse", "HEAD"], text=True).strip()
    except Exception:
        pass
    try:
        info["describe"] = subprocess.check_output(["git", "describe", "--tags", "--always"], text=True).strip()
    except Exception:
        pass
    return info


def input_manifest(raw_root: Path, out_csv: Path) -> None:
    rows = []
    exts = {".csv", ".tsv", ".txt", ".gz"}
    for year_dir in sorted(raw_root.glob("[0-9][0-9][0-9][0-9]")):
        year = year_dir.name
        for fp in year_dir.rglob("*"):
            if not fp.is_file():
                continue
            if fp.suffix.lower() not in exts:
                continue
            if any("_dict" in part.lower() for part in fp.parts):
                continue
            component = fp.parent.name
            stat = fp.stat()
            rows.append(
                {
                    "year": year,
                    "component": component,
                    "filename": str(fp),
                    "size": stat.st_size,
                    "modified_time": datetime.fromtimestamp(stat.st_mtime, tz=timezone.utc).isoformat(),
                }
            )
    df = pd.DataFrame(rows)
    df.to_csv(out_csv, index=False)


def input_hashes(raw_root: Path, out_csv: Path) -> None:
    exts = {".csv", ".tsv", ".txt", ".gz"}
    rows = []
    for year_dir in sorted(raw_root.glob("[0-9][0-9][0-9][0-9]")):
        for fp in year_dir.rglob("*"):
            if not fp.is_file():
                continue
            if fp.suffix.lower() not in exts:
                continue
            if any("_dict" in part.lower() for part in fp.parts):
                continue
            rows.append({"filename": str(fp), "sha256": sha256_file(fp)})
    pd.DataFrame(rows).to_csv(out_csv, index=False)


def output_hashes(files: list[Path], out_csv: Path) -> None:
    rows = []
    for fp in files:
        if fp and fp.exists():
            rows.append({"filename": str(fp), "sha256": sha256_file(fp)})
    pd.DataFrame(rows).to_csv(out_csv, index=False)


def long_key_integrity(long_panel: Path, years: list[int], out_csv: Path) -> tuple[int, int, int]:
    con = duckdb.connect()
    years_list = ",".join(str(y) for y in years)
    q = f"""
        SELECT
          COUNT(*) AS rows,
          COUNT(*) - COUNT(DISTINCT (UNITID, year, varname)) AS dup_rows,
          SUM(CASE WHEN UNITID IS NULL OR year IS NULL OR varname IS NULL THEN 1 ELSE 0 END) AS missing_keys
        FROM read_parquet('{long_panel}')
        WHERE year IN ({years_list})
    """
    rows, dup_rows, missing_keys = con.execute(q).fetchone()
    pd.DataFrame([
        {"rows": rows, "duplicate_key_rows": dup_rows, "missing_key_rows": missing_keys}
    ]).to_csv(out_csv, index=False)
    return int(rows), int(dup_rows), int(missing_keys)


def long_schema(long_panel: Path, out_json: Path) -> None:
    pf = pq.ParquetFile(long_panel)
    schema = pf.schema.to_arrow_schema()
    obj = {
        "n_fields": len(schema),
        "fields": [
            {"name": f.name, "type": str(f.type), "nullable": bool(getattr(f, "nullable", True))}
            for f in schema
        ],
    }
    out_json.write_text(json.dumps(obj, indent=2))


def write_output_sizes(paths: list[Path], out_csv: Path) -> None:
    rows = []
    for p in paths:
        if p and p.exists():
            rows.append({"path": str(p), "size_bytes": p.stat().st_size})
    pd.DataFrame(rows).to_csv(out_csv, index=False)


def wide_integrity(wide_panel: Path, years: list[int], out_csv: Path) -> tuple[int, int, int]:
    con = duckdb.connect()
    years_list = ",".join(str(y) for y in years)
    q = f"""
        SELECT
          COUNT(*) AS rows,
          COUNT(*) - COUNT(DISTINCT (UNITID, year)) AS dup_rows,
          SUM(CASE WHEN UNITID IS NULL OR year IS NULL THEN 1 ELSE 0 END) AS missing_keys
        FROM read_parquet('{wide_panel}')
        WHERE year IN ({years_list})
    """
    rows, dup_rows, missing_keys = con.execute(q).fetchone()
    pf = pq.ParquetFile(wide_panel)
    cols = len(pf.schema.names)
    pd.DataFrame([
        {"rows": rows, "cols": cols, "duplicate_key_rows": dup_rows, "missing_key_rows": missing_keys}
    ]).to_csv(out_csv, index=False)
    return int(rows), int(dup_rows), int(missing_keys)


def wide_schema_diff(raw_path: Path, prch_path: Path | None, clean_path: Path | None, out_csv: Path) -> None:
    def cols(p: Path | None) -> set[str]:
        if not p or not p.exists():
            return set()
        return set(pq.ParquetFile(p).schema.names)

    raw_cols = cols(raw_path)
    prch_cols = cols(prch_path)
    clean_cols = cols(clean_path)

    rows = []
    for name, cset in [("raw", raw_cols), ("prchclean", prch_cols), ("clean", clean_cols)]:
        rows.append({"panel": name, "n_cols": len(cset)})

    # diff rows
    diff_rows = []
    if prch_cols:
        for c in sorted(raw_cols - prch_cols):
            diff_rows.append({"from": "raw", "to": "prchclean", "change": "removed", "column": c})
        for c in sorted(prch_cols - raw_cols):
            diff_rows.append({"from": "raw", "to": "prchclean", "change": "added", "column": c})
    if clean_cols:
        for c in sorted(raw_cols - clean_cols):
            diff_rows.append({"from": "raw", "to": "clean", "change": "removed", "column": c})
        for c in sorted(clean_cols - raw_cols):
            diff_rows.append({"from": "raw", "to": "clean", "change": "added", "column": c})

    pd.DataFrame(rows).to_csv(out_csv, index=False)
    if diff_rows:
        pd.DataFrame(diff_rows).to_csv(out_csv.parent / "wide_schema_diff_columns.csv", index=False)


def dictionary_coverage(dictionary_lake: Path, out_csv: Path) -> None:
    df = pd.read_parquet(dictionary_lake)
    df["varname"] = df["varname"].astype(str)
    out = (
        df.groupby(["year", "source_file"], dropna=False)
          .agg(
              vars_total=("varname", "count"),
              vars_unique=("varname", "nunique"),
          )
          .reset_index()
    )
    out.to_csv(out_csv, index=False)


def drift_summary(dictionary_lake: Path, years: list[int], out_csv: Path) -> None:
    df = pd.read_parquet(dictionary_lake)
    df["varname"] = df["varname"].astype(str)
    year_vars = {y: set(df.loc[df["year"] == y, "varname"].dropna().unique()) for y in years}
    rows = []
    for y in years:
        prev = year_vars.get(y - 1, set())
        curr = year_vars.get(y, set())
        new = curr - prev
        retired = prev - curr
        rows.append({"year": y, "new_vars": len(new), "retired_vars": len(retired), "net_change": len(new) - len(retired)})
    pd.DataFrame(rows).to_csv(out_csv, index=False)


def mapping_collisions(dictionary_lake: Path, out_csv: Path) -> None:
    df = pd.read_parquet(dictionary_lake)
    df["varname"] = df["varname"].astype(str)
    # collisions where a varnumber maps to multiple varnames
    a = df.groupby("varnumber")["varname"].nunique().reset_index(name="n_varnames")
    a = a[a["n_varnames"] > 1]
    # collisions where a varname maps to multiple varnumbers
    b = df.groupby("varname")["varnumber"].nunique().reset_index(name="n_varnumbers")
    b = b[b["n_varnumbers"] > 1]
    a.to_csv(out_csv, index=False)
    b.to_csv(out_csv.parent / "mapping_collisions_varname.csv", index=False)


def build_checks_index(checks_dir: Path, out_md: Path) -> None:
    lines = ["# Checks Index"]
    for fp in sorted(checks_dir.rglob("*")):
        if fp.is_file() and fp.suffix.lower() in {".csv", ".json", ".md"}:
            try:
                if fp.suffix.lower() == ".csv":
                    n = sum(1 for _ in fp.open("r", encoding="utf-8", errors="ignore")) - 1
                    lines.append(f"- {fp.relative_to(checks_dir)} — {n} rows")
                else:
                    lines.append(f"- {fp.relative_to(checks_dir)}")
            except Exception:
                lines.append(f"- {fp.relative_to(checks_dir)}")
    out_md.write_text("\n".join(lines))


def write_readme(out_path: Path, template: str, context: dict) -> None:
    for k, v in context.items():
        template = template.replace("{" + k + "}", str(v))
    out_path.write_text(template)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--out-dir", default="audit_pack")
    ap.add_argument("--zip", action="store_true")
    ap.add_argument("--overwrite", action=argparse.BooleanOptionalAction, default=True)
    ap.add_argument("--years", default="2004:2024")
    ap.add_argument("--raw-root", default=None, help="Raw_Cross_Section_Data (optional for input hashes)")
    repo_root = Path(os.environ.get("IPEDS_ROOT", str(Path(__file__).resolve().parent)))
    ap.add_argument("--checks-dir", default=str(repo_root / "Checks"))
    ap.add_argument("--dictionary", default=str(repo_root / "Dictionary" / "dictionary_lake.parquet"))
    ap.add_argument("--dictionary-codes", default=str(repo_root / "Dictionary" / "dictionary_codes.parquet"))
    ap.add_argument("--long-panel", default=str(repo_root / "Panels" / "2004-2024" / "panel_long_varnum_2004_2024.parquet"))
    ap.add_argument("--wide-raw", default=str(repo_root / "Panels" / "2004_2024_IPEDS_Raw_Panel_DS.parquet"))
    ap.add_argument("--wide-prch", default=str(repo_root / "Panels" / "2004_2024_IPEDS_PRCHclean_Panel_DS.parquet"))
    ap.add_argument("--wide-clean", default=str(repo_root / "Panels" / "2004_2024_IPEDS_clean_Panel_DS.parquet"))
    ap.add_argument("--zip-out", default=None, help="Optional zip output path (default: checks_dir/audit_pack.zip)")
    ap.add_argument("--run-command", default=None, help="Exact pipeline command string to record")
    ap.add_argument("--built-by", default="")
    ap.add_argument("--log-file", default=str(repo_root / "Checks" / "logs" / "09_build_audit_pack.log"), help="Optional log file path")
    ap.add_argument("--allow-duplicates", action=argparse.BooleanOptionalAction, default=False, help="Do not fail audit pack if long panel has duplicate or missing keys (record counts instead)")
    ap.add_argument("--spotcheck-cmd", default=None, help="Optional shell command to run spot checks; stdout/stderr saved to 07_spot_checks/spotcheck_run.log")
    args = ap.parse_args()

    setup_logging(args.log_file)

    years = parse_years(args.years)
    out_dir = Path(args.out_dir)
    if out_dir.exists() and args.overwrite:
        shutil.rmtree(out_dir)
    ensure_dir(out_dir)

    # folder tree
    folders = [
        "00_run", "01_inputs", "02_dictionary", "03_long_panel", "04_wide_panel",
        "05_prch", "06_qc", "07_spot_checks", "08_performance", "99_appendix"
    ]
    for f in folders:
        ensure_dir(out_dir / f)

    missing = []

    # 00_run
    run_dir = out_dir / "00_run"
    logs_dir = run_dir / "logs"
    ensure_dir(logs_dir)
    info = {
        "git": git_info(),
        "python_version": sys.version.split(" ")[0],
        "os": platform.platform(),
        "timestamp_utc": datetime.now(timezone.utc).isoformat(),
        "years": args.years,
        "built_by": args.built_by,
        "run_command": args.run_command or "",
    }
    (run_dir / "run_metadata.json").write_text(json.dumps(info, indent=2))
    (run_dir / "run_command.txt").write_text(args.run_command or "(not provided)")
    (run_dir / "run_exact.sh").write_text(args.run_command or "# provide exact pipeline command here\n")

    # Output hashes (always)
    outputs = [Path(args.long_panel), Path(args.wide_raw), Path(args.wide_prch), Path(args.wide_clean)]
    output_hashes(outputs, run_dir / "output_hashes.csv")

    # 01_inputs
    inputs_dir = out_dir / "01_inputs"
    if args.raw_root:
        raw_root = Path(args.raw_root)
        input_manifest(raw_root, inputs_dir / "input_manifest.csv")
        input_hashes(raw_root, inputs_dir / "input_hashes.csv")
    else:
        (inputs_dir / "input_manifest.csv").write_text("year,component,filename,size,modified_time\n")
        (inputs_dir / "input_hashes.csv").write_text("filename,sha256\n")

    # 02_dictionary
    dict_dir = out_dir / "02_dictionary"
    safe_copy(Path(args.dictionary), dict_dir / "dictionary_lake.parquet", missing)
    if Path(args.dictionary).exists():
        pd.read_parquet(args.dictionary).to_csv(dict_dir / "dictionary_lake.csv", index=False)
    if Path(args.dictionary_codes).exists():
        safe_copy(Path(args.dictionary_codes), dict_dir / "dictionary_codes.parquet", missing)
        pd.read_parquet(args.dictionary_codes).to_csv(dict_dir / "dictionary_codes.csv", index=False)
    dictionary_coverage(Path(args.dictionary), dict_dir / "dictionary_coverage_by_year_component.csv")
    mapping_collisions(Path(args.dictionary), dict_dir / "mapping_collisions.csv")
    drift_summary(Path(args.dictionary), years, dict_dir / "drift_summary.csv")

    # 03_long_panel
    long_dir = out_dir / "03_long_panel"
    safe_copy(Path(args.long_panel), long_dir / "panel_long_2004_2024.parquet", missing)
    rows, dup_rows, missing_keys = long_key_integrity(Path(args.long_panel), years, long_dir / "long_key_integrity.csv")
    long_schema(Path(args.long_panel), long_dir / "long_schema.json")

    # 04_wide_panel
    wide_dir = out_dir / "04_wide_panel"
    safe_copy(Path(args.wide_raw), wide_dir / "panel_wide_raw.parquet", missing)
    if Path(args.wide_prch).exists():
        safe_copy(Path(args.wide_prch), wide_dir / "panel_wide_prchclean.parquet", missing)
    if Path(args.wide_clean).exists():
        safe_copy(Path(args.wide_clean), wide_dir / "panel_wide_clean.parquet", missing)
    wide_integrity(Path(args.wide_raw), years, wide_dir / "wide_integrity.csv")
    wide_schema_diff(Path(args.wide_raw), Path(args.wide_prch) if Path(args.wide_prch).exists() else None, Path(args.wide_clean) if Path(args.wide_clean).exists() else None, wide_dir / "wide_schema_diff.csv")

    # 05_prch
    prch_dir = out_dir / "05_prch"
    prch_rules = prch_dir / "prch_rules.md"
    prch_rules.write_text(
        "# PRCH Cleaning Rules\n\n"
        "Child rows are identified via PRCH_* flags and only component-specific columns are nulled.\n"
        "See Cleaning/05_cleaning_panel.py for the exact mapping.\n"
    )
    prch_qc = Path(args.checks_dir) / "prch_qc"
    if prch_qc.exists():
        for fp in prch_qc.glob("*.csv"):
            safe_copy(fp, prch_dir / fp.name, missing)

    # 06_qc
    qc_dir = out_dir / "06_qc"
    for sub in ["release_qc", "disc_qc", "panel_qc", "wide_qc", "prch_qc"]:
        src = Path(args.checks_dir) / sub
        if src.exists():
            dst = qc_dir / sub
            ensure_dir(dst)
            for fp in src.rglob("*"):
                if fp.is_file():
                    safe_copy(fp, dst / fp.name, missing)

    # 07_spot_checks (placeholders + optional hook)
    spot_dir = out_dir / "07_spot_checks"
    ensure_dir(spot_dir)
    (spot_dir / "spotcheck_plan.md").write_text("Planned.\n")
    (spot_dir / "spotcheck_results.csv").write_text("varname,year,unitid,raw_value,panel_value,match\n")
    (spot_dir / "spotcheck_mismatches.md").write_text("Planned.\n")
    if args.spotcheck_cmd:
        log_path = spot_dir / "spotcheck_run.log"
        with log_path.open("w") as f:
            res = subprocess.run(args.spotcheck_cmd, shell=True, cwd=repo_root, stdout=f, stderr=subprocess.STDOUT)
        if res.returncode != 0:
            print(f"[warn] spotcheck command failed (see {log_path})")
    # 99_appendix placeholders
    (out_dir / "99_appendix" / "planned_checks.md").write_text("Planned checks not yet computed.\n")
    (out_dir / "99_appendix" / "planned_figures.md").write_text("Planned figures not yet generated.\n")
    artifact_dir = repo_root / "Artifacts"
    for fname in ["Figure_1_pipeline.svg", "section5_validation.md", "table3_validation_metrics_template.csv"]:
        src = artifact_dir / fname
        if src.exists():
            safe_copy(src, out_dir / "99_appendix" / fname, missing)

    # checks index
    build_checks_index(qc_dir, out_dir / "checks_index.md")
    safe_copy(out_dir / "checks_index.md", qc_dir / "checks_index.md", missing)

    # 08_performance (real, not placeholder)
    perf_dir = out_dir / "08_performance"
    ensure_dir(perf_dir)
    size_paths = [
        out_dir / "02_dictionary" / "dictionary_lake.parquet",
        out_dir / "03_long_panel" / "panel_long_2004_2024.parquet",
        out_dir / "04_wide_panel" / "panel_wide_raw.parquet",
        out_dir / "04_wide_panel" / "panel_wide_prchclean.parquet",
        out_dir / "04_wide_panel" / "panel_wide_clean.parquet",
        out_dir / "00_run" / "output_hashes.csv",
    ]
    write_output_sizes(size_paths, perf_dir / "output_sizes.csv")

    # Fail fast on integrity (unless explicitly allowed)
    if dup_rows > 0 or missing_keys > 0:
        msg = "Integrity check failed: duplicates or missing keys in long panel."
        if args.allow_duplicates:
            print(f"[warn] {msg} Continuing because --allow-duplicates was set.")
            (out_dir / "06_qc" / "long_integrity_warning.txt").write_text(
                f"{msg}\n"
                f"duplicate_key_rows={dup_rows}\n"
                f"missing_key_rows={missing_keys}\n"
            )
        else:
            raise SystemExit(msg)

    # README
    template_path = repo_root / "Artifacts" / "audit_pack_README_template.md"
    readme_template = template_path.read_text() if template_path.exists() else DEFAULT_AUDIT_PACK_README
    readme = (out_dir / "README.md")
    write_readme(
        readme,
        readme_template,
        {
            "AUDIT_PACK_VERSION": datetime.now(timezone.utc).strftime("%Y%m%d"),
            "GIT_TAG_OR_COMMIT_HASH": info["git"].get("describe") or info["git"].get("commit") or "",
            "BUILD_DATETIME_UTC": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M"),
            "BUILT_BY": args.built_by or "",
        },
    )

    if args.zip:
        zip_target = Path(args.zip_out) if args.zip_out else Path(args.checks_dir) / "audit_pack.zip"
        zip_target.parent.mkdir(parents=True, exist_ok=True)
        shutil.make_archive(str(zip_target).removesuffix(".zip"), "zip", root_dir=out_dir)

    if missing:
        (out_dir / "00_run" / "missing_artifacts.txt").write_text("\n".join(missing))

    print(f"Audit pack written to {out_dir}")


if __name__ == "__main__":
    main()
