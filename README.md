# IPEDS Paneling

Build reproducible IPEDS panel datasets from NCES cross-sections with strict release checks, harmonization guards, and DuckDB-backed analysis-wide exports.

![Figure 1. IPEDS Harmonization Pipeline (2004-2024)](Artifacts/Figure_1_pipeline.svg)

## Overview

This repository supports two outputs:

- `Panels/2004_2024_IPEDS_clean_Panel_DS.parquet` (full cleaned panel, 2004-2024)
- `Panels/panel_wide_analysis_2004_2023.parquet` (analysis release window, 2004-2023 by default)

`2024` is treated as provisional/schema-transition for analysis-wide builds.

`Scripts/04_build_wide_panel.py` now uses DuckDB as the build/query engine for wide-panel construction, QC, and partition export. Python remains the orchestration layer.

## Setup

```bash
python3 -m pip install -r requirements.txt
export IPEDS_ROOT="/path/to/IPEDS_Paneling"
```

Required input under `"$IPEDS_ROOT"`:

- `Raw_Cross_Section_Data/`
- `Dictionary/dictionary_lake.parquet`

## Quick Start (Full Clean Panel)

```bash
bash manual_commands.sh
```

This runs `Scripts/00_run_all.py` and produces:

- `Panels/2004-2024/panel_long_varnum_2004_2024.parquet`
- `Panels/2004_2024_IPEDS_Raw_Panel_DS.parquet`
- `Panels/2004_2024_IPEDS_PRCHclean_Panel_DS.parquet`
- `Panels/2004_2024_IPEDS_clean_Panel_DS.parquet`

## Build Analysis-Wide Panel (Lane Split)

Run this after harmonization output exists:

```bash
python3 Scripts/04_build_wide_panel.py \
  --input "$IPEDS_ROOT/Panels/2004-2024/panel_long_varnum_2004_2024.parquet" \
  --out_dir "$IPEDS_ROOT/Panels/wide_analysis_parts" \
  --years "2004:2023" \
  --dictionary "$IPEDS_ROOT/Dictionary/dictionary_lake.parquet" \
  --lane-split \
  --dim-sources "C_A,C_B,C_C,CDEP,EAP,IC_CAMPUSES,IC_PCCAMPUSES,F_FA_F,F_FA_G" \
  --dim-prefixes "C_,EF,GR,GR200,SAL,S_,OM,DRV" \
  --exclude-vars "SPORT1,SPORT2,SPORT3,SPORT4" \
  --scalar-long-out "$IPEDS_ROOT/Panels/panel_long_scalar_unique.parquet" \
  --dim-long-out "$IPEDS_ROOT/Panels/panel_long_dim.parquet" \
  --wide-analysis-out "$IPEDS_ROOT/Panels/panel_wide_analysis_2004_2023.parquet" \
  --typed-output \
  --drop-empty-cols \
  --collapse-disc \
  --drop-disc-components \
  --qc-dir "$IPEDS_ROOT/Checks/wide_qc" \
  --disc-qc-dir "$IPEDS_ROOT/Checks/disc_qc" \
  --duckdb-path "$IPEDS_ROOT/build/ipeds_build.duckdb" \
  --duckdb-temp-dir "$IPEDS_ROOT/build/duckdb_tmp" \
  --persist-duckdb
```

Analysis-wide schema notes:

- By default, lane-split analysis builds use `Artifacts/legacy_analysis_schema_seed.csv` to seed legacy compatibility columns.
- Those seeded columns are visible in year partitions and recorded in `Checks/wide_qc/qc_seeded_legacy_columns.csv`.
- If `--drop-globally-null-post` remains enabled, globally null compatibility columns are still removed from the stitched single-file output and listed in `Checks/wide_qc/qc_globally_null_columns_dropped.csv`.
- Use `--no-legacy-analysis-schema` if you want the narrower semantic-window schema surface instead of the legacy-compatible schema contract.

## Schema Audit And Monitoring

Target-lineage audit without running the year loop:

```bash
python3 Scripts/04_build_wide_panel.py \
  --input "$IPEDS_ROOT/Panels/2004-2024/panel_long_varnum_2004_2024.parquet" \
  --out_dir "$IPEDS_ROOT/Panels/wide_analysis_parts" \
  --years "2004:2023" \
  --dictionary "$IPEDS_ROOT/Dictionary/dictionary_lake.parquet" \
  --lane-split \
  --qc-dir "$IPEDS_ROOT/Checks/wide_qc" \
  --lineage-only
```

This writes `Checks/wide_qc/qc_target_lineage.csv` and stops before per-year builds.

Monitored real-data build with durable logs and telemetry:

```bash
python3 Scripts/QA_QC/03_monitored_analysis_build.py \
  --input "$IPEDS_ROOT/Panels/2004-2024/panel_long_varnum_2004_2024.parquet" \
  --dictionary "$IPEDS_ROOT/Dictionary/dictionary_lake.parquet" \
  --years "2004:2023"
```

The monitored runner writes `build.log`, `monitor.log`, `build_telemetry.json`, and `run_meta.json` under `Checks/real_parity_runs/<run_id>/`.

Parity harness:

```bash
python3 Scripts/QA_QC/02_wide_parity.py \
  --input "$IPEDS_ROOT/Panels/2004-2024/panel_long_varnum_2004_2024.parquet" \
  --dictionary "$IPEDS_ROOT/Dictionary/dictionary_lake.parquet" \
  --years "2004:2023" \
  --lane-split \
  --typed-output \
  --drop-empty-cols \
  --collapse-disc \
  --drop-disc-components \
  --parity-contract legacy_schema
```

Use `--parity-contract semantic_window` to compare the narrower semantic-window schema instead of the legacy-compatible analysis schema.

## Custom Panel

Use the input that matches your goal:

- Analysis-ready subset (recommended): `Panels/panel_wide_analysis_2004_2023.parquet`
- Broad full panel (includes 2024): `Panels/2004_2024_IPEDS_clean_Panel_DS.parquet`

`UNITID` and `year` are always included automatically.

Export a custom panel as parquet:

```bash
python3 Scripts/06_build_custom_panel.py \
  --input "$IPEDS_ROOT/Panels/panel_wide_analysis_2004_2023.parquet" \
  --vars-file "Customize_Panel/selectedvars.txt" \
  --years "2004:2023" \
  --format parquet \
  --output "$IPEDS_ROOT/Panels/custom_panel_2004_2023.parquet"
```

Export the same custom panel as CSV:

```bash
python3 Scripts/06_build_custom_panel.py \
  --input "$IPEDS_ROOT/Panels/panel_wide_analysis_2004_2023.parquet" \
  --vars-file "Customize_Panel/selectedvars.txt" \
  --years "2004:2023" \
  --format csv \
  --output "$IPEDS_ROOT/Panels/custom_panel_2004_2023.csv"
```

## QA/QC Outputs

- `Checks/release_qc/` release-manifest validation and selected-file evidence
- `Checks/harmonize_qc/` missing-UNITID drop logs and harmonize summaries
- `Checks/disc_qc/` discrete-collapse conflicts and collapse map
- `Checks/wide_qc/qc_scalar_conflicts.csv` scalar-lane key conflicts
- `Checks/wide_qc/qc_anti_garbage_failures.csv` blocked dimension identifiers in wide targets
- `Checks/wide_qc/qc_cast_report.csv` typed-cast parse report
- `Checks/wide_qc/qc_target_lineage.csv` target-universe lineage through scalar filter, discrete collapse, anti-garbage, and legacy seeding
- `Checks/wide_qc/qc_seeded_legacy_columns.csv` legacy compatibility columns injected into analysis-wide targets
- `Checks/wide_qc/qc_globally_null_columns_dropped.csv` globally null columns removed post-build
- `Checks/prch_qc/` PRCH cleaning evidence

## Troubleshooting

- `zsh: parse error near ')'`:
  - Run commands from a `.sh` file or run `bash manual_commands.sh` directly.
- `ModuleNotFoundError: duckdb`:
  - Install dependencies in the active Python environment.
- `scalar conflict gate failed`:
  - Inspect `Checks/wide_qc/qc_scalar_conflicts.csv`; add true dimensioned sources/prefixes or exclude known problem vars.
- `anti-garbage gate failed`:
  - Inspect `Checks/wide_qc/qc_anti_garbage_failures.csv`; treat those variables as dimensioned or exclude them.
- `OutOfMemoryException` during large analysis-wide builds:
  - Run `Scripts/QA_QC/03_monitored_analysis_build.py` to capture phase logs and resource telemetry under `Checks/real_parity_runs/`.
  - Inspect the latest `build.log`, `monitor.log`, and `build_telemetry.json` to determine whether memory pressure is happening in target discovery, scalar conflict scans, lane exports, or pivoting.
- Unexpected analysis-wide columns:
  - Inspect `Checks/wide_qc/qc_target_lineage.csv` and `Checks/wide_qc/qc_seeded_legacy_columns.csv`.
  - Use `--no-legacy-analysis-schema` to disable the legacy-compatible placeholder columns if you want only the semantic-window target set.

## Notes

- Keep generated large data out of git: `Raw_Cross_Section_Data/`, `Cross_sections/`, `Panels/`, `Checks/`, `build/`, `*.duckdb`, `*.duckdb.wal`.
- `UNITID` is documented in the dictionary as controlled metadata and also used as the panel key in harmonization.
- The legacy analysis schema seed manifest is `Artifacts/legacy_analysis_schema_seed.csv`.
