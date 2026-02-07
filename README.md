# IPEDS Paneling

Build reproducible IPEDS panel datasets (2004-2024) from raw NCES cross-sections.

## Pipeline Graphics

![Figure 1. IPEDS Harmonization Pipeline (2004-2024)](Artifacts/Figure_1_pipeline.svg)

## Main Outputs

- `Panels/2004_2024_IPEDS_Raw_Panel_DS.parquet`
- `Panels/2004_2024_IPEDS_PRCHclean_Panel_DS.parquet`
- `Panels/2004_2024_IPEDS_clean_Panel_DS.parquet`

## Prerequisites

- Python 3.10+
- Input folder: `Raw_Cross_Section_Data/`
- Dictionary file: `Dictionary/dictionary_lake.parquet` (or build it with the command below)

## Setup

```bash
pip install -r requirements.txt
export IPEDS_ROOT="/path/to/IPEDS_Paneling"
```

If `requirements.txt` install has issues, install core runtime packages directly:

```bash
pip install duckdb pandas pyarrow openpyxl xlrd pyyaml requests beautifulsoup4 matplotlib
```

## Quick Start (One Command)

```bash
bash manual_commands.sh
```

This runs the full pipeline and writes outputs to `Panels/` and QC results to `Checks/`.

## Full Step-by-Step Run

### 1) Build dictionary (only if missing)

```bash
python3 Scripts/02_dictionary_ingest.py \
  --root "$IPEDS_ROOT/Raw_Cross_Section_Data" \
  --output "$IPEDS_ROOT/Dictionary/dictionary_lake.parquet" \
  --output-csv "$IPEDS_ROOT/Dictionary/dictionary_lake.csv" \
  --codes-output "$IPEDS_ROOT/Dictionary/dictionary_codes.parquet" \
  --codes-output-csv "$IPEDS_ROOT/Dictionary/dictionary_codes.csv"
```

### 2) Run full panel build

```bash
python3 Scripts/00_run_all.py \
  --root "$IPEDS_ROOT/Raw_Cross_Section_Data" \
  --lake "$IPEDS_ROOT/Dictionary/dictionary_lake.parquet" \
  --years "2004:2024" \
  --cross-sections-dir "$IPEDS_ROOT/Cross_sections" \
  --parts-dir-base "$IPEDS_ROOT/Cross_sections" \
  --stitch-out "$IPEDS_ROOT/Panels/2004-2024/panel_long_varnum_2004_2024.parquet" \
  --wide-out-dir "$IPEDS_ROOT/Panels/wide_2004_2024" \
  --wide-write-single "$IPEDS_ROOT/Panels/2004_2024_IPEDS_Raw_Panel_DS.parquet" \
  --stitch-wide \
  --stitch-wide-out "$IPEDS_ROOT/Panels/2004_2024_IPEDS_Raw_Panel_DS.parquet" \
  --run-cleaning \
  --prch-clean-out "$IPEDS_ROOT/Panels/2004_2024_IPEDS_PRCHclean_Panel_DS.parquet" \
  --clean-out "$IPEDS_ROOT/Panels/2004_2024_IPEDS_clean_Panel_DS.parquet" \
  --prch-qc-dir "$IPEDS_ROOT/Checks/prch_qc" \
  --disc-qc-dir "$IPEDS_ROOT/Checks/disc_qc" \
  --qc-dir "$IPEDS_ROOT/Checks/wide_qc" \
  --release-allow "revised,final" \
  --release-qc-dir "$IPEDS_ROOT/Checks/release_qc" \
  --log-dir "$IPEDS_ROOT/Checks/logs" \
  --no-final-dedupe \
  --drop-imputation-flags
```

## Build a Custom Panel

Use explicit variables:

```bash
python3 Scripts/06_build_custom_panel.py \
  --input "$IPEDS_ROOT/Panels/2004_2024_IPEDS_clean_Panel_DS.parquet" \
  --vars "INSTNM,SECTOR,TUITION1,PELL_RECP" \
  --output "$IPEDS_ROOT/Panels/custom_panel.parquet"
```

Use a variable list file:

```bash
python3 Scripts/06_build_custom_panel.py \
  --input "$IPEDS_ROOT/Panels/2004_2024_IPEDS_clean_Panel_DS.parquet" \
  --vars-file "Customize_Panel/selectedvars.txt" \
  --output "$IPEDS_ROOT/Panels/custom_panel.parquet"
```

## QC Folders

- `Checks/release_qc/` release manifest checks
- `Checks/harmonize_qc/` harmonization checks (including dropped missing UNITID rows)
- `Checks/disc_qc/` discrete-collapse conflicts
- `Checks/wide_qc/` wide panel summary checks
- `Checks/prch_qc/` parent-child cleaning checks
- `Checks/panel_qc/` raw vs PRCH-clean comparison

## Repository Layout

- `Scripts/` main pipeline scripts
- `Scripts/QA_QC/` QA/QC scripts
- `Customize_Panel/` variable-list inputs for custom paneling
- `Artifacts/` tracked figures/docs only
- `manual_commands.sh` one-command pipeline runner

## Troubleshooting

- `zsh: parse error near ')'`
  - You likely pasted a broken multiline block. Run `bash manual_commands.sh` or save commands to a `.sh` file and run with `bash`.
- `Missing dictionary_lake.parquet`
  - Run `Scripts/02_dictionary_ingest.py` first.
- `ModuleNotFoundError: duckdb`
  - Install dependencies in your active environment.
- Out-of-memory during final dedupe
  - Keep `--no-final-dedupe` (already set in recommended command).

## Notes

- `Scripts/03_harmonize.py` excludes mission folders from ingestion.
- Keep generated large outputs out of git (`Raw_Cross_Section_Data/`, `Cross_sections/`, `Panels/`, `Checks/`).
