# IPEDS Paneling

Build reproducible IPEDS panel datasets (2004-2024) from raw NCES cross-sections.

## Pipeline Graphics

![Figure 1. IPEDS Harmonization Pipeline (2004-2024)](Artifacts/Figure_1_pipeline.svg)

- 1) Downloading Cross-Sectional Complete Data
  - This script automates the download and extraction of IPEDS "Complete Data Files" for a specified range of years (2004-2024).
- 2) Ingesting Dictionary
  - Build a lean IPEDS dictionary lake (2004–2024) with core metadata only.
- 3) Harmonize Variables
  - Harmonizer that builds a LONG panel with provenance-preserving grain.
- 4) Build Wide Panel
  - Build a wide institution–year panel from the stitched long panel.
- 5) Parent/Child Cleaning
  - Parent/Child cleaning for the stitched wide panel.
- 6) Customizing Panel Data
  - Build a custom wide panel by selecting specific variables from a wide panel.

## Main Outputs

- `Panels/2004_2024_IPEDS_Raw_Panel_DS.parquet`
- `Panels/2004_2024_IPEDS_PRCHclean_Panel_DS.parquet`
- `Panels/2004_2024_IPEDS_clean_Panel_DS.parquet`

## Prerequisites

- Python 3.10+
- Input folder: `Raw_Cross_Section_Data/`
- Dictionary file: `Dictionary/dictionary_lake.parquet` 

## Setup

```bash
pip install -r requirements.txt
export IPEDS_ROOT="/path/to/IPEDS_Paneling"
```

If `requirements.txt` install has issues, install core runtime packages directly:

```bash
pip install duckdb pandas pyarrow openpyxl xlrd pyyaml requests beautifulsoup4 matplotlib
```

## Quick Start

```bash
bash manual_commands.sh
```

This runs the full pipeline and writes outputs to `Panels/` and QC results to `Checks/`.
It will output a cleaned wide panel dataset ready for analysis `Panels/2004_2024_IPEDS_clean_Panel_DS.parquet`

## Customize Panel Data

Using `Panels/2004_2024_IPEDS_clean_Panel_DS.parquet`, the panel data is customizable by keeping only relevant variables.
Use `panel_var_reference.xlsx` as a reference to select variables. Its title and description is included. Use the given varnme in `panel_var_reference.xlsx` to properly extract the correct variable. Customizing can be done either direct shell code or using `selectedvars.txt` to list selected variables

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
