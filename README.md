# IPEDS Panel Builder (2004-2024)

Builds a reproducible IPEDS panel from raw cross-sections to:
- `Panels/2004_2024_IPEDS_Raw_Panel_DS.parquet`
- `Panels/2004_2024_IPEDS_PRCHclean_Panel_DS.parquet`
- `Panels/2004_2024_IPEDS_clean_Panel_DS.parquet`

## Setup

```bash
pip install -r requirements.txt
```

Set data root (optional, defaults to repo root):

```bash
export IPEDS_ROOT="/path/to/IPEDS_Paneling"
```

Required input folders/files under `IPEDS_ROOT`:
- `Raw_Cross_Section_Data/`
- `Dictionary/dictionary_lake.parquet`

If dictionary is missing:

```bash
python3 Scripts/02_dictionary_ingest.py \
  --root "$IPEDS_ROOT/Raw_Cross_Section_Data" \
  --output "$IPEDS_ROOT/Dictionary/dictionary_lake.parquet" \
  --output-csv "$IPEDS_ROOT/Dictionary/dictionary_lake.csv" \
  --codes-output "$IPEDS_ROOT/Dictionary/dictionary_codes.parquet" \
  --codes-output-csv "$IPEDS_ROOT/Dictionary/dictionary_codes.csv"
```

## One-line Run (Recommended)

```bash
bash manual_commands.sh
```

This runs the full pipeline with current script paths and writes outputs into `Panels/` and QC into `Checks/`.

## Direct Full Run

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

```bash
python3 Scripts/06_build_custom_panel.py \
  --input "$IPEDS_ROOT/Panels/2004_2024_IPEDS_clean_Panel_DS.parquet" \
  --vars "INSTNM,SECTOR,TUITION1,PELL_RECP" \
  --output "$IPEDS_ROOT/Panels/custom_panel.parquet"
```

Or use variable list file:

```bash
python3 Scripts/06_build_custom_panel.py \
  --input "$IPEDS_ROOT/Panels/2004_2024_IPEDS_clean_Panel_DS.parquet" \
  --vars-file "Customize_Panel/selectedvars.txt" \
  --output "$IPEDS_ROOT/Panels/custom_panel.parquet"
```

## QC Outputs

- `Checks/release_qc/` release manifest checks
- `Checks/harmonize_qc/` harmonize anomalies (including dropped missing UNITID rows)
- `Checks/disc_qc/` discrete collapse conflicts
- `Checks/wide_qc/` wide panel summaries
- `Checks/prch_qc/` parent-child cleaning summaries
- `Checks/panel_qc/` raw vs PRCH-clean QA

## Repository Layout

- `Scripts/` pipeline scripts (`00` to `06`)
- `Scripts/QA_QC/` QA/QC scripts
- `Customize_Panel/` custom panel variable lists
- `Artifacts/` small tracked artifacts/docs only
- `manual_commands.sh` public one-line runner

## Notes

- `Scripts/03_harmonize.py` excludes mission folders from ingestion.
- Keep large generated outputs out of git (`Checks/`, `Panels/`, `Cross_sections/`, raw inputs).
