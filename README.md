IPEDS Panel Builder (2004–2024)
================================

This repo builds a **longitudinal IPEDS panel** from raw cross‑section files (2004–2024).  
It is designed for **data science users** who want a reproducible, transparent pipeline and for **IPEDS users** who want consistent metadata across years.

Highlights
----------
- **Dictionary lake** with core metadata (year, varnumber, varname, varTitle, longDescription, DataType, format, Fieldwidth, imputationvar)
- **Long panel** output keyed by `(UNITID, year, varname)` with `varnumber` for cross‑year robustness
- **Wide panel** builder with optional **discrete-category collapse** (e.g., program-level indicator groups → *_CAT)
- **Parent/child (PRCH) cleaning** to null out component data for child records while keeping all rows
- **Release validation** (Revised/Final only) via yearly manifests
- **_rv preference** when revised files exist (non‑_rv are skipped in that folder)
- **QC outputs** (duplicate samples, discrete conflicts, wide summary stats)

Pipeline Overview
-----------------
```
Download (optional): Download Scripts/00_download_ipeds.py
        │
Raw IPEDS files (Raw_Cross_Section_Data/)
        │
        ▼
Dictionary ingest (Dictionary/01_ingest_dictionaries.py)
  ├─ dictionary_lake.parquet  (core metadata)
  ├─ dictionary_lake.csv      (auto‑generated for inspection)
  └─ dictionary_codes.parquet (value labels)
        │
        ▼
Harmonize (03_harmonize.py)  →  Cross_sections/panel_long_varnum_<year>.parquet
  └─ Prefers *_rv files when present; logs how many non‑_rv were skipped per year
        │
        ▼
Stitch per-year longs  →  Panels/2004-2024/panel_long_varnum_2004_2024.parquet
        │
        ▼
Wide panel build (Panels/04_build_wide_panel.py)
  ├─ Panels/wide_2004_2024/year=YYYY/part.parquet
  └─ Stitched wide: Panels/2004_2024_IPEDS_Raw_Panel_DS.parquet
  └─ QC: Checks/disc_qc + Checks/wide_qc
        │
        ▼
PRCH clean (Cleaning/05_cleaning_panel.py)
  └─ Panels/2004_2024_IPEDS_PRCHclean_Panel_DS.parquet
        │
        ▼
Research‑ready clean (Cleaning/05_cleaning_panel.py --drop-imputation-flags)
  └─ Panels/2004_2024_IPEDS_clean_Panel_DS.parquet
        │
        ▼
Custom panel builder (Panels/06_build_custom_panel.py)
  └─ User‑selected subset (always keeps UNITID + year)
```

Data Shape (Long vs Wide)
-------------------------
```
LONG (authoritative)
┌────────┬────────┬────────┬──────────┬────────┬───────────┬──────────────┐
│ year   │ UNITID │ varname│ varnumber│ value  │ varTitle  │ DataType      │
├────────┼────────┼────────┼──────────┼────────┼───────────┼──────────────┤
│ 2018   │ 100654 │ INSTNM │ 00000002 │ Univ…  │ Institution name │ char     │
│ 2018   │ 100654 │ SECTOR │ 00000010 │ 4      │ Sector of institution │ disc │
└────────┴────────┴────────┴──────────┴────────┴───────────┴──────────────┘

WIDE (optional)
┌────────┬────────┬──────────┬───────────┬──────────┐
│ year   │ UNITID │ INSTNM   │ SECTOR    │ CONTROL  │
├────────┼────────┼──────────┼───────────┼──────────┤
│ 2018   │ 100654 │ Univ…    │ 4         │ 1        │
└────────┴────────┴──────────┴───────────┴──────────┘
```

Discrete Collapse (visual)
--------------------------
```
Indicators (disc)
NONCRDT1  NONCRDT2  NONCRDT3  ...  NONCRDT9
  1       .       .          .

Collapsed category
NONCRDT_CAT = 1
```

What You Get
------------
- **Long panel (authoritative)**  
  One row per `(UNITID, year, varname)`:
  ```
  year, UNITID, varname, varnumber, value, varTitle, longDescription, DataType, format, Fieldwidth, imputationvar, source_file
  ```

- **Dictionary lake**  
  `/Dictionary/dictionary_lake.parquet` with consistent metadata across years  
  `/Dictionary/dictionary_lake.csv` auto‑generated for inspection.

- **Value labels**  
  `/Dictionary/dictionary_codes.parquet` from Frequencies/FrequenciesRV/Imputation sheets.

- **Wide panel**  
  One row per `(UNITID, year)` and one column per `varname`, with optional disc collapse.  
  Official stitched output: `Panels/2004_2024_IPEDS_Raw_Panel_DS.parquet`

- **PRCH cleaned wide panel**  
  Safe Option A: keep all rows; null out component data for child records.  
  Output: `Panels/2004_2024_IPEDS_PRCHclean_Panel_DS.parquet`

- **Research‑ready wide panel**  
  PRCH‑cleaned + imputation flags removed (X* columns dropped).  
  Output: `Panels/2004_2024_IPEDS_clean_Panel_DS.parquet`

Quick Start (Recommended)
-------------------------
Use the wrapper script with defaults baked in:

```bash
python3 08_run_pipeline.py
```

This will:
1) Harmonize 2004–2024 into per‑year long files  
2) Stitch them into one master long panel  
3) Build the wide panel with discrete collapse + QC outputs

Optional PRCH cleaning (safe Option A):
```bash
python3 Cleaning/05_cleaning_panel.py \
  --input "/Users/markjaysonfarol13/IPEDS_Paneling/Panels/2004_2024_IPEDS_Raw_Panel_DS.parquet" \
  --output "/Users/markjaysonfarol13/IPEDS_Paneling/Panels/2004_2024_IPEDS_PRCHclean_Panel_DS.parquet" \
  --dictionary "/Users/markjaysonfarol13/IPEDS_Paneling/Dictionary/dictionary_lake.parquet" \
  --qc-dir "/Users/markjaysonfarol13/IPEDS_Paneling/Checks/prch_qc"
```

Research‑ready clean (drop imputation flags):
```bash
python3 Cleaning/05_cleaning_panel.py \
  --input "/Users/markjaysonfarol13/IPEDS_Paneling/Panels/2004_2024_IPEDS_Raw_Panel_DS.parquet" \
  --output "/Users/markjaysonfarol13/IPEDS_Paneling/Panels/2004_2024_IPEDS_clean_Panel_DS.parquet" \
  --dictionary "/Users/markjaysonfarol13/IPEDS_Paneling/Dictionary/dictionary_lake.parquet" \
  --qc-dir "/Users/markjaysonfarol13/IPEDS_Paneling/Checks/prch_qc" \
  --drop-imputation-flags
```

Custom panel (user‑selected variables; always keeps UNITID + year):
```bash
python3 Panels/06_build_custom_panel.py \
  --input "/Users/markjaysonfarol13/IPEDS_Paneling/Panels/2004_2024_IPEDS_clean_Panel_DS.parquet" \
  --vars "INSTNM,SECTOR,TUITION1,PELL_RECP" \
  --output "/Users/markjaysonfarol13/IPEDS_Paneling/Panels/custom_panel.parquet"
```

Recommended Single‑Command Run (Raw → PRCH Clean → Clean)
---------------------------------------------------------
This is the preferred end‑to‑end command. It:
1) enforces Revised/Final releases,  
2) builds the raw wide panel,  
3) PRCH‑cleans the panel, and  
4) produces the research‑ready clean panel.

```bash
python3 08_run_pipeline.py \
  --no-skip-existing \
  --release-allow "revised,final" \
  --release-qc-dir "/Users/markjaysonfarol13/IPEDS_Paneling/Checks/release_qc" \
  --stitch-wide \
  --stitch-wide-out "/Users/markjaysonfarol13/IPEDS_Paneling/Panels/2004_2024_IPEDS_Raw_Panel_DS.parquet" \
  --run-cleaning \
  --prch-clean-out "/Users/markjaysonfarol13/IPEDS_Paneling/Panels/2004_2024_IPEDS_PRCHclean_Panel_DS.parquet" \
  --clean-out "/Users/markjaysonfarol13/IPEDS_Paneling/Panels/2004_2024_IPEDS_clean_Panel_DS.parquet" \
  --prch-qc-dir "/Users/markjaysonfarol13/IPEDS_Paneling/Checks/prch_qc" \
  --drop-imputation-flags
```

Optional: clean up per‑year long files after stitching
-----------------------------------------------------
If you are disk‑constrained and do not need the per‑year long files:

```bash
python3 08_run_pipeline.py \
  --years 2004:2024 \
  --no-skip-existing \
  --stitch \
  --cleanup-year-longs \
  --build-wide \
  --stitch-wide \
  --run-cleaning \
  --release-allow "revised,final" \
  --release-qc-dir "/Users/markjaysonfarol13/IPEDS_Paneling/Checks/release_qc" \
  --no-final-dedupe
```

Audit Pack (Reviewer Bundle)
----------------------------
This builds `audit_pack/` and writes a zip to **`/Users/markjaysonfarol13/IPEDS_Paneling/Checks/audit_pack.zip`**.
If the long panel contains duplicate keys, use `--allow-duplicates` to record counts and continue.

```bash
python3 09_build_audit_pack.py \
  --out-dir audit_pack \
  --zip \
  --allow-duplicates \
  --years "2004:2024" \
  --raw-root "/Users/markjaysonfarol13/IPEDS_Paneling/Raw_Cross_Section_Data" \
  --checks-dir "/Users/markjaysonfarol13/IPEDS_Paneling/Checks" \
  --dictionary "/Users/markjaysonfarol13/IPEDS_Paneling/Dictionary/dictionary_lake.parquet" \
  --dictionary-codes "/Users/markjaysonfarol13/IPEDS_Paneling/Dictionary/dictionary_codes.parquet" \
  --long-panel "/Users/markjaysonfarol13/IPEDS_Paneling/Panels/2004-2024/panel_long_varnum_2004_2024.parquet" \
  --wide-raw "/Users/markjaysonfarol13/IPEDS_Paneling/Panels/2004_2024_IPEDS_Raw_Panel_DS.parquet" \
  --wide-prch "/Users/markjaysonfarol13/IPEDS_Paneling/Panels/2004_2024_IPEDS_PRCHclean_Panel_DS.parquet" \
  --wide-clean "/Users/markjaysonfarol13/IPEDS_Paneling/Panels/2004_2024_IPEDS_clean_Panel_DS.parquet"
```

Minimal End‑to‑End (run_pipeline + custom subset)
-------------------------------------------------
This is the shortest safe path to a research‑ready custom panel.
It includes **release QC outputs** and **RAM‑friendly stitching** of the wide panel:

```bash
# 1) Build raw wide from 2004–2024 with release QC + streaming wide stitch
python3 08_run_pipeline.py \
  --release-allow "revised,final" \
  --release-qc-dir "/Users/markjaysonfarol13/IPEDS_Paneling/Checks/release_qc" \
  --stitch-wide \
  --stitch-wide-out "/Users/markjaysonfarol13/IPEDS_Paneling/Panels/2004_2024_IPEDS_Raw_Panel_DS.parquet"

# 2) PRCH clean + drop imputation flags (research‑ready clean)
python3 Cleaning/05_cleaning_panel.py \
  --input "/Users/markjaysonfarol13/IPEDS_Paneling/Panels/2004_2024_IPEDS_Raw_Panel_DS.parquet" \
  --output "/Users/markjaysonfarol13/IPEDS_Paneling/Panels/2004_2024_IPEDS_clean_Panel_DS.parquet" \
  --dictionary "/Users/markjaysonfarol13/IPEDS_Paneling/Dictionary/dictionary_lake.parquet" \
  --qc-dir "/Users/markjaysonfarol13/IPEDS_Paneling/Checks/prch_qc" \
  --drop-imputation-flags

# 3) Build your custom research panel (keeps UNITID + year)
python3 Panels/06_build_custom_panel.py \
  --input "/Users/markjaysonfarol13/IPEDS_Paneling/Panels/2004_2024_IPEDS_clean_Panel_DS.parquet" \
  --vars "INSTNM,SECTOR,TUITION1,PELL_RECP" \
  --output "/Users/markjaysonfarol13/IPEDS_Paneling/Panels/custom_panel.parquet"
```

One‑liner: full end‑to‑end (raw → PRCH clean → clean → custom subset)
---------------------------------------------------------------------
```bash
python3 08_run_pipeline.py --release-allow "revised,final" --release-qc-dir "/Users/markjaysonfarol13/IPEDS_Paneling/Checks/release_qc" --stitch-wide --stitch-wide-out "/Users/markjaysonfarol13/IPEDS_Paneling/Panels/2004_2024_IPEDS_Raw_Panel_DS.parquet" && python3 Cleaning/05_cleaning_panel.py --input "/Users/markjaysonfarol13/IPEDS_Paneling/Panels/2004_2024_IPEDS_Raw_Panel_DS.parquet" --output "/Users/markjaysonfarol13/IPEDS_Paneling/Panels/2004_2024_IPEDS_PRCHclean_Panel_DS.parquet" --dictionary "/Users/markjaysonfarol13/IPEDS_Paneling/Dictionary/dictionary_lake.parquet" --qc-dir "/Users/markjaysonfarol13/IPEDS_Paneling/Checks/prch_qc" && python3 Cleaning/05_cleaning_panel.py --input "/Users/markjaysonfarol13/IPEDS_Paneling/Panels/2004_2024_IPEDS_Raw_Panel_DS.parquet" --output "/Users/markjaysonfarol13/IPEDS_Paneling/Panels/2004_2024_IPEDS_clean_Panel_DS.parquet" --dictionary "/Users/markjaysonfarol13/IPEDS_Paneling/Dictionary/dictionary_lake.parquet" --qc-dir "/Users/markjaysonfarol13/IPEDS_Paneling/Checks/prch_qc" --drop-imputation-flags && python3 Panels/06_build_custom_panel.py --input "/Users/markjaysonfarol13/IPEDS_Paneling/Panels/2004_2024_IPEDS_clean_Panel_DS.parquet" --vars "INSTNM,SECTOR,TUITION1,PELL_RECP" --output "/Users/markjaysonfarol13/IPEDS_Paneling/Panels/custom_panel.parquet"
```

Manual Commands (Advanced)
--------------------------

1) Build dictionary lake (2004–2024) + value labels:
```bash
python3 Dictionary/01_ingest_dictionaries.py \
  --root "/Users/markjaysonfarol13/IPEDS_Paneling/Raw_Cross_Section_Data" \
  --min-year 2004 \
  --output "/Users/markjaysonfarol13/IPEDS_Paneling/Dictionary/dictionary_lake.parquet" \
  --codes-output "/Users/markjaysonfarol13/IPEDS_Paneling/Dictionary/dictionary_codes.parquet" \
  --codes-output-csv "/Users/markjaysonfarol13/IPEDS_Paneling/Dictionary/dictionary_codes.csv"
```

2) Dictionary QA/QC + collapsed codes:
```bash
python3 Dictionary/02_dictionary_qaqc.py \
  --year-sep "|" \
  --excel-text
```

3) Harmonize per‑year (streaming, low‑RAM):
```bash
python3 03_harmonize.py \
  --root "/Users/markjaysonfarol13/IPEDS_Paneling/Raw_Cross_Section_Data" \
  --lake "/Users/markjaysonfarol13/IPEDS_Paneling/Dictionary/dictionary_lake.parquet" \
  --years 2018:2018 \
  --output "/Users/markjaysonfarol13/IPEDS_Paneling/Cross_sections/panel_long_varnum_2018.parquet" \
  --release-allow "revised,final" \
  --release-strict \
  --release-qc-dir "/Users/markjaysonfarol13/IPEDS_Paneling/Checks/release_qc"
```
Release QC outputs are written per year to `Checks/release_qc/` for proof of Revised/Final filtering.

4) Stitch per‑year long files:
```bash
python3 - <<'PY'
from pathlib import Path
import pyarrow.parquet as pq

base = Path("/Users/markjaysonfarol13/IPEDS_Paneling/Cross_sections")
out = Path("/Users/markjaysonfarol13/IPEDS_Paneling/Panels/2004-2024/panel_long_varnum_2004_2024.parquet")
out.parent.mkdir(parents=True, exist_ok=True)

writer = None
for y in range(2004, 2025):
    p = base / f"panel_long_varnum_{y}.parquet"
    if not p.exists():
        continue
    pf = pq.ParquetFile(p)
    for batch in pf.iter_batches():
        if writer is None:
            writer = pq.ParquetWriter(out, batch.schema)
        writer.write_batch(batch)
if writer:
    writer.close()
print("Wrote", out)
PY
```

5) Build wide panel with discrete collapse:
```bash
python3 Panels/04_build_wide_panel.py \
  --input "/Users/markjaysonfarol13/IPEDS_Paneling/Panels/2004-2024/panel_long_varnum_2004_2024.parquet" \
  --out_dir "/Users/markjaysonfarol13/IPEDS_Paneling/Panels/wide_2004_2024" \
  --years "2004:2024" \
  --dictionary "/Users/markjaysonfarol13/IPEDS_Paneling/Dictionary/dictionary_lake.parquet" \
  --collapse-disc \
  --drop-disc-components \
  --disc-qc-dir "/Users/markjaysonfarol13/IPEDS_Paneling/Checks/disc_qc" \
  --qc-dir "/Users/markjaysonfarol13/IPEDS_Paneling/Checks/wide_qc"
```

6) Build a custom panel subset:
```bash
python3 Panels/06_build_custom_panel.py \
  --input "/Users/markjaysonfarol13/IPEDS_Paneling/Panels/2004_2024_IPEDS_clean_Panel_DS.parquet" \
  --vars-file "/Users/markjaysonfarol13/IPEDS_Paneling/Mapping/vars_for_my_study.txt" \
  --output "/Users/markjaysonfarol13/IPEDS_Paneling/Panels/custom_panel.parquet"
```

Discrete Category Collapse (Disc → *_CAT)
-----------------------------------------
Some IPEDS variables are stored as **mutually exclusive indicators** (e.g., LEVEL1…LEVEL19).  
The wide builder can collapse them into a single categorical variable:
- `LEVEL1…LEVEL19` → `LEVEL_CAT`
- Conflicts (more than one active category) are logged to `Checks/disc_qc/`.
- If the base name already exists independently (e.g., `LEVEL`), the collapsed variable uses `_CAT` to avoid overwriting.

Storage Notes
-------------
- Long panels are very large (billions of rows).  
- Keep Parquet as the canonical format. Export CSV only for small subsets.

FAQ
---
**Q: Why is the wide panel so large?**  
Because it contains one row per `(UNITID, year)` and **thousands of columns** (every varname).  
Wide is intended for **analysis**, not for manual inspection. Use the custom builder to slice.

**Q: Why are long panels so huge (billions of rows)?**  
Long panels are fully normalized: one row per `(UNITID, year, varname)`.  
This is the most robust shape for merges, QC, and cross‑year consistency, but it is massive.

**Q: Why do we run QC at every stage?**  
IPEDS releases change by year and by survey. QC ensures:  
1) releases are Revised/Final,  
2) discrete categories are properly collapsed,  
3) parent/child records are handled consistently.

**Q: Why do some variables appear all‑null in a given year?**  
Some components are not collected every year, or the variable is new/retired.  
This is expected in a 2004–2024 panel. Use `dictionary_lake` for availability context.

How the pipeline mitigates common IPEDS pain points
---------------------------------------------------
Below is how the current workflow explicitly handles the most common IPEDS integration problems.

1) Inconsistent Variable Naming (Schema Drift)
   - **Dictionary‑first normalization**: raw column names are mapped to a stable `varnumber + varname`.  
   - **Standardized casing**: all varnames are upper‑cased to prevent drift from case changes.  
   - **`source_file` + `source_file_label`**: each variable is tagged with its survey source for traceability.

2) Changing Survey Universes and Definitions
   - The pipeline does **not** impute missing values across years.  
   - Gaps are visible in the wide panel and can be interpreted as “not asked” vs “missing.”  
   - Optional **disc collapse** (`*_CAT`) lets you aggregate split categories while logging conflicts.

3) Institution Entity Changes (UNITID and Parent/Child)
   - **PRCH cleaning (Option A)** keeps all rows but nulls child‑reported components.  
   - This prevents artificial spikes/drops while preserving the full UNITID history.

4) File Structure Variations (Wide vs Long, headers, legacy quirks)
   - All raw files are normalized into a **long canonical** format first.  
   - A single, consistent wide build step avoids mixing heterogeneous layouts.

5) Imputation and Response Flags
   - Imputation variables (X*) are preserved in **raw** output.  
   - **Research‑ready clean** drops X* flags for analysis use.  
   - You can always revert to raw for imputation diagnostics.

Repo Layout
-----------
- `Raw_Cross_Section_Data/` raw IPEDS downloads  
- `Download Scripts/` IPEDS downloader (`00_download_ipeds.py`)  
- `Dictionary/` dictionary lake + value labels  
- `Cross_sections/` per‑year long panels  
- `Panels/` stitched long + wide outputs  
- `Cleaning/` parent/child cleaner  
- `Checks/` QC output (disc conflicts, wide summary)

Attribution
-----------
IPEDS data © NCES. Please cite IPEDS per NCES guidance.
