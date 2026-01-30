IPEDS Panel Builder (2004–2024)
================================

This repo builds a **longitudinal IPEDS panel** from raw cross‑section files (2004–2024).  
It is designed for **data science users** who want a reproducible, transparent pipeline and for **IPEDS users** who want consistent metadata across years.

Highlights
----------
- **Dictionary lake** with core metadata (year, varnumber, varname, varTitle, longDescription, DataType, format, Fieldwidth, imputationvar)
- **Long panel** output keyed by `(UNITID, year, varname)` with `varnumber` for cross‑year robustness
- **Wide panel** builder with optional **discrete-category collapse** (e.g., LEVEL1‑LEVEL19 → LEVEL_CAT)
- **QC outputs** (duplicate samples, discrete conflicts, wide summary stats)

Pipeline Overview
-----------------
```
Raw IPEDS files (Raw_Cross_Section_Data/)
        │
        ▼
Dictionary ingest (Dictionary/01_ingest_dictionaries.py)
  ├─ dictionary_lake.parquet  (core metadata)
  └─ dictionary_codes.parquet (value labels)
        │
        ▼
Harmonize (harmonize.py)  →  Cross_sections/panel_long_varnum_<year>.parquet
        │
        ▼
Stitch per-year longs  →  Panels/2004-2024/panel_long_varnum_2004_2024.parquet
        │
        ▼
Wide panel build (Panels/03_build_wide_panel.py)
  ├─ Panels/wide_2004_2024/year=YYYY/part.parquet
  └─ QC: Checks/disc_qc + Checks/wide_qc
```

Data Shape (Long vs Wide)
-------------------------
```
LONG (authoritative)
┌────────┬────────┬────────┬──────────┬────────┬───────────┬──────────────┐
│ year   │ UNITID │ varname│ varnumber│ value  │ varTitle  │ DataType      │
├────────┼────────┼────────┼──────────┼────────┼───────────┼──────────────┤
│ 2018   │ 100654 │ LEVEL1 │ 00000123 │ 1      │ Award...  │ disc          │
│ 2018   │ 100654 │ LEVEL2 │ 00000124 │        │ Award...  │ disc          │
└────────┴────────┴────────┴──────────┴────────┴───────────┴──────────────┘

WIDE (optional)
┌────────┬────────┬──────────┬───────────┬──────────┐
│ year   │ UNITID │ LEVEL    │ LEVEL_CAT │ TUITION2 │
├────────┼────────┼──────────┼───────────┼──────────┤
│ 2018   │ 100654 │ 2        │ 1         │ 10234    │
└────────┴────────┴──────────┴───────────┴──────────┘
```

Discrete Collapse (visual)
--------------------------
```
Indicators (disc)
LEVEL1  LEVEL2  LEVEL3  ...  LEVEL19
  1       .       .          .

Collapsed category
LEVEL_CAT = 1
```

What You Get
------------
- **Long panel (authoritative)**  
  One row per `(UNITID, year, varname)`:
  ```
  year, UNITID, varname, varnumber, value, varTitle, longDescription, DataType, format, Fieldwidth, imputationvar, source_file
  ```

- **Dictionary lake**  
  `/Dictionary/dictionary_lake.parquet` with consistent metadata across years.

- **Value labels**  
  `/Dictionary/dictionary_codes.parquet` from Frequencies/FrequenciesRV/Imputation sheets.

- **Wide panel (optional)**  
  One row per `(UNITID, year)` and one column per `varname`, with optional disc collapse.

Quick Start (Recommended)
-------------------------
Use the wrapper script with defaults baked in:

```bash
python3 run_pipeline.py
```

This will:
1) Harmonize 2004–2024 into per‑year long files  
2) Stitch them into one master long panel  
3) Build the wide panel with discrete collapse + QC outputs

Manual Commands (Advanced)
--------------------------

1) Build dictionary lake:
```bash
python3 Dictionary/01_ingest_dictionaries.py \
  --root "/Users/markjaysonfarol13/IPEDS_Paneling/Raw_Cross_Section_Data" \
  --min-year 2004 \
  --output "/Users/markjaysonfarol13/IPEDS_Paneling/Dictionary/dictionary_lake.parquet" \
  --codes-output "/Users/markjaysonfarol13/IPEDS_Paneling/Dictionary/dictionary_codes.parquet" \
  --codes-output-csv "/Users/markjaysonfarol13/IPEDS_Paneling/Dictionary/dictionary_codes.csv"
```

2) Harmonize per‑year (streaming, low‑RAM):
```bash
python3 harmonize.py \
  --root "/Users/markjaysonfarol13/IPEDS_Paneling/Raw_Cross_Section_Data" \
  --lake "/Users/markjaysonfarol13/IPEDS_Paneling/Dictionary/dictionary_lake.parquet" \
  --years 2018:2018 \
  --output "/Users/markjaysonfarol13/IPEDS_Paneling/Cross_sections/panel_long_varnum_2018.parquet"
```

3) Stitch per‑year long files:
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

4) Build wide panel with discrete collapse:
```bash
python3 Panels/03_build_wide_panel.py \
  --input "/Users/markjaysonfarol13/IPEDS_Paneling/Panels/2004-2024/panel_long_varnum_2004_2024.parquet" \
  --out_dir "/Users/markjaysonfarol13/IPEDS_Paneling/Panels/wide_2004_2024" \
  --years "2004:2024" \
  --dictionary "/Users/markjaysonfarol13/IPEDS_Paneling/Dictionary/dictionary_lake.parquet" \
  --collapse-disc \
  --drop-disc-components \
  --disc-qc-dir "/Users/markjaysonfarol13/IPEDS_Paneling/Checks/disc_qc" \
  --qc-dir "/Users/markjaysonfarol13/IPEDS_Paneling/Checks/wide_qc"
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

Repo Layout
-----------
- `Raw_Cross_Section_Data/` raw IPEDS downloads  
- `Dictionary/` dictionary lake + value labels  
- `Cross_sections/` per‑year long panels  
- `Panels/` stitched long + wide outputs  
- `Checks/` QC output (disc conflicts, wide summary)

Attribution
-----------
IPEDS data © NCES. Please cite IPEDS per NCES guidance.
