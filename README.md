IPEDS Harmonized Panel Builder (1987–2024)
===========================================

This repo builds a harmonized, longitudinal IPEDS panel with short varnames, scored matching, and per‑chunk QC. It supports low‑RAM chunked runs and optional wide exports for selected variables.

Why this exists (problem → solution)
------------------------------------
IPEDS is rich but messy for longitudinal work: survey forms change, short varnames repeat across components, and dictionaries are inconsistent by year. Researchers often spend days just downloading, unzipping, and guessing which column maps to which concept. This pipeline solves that by:
- Automating downloads back to 1987 (with early coverage via DCP for pre‑2002).
- Building a normalized “dictionary lake” keyed by (year, form, varname).
- Scored matching from a concept catalog to the correct short varnames, with per‑chunk QC so you see match rates and conflicts.
- Chunked, low‑RAM harmonization to produce a ready‑to‑analyze long panel Parquet, plus an optional wide subset for selected variables.
The result: a transparent, reproducible IPEDS panel that makes schema drift explicit and measurable, not a hidden source of error.

Pipeline at a glance
--------------------
```
Raw downloads (download_ipeds.py)
      │
      ▼
Dictionary ingest (01_ingest_dictionaries.py)
  - builds dictionary_lake.parquet keyed by (year, form, varname)
      │
      ▼
Harmonize (harmonize_new.py, chunked)
  - matches concepts → short varnames
  - outputs panel_long_*.parquet + Checks/*
  - optional wide subset (selected vars pivoted by year)
      │
      ▼
Stitch chunks (optional)
  - panel_long_1987_2024.parquet (master long panel)
```

What you get
- Long panel Parquet: one row per UNITID × year, all survey items (`Panels/panel_long_….parquet`).
- Chunked diagnostics: `Checks/<chunk>/match_stats.csv`, `unmatched_details.csv`, etc.
- Optional wide subset Parquet (selected vars pivoted by year).
- Dictionary lake keyed by (year, form, varname_short): `Dictionary/dictionary_lake.parquet`.

Prereqs
- Python 3.10+ with `pip install -r requirements.txt`
- Disk space: raw IPEDS downloads are large; keep Parquet, avoid full CSV/DTA unless necessary.

Quick start (full span, RAM-friendly)
1) Download raw IPEDS (1987–2024):
```bash
python3 "Download Scripts/download_ipeds.py" \
  --out-root "/Users/markjaysonfarol13/IPEDS_Paneling/Raw_Cross_Section_Data" \
  --years 1987:2024 \
  --extract-varnames
```
2) Build dictionary lake:
```bash
python3 Dictionary/01_ingest_dictionaries.py \
  --root "/Users/markjaysonfarol13/IPEDS_Paneling/Raw_Cross_Section_Data" \
  --output "/Users/markjaysonfarol13/IPEDS_Paneling/Dictionary/dictionary_lake.parquet"
```
3) Harmonize in chunks (low RAM):
```bash
# 1987–2001
python3 harmonize_new.py --root "/Users/markjaysonfarol13/IPEDS_Paneling/Raw_Cross_Section_Data" \
  --lake "/Users/markjaysonfarol13/IPEDS_Paneling/Dictionary/dictionary_lake.parquet" \
  --years 1987:2001 \
  --output "/Users/markjaysonfarol13/IPEDS_Paneling/Panels/panel_long_1987_2001.parquet" \
  --checks-dir "/Users/markjaysonfarol13/IPEDS_Paneling/Checks/1987_2001" \
  --strict-release --strict-coverage

# 2002–2010
python3 harmonize_new.py --root "/Users/markjaysonfarol13/IPEDS_Paneling/Raw_Cross_Section_Data" \
  --lake "/Users/markjaysonfarol13/IPEDS_Paneling/Dictionary/dictionary_lake.parquet" \
  --years 2002:2010 \
  --output "/Users/markjaysonfarol13/IPEDS_Paneling/Panels/panel_long_2002_2010.parquet" \
  --checks-dir "/Users/markjaysonfarol13/IPEDS_Paneling/Checks/2002_2010" \
  --strict-release --strict-coverage

# 2011–2017
python3 harmonize_new.py --root "/Users/markjaysonfarol13/IPEDS_Paneling/Raw_Cross_Section_Data" \
  --lake "/Users/markjaysonfarol13/IPEDS_Paneling/Dictionary/dictionary_lake.parquet" \
  --years 2011:2017 \
  --output "/Users/markjaysonfarol13/IPEDS_Paneling/Panels/panel_long_2011_2017.parquet" \
  --checks-dir "/Users/markjaysonfarol13/IPEDS_Paneling/Checks/2011_2017" \
  --strict-release --strict-coverage

# 2018–2024 (with optional wide subset)
python3 harmonize_new.py --root "/Users/markjaysonfarol13/IPEDS_Paneling/Raw_Cross_Section_Data" \
  --lake "/Users/markjaysonfarol13/IPEDS_Paneling/Dictionary/dictionary_lake.parquet" \
  --years 2018:2024 \
  --output "/Users/markjaysonfarol13/IPEDS_Paneling/Panels/panel_long_2018_2024.parquet" \
  --checks-dir "/Users/markjaysonfarol13/IPEDS_Paneling/Checks/2018_2024" \
  --strict-release --strict-coverage \
  --wide-output "/Users/markjaysonfarol13/IPEDS_Paneling/Panels/panel_wide_subset.parquet" \
  --wide-vars "tuition01,net_student_tuition,applcn,admssn,enrlt"
```
4) Stitch chunks to one Parquet:
```bash
python3 - <<'PY'
import pyarrow.parquet as pq
from pathlib import Path
parts=[
    Path("/Users/markjaysonfarol13/IPEDS_Paneling/Panels/panel_long_1987_2001.parquet"),
    Path("/Users/markjaysonfarol13/IPEDS_Paneling/Panels/panel_long_2002_2010.parquet"),
    Path("/Users/markjaysonfarol13/IPEDS_Paneling/Panels/panel_long_2011_2017.parquet"),
    Path("/Users/markjaysonfarol13/IPEDS_Paneling/Panels/panel_long_2018_2024.parquet"),
]
out=Path("/Users/markjaysonfarol13/IPEDS_Paneling/Panels/panel_long_1987_2024.parquet")
w=None
for p in parts:
    pf=pq.ParquetFile(p)
    for b in pf.iter_batches():
        if w is None:
            w=pq.ParquetWriter(out,b.schema)
        w.write_batch(b)
if w: w.close()
print("Wrote", out)
PY
```

Using the data (examples)
- DuckDB (recommended for large/filter):
```python
import duckdb
duckdb.sql(\"\"\"\nSELECT UNITID, year, tuition01\nFROM parquet_scan('Panels/panel_long_1987_2024.parquet')\nWHERE year BETWEEN 2010 AND 2020\n\"\"\").df()
```
- Pandas (small slices):
```python
import pandas as pd
df = pd.read_parquet("Panels/panel_long_1987_2024.parquet", columns=["UNITID","year","tuition01","applcn"])
```

Wide output (optional)
- Use `--wide-output` and `--wide-vars` on the harmonize command to pivot only selected columns (keeps memory small). The long Parquet remains the authoritative format.

Storage tips
- Keep Parquet as the master. Full CSV/DTA exports get very large; if needed, export subsets only.
- Regenerable outputs you can delete safely: `Panels/csv/`, `Panels/dta/`, ad-hoc wide subset files.

Repo structure (recommended to keep)
- Raw_Cross_Section_Data/ (downloads)
- Dictionary/ (dictionary lake)
- Panels/ (Parquet panels)
- Checks/ (per-chunk diagnostics)
- Scripts: Download Scripts/, Dictionary/, harmonize_new.py, concept_catalog.py, dcp_ingest.py

Notes
- Download script default years now cover 1987–2024; adjust `--years` as desired.
- Chunked harmonize commands are low-RAM; use them for reliability.

License/Attribution
- IPEDS data © NCES; cite IPEDS. Delta Cost Project (where used) per DCP terms.
