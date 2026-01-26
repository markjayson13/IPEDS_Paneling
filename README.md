# IPEDS Paneling (varName-first)

This project builds IPEDS panels using the short `varName` codes as the authoritative key. Dictionaries are ingested with a varName-first mindset; when dictionaries are missing or HTML-only, the ingest falls back to raw data headers so varNames are always present.

## Install
```bash
python3 -m venv .venv
source .venv/bin/activate
python -m pip install -U pip
python -m pip install -r requirements.txt
```

## Workflow (data live outside the repo)
Outputs stay in `/Users/markjaysonfarol13/IPEDS_Paneling/...` to keep the repo code-only.

1) Download raw files (with manifests and varName extraction):
```bash
python "Download Scripts/download_ipeds.py" \
  --out-root "/Users/markjaysonfarol13/IPEDS_Paneling/Raw_Cross_Section_Data" \
  --years 2002:2024 \
  --extract-varnames
```

2) Build the dictionary lake (varNames prioritized; headers used if dictionaries fail):
```bash
python Dictionary/01_ingest_dictionaries.py
```
Outputs: `/Users/markjaysonfarol13/IPEDS_Paneling/Dictionary/dictionary_lake.parquet` (+ profiles).

3) Harmonize to long panel (strict by default; varName-first scoring):
```bash
python harmonize_new.py --years 2002:2024
```
Outputs: `/Users/markjaysonfarol13/IPEDS_Paneling/Panels/panel_long.parquet`  
QC: `/Users/markjaysonfarol13/IPEDS_Paneling/Checks/label_matches.csv`, `coverage_summary.csv`, `form_conflicts.csv`, `validation_report.csv`.

RAM-friendly chunked harmonize:
```bash
python harmonize_new.py --years 2002:2010 --output /Users/markjaysonfarol13/IPEDS_Paneling/Panels/panel_long_2002_2010.parquet --log-level error
python harmonize_new.py --years 2011:2017 --output /Users/markjaysonfarol13/IPEDS_Paneling/Panels/panel_long_2011_2017.parquet --log-level error
python harmonize_new.py --years 2018:2024 --output /Users/markjaysonfarol13/IPEDS_Paneling/Panels/panel_long_2018_2024.parquet --log-level error
# stitch (streaming)
python - <<'PY'
import pyarrow.parquet as pq
from pathlib import Path
parts = [
    Path("/Users/markjaysonfarol13/IPEDS_Paneling/Panels/panel_long_2002_2010.parquet"),
    Path("/Users/markjaysonfarol13/IPEDS_Paneling/Panels/panel_long_2011_2017.parquet"),
    Path("/Users/markjaysonfarol13/IPEDS_Paneling/Panels/panel_long_2018_2024.parquet"),
]
out = Path("/Users/markjaysonfarol13/IPEDS_Paneling/Panels/panel_long_2002_2024.parquet")
writer=None
for p in parts:
    pf=pq.ParquetFile(p)
    for batch in pf.iter_batches():
        if writer is None:
            writer=pq.ParquetWriter(out, batch.schema)
        writer.write_batch(batch)
if writer: writer.close()
print("Wrote", out)
PY
```

4) (Optional) Pivot to wide:
```bash
python panelize_panel.py \
  --source "/Users/markjaysonfarol13/IPEDS_Paneling/Panels/panel_long.parquet" \
  --output "/Users/markjaysonfarol13/IPEDS_Paneling/Panels/panel_wide.csv"
```

## Key design points
- Short `varName` is the unifier (`varname_exact/regex` wins even with weak labels).
- Dictionary ingest backstops HTML/missing codebooks by seeding varNames from raw data headers.
- Generated artifacts are kept out of Git; use the `/Users/markjaysonfarol13/IPEDS_Paneling/...` folders for outputs.
- Checks now include `match_rate_expected` (excludes concepts flagged `expected_available=False`) and `expected_available` in `unmatched_details.csv` so “not in public columns” stops dragging headline metrics.

## Repo layout
- `Download Scripts/` – downloader (BGP endpoints) + varName extraction.
- `Dictionary/` – builds `dictionary_lake.parquet` with header fallback.
- `harmonize_new.py`, `concept_catalog.py` – varName-first harmonizer and catalog.
- `Artifacts/Legacy/` – archived legacy outputs/scripts moved out of the main tree.
- Legacy script folders (`Harmonize Scripts/`, `Panelize Scripts/`, `CrossWalk Scripts/`, etc.) remain for reference; prefer the pipeline above.
