# IPEDS Paneling

The IPEDS paneling workflow harmonizes dictionary metadata with survey data
files using a label-driven concept catalog. Use the following terminal commands
to rebuild the dictionary lake and run key smoke tests once your Python
environment is ready.

## 1. Bootstrap the virtual environment

```bash
python3 -m venv .venv
source .venv/bin/activate
python -m pip install -U pip
python -m pip install -r requirements.txt
```

## 2. Refresh the dictionary lake

```bash
python 01_ingest_dictionaries.py \
  --root "/Users/markjaysonfarol13/Higher Ed research/IPEDS/Cross sectional Datas" \
  --output dictionary_lake.parquet
```

## 3. Run harmonizer gates

Legacy Finance labels (2004):

```bash
python harmonize_new.py \
  --root "/Users/markjaysonfarol13/Higher Ed research/IPEDS/Cross sectional Datas" \
  --lake dictionary_lake.parquet \
  --years 2004 \
  --output panel_2004.parquet \
  --strict-release
```

Modern labels (2017–2018):

```bash
python harmonize_new.py \
  --root "/Users/markjaysonfarol13/Higher Ed research/IPEDS/Cross sectional Datas" \
  --lake dictionary_lake.parquet \
  --years 2017:2018 \
  --output panel_2017_2018.parquet \
  --strict-release
```

## 4. Spot-check label matches

```bash
python - <<'PY'
import pandas as pd
df = pd.read_csv("label_matches.csv")
print(df.sort_values(["score", "year"]).head(10))
PY
```

Run each block from the repository root. The environment bootstrap only needs
to be performed once per machine (repeat the `source .venv/bin/activate` step
whenever you open a new shell).
