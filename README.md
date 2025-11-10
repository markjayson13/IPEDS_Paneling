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

## 3. Build the full 2004–2024 panel (strict release + coverage)

```bash
python harmonize_new.py \
  --root "/Users/markjaysonfarol13/Higher Ed research/IPEDS/Cross sectional Datas" \
  --lake dictionary_lake.parquet \
  --years 2004:2024 \
  --output "/Users/markjaysonfarol13/Higher Ed research/IPEDS/Paneled Datasets/panel_long.parquet" \
  --rules validation_rules.yaml \
  --strict-release \
  --strict-coverage
```

*Optional:* include `--reporting-map reporting_map.csv` if you maintain a UNITID→reporting-unit crosswalk.

## 4. Produce the classic wide CSV (one row per UNITID-year)

```bash
python - <<'PY'
import pandas as pd
from pathlib import Path

base_dir = Path("/Users/markjaysonfarol13/Higher Ed research/IPEDS/Paneled Datasets")
long_path = base_dir / "panel_long.parquet"
df = pd.read_parquet(long_path)
df["reporting_unitid"] = df.get("reporting_unitid", df["UNITID"])

wide = (
    df.pivot_table(index=["UNITID","year"], columns="target_var", values="value", aggfunc="first")
      .reset_index()
)
ru = df[["UNITID","year","reporting_unitid"]].drop_duplicates()
wide = wide.merge(ru, on=["UNITID","year"], how="left")
cols = ["UNITID","reporting_unitid","year"] + [
    c for c in wide.columns if c not in {"UNITID","reporting_unitid","year"}
]
wide = wide[cols]

wide.to_csv(base_dir / "panel_long_wide.csv", index=False)
PY
```

## 5. Spot-check label matches

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
