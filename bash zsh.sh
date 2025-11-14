# Code Runs
# 01_ingest_dictionaries.py 
# build_raw_panel.py panelize_raw.py 
# merge_raw_panels.py 
# unify_finance.py
# finance_build_crosswalk_template.py 
# harmonize_finance_concepts.py
# finance_validate_panel.py

# 1. Build dictionary lake (run once, or when dictionaries change)
python3 01_ingest_dictionaries.py \
  --root "/Users/markjaysonfarol13/Higher Ed research/IPEDS/Cross sectional Datas" \
  --output "/Users/markjaysonfarol13/Higher Ed research/IPEDS/Parquets/dictionary_lake.parquet"

# 2. Build per-year long + wide panels (2004–2024)
for YEAR in {2004..2024}; do
  python3 build_raw_panel.py \
    --root "/Users/markjaysonfarol13/Higher Ed research/IPEDS/Cross sectional Datas" \
    --years "$YEAR" \
    --surveys HD,IC,IC_AY,EF,E12,EFIA,E1D,EFFY,SFA,FIN,F1A,F2A,F3A,GR,GR200,ADM,OM,CST \
    --output "/Users/markjaysonfarol13/Higher Ed research/IPEDS/Parquets/panel_long_raw_${YEAR}.parquet"

  python3 panelize_raw.py \
    --input "/Users/markjaysonfarol13/Higher Ed research/IPEDS/Parquets/panel_long_raw_${YEAR}.parquet" \
    --output "/Users/markjaysonfarol13/Higher Ed research/IPEDS/Paneled Datasets/Crosssections/panel_wide_raw_${YEAR}.csv" \
    --column-field source_var \
    --survey-order "HD,IC,IC_AY,EF,E12,EFIA,E1D,EFFY,SFA,FIN,F1A,F2A,F3A,ADM,GR,GR200,OM,CST"
done

# 3. Merge all yearly wides into one big wide panel
python3 merge_raw_panels.py \
  --input-dir "/Users/markjaysonfarol13/Higher Ed research/IPEDS/Paneled Datasets/Crosssections" \
  --pattern "panel_wide_raw_*.csv" \
  --output-wide "/Users/markjaysonfarol13/Higher Ed research/IPEDS/Paneled Datasets/Raw panel/panel_wide_raw_2004_2024_merged.csv" \
  --component-order "HD,IC,IC_AY,EF,E12,EFIA,E1D,EFFY,SFA,FIN,F1A,F2A,F3A,ADM,GR,GR200,OM,CST"

# 4. Finance Step 0: form-level extraction (F1/F2/F3 + components)
python3 unify_finance.py \
  --input "/Users/markjaysonfarol13/Higher Ed research/IPEDS/Paneled Datasets/Crosssections/panel_wide_raw_2004_2024_merged.csv" \
  --output-long "/Users/markjaysonfarol13/Higher Ed research/IPEDS/Parquets/Long/finance_step0_long.parquet" \
  --output-wide "/Users/markjaysonfarol13/Higher Ed research/IPEDS/Paneled Datasets/Raw panel/Final/finance_step0_wide.csv"

# quick coverage check
python3 - <<'PY'
import pandas as pd
from pathlib import Path
step0 = Path("/Users/markjaysonfarol13/Higher Ed research/IPEDS/Parquets/Long/finance_step0_long.parquet")
df = pd.read_parquet(step0)
summary = df.groupby(["YEAR", "form_family"]).size().reset_index(name="n_rows")
print(summary.head())
PY

# 5. Build crosswalk template (then edit manually)
python3 finance_build_crosswalk_template.py \
  --dict-lake "/Users/markjaysonfarol13/Higher Ed research/IPEDS/Parquets/dictionary_lake.parquet" \
  --output "/Users/markjaysonfarol13/Higher Ed research/IPEDS/Paneled Datasets/Crosswalks/finance_crosswalk_template.csv" \
  --year-min 2004 \
  --year-max 2024

# [MANUAL STEP] Edit finance_crosswalk_template.csv to fill concept_key, year ranges, weights.

# 6. Apply crosswalk to create concept-level finance panel
python3 harmonize_finance_concepts.py \
  --step0 "/Users/markjaysonfarol13/Higher Ed research/IPEDS/Parquets/Long/finance_step0_long.parquet" \
  --crosswalk "/Users/markjaysonfarol13/Higher Ed research/IPEDS/Paneled Datasets/Crosswalks/finance_crosswalk_template.csv" \
  --output-long "/Users/markjaysonfarol13/Higher Ed research/IPEDS/Parquets/Long/finance_concepts_long.parquet" \
  --output-wide "/Users/markjaysonfarol13/Higher Ed research/IPEDS/Parquets/Long/finance_concepts_wide.parquet" \
  --coverage "/Users/markjaysonfarol13/Higher Ed research/IPEDS/Paneled Datasets/Crosswalks/finance_concepts_coverage.csv"

# 7. Validate the concept-wide panel
python3 finance_validate_panel.py \
  --panel "/Users/markjaysonfarol13/Higher Ed research/IPEDS/Parquets/Long/finance_concepts_wide.parquet" \
  --tolerance 100000 \
  --tol-rel 0.05
