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
for YEAR in {2004..2024}; do
  echo "Running unify_finance for YEAR=${YEAR}..."

  python3 unify_finance.py \
    --input "/Users/markjaysonfarol13/Higher Ed research/IPEDS/Paneled Datasets/Crosssections/panel_wide_raw_${YEAR}.csv" \
    --output-long "/Users/markjaysonfarol13/Higher Ed research/IPEDS/Parquets/Long/finance_step0_long_${YEAR}.parquet" \
    --output-wide "/Users/markjaysonfarol13/Higher Ed research/IPEDS/Paneled Datasets/Raw panel/Finance/finance_step0_wide_${YEAR}.csv"

done

# quick coverage check
python3 - <<'PY'
import pandas as pd
from pathlib import Path

base = Path("/Users/markjaysonfarol13/Higher Ed research/IPEDS/Parquets/Long")
rows = []

for year in range(2004, 2025):
    path = base / f"finance_step0_long_{year}.parquet"
    try:
        df = pd.read_parquet(path)
    except FileNotFoundError:
        print(f"Missing Step 0 file for YEAR={year}: {path}")
        continue

    # Group within this year only
    counts = (
        df.groupby("form_family")
          .size()
          .reset_index(name="n_rows")
    )
    counts["YEAR"] = year
    rows.append(counts)

if rows:
    summary = pd.concat(rows, ignore_index=True)[["YEAR", "form_family", "n_rows"]]
    summary = summary.sort_values(["YEAR", "form_family"])
    print(summary.head(30))  # show first 30 rows
else:
    print("No Step 0 files found.")
PY


# 5. Build crosswalk template (then edit manually)
python3 finance_build_crosswalk_template.py \
  --dict-lake "/Users/markjaysonfarol13/Higher Ed research/IPEDS/Parquets/dictionary_lake.parquet" \
  --output "/Users/markjaysonfarol13/Higher Ed research/IPEDS/Paneled Datasets/Crosswalks/finance_crosswalk_template.csv" \
  --year-min 2004 \
  --year-max 2023

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


  python3 build_raw_panel.py \
    --root "/Users/markjaysonfarol13/Higher Ed research/IPEDS/Cross sectional Datas" \
    --years 2023 \
    --surveys HD,IC,IC_AY,EF,E12,EFIA,E1D,EFFY,SFA,FIN,F1A,F2A,F3A,GR,GR200,ADM,OM,CST \
    --output "/Users/markjaysonfarol13/Higher Ed research/IPEDS/Parquets/panel_long_raw_2023.parquet"

  python3 panelize_raw.py \
    --input "/Users/markjaysonfarol13/Higher Ed research/IPEDS/Parquets/panel_long_raw_2023.parquet" \
    --output "/Users/markjaysonfarol13/Higher Ed research/IPEDS/Paneled Datasets/Crosssections/panel_wide_raw_2023.csv" \
    --column-field source_var \
    --survey-order "HD,IC,IC_AY,EF,E12,EFIA,E1D,EFFY,SFA,FIN,F1A,F2A,F3A,ADM,GR,GR200,OM,CST"



# Enrollment Step 0: unify enrollment data across years
python3 /Users/markjaysonfarol13/Documents/GitHub/IPEDS_Paneling/unify_enrollment.py \
  --dictionary "/Users/markjaysonfarol13/Higher Ed research/IPEDS/Parquets/dictionary_lake.parquet" \
  --panel-root "/Users/markjaysonfarol13/Higher Ed research/IPEDS/Paneled Datasets/Crosssections" \
  --years "2004-2024" \
  --output "/Users/markjaysonfarol13/Higher Ed research/IPEDS/Parquets/Enrollment0/enrollment_step0_long.parquet"


# Enrollment Crosswalk
python3 "/Users/markjaysonfarol13/Documents/GitHub/IPEDS_Paneling/enrollment_build_crosswalk_template.py" \
  --dictionary "/Users/markjaysonfarol13/Higher Ed research/IPEDS/Parquets/dictionary_lake.parquet" \
  --years "2004-2024" \
  --output "/Users/markjaysonfarol13/Higher Ed research/IPEDS/Paneled Datasets/Crosswalks/enrollment_crosswalk_template.csv"




python3 /Users/markjaysonfarol13/Documents/GitHub/IPEDS_Paneling/harmonize_enrollment_concepts.py \
  --step0 "/Users/markjaysonfarol13/Higher Ed research/IPEDS/Parquets/Enrollment0/enrollment_step0_long.parquet" \
  --crosswalk "/Users/markjaysonfarol13/Higher Ed research/IPEDS/Paneled Datasets/Crosssections/enrollment_crosswalk_template.csv" \
  --output "/Users/markjaysonfarol13/Higher Ed research/IPEDS/Parquets/Wide/Enrollment/enrollment_concepts_wide.parquet"

cd "/Users/markjaysonfarol13/Higher Ed research/IPEDS"

python3 validate_enrollment_panel.py \
  --input "/Users/markjaysonfarol13/Higher Ed research/IPEDS/Parquets/enrollment_concepts_wide.parquet" \
  --output-dir "/Users/markjaysonfarol13/Higher Ed research/IPEDS/Parquets/validation_enrollment"
