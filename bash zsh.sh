# Code 
#Dictionary
Dictionary/01_ingest_dictionaries.py

# Panelize raw data scripts:
Panelize Scripts/build_raw_panel.py
Panelize Scripts/panelize_raw.py
Panelize Scripts/merge_raw_panels.py

#Fiannce Unify and crosswalk scripts:
Unification Scripts/unify_finance.py
CrossWalk Scripts/finance_build_crosswalk_template.py
Harmonize Scripts/harmonize_finance_concepts.py
Validation Scripts/finance_validate_panel.py

# Enrollment Unify and crosswalk scripts:
Unification /Users/markjaysonfarol13/Documents/GitHub/IPEDS_Paneling/Unification Scripts/unify_enrollment.py
Unification /Users/markjaysonfarol13/Documents/GitHub/IPEDS_Paneling/Unification Scripts/build_efres_residency_buckets.py
CrossWalk /Users/markjaysonfarol13/Documents/GitHub/IPEDS_Paneling/CrossWalk Scripts/enrollment_build_crosswalk_template.py
CrossWalk /Users/markjaysonfarol13/Documents/GitHub/IPEDS_Paneling/CrossWalk Scripts/autofill_enrollment_crosswalk_core.py

#HD and IC Unify and crosswalk scripts:
python3 "/Users/markjaysonfarol13/Documents/GitHub/IPEDS_Paneling/Unification Scripts/stabilize_hd.py" --run-smoke-test
python3 "/Users/markjaysonfarol13/Documents/GitHub/IPEDS_Paneling/CrossWalk Scripts/hd_build_crosswalk_template.py"
python3 "/Users/markjaysonfarol13/Documents/GitHub/IPEDS_Paneling/CrossWalk Scripts/Fill Scripts/auto_fill_hd_crosswalk.py"

# SFA Unify and crosswalk scripts:
python3 "/Users/markjaysonfarol13/Documents/GitHub/IPEDS_Paneling/Unification Scripts/unify_sfa.py"
python3 "/Users/markjaysonfarol13/Documents/GitHub/IPEDS_Paneling/CrossWalk Scripts/sfa_build_crosswalk_template.py"
python3 "/Users/markjaysonfarol13/Documents/GitHub/IPEDS_Paneling/Harmonize Scripts/harmonize_sfa_concepts.py"
python3 "/Users/markjaysonfarol13/Documents/GitHub/IPEDS_Paneling/Validation Scripts/validate_sfa_panel.py"

#ADM Unify and crosswalk scripts:
python3 "/Users/markjaysonfarol13/Documents/GitHub/IPEDS_Paneling/Unification Scripts/unify_admissions.py" 
python3 "/Users/markjaysonfarol13/Documents/GitHub/IPEDS_Paneling/CrossWalk Scripts/adm_build_crosswalk_template.py"
python3 "/Users/markjaysonfarol13/Documents/GitHub/IPEDS_Paneling/Harmonize Scripts/harmonize_admissions.py"
python3 "/Users/markjaysonfarol13/Documents/GitHub/IPEDS_Paneling/Validation Scripts/validate_admissions.py"










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
python3 /Users/markjaysonfarol13/Documents/GitHub/IPEDS_Paneling/merge_raw_panels.py \
  --input-dir "/Users/markjaysonfarol13/Higher Ed research/IPEDS/Paneled Datasets/Crosssections" \
  --pattern "panel_wide_raw_*.csv" \
  --output-wide "/Users/markjaysonfarol13/Higher Ed research/IPEDS/Paneled Datasets/Raw panel/panel_wide_raw_2004_2024_merged.csv" \
  --component-order "HD,IC,IC_AY,EF,E12,EFIA,E1D,EFFY,SFA,FIN,F1A,F2A,F3A,ADM,GR,GR200,OM,CST"

python3 - <<'PY'
import pandas as pd
from pathlib import Path

path = Path("/Users/markjaysonfarol13/Higher Ed research/IPEDS/Paneled Datasets/Crosssections/panel_wide_raw_2004_2024_merged.csv")
df = pd.read_csv(path, low_memory=False)

nunique = df.nunique(dropna=False)
stable_cols = nunique[nunique == 1].sort_index()
print("Stable columns (same value in every row):")
print(stable_cols)

# Example: look specifically at HD columns
hd_cols = [c for c in df.columns if c.startswith("HD")]
hd_nunique = nunique.loc[hd_cols].sort_values()
print("\nHD columns with few unique values:")
print(hd_nunique.head(20))
PY

#===============================================================
# Finance paneling steps
#===============================================================
# Finance Step 0: form-level extraction (F1/F2/F3 + components)
for YEAR in {2004..2024}; do
  echo "Running unify_finance for YEAR=${YEAR}..."

  python3 /Users/markjaysonfarol13/Documents/GitHub/IPEDS_Paneling/unify_finance.py \
    --input "/Users/markjaysonfarol13/Higher Ed research/IPEDS/Paneled Datasets/Crosssections/panel_wide_raw_${YEAR}.csv" \
    --output-long "/Users/markjaysonfarol13/Higher Ed research/IPEDS/Parquets/Unify/Step0Finlong/Step0Finlong_${YEAR}.parquet" \
    --output-wide "/Users/markjaysonfarol13/Higher Ed research/IPEDS/Paneled Datasets/Raw panel/Finance/Step0wide/Step0_${YEAR}.csv"

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

# Build crosswalk template
python3 /Users/markjaysonfarol13/Documents/GitHub/IPEDS_Paneling/finance_build_crosswalk_template.py \
  --dict-lake "/Users/markjaysonfarol13/Higher Ed research/IPEDS/Parquets/dictionary_lake.parquet" \
  --output "/Users/markjaysonfarol13/Higher Ed research/IPEDS/Paneled Datasets/Crosswalks/finance_crosswalk_template.csv" \
  --year-min 2004 \
  --year-max 2023

#  Edit finance_crosswalk_template.csv to fill concept_key, year ranges, weights.
python3 "/Users/markjaysonfarol13/Documents/GitHub/IPEDS_Paneling/fill_finance_crosswalk.py"

# Apply crosswalk to create concept-level finance panel
for YEAR in {2004..2023}; do
  python3 /Users/markjaysonfarol13/Documents/GitHub/IPEDS_Paneling/harmonize_finance_concepts.py \
    --step0 "/Users/markjaysonfarol13/Higher Ed research/IPEDS/Parquets/Unify/Step0Finlong/Step0Finlong_${YEAR}.parquet" \
    --crosswalk "/Users/markjaysonfarol13/Higher Ed research/IPEDS/Paneled Datasets/Crosswalks/Filled/finance_crosswalk_filled.csv" \
    --output-long "/Users/markjaysonfarol13/Higher Ed research/IPEDS/Parquets/Unify/Financelong/finance_concepts_long_${YEAR}.parquet" \
    --output-wide "/Users/markjaysonfarol13/Higher Ed research/IPEDS/Parquets/Unify/Financewide/finance_concepts_wide_${YEAR}.parquet" \
    --coverage "/Users/markjaysonfarol13/Higher Ed research/IPEDS/Paneled Datasets/Harmonized/Finance/finance_concepts_coverage_${YEAR}.csv"
done

# Validate the concept-wide panel
for YEAR in {2004..2023}; do
python3 "/Users/markjaysonfarol13/Documents/GitHub/IPEDS_Paneling/finance_validate_panel.py" \
  --panel "/Users/markjaysonfarol13/Higher Ed research/IPEDS/Parquets/Unify/Financewide/finance_concepts_wide_${YEAR}.parquet" \
  --tolerance 100000 \
  --tol-rel 0.05
done

#===============================================================
# Enrollment paneling steps
#===============================================================
# Enrollment Crosswalk
python3 "/Users/markjaysonfarol13/Documents/GitHub/IPEDS_Paneling/enrollment_build_crosswalk_template.py" \
  --dictionary "/Users/markjaysonfarol13/Higher Ed research/IPEDS/Parquets/dictionary_lake.parquet" \
  --years "2004-2024" \
  --output "/Users/markjaysonfarol13/Higher Ed research/IPEDS/Paneled Datasets/Crosswalks/enrollment_crosswalk_template.csv"

# Autofill the key EF/E12 concepts
python3 "/Users/markjaysonfarol13/Documents/GitHub/IPEDS_Paneling/autofill_enrollment_crosswalk_core.py" \
  --input "/Users/markjaysonfarol13/Higher Ed research/IPEDS/Paneled Datasets/Crosswalks/enrollment_crosswalk_template.csv" \
  --output "/Users/markjaysonfarol13/Higher Ed research/IPEDS/Paneled Datasets/Crosswalks/Filled/enrollment_crosswalk_autofilled.csv"

# Enrollment Step 0: unify enrollment data across years
python3 "/Users/markjaysonfarol13/Documents/GitHub/IPEDS_Paneling/unify_enrollment.py" \
  --dictionary "/Users/markjaysonfarol13/Higher Ed research/IPEDS/Parquets/dictionary_lake.parquet" \
  --panel-root "/Users/markjaysonfarol13/Higher Ed research/IPEDS/Paneled Datasets/Crosssections" \
  --years "2004-2024" \
  --output "/Users/markjaysonfarol13/Higher Ed research/IPEDS/Parquets/Unify/Enrolllong/enrollment_step0_long.parquet"

# Apply crosswalk to create concept-level enrollment panel
for YEAR in {2004..2024}; do
  python3 "/Users/markjaysonfarol13/Documents/GitHub/IPEDS_Paneling/harmonize_enrollment_concepts.py" \
    --step0 "/Users/markjaysonfarol13/Higher Ed research/IPEDS/Parquets/Unify/Enrolllong/enrollment_step0_long.parquet" \
    --crosswalk "/Users/markjaysonfarol13/Higher Ed research/IPEDS/Paneled Datasets/Crosswalks/Filled/enrollment_crosswalk_autofilled.csv" \
    --output "/Users/markjaysonfarol13/Higher Ed research/IPEDS/Parquets/Unify/Enrollwide/enrollment_concepts_wide_${YEAR}.parquet"
done

# Validate the concept-wide enrollment panel
python3 "/Users/markjaysonfarol13/Documents/GitHub/IPEDS_Paneling/validate_enrollment_panel.py" \
  --input "/Users/markjaysonfarol13/Higher Ed research/IPEDS/Parquets/Unify/Enrollwide/enrollment_concepts_wide.parquet" \
  --output-dir "/Users/markjaysonfarol13/Higher Ed research/IPEDS/Parquets/Unify/Enrollwide/enrollment_concepts_wide_${YEAR}.parquet"

# Build EFRES residency buckets
python3 "/Users/markjaysonfarol13/Documents/GitHub/IPEDS_Paneling/build_efres_residency_buckets.py" \
  --efres "/path/to/efres_long.parquet" \
  --hd "/path/to/hd_state_panel.parquet" \
  --output "/Users/markjaysonfarol13/Higher Ed research/IPEDS/Parquets/efres_residency_buckets.parquet"


