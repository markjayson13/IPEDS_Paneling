#!/usr/bin/env bash

: <<'LEGACY'
# Code 
python3 "Unification Scripts/combine_panel_wide_raw.py"
python3 "Unification Scripts/unify_sfa.py"


# Download IPEDS data for years 2004 to 2024
python3 "Download Scripts/download_ipeds.py" \
--out-root "/Users/markjaysonfarol13/Higher Ed research/IPEDS/Cross sectional Datas" \
--years 2004 (line 2024)

#Dictionary
python3 Dictionary/01_ingest_dictionaries.py

#Crosswalks
python3 "CrossWalk Scripts/hd_build_crosswalk_template.py"
python3 "CrossWalk Scripts/ic_ay_build_crosswalk_template.py"
python3 "CrossWalk Scripts/adm_build_crosswalk_template.py"
python3 "Cr/Users/markjaysonfarol13/Higher Ed research/IPEDS/Paneled Datasets/Crosswalks/Filled/enrollment_crosswalk_autofilled.csvossWalk Scripts/enrollment_build_crosswalk_template.py"
python3 "CrossWalk Scripts/sfa_build_crosswalk_template.py"
python3 "CrossWalk Scripts/finance_build_crosswalk_template.py"
    #Auto fill crosswalks
    python3 "CrossWalk Scripts/Fill Scripts/auto_fill_hd_crosswalk.py"
    python3 "CrossWalk Scripts/Fill Scripts/auto_fill_sfa_crosswalk.py"
    python3 "CrossWalk Scripts/Fill Scripts/autofill_enrollment_crosswalk_core.py"
    python3 "CrossWalk Scripts/Fill Scripts/fill_finance_crosswalk.py"

#Step 0 Unify scripts:
python3 "Unification Scripts/unify_admissions.py"
python3 "Unification Scripts/unify_enrollment.py"
python3 "Unification Scripts/unify_sfa.py"
python3 "Unification Scripts/unify_finance.py"
python3 "Unification Scripts/combine_step0_finance.py" 
python3 "Unification Scripts/build_efres_residency_buckets.py" 

#Harmonize scripts:
python3 "Harmonize Scripts/harmonize_admissions.py"
python3 "Harmonize Scripts/harmonize_enrollment_concepts.py"
python3 "Harmonize Scripts/stabilize_ic_ay.py" --overwrite
python3 "Harmonize Scripts/harmonize_sfa_concepts.py" \
  --input-long "$SFA_STEP0" \
  --crosswalk "$SFA_CROSSWALK"
python3 "Harmonize Scripts/harmonize_finance_concepts.py"
python3 "Harmonize Scripts/stabilize_hd.py" --crosswalk "$HD_CROSSWALK"

#Global Harmonization & Panel Build
python3 harmonize_new.py \
  --root "/Users/markjaysonfarol13/Higher Ed research/IPEDS/Cross sectional Datas"\
  --lake "/Users/markjaysonfarol13/Higher Ed research/IPEDS/Parquets/Dictionary/dictionary_lake.parquet" \
  --years 2004:2024 \
  --output "/Users/markjaysonfarol13/Higher Ed research/IPEDS/Parquets/panel_long.parquet" \
  --rules validation_rules.yaml \
  --strict-release \
  --strict-coverage

# Final Wide Panel
python3 panelize_panel.py \
  --source "/Users/markjaysonfarol13/Higher Ed research/IPEDS/Parquets/panel_long.parquet" \
  --output "/Users/markjaysonfarol13/Higher Ed research/IPEDS/Paneled Datasets/Final/panel_wide.csv"

# Validation scripts:
python3 "Validation Scripts/hd_validate_master_panel.py"
python3 "Validation Scripts/validate_admissions.py"

python3 "Validation Scripts/validate_enrollment_panel.py"
python3 "Validation Scripts/validate_sfa_panel.py"
python3 "Validation Scripts/finance_validate_panel.py"
python3 "Validation Scripts/validate_ic_ay.py"
LEGACY


: <<'LEGACY_STEP0'
# Panelize raw data scripts:
python3 Scripts/build_raw_panel.py
python3 Scripts/panelize_raw.py
python3 Scripts/merge_raw_panels.py

#HD and IC Unify and crosswalk scripts:
python3 "CrossWalk Scripts/hd_build_crosswalk_template.py"
python3 "CrossWalk Scripts/Fill Scripts/auto_fill_hd_crosswalk.py"
python3 "Harmonize Scripts/stabilize_hd.py"
python3 "Validation Scripts/hd_validate_master_panel.py"


python3 "CrossWalk Scripts/ic_ay_build_crosswalk_template.py"
python3 "Harmonize Scripts/stabilize_ic_ay.py" --overwrite
python3 "Validation Scripts/validate_ic_ay.py"


# SFA Unify and crosswalk scripts:
python3 "Unification Scripts/combine_step0_finance.py"
python3 "/Users/markjaysonfarol13/Documents/GitHub/IPEDS_Paneling/Unification Scripts/unify_sfa.py"
python3 "/Users/markjaysonfarol13/Documents/GitHub/IPEDS_Paneling/CrossWalk Scripts/sfa_build_crosswalk_template.py"
python3 "Harmonize Scripts/harmonize_sfa_concepts.py" \
  --input-long "/Users/markjaysonfarol13/Higher Ed research/IPEDS/Parquets/Unify/Step0sfa/sfa_step0_long.parquet" \
  --crosswalk "/Users/markjaysonfarol13/Higher Ed research/IPEDS/Paneled Datasets/Crosswalks/Filled/sfa_crosswalk_filled.csv"
python3 "Harmonize Scripts/harmonize_finance_concepts.py" \
  --step0 "/Users/markjaysonfarol13/Higher Ed research/IPEDS/Parquets/Unify/Step0Finlong/finance_step0_long.parquet"
python3 "Validation Scripts/validate_sfa_panel.py"

#ADM Unify and crosswalk scripts:
python3 "/Users/markjaysonfarol13/Documents/GitHub/IPEDS_Paneling/Unification Scripts/unify_admissions.py" 
python3 "/Users/markjaysonfarol13/Documents/GitHub/IPEDS_Paneling/CrossWalk Scripts/adm_build_crosswalk_template.py"
python3 "/Users/markjaysonfarol13/Documents/GitHub/IPEDS_Paneling/Harmonize Scripts/harmonize_admissions.py"
python3 "/Users/markjaysonfarol13/Documents/GitHub/IPEDS_Paneling/Validation Scripts/validate_admissions.py"

# Enrollment Unify and crosswalk scripts:
python3 "Unification Scripts/build_efres_residency_buckets.py"
python3 "CrossWalk Scripts/enrollment_build_crosswalk_template.py"
python3 "CrossWalk Scripts/Fill Scripts/autofill_enrollment_crosswalk_core.py"
python3 "Unification Scripts/unify_enrollment.py"
python3 "Validation Scripts/validate_enrollment_panel.py"

#Finance Unify and crosswalk scripts:
python3 "CrossWalk Scripts/finance_build_crosswalk_template.py"
python3 "CrossWalk Scripts/Fill Scripts/fill_finance_crosswalk.py"
python3 "Unification Scripts/combine_step0_finance.py"
python3 "Harmonize Scripts/harmonize_finance_concepts.py" \
  --step0 "/Users/markjaysonfarol13/Higher Ed research/IPEDS/Parquets/Unify/Step0Finlong/finance_step0_long.parquet"
LEGACY_STEP0

set -euo pipefail
IFS=$'\n\t'

require_file() {
  local path="$1"
  local label="$2"
  if [ ! -f "$path" ]; then
    echo "[ERROR] Missing ${label}: $path"
    echo "Please generate $label before rerunning this step."
    exit 1
  fi
}



# === User paths ===
REPO="/Users/markjaysonfarol13/Documents/GitHub/IPEDS_Paneling"
OUT_ROOT="/Users/markjaysonfarol13/Higher Ed research/IPEDS/Cross sectional Datas"
PARQUETS="/Users/markjaysonfarol13/Higher Ed research/IPEDS/Parquets"
PANELED_DIR="/Users/markjaysonfarol13/Higher Ed research/IPEDS/Paneled Datasets"
CROSSSECT_DIR="$PANELED_DIR/Crosssections"
CROSSWALK_DIR="$PANELED_DIR/Crosswalks"
FILLED_CROSSWALKS="$CROSSWALK_DIR/Filled"
DICT_LAKE="$PARQUETS/Dictionary/dictionary_lake.parquet"
PANEL_LONG="$PARQUETS/panel_long.parquet"
PANEL_WIDE="$PANELED_DIR/Final/panel_wide.csv"
PANEL_WIDE_CLEAN="$PANELED_DIR/Final/panel_wide_cleanparent.csv"
PANEL_WIDE_CLEANROBUST="$PANELED_DIR/Final/panel_wide_cleanrobust.csv"
PANEL_WIDE_RAW="/Users/markjaysonfarol13/Higher Ed research/IPEDS/Paneled Datasets/panel_wide_raw.csv"
ENROLL_STEP0="$PARQUETS/Unify/Enrolllong/enrollment_step0_long.parquet"
ENROLL_WIDE="$PARQUETS/Unify/Enrollwide/enrollment_concepts_wide.parquet"
HD_CROSSWALK="$FILLED_CROSSWALKS/hd_crosswalk.csv"
ENROLL_CROSSWALK="$FILLED_CROSSWALKS/enrollment_crosswalk_autofilled.csv"
SFA_CROSSWALK="$FILLED_CROSSWALKS/sfa_crosswalk_filled.csv"
FINANCE_CROSSWALK="$FILLED_CROSSWALKS/finance_crosswalk_filled.csv"
ICAY_CROSSWALK="$FILLED_CROSSWALKS/ic_ay_crosswalk_all.csv"
ADMISSIONS_STEP0="$PARQUETS/Unify/Step0adm/adm_step0_long.parquet"
SFA_STEP0="$PARQUETS/Unify/Step0sfa/sfa_step0_long.parquet"
FINANCE_STEP0="$PARQUETS/Unify/Step0Finlong/finance_step0_long.parquet"
FINANCE_CONCEPT_WIDE="$PARQUETS/Unify/Financewide/finance_concepts_wide.parquet"
FINANCE_CONCEPT_LONG="$PARQUETS/Unify/Financelong/finance_concepts_long.parquet"
EFRES_LONG="$PARQUETS/Unify/Enrolllong/efres_long.parquet"
HD_STATE_PANEL="$PARQUETS/Unify/HD/hd_state_panel.parquet"
HD_MASTER="$PARQUETS/Unify/HDICwide/hd_master_panel.parquet"
ADM_WIDE="$PARQUETS/Unify/ADMwide/adm_concepts_wide.parquet"
SFA_WIDE="$PARQUETS/Unify/SFAwide/sfa_concepts_wide.parquet"
ICAY_WIDE="$PARQUETS/Unify/ICAYwide/icay_concepts_wide.parquet"

cd "$REPO" || exit 1

echo "1) Merge raw cross-sections into yearly CSVs"
#python3 "Panelize Scripts/build_raw_panel.py" ...
#python3 "Panelize Scripts/panelize_raw.py" ...
#python3 "Panelize Scripts/merge_raw_panels.py"

echo "1b) Combine yearly raw files into master panel_wide_raw.csv"
python3 "Unification Scripts/combine_panel_wide_raw.py" \
  --input-dir "$CROSSSECT_DIR" \
  --pattern "panel_wide_raw_*.csv" \
  --output "$PANEL_WIDE_RAW"

echo "2) Step 0 unification (survey specific longs)"
python3 "Unification Scripts/unify_enrollment.py" \
  --dictionary "$DICT_LAKE" \
  --panel-root "$CROSSSECT_DIR" \
  --years "2004-2024" \
  --output "$ENROLL_STEP0"
python3 "Unification Scripts/unify_admissions.py" \
  --panel-dir "$CROSSSECT_DIR" \
  --year-start 2004 \
  --year-end 2024 \
  --dictionary-lake "$DICT_LAKE" \
  --output "$ADMISSIONS_STEP0"
python3 "Unification Scripts/unify_sfa.py" \
  --input-wide "$PANEL_WIDE_RAW" \
  --output-long "$SFA_STEP0" \
  --dictionary-lake "$DICT_LAKE"
python3 "Unification Scripts/unify_finance.py" \
  --input "$PANEL_WIDE_RAW" \
  --output-long "$FINANCE_STEP0" \
  --chunk-size 50000
if [ -f "$EFRES_LONG" ] && [ -f "$HD_STATE_PANEL" ]; then
  python3 "Unification Scripts/build_efres_residency_buckets.py" \
    --efres "$EFRES_LONG" \
    --hd "$HD_STATE_PANEL" \
    --output "$PARQUETS/Unify/Enrolllong/efres_residency_buckets.parquet"
else
  echo "Skipping build_efres_residency_buckets (missing $EFRES_LONG or $HD_STATE_PANEL)"
fi

echo "3) Build crosswalk templates"
python3 "CrossWalk Scripts/hd_build_crosswalk_template.py"
python3 "CrossWalk Scripts/ic_ay_build_crosswalk_template.py"
python3 "CrossWalk Scripts/enrollment_build_crosswalk_template.py"
python3 "CrossWalk Scripts/sfa_build_crosswalk_template.py"
python3 "CrossWalk Scripts/finance_build_crosswalk_template.py"
python3 "CrossWalk Scripts/adm_build_crosswalk_template.py"

echo "4) Generate filled/final crosswalks"
python3 "CrossWalk Scripts/Fill Scripts/auto_fill_hd_crosswalk.py"
python3 "CrossWalk Scripts/Fill Scripts/fill_ic_ay_crosswalk_all.py" --overwrite
python3 "CrossWalk Scripts/Fill Scripts/auto_fill_sfa_crosswalk.py"
python3 "CrossWalk Scripts/Fill Scripts/autofill_enrollment_crosswalk_core.py"
python3 "CrossWalk Scripts/Fill Scripts/fill_finance_crosswalk.py"

require_file "$HD_CROSSWALK" "HD crosswalk"
require_file "$ICAY_CROSSWALK" "ICAY crosswalk"
require_file "$ENROLL_CROSSWALK" "Enrollment crosswalk"
require_file "$SFA_CROSSWALK" "SFA crosswalk"
require_file "$FINANCE_CROSSWALK" "Finance crosswalk"

echo "5) Harmonize or stabilize by survey"
python3 "Harmonize Scripts/stabilize_hd.py"
python3 "Harmonize Scripts/stabilize_ic_ay.py" --crosswalk "$ICAY_CROSSWALK" --overwrite
python3 "Harmonize Scripts/harmonize_admissions.py"
python3 "Harmonize Scripts/harmonize_enrollment_concepts.py" \
  --step0 "$ENROLL_STEP0" \
  --crosswalk "$ENROLL_CROSSWALK" \
  --output "$ENROLL_WIDE"
python3 "Harmonize Scripts/harmonize_sfa_concepts.py"
python3 "Harmonize Scripts/harmonize_finance_concepts.py" \
  --step0 "$FINANCE_STEP0" \
  --crosswalk "$FINANCE_CROSSWALK" \
  --output-long "$FINANCE_CONCEPT_LONG" \
  --output-wide "$FINANCE_CONCEPT_WIDE" \
  --coverage "$CROSSWALK_DIR/finance_concepts_coverage.csv"

echo "6) Coverage validators"
python3 "Validation Scripts/validate_enrollment_crosswalk_coverage.py"
python3 "Validation Scripts/validate_ic_ay_coverage.py"
python3 "Validation Scripts/validate_ic_ay_year_scopes.py" \
  --crosswalk "$ICAY_CROSSWALK" \
  --out-dir "$PARQUETS/Validation"
python3 "Validation Scripts/check_sfa_coverage.py"

echo "7) Merge harmonized components into final panel"
python3 "Panelize Scripts/panelize_components.py" \
  --component hd="$HD_MASTER" \
  --component icay="$ICAY_WIDE" \
  --component adm="$ADM_WIDE" \
  --component enroll="$ENROLL_WIDE" \
  --component sfa="$SFA_WIDE" \
  --component finance="$FINANCE_CONCEPT_WIDE" \
  --parent-child-filter campus \
  --output "$PANEL_WIDE"

echo "8) Prune HD scaffolding for analysis panel"
python3 panel_prune_analysis.py \
  --input "$PANEL_WIDE" \
  --output "$PANEL_WIDE_CLEANROBUST"

echo "Done."

