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
FINANCE_CROSSWALK_FULL="$FILLED_CROSSWALKS/finance_crosswalk_filled.csv"
FINANCE_CROSSWALK="$FILLED_CROSSWALKS/finance_crosswalk_core_only.csv"
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
python3 - <<'PY'
import pandas as pd
from pathlib import Path
full = Path("$FINANCE_CROSSWALK_FULL")
core = Path("$FINANCE_CROSSWALK")
df = pd.read_csv(full)
mask = ~df["concept_key"].astype(str).str.contains("UNRESTRICTED|TEMP_RESTRICTED|PERM_RESTRICTED", na=False)
df_core = df[mask]
core.parent.mkdir(parents=True, exist_ok=True)
df_core.to_csv(core, index=False)
print(f"[INFO] Filtered finance crosswalk to core concepts: kept {len(df_core)}/{len(df)} rows at {core}")
PY

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

echo "Cleanup: Harmonize scripts (step 5) edits"

ADM__ADM_SAT_EBRW_25_PCT_NEW can be integrated to ADM__ADM_SAT_CR_25_PCT_OLD
ADM__ADM_SAT_EBRW_75_PCT_NEW can be integrated to ADM__ADM_SAT_CR_75_PCT_OLD
ADM__ADM_SAT_MATH_25_PCT_NEW can be integrated to ADM__ADM_SAT_MATH_25_PCT_OLD
ADM__ADM_SAT_MATH_75_PCT_NEW can be integrated to ADM__ADM_SAT_MATH_75_PCT_OLD
ADM__ADM_SAT_WRIT_25_PCT_OLD can be integrated to ADM__ADM_SAT_WRIT_25_PCT_NEW
ADM__ADM_SAT_WRIT_75_PCT_OLD can be integrated to ADM__ADM_SAT_WRIT_75_PCT_NEW




lets redo the CrossWalk Scripts/Fill Scripts/auto_fill_sfa_crosswalk.py. erase everything and lets build it. we have Paneled Datasets/Crosswalks/sfa_crosswalk_template.csv. Now i want to autofill the template with concept keys. These are the variables i really care about. I included there source_var names:

1. Residency and Student counts
Student counts cohort:
SCFA1N, SCFY1N: Number of students in fall cohort
SCFA2DG, SCFY2DG: Total number of degree/certificate-seeking undergraduates - fall cohort
Total undergraduates:
SCFA2, SCFY2, SCUGRAD: Total number of undergraduates
SCUGDGSK: Total number of degree/certificate-seeking undergraduate students
SCUGFFN: Total number of full-time first-time degree/certificate seeking undergraduates - financial aid cohort
SCFA11N, SCFY11N: Number of students in fall cohort who are paying in-district tuition rates
SCFA12N, SCFY12N: Number of students in fall cohort who are paying in-state tuititon rates
SCFA13N, SCFY13N: Number of students in fall cohort who are paying out-of-state tuititon rates
SCFA14N, SCFY14N: Number of students in fall cohort whose residence/tuition rate is unknown

2. Living status:
All income:
GIS4ON0, GIS4ON1, GIS4ON2, GISTON0, GISTON1, GISTON2: Number living on-campus in all income levels Title IV Cohort
GIS4WF0, GIS4WF1, GIS4WF2, GISTWF0, GISTWF1, GISTWF2: Number living off-campus with family in all income levels Title IV Cohort
GIS4OF0, GIS4OF1, GIS4OF2, GISTOF0, GISTOF1, GISTOF2: Number living off-campus not with family Title IV Cohort
GIS4UN0, GIS4UN1, GIS4UN2, GIS4UN2, GISTUN0, GISTUN1,GISTUN2: Number with unknown living status
Income levels:
GIS4N10, GIS4N11, GIS4N12: Number in income level (0-30,000) Title IV Cohort
GIS4N20, GIS4N21, GIS4N22: Number in income level (30,001-48,000) Title IV Cohort
GIS4N30, GIS4N31, GIS4N32: Number in income level (48,001-75,000) Title IV Cohort
GIS4N40, GIS4N41, GIS4N42: Number in income level (75,001-110,000) Title IV Cohort
GIS4N50, GIS4N51, GIS4N52: Number in income level (over 110,000) Title IV Cohort

GRNTON0, GRNTON1, GRNTON2:  Number living on-campus. Grant/Scholarship Cohort
GRNTWF0, GRNTWF1, GRNTWF2: Number living off-campus with family. Grant/Scholarship Cohort
GRNTOF0, GRNTOF1, GRNTOF2: Number living off-campus not with family. Grant/Scholarship Cohort
GRNTUN0, GRNTUN1, GRNTUN2: Number with unknown living status. Grant/Scholarship Cohort

GRN4N10, GRN4N11, GRN4N12: Number in income level (0-30,000)
GRN4N20, GRN4N21, GRN4N22: Number in income level (30,001-48,000)
GRN4N30, GRN4N31, GRN4N32: Number in income level (48,001-75,000)
GRN4N40, GRN4N41, GRN4N42: Number in income level (75,001-110,000)
GRN4N50, GRN4N51, GRN4N52: Number in income level (over 110,000)

3. Net Price:

NPIS410, NPIS411, NPIS412, NPT410, NPT411, NPT412: Average net price (income 0-30,000)-students awarded Title IV federal financial aid
NPIS420, NPIS421, NPIS422, NPT420, NPT421, NPT422: Average net price (income 30,001-48,000)-students awarded Title IV federal financial aid
NPIS430, NPIS431, NPIS432, NPT430, NPT431, NPT432: Average net price (income 48,001-75,000)-students awarded Title IV federal financial aid
NPIS440, NPIS441, NPIS442, NPT440, NPT441, NPT442: Average net price (income 75,001-110,000)-students awarded Title IV federal financial aid
NPIS450, NPIS451, NPIS452, NPT450, NPT451, NPT452: Average net price (income over 110,000)-students awarded Title IV federal financial aid

NPIST0, NPIST1, NPIST2, NPGRN0, NPGRN1, NPGRN2: Average net price-students awarded grant or scholarship aid

4. Financial Aid:
FALL COHORT Entering Class
PGRNT_T: Total amount of Pell grant aid received by full-time first-time undergraduates.
PGRNT_N: Number of full-time first-time undergraduates receiving Pell grants
PGRNT_A: Average amount of Pell grant aid received by full-time first-time undergraduates

OFGRT_T: Total amount of other federal grant aid received by full-time first-time undergraduates
OFGRT_N: Number of full-time first-time undergraduates receiving other federal grant aid
OFGRT_A: Average amount of other federal grant aid received by full-time first-time undergraduates

SGRNT_T: Average amount of state/local grant aid received by full-time first-time undergraduates
SGRNT_N: Number of full-time first-time undergraduates receiving state/local grant aid
SGRNT_A: Average amount of state/local grant aid received by full-time first-time undergraduates

IGRNT_T: Total amount of institutional grant aid received by full-time first-time undergraduates
IGRNT_N: Number of full-time first-time undergraduates receiving  institutional grant aid
IGRNT_A: Average amount of institutional grant aid received by full-time first-time undergraduates

FLOAN_T: Total amount of Federal student loan aid received by full-time first-time undergraduates
FLOAN_N: Number of full-time first-time undergraduates receiving Federal student loan aid
FLOAN_A: Average amount of Federal student loan aid received by full-time first-time under

OLOAN_T: Total amount of other student loan aid received by full-time first-time undergraduates
OLOAN_N: Number of full-time first-time undergraduates receiving other student loan aid
OLOAN_A: Average amount of other student loan aid received by full-time first-time undergraduates

All undergraduates:
UPGRNTT: Total amount of Pell grant aid received by undergraduate students. Total amount of Pell grant aid awarded by undergraduate students
UPGRNTN: Number of undergraduate students receiving Pell grants. Number of undergraduate students awarded Pell grants.
UPGRNTA: Average amount of Pell grant aid received by undergraduate students. Average amount of Pell grant aid awarded to undergraduate students

UAGRNTT: Total amount of federal, state, local, institutional or other sources of grant aid dollars received by undergraduate students
UAGRNTA: Average amount of federal, state, local, institutional or other sources of grant aid dollars received by undergraduate students
UAGRNTN: Number of undergraduate students awarded federal, state, local, institutional or other sources of grant aid

UFLOANT: Total amount of Federal student loan aid received by undergraduate students. Total amount of federal student loans awarded to undergraduate students, 
UFLOANN: Number of undergraduate students receiving federal student loans. Number of undergraduate students awarded federal student loans
UFLOANA: Average amount of federal student loans awarded to undergraduate students. Average amount of federal student loan aid received by undergraduate students.

generate CrossWalk Scripts/Fill Scripts/auto_fill_sfa_crosswalk.py to fill the concept key of the following variables. I provided what concept key they should be and the variables. FOr those that do not need a unification concept key, just use its source_var as the concept key. Unifications concept key:

1. Residency and Student counts
SFA_ALL_N_UG: SCFA2, SCFY2, SCUGRAD 
SFA_COHORT_N_FTFT: SCFA1N, SCFY1N, SCUGFFN, AIDFSIN, AIDFSIN
SFA_COHORT_N_DS_UG: SCFA2DG, SCFY2DG, SCUGDGSK

SFA_RESIDENCY_INDT_N: SCFA11N, SCFY11N
SFA_RESIDENCY_INST_N: SCFA12N, SCFY12N
SFA_RESIDENCY_OUTST_N: SCFA13N, SCFY13N
SFA_RESIDENCY_UNKWN_N: SCFA14N, SCFY14N

GISTN0, GISTN1, GISTN2


2. Living status :
SFA_LIVING_ONC_T4_N: GIS4ON0 GIS4ON1 GIS4ON2, GISTON0, GISTON1, GISTON2
SFA_LIVING_OFFWF_T4_N: GIS4WF0, GIS4WF1, GIS4WF2, GISTWF0, GISTWF1, GISTWF2
SFA_LIVING_OFFNWF_T4_N: GIS4OF0, GIS4OF1, GIS4OF2, GISTOF0, GISTOF1, GISTOF2
SFA_LIVING_UNKWN_T4_N: GIS4UN0, GIS4UN1, GIS4UN2, GISTUN0, GISTUN1,GISTUN2

SFA_LIVING_ONC_GR_N: GRN4ON0 GRN4ON1 GRN4ON2, GRNTON0, GRNTON1, GRNTON2
SFA_LIVING_OFFWF_GR_N: GRN4WF0, GRN4WF1, GRN4WF2, GRNTWF0, GRNTWF1, GRNTWF2
SFA_LIVING_OFFNWF_GR_N:  GRN4OF0, GRN4OF1, GRN4OF2, GRNTOF0, GRNTOF1, GRNTOF2
SFA_LIVING_UNKWN_GR_N: GRN4UN0, GRN4UN1, GRN4UN2, GRNTUN0, GRNTUN1, GRNTUN2

SFA_T4N_N:  GIS4N0, GIS4N1, GIS4N2 
SFA_T4N1_N: GIS4N10, GIS4N11, GIS4N12
SFA_T4N2_N: GIS4N20, GIS4N21, GIS4N22
SFA_T4N3_N: GIS4N30, GIS4N31, GIS4N32
SFA_T4N4_N: GIS4N40, GIS4N41, GIS4N42
SFA_T4N5_N: GIS4N50, GIS4N51, GIS4N52

SFA_T4N_G: GIS4G0, GIS4G1, GIS4G2
SFA_T4N1_G1: GIS4G10, GIS4G11, GIS4G12
SFA_T4N2_G2: GIS4G20, GIS4G21, GIS4G22
SFA_T4N3_G3: GIS4G30, GIS4G31, GIS4G32
SFA_T4N4_G4: GIS4G40, GIS4G41, GIS4G42
SFA_T4N5_G5: GRN4N50, GRN4N51, GRN4N52

SFA_GRN_N:  GRN4N0, GRN4N1, GRN4N2, GRNTN0, GRNTN1, GRNTN2
SFA_GRN1_N: GRN4G10, GRN4G11, GRN4G12
SFA_GRN2_N: GRN4G20, GRN4G21, GRN4G22
SFA_GRN3_N: GRN4G30, GRN4G31, GRN4G32
SFA_GRN4_N: GRN4G40, GRN4G41, GRN4G42
SFA_GRN5_N: GIS4G50, GIS4G51, GIS4G52

SFA_GRN_G: GRN4G0, GRN4G1, GRN4G2
SFA_GRN1_G: GRN4N10, GRN4N11, GRN4N12
SFA_GRN2_G: GRN4N20, GRN4N21, GRN4N22
SFA_GRN3_G: GRN4N30, GRN4N31, GRN4N32
SFA_GRN4_G: GRN4N40, GRN4N41, GRN4N42
SFA_GRN5_G: GRN4G50, GRN4G51, GRN4G52

3. Net Price
SFA_NP_T4N_A: NPIST0, NPIST1, NPIST2, NPGRN0,NPGRN1, NPGRN2
SFA_NP_T4N1_A: NPIS410, NPIS411, NPIS412, NPT410, NPT411, NPT412
SFA_NP_T4N2_A: NPIS420, NPIS421, NPIS422, NPT420, NPT421, NPT422
SFA_NP_T4N3_A: NPIS430, NPIS431, NPIS432, NPT430, NPT431, NPT432
SFA_NP_T4N4_A: NPIS440, NPIS441, NPIS442, NPT440, NPT441, NPT442
SFA_NP_T4N5_A: NPIS450, NPIS451, NPIS452, NPT450, NPT451, NPT452

So, when we panelize, I should have the following unified variables in order:
SFA_ALL_N_UG, SFA_COHORT_N_FTFT, SFA_COHORT_N_DS_UG,
SFA_RESIDENCY_INDT_N, SFA_RESIDENCY_INST_N, SFA_RESIDENCY_OUTST_N, SFA_RESIDENCY_UNKWN_N,
SFA_LIVING_ONC_T4_N, SFA_LIVING_OFFWF_T4_N, SFA_LIVING_OFFNWF_T4_N, SFA_LIVING_UNKWN_T4_N,
SFA_LIVING_ONC_GR_N, SFA_LIVING_OFFWF_GR_N, SFA_LIVING_OFFNWF_GR_N, SFA_LIVING_UNKWN_GR_N,
SFA_T4N_N, SFA_T4N1_N, SFA_T4N2_N, SFA_T4N3_N, SFA_T4N4_N, SFA_T4N5_N,
SFA_T4N_G, SFA_T4N1_G1, SFA_T4N2_G2, SFA_T4N3_G3, SFA_T4N4_G4, SFA_T4N5_G5,
SFA_GRN_N, SFA_GRN1_N, SFA_GRN2_N, SFA_GRN3_N, SFA_GRN4_N, SFA_GRN5_N,
SFA_GRN_G, SFA_GRN1_G, SFA_GRN2_G, SFA_GRN3_G, SFA_GRN4_G, SFA_GRN5_G,
SFA_NP_T4N_A, SFA_NP_T4N1_A, SFA_NP_T4N2_A, SFA_NP_T4N3_A, SFA_NP_T4N4_A, SFA_NP_T4N5_A,
PGRNT_T, PGRNT_N, PGRNT_A, OFGRT_T, OFGRT_N, OFGRT_A, SGRNT_T, SGRNT_N, SGRNT_A, IGRNT_T, IGRNT_N, IGRNT_A, FLOAN_T, FLOAN_N, FLOAN_A, OLOAN_T, OLOAN_N, OLOAN_A,
UPGRNTT, UPGRNTN, UPGRNTA, UAGRNTT, UAGRNTN, UAGRNTA, UFLOANT, UFLOANN, UFLOANA

 map both to a single concept and coalesce

 Harmonize Scripts/harmonize_sfa_concepts.py





COA for in-district students living on campus
CINDON
COA for in-district students living off campus (with family) 
CINDFAM
COA for in-district students living off campus (not with family) 
CINDOFF
COA Comprehensive for in-district students
CMP1AY3

COA for in-state students living on campus 
CINSON
COA for in-state students living off campus (with family) 
CINSFAM
COA for in-state students living off campus (not with family) 
CINSOFF
COA Comprehensive for in-state students
CMP2AY3

COA for out-of-state students living on campus (with family) 
COTSON
COA for out-of-state students living off campus (with family) 
COTSFAM
CINSON
Total price for in-state students living off campus (with family) 
CINSFAM
Total price for in-state students living off campus (not with family) 
CINSOFF
COA Comprehensive for out-of-state students
CMP3AY3

Total price for out-of-state students living on campus (with family) 
COTSON
Total price for out-of-state students living off campus (with family) 
COTSFAM
Total price for out-of-state students living off campus (not with family) 
COTSOFF

Total Price PY
CMP1PY3 

Published in-district tuition
PCCHG1AT3 TUITION1 CHG1AT3
Published in-state tuition
PCCHG2AT3 TUITION2 CHG2AT3
Published out-of-state tuition
PCCHG3AT3 TUITION3 CHG3AT3

Published in-district fees
PCCHG1AF3 FEE1 CHG1AF3
Published in-state fees
PCCHG2AF3 FEE2 CHG2AF3
Published out-of-state fees
PCCHG3AF3 FEE3 CHG3AF3

Published in-district tuition and fees
PCCHG1AY3 CHG1AY3
Published in-state tuition and fees
PCCHG2AY3 CHG2AY3
Published out-of-state tuition and fees
PCCHG3AY3 CHG3AY3
Published tuition and fees
PCCHG1PY3 CHG1PY3

Books and supplies
CHG4AY3 CHG4PY3 PCCHG4AY3 PCCHG4PY3

On campus, room and board
CHG5AY3 CHG5PY3 PCCHG5AY3 PCCHG5PY3 PCCHG5PY3 RMBRDAMT BOARDAMT

On campus, other expenses
CHG6AY3 CHG6PY3 PCCHG6AY3 PCCHG6PY3

Off campus (not with family), room and board
CHG7AY3 CHG7PY3 PCCHG7AY3 PCCHG7PY3

Off campus (not with family), other expenses
CHG8AY3 CHG8PY3 PCCHG8AY3 PCCHG8PY3 

Off campus (with family), other expenses
CHG9AY3 CHG9PY3 PCCHG9AY3 PCCHG9PY3

Flags: TUITVARY



ICAY_COA_INDONC: COA for in-district students living on campus
ICAY_COA_INDFAM: COA for in-district students living off campus (with family)
ICAY_COA_INDOFFC: COA for in-district students living off campus (not with family)
ICAY_COA_COMPIND: COA Comprehensive for in-district students
ICAY_COA_INSTC: COA for in-state students living on campus
ICAY_COA_INSTFAM: COA for in-state students living off campus (with family)
ICAY_COA_INSTOFF: COA for in-state students living off campus (not with family)
ICAY_COA_COMPSTATE: COA Comprehensive for in-state students
ICAY_COA_OUTSON: COA for out-of-state students living on campus
ICAY_COA_OUTSFAM: COA for out-of-state students living off campus (with family)
ICAY_COA_OUTSOFF: COA for out-of-state students living off campus (not with family)
ICAY_COA_COMPOUTST: COA Comprehensive for out-of-state students
ICAY_COA_PY:  Total Price PY

ICAY_T_IND:  in-district tuition
ICAY_F_IND:  in-district fees
ICAY_TF_IND:  in-district tuition and fees
ICAY_T_STATE:  in-state tuition
ICAY_F_STATE:  in-state fees
ICAY_TF_STATE:  in-state tuition and fees
ICAY_T_OUTST:  out-of-state tuition
ICAY_F_OUTST:  out-of-state fees
ICAY_TF_OUTST:  out-of-state tuition and fees
ICAY_TOT_PY:  Published tuition and fees PY
ICAY_BOOKSUPP: Books and supplies
ICAY_ONCRMBRD:  campus, room and board
ICAY_ONCOTHEXP:  campus, room and board
ICAY_OFFCRMBRD: Off campus (not with family), room and board
ICAY_OFFCOTHEXP: Off campus (not with family), other expenses
ICAY_OFFCFOTHEXP: Off campus (with family), other expenses

ICAY_COA_INDONC, ICAY_COA_INDFAM, ICAY_COA_INDOFFC, ICAY_COA_COMPIND,
ICAY_COA_INSTC, ICAY_COA_INSTFAM, ICAY_COA_INSTOFF, ICAY_COA_COMPSTATE,
ICAY_COA_OUTSON, ICAY_COA_OUTSFAM, ICAY_COA_OUTSOFF, ICAY_COA_COMPOUTST,
ICAY_COA_PY,
ICAY_T_IND, ICAY_F_IND, ICAY_TF_IND,
ICAY_T_STATE, ICAY_F_STATE, ICAY_TF_STATE,
ICAY_T_OUTST, ICAY_F_OUTST, ICAY_TF_OUTST,
ICAY_TOT_PY, ICAY_BOOKSUPP,
ICAY_ONCRMBRD, ICAY_ONCOTHEXP,
ICAY_OFFCRMBRD, ICAY_OFFCOTHEXP, ICAY_OFFCFOTHEXP
