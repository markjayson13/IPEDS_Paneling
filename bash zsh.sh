#Code runs


#Build Dictionaries
python3 01_ingest_dictionaries.py \
  --root "/Users/markjaysonfarol13/Higher Ed research/IPEDS/Cross sectional Datas" \
  --output "/Users/markjaysonfarol13/Higher Ed research/IPEDS/Parquets/dictionary_lake.parquet"


#==============================================================================
# 2004-2007
#==============================================================================
# aggregate_f1_components.py is deprecated; component aggregation now happens via the finance crosswalk
# python3 scripts/aggregate_f1_components.py \
#   --root "/Users/markjaysonfarol13/Higher Ed research/IPEDS/Cross sectional Datas" \
#   --years 2004-2007 \
#   --log-level INFO

# Build raw panel parquet files for 2004-2007
for YEAR in 2004 2005 2006 2007; do
  python3 build_raw_panel.py \
    --root "/Users/markjaysonfarol13/Higher Ed research/IPEDS/Cross sectional Datas" \
    --years "$YEAR" \
    --surveys HD,IC,IC_AY,EF,E12,EFIA,E1D,EFFY,SFA,FIN,F1A,F2A,F3A,GR,GR200,ADM,OM,CST \
    --output "/Users/markjaysonfarol13/Higher Ed research/IPEDS/Parquets/panel_long_raw_${YEAR}.parquet"
done

# Panelize long -> wide per year
for YEAR in 2004 2005 2006 2007; do
  python3 panelize_raw.py \
    --input "/Users/markjaysonfarol13/Higher Ed research/IPEDS/Parquets/panel_long_raw_${YEAR}.parquet" \
    --output "/Users/markjaysonfarol13/Higher Ed research/IPEDS/Paneled Datasets/Crosssections/panel_wide_raw_${YEAR}.csv" \
    --column-field source_var \
    --survey-order "HD,IC,IC_AY,EF,E12,EFIA,E1D,EFFY,SFA,F1A,F2A,F3A,ADM,GR,GR200,ADM,OM,CST"
done

# Finance Unification for 2004-2007
for YEAR in 2004 2005 2006 2007; do
  python3 unify_finance.py \
    --input "/Users/markjaysonfarol13/Higher Ed research/IPEDS/Paneled Datasets/Crosssections/panel_wide_raw_${YEAR}.csv" \
    --year "$YEAR" 
done

#Check Finance Unification Logs before proceeding to the next steps
python3 - <<'PY'
import pandas as pd
path = "/Users/markjaysonfarol13/Higher Ed research/IPEDS/Paneled Datasets/Raw panel/finance_unified_wide_2004.csv"
df = pd.read_csv(path, dtype=str)
forms = df["finance_form_used"].value_counts(dropna=False).to_dict() if "finance_form_used" in df.columns else {}
print(path.split("/")[-1], "rows", len(df), "forms", forms)
PY

#==============================================================================
# 2008-2010
#==============================================================================
# Build raw panel parquet files for 2008-2010
for YEAR in 2008 2009 2010; do
  python3 build_raw_panel.py \
    --root "/Users/markjaysonfarol13/Higher Ed research/IPEDS/Cross sectional Datas" \
    --years "$YEAR" \
    --surveys HD,IC,IC_AY,EF,E12,EFIA,E1D,EFFY,SFA,FIN,F1A,F2A,F3A,GR,GR200,ADM,OM,CST \
    --output "/Users/markjaysonfarol13/Higher Ed research/IPEDS/Parquets/panel_long_raw_${YEAR}.parquet"
done

# Panelize long -> wide per year
for YEAR in 2008 2009 2010; do
  python3 panelize_raw.py \
    --input "/Users/markjaysonfarol13/Higher Ed research/IPEDS/Parquets/panel_long_raw_${YEAR}.parquet" \
    --output "/Users/markjaysonfarol13/Higher Ed research/IPEDS/Paneled Datasets/Crosssections/panel_wide_raw_${YEAR}.csv" \
    --column-field source_var \
    --survey-order "HD,IC,IC_AY,EF,E12,EFIA,E1D,EFFY,SFA,FIN,F1A,F2A,F3A,ADM,GR,GR200,ADM,OM,CST"
done

# Finance Unification for 2008-2010
  python3 unify_finance.py \
    --input "/Users/markjaysonfarol13/Higher Ed research/IPEDS/Paneled Datasets/Crosssections/panel_wide_raw_${YEAR}.csv" \
    --year "$YEAR"
done

#==============================================================================
# 2011-2013
#==============================================================================
# Build raw panel parquet files for 2011-2013
for YEAR in 2011 2012 2013; do
  python3 build_raw_panel.py \
    --root "/Users/markjaysonfarol13/Higher Ed research/IPEDS/Cross sectional Datas" \
    --years "$YEAR" \
    --surveys HD,IC,IC_AY,EF,E12,EFIA,E1D,EFFY,SFA,FIN,F1A,F2A,F3A,GR,GR200,ADM,OM,CST \
    --output "/Users/markjaysonfarol13/Higher Ed research/IPEDS/Parquets/panel_long_raw_${YEAR}.parquet"
done

# Panelize long -> wide per year
for YEAR in 2011 2012 2013; do
  python3 panelize_raw.py \
    --input "/Users/markjaysonfarol13/Higher Ed research/IPEDS/Parquets/panel_long_raw_${YEAR}.parquet" \
    --output "/Users/markjaysonfarol13/Higher Ed research/IPEDS/Paneled Datasets/Crosssections/panel_wide_raw_${YEAR}.csv" \
    --column-field source_var \
    --survey-order "HD,IC,IC_AY,EF,E12,EFIA,E1D,EFFY,SFA,FIN,F1A,F2A,F3A,ADM,GR,GR200,ADM,OM,CST"
done

# Finance Unification for 2011-2013
  python3 unify_finance.py \
    --input "/Users/markjaysonfarol13/Higher Ed research/IPEDS/Paneled Datasets/Crosssections/panel_wide_raw_${YEAR}.csv" \
    --year "$YEAR"
done

#==============================================================================
# 2014-2023
#==============================================================================
# Build raw panel parquet files for 2014-2023
for YEAR in 2014 2015 2016 2017 2018 2019 2020 2021 2022 2023; do
  python3 build_raw_panel.py \
    --root "/Users/markjaysonfarol13/Higher Ed research/IPEDS/Cross sectional Datas" \
    --years "$YEAR" \
    --surveys HD,IC,IC_AY,EF,E12,EFIA,E1D,EFFY,SFA,FIN,F1A,F2A,F3A,GR,GR200,ADM,OM,CST \
    --output "/Users/markjaysonfarol13/Higher Ed research/IPEDS/Parquets/panel_long_raw_${YEAR}.parquet"
done

# Panelize long -> wide per year
for YEAR in 2014 2015 2016 2017 2018 2019 2020 2021 2022 2023; do
  python3 panelize_raw.py \
    --input "/Users/markjaysonfarol13/Higher Ed research/IPEDS/Parquets/panel_long_raw_${YEAR}.parquet" \
    --output "/Users/markjaysonfarol13/Higher Ed research/IPEDS/Paneled Datasets/Crosssections/panel_wide_raw_${YEAR}.csv" \
    --column-field source_var \
    --survey-order "HD,IC,IC_AY,EF,E12,EFIA,E1D,EFFY,SFA,FIN,F1A,F2A,F3A,ADM,GR,GR200,ADM,OM,CST"
done

# Finance Unification for 2014-2023
  python3 unify_finance.py \
    --input "/Users/markjaysonfarol13/Higher Ed research/IPEDS/Paneled Datasets/Crosssections/panel_wide_raw_${YEAR}.csv" \
    --year "$YEAR"
done

#==============================================================================
# 2024 (Cost moves to CST; keep SFA for non-cost items)
#==============================================================================
# Build raw panel parquet files for 2024
for YEAR in 2024; do
  python3 build_raw_panel.py \
    --root "/Users/markjaysonfarol13/Higher Ed research/IPEDS/Cross sectional Datas" \
    --years "$YEAR" \
    --surveys HD,IC,IC_AY,EF,E12,EFIA,E1D,EFFY,SFA,F1A,F2A,F3A,GR,GR200,ADM,OM,CST \
    --output "/Users/markjaysonfarol13/Higher Ed research/IPEDS/Parquets/panel_long_raw_${YEAR}.parquet"
done
# Panelize long -> wide per year
for YEAR in 2024; do
  python3 panelize_raw.py \
    --input "/Users/markjaysonfarol13/Higher Ed research/IPEDS/Parquets/panel_long_raw_${YEAR}.parquet" \
    --output "/Users/markjaysonfarol13/Higher Ed research/IPEDS/Paneled Datasets/Crosssections/panel_wide_raw_${YEAR}.csv" \
    --column-field source_var \
    --survey-order "HD,IC,IC_AY,EF,E12,EFIA,E1D,EFFY,SFA,F1A,F2A,F3A,ADM,GR,GR200,ADM,OM,CST"
done
# Finance Unification for 2014-2023
  python3 unify_finance.py \
    --input "/Users/markjaysonfarol13/Higher Ed research/IPEDS/Paneled Datasets/Crosssections/panel_wide_raw_${YEAR}.csv" \
    --year "$YEAR"
done

#==============================================================================
# Merge all years 2004-2024
#==============================================================================

# Wide only
python3 merge_raw_panels.py \
  --input-dir "$OUT" \
  --pattern "panel_wide_raw_*.csv" \
  --output-wide "$OUT/panel_wide_raw_2004_2024_merged.csv" \
  --component-order "HD,IC,IC_AY,EF,E12,EFIA,E1D,EFFY,SFA,F1A,F2A,F3A,ADM,GR,GR200"


