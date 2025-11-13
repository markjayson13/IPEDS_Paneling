bash zsh

ROOT="/Users/markjaysonfarol13/Higher Ed research/IPEDS/Cross sectional Datas"
PARQ="/Users/markjaysonfarol13/Higher Ed research/IPEDS/Parquets"
OUT="/Users/markjaysonfarol13/Higher Ed research/IPEDS/Paneled Datasets/Crosssections"


#2004–2007 (no ADM, no E12 yet; use EFIA/E1D/EFFY for 12-month; FIN is F1A/F2A/F3A)
for Y in 2004 2005 2006 2007; do
  python3 build_raw_panel.py \
    --root "$ROOT" \
    --years $Y \
    --surveys HD,IC,IC_AY,EF,EFIA,E1D,EFFY,SFA,F1A,F2A,F3A,GR \
    --output "$PARQ/panel_long_raw_${Y}.parquet"

  python3 panelize_raw.py \
    --input "$PARQ/panel_long_raw_${Y}.parquet" \
    --output "$OUT/panel_wide_raw_${Y}.csv" \
    --column-field source_var
done

#2008–2010 (Admissions begins in 2008; still pre-E12 so keep EFIA/E1D/EFFY)
for Y in 2008 2009 2010; do
  python3 build_raw_panel.py \
    --root "$ROOT" \
    --years $Y \
    --surveys HD,IC,IC_AY,EF,EFIA,E1D,EFFY,SFA,F1A,F2A,F3A,GR,GR200,ADM \
    --output "$PARQ/panel_long_raw_${Y}.parquet"

  python3 panelize_raw.py \
    --input "$PARQ/panel_long_raw_${Y}.parquet" \
    --output "$OUT/panel_wide_raw_${Y}.csv" \
    --column-field source_var
done

#2011–2013 (official E12 starts; you can keep EFIA/E1D/EFFY in case filenames use them)
for Y in 2011 2012 2013; do
  python3 build_raw_panel.py \
    --root "$ROOT" \
    --years $Y \
    --surveys HD,IC,IC_AY,EF,E12,EFIA,E1D,EFFY,SFA,F1A,F2A,F3A,GR,GR200,ADM \
    --output "$PARQ/panel_long_raw_${Y}.parquet"

  python3 panelize_raw.py \
    --input "$PARQ/panel_long_raw_${Y}.parquet" \
    --output "$OUT/panel_wide_raw_${Y}.csv" \
    --column-field source_var
done

#2014–2023 (Outcome Measures starts 2014)
for Y in 2014 2015 2016 2017 2018 2019 2020 2021 2022 2023; do
  python3 build_raw_panel.py \
    --root "$ROOT" \
    --years $Y \
    --surveys HD,IC,IC_AY,EF,E12,SFA,F1A,F2A,F3A,GR,GR200,ADM,OM \
    --output "$PARQ/panel_long_raw_${Y}.parquet"

  python3 panelize_raw.py \
    --input "$PARQ/panel_long_raw_${Y}.parquet" \
    --output "$OUT/panel_wide_raw_${Y}.csv" \
    --column-field source_var
done

#2024 (Cost moves to CST; keep SFA for non-cost items)
python3 build_raw_panel.py \
  --root "$ROOT" \
  --years 2024 \
  --surveys HD,IC,EF,E12,SFA,F1A,F2A,F3A,GR,GR200,ADM,OM,CST \
  --output "$PARQ/panel_long_raw_2024.parquet"

python3 panelize_raw.py \
  --input "$PARQ/panel_long_raw_2024.parquet" \
  --output "$OUT/panel_wide_raw_2024.csv" \
  --column-field source_var




ROOT="/Users/markjaysonfarol13/Higher Ed research/IPEDS/Cross sectional Datas"
PARQ="/Users/markjaysonfarol13/Higher Ed research/IPEDS/Parquets"
OUT="/Users/markjaysonfarol13/Higher Ed research/IPEDS/Paneled Datasets/Raw panel"

# Wide only
python3 merge_raw_panels.py \
  --input-dir "$OUT" \
  --pattern "panel_wide_raw_*.csv" \
  --output-wide "$OUT/panel_wide_raw_2004_2024_merged.csv" \
  --component-order "HD,IC,IC_AY,EF,E12,EFIA,E1D,EFFY,SFA,F1A,F2A,F3A,ADM,GR,GR200"


#Running Finance Unification
python3 /Users/markjaysonfarol13/Documents/GitHub/IPEDS_Paneling/unify_finance.py
