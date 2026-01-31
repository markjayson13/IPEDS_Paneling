# 5. Evaluation and Validation

This section reports validation metrics produced by the pipeline’s built‑in QC artifacts.  
All numbers below are reproducible from the Parquet outputs and QC directories.

## 5.1 Release‑stage validation

**Claim.** Restricting inputs to revised/final releases reduces non‑substantive breaks caused by provisional revisions.  
**Evidence.** The recommended run enforces revised/final when available and writes release QC outputs.

**Report from QC (`Checks/release_qc/`):**
- Number of component files included per year  
- Number of files excluded due to disallowed release stage  
- Years with missing required components under strict mode

## 5.2 Mapping coverage and schema‑drift mitigation

**Claim.** Dictionary‑first mapping yields stable variable identities across years.  
**Evidence.** The dictionary lake defines canonical `(varnumber, varname)`, and long‑panel rows retain `source_file` provenance.

**Report from dictionary + long panel:**
- Total dictionary rows (variable‑year definitions)  
- Unique canonical variables (unique `varname`)  
- Percent of raw columns mapped to a canonical identity (per year)  
- Count of unmapped headers (expected to be near zero; explain exceptions)

## 5.3 Long‑panel integrity checks

**Claim.** The long panel enforces a single grain, reducing silent join errors.  
**Evidence.** One row per `(UNITID, year, varname)`.

**Report:**
- Total rows by year in the long panel  
- Duplicate key rate on `(UNITID, year, varname)`  
- Share of variables with at least one observation in each year

## 5.4 Discrete category collapse conflict rate

**Claim.** Collapsing mutually exclusive indicator families is safe when conflicts are logged and rare.  
**Evidence.** Conflicts are written to `Checks/disc_qc/`.

**Report:**
- Number of collapsed families  
- Total conflicts detected (by family and year)  
- Conflict rate = conflicts / institution‑years in universe

## 5.5 Parent/child validation (PRCH effect test)

**Claim.** PRCH cleaning prevents systematic distortion in component merges without deleting child institutions.  
**Evidence.** Child flags are identified and component data are nulled for child rows only.

**Report from `Checks/prch_qc/`:**
- Count of child institution‑years identified  
- Count of fields nulled per component family  
- Distributional sanity check: extreme outliers in finance‑per‑student before vs after PRCH cleaning

## 5.6 Reproducibility checks

- Fixed release selection rules (revised/final)  
- Deterministic canonicalization (uppercase varnames, standardized varnumbers)  
- Machine‑readable outputs (Parquet) with stable schemas

---

### Table 3: Validation metrics (template)

Use the template in `Artifacts/table3_validation_metrics_template.csv` and fill it with metrics produced by the script below.

### Reproducible metrics script

Run:

```bash
python3 09_paper_metrics.py \
  --dictionary "/Users/markjaysonfarol13/IPEDS_Paneling/Dictionary/dictionary_lake.parquet" \
  --long-panel "/Users/markjaysonfarol13/IPEDS_Paneling/Panels/2004-2024/panel_long_varnum_2004_2024.parquet" \
  --raw-root "/Users/markjaysonfarol13/IPEDS_Paneling/Raw_Cross_Section_Data" \
  --release-qc-dir "/Users/markjaysonfarol13/IPEDS_Paneling/Checks/release_qc" \
  --disc-qc-dir "/Users/markjaysonfarol13/IPEDS_Paneling/Checks/disc_qc" \
  --prch-qc-summary "/Users/markjaysonfarol13/IPEDS_Paneling/Checks/prch_qc/prch_clean_summary.csv" \
  --wide-panel "/Users/markjaysonfarol13/IPEDS_Paneling/Panels/2004_2024_IPEDS_Raw_Panel_DS.parquet" \
  --years "2004:2024" \
  --out-dir "/Users/markjaysonfarol13/IPEDS_Paneling/Checks/paper_metrics"
```

Optional (slower): add `--scan-raw` to compute raw‑header mapping coverage.
