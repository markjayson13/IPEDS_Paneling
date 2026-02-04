# IPEDS_Paneling — Audit Pack (End-to-End Reproducibility & Validation)

**Project:** IPEDS_Paneling  
**Repository:** https://github.com/markjayson13/IPEDS_Paneling  
**Audit Pack Version:** {AUDIT_PACK_VERSION}  
**Code Version:** {GIT_TAG_OR_COMMIT_HASH}  
**Build Date (UTC):** {BUILD_DATETIME_UTC}  
**Built By:** {BUILT_BY}

## What this is
This Audit Pack is a reviewer-facing bundle designed to verify that the pipeline is:
1) **Reproducible** (same inputs + same commit → same outputs)  
2) **Auditable** (transformations produce QC artifacts you can inspect)  
3) **Safe for panel use** (explicit defenses for schema drift, universe instability, and parent–child reporting)

Raw IPEDS files are not redistributed. Instead, this pack includes **input manifests + hashes**, **output hashes**, and **QC artifacts**.

## Where to start
- **Reproduction metadata:** `00_run/run_metadata.json`, `00_run/run_command.txt`
- **Input manifest + hashes:** `01_inputs/input_manifest.csv`, `01_inputs/input_hashes.csv`
- **Output hashes:** `00_run/output_hashes.csv`
- **QC index:** `06_qc/checks_index.md`

## Key checks (what to inspect)

### Dictionary / mapping (schema drift defense)
- `02_dictionary/dictionary_coverage_by_year_component.csv`
- `02_dictionary/mapping_collisions.csv` and `02_dictionary/mapping_collisions_varname.csv`
- `02_dictionary/drift_summary.csv`

### Long-panel integrity (source-of-truth layer)
- `03_long_panel/long_key_integrity.csv`  
  *Duplicate keys should be 0 for (UNITID, year, varname).*
- `03_long_panel/long_schema.json`

### Wide-panel integrity (analysis convenience layer)
- `04_wide_panel/wide_integrity.csv`  
  *Duplicate keys should be 0 for (UNITID, year).*
- `04_wide_panel/wide_schema_diff.csv` and `04_wide_panel/wide_schema_diff_columns.csv`

### Parent–child reporting (PRCH)
- `05_prch/prch_rules.md`
- `05_prch/*.csv` (copied PRCH QC summaries, if present)

### Other QC outputs (release filtering, discrete collapse, wide build QC)
- `06_qc/release_qc/*`
- `06_qc/disc_qc/*`
- `06_qc/wide_qc/*`
- `06_qc/panel_qc/*`
- `06_qc/prch_qc/*` (duplicated here for reviewer convenience)

## Performance & size
- `08_performance/output_sizes.csv`

## Notes
- If any key integrity check reports duplicates or missing keys, treat that as a **blocking issue** unless explicitly justified and logged.
- Large discrete-collapse conflict files are expected in raw form; reviewers should consult summary rows and the conflict handling policy.
