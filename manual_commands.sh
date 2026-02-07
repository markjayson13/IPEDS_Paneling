#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
export IPEDS_ROOT="${IPEDS_ROOT:-$ROOT}"

usage() {
  cat <<'EOF'
Run full IPEDS panel build (raw -> PRCH clean -> clean) and print custom-panel next step.

Usage:
  bash manual_commands.sh

Environment:
  IPEDS_ROOT  Data root (default: repo root)

Required inputs under IPEDS_ROOT:
  Raw_Cross_Section_Data/
  Dictionary/dictionary_lake.parquet

Outputs:
  Panels/2004_2024_IPEDS_Raw_Panel_DS.parquet
  Panels/2004_2024_IPEDS_PRCHclean_Panel_DS.parquet
  Panels/2004_2024_IPEDS_clean_Panel_DS.parquet
EOF
}

if [[ "${1:-}" == "-h" || "${1:-}" == "--help" ]]; then
  usage
  exit 0
fi

if [[ -f "$ROOT/.venv/bin/activate" ]]; then
  # shellcheck disable=SC1090
  source "$ROOT/.venv/bin/activate"
fi

RAW_ROOT="$IPEDS_ROOT/Raw_Cross_Section_Data"
LAKE="$IPEDS_ROOT/Dictionary/dictionary_lake.parquet"
if [[ ! -d "$RAW_ROOT" ]]; then
  echo "Missing raw data root: $RAW_ROOT" >&2
  exit 1
fi
if [[ ! -f "$LAKE" ]]; then
  echo "Missing dictionary lake: $LAKE" >&2
  echo "Build it with:" >&2
  echo "  python3 Scripts/02_dictionary_ingest.py --root \"$RAW_ROOT\" --output \"$LAKE\" --output-csv \"$IPEDS_ROOT/Dictionary/dictionary_lake.csv\" --codes-output \"$IPEDS_ROOT/Dictionary/dictionary_codes.parquet\" --codes-output-csv \"$IPEDS_ROOT/Dictionary/dictionary_codes.csv\"" >&2
  exit 1
fi

mkdir -p \
  "$IPEDS_ROOT/Cross_sections" \
  "$IPEDS_ROOT/Panels/2004-2024" \
  "$IPEDS_ROOT/Checks"

python3 "$ROOT/Scripts/00_run_all.py" \
  --root "$RAW_ROOT" \
  --lake "$LAKE" \
  --years "2004:2024" \
  --cross-sections-dir "$IPEDS_ROOT/Cross_sections" \
  --parts-dir-base "$IPEDS_ROOT/Cross_sections" \
  --stitch-out "$IPEDS_ROOT/Panels/2004-2024/panel_long_varnum_2004_2024.parquet" \
  --wide-out-dir "$IPEDS_ROOT/Panels/wide_2004_2024" \
  --wide-write-single "$IPEDS_ROOT/Panels/2004_2024_IPEDS_Raw_Panel_DS.parquet" \
  --stitch-wide \
  --stitch-wide-out "$IPEDS_ROOT/Panels/2004_2024_IPEDS_Raw_Panel_DS.parquet" \
  --run-cleaning \
  --prch-clean-out "$IPEDS_ROOT/Panels/2004_2024_IPEDS_PRCHclean_Panel_DS.parquet" \
  --clean-out "$IPEDS_ROOT/Panels/2004_2024_IPEDS_clean_Panel_DS.parquet" \
  --prch-qc-dir "$IPEDS_ROOT/Checks/prch_qc" \
  --disc-qc-dir "$IPEDS_ROOT/Checks/disc_qc" \
  --qc-dir "$IPEDS_ROOT/Checks/wide_qc" \
  --release-allow "revised,final" \
  --release-qc-dir "$IPEDS_ROOT/Checks/release_qc" \
  --log-dir "$IPEDS_ROOT/Checks/logs" \
  --no-final-dedupe \
  --drop-imputation-flags

echo ""
echo "Next step: build a custom panel (UNITID + year are always kept)"
echo "python3 $ROOT/Scripts/06_build_custom_panel.py --input \"$IPEDS_ROOT/Panels/2004_2024_IPEDS_clean_Panel_DS.parquet\" --vars \"INSTNM,SECTOR,TUITION1,PELL_RECP\" --output \"$IPEDS_ROOT/Panels/custom_panel.parquet\""
