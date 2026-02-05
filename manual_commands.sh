#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
export IPEDS_ROOT="${IPEDS_ROOT:-$ROOT}"

usage() {
  cat <<'EOF'
Manual Commands (one-line friendly)

Usage:
  bash manual_commands.sh

What it does:
  Builds the research-ready wide panel:
  Panels/2004_2024_IPEDS_clean_Panel_DS.parquet

Notes:
  - IPEDS_ROOT defaults to the repo root (this script's folder).
  - If .venv exists, it will be activated automatically.
EOF
}

MODE="${1:-clean}"
if [[ "${MODE}" == "-h" || "${MODE}" == "--help" ]]; then
  usage
  exit 0
fi

if [[ -f "$ROOT/.venv/bin/activate" ]]; then
  # shellcheck disable=SC1090
  source "$ROOT/.venv/bin/activate"
fi

mkdir -p \
  "$IPEDS_ROOT/Panels" \
  "$IPEDS_ROOT/Checks" \
  "$IPEDS_ROOT/Cross_sections"

if [[ "$MODE" != "clean" ]]; then
  echo "Unknown mode: $MODE" >&2
  usage
  exit 1
fi

if [[ ! -f "$IPEDS_ROOT/Dictionary/dictionary_lake.parquet" ]]; then
  echo "Missing dictionary lake: $IPEDS_ROOT/Dictionary/dictionary_lake.parquet" >&2
  echo "Run:" >&2
  echo "  python3 \"$ROOT/Dictionary/01_ingest_dictionaries.py\" \\" >&2
  echo "    --root \"$IPEDS_ROOT/Raw_Cross_Section_Data\" \\" >&2
  echo "    --min-year 2004 \\" >&2
  echo "    --output \"$IPEDS_ROOT/Dictionary/dictionary_lake.parquet\" \\" >&2
  echo "    --codes-output \"$IPEDS_ROOT/Dictionary/dictionary_codes.parquet\" \\" >&2
  echo "    --codes-output-csv \"$IPEDS_ROOT/Dictionary/dictionary_codes.csv\"" >&2
  exit 1
fi
python3 "$ROOT/08_run_pipeline.py" \
  --release-allow "revised,final" \
  --release-qc-dir "$IPEDS_ROOT/Checks/release_qc" \
  --stitch-wide \
  --stitch-wide-out "$IPEDS_ROOT/Panels/2004_2024_IPEDS_Raw_Panel_DS.parquet" \
  --run-cleaning \
  --prch-clean-out "$IPEDS_ROOT/Panels/2004_2024_IPEDS_PRCHclean_Panel_DS.parquet" \
  --clean-out "$IPEDS_ROOT/Panels/2004_2024_IPEDS_clean_Panel_DS.parquet" \
  --prch-qc-dir "$IPEDS_ROOT/Checks/prch_qc" \
  --drop-imputation-flags

echo ""
echo "Next step: build a custom panel (keeps UNITID + year)"
echo "  python3 \"$ROOT/Panels/06_build_custom_panel.py\" \\"
echo "    --input \"$IPEDS_ROOT/Panels/2004_2024_IPEDS_clean_Panel_DS.parquet\" \\"
echo "    --vars \"INSTNM,SECTOR,TUITION1,PELL_RECP\" \\"
echo "    --output \"$IPEDS_ROOT/Panels/custom_panel.parquet\""


/Users/markjaysonfarol13/IPEDS_Paneling/IPEDS_Paneling paper/audit_pack_out/03_long_panel/panel_long_2004_2024.parquet

python3 -c "import pyarrow.parquet as pq; print(pq.ParquetFile('/Users/markjaysonfarol13/IPEDS_Paneling/IPEDS_Paneling paper/audit_pack_out/03_long_panel/panel_long_2004_2024.parquet').schema.names)"
