#!/usr/bin/env bash

set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
export IPEDS_ROOT="${IPEDS_ROOT:-$ROOT}"

if [[ -f "$ROOT/.venv/bin/activate" ]]; then
  # shellcheck disable=SC1090
  source "$ROOT/.venv/bin/activate"
fi

CHECKS_DIR="$IPEDS_ROOT/Checks"
DICT_LAKE="$IPEDS_ROOT/Dictionary/dictionary_lake.parquet"
LONG_PANEL="$IPEDS_ROOT/Panels/2004-2024/panel_long_varnum_2004_2024.parquet"
WIDE_RAW="$IPEDS_ROOT/Panels/2004_2024_IPEDS_Raw_Panel_DS.parquet"
WIDE_PRCH="$IPEDS_ROOT/Panels/2004_2024_IPEDS_PRCHclean_Panel_DS.parquet"
WIDE_QC_TMP="$IPEDS_ROOT/Panels/wide_qc_tmp"

mkdir -p \
  "$CHECKS_DIR/release_qc" \
  "$CHECKS_DIR/disc_qc" \
  "$CHECKS_DIR/wide_qc" \
  "$CHECKS_DIR/panel_qc"

check_path() {
  local label="$1"
  local path="$2"
  if [[ -e "$path" ]]; then
    echo "[ok] $label: $path"
  else
    echo "[error] $label not found: $path"
    exit 1
  fi
}

check_path "Raw root" "$IPEDS_ROOT/Raw_Cross_Section_Data"
check_path "Dictionary lake" "$DICT_LAKE"
check_path "Long panel" "$LONG_PANEL"
check_path "Wide raw panel" "$WIDE_RAW"
check_path "Wide PRCH panel" "$WIDE_PRCH"

echo ""
echo "[info] Running release QC (scans raw year directories + manifests)"
python3 "$ROOT/Scripts/03_harmonize.py" \
  --root "$IPEDS_ROOT/Raw_Cross_Section_Data" \
  --lake "$DICT_LAKE" \
  --years "2004:2024" \
  --output "$CHECKS_DIR/qc_only_dummy.parquet" \
  --qc-only \
  --release-allow "revised,final" \
  --release-strict \
  --release-qc-dir "$CHECKS_DIR/release_qc"

echo ""
echo "[info] Running disc_qc + wide_qc (scans stitched long panel)"
python3 "$ROOT/Scripts/04_build_wide_panel.py" \
  --input "$LONG_PANEL" \
  --out_dir "$WIDE_QC_TMP" \
  --years "2004:2024" \
  --dictionary "$DICT_LAKE" \
  --collapse-disc \
  --drop-disc-components \
  --disc-qc-dir "$CHECKS_DIR/disc_qc" \
  --qc-dir "$CHECKS_DIR/wide_qc"

echo ""
echo "[info] Running panel_qc (raw vs PRCH clean wide panels)"
python3 "$ROOT/Scripts/QA_QC/01_panel_qa.py" \
  --raw "$WIDE_RAW" \
  --clean "$WIDE_PRCH" \
  --out-dir "$CHECKS_DIR/panel_qc" \
  --prch-qc-dir "$CHECKS_DIR/prch_qc"

echo ""
echo "QC outputs written to:"
echo "  $CHECKS_DIR/release_qc"
echo "  $CHECKS_DIR/disc_qc"
echo "  $CHECKS_DIR/wide_qc"
echo "  $CHECKS_DIR/panel_qc"
echo ""
echo "Note: temp wide output is in $WIDE_QC_TMP (delete if not needed)."
