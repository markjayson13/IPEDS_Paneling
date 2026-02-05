#!/usr/bin/env bash

set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
export IPEDS_ROOT="${IPEDS_ROOT:-$ROOT}"

if [[ -f "$ROOT/.venv/bin/activate" ]]; then
  # shellcheck disable=SC1090
  source "$ROOT/.venv/bin/activate"
fi

mkdir -p \
  "$IPEDS_ROOT/Checks/release_qc" \
  "$IPEDS_ROOT/Checks/disc_qc" \
  "$IPEDS_ROOT/Checks/wide_qc" \
  "$IPEDS_ROOT/Checks/panel_qc"

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
check_path "Dictionary lake" "$IPEDS_ROOT/Dictionary/dictionary_lake.parquet"
check_path "Long panel" "$IPEDS_ROOT/Panels/2004-2024/panel_long_varnum_2004_2024.parquet"
check_path "Wide raw panel" "$IPEDS_ROOT/Panels/2004_2024_IPEDS_Raw_Panel_DS.parquet"
check_path "Wide PRCH panel" "$IPEDS_ROOT/Panels/2004_2024_IPEDS_PRCHclean_Panel_DS.parquet"

echo ""
echo "[info] Running release QC (scans raw year directories + manifests)"
# 1) release_qc only (no outputs written)
python3 "$ROOT/03_harmonize.py" \
  --root "$IPEDS_ROOT/Raw_Cross_Section_Data" \
  --lake "$IPEDS_ROOT/Dictionary/dictionary_lake.parquet" \
  --years "2004:2024" \
  --output "$IPEDS_ROOT/Checks/qc_only_dummy.parquet" \
  --qc-only \
  --release-allow "revised,final" \
  --release-strict \
  --release-qc-dir "$IPEDS_ROOT/Checks/release_qc"

echo ""
echo "[info] Running disc_qc + wide_qc (scans stitched long panel)"
# 2) disc_qc + wide_qc from stitched long panel (writes a temp wide output)
python3 "$ROOT/Panels/04_build_wide_panel.py" \
  --input "$IPEDS_ROOT/Panels/2004-2024/panel_long_varnum_2004_2024.parquet" \
  --out_dir "$IPEDS_ROOT/Panels/wide_qc_tmp" \
  --years "2004:2024" \
  --dictionary "$IPEDS_ROOT/Dictionary/dictionary_lake.parquet" \
  --collapse-disc \
  --drop-disc-components \
  --disc-qc-dir "$IPEDS_ROOT/Checks/disc_qc" \
  --qc-dir "$IPEDS_ROOT/Checks/wide_qc"

echo ""
echo "[info] Running panel_qc (raw vs PRCH clean wide panels)"
# 3) panel_qc (raw vs PRCH clean)
python3 "$ROOT/07_panel_QA.py" \
  --raw "$IPEDS_ROOT/Panels/2004_2024_IPEDS_Raw_Panel_DS.parquet" \
  --clean "$IPEDS_ROOT/Panels/2004_2024_IPEDS_PRCHclean_Panel_DS.parquet" \
  --out-dir "$IPEDS_ROOT/Checks/panel_qc" \
  --prch-qc-dir "$IPEDS_ROOT/Checks/prch_qc"

echo ""
echo "QC outputs written to:"
echo "  $IPEDS_ROOT/Checks/release_qc"
echo "  $IPEDS_ROOT/Checks/disc_qc"
echo "  $IPEDS_ROOT/Checks/wide_qc"
echo "  $IPEDS_ROOT/Checks/panel_qc"
echo ""
echo "Note: temp wide output is in $IPEDS_ROOT/Panels/wide_qc_tmp (delete if not needed)."
