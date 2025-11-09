#!/usr/bin/env bash
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
VENV_PATH="${REPO_ROOT}/.venv"

if [[ ! -d "${VENV_PATH}" ]]; then
  python3 -m venv "${VENV_PATH}"
fi

# shellcheck disable=SC1090
source "${VENV_PATH}/bin/activate"

python -m pip install -U pip
pip install -r "${REPO_ROOT}/requirements.txt"

echo "Environment bootstrapped. If imports still fail, run: source .venv/bin/activate && pip install -r requirements.txt"
