diff --git a/scripts/bootstrap_env.sh b/scripts/bootstrap_env.sh
new file mode 100755
index 0000000000000000000000000000000000000000..54b28b10d10de558faae7b718fab54c45afa0b4d
--- /dev/null
+++ b/scripts/bootstrap_env.sh
@@ -0,0 +1,17 @@
+#!/usr/bin/env bash
+set -euo pipefail
+
+REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
+VENV_PATH="${REPO_ROOT}/.venv"
+
+if [[ ! -d "${VENV_PATH}" ]]; then
+  python3 -m venv "${VENV_PATH}"
+fi
+
+# shellcheck disable=SC1090
+source "${VENV_PATH}/bin/activate"
+
+python -m pip install -U pip
+pip install -r "${REPO_ROOT}/requirements.txt"
+
+echo "Environment bootstrapped. If imports still fail, run: source .venv/bin/activate && pip install -r requirements.txt"
