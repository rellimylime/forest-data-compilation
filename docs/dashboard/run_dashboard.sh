#!/usr/bin/env bash
set -euo pipefail

# Reproducible launcher for the Forest Data Explorer on the UCSB host.
# The environment lives outside the repository and is safe to rebuild.

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
dashboard_env="${DASHBOARD_ENV_DIR:-${TMPDIR:-/tmp}/forest-data-explorer-venv}"
dashboard_port="${DASHBOARD_PORT:-8501}"
dashboard_host="${DASHBOARD_HOST:-$(hostname -s)}"

if [[ -n "${UV_BIN:-}" && -x "$UV_BIN" ]]; then
  uv_bin="$UV_BIN"
elif command -v uv >/dev/null 2>&1; then
  uv_bin="$(command -v uv)"
elif [[ -x /home/ermiller/.cache/R/reticulate/uv/bin/uv ]]; then
  uv_bin="/home/ermiller/.cache/R/reticulate/uv/bin/uv"
else
  echo "uv was not found. Set UV_BIN to the uv executable." >&2
  exit 1
fi

if [[ ! -x "$dashboard_env/bin/python" ]]; then
  "$uv_bin" venv --python 3.12 "$dashboard_env"
fi

# This is fast after the first run and repairs stale or incompatible packages.
"$uv_bin" pip install \
  --python "$dashboard_env/bin/python" \
  --requirements "$script_dir/requirements.txt"

echo "Open the dashboard at:"
echo "https://hpc.grit.ucsb.edu/rnode/$dashboard_host/$dashboard_port/"

cd "$script_dir"
exec "$dashboard_env/bin/python" -m streamlit run app.py \
  --server.port "$dashboard_port" \
  --server.headless true
