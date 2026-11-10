#!/bin/bash
set -euo pipefail
repo_dir="$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)"
python_path="$repo_dir/.venv/bin/python"
if [ ! -x "$python_path" ]; then
  echo "Run bash Setup-Mac.sh first." >&2
  exit 2
fi
exec "$python_path" -u -B "$repo_dir/tools/returns.py" pack "$@"
