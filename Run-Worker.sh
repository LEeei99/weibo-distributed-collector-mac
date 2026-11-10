#!/bin/bash
set -euo pipefail
repo_dir="$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)"
python_path="$repo_dir/.venv/bin/python"
if [ ! -x "$python_path" ]; then
  echo "Run bash Setup-Mac.sh first." >&2
  exit 2
fi
if [ "$#" -lt 1 ]; then
  echo "Usage: bash Run-Worker.sh prepare|run [worker.py options]" >&2
  exit 2
fi
mode="$1"
shift
case "$mode" in
  prepare)
    exec "$python_path" -u -B "$repo_dir/tools/worker.py" prepare "$@"
    ;;
  run)
    full_run=false
    for argument in "$@"; do
      if [ "$argument" = --until-finished ]; then full_run=true; fi
    done
    if [ "$full_run" = true ]; then
      exec "$python_path" -u -B "$repo_dir/tools/worker.py" run "$@"
    fi
    exec "$python_path" -u -B "$repo_dir/tools/worker.py" run \
      --max-posts 2 --max-batches 1 --request-limit 2 "$@"
    ;;
  -h|--help)
    exec "$python_path" -u -B "$repo_dir/tools/worker.py" --help
    ;;
  *)
    echo "First argument must be prepare or run." >&2
    exit 2
    ;;
esac
