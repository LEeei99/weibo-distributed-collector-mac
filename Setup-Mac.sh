#!/bin/bash
set -euo pipefail
repo_dir="$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)"
if [ "$#" -gt 1 ]; then
  echo "Usage: bash Setup-Mac.sh [path-to-python3.13]" >&2
  exit 2
fi
if [ "$(uname -s)" != Darwin ]; then
  echo "This setup script is for macOS." >&2
  exit 2
fi
mac_major="$(sw_vers -productVersion | cut -d. -f1)"
if [ "$mac_major" -lt 14 ]; then
  echo "macOS 14 or newer is required." >&2
  exit 2
fi
python_cmd="python3.13"
if [ "$#" -eq 1 ]; then python_cmd="$1"; fi
if ! command -v "$python_cmd" >/dev/null 2>&1; then
  echo "Install native Python 3.13, then rerun this script." >&2
  exit 2
fi
"$python_cmd" -c 'import platform, sys; assert sys.version_info[:2] == (3, 13), "Use Python 3.13"; assert platform.machine() == "arm64", "Use native Apple Silicon Python, outside Rosetta"'
if [ ! -e "$repo_dir/.venv" ]; then
  "$python_cmd" -m venv "$repo_dir/.venv"
fi
python_path="$repo_dir/.venv/bin/python"
if [ ! -x "$python_path" ]; then
  echo "The existing .venv is not usable. Create a fresh local environment; do not copy one from another computer." >&2
  exit 2
fi
"$python_path" -c 'import platform, sys; assert sys.version_info[:2] == (3, 13), "Existing .venv needs Python 3.13"; assert platform.machine() == "arm64", "Existing .venv is not native Apple Silicon"'
"$python_path" -m pip install -r "$repo_dir/requirements.txt"
"$python_path" - <<'PY'
from playwright.sync_api import sync_playwright
with sync_playwright() as playwright:
    browser = playwright.chromium.launch(channel="msedge", headless=True)
    try:
        page = browser.new_page()
        page.set_content("<title>Offline Mac preflight</title>")
        assert page.title() == "Offline Mac preflight"
    finally:
        browser.close()
print("Python and Edge local preflight passed. No Weibo collection was started.")
PY
echo "Ready for offline assignment validation. See WORKER.md."
