#!/usr/bin/env bash
# install-cron.sh — Install cron jobs for the job finder on macOS / Linux.
#
# Reads run_times from config/config.yaml and adds one cron entry per time.
# Re-running this script is safe — existing JobWise entries are replaced.
#
# Usage:
#   bash scheduler/install-cron.sh

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
VENV_PYTHON="$PROJECT_ROOT/venv/bin/python"
RUN_SCRIPT="$SCRIPT_DIR/run_scheduled.py"
LOG_DIR="$PROJECT_ROOT/logs"
CRON_MARKER="# JobWise"

# ── Preflight checks ──────────────────────────────────────────────────────────
if [ ! -f "$VENV_PYTHON" ]; then
    echo "ERROR: Python venv not found at $VENV_PYTHON"
    echo "Run the setup script first (python -m venv .venv && .venv/bin/pip install -r requirements.txt)."
    exit 1
fi

if [ ! -f "$RUN_SCRIPT" ]; then
    echo "ERROR: run_scheduled.py not found at $RUN_SCRIPT"
    exit 1
fi

mkdir -p "$LOG_DIR"

# ── Parse run_times from config.yaml using the venv Python ───────────────────
RUN_TIMES=$("$VENV_PYTHON" -c "
import yaml
from pathlib import Path
with open(Path('${PROJECT_ROOT}') / 'config' / 'config.yaml') as f:
    config = yaml.safe_load(f)
times = config.get('scheduler', {}).get('run_times', ['08:00', '18:00'])
print('\n'.join(times))
")

if [ -z "$RUN_TIMES" ]; then
    echo "ERROR: Could not read run_times from config/config.yaml"
    exit 1
fi

# ── Rebuild crontab: remove old entries, add new ones ────────────────────────
TEMP_CRON=$(mktemp)
trap 'rm -f "$TEMP_CRON"' EXIT

# Preserve existing non-JobWise entries
crontab -l 2>/dev/null | grep -v "$CRON_MARKER" > "$TEMP_CRON" || true

while IFS= read -r time_str; do
    # Strip leading zeros to avoid octal interpretation in cron fields
    HOUR=$(printf '%d' "${time_str%%:*}")
    MIN=$(printf  '%d' "${time_str##*:}")
    echo "$MIN $HOUR * * * \"$VENV_PYTHON\" \"$RUN_SCRIPT\" >> \"$LOG_DIR/cron.log\" 2>&1 $CRON_MARKER"  >> "$TEMP_CRON"
    echo "  Installed: daily at $(printf '%02d:%02d' "$HOUR" "$MIN")"
done <<< "$RUN_TIMES"

crontab "$TEMP_CRON"

echo ""
echo "Done. Cron jobs installed."
echo "  Logs: $LOG_DIR/cron.log  (pipeline detail: $LOG_DIR/scheduler.log)"
echo ""
echo "  NOTE: On macOS, cron does not wake the machine from sleep."
echo "  To run while sleeping, enable 'Power Nap' in System Settings > Battery."
echo "  On Linux, the machine must be awake at run time."
echo ""
echo "  View jobs : crontab -l"
echo "  Uninstall : bash scheduler/uninstall-cron.sh"
