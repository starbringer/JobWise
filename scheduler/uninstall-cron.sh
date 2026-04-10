#!/usr/bin/env bash
# uninstall-cron.sh — Remove all JobWise cron entries.
#
# Usage:
#   bash scheduler/uninstall-cron.sh

set -euo pipefail

CRON_MARKER="# JobWise"

TEMP_CRON=$(mktemp)
trap 'rm -f "$TEMP_CRON"' EXIT

BEFORE=$(crontab -l 2>/dev/null | grep -c "$CRON_MARKER" || true)

crontab -l 2>/dev/null | grep -v "$CRON_MARKER" > "$TEMP_CRON" || true
crontab "$TEMP_CRON"

if [ "$BEFORE" -gt 0 ]; then
    echo "Removed $BEFORE JobWise cron job(s)."
else
    echo "No JobWise cron jobs found."
fi

echo "Done. Verify with: crontab -l"
