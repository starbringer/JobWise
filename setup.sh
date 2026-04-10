#!/usr/bin/env bash
# ============================================================
# JobWise Setup — One-Click Installer
# Usage: bash setup.sh
# ============================================================

set -e
cd "$(dirname "$0")"

# Check Python 3 is available
if ! command -v python3 &>/dev/null; then
    echo ""
    echo " Python 3 was not found on this computer."
    echo ""
    echo " Install Python 3.11 or later:"
    echo "   macOS:   brew install python@3.13"
    echo "            (or download from https://www.python.org/downloads/)"
    echo "   Ubuntu:  sudo apt install python3.11 python3.11-venv"
    echo "   Other:   https://www.python.org/downloads/"
    echo ""
    exit 1
fi

# Check version is 3.11+
PYVER=$(python3 -c "import sys; print(f'{sys.version_info.major}.{sys.version_info.minor}')" 2>/dev/null)
PYMAJ=$(echo "$PYVER" | cut -d. -f1)
PYMIN=$(echo "$PYVER" | cut -d. -f2)

if [ "$PYMAJ" -lt 3 ] || { [ "$PYMAJ" -eq 3 ] && [ "$PYMIN" -lt 11 ]; }; then
    echo ""
    echo " Python 3.11 or later is required. You have Python $PYVER."
    echo " Download the latest version from: https://www.python.org/downloads/"
    echo ""
    exit 1
fi

python3 setup_wizard.py
