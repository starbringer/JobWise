#!/usr/bin/env bash
# ============================================================
# JobWise — Mac Installer
# This file is bundled inside JobWise-Mac.dmg.
# Open the DMG, then double-click this file to install.
# No Git, no Terminal knowledge needed.
# ============================================================

# Keep the terminal window open if an error occurs
trap 'echo ""; echo " An error occurred — see the message above."; read -rp " Press Enter to close..."; exit 1' ERR
set -euo pipefail

# Resolve the DMG mount directory (where this script lives)
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
FILES_DIR="$SCRIPT_DIR/jobwise-files"

clear
echo ""
echo " ========================================="
echo "  JobWise Installer"
echo " ========================================="
echo ""

# ── Step 1: Check Python 3.11+ ───────────────────────────────
need_python=false

find_python() {
    # Prefer explicit python3.11 / python3.12 / python3.13 binaries
    for bin in python3.13 python3.12 python3.11 python3; do
        if command -v "$bin" &>/dev/null; then
            local ver
            ver=$("$bin" -c "import sys; print(f'{sys.version_info.major}.{sys.version_info.minor}')" 2>/dev/null || echo "0.0")
            local maj min
            maj=$(echo "$ver" | cut -d. -f1)
            min=$(echo "$ver" | cut -d. -f2)
            if [ "$maj" -ge 3 ] && [ "$min" -ge 11 ]; then
                echo "$bin"
                return 0
            fi
        fi
    done
    return 1
}

if PYTHON=$(find_python); then
    echo " Found: $($PYTHON --version)"
else
    echo " Python 3.11 or later is required but was not found."
    need_python=true
fi

if [ "$need_python" = true ]; then
    echo ""
    # Try Homebrew first (fast, no UI)
    if command -v brew &>/dev/null; then
        echo " Installing Python via Homebrew (this may take a minute)..."
        brew install python@3.11
        export PATH="$(brew --prefix)/bin:$PATH"
    else
        # Open python.org and wait for the user
        echo " Opening the Python download page in your browser..."
        open "https://www.python.org/downloads/mac-osx/"
        echo ""
        echo " -------------------------------------------------------"
        echo "  Please download and install Python 3.11 (or later)"
        echo "  from the page that just opened."
        echo ""
        echo "  When the Python installer finishes, come back here"
        echo "  and press Enter to continue."
        echo " -------------------------------------------------------"
        read -rp " Press Enter once Python is installed: "
        echo ""
    fi

    if ! PYTHON=$(find_python); then
        echo ""
        echo " Python 3.11+ still not found."
        echo " Please re-open JobWise-Mac.dmg and double-click"
        echo " 'Install JobWise' again after installing Python."
        read -rp " Press Enter to close..."
        exit 1
    fi
    echo " Using: $($PYTHON --version)"
fi
echo ""

# ── Step 2: Choose install location ─────────────────────────
DEFAULT_DIR="$HOME/JobWise"
echo " JobWise will be installed to:"
echo "   $DEFAULT_DIR"
echo ""
read -rp "  Press Enter to accept, or type a different path: " CUSTOM_DIR
INSTALL_DIR="${CUSTOM_DIR:-$DEFAULT_DIR}"
echo ""

# ── Step 3: Copy files ───────────────────────────────────────
echo " Installing files to $INSTALL_DIR..."
mkdir -p "$INSTALL_DIR"
# Use rsync to avoid overwriting user data (profiles/, data/) on re-install
rsync -a --exclude='profiles/' --exclude='data/' --exclude='.env' \
    "$FILES_DIR/" "$INSTALL_DIR/"
# Copy profiles/ and data/ only if they don't exist yet (first install)
[ -d "$FILES_DIR/profiles" ] && [ ! -d "$INSTALL_DIR/profiles" ] && \
    cp -R "$FILES_DIR/profiles" "$INSTALL_DIR/profiles"
[ -d "$FILES_DIR/data" ] && [ ! -d "$INSTALL_DIR/data" ] && \
    cp -R "$FILES_DIR/data" "$INSTALL_DIR/data"

chmod +x "$INSTALL_DIR/start.sh" "$INSTALL_DIR/setup.sh" 2>/dev/null || true
echo " Files installed."
echo ""

# ── Step 4: Run the setup wizard ─────────────────────────────
cd "$INSTALL_DIR"
echo " Starting JobWise Setup Wizard..."
echo ""
"$PYTHON" setup_wizard.py

echo ""
echo " ========================================="
echo "  Installation complete!"
echo ""
echo "  To open JobWise in future, double-click:"
echo "    $INSTALL_DIR/start.sh"
echo ""
echo "  Or run this in Terminal:"
echo "    bash \"$INSTALL_DIR/start.sh\""
echo " ========================================="
echo ""
read -rp " Press Enter to close..."
