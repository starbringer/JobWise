#!/usr/bin/env bash
# ============================================================
# JobWise — Start the web app
# Usage: bash start.sh
# Keep this terminal open while you are using the app.
# Press Ctrl+C to stop the server.
# ============================================================

set -e
cd "$(dirname "$0")"

if [ ! -f "venv/bin/python" ]; then
    echo ""
    echo " JobWise has not been set up yet."
    echo " Please run:  bash setup.sh"
    echo ""
    exit 1
fi

echo ""
echo " Starting JobWise..."
echo " Your browser will open automatically in a few seconds."
echo ""
echo " Keep this terminal open while using the app."
echo " Press Ctrl+C to stop the server."
echo ""

# Open browser after a short delay (background, non-blocking)
(sleep 2 && python3 -c "import webbrowser; webbrowser.open('http://localhost:6868')") &

source venv/bin/activate
python run_web.py
