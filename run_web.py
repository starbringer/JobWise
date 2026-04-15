"""
web.py — Start the Flask web app only (no scheduler).

Usage:
    python web.py
    python web.py --port 8080
"""

import logging
import sys
from pathlib import Path

import yaml

PROJECT_ROOT = Path(__file__).parent
sys.path.insert(0, str(PROJECT_ROOT))

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    stream=sys.stdout,
)

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--port", type=int, default=None)
    parser.add_argument("--host", default=None)
    args = parser.parse_args()

    cfg_path = PROJECT_ROOT / "config" / "config.yaml"
    if cfg_path.exists():
        with open(cfg_path, encoding="utf-8") as f:
            config = yaml.safe_load(f) or {}
    else:
        config = {}

    web_cfg = config.get("web", {})
    host = args.host or web_cfg.get("host", "0.0.0.0")
    port = args.port or web_cfg.get("port", 6868)

    from web.app import app
    try:
        from waitress import serve
        import socket, logging as _logging
        _logging.getLogger('waitress').setLevel(logging.WARNING)
        print(f" * Running on http://127.0.0.1:{port}")
        try:
            local_ip = socket.gethostbyname(socket.gethostname())
            if local_ip != "127.0.0.1":
                print(f" * Running on http://{local_ip}:{port}")
        except Exception:
            pass
        serve(app, host=host, port=port, threads=4)
    except ImportError:
        # Waitress not installed — fall back to Flask dev server
        app.run(host=host, port=port, debug=False, use_reloader=False)
