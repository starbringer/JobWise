"""
run_scheduled.py — One-shot pipeline runner for Windows Task Scheduler.

Reads the configured profiles and run_times from config/config.yaml, runs the
pipeline for every profile, then exits. Windows Task Scheduler owns the schedule
and wake-from-sleep logic (WakeToRun); this script just does the work.

Usage (called by the scheduled task, not normally run by hand):
    pythonw.exe run_scheduled.py
"""

import logging
import logging.handlers
import subprocess
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

# ── Logging ────────────────────────────────────────────────────────────────────
LOG_DIR = PROJECT_ROOT / "logs"
LOG_DIR.mkdir(exist_ok=True)

_fmt = logging.Formatter("%(asctime)s [%(levelname)s] %(name)s: %(message)s")
_root = logging.getLogger()
_root.setLevel(logging.INFO)

_file_handler = logging.handlers.TimedRotatingFileHandler(
    LOG_DIR / "scheduler.log",
    when="midnight",
    backupCount=7,
    encoding="utf-8",
)
_file_handler.setFormatter(_fmt)
_root.addHandler(_file_handler)

if sys.stdout is not None:
    _console = logging.StreamHandler(sys.stdout)
    _console.setFormatter(_fmt)
    _root.addHandler(_console)

logger = logging.getLogger("run_scheduled")


def load_config() -> dict:
    import yaml
    with open(PROJECT_ROOT / "config" / "config.yaml", encoding="utf-8") as f:
        return yaml.safe_load(f)


def main() -> None:
    config = load_config()

    profiles_dir = PROJECT_ROOT / config.get("profiles_dir", "profiles")
    sched_cfg = config.get("scheduler", {})
    profile_names = sched_cfg.get("profiles") or sorted({
        p.stem
        for ext in (".txt", ".md", ".pdf", ".docx")
        for p in profiles_dir.glob(f"*{ext}")
    })

    if not profile_names:
        logger.error("No profiles found. Add a .txt, .md, .pdf, or .docx file to the profiles/ directory.")
        sys.exit(1)

    logger.info(f"Starting scheduled run for profiles: {', '.join(profile_names)}")

    for profile_name in profile_names:
        logger.info(f"Running pipeline for profile '{profile_name}'...")
        try:
            proc = subprocess.Popen(
                [sys.executable, "src/pipeline.py", "--profile", profile_name, "--triggered-by", "scheduler"],
                cwd=str(PROJECT_ROOT),
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
                encoding="utf-8",
                errors="replace",
            )

            # Stream each line from the pipeline into the scheduler log
            assert proc.stdout is not None
            for line in proc.stdout:
                line = line.rstrip("\n")
                if not line:
                    continue
                # Pipeline lines are already formatted as "timestamp [LEVEL] name: msg".
                # Re-log them at INFO level to preserve them verbatim in the scheduler log.
                logger.info(f"[pipeline/{profile_name}] {line}")

            try:
                proc.wait(timeout=900)
            except subprocess.TimeoutExpired:
                proc.kill()
                logger.error(f"Pipeline for '{profile_name}' timed out after 15 minutes.")
                continue

            if proc.returncode == 0:
                logger.info(f"Pipeline for '{profile_name}' completed successfully (exit 0).")
            else:
                logger.error(f"Pipeline for '{profile_name}' exited with code {proc.returncode}.")
        except Exception as e:
            logger.error(f"Pipeline for '{profile_name}' failed: {e}")

    logger.info("Scheduled run complete.")


if __name__ == "__main__":
    main()
