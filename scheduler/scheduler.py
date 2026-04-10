"""
scheduler.py — Runs pipeline jobs on a schedule. No web app.

Usage:
    python scheduler.py

Configure run times and profiles in config/config.yaml:
    scheduler:
      enabled: true
      run_times: ["08:00", "18:00"]
      profiles: ["john"]   # empty = all profiles in profiles/

Run the web app separately with:
    python web.py
"""

import logging
import logging.handlers
import subprocess
import sys
from pathlib import Path

import yaml
from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.triggers.cron import CronTrigger

PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

# ── Logging setup ──────────────────────────────────────────────────────────────
# Always write to a rotating log file; also write to stdout when it's available
# (interactive runs). pythonw.exe has no console so stdout may be None.

LOG_DIR = PROJECT_ROOT / "logs"
LOG_DIR.mkdir(exist_ok=True)

_fmt = logging.Formatter("%(asctime)s [%(levelname)s] %(name)s: %(message)s")
_root = logging.getLogger()
_root.setLevel(logging.INFO)

_file_handler = logging.handlers.TimedRotatingFileHandler(
    LOG_DIR / "scheduler.log",
    when="midnight",
    backupCount=7,  # keep last 7 days
    encoding="utf-8",
)
_file_handler.setFormatter(_fmt)
_root.addHandler(_file_handler)

if sys.stdout is not None:
    _console = logging.StreamHandler(sys.stdout)
    _console.setFormatter(_fmt)
    _root.addHandler(_console)

logger = logging.getLogger("scheduler")


def load_config() -> dict:
    with open(PROJECT_ROOT / "config" / "config.yaml", encoding="utf-8") as f:
        return yaml.safe_load(f)


def run_pipeline_job(profile_name: str) -> None:
    """Invoke the pipeline subprocess for a profile, streaming its output into this log."""
    logger.info(f"[scheduler] Running pipeline for profile '{profile_name}'...")
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
            logger.error(f"[scheduler] Pipeline for '{profile_name}' timed out after 15 minutes.")
            return

        if proc.returncode == 0:
            logger.info(f"[scheduler] Pipeline for '{profile_name}' completed successfully (exit 0).")
        else:
            logger.error(f"[scheduler] Pipeline for '{profile_name}' exited with code {proc.returncode}.")
    except Exception as e:
        logger.error(f"[scheduler] Pipeline for '{profile_name}' failed: {e}")


def get_all_profile_names(profiles_dir: Path) -> list[str]:
    names = {
        p.stem
        for ext in (".txt", ".md", ".pdf", ".docx")
        for p in profiles_dir.glob(f"*{ext}")
    }
    return sorted(names)


def main():
    config = load_config()
    profiles_dir = PROJECT_ROOT / config.get("profiles_dir", "profiles")
    profiles_dir.mkdir(parents=True, exist_ok=True)

    sched_cfg = config.get("scheduler", {})
    if not sched_cfg.get("enabled", False):
        logger.error("Scheduler is disabled. Set scheduler.enabled=true in config.yaml to use it.")
        sys.exit(1)

    run_times = sched_cfg.get("run_times", ["08:00", "18:00"])
    profile_names = sched_cfg.get("profiles") or get_all_profile_names(profiles_dir)

    if not profile_names:
        logger.error("No profiles found. Add .txt files to the profiles/ directory.")
        sys.exit(1)

    scheduler = BlockingScheduler()

    for time_str in run_times:
        hour, minute = time_str.split(":")
        for profile_name in profile_names:
            scheduler.add_job(
                run_pipeline_job,
                trigger=CronTrigger(hour=int(hour), minute=int(minute)),
                args=[profile_name],
                id=f"pipeline_{profile_name}_{time_str.replace(':', '')}",
                replace_existing=True,
            )
            logger.info(f"[scheduler] Scheduled '{profile_name}' at {time_str} daily.")

    logger.info(f"[scheduler] {len(scheduler.get_jobs())} job(s) registered. Press Ctrl+C to stop.")

    try:
        scheduler.start()
    except (KeyboardInterrupt, SystemExit):
        logger.info("[scheduler] Stopped.")


if __name__ == "__main__":
    main()
