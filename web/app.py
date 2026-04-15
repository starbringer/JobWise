"""
web/app.py — Flask web application for the job finder.
Accessible on local network at http://0.0.0.0:6868
"""

import json
import os
import re
import shutil
import sys
import threading
import time as _time
from datetime import date, timedelta
from pathlib import Path

import yaml
from flask import Flask, abort, jsonify, redirect, render_template, request, url_for

PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

import src.database as database
from src.ranker import extract_description, sanitize_description

app = Flask(__name__)
app.jinja_env.filters["extract_description"] = extract_description
app.jinja_env.filters["sanitize_description"] = sanitize_description

# ---------------------------------------------------------------------------
# Pipeline state — tracks background "Find New Jobs" runs
# Only one profile can run at a time (single-user local tool).
# ---------------------------------------------------------------------------
_pipeline_lock = threading.Lock()
_pipeline_state: dict = {
    "status": "idle",       # idle | running | done | error
    "step": None,           # fetching | filtering | scoring (current sub-step while running)
    "job_count": None,      # number of jobs being AI-scored (set when step='scoring')
    "profile": None,
    "started_at": None,     # Unix timestamp (float)
    "finished_at": None,    # Unix timestamp (float)
    "summary": None,        # dict returned by run_pipeline on success
    "error": None,          # error message string on failure
}


def _run_pipeline_bg(name: str) -> None:
    """Background thread: runs the full pipeline and updates _pipeline_state."""
    def _progress(step: str, count: int | None = None):
        with _pipeline_lock:
            _pipeline_state["step"] = step
            _pipeline_state["job_count"] = count

    try:
        from src.pipeline import run_pipeline  # imported here to avoid startup cost
        summary = run_pipeline(name, triggered_by="web", progress_callback=_progress)
        with _pipeline_lock:
            _pipeline_state.update({
                "status": "done",
                "finished_at": _time.time(),
                "summary": summary,
                "error": None,
            })
    except Exception as exc:
        with _pipeline_lock:
            _pipeline_state.update({
                "status": "error",
                "finished_at": _time.time(),
                "summary": None,
                "error": str(exc),
            })


# ---------------------------------------------------------------------------
# Profile-extract state — tracks background AI-extraction runs for new profiles.
# Only one extraction can run at a time.
# ---------------------------------------------------------------------------
_extract_lock = threading.Lock()
_extract_state: dict = {
    "status": "idle",       # idle | running | done | error
    "profile": None,
    "started_at": None,
    "finished_at": None,
    "error": None,
}


def _run_extract_bg(name: str) -> None:
    """Background thread: runs profile_processor.process() for a newly discovered profile."""
    try:
        from src import profile_processor  # lazy import
        config = load_config()
        profiles_dir = PROJECT_ROOT / config.get("profiles_dir", "profiles")
        conn = get_db()
        profile_processor.process(conn, name, profiles_dir)
        conn.close()
        with _extract_lock:
            _extract_state.update({
                "status": "done",
                "finished_at": _time.time(),
                "error": None,
            })
    except Exception as exc:
        with _extract_lock:
            _extract_state.update({
                "status": "error",
                "finished_at": _time.time(),
                "error": str(exc),
            })


def _ai_configured() -> bool:
    """Return True if the selected AI provider appears to have its credentials in place.

    - claude_cli / ollama: no API key needed — always True.
    - gemini / openai / anthropic: check os.environ first (loaded by dotenv on pipeline
      runs), then fall back to parsing the .env file directly so the check works even
      before the first pipeline run.
    """
    config = load_config()
    provider = config.get("ai", {}).get("provider", "")
    if provider in ("claude_cli", "ollama", ""):
        return True
    key_map = {
        "gemini":    "GEMINI_API_KEY",
        "openai":    "OPENAI_API_KEY",
        "anthropic": "ANTHROPIC_API_KEY",
    }
    env_var = key_map.get(provider)
    if not env_var:
        return True  # unknown provider — optimistically assume configured
    if os.environ.get(env_var):
        return True
    env_path = PROJECT_ROOT / ".env"
    if env_path.exists():
        for line in env_path.read_text(encoding="utf-8").splitlines():
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                k, _, v = line.partition("=")
                if k.strip() == env_var and v.strip():
                    return True
    return False


# ---------------------------------------------------------------------------
# Default configuration — used when config.yaml does not yet exist.
# Mirrors config.sample.yaml so the app is fully functional out of the box.
# ---------------------------------------------------------------------------
_DEFAULT_CONFIG: dict = {
    "ai": {
        "provider": "claude_cli",
        "claude_cli":  {"model": "claude-sonnet-4-6"},
        "anthropic":   {"model": "claude-haiku-4-5-20251001"},
        "openai":      {"model": "gpt-4o"},
        "gemini":      {"model": "gemini-2.0-flash"},
        "ollama":      {"model": "gemma2:9b", "host": "http://localhost", "port": 11434},
    },
    "top_n": 50,
    "top_n_display": 50,
    "jsearch_queries_per_run": 10,
    "sources": {
        "greenhouse": True,
        "lever": True,
        "jsearch": True,
        "jobspy": True,
        "max_ats_companies_per_run": 20,
    },
    "jobspy": {"sites": ["linkedin", "indeed"], "results_per_site": 25},
    "scheduler": {"enabled": False, "run_times": ["11:00", "18:00"], "profiles": []},
    "api": {"jsearch_reset_day": 1},
    "web": {"host": "0.0.0.0", "port": 6868, "debug": False},
    "database": {"path": "data/jobs.db"},
    "profiles_dir": "profiles",
    "job_retention_days": 30,
    "logging": {
        "level": "INFO",
        "file": "logs/pipeline.log",
        "max_bytes": 5242880,
        "backup_count": 3,
    },
    "ranker": {
        "batch_size": 50,
        "min_match_score": 0.4,
        "description_max_chars": 3500,
        "scoring": {
            "manager": {
                "required": 10, "preferred": 5, "nice_to_have": 5,
                "unknown": 1, "extra_skill": 2,
            },
            "candidate": {"must_have": 10, "nice_to_have": 7, "unknown": 5},
        },
    },
}


def load_config() -> dict:
    """Load config.yaml, falling back to _DEFAULT_CONFIG if the file is absent.

    The fallback lets the app start and the Settings page render correctly even
    before the user has created a config file.  The first save from the Settings
    page will write config.yaml to disk.
    """
    import copy
    cfg_path = PROJECT_ROOT / "config" / "config.yaml"
    if not cfg_path.exists():
        return copy.deepcopy(_DEFAULT_CONFIG)
    with open(cfg_path, encoding="utf-8") as f:
        on_disk = yaml.safe_load(f) or {}
    # Merge: defaults supply any keys absent from the file (e.g. a key added in
    # a newer version that isn't in the user's existing config.yaml yet).
    merged = copy.deepcopy(_DEFAULT_CONFIG)
    _deep_merge(merged, on_disk)
    return merged


def _deep_merge(base: dict, override: dict) -> None:
    """Recursively merge *override* into *base* in-place (override wins)."""
    for key, val in override.items():
        if key in base and isinstance(base[key], dict) and isinstance(val, dict):
            _deep_merge(base[key], val)
        else:
            base[key] = val


def get_db():
    config = load_config()
    db_path = PROJECT_ROOT / config["database"]["path"]
    return database.init_db(db_path)


_PROFILE_EXTENSIONS = {".txt", ".md", ".pdf", ".docx"}


# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------

@app.route("/")
def index():
    conn = get_db()
    profiles = conn.execute("SELECT * FROM profiles ORDER BY name").fetchall()
    profile_data = []
    for p in profiles:
        counts = conn.execute(
            """SELECT application_status, COUNT(*) as cnt
               FROM profile_jobs WHERE profile_id=? AND hidden=FALSE
               GROUP BY application_status""",
            (p["id"],),
        ).fetchall()
        status_counts = {r["application_status"]: r["cnt"] for r in counts}
        total = sum(status_counts.values())
        active = sum(v for k, v in status_counts.items()
                     if k in ("applied", "phone_screen", "interview_1", "interview_2",
                               "interview_3", "offer"))
        profile_data.append({
            "name": p["name"],
            "total_jobs": total,
            "active_applications": active,
            "new_jobs": status_counts.get("new", 0),
            "structured": json.loads(p["structured_content"]) if p["structured_content"] else {},
            "needs_setup": not bool(p["structured_content"]),
        })
    conn.close()
    return render_template("index.html", profiles=profile_data, ai_ready=_ai_configured())


@app.route("/profile/<name>")
def profile_jobs(name):
    conn = get_db()
    profile = database.get_profile(conn, name)
    if not profile:
        abort(404)

    status_filter = request.args.get("status", "all")
    sort_by = request.args.get("sort", "score")
    show_all = request.args.get("show_all", "0") == "1"

    config = load_config()
    retention_days = config.get("job_retention_days", 30)

    # hidden tab: all hidden scored jobs (dismissed by user or below top-N by pipeline),
    # regardless of application stage. Ordered by score so best candidates appear first.
    if status_filter == "hidden":
        hidden_order = {
            "score": "(COALESCE(pj.manager_score, 0) + COALESCE(pj.candidate_score, 0)) DESC",
            "date":  "j.date_posted DESC, pj.added_at DESC",
            "found": "pj.added_at DESC",
            "company": "j.company ASC, j.title ASC",
        }.get(sort_by, "(COALESCE(pj.manager_score, 0) + COALESCE(pj.candidate_score, 0)) DESC")
        query = f"""
            SELECT j.*, pj.match_score, pj.match_notes, pj.application_status,
                   pj.notes as user_notes, pj.added_at, pj.rank_at_discovery,
                   pj.manager_score, pj.candidate_score, pj.candidate_notes,
                   pj.match_pairs_json, pj.hidden as is_hidden, pj.saved
            FROM profile_jobs pj
            JOIN jobs j ON pj.job_key = j.job_key
            WHERE pj.profile_id = ? AND pj.hidden=TRUE AND pj.manager_score IS NOT NULL
            ORDER BY {hidden_order}
        """
        jobs = conn.execute(query, [profile["id"]]).fetchall()

        hidden_scored_count = len(jobs)
        scored_hidden_count = hidden_scored_count

        all_counts = conn.execute(
            """SELECT application_status, COUNT(*) as cnt FROM profile_jobs
               WHERE profile_id=? AND hidden=FALSE
               AND (manager_score IS NOT NULL OR COALESCE(saved, 0) = 1
                    OR application_status NOT IN ('new', 'missing_info'))
               GROUP BY application_status""",
            (profile["id"],),
        ).fetchall()
        status_counts = {r["application_status"]: r["cnt"] for r in all_counts}
        saved_count = conn.execute(
            "SELECT COUNT(*) FROM profile_jobs WHERE profile_id=? AND hidden=FALSE AND saved=TRUE",
            (profile["id"],),
        ).fetchone()[0]
        interviewing_count = sum(
            status_counts.get(s, 0)
            for s in ("phone_screen", "interview_1", "interview_2", "interview_3")
        )
        missing_info_count = conn.execute(
            "SELECT COUNT(*) FROM profile_jobs WHERE profile_id=? AND application_status='missing_info'",
            (profile["id"],),
        ).fetchone()[0]
        structured = {}
        if profile["structured_content"]:
            structured = json.loads(profile["structured_content"])
        conn.close()
        return render_template(
            "jobs.html",
            profile_name=name,
            profile=structured,
            jobs=jobs,
            status_filter=status_filter,
            sort_by=sort_by,
            show_all=False,
            scored_hidden_count=scored_hidden_count,
            status_counts=status_counts,
            saved_count=saved_count,
            interviewing_count=interviewing_count,
            missing_info_count=missing_info_count,
            hidden_scored_count=hidden_scored_count,
            total_count=sum(status_counts.values()),
        )

    # missing_info filter: special visibility path (stored as hidden=TRUE with no score).
    if status_filter == "missing_info":
        visibility_clause = "pj.application_status = 'missing_info'"
        hidden_gate = ""
    elif show_all:
        # show_all: include hidden scored jobs so the user can see the full pool inline.
        visibility_clause = "pj.manager_score IS NOT NULL"
        hidden_gate = ""
    else:
        # Default: only non-hidden jobs, scored or user-actioned beyond 'new'.
        # hidden=FALSE is the hard gate — removed jobs must not leak back through stage tabs.
        # saved=TRUE jobs are always visible regardless of score (user has expressed intent).
        visibility_clause = "(pj.manager_score IS NOT NULL OR COALESCE(pj.saved, 0) = 1 OR pj.application_status NOT IN ('new', 'missing_info'))"
        hidden_gate = "AND pj.hidden = FALSE"

    # Date freshness filter: actioned jobs always shown regardless of age.
    date_clause = f"""(
        pj.application_status NOT IN ('new', 'missing_info')
        OR CASE WHEN j.date_posted GLOB '[0-9][0-9][0-9][0-9]-[0-9][0-9]-[0-9][0-9]'
                THEN j.date_posted
                ELSE DATE(j.date_found)
           END >= DATE('now', '-{retention_days} days')
    )"""

    query = f"""
        SELECT j.*, pj.match_score, pj.match_notes, pj.application_status,
               pj.notes as user_notes, pj.added_at, pj.rank_at_discovery,
               pj.manager_score, pj.candidate_score, pj.candidate_notes,
               pj.match_pairs_json, pj.hidden as is_hidden, pj.saved
        FROM profile_jobs pj
        JOIN jobs j ON pj.job_key = j.job_key
        WHERE pj.profile_id = ? {hidden_gate} AND ({visibility_clause})
        AND {date_clause}
    """
    params = [profile["id"]]

    if status_filter != "all" and status_filter != "missing_info":
        if show_all:
            pass  # status filter only applies to promoted jobs; skip when showing all scored
        elif status_filter == "saved":
            query += " AND pj.saved = TRUE"
        elif status_filter == "interviewing":
            query += " AND pj.application_status IN ('phone_screen','interview_1','interview_2','interview_3')"
        else:
            query += " AND pj.application_status = ?"
            params.append(status_filter)

    order = {
        "score": "(COALESCE(pj.manager_score, 0) + COALESCE(pj.candidate_score, 0)) DESC",
        "date": "j.date_posted DESC, pj.added_at DESC",
        "found": "pj.added_at DESC",
        "company": "j.company ASC, j.title ASC",
        "status": "pj.application_status ASC",
    }.get(sort_by, "(COALESCE(pj.manager_score, 0) + COALESCE(pj.candidate_score, 0)) DESC")
    query += f" ORDER BY {order}"

    jobs = conn.execute(query, params).fetchall()

    # Count of scored-but-hidden jobs (for "show more" label and hidden tab badge).
    # Covers all hidden scored jobs regardless of application stage.
    scored_hidden_count = conn.execute(
        "SELECT COUNT(*) FROM profile_jobs WHERE profile_id=? AND hidden=TRUE AND manager_score IS NOT NULL",
        (profile["id"],),
    ).fetchone()[0]

    # Status counts for filter tabs: non-hidden jobs only.
    all_counts = conn.execute(
        """SELECT application_status, COUNT(*) as cnt FROM profile_jobs
           WHERE profile_id=? AND hidden=FALSE
           AND (manager_score IS NOT NULL OR COALESCE(saved, 0) = 1
                OR application_status NOT IN ('new', 'missing_info'))
           GROUP BY application_status""",
        (profile["id"],),
    ).fetchall()
    status_counts = {r["application_status"]: r["cnt"] for r in all_counts}
    saved_count = conn.execute(
        "SELECT COUNT(*) FROM profile_jobs WHERE profile_id=? AND hidden=FALSE AND saved=TRUE",
        (profile["id"],),
    ).fetchone()[0]
    interviewing_count = sum(
        status_counts.get(s, 0)
        for s in ("phone_screen", "interview_1", "interview_2", "interview_3")
    )

    missing_info_count = conn.execute(
        "SELECT COUNT(*) FROM profile_jobs WHERE profile_id=? AND application_status='missing_info'",
        (profile["id"],),
    ).fetchone()[0]

    structured = {}
    if profile["structured_content"]:
        structured = json.loads(profile["structured_content"])

    conn.close()
    return render_template(
        "jobs.html",
        profile_name=name,
        profile=structured,
        jobs=jobs,
        status_filter=status_filter,
        sort_by=sort_by,
        show_all=show_all,
        scored_hidden_count=scored_hidden_count,
        status_counts=status_counts,
        saved_count=saved_count,
        interviewing_count=interviewing_count,
        missing_info_count=missing_info_count,
        hidden_scored_count=scored_hidden_count,
        total_count=sum(status_counts.values()),
    )


@app.route("/profile/<name>/structured")
def profile_structured(name):
    conn = get_db()
    profile = database.get_profile(conn, name)
    conn.close()
    if not profile or not profile["structured_content"]:
        abort(404)
    structured = json.loads(profile["structured_content"])
    return render_template("profile_structured.html", profile_name=name, profile=structured)


@app.route("/profile/<name>/field", methods=["POST"])
def update_profile_field(name):
    conn = get_db()
    if not database.get_profile(conn, name):
        conn.close()
        abort(404)
    field = request.form.get("field", "").strip()
    action = request.form.get("action", "").strip()
    value = request.form.get("value", "").strip()
    if field and action:
        database.update_profile_field(conn, name, field, action, value)
    conn.close()
    return jsonify({"ok": True})


@app.route("/profile/<name>/sync", methods=["POST"])
def sync_profile(name):
    from src import profile_processor
    config = load_config()
    profiles_dir = PROJECT_ROOT / config.get("profiles_dir", "profiles")
    conn = get_db()
    if not database.get_profile(conn, name):
        conn.close()
        abort(404)
    try:
        result = profile_processor.sync_from_file(conn, name, profiles_dir)
        additions = result["additions"]
        msg = f"Synced — {additions} new item(s) added from file." if additions else "Synced — nothing new to add."
    except FileNotFoundError as e:
        msg = f"Sync failed: {e}"
    conn.close()
    # Pass flash message via query param (no session required)
    return redirect(url_for("profile_structured", name=name, msg=msg))


@app.route("/api/profiles/register", methods=["POST"])
def register_profile():
    """Register a profile from a resume file path supplied by the user.

    Accepts JSON: {"name": "alice", "file_path": "/path/to/resume.pdf"}
    Copies the file into profiles/ and upserts the profile row in the DB.
    The profile will be fully parsed (AI) on the next pipeline run.
    """
    data = request.json or {}
    raw_name = (data.get("name") or "").strip()
    file_path = (data.get("file_path") or "").strip().strip('"').strip("'")

    if not raw_name:
        return jsonify({"error": "Profile name is required."}), 400
    name = re.sub(r"[^a-zA-Z0-9_-]", "_", raw_name).strip("_").lower() or "me"

    if not file_path:
        return jsonify({"error": "File path is required."}), 400

    src = Path(file_path)
    if not src.exists():
        return jsonify({"error": f"File not found: {file_path}"}), 400
    if src.suffix.lower() not in _PROFILE_EXTENSIONS:
        return jsonify({"error": "Unsupported file type. Use .pdf, .docx, .txt, or .md"}), 400

    config = load_config()
    profiles_dir = PROJECT_ROOT / config.get("profiles_dir", "profiles")
    profiles_dir.mkdir(parents=True, exist_ok=True)
    dest = profiles_dir / f"{name}{src.suffix.lower()}"

    try:
        shutil.copy2(src, dest)
    except PermissionError:
        home_dir = str(Path.home())
        return jsonify({
            "error": (
                f"Permission denied reading '{file_path}'. "
                f"Move the file to your home folder ({home_dir}) and try again."
            )
        }), 400
    except Exception as exc:
        return jsonify({"error": str(exc)}), 500

    conn = get_db()
    database.upsert_profile(conn, name, str(dest))
    conn.close()
    return jsonify({"ok": True, "name": name})


@app.route("/profile/<name>/job/<job_key>")
def job_detail(name, job_key):
    conn = get_db()
    profile = database.get_profile(conn, name)
    if not profile:
        abort(404)
    row = conn.execute(
        """SELECT j.*, pj.match_score, pj.match_notes, pj.application_status,
                  pj.notes as user_notes, pj.added_at, pj.rank_at_discovery,
                  pj.manager_score, pj.candidate_score, pj.candidate_notes,
                  pj.match_pairs_json, pj.hidden as is_hidden, pj.saved
           FROM profile_jobs pj JOIN jobs j ON pj.job_key = j.job_key
           WHERE pj.profile_id = ? AND pj.job_key = ?""",
        (profile["id"], job_key),
    ).fetchone()
    conn.close()
    if not row:
        abort(404)
    return render_template("job_detail.html", profile_name=name, job=row)


@app.route("/profile/<name>/job/<job_key>/status", methods=["POST"])
def update_status(name, job_key):
    """
    Update a job's application status.

    Two-list model transitions:
    - 'new' → actioned (saved/applied/etc.): job leaves top-N list, enters user-action list.
      Rebalance is triggered to pull in the next best pipeline-hidden job to fill the vacancy.
    - 'new' → 'rejected'/'withdrawn': job leaves top-N and is not added to user-action list.
      Rebalance is triggered to fill the vacancy.
    - actioned → anything: job was already outside top-N; no top-N vacancy, no rebalance.
    - any → 'new': job re-enters top-N pool; rebalance enforces the cap.
    """
    conn = get_db()
    profile = database.get_profile(conn, name)
    if not profile:
        abort(404)
    new_status = request.form.get("status") or request.json.get("status")
    valid = {"new", "applied", "phone_screen", "interview_1",
             "interview_2", "interview_3", "offer", "rejected", "withdrawn"}
    if new_status not in valid:
        abort(400)

    # Capture old status before updating.
    row = conn.execute(
        "SELECT application_status FROM profile_jobs WHERE profile_id=? AND job_key=?",
        (profile["id"], job_key),
    ).fetchone()
    old_status = row["application_status"] if row else None

    # Visibility (hidden) is intentionally NOT changed here — application stage and
    # visibility are independent.  Only hide_job and promote_job control visibility.
    conn.execute(
        """UPDATE profile_jobs SET application_status=?, status_updated_at=CURRENT_TIMESTAMP
           WHERE profile_id=? AND job_key=?""",
        (new_status, profile["id"], job_key),
    )
    conn.commit()

    # Rebalance when the top-N pool changes:
    # - job leaves top-N (old='new', new≠'new'): fill the vacancy
    # - job re-enters top-N (new='new', old≠'new'): enforce cap
    if old_status != new_status and (old_status == "new" or new_status == "new"):
        cfg = load_config()
        database.rebalance_visible_jobs(
            conn,
            profile["id"],
            top_n_display=cfg.get("top_n_display", 50),
            min_match_score=cfg.get("ranker", {}).get("min_match_score", 0.4),
        )

    conn.close()
    if request.is_json:
        return jsonify({"ok": True, "status": new_status})
    return redirect(url_for("profile_jobs", name=name))



@app.route("/profile/<name>/job/<job_key>/save", methods=["POST"])
def toggle_save(name, job_key):
    """Toggle the saved bookmark flag independently of application_status."""
    conn = get_db()
    profile = database.get_profile(conn, name)
    if not profile:
        conn.close()
        abort(404)
    row = conn.execute(
        "SELECT saved, application_status FROM profile_jobs WHERE profile_id=? AND job_key=?",
        (profile["id"], job_key),
    ).fetchone()
    if not row:
        conn.close()
        abort(404)
    new_saved = not bool(row["saved"])
    conn.execute(
        "UPDATE profile_jobs SET saved=? WHERE profile_id=? AND job_key=?",
        (new_saved, profile["id"], job_key),
    )
    conn.commit()

    # Rebalance when a 'new'-status job enters or leaves the saved state,
    # because saved jobs are excluded from the top-N pool. Saving removes a
    # slot (replenish); unsaving adds one back (enforce cap).
    if row["application_status"] == "new":
        cfg = load_config()
        database.rebalance_visible_jobs(
            conn,
            profile["id"],
            top_n_display=cfg.get("top_n_display", 50),
            min_match_score=cfg.get("ranker", {}).get("min_match_score", 0.4),
        )

    conn.close()
    return jsonify({"ok": True, "saved": new_saved})


@app.route("/profile/<name>/job/<job_key>/promote", methods=["POST"])
def promote_job(name, job_key):
    """
    Un-remove (restore) a previously dismissed job.

    Two-list model:
    - Actioned status (saved/applied/etc.): job goes into the user-action list and is
      always visible; not counted toward top_n_display, no rebalance needed.
    - 'new' status: job re-enters the top-N pool. Rebalance enforces the cap — if it
      scores in the top N it stays visible, otherwise it is hidden again.

    In both cases status_updated_at is cleared so the job is no longer marked as
    user-dismissed and future pipeline runs can consider it normally.
    """
    conn = get_db()
    profile = database.get_profile(conn, name)
    if not profile:
        conn.close()
        abort(404)

    row = conn.execute(
        "SELECT application_status FROM profile_jobs WHERE profile_id=? AND job_key=?",
        (profile["id"], job_key),
    ).fetchone()
    if not row:
        conn.close()
        abort(404)
    status = row["application_status"]

    # Restore visibility and clear the user-dismissed marker.
    conn.execute(
        "UPDATE profile_jobs SET hidden=FALSE, status_updated_at=NULL WHERE profile_id=? AND job_key=?",
        (profile["id"], job_key),
    )
    conn.commit()

    # For 'new' jobs re-entering the top-N pool, enforce the display cap.
    # Actioned jobs go straight to the user-action list — no cap applies.
    if status == "new":
        cfg = load_config()
        database.rebalance_visible_jobs(
            conn,
            profile["id"],
            top_n_display=cfg.get("top_n_display", 50),
            min_match_score=cfg.get("ranker", {}).get("min_match_score", 0.4),
        )

    conn.close()
    return jsonify({"ok": True})


@app.route("/profile/<name>/job/<job_key>/hide", methods=["POST"])
def hide_job(name, job_key):
    """
    Dismiss a job from the recommended list (set hidden=TRUE, preserve application stage).

    Two-list model:
    - If the job was in the top-N list (status='new') → replenish by rebalancing so the
      top-N count stays at top_n_display.
    - If the job was in the user-action list (saved/applied/etc.) → just hide it; these
      jobs are not counted toward top_n_display so no replenishment is needed.
    """
    conn = get_db()
    profile = database.get_profile(conn, name)
    if not profile:
        conn.close()
        abort(404)

    # Capture status before hiding so we know which list the job came from.
    row = conn.execute(
        "SELECT application_status FROM profile_jobs WHERE profile_id=? AND job_key=?",
        (profile["id"], job_key),
    ).fetchone()
    if not row:
        conn.close()
        abort(404)
    old_status = row["application_status"]

    # Hide the job and mark it as user-dismissed.
    conn.execute(
        "UPDATE profile_jobs SET hidden=TRUE, status_updated_at=CURRENT_TIMESTAMP WHERE profile_id=? AND job_key=?",
        (profile["id"], job_key),
    )
    conn.commit()

    # Only replenish the top-N list when a 'new' (top-N) job was removed.
    # Actioned jobs (saved/applied/etc.) are not counted in top_n_display, so
    # hiding them does not create a vacancy that needs filling.
    if old_status == "new":
        cfg = load_config()
        database.rebalance_visible_jobs(
            conn,
            profile["id"],
            top_n_display=cfg.get("top_n_display", 50),
            min_match_score=cfg.get("ranker", {}).get("min_match_score", 0.4),
        )

    conn.close()
    return jsonify({"ok": True})


@app.route("/profile/<name>/job/<job_key>/notes", methods=["POST"])
def save_notes(name, job_key):
    conn = get_db()
    profile = database.get_profile(conn, name)
    if not profile:
        abort(404)
    notes = request.form.get("notes") or (request.json or {}).get("notes", "")
    conn.execute(
        "UPDATE profile_jobs SET notes=? WHERE profile_id=? AND job_key=?",
        (notes, profile["id"], job_key),
    )
    conn.commit()
    conn.close()
    return jsonify({"ok": True})


@app.route("/database")
def jobs_database():
    conn = get_db()
    config = load_config()

    search       = request.args.get("q", "").strip()
    source       = request.args.get("source", "")
    remote       = request.args.get("remote", "")
    sort_by      = request.args.get("sort", "date_found")
    has_salary   = request.args.get("has_salary", "") == "1"
    posted_after = request.args.get("posted_after", "").strip()
    profile_filter = request.args.get("profile", "")   # profile name or "none"
    missing_info = request.args.get("missing_info", "") == "1"
    has_score    = request.args.get("has_score", "") == "1"
    page         = max(1, int(request.args.get("page", 1) or 1))
    per_page     = 50

    conditions = []
    params: list = []

    if search:
        conditions.append("(LOWER(j.title) LIKE ? OR LOWER(j.company) LIKE ?)")
        like = f"%{search.lower()}%"
        params += [like, like]
    if source:
        conditions.append("j.source = ?")
        params.append(source)
    if remote:
        conditions.append("j.remote_type = ?")
        params.append(remote)
    if has_salary:
        conditions.append("(j.salary_min IS NOT NULL OR j.salary_max IS NOT NULL OR j.salary_raw IS NOT NULL)")
    if posted_after:
        conditions.append(
            "j.date_posted GLOB '[0-9][0-9][0-9][0-9]-[0-9][0-9]-[0-9][0-9]' AND j.date_posted >= ?"
        )
        params.append(posted_after)
    if profile_filter == "none":
        # Jobs not associated with any profile at all
        conditions.append("j.job_key NOT IN (SELECT DISTINCT job_key FROM profile_jobs)")
    elif profile_filter:
        conditions.append(
            "j.job_key IN (SELECT pj.job_key FROM profile_jobs pj JOIN profiles p ON pj.profile_id=p.id WHERE p.name=?)"
        )
        params.append(profile_filter)
    if missing_info:
        conditions.append(
            "j.job_key IN (SELECT job_key FROM profile_jobs WHERE application_status='missing_info')"
        )
    if has_score:
        conditions.append(
            "j.job_key IN (SELECT job_key FROM profile_jobs WHERE manager_score IS NOT NULL)"
        )

    where = ("WHERE " + " AND ".join(conditions)) if conditions else ""

    order_map = {
        "date_found":  "j.date_found DESC",
        "date_posted": "CASE WHEN j.date_posted GLOB '[0-9][0-9][0-9][0-9]-[0-9][0-9]-[0-9][0-9]' THEN j.date_posted ELSE DATE(j.date_found) END DESC",
        "company":     "j.company ASC, j.title ASC",
        "title":       "j.title ASC",
    }
    order = order_map.get(sort_by, "j.date_found DESC")

    total = conn.execute(f"SELECT COUNT(*) FROM jobs j {where}", params).fetchone()[0]
    offset = (page - 1) * per_page
    jobs = conn.execute(
        f"""SELECT j.*, p.name AS fetched_for_profile_name
            FROM jobs j
            LEFT JOIN profiles p ON j.fetched_for_profile_id = p.id
            {where} ORDER BY {order} LIMIT ? OFFSET ?""",
        params + [per_page, offset],
    ).fetchall()

    # Build profile-association map for this page's jobs (single query, no N+1)
    job_keys = [j["job_key"] for j in jobs]
    profile_map: dict = {}
    if job_keys:
        placeholders = ",".join("?" * len(job_keys))
        rows = conn.execute(
            f"""SELECT pj.job_key, p.name, pj.application_status,
                       pj.manager_score, pj.candidate_score, pj.match_score,
                       pj.match_notes, pj.hidden, pj.ai_raw_response
                FROM profile_jobs pj
                JOIN profiles p ON pj.profile_id = p.id
                WHERE pj.job_key IN ({placeholders})
                ORDER BY p.name, pj.application_status""",
            job_keys,
        ).fetchall()
        for r in rows:
            profile_map.setdefault(r["job_key"], []).append(dict(r))

    remote_types = [r[0] for r in conn.execute(
        "SELECT DISTINCT remote_type FROM jobs WHERE remote_type IS NOT NULL ORDER BY remote_type"
    ).fetchall()]
    source_counts = {r[0]: r[1] for r in conn.execute(
        "SELECT source, COUNT(*) FROM jobs GROUP BY source ORDER BY COUNT(*) DESC"
    ).fetchall()}
    all_profiles = [r[0] for r in conn.execute(
        "SELECT name FROM profiles ORDER BY name"
    ).fetchall()]

    conn.close()
    total_pages = max(1, (total + per_page - 1) // per_page)
    return render_template(
        "jobs_database.html",
        jobs=jobs,
        profile_map=profile_map,
        search=search,
        source=source,
        remote=remote,
        sort_by=sort_by,
        has_salary=has_salary,
        posted_after=posted_after,
        profile_filter=profile_filter,
        missing_info=missing_info,
        has_score=has_score,
        page=page,
        per_page=per_page,
        total=total,
        total_pages=total_pages,
        remote_types=remote_types,
        source_counts=source_counts,
        all_profiles=all_profiles,
        retention_days=config.get("job_retention_days", 30),
        retention_cutoff=(date.today() - timedelta(days=config.get("job_retention_days", 30))).isoformat(),
    )


@app.route("/profile/<name>/stats")
def profile_stats(name):
    conn = get_db()
    profile = database.get_profile(conn, name)
    if not profile:
        abort(404)
    counts = conn.execute(
        """SELECT application_status, COUNT(*) as cnt FROM profile_jobs
           WHERE profile_id=? AND hidden=FALSE GROUP BY application_status""",
        (profile["id"],),
    ).fetchall()
    by_source = conn.execute(
        """SELECT j.source, COUNT(*) as cnt FROM profile_jobs pj
           JOIN jobs j ON pj.job_key=j.job_key
           WHERE pj.profile_id=? AND pj.hidden=FALSE GROUP BY j.source""",
        (profile["id"],),
    ).fetchall()
    runs = conn.execute(
        """SELECT * FROM search_runs WHERE profile_id=? ORDER BY started_at DESC LIMIT 10""",
        (profile["id"],),
    ).fetchall()
    conn.close()
    return render_template(
        "stats.html",
        profile_name=name,
        status_counts={r["application_status"]: r["cnt"] for r in counts},
        source_counts={r["source"]: r["cnt"] for r in by_source},
        runs=runs,
    )


@app.route("/settings")
def settings():
    config = load_config()
    config_exists = (PROJECT_ROOT / "config" / "config.yaml").exists()
    return render_template("settings.html", config=config, config_exists=config_exists)


@app.route("/api/settings/save", methods=["POST"])
def save_settings():
    """Write a subset of config.yaml fields sent as JSON. Takes effect immediately (config is
    reloaded per-request). Note: web.host / web.port changes require a server restart."""
    try:
        data = request.json or {}
        config_path = PROJECT_ROOT / "config" / "config.yaml"
        config = load_config()

        # ── AI provider ────────────────────────────────────────────────────────
        if "ai" in data:
            ai = data["ai"]
            if "provider" in ai:
                config["ai"]["provider"] = ai["provider"]
            for provider in ("claude_cli", "anthropic", "openai", "gemini"):
                if provider in ai and "model" in ai[provider]:
                    config["ai"].setdefault(provider, {})["model"] = ai[provider]["model"]
            if "ollama" in ai:
                o = ai["ollama"]
                config["ai"].setdefault("ollama", {})
                for k in ("model", "host", "port"):
                    if k in o:
                        config["ai"]["ollama"][k] = o[k]

        # ── Pipeline top-level ─────────────────────────────────────────────────
        for key in ("top_n", "top_n_display", "jsearch_queries_per_run", "job_retention_days"):
            if key in data:
                config[key] = int(data[key])

        # ── API credentials / quota ────────────────────────────────────────────
        if "api" in data:
            if "jsearch_reset_day" in data["api"]:
                config["api"]["jsearch_reset_day"] = int(data["api"]["jsearch_reset_day"])

        # ── Sources ────────────────────────────────────────────────────────────
        if "sources" in data:
            s = data["sources"]
            for src in ("greenhouse", "lever", "jsearch", "jobspy"):
                if src in s:
                    config["sources"][src] = bool(s[src])
            if "max_ats_companies_per_run" in s:
                config["sources"]["max_ats_companies_per_run"] = int(s["max_ats_companies_per_run"])

        # ── JobSpy ────────────────────────────────────────────────────────────
        if "jobspy" in data:
            j = data["jobspy"]
            if "sites" in j:
                config["jobspy"]["sites"] = j["sites"]
            if "results_per_site" in j:
                config["jobspy"]["results_per_site"] = int(j["results_per_site"])

        # ── Ranker ────────────────────────────────────────────────────────────
        if "ranker" in data:
            r = data["ranker"]
            for k in ("batch_size", "description_max_chars"):
                if k in r:
                    config["ranker"][k] = int(r[k])
            if "min_match_score" in r:
                config["ranker"]["min_match_score"] = float(r["min_match_score"])
            if "scoring" in r:
                sc = r["scoring"]
                for side in ("manager", "candidate"):
                    if side in sc:
                        for k, v in sc[side].items():
                            config["ranker"]["scoring"][side][k] = int(v)

        # ── Scheduler ─────────────────────────────────────────────────────────
        if "scheduler" in data:
            sch = data["scheduler"]
            if "enabled" in sch:
                config["scheduler"]["enabled"] = bool(sch["enabled"])
            if "run_times" in sch:
                config["scheduler"]["run_times"] = sch["run_times"]
            if "profiles" in sch:
                config["scheduler"]["profiles"] = sch["profiles"]

        # ── Logging ───────────────────────────────────────────────────────────
        if "logging" in data:
            lg = data["logging"]
            if "level" in lg:
                config["logging"]["level"] = lg["level"]
            if "file" in lg:
                config["logging"]["file"] = lg["file"]

        # ── Web (restart required for host/port) ──────────────────────────────
        if "web" in data:
            w = data["web"]
            if "host" in w:
                config["web"]["host"] = w["host"]
            if "port" in w:
                config["web"]["port"] = int(w["port"])

        config_path.parent.mkdir(parents=True, exist_ok=True)
        with open(config_path, "w", encoding="utf-8") as f:
            yaml.dump(config, f, default_flow_style=False, allow_unicode=True, sort_keys=False)

        return jsonify({"ok": True})
    except Exception as exc:
        return jsonify({"ok": False, "error": str(exc)}), 500


@app.route("/profile/<name>/import_companies", methods=["POST"])
def import_companies(name):
    """Import companies from the global preferred_companies.txt into the profile's
    preferred_companies list. One-time migration helper — idempotent."""
    conn = get_db()
    profile = database.get_profile(conn, name)
    if not profile:
        conn.close()
        abort(404)

    config = load_config()
    preferred_file = PROJECT_ROOT / config.get(
        "preferred_companies_file", "config/preferred_companies.txt"
    )
    companies = []
    if preferred_file.exists():
        for line in preferred_file.read_text(encoding="utf-8").splitlines():
            line = line.strip()
            if line and not line.startswith("#"):
                companies.append(line)

    structured = json.loads(profile["structured_content"] or "{}")
    existing = set(structured.get("preferred_companies") or [])
    added = 0
    for company in companies:
        if company not in existing:
            database.update_profile_field(conn, name, "preferred_companies", "add", company)
            added += 1

    conn.close()
    return jsonify({"ok": True, "added": added, "total": len(companies)})


@app.route("/api/profiles/<name>/extract", methods=["POST"])
def api_profile_extract(name):
    """Start a background AI extraction run for a newly registered profile.

    Refuses to start if the pipeline or another extraction is already running.
    """
    conn = get_db()
    profile = database.get_profile(conn, name)
    conn.close()
    if not profile:
        abort(404)

    with _pipeline_lock:
        if _pipeline_state["status"] == "running":
            return jsonify({"ok": False, "error": "pipeline_running"}), 409

    with _extract_lock:
        if _extract_state["status"] == "running":
            return jsonify({
                "ok": False,
                "error": "already_running",
                "profile": _extract_state["profile"],
            }), 409
        _extract_state.update({
            "status": "running",
            "profile": name,
            "started_at": _time.time(),
            "finished_at": None,
            "error": None,
        })

    t = threading.Thread(target=_run_extract_bg, args=(name,), daemon=True)
    t.start()
    return jsonify({"ok": True})


@app.route("/api/profiles/extract/status")
def api_profile_extract_status():
    """Return the current profile-extraction state as JSON (polled by the frontend)."""
    with _extract_lock:
        state = dict(_extract_state)
    return jsonify(state)


@app.route("/api/pipeline/run/<name>", methods=["POST"])
def api_pipeline_run(name):
    """Start a background 'Find New Jobs' run for the given profile.
    Returns 409 if a run is already in progress."""
    conn = get_db()
    profile = database.get_profile(conn, name)
    conn.close()
    if not profile:
        abort(404)

    with _pipeline_lock:
        if _pipeline_state["status"] == "running":
            return jsonify({
                "ok": False,
                "error": "already_running",
                "profile": _pipeline_state["profile"],
            }), 409
        _pipeline_state.update({
            "status": "running",
            "step": None,
            "profile": name,
            "started_at": _time.time(),
            "finished_at": None,
            "summary": None,
            "error": None,
        })

    t = threading.Thread(target=_run_pipeline_bg, args=(name,), daemon=True)
    t.start()
    return jsonify({"ok": True})


@app.route("/api/pipeline/status")
def api_pipeline_status():
    """Return the current pipeline state as JSON (polled by the frontend)."""
    with _pipeline_lock:
        state = dict(_pipeline_state)
    return jsonify(state)


if __name__ == "__main__":
    config = load_config()
    web_cfg = config.get("web", {})
    app.run(
        host=web_cfg.get("host", "0.0.0.0"),
        port=web_cfg.get("port", 6868),
        debug=web_cfg.get("debug", False),
    )
