"""
database.py — All SQLite read/write operations.
Runs PRAGMAs and migrations on every startup.
"""

import sqlite3
import logging
from pathlib import Path

logger = logging.getLogger(__name__)


def get_connection(db_path: str | Path) -> sqlite3.Connection:
    """
    Open a SQLite connection with required PRAGMAs set.
    Returns rows as sqlite3.Row objects (dict-like access by column name).
    """
    db_path = Path(db_path)
    db_path.parent.mkdir(parents=True, exist_ok=True)

    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row

    # Must be set per-connection before any FK-dependent operations
    conn.execute("PRAGMA foreign_keys = ON")
    # WAL mode: concurrent readers + one writer; persistent once set
    conn.execute("PRAGMA journal_mode = WAL")

    return conn


def get_schema_version(conn: sqlite3.Connection) -> int:
    """Return current schema version, or 0 if schema_version table doesn't exist."""
    try:
        row = conn.execute("SELECT MAX(version) FROM schema_version").fetchone()
        return row[0] or 0
    except sqlite3.OperationalError:
        return 0


def migrate(conn: sqlite3.Connection) -> None:
    """Apply any pending migrations in order."""
    from src.migrations import v1_initial_schema

    current_version = get_schema_version(conn)
    if current_version < 1:
        logger.info("Applying migration v1...")
        v1_initial_schema.migrate(conn)
        logger.info("Migration v1 applied.")


def init_db(db_path: str | Path) -> sqlite3.Connection:
    """
    Open connection, run migrations, and return the connection.
    Call this once at application startup.
    """
    conn = get_connection(db_path)
    migrate(conn)
    return conn


# ---------------------------------------------------------------------------
# Profile helpers
# ---------------------------------------------------------------------------

def get_profile(conn: sqlite3.Connection, name: str) -> sqlite3.Row | None:
    """Case-insensitive profile lookup."""
    return conn.execute(
        "SELECT * FROM profiles WHERE LOWER(name) = LOWER(?)", (name,)
    ).fetchone()


def upsert_profile(conn: sqlite3.Connection, name: str, input_file: str) -> int:
    """Create profile row if it doesn't exist (case-insensitive). Returns profile id."""
    conn.execute(
        "INSERT OR IGNORE INTO profiles (name, input_file) VALUES (?, ?)",
        (name, input_file),
    )
    conn.commit()
    row = conn.execute(
        "SELECT id FROM profiles WHERE LOWER(name) = LOWER(?)", (name,)
    ).fetchone()
    return row["id"]


def update_profile_structured_content(
    conn: sqlite3.Connection,
    profile_id: int,
    structured_content: str,
    input_hash: str,
    input_modified_at: str,
) -> None:
    conn.execute(
        """
        UPDATE profiles
        SET structured_content = ?,
            input_hash = ?,
            input_modified_at = ?,
            updated_at = CURRENT_TIMESTAMP
        WHERE id = ?
        """,
        (structured_content, input_hash, input_modified_at, profile_id),
    )
    conn.commit()


def update_ideal_cand_pairs(conn: sqlite3.Connection, profile_id: int, value: int) -> None:
    """Store the computed ideal_cand_pairs count on the profile row."""
    conn.execute(
        "UPDATE profiles SET ideal_cand_pairs = ? WHERE id = ?",
        (value, profile_id),
    )
    conn.commit()


def update_profile_field(
    conn: sqlite3.Connection,
    name: str,
    field: str,
    action: str,
    value,
) -> None:
    """
    Modify a single field inside structured_content JSON.

    field:  dot-notation path, e.g. "technical_skills" or "hard_requirements.remote_type"
    action: "add"    — append value to a list field (no-op if already present)
            "remove" — remove value from a list field
            "set"    — replace a scalar field with value
    """
    import json as _json
    row = conn.execute(
        "SELECT structured_content FROM profiles WHERE name = ?", (name,)
    ).fetchone()
    if not row or not row["structured_content"]:
        return

    data = _json.loads(row["structured_content"])

    # Navigate to the parent object using dot notation
    parts = field.split(".")
    target = data
    for part in parts[:-1]:
        if part not in target or not isinstance(target[part], dict):
            target[part] = {}
        target = target[part]
    key = parts[-1]

    if action == "add":
        lst = target.get(key) or []
        if value not in lst:
            lst.append(value)
        target[key] = lst
    elif action == "remove":
        lst = target.get(key) or []
        target[key] = [x for x in lst if x != value]
    elif action == "set":
        # Coerce numeric strings for known numeric fields
        numeric_fields = {"salary_min", "salary_max", "years_experience_total",
                          "years_experience_primary"}
        bool_fields = {"has_clearance"}
        if key in numeric_fields:
            try:
                value = int(value) if value not in (None, "", "null") else None
            except (ValueError, TypeError):
                value = None
        elif key in bool_fields:
            if value in (None, "", "null"):
                value = None
            elif value in (True, "true", "True", "yes", "1"):
                value = True
            else:
                value = False
        elif value == "null":
            value = None
        target[key] = value

    conn.execute(
        "UPDATE profiles SET structured_content = ?, updated_at = CURRENT_TIMESTAMP WHERE name = ?",
        (_json.dumps(data, ensure_ascii=False), name),
    )
    conn.commit()


def get_custom_job_titles(conn: sqlite3.Connection, name: str) -> list[str]:
    """Return user-defined custom job titles for a profile."""
    import json as _json
    row = conn.execute("SELECT custom_job_titles FROM profiles WHERE name = ?", (name,)).fetchone()
    if not row or not row["custom_job_titles"]:
        return []
    try:
        return _json.loads(row["custom_job_titles"])
    except Exception:
        return []


def set_custom_job_titles(conn: sqlite3.Connection, name: str, titles: list[str]) -> None:
    """Persist the full custom job titles list for a profile."""
    import json as _json
    conn.execute(
        "UPDATE profiles SET custom_job_titles = ? WHERE name = ?",
        (_json.dumps(titles), name),
    )
    conn.commit()


# ---------------------------------------------------------------------------
# Job helpers
# ---------------------------------------------------------------------------

def insert_job(conn: sqlite3.Connection, job: dict) -> bool:
    """Insert a job if it doesn't already exist. Returns True if inserted.

    If job contains 'fetched_for_profile_id', it is stored on the row.
    INSERT OR IGNORE means a duplicate job keeps the original value (first
    profile to discover the job wins).
    """
    try:
        cursor = conn.execute(
            """
            INSERT OR IGNORE INTO jobs
              (job_key, title, company, location, remote_type,
               salary_min, salary_max, salary_currency, salary_period, salary_raw,
               description, apply_url, source, source_company_slug,
               date_posted, raw_data, fetched_for_profile_id)
            VALUES
              (:job_key, :title, :company, :location, :remote_type,
               :salary_min, :salary_max, :salary_currency, :salary_period, :salary_raw,
               :description, :apply_url, :source, :source_company_slug,
               :date_posted, :raw_data, :fetched_for_profile_id)
            """,
            {**job, "fetched_for_profile_id": job.get("fetched_for_profile_id")},
        )
        inserted = cursor.rowcount > 0
        conn.commit()
        return inserted
    except sqlite3.IntegrityError:
        return False


def delete_expired_jobs(conn: sqlite3.Connection, retention_days: int) -> int:
    """Delete jobs older than retention_days that are not referenced by any profile."""
    cursor = conn.execute(
        """
        DELETE FROM jobs
        WHERE date_found < datetime('now', ?)
          AND job_key NOT IN (SELECT job_key FROM profile_jobs)
        """,
        (f"-{retention_days} days",),
    )
    deleted = cursor.rowcount
    conn.commit()
    return deleted


def get_unscored_jobs(
    conn: sqlite3.Connection, profile_id: int, retention_days: int = 30
) -> list[sqlite3.Row]:
    """Return fresh jobs (within retention_days) not yet scored for this profile."""
    return conn.execute(
        """
        SELECT j.*
        FROM jobs j
        WHERE j.job_key NOT IN (
            SELECT job_key FROM profile_jobs WHERE profile_id = ?
        )
        AND (
            CASE WHEN j.date_posted GLOB '[0-9][0-9][0-9][0-9]-[0-9][0-9]-[0-9][0-9]'
                 THEN j.date_posted
                 ELSE DATE(j.date_found)
            END >= DATE('now', ?)
        )
        ORDER BY j.date_found DESC
        """,
        (profile_id, f"-{retention_days} days"),
    ).fetchall()


def insert_profile_job(
    conn: sqlite3.Connection,
    profile_id: int,
    job_key: str,
    match_score: float,
    match_notes: str,
    rank_at_discovery: int | None,
    hidden: bool = False,
    manager_score: int | None = None,
    candidate_score: int | None = None,
    candidate_notes: str | None = None,
    match_pairs_json: str | None = None,
    application_status: str = "new",
    total_job_requirements: int | None = None,
    ai_raw_response: str | None = None,
) -> bool:
    """
    Insert a scored job into profile_jobs. Returns True if inserted (not duplicate).
    hidden=True for jobs that were scored but didn't qualify (below threshold / outside top-N)
    or were discarded by the hard-requirement pre-filter.
    These are still tracked so they aren't re-scored on subsequent runs.
    match_pairs_json         — JSON-serialised list of match_pair dicts from the ranker.
    total_job_requirements   — count of all job requirements reported by Claude; used as the
                               manager depth denominator so --repromote can re-calculate scores.
    application_status       — defaults to 'new'; use 'missing_info' for jobs with no usable description.
    ai_raw_response          — raw text returned by the AI for the scoring batch that included this job.
    """
    try:
        cursor = conn.execute(
            """
            INSERT OR IGNORE INTO profile_jobs
              (profile_id, job_key, match_score, match_notes, rank_at_discovery, hidden,
               manager_score, candidate_score, candidate_notes, match_pairs_json,
               application_status, total_job_requirements, ai_raw_response)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (profile_id, job_key, match_score, match_notes, rank_at_discovery, hidden,
             manager_score, candidate_score, candidate_notes, match_pairs_json,
             application_status, total_job_requirements, ai_raw_response),
        )
        inserted = cursor.rowcount > 0
        conn.commit()
        return inserted
    except sqlite3.IntegrityError:
        return False


def promote_profile_job(
    conn: sqlite3.Connection,
    profile_id: int,
    job_key: str,
    rank_at_discovery: int,
) -> bool:
    """
    Promote a previously hidden profile_job to visible.
    Called after all batches are scored to surface the top-N qualifying jobs.
    Returns True if a row was updated.

    User actions always take priority: never promotes a job the user has
    explicitly dismissed (status_updated_at IS NOT NULL) or that has a
    non-'new' application status (actioned/rejected/withdrawn).
    """
    cursor = conn.execute(
        """UPDATE profile_jobs SET hidden=FALSE, rank_at_discovery=?
           WHERE profile_id=? AND job_key=?
             AND application_status = 'new'
             AND (hidden = FALSE OR status_updated_at IS NULL)""",
        (rank_at_discovery, profile_id, job_key),
    )
    conn.commit()
    return cursor.rowcount > 0


def rebalance_visible_jobs(
    conn: sqlite3.Connection,
    profile_id: int,
    top_n_display: int,
    min_match_score: float = 0.4,
) -> dict:
    """
    Enforce the top_n_display cap on the top-N list (the 'new'-status portion of
    the recommended list).

    The recommended list is conceptually two sub-lists:
      1. Top-N list   — up to top_n_display jobs with application_status='new',
                        ranked by score.  This function manages this list.
      2. User-action list — jobs with actioned statuses (saved, applied, interviews,
                            offer).  Always visible, not counted toward top_n_display,
                            never touched by this function.

    Eligibility for the top-N pool (jobs that can be promoted/demoted here):
      - application_status = 'new'
      - manager_score IS NOT NULL  (AI-scored)
      - Not hard/title-filtered    (match_notes NOT LIKE '[Hard filter/Title filter]%')
      - Not user-dismissed         (hidden=TRUE AND status_updated_at IS NOT NULL)
        i.e. only currently-visible jobs OR pipeline-hidden jobs (status_updated_at IS NULL)

    After rebalancing, the top top_n_display qualifying jobs (match_score >=
    min_match_score) are visible; the rest are hidden.  rank_at_discovery is
    re-stamped to reflect global ranking.

    Returns a dict with 'promoted' and 'demoted' counts.
    """
    scored = conn.execute(
        """
        SELECT job_key, match_score, manager_score, candidate_score
        FROM profile_jobs
        WHERE profile_id = ?
          AND application_status = 'new'
          AND COALESCE(saved, 0) = 0
          AND manager_score IS NOT NULL
          -- exclude hard/title-filter rejections
          AND (match_notes IS NULL
               OR (match_notes NOT LIKE '[Hard filter]%'
                   AND match_notes NOT LIKE '[Title filter]%'))
          -- exclude jobs the user explicitly dismissed (status_updated_at set by hide action)
          AND (hidden = FALSE OR status_updated_at IS NULL)
        ORDER BY (COALESCE(manager_score, 0) + COALESCE(candidate_score, 0)) DESC,
                 COALESCE(manager_score, 0) DESC,
                 COALESCE(candidate_score, 0) DESC
        """,
        (profile_id,),
    ).fetchall()

    qualifying = [r for r in scored if (r["match_score"] or 0) >= min_match_score]
    top_keys   = {r["job_key"] for r in qualifying[:top_n_display]}
    all_keys   = {r["job_key"] for r in scored}
    hide_keys  = all_keys - top_keys

    promoted = demoted = 0
    if top_keys:
        placeholders = ",".join("?" * len(top_keys))
        cur = conn.execute(
            f"UPDATE profile_jobs SET hidden=FALSE"
            f" WHERE profile_id=? AND job_key IN ({placeholders})"
            f"   AND hidden=TRUE AND status_updated_at IS NULL",  # never un-hide dismissed jobs
            [profile_id, *top_keys],
        )
        promoted = cur.rowcount

    if hide_keys:
        placeholders = ",".join("?" * len(hide_keys))
        cur = conn.execute(
            # Also clear status_updated_at so pipeline-demoted jobs remain eligible
            # for future runs.  Without this, a job that had its status changed and
            # back to 'new' (status_updated_at IS NOT NULL) would be treated as
            # user-dismissed after demotion and never surface again.
            f"UPDATE profile_jobs SET hidden=TRUE, status_updated_at=NULL"
            f" WHERE profile_id=? AND job_key IN ({placeholders})"
            f"   AND hidden=FALSE AND application_status='new'",  # never hide actioned jobs
            [profile_id, *hide_keys],
        )
        demoted = cur.rowcount

    # Re-stamp rank_at_discovery for the visible list to reflect global ranking.
    for rank_pos, row in enumerate(qualifying[:top_n_display], start=1):
        conn.execute(
            "UPDATE profile_jobs SET rank_at_discovery=? WHERE profile_id=? AND job_key=?",
            (rank_pos, profile_id, row["job_key"]),
        )

    conn.commit()
    return {"promoted": promoted, "demoted": demoted}


def get_scored_profile_jobs(
    conn: sqlite3.Connection, profile_id: int, retention_days: int = 30
) -> list[sqlite3.Row]:
    """
    Return AI-scored profile_jobs within the retention window, eligible for
    (re-)promotion.  Excludes:
      - Hard/title-filter rejections
      - User-dismissed jobs  (hidden=TRUE AND status_updated_at IS NOT NULL)
      - Non-'new' application statuses (rejected, withdrawn, saved, applied, etc.)
    """
    return conn.execute(
        """
        SELECT pj.job_key, pj.match_score, pj.manager_score, pj.candidate_score,
               pj.match_pairs_json, pj.total_job_requirements
        FROM profile_jobs pj
        JOIN jobs j ON pj.job_key = j.job_key
        WHERE pj.profile_id = ?
          AND pj.match_notes NOT LIKE '[Hard filter]%'
          AND pj.match_notes NOT LIKE '[Title filter]%'
          AND pj.application_status = 'new'
          AND (pj.hidden = FALSE OR pj.status_updated_at IS NULL)
          AND (
              CASE WHEN j.date_posted GLOB '[0-9][0-9][0-9][0-9]-[0-9][0-9]-[0-9][0-9]'
                   THEN j.date_posted
                   ELSE DATE(j.date_found)
              END >= DATE('now', ?)
          )
        """,
        (profile_id, f"-{retention_days} days"),
    ).fetchall()


def update_profile_job_scores(
    conn: sqlite3.Connection,
    profile_id: int,
    job_key: str,
    manager_score: int,
    candidate_score: int,
    match_score: float,
) -> None:
    """Update the computed scores for an existing profile_jobs row (used by --repromote)."""
    conn.execute(
        """UPDATE profile_jobs
           SET manager_score=?, candidate_score=?, match_score=?
           WHERE profile_id=? AND job_key=?""",
        (manager_score, candidate_score, match_score, profile_id, job_key),
    )
    conn.commit()


def reset_promotion(conn: sqlite3.Connection, profile_id: int) -> int:
    """
    Hide all 'new' visible jobs so the promotion step can be re-run cleanly.

    Also clears status_updated_at so every previously-visible 'new' job becomes
    pipeline-eligible again.  Without this, jobs whose status was ever changed and
    set back to 'new' via update_status (status_updated_at IS NOT NULL) would look
    like user-dismissed jobs after being hidden, and get_scored_profile_jobs /
    promote_profile_job would permanently exclude them.
    """
    cursor = conn.execute(
        "UPDATE profile_jobs SET hidden=TRUE, rank_at_discovery=NULL, status_updated_at=NULL "
        "WHERE profile_id=? AND hidden=FALSE AND application_status='new'",
        (profile_id,),
    )
    conn.commit()
    return cursor.rowcount


def get_all_jobs(conn: sqlite3.Connection) -> list[sqlite3.Row]:
    """Return every job in the database (used by refilter mode)."""
    return conn.execute("SELECT * FROM jobs ORDER BY date_found DESC").fetchall()


def get_profile_jobs_full(conn: sqlite3.Connection, profile_id: int) -> list[sqlite3.Row]:
    """Return all jobs associated with a specific profile (joined from jobs + profile_jobs)."""
    return conn.execute(
        """
        SELECT j.*
        FROM jobs j
        JOIN profile_jobs pj ON j.job_key = pj.job_key
        WHERE pj.profile_id = ?
        ORDER BY j.date_found DESC
        """,
        (profile_id,),
    ).fetchall()


def refilter_profile_job(
    conn: sqlite3.Connection,
    profile_id: int,
    job_key: str,
    reason: str,
) -> str:
    """
    Mark a job as rejected by the hard filter.
    - Returns 'skipped' if the job has a user action (saved/applied/etc.) — never touch those.
    - Returns 'updated' if an existing profile_jobs row was updated.
    - Returns 'inserted' if a new row was inserted.
    """
    existing = conn.execute(
        "SELECT application_status, saved, match_notes, hidden FROM profile_jobs WHERE profile_id=? AND job_key=?",
        (profile_id, job_key),
    ).fetchone()

    note = f"[Hard filter] {reason}"

    if existing:
        if existing["application_status"] != "new" or existing["saved"]:
            return "skipped"
        # Already correctly hard-filtered — nothing to do
        if (existing["match_notes"] or "").startswith("[Hard filter]") and existing["hidden"]:
            return "already_filtered"
        conn.execute(
            """UPDATE profile_jobs
               SET hidden=TRUE, match_score=0.0, match_notes=?,
                   manager_score=NULL, candidate_score=NULL, rank_at_discovery=NULL
               WHERE profile_id=? AND job_key=?""",
            (note, profile_id, job_key),
        )
        conn.commit()
        return "updated"
    else:
        conn.execute(
            """INSERT OR IGNORE INTO profile_jobs
               (profile_id, job_key, match_score, match_notes, hidden)
               VALUES (?, ?, 0.0, ?, TRUE)""",
            (profile_id, job_key, note),
        )
        conn.commit()
        return "inserted"


def unfilter_profile_job(conn: sqlite3.Connection, profile_id: int, job_key: str) -> bool:
    """
    Remove a [Hard filter] entry so the job is re-evaluated on the next pipeline run.
    Only removes rows whose notes start with '[Hard filter]' — leaves scored rows alone.
    Returns True if a row was removed.
    """
    cursor = conn.execute(
        """DELETE FROM profile_jobs
           WHERE profile_id=? AND job_key=? AND match_notes LIKE '[Hard filter]%'""",
        (profile_id, job_key),
    )
    conn.commit()
    return cursor.rowcount > 0


def purge_stale_profile_jobs(conn: sqlite3.Connection, profile_id: int) -> int:
    """
    On profile update: remove rows that the user hasn't acted on.
    - hidden rows: re-score against new profile
    - 'new' status: user hasn't expressed any interest yet
    Preserves 'saved' and beyond — user has expressed intent, keep regardless of profile change.
    User-actioned jobs (saved/applied/interviews/offer) are NEVER deleted, even if hidden=TRUE.
    """
    cursor = conn.execute(
        """
        DELETE FROM profile_jobs
        WHERE profile_id = ?
          AND (hidden = TRUE OR application_status = 'new')
          AND COALESCE(saved, 0) = 0
          AND application_status NOT IN (
              'applied', 'phone_screen',
              'interview_1', 'interview_2', 'interview_3', 'offer'
          )
        """,
        (profile_id,),
    )
    deleted = cursor.rowcount
    conn.commit()
    return deleted


def get_last_run_at(conn: sqlite3.Connection, profile_id: int) -> str | None:
    """Return ISO timestamp of the last successful pipeline run for this profile, or None."""
    row = conn.execute(
        """
        SELECT finished_at FROM search_runs
        WHERE profile_id = ? AND status = 'success'
        ORDER BY started_at DESC
        LIMIT 1
        """,
        (profile_id,),
    ).fetchone()
    return row["finished_at"] if row else None


# ---------------------------------------------------------------------------
# ATS companies helpers
# ---------------------------------------------------------------------------

def get_ats_companies(conn: sqlite3.Connection, ats: str) -> list[sqlite3.Row]:
    return conn.execute(
        "SELECT * FROM ats_companies WHERE ats = ?", (ats,)
    ).fetchall()


def get_all_ats_slugs(conn: sqlite3.Connection) -> dict:
    """Return dict mapping company_lower → slug for all known ATS companies."""
    rows = conn.execute("SELECT company, slug FROM ats_companies").fetchall()
    result = {}
    for row in rows:
        result[row["company"].lower()] = row["slug"]
        result[row["slug"].lower()] = row["slug"]
    return result


def upsert_ats_company(
    conn: sqlite3.Connection, company: str, ats: str, slug: str
) -> None:
    conn.execute(
        "INSERT OR IGNORE INTO ats_companies (company, ats, slug) VALUES (?, ?, ?)",
        (company, ats, slug),
    )
    conn.commit()


# ---------------------------------------------------------------------------
# Backfill helpers
# ---------------------------------------------------------------------------

def get_missing_info_jobs(conn: sqlite3.Connection, profile_id: int) -> list[sqlite3.Row]:
    """
    Return all jobs flagged as 'missing_info' for this profile.
    These are jobs whose description was absent or too short to score at ingest time.
    Used by --fetch-missing to attempt description retrieval from apply_url.
    """
    return conn.execute(
        """
        SELECT j.job_key, j.title, j.company, j.location, j.remote_type,
               j.salary_min, j.salary_max, j.salary_currency, j.salary_period, j.salary_raw,
               j.description, j.apply_url, j.source, j.source_company_slug,
               j.date_posted, j.date_found
        FROM profile_jobs pj
        JOIN jobs j ON pj.job_key = j.job_key
        WHERE pj.profile_id = ? AND pj.application_status = 'missing_info'
        ORDER BY j.date_found DESC
        """,
        (profile_id,),
    ).fetchall()


def delete_profile_job(conn: sqlite3.Connection, profile_id: int, job_key: str) -> bool:
    """
    Delete a single profile_jobs row so the ranker treats the job as unscored.
    Returns True if a row was deleted.
    Only used for 'missing_info' rows after a successful description fetch —
    these rows contain no user data worth preserving.
    """
    cursor = conn.execute(
        "DELETE FROM profile_jobs WHERE profile_id=? AND job_key=? AND application_status='missing_info'",
        (profile_id, job_key),
    )
    conn.commit()
    return cursor.rowcount > 0


def get_greenhouse_jobs_for_backfill(conn: sqlite3.Connection, profile_id: int) -> list[sqlite3.Row]:
    """
    Return Greenhouse jobs linked to this profile that have empty descriptions
    and haven't been actioned by the user (status = 'new').
    Used by backfill mode to find candidates for description enrichment.
    """
    return conn.execute(
        """
        SELECT j.job_key, j.title, j.company, j.location, j.remote_type,
               j.salary_min, j.salary_max, j.salary_currency, j.salary_period, j.salary_raw,
               j.description, j.apply_url, j.source, j.source_company_slug,
               j.date_posted, j.raw_data
        FROM jobs j
        JOIN profile_jobs pj ON j.job_key = pj.job_key
        WHERE j.source = 'greenhouse'
          AND (j.description IS NULL OR j.description = '')
          AND pj.profile_id = ?
          AND pj.application_status = 'new'
        ORDER BY j.date_found DESC
        """,
        (profile_id,),
    ).fetchall()


def update_job_description(conn: sqlite3.Connection, job_key: str, description: str, raw_data: str) -> None:
    """Overwrite the description (and raw_data) for a job row."""
    conn.execute(
        "UPDATE jobs SET description=?, raw_data=? WHERE job_key=?",
        (description, raw_data, job_key),
    )
    conn.commit()


def reset_scores_for_backfill(conn: sqlite3.Connection, profile_id: int, job_keys: list[str]) -> int:
    """
    Clear AI scores for the given jobs so they are re-evaluated by the ranker.
    Only touches rows with status='new' — never resets user-actioned jobs.
    Returns the number of rows reset.
    """
    if not job_keys:
        return 0
    placeholders = ",".join("?" * len(job_keys))
    cursor = conn.execute(
        f"""
        UPDATE profile_jobs
        SET manager_score=NULL, candidate_score=NULL,
            match_notes=NULL, candidate_notes=NULL,
            match_score=0.0, hidden=FALSE, rank_at_discovery=NULL
        WHERE profile_id=?
          AND application_status='new'
          AND (hidden = FALSE OR status_updated_at IS NULL)
          AND job_key IN ({placeholders})
        """,
        [profile_id, *job_keys],
    )
    conn.commit()
    return cursor.rowcount


# ---------------------------------------------------------------------------
# Clear helpers
# ---------------------------------------------------------------------------

def clear_all_jobs(conn: sqlite3.Connection) -> dict:
    """
    Delete all job data while preserving profiles and ATS company data.
    Clears: profile_jobs, jobs, search_runs.
    Keeps:  profiles, ats_companies, schema_version.
    Returns counts of deleted rows per table.
    """
    pj = conn.execute("DELETE FROM profile_jobs").rowcount
    j  = conn.execute("DELETE FROM jobs").rowcount
    sr = conn.execute("DELETE FROM search_runs").rowcount
    conn.commit()
    return {"profile_jobs": pj, "jobs": j, "search_runs": sr}


def clear_profile_jobs(conn: sqlite3.Connection, profile_id: int) -> dict:
    """
    Delete all scoring/recommendation data for a single profile.
    Clears: profile_jobs rows and search_runs rows for this profile.
    Jobs that are no longer referenced by any profile_jobs row are also deleted.
    Keeps:  the profile itself, ats_companies, and jobs still used by other profiles.
    Returns counts of deleted rows per table.
    """
    pj = conn.execute(
        "DELETE FROM profile_jobs WHERE profile_id = ?", (profile_id,)
    ).rowcount
    sr = conn.execute(
        "DELETE FROM search_runs WHERE profile_id = ?", (profile_id,)
    ).rowcount
    # Remove orphaned jobs (not referenced by any other profile)
    j = conn.execute(
        "DELETE FROM jobs WHERE job_key NOT IN (SELECT DISTINCT job_key FROM profile_jobs)"
    ).rowcount
    conn.commit()
    return {"profile_jobs": pj, "jobs": j, "search_runs": sr}


def delete_profile(conn: sqlite3.Connection, profile_id: int) -> dict:
    """
    Fully delete a profile and all its associated data.
    Clears: profile_jobs, search_runs, orphaned jobs, and the profile row itself.
    Keeps:  ats_companies and jobs still used by other profiles.
    Returns counts of deleted rows per table.
    """
    pj = conn.execute(
        "DELETE FROM profile_jobs WHERE profile_id = ?", (profile_id,)
    ).rowcount
    sr = conn.execute(
        "DELETE FROM search_runs WHERE profile_id = ?", (profile_id,)
    ).rowcount
    j = conn.execute(
        "DELETE FROM jobs WHERE job_key NOT IN (SELECT DISTINCT job_key FROM profile_jobs)"
    ).rowcount
    p = conn.execute(
        "DELETE FROM profiles WHERE id = ?", (profile_id,)
    ).rowcount
    conn.commit()
    return {"profile_jobs": pj, "jobs": j, "search_runs": sr, "profiles": p}


# ---------------------------------------------------------------------------
# Search run helpers
# ---------------------------------------------------------------------------

def insert_search_run(conn: sqlite3.Connection, run: dict) -> int:
    c = conn.execute(
        """
        INSERT INTO search_runs
          (profile_id, triggered_by, sources_used, jobs_found, jobs_added,
           jsearch_credits, status, error_message, finished_at)
        VALUES
          (:profile_id, :triggered_by, :sources_used, :jobs_found, :jobs_added,
           :jsearch_credits, :status, :error_message, :finished_at)
        """,
        run,
    )
    conn.commit()
    return c.lastrowid
