"""
pipeline.py — Main orchestrator. Runs the full job search pipeline for one profile.

Search source hierarchy (per run):
  1. JSearch   — company-targeted queries (required + preferred), within quota budget
  2. Greenhouse/Lever — all known ATS company boards (free, unlimited)
  3. JobSpy    — general keyword discovery (always runs, no company targeting)
"""

import argparse
import json
import logging
import logging.handlers
import os
import sys
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import yaml
from dotenv import load_dotenv

PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

load_dotenv(PROJECT_ROOT / ".env")

import src.database as database
import src.deduplicator as deduplicator
import src.profile_processor as profile_processor
from src.salary_parser import parse_salary
import src.query_builder as query_builder
import src.ranker as ranker
from src import ai_client
from src.quota_tracker import QuotaTracker
from src.sources.greenhouse import GreenhouseSource
from src.sources.jsearch import JSearchSource, QuotaExhaustedException
from src.sources.jobspy_source import JobSpySource
from src.sources.lever import LeverSource


def load_config() -> dict:
    config_path = PROJECT_ROOT / "config" / "config.yaml"
    with open(config_path, encoding="utf-8") as f:
        return yaml.safe_load(f)


def setup_logging(config: dict) -> None:
    log_cfg = config.get("logging", {})
    level = getattr(logging, log_cfg.get("level", "INFO").upper(), logging.INFO)
    log_file = PROJECT_ROOT / log_cfg.get("file", "logs/pipeline.log")
    log_file.parent.mkdir(parents=True, exist_ok=True)

    handlers = [logging.StreamHandler(sys.stdout)]
    try:
        handlers.append(
            logging.handlers.TimedRotatingFileHandler(
                log_file,
                when="midnight",
                backupCount=7,  # keep last 7 days
                encoding="utf-8",
            )
        )
    except Exception:
        pass

    logging.basicConfig(
        level=level,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        handlers=handlers,
    )


def load_preferred_companies(config: dict, profile: dict | None = None) -> list[str]:
    """Return the preferred-companies list for a profile run.

    The list is read exclusively from the profile's ``preferred_companies`` field,
    which is managed via the Profile page in the web UI.  Use the "Import from
    global list" button there to migrate from the legacy .txt file.
    """
    if profile:
        pc = profile.get("preferred_companies") or []
        return [str(c).strip() for c in pc if str(c).strip()]
    return []


def _determine_search_mode(conn, profile_id: int, config: dict, force_full: bool) -> tuple[str, int]:
    """
    Returns (mode, jobspy_hours_old).
    mode: "full" or "incremental"
    """
    if force_full:
        return "full", 720

    override = config.get("search_mode", "auto")
    if override == "full":
        return "full", 720
    if override == "incremental":
        # hours since last run + 24h buffer
        last_run = database.get_last_run_at(conn, profile_id)
        hours_old = _hours_since(last_run) + 24 if last_run else 720
        return "incremental", min(hours_old, 720)

    # auto: check last run timestamp
    last_run = database.get_last_run_at(conn, profile_id)
    if not last_run:
        return "full", 720

    hours_elapsed = _hours_since(last_run)
    if hours_elapsed > 30 * 24:  # > 30 days
        return "full", 720

    return "incremental", min(hours_elapsed + 24, 720)


def _hours_since(iso_timestamp: str | None) -> int:
    if not iso_timestamp:
        return 999999
    try:
        dt = datetime.fromisoformat(iso_timestamp.replace("Z", "+00:00"))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        elapsed = datetime.now(tz=timezone.utc) - dt
        return int(elapsed.total_seconds() / 3600)
    except Exception:
        return 999999


def _filter_gh_lever_by_date(
    jobs: list[dict],
    last_run_iso: str | None,
    max_age_days: int = 30,
) -> list[dict]:
    """
    Client-side date filter for Greenhouse/Lever results (they have no server-side filter).
    Always applies a hard max_age_days cap (default 30 days) so stale postings are excluded
    even on a full run.  In incremental mode the cutoff is further tightened to last_run_iso
    so only genuinely new postings are processed.
    Jobs with an unparseable or missing date_posted are kept (date unknown → assume fresh).
    """
    from datetime import timedelta
    age_cutoff = datetime.now(timezone.utc) - timedelta(days=max_age_days)

    cutoff = age_cutoff
    if last_run_iso:
        try:
            last_run_dt = datetime.fromisoformat(last_run_iso.replace("Z", "+00:00"))
            if last_run_dt.tzinfo is None:
                last_run_dt = last_run_dt.replace(tzinfo=timezone.utc)
            cutoff = max(age_cutoff, last_run_dt)
        except Exception:
            pass

    filtered = []
    for job in jobs:
        date_posted = job.get("date_posted")
        if not date_posted:
            filtered.append(job)  # keep if unknown
            continue
        try:
            dp = datetime.fromisoformat(date_posted)
            if dp.tzinfo is None:
                dp = dp.replace(tzinfo=timezone.utc)
            if dp >= cutoff:
                filtered.append(job)
        except Exception:
            filtered.append(job)  # keep if unparseable

    return filtered


def _ensure_ideal_cand_pairs(conn, profile_row, profile: dict, logger) -> int:
    """
    Return ideal_cand_pairs for this profile.
    Loads from DB if stored; otherwise computes from the profile and persists it.
    """
    stored = profile_row["ideal_cand_pairs"]
    if stored is not None:
        logger.debug(f"ideal_cand_pairs loaded from DB: {stored}")
        return stored
    computed = ranker.get_ideal_cand_pairs(profile)
    database.update_ideal_cand_pairs(conn, profile_row["id"], computed)
    logger.info(f"ideal_cand_pairs not in DB — computed and stored: {computed}")
    return computed


def run_fetch_jobs(profile_name: str, force_full: bool = False) -> dict:
    """
    Fetch-jobs mode: run all source fetching and store results in the DB.
    Stops before AI scoring and promotion — no Claude calls, no quota used.
    Use --score (or a normal pipeline run) afterwards to score the stored jobs.
    """
    config = load_config()
    setup_logging(config)
    logger = logging.getLogger("pipeline")

    db_path = PROJECT_ROOT / config["database"]["path"]
    profiles_dir = PROJECT_ROOT / config.get("profiles_dir", "profiles")
    profiles_dir.mkdir(parents=True, exist_ok=True)

    conn = database.init_db(db_path)
    logger.info(f"=== Fetch-jobs start: profile='{profile_name}' ===")

    # Step 1: Check/reset JSearch quota
    reset_day = config.get("api", {}).get("jsearch_reset_day", 1)
    qt = QuotaTracker(conn)
    qt.ensure_initialized(monthly_limit=200, reset_day=reset_day)
    qt.check_and_reset()
    logger.info(f"JSearch quota: {qt.remaining()} remaining, cycle ends {qt.cycle_end()}")

    # Step 2: Delete expired jobs
    retention_days = config.get("job_retention_days", 30)
    deleted = database.delete_expired_jobs(conn, retention_days)
    if deleted:
        logger.info(f"Deleted {deleted} expired jobs (>{retention_days} days old, unreferenced).")

    # Step 3: Profile processing
    try:
        proc_result = profile_processor.process(conn, profile_name, profiles_dir)
    except FileNotFoundError as e:
        logger.error(str(e))
        return {"status": "failed", "error": str(e)}

    profile_id = proc_result["profile_id"]
    profile_changed = proc_result["profile_changed"]
    structured = proc_result["structured_content"]

    if profile_changed:
        logger.info(f"Profile '{profile_name}' changed — purging stale recommendations...")
        purged = database.purge_stale_profile_jobs(conn, profile_id)
        logger.info(f"Purged {purged} stale profile_jobs rows (new status + hidden).")
        ideal_cand_pairs = ranker.get_ideal_cand_pairs(structured)
        database.update_ideal_cand_pairs(conn, profile_id, ideal_cand_pairs)

    # Step 4: Determine search mode (full vs incremental)
    search_mode, jobspy_hours_old = _determine_search_mode(conn, profile_id, config, force_full)
    jsearch_date_posted = "month" if search_mode == "full" else "week"
    last_run_iso = database.get_last_run_at(conn, profile_id) if search_mode == "incremental" else None
    logger.info(f"Search mode: {search_mode} (JSearch date_posted={jsearch_date_posted})")

    # Step 5: Build search plan
    jsearch_budget = config.get("jsearch_queries_per_run", 10)
    preferred_companies = load_preferred_companies(config, structured)
    search_plan = query_builder.build_search_plan(
        conn=conn,
        profile=structured,
        preferred_companies=preferred_companies,
        jsearch_budget=jsearch_budget,
    )
    jsearch_queries = search_plan["jsearch_queries"]
    ats_slugs = search_plan["ats_slugs"]
    jobspy_queries = search_plan["jobspy_queries"]
    companies_not_covered = search_plan["companies_not_covered"]

    all_raw_jobs: list[dict] = []
    sources_used: list[str] = []
    jsearch_credits_used = 0

    # Step 6: Greenhouse
    if config.get("sources", {}).get("greenhouse", True) and ats_slugs:
        gh_slugs = [
            row["slug"]
            for row in conn.execute(
                "SELECT slug FROM ats_companies WHERE ats='greenhouse' AND slug IN ({})".format(
                    ",".join("?" * len(ats_slugs))
                ),
                ats_slugs,
            ).fetchall()
        ]
        if gh_slugs:
            logger.info(f"[greenhouse] Fetching {len(gh_slugs)} slugs...")
            gh = GreenhouseSource()
            gh_jobs = gh.fetch_many(gh_slugs)
            gh_jobs = _filter_gh_lever_by_date(gh_jobs, last_run_iso, max_age_days=retention_days)
            all_raw_jobs.extend(gh_jobs)
            if gh_jobs:
                sources_used.append("greenhouse")
            logger.info(f"[greenhouse] {len(gh_jobs)} jobs after date filter.")

    # Step 7: Lever
    if config.get("sources", {}).get("lever", True) and ats_slugs:
        lv_slugs = [
            row["slug"]
            for row in conn.execute(
                "SELECT slug FROM ats_companies WHERE ats='lever' AND slug IN ({})".format(
                    ",".join("?" * len(ats_slugs))
                ),
                ats_slugs,
            ).fetchall()
        ]
        if lv_slugs:
            logger.info(f"[lever] Fetching {len(lv_slugs)} slugs...")
            lv = LeverSource()
            lv_jobs = lv.fetch_many(lv_slugs)
            lv_jobs = _filter_gh_lever_by_date(lv_jobs, last_run_iso, max_age_days=retention_days)
            all_raw_jobs.extend(lv_jobs)
            if lv_jobs:
                sources_used.append("lever")
            logger.info(f"[lever] {len(lv_jobs)} jobs after date filter.")

    # Step 8: JSearch
    if jsearch_queries and config.get("sources", {}).get("jsearch", True):
        if qt.is_available():
            logger.info(f"[jsearch] Running {len(jsearch_queries)} queries (mode={search_mode})...")
            js = JSearchSource()
            try:
                js_jobs = js.fetch_many(jsearch_queries, quota_tracker=qt, date_posted=jsearch_date_posted)
                all_raw_jobs.extend(js_jobs)
                if js_jobs:
                    sources_used.append("jsearch")
                jsearch_credits_used = len(jsearch_queries)
                for ats, slug in js.discovered_slugs:
                    database.upsert_ats_company(conn, slug, ats, slug)
                logger.info(
                    f"[jsearch] {len(js_jobs)} jobs fetched. "
                    f"{len(js.discovered_slugs)} new ATS slugs discovered."
                )
            except QuotaExhaustedException:
                logger.warning("[jsearch] Quota exhausted mid-run.")
        else:
            logger.warning("[jsearch] Quota unavailable — skipping JSearch queries.")

    # Step 9: JobSpy
    if config.get("sources", {}).get("jobspy", True) and jobspy_queries:
        logger.info(f"[jobspy] Running {len(jobspy_queries)} general queries (hours_old={jobspy_hours_old})...")
        spy_cfg = config.get("jobspy", {})
        spy = JobSpySource(
            sites=spy_cfg.get("sites", ["linkedin", "indeed"]),
            results_per_site=spy_cfg.get("results_per_site", 25),
        )
        spy_jobs = spy.fetch_many(jobspy_queries, hours_old=jobspy_hours_old)
        all_raw_jobs.extend(spy_jobs)
        if spy_jobs:
            sources_used.append("jobspy")
        logger.info(f"[jobspy] {len(spy_jobs)} jobs fetched.")

    # Step 10a: Enrich missing salary fields from description text
    preferred_locations = (structured.get("preferred_locations") or [])
    for job in all_raw_jobs:
        if job.get("salary_min") is None and job.get("salary_max") is None:
            desc = job.get("description") or ""
            lo, hi = parse_salary(desc, preferred_locations)
            if lo:
                job["salary_min"] = lo
                job["salary_max"] = hi if (hi and hi != lo) else None

    # Step 10b: Validate descriptions; fetch from apply_url when missing
    _validate_and_enrich_raw_jobs(all_raw_jobs, conn, logger)

    # Step 10: Deduplicate and store
    total_processed, new_inserted = deduplicator.process(conn, all_raw_jobs, profile_id=profile_id)
    logger.info(f"Dedup: {total_processed} processed, {new_inserted} new jobs in DB.")

    # Step 10c: Record missing_info profile_jobs for jobs that still lack descriptions
    missing_info_count = 0
    for job in all_raw_jobs:
        if not job.get("_missing_info") or not job.get("job_key"):
            continue
        inserted_mi = database.insert_profile_job(
            conn,
            profile_id=profile_id,
            job_key=job["job_key"],
            match_score=0.0,
            match_notes=(
                "[Missing info] Job description absent or too short to evaluate. "
                "Check the apply link for full details."
            ),
            rank_at_discovery=None,
            hidden=True,
            application_status="missing_info",
        )
        if inserted_mi:
            missing_info_count += 1
    if missing_info_count:
        logger.info(f"[ingest-validate] {missing_info_count} jobs recorded as missing_info.")

    # Write audit record (jobs_added = 0 since no scoring/promotion was run)
    finished_at = datetime.now(tz=timezone.utc).isoformat()
    database.insert_search_run(conn, {
        "profile_id": profile_id,
        "triggered_by": "fetch-jobs",
        "sources_used": json.dumps(sources_used),
        "jobs_found": total_processed,
        "jobs_added": 0,
        "jsearch_credits": jsearch_credits_used,
        "status": "success",
        "error_message": None,
        "finished_at": finished_at,
    })

    conn.close()

    unscored_count = new_inserted  # all newly inserted jobs are unscored
    logger.info(
        f"=== Fetch-jobs complete ===\n"
        f"  Search mode    : {search_mode}\n"
        f"  Sources used   : {', '.join(sources_used) or 'none'}\n"
        f"  Jobs fetched   : {total_processed} ({new_inserted} new in DB)\n"
        f"  Ready to score : {unscored_count} (run --score to evaluate)\n"
    )
    return {
        "status": "success",
        "profile": profile_name,
        "search_mode": search_mode,
        "sources_used": sources_used,
        "jobs_fetched": total_processed,
        "jobs_new_in_db": new_inserted,
        "jsearch_credits_used": jsearch_credits_used,
    }


def run_pipeline(profile_name: str, triggered_by: str = "manual", force_full: bool = False, score_limit: int | None = None, progress_callback=None) -> dict:
    """
    Run the full pipeline for one profile.
    Returns a summary dict.
    """
    config = load_config()
    setup_logging(config)
    logger = logging.getLogger("pipeline")

    db_path = PROJECT_ROOT / config["database"]["path"]
    profiles_dir = PROJECT_ROOT / config.get("profiles_dir", "profiles")
    profiles_dir.mkdir(parents=True, exist_ok=True)

    conn = database.init_db(db_path)
    started_at = datetime.now(tz=timezone.utc).isoformat()

    ai_client.reset_usage()
    logger.info(f"=== Pipeline start: profile='{profile_name}', triggered_by='{triggered_by}' ===")

    # Step 1: Check/reset JSearch quota
    reset_day = config.get("api", {}).get("jsearch_reset_day", 1)
    qt = QuotaTracker(conn)
    qt.ensure_initialized(monthly_limit=200, reset_day=reset_day)
    qt.check_and_reset()
    logger.info(f"JSearch quota: {qt.remaining()} remaining, cycle ends {qt.cycle_end()}")

    # Step 2: Delete expired jobs
    retention_days = config.get("job_retention_days", 30)
    deleted = database.delete_expired_jobs(conn, retention_days)
    if deleted:
        logger.info(f"Deleted {deleted} expired jobs (>{retention_days} days old, unreferenced).")

    # Step 3: Profile processing
    try:
        proc_result = profile_processor.process(conn, profile_name, profiles_dir)
    except FileNotFoundError as e:
        logger.error(str(e))
        return {"status": "failed", "error": str(e)}

    profile_id = proc_result["profile_id"]
    profile_changed = proc_result["profile_changed"]
    structured = proc_result["structured_content"]

    if profile_changed:
        logger.info(f"Profile '{profile_name}' changed — purging stale recommendations...")
        purged = database.purge_stale_profile_jobs(conn, profile_id)
        logger.info(f"Purged {purged} stale profile_jobs rows (new status + hidden).")

    # Compute and persist ideal_cand_pairs if profile changed or value is missing
    profile_row = database.get_profile(conn, profile_name)
    if profile_changed:
        # Profile changed: always recompute and overwrite stored value
        ideal_cand_pairs = ranker.get_ideal_cand_pairs(structured)
        database.update_ideal_cand_pairs(conn, profile_id, ideal_cand_pairs)
        logger.info(f"Profile changed — ideal_cand_pairs recomputed and stored: {ideal_cand_pairs}")
    else:
        ideal_cand_pairs = _ensure_ideal_cand_pairs(conn, profile_row, structured, logger)

    # Step 4: Determine search mode (full vs incremental)
    search_mode, jobspy_hours_old = _determine_search_mode(conn, profile_id, config, force_full)
    jsearch_date_posted = "month" if search_mode == "full" else "week"
    last_run_iso = database.get_last_run_at(conn, profile_id) if search_mode == "incremental" else None
    logger.info(f"Search mode: {search_mode} (JSearch date_posted={jsearch_date_posted}, JobSpy hours_old={jobspy_hours_old})")

    # Step 5: Build search plan
    jsearch_budget = config.get("jsearch_queries_per_run", 10)
    preferred_companies = load_preferred_companies(config, structured)

    search_plan = query_builder.build_search_plan(
        conn=conn,
        profile=structured,
        preferred_companies=preferred_companies,
        jsearch_budget=jsearch_budget,
    )

    jsearch_queries = search_plan["jsearch_queries"]
    ats_slugs = search_plan["ats_slugs"]
    jobspy_queries = search_plan["jobspy_queries"]
    companies_not_covered = search_plan["companies_not_covered"]

    if companies_not_covered:
        logger.warning(
            f"Companies not covered by JSearch budget ({len(companies_not_covered)}): "
            + ", ".join(companies_not_covered[:15])
        )

    all_raw_jobs: list[dict] = []
    sources_used: list[str] = []
    jsearch_credits_used = 0

    if progress_callback:
        progress_callback("fetching")

    # Step 6: Greenhouse
    if config.get("sources", {}).get("greenhouse", True) and ats_slugs:
        gh_slugs = [
            row["slug"]
            for row in conn.execute(
                "SELECT slug FROM ats_companies WHERE ats='greenhouse' AND slug IN ({})".format(
                    ",".join("?" * len(ats_slugs))
                ),
                ats_slugs,
            ).fetchall()
        ]
        if gh_slugs:
            logger.info(f"[greenhouse] Fetching {len(gh_slugs)} slugs...")
            gh = GreenhouseSource()
            gh_jobs = gh.fetch_many(gh_slugs)
            gh_jobs = _filter_gh_lever_by_date(gh_jobs, last_run_iso, max_age_days=retention_days)
            all_raw_jobs.extend(gh_jobs)
            if gh_jobs:
                sources_used.append("greenhouse")
            logger.info(f"[greenhouse] {len(gh_jobs)} jobs after date filter.")

    # Step 7: Lever
    if config.get("sources", {}).get("lever", True) and ats_slugs:
        lv_slugs = [
            row["slug"]
            for row in conn.execute(
                "SELECT slug FROM ats_companies WHERE ats='lever' AND slug IN ({})".format(
                    ",".join("?" * len(ats_slugs))
                ),
                ats_slugs,
            ).fetchall()
        ]
        if lv_slugs:
            logger.info(f"[lever] Fetching {len(lv_slugs)} slugs...")
            lv = LeverSource()
            lv_jobs = lv.fetch_many(lv_slugs)
            lv_jobs = _filter_gh_lever_by_date(lv_jobs, last_run_iso, max_age_days=retention_days)
            all_raw_jobs.extend(lv_jobs)
            if lv_jobs:
                sources_used.append("lever")
            logger.info(f"[lever] {len(lv_jobs)} jobs after date filter.")

    # Step 8: JSearch (company-targeted, within budget)
    if jsearch_queries and config.get("sources", {}).get("jsearch", True):
        if qt.is_available():
            logger.info(f"[jsearch] Running {len(jsearch_queries)} queries (mode={search_mode})...")
            js = JSearchSource()
            try:
                js_jobs = js.fetch_many(jsearch_queries, quota_tracker=qt, date_posted=jsearch_date_posted)
                all_raw_jobs.extend(js_jobs)
                if js_jobs:
                    sources_used.append("jsearch")
                jsearch_credits_used = len(jsearch_queries)

                # ATS slug auto-discovery
                for ats, slug in js.discovered_slugs:
                    database.upsert_ats_company(conn, slug, ats, slug)

                logger.info(
                    f"[jsearch] {len(js_jobs)} jobs fetched. "
                    f"{len(js.discovered_slugs)} new ATS slugs discovered."
                )
            except QuotaExhaustedException:
                logger.warning("[jsearch] Quota exhausted mid-run.")
        else:
            logger.warning("[jsearch] Quota unavailable — skipping JSearch queries.")

    # Step 9: JobSpy — always runs for general keyword discovery
    if config.get("sources", {}).get("jobspy", True) and jobspy_queries:
        logger.info(f"[jobspy] Running {len(jobspy_queries)} general queries (hours_old={jobspy_hours_old})...")
        spy_cfg = config.get("jobspy", {})
        spy = JobSpySource(
            sites=spy_cfg.get("sites", ["linkedin", "indeed"]),
            results_per_site=spy_cfg.get("results_per_site", 25),
        )
        spy_jobs = spy.fetch_many(jobspy_queries, hours_old=jobspy_hours_old)
        all_raw_jobs.extend(spy_jobs)
        if spy_jobs:
            sources_used.append("jobspy")
        logger.info(f"[jobspy] {len(spy_jobs)} jobs fetched.")

    # Step 10a: Enrich missing salary fields from description text
    preferred_locations = (structured.get("preferred_locations") or [])
    for job in all_raw_jobs:
        if job.get("salary_min") is None and job.get("salary_max") is None:
            desc = job.get("description") or ""
            lo, hi = parse_salary(desc, preferred_locations)
            if lo:
                job["salary_min"] = lo
                job["salary_max"] = hi if (hi and hi != lo) else None

    # Step 10b: Validate descriptions; fetch from apply_url when missing
    _validate_and_enrich_raw_jobs(all_raw_jobs, conn, logger)

    # Step 10: Deduplicate and store
    total_processed, new_inserted = deduplicator.process(conn, all_raw_jobs, profile_id=profile_id)
    logger.info(f"Dedup: {total_processed} processed, {new_inserted} new jobs in DB.")

    # Step 10c: Record missing_info profile_jobs for jobs that still lack descriptions
    # (The ranker's Stage 2.5 is still a fallback for jobs that slip through, e.g. via
    # --score without a prior --fetch-jobs run.)
    missing_info_count = 0
    for job in all_raw_jobs:
        if not job.get("_missing_info") or not job.get("job_key"):
            continue
        inserted_mi = database.insert_profile_job(
            conn,
            profile_id=profile_id,
            job_key=job["job_key"],
            match_score=0.0,
            match_notes=(
                "[Missing info] Job description absent or too short to evaluate. "
                "Check the apply link for full details."
            ),
            rank_at_discovery=None,
            hidden=True,
            application_status="missing_info",
        )
        if inserted_mi:
            missing_info_count += 1
    if missing_info_count:
        logger.info(f"[ingest-validate] {missing_info_count} jobs recorded as missing_info.")

    # Step 11: Rank (includes pre-filter for hard requirements)
    ranker_cfg = config.get("ranker", {})
    rank_result = ranker.rank(
        conn,
        profile_id=profile_id,
        profile=structured,
        profile_changed=profile_changed,
        top_n=config.get("top_n", 15),
        batch_size=ranker_cfg.get("batch_size", 50),
        min_match_score=ranker_cfg.get("min_match_score", 0.4),
        scoring_cfg=ranker_cfg.get("scoring", {}),
        score_limit=score_limit,
        ideal_cand_pairs=ideal_cand_pairs,
        desc_max_chars=ranker_cfg.get("description_max_chars", 3500),
        retention_days=retention_days,
        top_n_display=config.get("top_n_display", 50),
        progress_callback=progress_callback,
    )

    # Step 12: Write audit record
    finished_at = datetime.now(tz=timezone.utc).isoformat()
    database.insert_search_run(conn, {
        "profile_id": profile_id,
        "triggered_by": triggered_by,
        "sources_used": json.dumps(sources_used),
        "jobs_found": total_processed,
        "jobs_added": rank_result["jobs_added"],
        "jsearch_credits": jsearch_credits_used,
        "status": "success",
        "error_message": None,
        "finished_at": finished_at,
    })

    conn.close()

    # Step 13: Summary
    usage = ai_client.get_usage()
    summary = {
        "status": "success",
        "profile": profile_name,
        "search_mode": search_mode,
        "profile_regenerated": profile_changed,
        "sources_used": sources_used,
        "jobs_fetched": total_processed,
        "jobs_new_in_db": new_inserted,
        "jobs_pre_filtered": rank_result.get("jobs_pre_filtered", 0),
        "jobs_scored": rank_result["jobs_scored"],
        "jobs_added_to_profile": rank_result["jobs_added"],
        "jsearch_credits_used": jsearch_credits_used,
        "companies_not_covered": companies_not_covered,
        "ai_usage": usage,
    }

    logger.info(
        f"=== Pipeline complete ===\n"
        f"  Search mode: {search_mode}\n"
        f"  Profile regenerated: {profile_changed}\n"
        f"  Jobs fetched: {total_processed} ({new_inserted} new in DB)\n"
        f"  Pre-filtered (hard limits): {rank_result.get('jobs_pre_filtered', 0)}\n"
        f"  Jobs scored: {rank_result['jobs_scored']}\n"
        f"  Jobs added to profile: {rank_result['jobs_added']}\n"
        f"  JSearch credits used: {jsearch_credits_used}\n"
        f"  AI token usage (estimated): {usage['total_tokens']:,} total "
        f"({usage['input_tokens']:,} in / {usage['output_tokens']:,} out) "
        f"across {usage['calls']} call(s)\n"
        f"  Open the web app to review results."
    )
    return summary


def run_rescore(profile_name: str, score_limit: int | None = None) -> dict:
    """
    Rescore mode: re-evaluate all existing DB jobs for a profile using the latest profile data.
    No job fetching, no API quota used. AI scoring is run on all jobs.

    Jobs the user has acted on (saved/applied/etc.) keep their status but their
    scores and notes are refreshed. Unacted jobs are fully cleared and re-ranked.

    score_limit — if set, cap AI scoring to the first N jobs after pre-filters
                  (same as --score-number on --score / full pipeline runs).
    """
    config = load_config()
    setup_logging(config)
    logger = logging.getLogger("pipeline")

    db_path = PROJECT_ROOT / config["database"]["path"]
    conn = database.init_db(db_path)

    ai_client.reset_usage()
    logger.info(f"=== Rescore start: profile='{profile_name}' ===")

    profile_row = database.get_profile(conn, profile_name)
    if not profile_row or not profile_row["structured_content"]:
        logger.error(f"Profile '{profile_name}' not found in DB. Run the full pipeline first.")
        return {"status": "error", "message": "profile not found"}

    import json as _json
    profile = _json.loads(profile_row["structured_content"])
    profile_id = profile_row["id"]

    # Clear all unacted profile_jobs so they are re-evaluated as "unscored"
    purged = database.purge_stale_profile_jobs(conn, profile_id)
    logger.info(f"[rescore] Cleared {purged} unacted job entries — will re-score against latest profile.")

    ranker_cfg = config.get("ranker", {})
    top_n = config.get("top_n", 15)
    batch_size = ranker_cfg.get("batch_size", 50)
    min_match_score = ranker_cfg.get("min_match_score", 0.4)
    retention_days = config.get("job_retention_days", 30)
    # Rescore recomputes ideal_cand_pairs since the profile may have changed
    ideal_cand_pairs = ranker.get_ideal_cand_pairs(profile)
    database.update_ideal_cand_pairs(conn, profile_id, ideal_cand_pairs)

    rank_result = ranker.rank(
        conn,
        profile_id=profile_id,
        profile=profile,
        profile_changed=True,  # treat as changed so all jobs are considered
        top_n=top_n,
        batch_size=batch_size,
        min_match_score=min_match_score,
        score_limit=score_limit,
        ideal_cand_pairs=ideal_cand_pairs,
        desc_max_chars=ranker_cfg.get("description_max_chars", 3500),
        retention_days=retention_days,
        top_n_display=config.get("top_n_display", 50),
    )

    usage = ai_client.get_usage()
    result = {
        "status": "success",
        "profile": profile_name,
        "jobs_cleared": purged,
        "jobs_pre_filtered": rank_result.get("jobs_pre_filtered", 0),
        "jobs_scored": rank_result.get("jobs_scored", 0),
        "jobs_promoted": rank_result.get("jobs_added", 0),
        "ai_usage": usage,
    }

    logger.info(
        f"=== Rescore complete ===\n"
        f"  Cleared (unacted)  : {result['jobs_cleared']}\n"
        f"  Pre-filtered       : {result['jobs_pre_filtered']}\n"
        f"  Scored             : {result['jobs_scored']}\n"
        f"  Promoted to list   : {result['jobs_promoted']}\n"
        f"  AI token usage (estimated): {usage['total_tokens']:,} total "
        f"({usage['input_tokens']:,} in / {usage['output_tokens']:,} out) "
        f"across {usage['calls']} call(s)\n"
    )
    return result


def run_backfill(profile_name: str) -> dict:
    """
    Backfill mode: re-fetch job descriptions from Greenhouse for all jobs that were
    stored with empty descriptions due to the missing ?content=true bug.

    Steps:
      1. Find Greenhouse jobs for this profile with empty descriptions (status='new' only).
      2. Pre-filter them against hard requirements — skip jobs that would be rejected anyway.
      3. Re-fetch descriptions from the Greenhouse API (by slug, one call per company).
      4. Update jobs table with the enriched descriptions.
      5. Reset AI scores so the ranker re-evaluates with the full description.
      6. Run the ranker to re-score and refresh the top-N recommendation list.
    """
    import json as _json
    from urllib.parse import urlparse, parse_qs

    config = load_config()
    setup_logging(config)
    logger = logging.getLogger("pipeline")

    db_path = PROJECT_ROOT / config["database"]["path"]
    conn = database.init_db(db_path)

    ai_client.reset_usage()
    logger.info(f"=== Backfill start: profile='{profile_name}' ===")

    profile_row = database.get_profile(conn, profile_name)
    if not profile_row or not profile_row["structured_content"]:
        logger.error(f"Profile '{profile_name}' not found in DB. Run the full pipeline first.")
        return {"status": "error", "message": "profile not found"}

    profile = _json.loads(profile_row["structured_content"])
    profile_id = profile_row["id"]

    # Step 1: Find candidate jobs
    candidates = database.get_greenhouse_jobs_for_backfill(conn, profile_id)
    logger.info(f"[backfill] Found {len(candidates)} Greenhouse jobs with empty description.")

    if not candidates:
        logger.info("[backfill] Nothing to backfill.")
        conn.close()
        return {"status": "success", "profile": profile_name, "backfilled": 0,
                "pre_filtered": 0, "jobs_scored": 0, "jobs_promoted": 0,
                "ai_usage": ai_client.get_usage()}

    # Step 2: Pre-filter against hard requirements (avoids wasting API calls on rejects)
    passed, rejected = ranker.pre_filter(list(candidates), profile)
    logger.info(
        f"[backfill] Pre-filter: {len(passed)} passed, {len(rejected)} rejected by hard requirements."
    )

    if not passed:
        logger.info("[backfill] No jobs passed pre-filter — nothing to fetch.")
        conn.close()
        return {"status": "success", "profile": profile_name, "backfilled": 0,
                "pre_filtered": len(rejected), "jobs_scored": 0, "jobs_promoted": 0,
                "ai_usage": ai_client.get_usage()}

    # Step 3: Re-fetch descriptions from Greenhouse API, grouped by slug
    slugs = list({job["source_company_slug"] for job in passed if job["source_company_slug"]})
    logger.info(f"[backfill] Re-fetching from Greenhouse for {len(slugs)} slugs: {slugs}")

    gh = GreenhouseSource()
    fetched_jobs = gh.fetch_many(slugs)

    # Build lookup: apply_url → {description, raw_data}
    url_to_content: dict[str, dict] = {}
    for job in fetched_jobs:
        if job.get("apply_url") and job.get("description"):
            url_to_content[job["apply_url"]] = {
                "description": job["description"],
                "raw_data": job["raw_data"],
            }
    logger.info(f"[backfill] Greenhouse returned {len(fetched_jobs)} jobs, "
                f"{len(url_to_content)} with non-empty descriptions.")

    # Step 4: Update descriptions in the jobs table
    backfilled_keys: list[str] = []
    no_match = 0
    for job in passed:
        content = url_to_content.get(job["apply_url"])
        if content:
            database.update_job_description(conn, job["job_key"], content["description"], content["raw_data"])
            backfilled_keys.append(job["job_key"])
        else:
            no_match += 1
            logger.debug(f"[backfill] No description found for: {job['apply_url']}")

    logger.info(f"[backfill] Updated {len(backfilled_keys)} job descriptions. "
                f"{no_match} had no match in current Greenhouse listings (may have been removed).")

    if not backfilled_keys:
        logger.info("[backfill] No descriptions updated — skipping re-score.")
        conn.close()
        return {"status": "success", "profile": profile_name, "backfilled": 0,
                "pre_filtered": len(rejected), "jobs_scored": 0, "jobs_promoted": 0,
                "ai_usage": ai_client.get_usage()}

    # Step 5: Reset AI scores so the ranker re-evaluates with the full description.
    # profile_jobs rows that exist (already scored) are cleared; rows that don't exist
    # yet will simply be treated as unscored by the ranker.
    reset_count = database.reset_scores_for_backfill(conn, profile_id, backfilled_keys)
    logger.info(f"[backfill] Reset scores for {reset_count} profile_jobs rows.")

    # Step 6: Re-score and refresh top-N
    ranker_cfg = config.get("ranker", {})
    rank_result = ranker.rank(
        conn,
        profile_id=profile_id,
        profile=profile,
        profile_changed=False,
        top_n=config.get("top_n", 15),
        batch_size=ranker_cfg.get("batch_size", 50),
        min_match_score=ranker_cfg.get("min_match_score", 0.4),
        ideal_cand_pairs=_ensure_ideal_cand_pairs(conn, profile_row, profile, logger),
        desc_max_chars=ranker_cfg.get("description_max_chars", 3500),
        retention_days=retention_days,
        top_n_display=config.get("top_n_display", 50),
    )

    conn.close()
    usage = ai_client.get_usage()

    result = {
        "status": "success",
        "profile": profile_name,
        "candidates_found": len(candidates),
        "pre_filtered": len(rejected),
        "backfilled": len(backfilled_keys),
        "no_gh_match": no_match,
        "scores_reset": reset_count,
        "jobs_scored": rank_result.get("jobs_scored", 0),
        "jobs_promoted": rank_result.get("jobs_added", 0),
        "ai_usage": usage,
    }

    logger.info(
        f"=== Backfill complete ===\n"
        f"  Candidates found    : {result['candidates_found']}\n"
        f"  Pre-filtered (skip) : {result['pre_filtered']}\n"
        f"  Descriptions fetched: {result['backfilled']}\n"
        f"  No GH match (gone)  : {result['no_gh_match']}\n"
        f"  Scores reset        : {result['scores_reset']}\n"
        f"  Jobs re-scored      : {result['jobs_scored']}\n"
        f"  Promoted to list    : {result['jobs_promoted']}\n"
        f"  AI token usage (estimated): {usage['total_tokens']:,} total "
        f"({usage['input_tokens']:,} in / {usage['output_tokens']:,} out) "
        f"across {usage['calls']} call(s)\n"
    )
    return result


_MIN_DESC_CHARS = 150  # must match ranker Stage 2.5 threshold


def _validate_and_enrich_raw_jobs(
    jobs: list[dict],
    conn,
    logger,
) -> list[dict]:
    """
    Pre-DB validation: check each job's description for scoreable content
    (responsibilities or requirements sections, >= 150 chars after extraction).

    If a job fails validation and has an apply_url, fetches that URL once and
    updates the job's description in-place.  Jobs that still fail after the
    fetch attempt are marked with ``_missing_info=True`` so the caller can
    insert a profile_jobs row with application_status='missing_info'.

    Jobs already present in the DB with a valid description are left untouched
    (INSERT OR IGNORE means the DB copy is preserved anyway).

    A 1-second polite delay is added between each URL fetch.
    """
    import time

    enriched = 0
    flagged = 0

    for job in jobs:
        title   = job.get("title")   or ""
        company = job.get("company") or ""
        location = job.get("location") or ""

        if not title or not company:
            continue  # will be skipped by deduplicator anyway

        # Check whether the DB already has a valid description for this job key.
        job_key = deduplicator.make_job_key(title, company, location)
        existing = conn.execute(
            "SELECT description FROM jobs WHERE job_key = ?", (job_key,)
        ).fetchone()
        if existing:
            existing_desc = existing["description"] or ""
            if (
                len(ranker.extract_description(existing_desc, max_chars=999_999)) >= _MIN_DESC_CHARS
                and ranker.is_description_scoreable(existing_desc)
            ):
                continue  # DB copy is valid; INSERT OR IGNORE will keep it

        # Check the raw description from this fetch.
        raw_desc = job.get("description") or ""
        extracted = ranker.extract_description(raw_desc, max_chars=999_999)
        if len(extracted) >= _MIN_DESC_CHARS and ranker.is_description_scoreable(raw_desc):
            continue  # raw description is good

        # Description is insufficient — try the apply_url.
        url = job.get("apply_url") or ""
        if not url:
            job["_missing_info"] = True
            flagged += 1
            continue

        logger.debug(
            f"[ingest-validate] Fetching apply_url for '{title}' @ '{company}': {url}"
        )
        html = _fetch_url_description(url)
        time.sleep(1)

        if not html:
            logger.debug(f"[ingest-validate]   ✗ Fetch failed.")
            job["_missing_info"] = True
            flagged += 1
            continue

        # Validate against full content (no truncation); truncation happens later at AI scoring.
        fetched_extracted = ranker.extract_description(html, max_chars=999_999)
        if (
            len(fetched_extracted) >= _MIN_DESC_CHARS
            and ranker.is_description_scoreable(html)
        ):
            job["description"] = html  # full raw content; AI scorer truncates at scoring time
            enriched += 1
            logger.info(
                f"[ingest-validate] ✓ Enriched description for '{title}' @ '{company}' "
                f"({len(html)} chars saved, {len(fetched_extracted)} chars extracted)"
            )
        else:
            logger.debug(
                f"[ingest-validate]   ✗ apply_url content insufficient "
                f"({len(fetched_extracted)} chars extracted)."
            )
            job["_missing_info"] = True
            flagged += 1

    if enriched or flagged:
        logger.info(
            f"[ingest-validate] Description check: {enriched} enriched from apply_url, "
            f"{flagged} flagged as missing_info."
        )

    return jobs


def _fetch_url_description(url: str, timeout: int = 20) -> str | None:
    """
    Fetch a URL with browser-like headers and return the raw HTML, or None on failure.
    Uses only stdlib (urllib) so no extra dependencies are needed.
    """
    import urllib.request
    import urllib.error

    if not url or not url.startswith("http"):
        return None

    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/124.0.0.0 Safari/537.36"
        ),
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.5",
        "Accept-Encoding": "identity",  # avoid compressed responses that need extra decoding
    }
    try:
        req = urllib.request.Request(url, headers=headers)
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            if resp.status != 200:
                return None
            content_type = resp.headers.get("Content-Type", "")
            if "text" not in content_type and "html" not in content_type:
                return None
            raw = resp.read(1_000_000)  # cap at ~1 MB
            charset = "utf-8"
            if "charset=" in content_type:
                charset = content_type.split("charset=")[-1].strip().split(";")[0].strip()
            return raw.decode(charset, errors="replace")
    except Exception:
        return None


def run_fetch_missing(profile_name: str) -> dict:
    """
    Fetch-missing mode: attempt to retrieve job descriptions for all jobs
    currently flagged as 'missing_info' by performing a direct HTTP GET on
    each job's apply_url.

    Steps:
      1. Find all 'missing_info' profile_jobs for this profile.
      2. For each job, fetch apply_url with browser-like headers.
      3. If the fetched HTML yields >= 150 chars of meaningful description text,
         update the job's description in the jobs table.
      4. Delete the profile_jobs row so the ranker re-evaluates from scratch.
      5. Run the ranker to score the newly-described jobs and refresh top-N.
         Jobs that could not be fetched remain as 'missing_info'.
    """
    import json as _json
    import time

    config = load_config()
    setup_logging(config)
    logger = logging.getLogger("pipeline")

    db_path = PROJECT_ROOT / config["database"]["path"]
    conn = database.init_db(db_path)

    ai_client.reset_usage()
    logger.info(f"=== Fetch-missing start: profile='{profile_name}' ===")

    profile_row = database.get_profile(conn, profile_name)
    if not profile_row or not profile_row["structured_content"]:
        logger.error(f"Profile '{profile_name}' not found in DB. Run the full pipeline first.")
        return {"status": "error", "message": "profile not found"}

    profile = _json.loads(profile_row["structured_content"])
    profile_id = profile_row["id"]

    # Step 1: Find missing_info jobs
    candidates = database.get_missing_info_jobs(conn, profile_id)
    logger.info(f"[fetch-missing] Found {len(candidates)} missing_info jobs.")

    if not candidates:
        logger.info("[fetch-missing] Nothing to fetch.")
        conn.close()
        return {
            "status": "success", "profile": profile_name,
            "candidates": 0, "fetched": 0, "failed": 0,
            "jobs_scored": 0, "jobs_promoted": 0,
            "ai_usage": ai_client.get_usage(),
        }

    # Step 2–4: Fetch and update descriptions
    fetched_keys: list[str] = []
    failed: list[str] = []
    _MIN_DESC_CHARS = 150  # must match ranker Stage 2.5 threshold

    for job in candidates:
        url = job["apply_url"] or ""
        if not url:
            logger.warning(f"[fetch-missing] No apply_url for {job['job_key']} ({job['title']}), skipping.")
            failed.append(job["job_key"])
            continue

        if "linkedin.com" in url:
            logger.warning(
                f"[fetch-missing] Skipping LinkedIn URL (login wall, cannot scrape): {url}"
            )
            failed.append(job["job_key"])
            continue

        logger.info(f"[fetch-missing] Fetching: {job['title']} @ {job['company']} — {url}")
        html = _fetch_url_description(url)

        if not html:
            logger.warning(f"[fetch-missing]   ✗ Fetch failed (network error or non-HTML response).")
            failed.append(job["job_key"])
            time.sleep(1)
            continue

        # Check meaningful content using the same extraction logic as the ranker
        extracted = ranker.extract_description(html)
        if len(extracted) < _MIN_DESC_CHARS:
            logger.warning(
                f"[fetch-missing]   ✗ Content too short after extraction "
                f"({len(extracted)} chars) — likely a login wall or empty page."
            )
            failed.append(job["job_key"])
            time.sleep(1)
            continue

        # Update description in jobs table
        database.update_job_description(conn, job["job_key"], html, None)

        # Delete the missing_info profile_jobs row so the ranker re-evaluates fresh
        database.delete_profile_job(conn, profile_id, job["job_key"])

        logger.info(f"[fetch-missing]   ✓ {len(extracted)} chars extracted — queued for re-scoring.")
        fetched_keys.append(job["job_key"])
        time.sleep(1)  # polite delay between requests

    logger.info(
        f"[fetch-missing] Fetched {len(fetched_keys)}/{len(candidates)} descriptions. "
        f"{len(failed)} could not be retrieved."
    )

    if not fetched_keys:
        logger.info("[fetch-missing] No descriptions updated — skipping re-score.")
        conn.close()
        return {
            "status": "success", "profile": profile_name,
            "candidates": len(candidates), "fetched": 0, "failed": len(failed),
            "jobs_scored": 0, "jobs_promoted": 0,
            "ai_usage": ai_client.get_usage(),
        }

    # Step 5: Re-score and refresh top-N (ranker picks up the deleted rows as unscored)
    ranker_cfg = config.get("ranker", {})
    retention_days = config.get("job_retention_days", 30)
    rank_result = ranker.rank(
        conn,
        profile_id=profile_id,
        profile=profile,
        profile_changed=False,
        top_n=config.get("top_n", 15),
        batch_size=ranker_cfg.get("batch_size", 50),
        min_match_score=ranker_cfg.get("min_match_score", 0.4),
        scoring_cfg=ranker_cfg.get("scoring", {}),
        ideal_cand_pairs=_ensure_ideal_cand_pairs(conn, profile_row, profile, logger),
        desc_max_chars=ranker_cfg.get("description_max_chars", 3500),
        retention_days=retention_days,
        top_n_display=config.get("top_n_display", 50),
    )

    conn.close()
    usage = ai_client.get_usage()

    result = {
        "status": "success",
        "profile": profile_name,
        "candidates": len(candidates),
        "fetched": len(fetched_keys),
        "failed": len(failed),
        "jobs_scored": rank_result.get("jobs_scored", 0),
        "jobs_promoted": rank_result.get("jobs_added", 0),
        "ai_usage": usage,
    }
    print(
        f"\n=== Fetch-missing complete ===\n"
        f"  Missing-info jobs found : {result['candidates']}\n"
        f"  Descriptions fetched    : {result['fetched']}\n"
        f"  Could not fetch         : {result['failed']}\n"
        f"  Jobs re-scored          : {result['jobs_scored']}\n"
        f"  Jobs promoted to list   : {result['jobs_promoted']}\n"
        f"  Est. tokens used        : {usage.get('total_tokens', 0):,}\n"
    )
    return result


def run_repromote(profile_name: str) -> dict:
    """
    Repromote mode: re-apply hard requirements + promotion step without any AI calls.
    1. Re-applies hard requirements to all DB jobs (same as --refilter)
    2. Re-sorts existing AI scores and promotes the top-N
    No job fetching, no Claude calls.
    """
    config = load_config()
    setup_logging(config)
    logger = logging.getLogger("pipeline")

    db_path = PROJECT_ROOT / config["database"]["path"]
    conn = database.init_db(db_path)

    top_n_display = config.get("top_n_display", 50)
    min_match_score = config.get("ranker", {}).get("min_match_score", 0.4)

    logger.info(f"=== Repromote start: profile='{profile_name}' top_n_display={top_n_display} min_match_score={min_match_score} ===")

    profile_row = database.get_profile(conn, profile_name)
    if not profile_row or not profile_row["structured_content"]:
        logger.error(f"Profile '{profile_name}' not found in DB. Run the full pipeline first.")
        return {"status": "error", "message": "profile not found"}

    import json as _json
    profile = _json.loads(profile_row["structured_content"])
    profile_id = profile_row["id"]

    ideal_cand_pairs = _ensure_ideal_cand_pairs(conn, profile_row, profile, logger)

    # Step 1: re-apply hard requirements (same logic as --refilter)
    logger.info(f"[repromote] Step 1/2: Re-applying hard requirements...")
    refilter_result = ranker.refilter(conn, profile_id, profile)
    logger.info(
        f"[repromote] Hard filter: {refilter_result['rejected_new']} newly rejected, "
        f"{refilter_result['rejected_updated']} updated, "
        f"{refilter_result['cleared_for_reeval']} cleared for re-eval."
    )

    # Step 2: re-calculate scores from stored pairs then re-promote
    logger.info(f"[repromote] Step 2/2: Re-calculating scores from stored pairs and re-promoting...")
    demoted = database.reset_promotion(conn, profile_id)
    logger.info(f"[repromote] Reset {demoted} currently-visible jobs to hidden.")

    retention_days = config.get("job_retention_days", 30)
    ranker_cfg = config.get("ranker", {})
    scoring_cfg = ranker_cfg.get("scoring", {})

    # Pass 1: rescore ALL scored jobs within the retention window, regardless of
    # application_status.  This keeps displayed scores accurate for applied/saved
    # jobs after a scoring formula change.
    all_scored_rows = conn.execute(
        """
        SELECT pj.job_key, pj.match_pairs_json, pj.total_job_requirements
        FROM profile_jobs pj
        JOIN jobs j ON pj.job_key = j.job_key
        WHERE pj.profile_id = ?
          AND pj.match_notes NOT LIKE '[Hard filter]%'
          AND pj.match_notes NOT LIKE '[Title filter]%'
          AND pj.match_pairs_json IS NOT NULL
          AND (
              CASE WHEN j.date_posted GLOB '[0-9][0-9][0-9][0-9]-[0-9][0-9]-[0-9][0-9]'
                   THEN j.date_posted
                   ELSE DATE(j.date_found)
              END >= DATE('now', ?)
          )
        """,
        (profile_id, f"-{retention_days} days"),
    ).fetchall()

    for row in all_scored_rows:
        pairs = []
        if row["match_pairs_json"]:
            try:
                pairs = _json.loads(row["match_pairs_json"])
            except Exception:
                pass
        mgr_score, cand_score, match_score = ranker.compute_scores_public(
            pairs, scoring_cfg,
            total_job_requirements=row["total_job_requirements"],
            ideal_cand_pairs=ideal_cand_pairs,
        )
        database.update_profile_job_scores(
            conn, profile_id, row["job_key"],
            manager_score=mgr_score,
            candidate_score=cand_score,
            match_score=match_score,
        )
    logger.info(f"[repromote] Re-calculated scores for {len(all_scored_rows)} jobs.")

    # Pass 2: build the promotion list from new-only jobs (already updated above).
    scored_rows = database.get_scored_profile_jobs(conn, profile_id, retention_days=retention_days)
    logger.info(f"[repromote] {len(scored_rows)} new jobs eligible for promotion.")

    recalculated = []
    for row in scored_rows:
        recalculated.append({
            "job_key":       row["job_key"],
            "match_score":   row["match_score"],
            "manager_score": row["manager_score"],
            "candidate_score": row["candidate_score"],
        })

    qualifying = sorted(
        [r for r in recalculated if r["match_score"] >= min_match_score],
        key=lambda r: (
            r["manager_score"] + r["candidate_score"],
            r["manager_score"],
            r["candidate_score"],
        ),
        reverse=True,
    )
    top_candidates = qualifying[:top_n_display]

    promoted = 0
    for rank_pos, row in enumerate(top_candidates, start=1):
        if database.promote_profile_job(conn, profile_id, row["job_key"], rank_pos):
            promoted += 1

    logger.info(f"[repromote] {len(qualifying)} qualifying, {promoted} promoted to visible (cap={top_n_display}).")

    # Final enforcement: trim any excess visible 'new' jobs down to exactly top_n_display.
    # This catches historical data that accumulated before the cap was introduced, and
    # also handles edge cases where promote_profile_job couldn't update a row.
    rebalance = database.rebalance_visible_jobs(conn, profile_id, top_n_display, min_match_score)
    if rebalance["demoted"]:
        logger.info(f"[repromote] Rebalance trimmed {rebalance['demoted']} excess visible jobs.")

    logger.info(f"=== Repromote complete: profile='{profile_name}' ===")
    print(
        f"\n=== Repromote complete ===\n"
        f"  Hard-filtered (new)    : {refilter_result['rejected_new']}\n"
        f"  Hard-filtered (update) : {refilter_result['rejected_updated']}\n"
        f"  Cleared for re-eval    : {refilter_result['cleared_for_reeval']}\n"
        f"  Scored in DB           : {len(scored_rows)}\n"
        f"  Scores re-calculated   : {len(recalculated)}\n"
        f"  Qualifying             : {len(qualifying)}\n"
        f"  Promoted               : {promoted}\n"
        f"  Trimmed by rebalance   : {rebalance['demoted']}\n"
    )
    return {"status": "success", "scored": len(scored_rows), "qualifying": len(qualifying), "promoted": promoted}


def run_score_unscored(profile_name: str, score_limit: int | None = None) -> dict:
    """
    Score-unscored mode: run AI scoring only on jobs that have no profile_jobs entry yet.
    Does not clear or touch any existing scored rows.
    No job fetching, no quota used.
    """
    config = load_config()
    setup_logging(config)
    logger = logging.getLogger("pipeline")
    retention_days = config.get("job_retention_days", 30)

    db_path = PROJECT_ROOT / config["database"]["path"]
    conn = database.init_db(db_path)

    ai_client.reset_usage()
    logger.info(f"=== Score-unscored start: profile='{profile_name}' ===")

    profile_row = database.get_profile(conn, profile_name)
    if not profile_row or not profile_row["structured_content"]:
        logger.error(f"Profile '{profile_name}' not found in DB. Run the full pipeline first.")
        return {"status": "error", "message": "profile not found"}

    import json as _json
    profile = _json.loads(profile_row["structured_content"])
    profile_id = profile_row["id"]

    unscored_count = len(database.get_unscored_jobs(conn, profile_id, retention_days=retention_days))
    logger.info(f"[score-unscored] {unscored_count} jobs without a score for this profile.")

    if not unscored_count:
        logger.info("[score-unscored] Nothing to score.")
        conn.close()
        return {
            "status": "success",
            "profile": profile_name,
            "jobs_pre_filtered": 0,
            "jobs_scored": 0,
            "jobs_promoted": 0,
            "ai_usage": ai_client.get_usage(),
        }

    ranker_cfg = config.get("ranker", {})
    rank_result = ranker.rank(
        conn,
        profile_id=profile_id,
        profile=profile,
        profile_changed=False,
        top_n=config.get("top_n", 15),
        batch_size=ranker_cfg.get("batch_size", 50),
        min_match_score=ranker_cfg.get("min_match_score", 0.4),
        scoring_cfg=ranker_cfg.get("scoring", {}),
        score_limit=score_limit,
        ideal_cand_pairs=_ensure_ideal_cand_pairs(conn, profile_row, profile, logger),
        desc_max_chars=ranker_cfg.get("description_max_chars", 3500),
        retention_days=retention_days,
        top_n_display=config.get("top_n_display", 50),
    )

    conn.close()
    usage = ai_client.get_usage()
    result = {
        "status": "success",
        "profile": profile_name,
        "jobs_pre_filtered": rank_result.get("jobs_pre_filtered", 0),
        "jobs_scored": rank_result.get("jobs_scored", 0),
        "jobs_promoted": rank_result.get("jobs_added", 0),
        "ai_usage": usage,
    }

    logger.info(
        f"=== Score-unscored complete ===\n"
        f"  Pre-filtered       : {result['jobs_pre_filtered']}\n"
        f"  Scored             : {result['jobs_scored']}\n"
        f"  Promoted to list   : {result['jobs_promoted']}\n"
        f"  AI token usage (estimated): {usage['total_tokens']:,} total "
        f"({usage['input_tokens']:,} in / {usage['output_tokens']:,} out) "
        f"across {usage['calls']} call(s)\n"
    )
    return result


def run_refilter(profile_name: str) -> dict:
    """
    Refilter mode: re-apply hard requirements to all DB jobs for a profile.
    No API calls, no AI — pure Python filter only.
    """
    config = load_config()
    setup_logging(config)
    logger = logging.getLogger("pipeline")

    db_path = PROJECT_ROOT / config["database"]["path"]
    conn = database.init_db(db_path)

    logger.info(f"=== Refilter start: profile='{profile_name}' ===")

    profile_row = database.get_profile(conn, profile_name)
    if not profile_row or not profile_row["structured_content"]:
        logger.error(f"Profile '{profile_name}' not found in DB. Run the full pipeline first.")
        return {"status": "error", "message": "profile not found"}

    import json
    profile = json.loads(profile_row["structured_content"])
    profile_id = profile_row["id"]

    result = ranker.refilter(conn, profile_id, profile)
    result["status"] = "success"
    result["profile"] = profile_name

    logger.info(
        f"=== Refilter complete ===\n"
        f"  Total jobs (profile)  : {result['total_jobs']}\n"
        f"  Passed filter         : {result['jobs_passed_filter']}\n"
        f"  ── Changes this run ──\n"
        f"  Newly hard-filtered   : {result['rejected_new']}\n"
        f"  Hidden (were scored)  : {result['rejected_updated']}\n"
        f"  Cleared for re-eval   : {result['cleared_for_reeval']}\n"
        f"  ── Unchanged ─────────\n"
        f"  Already filtered      : {result['rejected_already_filtered']}\n"
        f"  Skipped (user acted)  : {result['skipped_actioned']}\n"
    )
    return result


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run the job finder pipeline.")
    parser.add_argument("--profile", help="Profile name (e.g. 'john' for profiles/john.txt)")
    parser.add_argument("--triggered-by", default="manual", choices=["manual", "scheduler"])
    parser.add_argument("--full-search", action="store_true", help="Force full search mode (ignore last run date)")
    parser.add_argument("--refilter", action="store_true",
                        help="Refilter mode: re-apply hard requirements to all DB jobs. No API calls or AI.")
    parser.add_argument("--rescore", action="store_true",
                        help="Rescore mode: re-evaluate all existing DB jobs with AI using the latest profile. No job fetching. "
                             "Combine with --score-number N to cap AI scoring to the first N jobs (newest-posted first).")
    parser.add_argument("--repromote", action="store_true",
                        help="Repromote mode: re-apply promotion logic to already-scored jobs. No AI calls.")
    parser.add_argument("--backfill", action="store_true",
                        help="Backfill mode: re-fetch descriptions for Greenhouse jobs stored with empty "
                             "descriptions, then re-score and refresh recommendations.")
    parser.add_argument("--fetch-missing", action="store_true",
                        help="Fetch-missing mode: attempt to retrieve descriptions for jobs flagged as "
                             "'missing_info' by fetching their apply_url directly, then re-score.")
    parser.add_argument("--fetch-jobs", action="store_true",
                        help="Fetch-jobs mode: fetch jobs from all sources and store in the DB. "
                             "No scoring or promotion. Use --full-search to force a full refresh.")
    parser.add_argument("--score", action="store_true",
                        help="Score mode: AI-score only jobs that have no profile_jobs entry yet. "
                             "Does not clear or touch existing scored rows. No job fetching.")
    parser.add_argument("--score-number", type=int, default=None, metavar="N",
                        help="Testing mode: run the full fetch pipeline but AI-score only the first N "
                             "jobs that pass pre-filters. Useful for smoke-testing without burning quota.")
    parser.add_argument("--clear-jobs", action="store_true",
                        help="Clear jobs, scores, and search history from the DB. "
                             "With --profile: clears only that profile's data. "
                             "Without --profile: clears ALL profiles. Profiles and ATS company data are preserved.")
    parser.add_argument("--delete-profile", action="store_true",
                        help="Permanently delete a profile and all its data from the DB. "
                             "Requires --profile. Cannot be undone.")
    parser.add_argument("--list-profiles", action="store_true",
                        help="List all profiles in the DB with their ID, name, and initialization status.")
    args = parser.parse_args()

    if args.list_profiles:
        config = load_config()
        db_path = PROJECT_ROOT / config["database"]["path"]
        conn = database.init_db(db_path)
        profiles = conn.execute(
            "SELECT id, name, structured_content IS NOT NULL AS initialized, created_at "
            "FROM profiles ORDER BY LOWER(name)"
        ).fetchall()
        conn.close()
        if not profiles:
            print("No profiles found.")
        else:
            print(f"\n{'ID':<6}  {'Name':<30}  {'Status':<18}  Created")
            print("-" * 72)
            for p in profiles:
                status = "initialized" if p["initialized"] else "NOT initialized"
                print(f"{p['id']:<6}  {p['name']:<30}  {status:<18}  {p['created_at']}")
            print()
        sys.exit(0)

    if args.delete_profile:
        if not args.profile:
            parser.error("--delete-profile requires --profile")
        config = load_config()
        db_path = PROJECT_ROOT / config["database"]["path"]
        conn = database.init_db(db_path)
        profile_row = database.get_profile(conn, args.profile)
        if not profile_row:
            print(f"ERROR: Profile '{args.profile}' not found.")
            conn.close()
            sys.exit(1)
        profile_id = profile_row["id"]
        pj_count  = conn.execute("SELECT COUNT(*) FROM profile_jobs WHERE profile_id = ?", (profile_id,)).fetchone()[0]
        sr_count  = conn.execute("SELECT COUNT(*) FROM search_runs WHERE profile_id = ?", (profile_id,)).fetchone()[0]
        print("=" * 60)
        print(f"  WARNING: DESTRUCTIVE ACTION — THIS CANNOT BE UNDONE")
        print("=" * 60)
        print(f"  Profile to delete:       {args.profile}")
        print(f"  Profile-job rows:        {pj_count}")
        print(f"  Search run records:      {sr_count}")
        print("  Orphaned jobs (no other profile) will also be removed.")
        print("=" * 60)
        answer = input('  Type "delete profile" to confirm, or anything else to abort: ')
        if answer.strip().lower() != "delete profile":
            print("Aborted.")
            conn.close()
            sys.exit(0)
        counts = database.delete_profile(conn, profile_id)
        conn.close()
        print(f"Deleted profile '{args.profile}': {counts['profiles']} profile, "
              f"{counts['jobs']} jobs, {counts['profile_jobs']} profile_job rows, "
              f"{counts['search_runs']} search runs.")
        sys.exit(0)

    if args.clear_jobs:
        config = load_config()
        db_path = PROJECT_ROOT / config["database"]["path"]
        conn = database.init_db(db_path)
        if args.profile:
            # Profile-scoped clear
            profile_row = database.get_profile(conn, args.profile)
            if not profile_row:
                print(f"ERROR: Profile '{args.profile}' not found.")
                conn.close()
                sys.exit(1)
            profile_id = profile_row["id"]
            pj_count  = conn.execute("SELECT COUNT(*) FROM profile_jobs WHERE profile_id = ?", (profile_id,)).fetchone()[0]
            run_count = conn.execute("SELECT COUNT(*) FROM search_runs WHERE profile_id = ?", (profile_id,)).fetchone()[0]
            print("=" * 60)
            print(f"  WARNING: DESTRUCTIVE ACTION — THIS CANNOT BE UNDONE")
            print("=" * 60)
            print(f"  Profile:                 {args.profile}")
            print(f"  Profile-job rows:        {pj_count}")
            print(f"  Search run records:      {run_count}")
            print("  Orphaned jobs (no other profile) will also be removed.")
            print("=" * 60)
            answer = input('  Type "clear jobs" to confirm, or anything else to abort: ')
            if answer.strip().lower() != "clear jobs":
                print("Aborted.")
                conn.close()
                sys.exit(0)
            counts = database.clear_profile_jobs(conn, profile_id)
        else:
            # Global clear
            job_count  = conn.execute("SELECT COUNT(*) FROM jobs").fetchone()[0]
            pj_count   = conn.execute("SELECT COUNT(*) FROM profile_jobs").fetchone()[0]
            run_count  = conn.execute("SELECT COUNT(*) FROM search_runs").fetchone()[0]
            print("=" * 60)
            print("  WARNING: DESTRUCTIVE ACTION — THIS CANNOT BE UNDONE")
            print("=" * 60)
            print(f"  Jobs to delete:          {job_count}")
            print(f"  Profile-job rows:        {pj_count}")
            print(f"  Search run records:      {run_count}")
            print("  Profiles will be kept.")
            print("=" * 60)
            answer = input('  Type "clear jobs" to confirm, or anything else to abort: ')
            if answer.strip().lower() != "clear jobs":
                print("Aborted.")
                conn.close()
                sys.exit(0)
            counts = database.clear_all_jobs(conn)
        conn.close()
        print(f"Cleared: {counts['jobs']} jobs, {counts['profile_jobs']} profile_job rows, {counts['search_runs']} search runs.")
        sys.exit(0)

    if not args.profile:
        parser.error("--profile is required unless using --clear-jobs")

    modes = [args.refilter, args.rescore, args.repromote, args.backfill, args.fetch_missing, args.fetch_jobs, args.score]
    if sum(modes) > 1:
        print("ERROR: --refilter, --rescore, --repromote, --backfill, --fetch-missing, --fetch-jobs, and --score are mutually exclusive.")
        sys.exit(1)
    elif args.refilter:
        result = run_refilter(args.profile)
    elif args.rescore:
        result = run_rescore(args.profile, score_limit=args.score_number)
    elif args.repromote:
        result = run_repromote(args.profile)
    elif args.backfill:
        result = run_backfill(args.profile)
    elif args.fetch_missing:
        result = run_fetch_missing(args.profile)
    elif args.fetch_jobs:
        result = run_fetch_jobs(args.profile, force_full=args.full_search)
    elif args.score:
        result = run_score_unscored(args.profile, score_limit=args.score_number)
    else:
        result = run_pipeline(args.profile, args.triggered_by, force_full=args.full_search,
                              score_limit=args.score_number)
    sys.exit(0 if result.get("status") == "success" else 1)
