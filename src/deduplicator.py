"""
deduplicator.py — Job key generation and DB upsert.
Processes raw job dicts from sources, generates stable keys, inserts new jobs.
"""

import hashlib
import logging
import re

from src import database

logger = logging.getLogger(__name__)


def make_job_key(title: str, company: str, location: str) -> str:
    """
    Generate a stable 16-char hex key from title + company + location.
    Normalized: lowercased, punctuation stripped, whitespace collapsed.
    """
    def normalize(s: str) -> str:
        s = s.lower().strip()
        s = re.sub(r"[^\w|]", "", s)
        s = re.sub(r"\s+", " ", s)
        return s

    normalized = f"{normalize(title)}|{normalize(company)}|{normalize(location)}"
    return hashlib.md5(normalized.encode("utf-8")).hexdigest()[:16]


def process(conn, raw_jobs: list[dict], profile_id: int | None = None) -> tuple[int, int]:
    """
    Deduplicate and insert new jobs into the jobs table.
    Returns (total_processed, new_inserted).

    profile_id — when provided, stamped onto each newly inserted job as
    fetched_for_profile_id so the database view can show which profile's
    pipeline discovered the job, even before it has been scored.
    INSERT OR IGNORE means a job that already exists keeps the original
    fetched_for_profile_id (the first profile to discover it wins).
    """
    new_inserted = 0

    for job in raw_jobs:
        title = job.get("title") or ""
        company = job.get("company") or ""
        location = job.get("location") or ""

        if not title or not company:
            logger.debug(f"[dedup] Skipping job with missing title/company: {job.get('apply_url')}")
            continue

        job_key = make_job_key(title, company, location)
        job["job_key"] = job_key
        job["fetched_for_profile_id"] = profile_id

        inserted = database.insert_job(conn, job)
        if inserted:
            new_inserted += 1

    total = len(raw_jobs)
    logger.info(f"[dedup] Processed {total} jobs: {new_inserted} new, {total - new_inserted} duplicates skipped.")
    return total, new_inserted
