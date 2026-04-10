"""
greenhouse.py — Greenhouse public API client.
Endpoint: GET https://boards-api.greenhouse.io/v1/boards/{slug}/jobs?content=true
No auth, no rate limit.
"""

import json
import logging
import re
from datetime import datetime, timezone

import requests

from src.sources.base import BaseSource

logger = logging.getLogger(__name__)

BASE_URL = "https://boards-api.greenhouse.io/v1/boards/{slug}/jobs"

REMOTE_KEYWORDS = {
    "remote": "remote",
    "hybrid": "hybrid",
    "on-site": "on-site",
    "onsite": "on-site",
    "in office": "on-site",
    "in-office": "on-site",
}


def _infer_remote_type(title: str, location: str) -> str:
    text = f"{title} {location}".lower()
    for kw, label in REMOTE_KEYWORDS.items():
        if kw in text:
            return label
    return "unknown"


def _parse_date(date_str: str | None) -> str | None:
    if not date_str:
        return None
    try:
        # Greenhouse returns ISO 8601 with timezone
        dt = datetime.fromisoformat(date_str.replace("Z", "+00:00"))
        return dt.strftime("%Y-%m-%d")
    except Exception:
        return None


class GreenhouseSource(BaseSource):
    def __init__(self, session: requests.Session | None = None):
        self.session = session or requests.Session()
        self.session.headers.update({"User-Agent": "jobwise/1.0"})

    def fetch(self, slug: str) -> list[dict]:
        """
        Fetch all live jobs for a Greenhouse company slug.
        Returns a list of normalized job dicts.
        """
        url = BASE_URL.format(slug=slug)
        logger.info(f"[greenhouse] Fetching jobs for slug={slug}")

        try:
            resp = self.session.get(url, params={"content": "true"}, timeout=15)
        except requests.RequestException as e:
            logger.error(f"[greenhouse] Request failed for {slug}: {e}")
            return []

        if resp.status_code == 404:
            logger.warning(f"[greenhouse] Slug not found: {slug}")
            return []
        if resp.status_code != 200:
            logger.error(f"[greenhouse] HTTP {resp.status_code} for {slug}")
            return []

        try:
            data = resp.json()
        except Exception as e:
            logger.error(f"[greenhouse] Failed to parse JSON for {slug}: {e}")
            return []

        raw_jobs = data.get("jobs", [])
        logger.info(f"[greenhouse] {len(raw_jobs)} jobs found for {slug}")

        results = []
        for job in raw_jobs:
            title = job.get("title", "")
            location_parts = job.get("location", {})
            location = location_parts.get("name", "") if isinstance(location_parts, dict) else ""
            apply_url = job.get("absolute_url", "")
            description = job.get("content", "")  # HTML; stored as-is
            date_posted = _parse_date(job.get("updated_at"))

            # Extract company name from metadata if available (Greenhouse doesn't always include it)
            # Use slug as fallback identifier
            company = job.get("company_name") or slug

            normalized = self.empty_job()
            normalized.update({
                "title": title,
                "company": company,
                "location": location,
                "remote_type": _infer_remote_type(title, location),
                "description": description,
                "apply_url": apply_url,
                "source": "greenhouse",
                "source_company_slug": slug,
                "date_posted": date_posted,
                "raw_data": json.dumps(job),
            })
            results.append(normalized)

        return results

    def fetch_many(self, slugs: list[str]) -> list[dict]:
        """Fetch jobs for multiple slugs, aggregating results."""
        all_jobs = []
        for slug in slugs:
            all_jobs.extend(self.fetch(slug))
        return all_jobs
