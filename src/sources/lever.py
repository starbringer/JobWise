"""
lever.py — Lever public API client.
Endpoint: GET https://api.lever.co/v0/postings/{slug}?mode=json
No auth, no rate limit.
"""

import json
import logging
from datetime import datetime, timezone

import requests

from src.sources.base import BaseSource

logger = logging.getLogger(__name__)

BASE_URL = "https://api.lever.co/v0/postings/{slug}?mode=json"

REMOTE_KEYWORDS = {
    "remote": "remote",
    "hybrid": "hybrid",
    "on-site": "on-site",
    "onsite": "on-site",
    "in office": "on-site",
    "in-office": "on-site",
}


def _infer_remote_type(location: str, description: str) -> str:
    text = f"{location} {description[:500]}".lower()
    for kw, label in REMOTE_KEYWORDS.items():
        if kw in text:
            return label
    return "unknown"


def _parse_lever_timestamp(ts: int | None) -> str | None:
    """Lever timestamps are milliseconds since epoch."""
    if not ts:
        return None
    try:
        dt = datetime.fromtimestamp(ts / 1000, tz=timezone.utc)
        return dt.strftime("%Y-%m-%d")
    except Exception:
        return None


class LeverSource(BaseSource):
    def __init__(self, session: requests.Session | None = None):
        self.session = session or requests.Session()
        self.session.headers.update({"User-Agent": "jobwise/1.0"})

    def fetch(self, slug: str) -> list[dict]:
        """
        Fetch all live job postings for a Lever company slug.
        Returns a list of normalized job dicts.
        """
        url = BASE_URL.format(slug=slug)
        logger.info(f"[lever] Fetching jobs for slug={slug}")

        try:
            resp = self.session.get(url, timeout=15)
        except requests.RequestException as e:
            logger.error(f"[lever] Request failed for {slug}: {e}")
            return []

        if resp.status_code == 404:
            logger.warning(f"[lever] Slug not found: {slug}")
            return []
        if resp.status_code != 200:
            logger.error(f"[lever] HTTP {resp.status_code} for {slug}")
            return []

        try:
            raw_jobs = resp.json()
        except Exception as e:
            logger.error(f"[lever] Failed to parse JSON for {slug}: {e}")
            return []

        if not isinstance(raw_jobs, list):
            logger.warning(f"[lever] Unexpected response format for {slug}: {type(raw_jobs)}")
            return []

        logger.info(f"[lever] {len(raw_jobs)} jobs found for {slug}")

        results = []
        for job in raw_jobs:
            title = job.get("text", "")
            categories = job.get("categories", {})
            location = categories.get("location", "") or categories.get("allLocations", "")
            if isinstance(location, list):
                location = ", ".join(location)

            hosted_url = job.get("hostedUrl", "")
            apply_url = job.get("applyUrl", hosted_url)

            # Lever stores description in lists.description and lists.additional
            description_lists = job.get("lists", [])
            description_text = job.get("descriptionPlain", "") or job.get("description", "")
            if description_lists and not description_text:
                parts = []
                for item in description_lists:
                    parts.append(item.get("text", ""))
                    parts.append(item.get("content", ""))
                description_text = "\n".join(filter(None, parts))

            date_posted = _parse_lever_timestamp(job.get("createdAt"))

            normalized = self.empty_job()
            normalized.update({
                "title": title,
                "company": slug,   # Lever doesn't return company name in the listing
                "location": location,
                "remote_type": _infer_remote_type(location, description_text),
                "description": description_text,
                "apply_url": apply_url,
                "source": "lever",
                "source_company_slug": slug,
                "date_posted": date_posted,
                "raw_data": json.dumps(job),
            })
            results.append(normalized)

        return results

    def fetch_many(self, slugs: list[str]) -> list[dict]:
        all_jobs = []
        for slug in slugs:
            all_jobs.extend(self.fetch(slug))
        return all_jobs
