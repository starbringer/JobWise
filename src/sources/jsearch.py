"""
jsearch.py — JSearch (RapidAPI) client.
Endpoint: GET https://jsearch.p.rapidapi.com/search
Auth: X-RapidAPI-Key header
Rate limit: 200 requests/month (free tier). Each call with num_pages=10 returns up to 100 results.

Also performs ATS slug auto-discovery from apply_options URLs.
"""

import json
import logging
import os
import re
from datetime import date, datetime, timedelta, timezone

import requests

from src.sources.base import BaseSource

logger = logging.getLogger(__name__)

JSEARCH_URL = "https://jsearch.p.rapidapi.com/search"
JSEARCH_HOST = "jsearch.p.rapidapi.com"

# Patterns to extract Greenhouse/Lever slugs from apply URLs
GH_PATTERN = re.compile(r"boards\.greenhouse\.io/([^/?#]+)", re.IGNORECASE)
LEVER_PATTERN = re.compile(r"jobs\.lever\.co/([^/?#]+)", re.IGNORECASE)

REMOTE_KEYWORDS = {
    "remote": "remote",
    "hybrid": "hybrid",
    "on-site": "on-site",
    "onsite": "on-site",
    "in office": "on-site",
    "in-office": "on-site",
}


def _infer_remote_type(job_type: str, title: str, location: str) -> str:
    text = f"{job_type} {title} {location}".lower()
    for kw, label in REMOTE_KEYWORDS.items():
        if kw in text:
            return label
    return "unknown"


def _parse_relative_date(date_str: str | None) -> str | None:
    """
    Convert JSearch relative dates like 'Today', '3 days ago', '1 month ago'
    to YYYY-MM-DD absolute dates.
    """
    if not date_str:
        return None
    today = datetime.now(tz=timezone.utc).date()
    s = date_str.lower().strip()
    if s in ("today", "just posted", ""):
        return today.isoformat()
    m = re.match(r"(\d+)\s+day", s)
    if m:
        return (today - timedelta(days=int(m.group(1)))).isoformat()
    m = re.match(r"(\d+)\s+hour", s)
    if m:
        return today.isoformat()
    m = re.match(r"(\d+)\s+month", s)
    if m:
        return (today - timedelta(days=int(m.group(1)) * 30)).isoformat()
    m = re.match(r"(\d+)\s+week", s)
    if m:
        return (today - timedelta(weeks=int(m.group(1)))).isoformat()
    # Already an ISO date?
    try:
        datetime.strptime(date_str[:10], "%Y-%m-%d")
        return date_str[:10]
    except ValueError:
        return None


def _parse_salary(job: dict) -> dict:
    """Extract and normalize salary info from a JSearch job dict."""
    salary_min = job.get("job_min_salary")
    salary_max = job.get("job_max_salary")
    salary_period = (job.get("job_salary_period") or "").lower()
    salary_currency = job.get("job_salary_currency") or "USD"
    salary_raw = None

    if salary_min or salary_max:
        salary_raw = f"{salary_min}-{salary_max} {salary_currency} {salary_period}".strip()

    # Normalize to annual
    if salary_period == "hourly":
        if salary_min:
            salary_min = int(float(salary_min) * 2080)
        if salary_max:
            salary_max = int(float(salary_max) * 2080)
        salary_period = "annual"
    elif salary_period == "monthly":
        if salary_min:
            salary_min = int(float(salary_min) * 12)
        if salary_max:
            salary_max = int(float(salary_max) * 12)
        salary_period = "annual"
    elif salary_period in ("yearly", "annual", ""):
        salary_period = "annual"
        if salary_min:
            salary_min = int(float(salary_min))
        if salary_max:
            salary_max = int(float(salary_max))

    return {
        "salary_min": salary_min or None,
        "salary_max": salary_max or None,
        "salary_currency": salary_currency,
        "salary_period": salary_period or "annual",
        "salary_raw": salary_raw,
    }


def _extract_ats_slugs(apply_options: list) -> list[tuple[str, str]]:
    """
    Scan apply_options URLs for Greenhouse/Lever slugs.
    Returns list of (ats, slug) tuples for auto-discovery.
    """
    found = []
    for opt in apply_options or []:
        url = opt.get("link", "") or opt.get("url", "")
        m = GH_PATTERN.search(url)
        if m:
            found.append(("greenhouse", m.group(1).lower()))
        m = LEVER_PATTERN.search(url)
        if m:
            found.append(("lever", m.group(1).lower()))
    return found


class JSearchSource(BaseSource):
    def __init__(self, api_key: str | None = None, session: requests.Session | None = None):
        self.api_key = api_key or os.environ.get("JSEARCH_API_KEY", "")
        if not self.api_key:
            raise ValueError(
                "JSearch API key not set. Add JSEARCH_API_KEY to your .env file."
            )
        self.session = session or requests.Session()
        self.session.headers.update({
            "X-RapidAPI-Key": self.api_key,
            "X-RapidAPI-Host": JSEARCH_HOST,
        })
        self._discovered_slugs: list[tuple[str, str]] = []

    @property
    def discovered_slugs(self) -> list[tuple[str, str]]:
        """ATS slugs discovered in the last fetch_many() call."""
        return self._discovered_slugs

    def fetch(
        self,
        query: str,
        location: str = "",
        num_pages: int = 10,
        date_posted: str = "month",
        extra_params: dict | None = None,
    ) -> list[dict]:
        """
        Search JSearch for a single query.
        Returns normalized job dicts. May raise RuntimeError on quota exhaustion (429).

        date_posted: "today" | "3days" | "week" | "month" | "all"
        extra_params: additional JSearch API params (e.g. job_is_remote, min_salary)
        """
        q = f"{query} {location}".strip() if location else query
        params = {
            "query": q,
            "num_pages": str(num_pages),
            "date_posted": date_posted,
        }
        if extra_params:
            params.update(extra_params)
        logger.info(f"[jsearch] Searching: '{q}' (num_pages={num_pages})")

        try:
            resp = self.session.get(JSEARCH_URL, params=params, timeout=30)
        except requests.RequestException as e:
            logger.error(f"[jsearch] Request failed: {e}")
            return []

        if resp.status_code == 429:
            logger.warning("[jsearch] 429 — quota exhausted.")
            raise QuotaExhaustedException("JSearch quota exhausted (HTTP 429)")

        if resp.status_code != 200:
            logger.error(f"[jsearch] HTTP {resp.status_code}: {resp.text[:200]}")
            return []

        try:
            data = resp.json()
        except Exception as e:
            logger.error(f"[jsearch] Failed to parse JSON: {e}")
            return []

        raw_jobs = data.get("data", [])
        logger.info(f"[jsearch] {len(raw_jobs)} jobs returned for '{q}'")

        results = []
        for job in raw_jobs:
            title = job.get("job_title", "")
            company = job.get("employer_name", "")
            location_str = job.get("job_city", "")
            state = job.get("job_state", "")
            country = job.get("job_country", "")
            if state:
                location_str = f"{location_str}, {state}".strip(", ")
            elif country and country != "US":
                location_str = f"{location_str}, {country}".strip(", ")

            job_type = job.get("job_employment_type", "")
            apply_url = job.get("job_apply_link", "")
            description = job.get("job_description", "")
            date_posted = _parse_relative_date(job.get("job_posted_at_datetime_utc") or job.get("job_posted_at_timestamp"))
            if not date_posted:
                date_posted = _parse_relative_date(job.get("job_posted_at"))

            salary_info = _parse_salary(job)

            # ATS slug auto-discovery
            apply_options = job.get("apply_options", [])
            slugs = _extract_ats_slugs(apply_options)
            self._discovered_slugs.extend(slugs)

            normalized = self.empty_job()
            normalized.update({
                "title": title,
                "company": company,
                "location": location_str,
                "remote_type": _infer_remote_type(job_type, title, location_str),
                "description": description,
                "apply_url": apply_url,
                "source": "jsearch",
                "date_posted": date_posted,
                "raw_data": json.dumps(job),
                **salary_info,
            })
            results.append(normalized)

        return results

    def fetch_many(
        self,
        queries: list[dict],
        quota_tracker=None,
        date_posted: str = "month",
    ) -> list[dict]:
        """
        Execute multiple queries. Each query dict has 'query', optional 'location',
        and optional 'extra_params' dict.
        Stops if quota is exhausted (raises QuotaExhaustedException to caller).

        date_posted: default date window applied to all queries unless overridden per-query.
        """
        self._discovered_slugs = []
        all_jobs = []

        for q in queries:
            if quota_tracker and not quota_tracker.is_available():
                logger.warning("[jsearch] Quota not available, stopping early.")
                break
            try:
                jobs = self.fetch(
                    q["query"],
                    q.get("location", ""),
                    date_posted=q.get("date_posted", date_posted),
                    extra_params=q.get("extra_params"),
                )
                all_jobs.extend(jobs)
                if quota_tracker:
                    quota_tracker.consume(1)
            except QuotaExhaustedException:
                if quota_tracker:
                    quota_tracker.mark_exhausted()
                raise

        return all_jobs


class QuotaExhaustedException(Exception):
    pass
