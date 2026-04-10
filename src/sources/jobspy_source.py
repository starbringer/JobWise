"""
jobspy_source.py — JobSpy general keyword discovery.
Always runs as part of the pipeline for broad, company-agnostic job discovery.
Searches LinkedIn + Indeed (no proxy in v1).
"""

import json
import logging

from src.sources.base import BaseSource

logger = logging.getLogger(__name__)

REMOTE_KEYWORDS = {
    "remote": "remote",
    "hybrid": "hybrid",
    "on-site": "on-site",
    "onsite": "on-site",
    "in office": "on-site",
}


def _infer_remote_type(job_type: str, title: str, location: str) -> str:
    text = f"{job_type} {title} {location}".lower()
    for kw, label in REMOTE_KEYWORDS.items():
        if kw in text:
            return label
    return "unknown"


def _normalize_salary(value, period: str) -> int | None:
    """Normalize salary to annual USD integer."""
    if value is None:
        return None
    try:
        v = float(str(value).replace(",", "").replace("$", "").strip())
    except (ValueError, TypeError):
        return None
    if v != v:  # NaN check
        return None
    if period == "hourly":
        return int(v * 2080)
    if period == "monthly":
        return int(v * 12)
    return int(v)


class JobSpySource(BaseSource):
    def __init__(self, sites: list[str] | None = None, results_per_site: int = 25):
        self.sites = sites or ["linkedin", "indeed"]
        self.results_per_site = results_per_site

    def fetch(self, query: str, location: str = "", hours_old: int = 720) -> list[dict]:
        """
        Search via JobSpy for a single query.
        Returns normalized job dicts.

        hours_old: limit results to jobs posted within this many hours.
                   720 = ~30 days (full search mode).
                   Set based on time since last run for incremental mode.
                   Note: LinkedIn and Indeed have restrictions — hours_old cannot be
                   combined with job_type/is_remote on those platforms.
        """
        try:
            from jobspy import scrape_jobs
        except ImportError:
            logger.error(
                "[jobspy] python-jobspy is not installed. "
                "Run: pip install python-jobspy"
            )
            return []

        logger.info(
            f"[jobspy] Searching: '{query}' location='{location}' "
            f"hours_old={hours_old} sites={self.sites}"
        )

        try:
            df = scrape_jobs(
                site_name=self.sites,
                search_term=query,
                location=location or None,
                results_wanted=self.results_per_site,
                hours_old=hours_old,
                country_indeed="USA",
            )
        except Exception as e:
            logger.error(f"[jobspy] Scrape failed for '{query}': {e}")
            return []

        if df is None or df.empty:
            logger.info(f"[jobspy] No results for '{query}'")
            return []

        logger.info(f"[jobspy] {len(df)} jobs returned for '{query}'")

        results = []
        for _, row in df.iterrows():
            title = str(row.get("title") or "")
            company = str(row.get("company") or "")
            location_str = str(row.get("location") or "")
            job_type = str(row.get("job_type") or "")
            apply_url = str(row.get("job_url") or row.get("job_url_direct") or "")
            description = str(row.get("description") or "")
            if description.strip().lower() == "nan":
                description = ""

            # Date
            date_posted = None
            dp = row.get("date_posted")
            if dp is not None:
                try:
                    date_posted = str(dp)[:10]
                except Exception:
                    pass

            # Salary
            salary_min = _normalize_salary(row.get("min_amount"), str(row.get("interval") or ""))
            salary_max = _normalize_salary(row.get("max_amount"), str(row.get("interval") or ""))
            salary_raw = None
            if salary_min or salary_max:
                currency = str(row.get("currency") or "USD")
                interval = str(row.get("interval") or "")
                salary_raw = f"{salary_min}-{salary_max} {currency} {interval}".strip()

            site = str(row.get("site") or "jobspy")

            normalized = self.empty_job()
            normalized.update({
                "title": title,
                "company": company,
                "location": location_str,
                "remote_type": _infer_remote_type(job_type, title, location_str),
                "salary_min": salary_min,
                "salary_max": salary_max,
                "salary_raw": salary_raw,
                "description": description,
                "apply_url": apply_url,
                "source": f"jobspy_{site}",
                "date_posted": date_posted,
                "raw_data": json.dumps(row.to_dict(), default=str),
            })
            results.append(normalized)

        return results

    def fetch_many(self, queries: list[dict], hours_old: int = 720) -> list[dict]:
        """
        Execute multiple queries.
        hours_old: passed to each fetch call (overridden by per-query 'hours_old' key if present).
        """
        all_jobs = []
        for q in queries:
            all_jobs.extend(
                self.fetch(
                    q["query"],
                    q.get("location", ""),
                    hours_old=q.get("hours_old", hours_old),
                )
            )
        return all_jobs
