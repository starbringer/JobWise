"""
base.py — Abstract base class for all job sources.
Each source adapter returns a list of normalized job dicts.
"""

from abc import ABC, abstractmethod


class BaseSource(ABC):
    """
    All source adapters must implement fetch().
    Returns a list of job dicts ready for the deduplicator.
    Each dict must include at minimum:
      title, company, location, source, apply_url
    All other fields default to None if unavailable.
    """

    @abstractmethod
    def fetch(self, **kwargs) -> list[dict]:
        ...

    @staticmethod
    def empty_job() -> dict:
        return {
            "job_key": None,          # filled by deduplicator
            "title": None,
            "company": None,
            "location": None,
            "remote_type": None,      # "remote" | "hybrid" | "on-site" | "unknown"
            "salary_min": None,
            "salary_max": None,
            "salary_currency": "USD",
            "salary_period": "annual",
            "salary_raw": None,
            "description": None,
            "apply_url": None,
            "source": None,
            "source_company_slug": None,
            "date_posted": None,
            "raw_data": None,
        }
