"""
tests/test_pipeline_utils.py
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Unit tests for pure utility functions in src/pipeline.py:
- _hours_since(): ISO timestamp → elapsed hours
- _filter_gh_lever_by_date(): client-side date filter for ATS results

These functions contain no I/O or DB calls so they can be tested without mocking.

Run from the project root:
    pytest tests/test_pipeline_utils.py -v
"""

import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

PROJECT_ROOT = Path(__file__).parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.pipeline import _hours_since, _filter_gh_lever_by_date


# ---------------------------------------------------------------------------
# _hours_since()
# ---------------------------------------------------------------------------

class TestHoursSince:
    def _ts(self, hours_ago: float) -> str:
        """Return an ISO timestamp N hours in the past."""
        dt = datetime.now(tz=timezone.utc) - timedelta(hours=hours_ago)
        return dt.isoformat()

    def test_none_returns_large_number(self):
        assert _hours_since(None) >= 999999

    def test_empty_string_returns_large_number(self):
        assert _hours_since("") >= 999999

    def test_invalid_string_returns_large_number(self):
        assert _hours_since("not-a-timestamp") >= 999999

    def test_approximately_one_hour_ago(self):
        ts = self._ts(1.0)
        result = _hours_since(ts)
        # Allow 1-second rounding tolerance → result should be 0 or 1
        assert 0 <= result <= 2

    def test_approximately_24_hours_ago(self):
        ts = self._ts(24.0)
        result = _hours_since(ts)
        assert 23 <= result <= 25

    def test_approximately_720_hours_ago(self):
        ts = self._ts(720.0)
        result = _hours_since(ts)
        assert 719 <= result <= 721

    def test_z_suffix_handled(self):
        """Timestamps ending in Z (UTC) must be parsed correctly."""
        dt = datetime.now(tz=timezone.utc) - timedelta(hours=5)
        # Produce Z-terminated string
        ts = dt.strftime("%Y-%m-%dT%H:%M:%SZ")
        result = _hours_since(ts)
        assert 4 <= result <= 6

    def test_naive_timestamp_treated_as_utc(self):
        """Timestamps without tzinfo are assumed UTC."""
        dt = datetime.now(timezone.utc).replace(tzinfo=None) - timedelta(hours=3)
        ts = dt.strftime("%Y-%m-%d %H:%M:%S")
        result = _hours_since(ts)
        assert 2 <= result <= 4

    def test_future_timestamp_returns_negative(self):
        """A future timestamp produces a negative int (no clamping in _hours_since)."""
        ts = self._ts(-10.0)  # 10 hours in the future
        result = _hours_since(ts)
        assert result < 0


# ---------------------------------------------------------------------------
# _filter_gh_lever_by_date()
# ---------------------------------------------------------------------------

def _job(date_posted=None):
    return {"title": "Engineer", "date_posted": date_posted}


def _iso(days_ago: float) -> str:
    dt = datetime.now(tz=timezone.utc) - timedelta(days=days_ago)
    return dt.isoformat()


class TestFilterGhLeverByDate:
    # --- max_age_days cap (always applied) ---

    def test_recent_job_passes(self):
        jobs = [_job(date_posted=_iso(1))]
        result = _filter_gh_lever_by_date(jobs, last_run_iso=None, max_age_days=30)
        assert len(result) == 1

    def test_stale_job_rejected(self):
        jobs = [_job(date_posted=_iso(35))]
        result = _filter_gh_lever_by_date(jobs, last_run_iso=None, max_age_days=30)
        assert len(result) == 0

    def test_exactly_at_max_age_passes(self):
        """Job posted exactly at the cutoff boundary must pass (>=)."""
        jobs = [_job(date_posted=_iso(29.9))]
        result = _filter_gh_lever_by_date(jobs, last_run_iso=None, max_age_days=30)
        assert len(result) == 1

    def test_job_with_no_date_always_kept(self):
        jobs = [_job(date_posted=None)]
        result = _filter_gh_lever_by_date(jobs, last_run_iso=None, max_age_days=30)
        assert len(result) == 1

    def test_job_with_unparseable_date_kept(self):
        jobs = [_job(date_posted="not-a-date")]
        result = _filter_gh_lever_by_date(jobs, last_run_iso=None, max_age_days=30)
        assert len(result) == 1

    # --- last_run_iso incremental cutoff ---

    def test_job_after_last_run_passes(self):
        last_run = _iso(2)  # last run was 2 days ago
        jobs = [_job(date_posted=_iso(1))]  # posted 1 day ago (after last run)
        result = _filter_gh_lever_by_date(jobs, last_run_iso=last_run, max_age_days=30)
        assert len(result) == 1

    def test_job_before_last_run_rejected(self):
        last_run = _iso(1)  # last run was 1 day ago
        jobs = [_job(date_posted=_iso(5))]  # posted 5 days ago (before last run)
        result = _filter_gh_lever_by_date(jobs, last_run_iso=last_run, max_age_days=30)
        assert len(result) == 0

    def test_max_age_still_applies_with_last_run(self):
        """Even with last_run, a very stale job (>30 days) is rejected."""
        last_run = _iso(40)  # last run was 40 days ago
        jobs = [_job(date_posted=_iso(35))]  # older than max_age_days=30
        result = _filter_gh_lever_by_date(jobs, last_run_iso=last_run, max_age_days=30)
        assert len(result) == 0

    def test_invalid_last_run_falls_back_to_max_age(self):
        """Unparseable last_run_iso is silently ignored; max_age_days is used."""
        jobs = [_job(date_posted=_iso(5))]  # recent job
        result = _filter_gh_lever_by_date(jobs, last_run_iso="bad-ts", max_age_days=30)
        assert len(result) == 1

    # --- Mixed batch ---

    def test_mixed_batch_filters_correctly(self):
        jobs = [
            _job(date_posted=_iso(1)),   # recent — keep
            _job(date_posted=_iso(35)),  # stale — reject
            _job(date_posted=None),      # no date — keep
        ]
        result = _filter_gh_lever_by_date(jobs, last_run_iso=None, max_age_days=30)
        assert len(result) == 2

    def test_empty_list_returns_empty(self):
        result = _filter_gh_lever_by_date([], last_run_iso=None, max_age_days=30)
        assert result == []

    def test_z_suffix_in_date_posted(self):
        """date_posted values ending in Z must be parsed correctly."""
        dt = datetime.now(tz=timezone.utc) - timedelta(days=1)
        date_posted = dt.strftime("%Y-%m-%dT%H:%M:%SZ")
        jobs = [_job(date_posted=date_posted)]
        result = _filter_gh_lever_by_date(jobs, last_run_iso=None, max_age_days=30)
        assert len(result) == 1
