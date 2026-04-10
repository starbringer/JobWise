"""
tests/test_quota_tracker.py
~~~~~~~~~~~~~~~~~~~~~~~~~~~
Unit tests for src/quota_tracker.py — JSearch monthly quota management.

Covers:
- _compute_next_reset(): month-end clamping, month rollover
- QuotaTracker.ensure_initialized(): row creation
- QuotaTracker.check_and_reset(): cycle boundary detection
- QuotaTracker.remaining(): credit calculation
- QuotaTracker.consume(): deduction and exhaustion transition
- QuotaTracker.mark_exhausted(): immediate exhaustion
- QuotaTracker.is_available(): availability check

Uses an in-memory SQLite DB seeded with the real schema.

Run from the project root:
    pytest tests/test_quota_tracker.py -v
"""

import sys
from datetime import date, timedelta
from pathlib import Path

PROJECT_ROOT = Path(__file__).parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

import pytest
from src.quota_tracker import _compute_next_reset, QuotaTracker, _today as quota_today
from src.database import init_db


def _make_db():
    """Return an in-memory SQLite connection with the full schema applied."""
    return init_db(":memory:")


# ---------------------------------------------------------------------------
# _compute_next_reset()
# ---------------------------------------------------------------------------

class TestComputeNextReset:
    def test_advances_one_month(self):
        result = _compute_next_reset(reset_day=1, from_date=date(2024, 3, 15))
        assert result == date(2024, 4, 1)

    def test_december_wraps_to_january(self):
        result = _compute_next_reset(reset_day=15, from_date=date(2024, 12, 1))
        assert result == date(2025, 1, 15)

    def test_clamps_day_for_short_month(self):
        """reset_day=31 in February → last day of Feb."""
        result = _compute_next_reset(reset_day=31, from_date=date(2024, 1, 15))
        # February 2024 has 29 days (leap year)
        assert result == date(2024, 2, 29)

    def test_clamps_day_for_30_day_month(self):
        """reset_day=31 in April (30 days) → April 30."""
        result = _compute_next_reset(reset_day=31, from_date=date(2024, 3, 1))
        assert result == date(2024, 4, 30)

    def test_day_within_range_not_clamped(self):
        result = _compute_next_reset(reset_day=28, from_date=date(2024, 1, 1))
        assert result == date(2024, 2, 28)

    def test_reset_day_1_always_valid(self):
        for month in range(1, 13):
            result = _compute_next_reset(reset_day=1, from_date=date(2024, month, 15))
            assert result.day == 1

    def test_non_leap_year_february_clamp(self):
        """reset_day=31 in non-leap year Feb → Feb 28."""
        result = _compute_next_reset(reset_day=31, from_date=date(2023, 1, 15))
        assert result == date(2023, 2, 28)


# ---------------------------------------------------------------------------
# QuotaTracker — ensure_initialized()
# ---------------------------------------------------------------------------

class TestEnsureInitialized:
    def test_creates_row_if_not_exists(self):
        conn = _make_db()
        qt = QuotaTracker(conn, "jsearch")
        qt.ensure_initialized(monthly_limit=500, reset_day=1)
        row = conn.execute(
            "SELECT * FROM api_quota WHERE service = 'jsearch'"
        ).fetchone()
        assert row is not None
        assert row["monthly_limit"] == 500
        assert row["requests_used"] == 0
        assert not bool(row["is_exhausted"])

    def test_idempotent_second_call(self):
        """Calling ensure_initialized twice must not raise or change the row."""
        conn = _make_db()
        qt = QuotaTracker(conn, "jsearch")
        qt.ensure_initialized(monthly_limit=500, reset_day=1)
        qt.ensure_initialized(monthly_limit=500, reset_day=1)
        rows = conn.execute(
            "SELECT COUNT(*) FROM api_quota WHERE service = 'jsearch'"
        ).fetchone()[0]
        assert rows == 1

    def test_next_reset_date_set(self):
        conn = _make_db()
        qt = QuotaTracker(conn, "jsearch")
        qt.ensure_initialized(monthly_limit=500, reset_day=15)
        row = conn.execute(
            "SELECT next_reset_date FROM api_quota WHERE service = 'jsearch'"
        ).fetchone()
        next_reset = date.fromisoformat(row["next_reset_date"])
        assert next_reset.day == 15


# ---------------------------------------------------------------------------
# QuotaTracker — remaining() and consume()
# ---------------------------------------------------------------------------

class TestRemainingAndConsume:
    def _setup(self, conn, monthly_limit=100, requests_used=0):
        conn.execute(
            """
            INSERT INTO api_quota (service, requests_used, monthly_limit, reset_day, next_reset_date, is_exhausted)
            VALUES ('jsearch', ?, ?, 1, date('now', '+1 month'), FALSE)
            """,
            (requests_used, monthly_limit),
        )
        conn.commit()
        return QuotaTracker(conn, "jsearch")

    def test_remaining_full_quota(self):
        conn = _make_db()
        qt = self._setup(conn, monthly_limit=100, requests_used=0)
        assert qt.remaining() == 100

    def test_remaining_after_partial_use(self):
        conn = _make_db()
        qt = self._setup(conn, monthly_limit=100, requests_used=40)
        assert qt.remaining() == 60

    def test_remaining_never_negative(self):
        conn = _make_db()
        qt = self._setup(conn, monthly_limit=100, requests_used=150)
        assert qt.remaining() == 0

    def test_remaining_returns_zero_if_no_row(self):
        conn = _make_db()
        qt = QuotaTracker(conn, "jsearch")
        assert qt.remaining() == 0

    def test_consume_decrements_used(self):
        conn = _make_db()
        qt = self._setup(conn, monthly_limit=100, requests_used=10)
        qt.consume(5)
        assert qt.remaining() == 85

    def test_consume_marks_exhausted_at_limit(self):
        conn = _make_db()
        qt = self._setup(conn, monthly_limit=100, requests_used=95)
        qt.consume(5)
        assert not qt.is_available()

    def test_consume_marks_exhausted_over_limit(self):
        conn = _make_db()
        qt = self._setup(conn, monthly_limit=100, requests_used=98)
        qt.consume(5)  # goes to 103 ≥ 100
        assert not qt.is_available()

    def test_consume_does_nothing_if_no_row(self):
        """consume() on uninitialized tracker must not raise."""
        conn = _make_db()
        qt = QuotaTracker(conn, "jsearch")
        qt.consume(10)  # should silently no-op


# ---------------------------------------------------------------------------
# QuotaTracker — mark_exhausted() and is_available()
# ---------------------------------------------------------------------------

class TestMarkExhaustedAndIsAvailable:
    def _setup(self, conn):
        conn.execute(
            """
            INSERT INTO api_quota (service, requests_used, monthly_limit, reset_day, next_reset_date, is_exhausted)
            VALUES ('jsearch', 0, 100, 1, date('now', '+1 month'), FALSE)
            """
        )
        conn.commit()
        return QuotaTracker(conn, "jsearch")

    def test_initially_available(self):
        conn = _make_db()
        qt = self._setup(conn)
        assert qt.is_available() is True

    def test_mark_exhausted_makes_unavailable(self):
        conn = _make_db()
        qt = self._setup(conn)
        qt.mark_exhausted()
        assert qt.is_available() is False

    def test_is_available_false_if_no_row(self):
        conn = _make_db()
        qt = QuotaTracker(conn, "jsearch")
        assert qt.is_available() is False


# ---------------------------------------------------------------------------
# QuotaTracker — check_and_reset()
# ---------------------------------------------------------------------------

class TestCheckAndReset:
    def test_no_reset_before_date(self):
        conn = _make_db()
        # Use UTC-based today (same clock as quota_tracker) to avoid timezone mismatch
        tomorrow = (quota_today() + timedelta(days=1)).isoformat()
        conn.execute(
            """
            INSERT INTO api_quota (service, requests_used, monthly_limit, reset_day, next_reset_date, is_exhausted)
            VALUES ('jsearch', 50, 100, 1, ?, FALSE)
            """,
            (tomorrow,),
        )
        conn.commit()
        qt = QuotaTracker(conn, "jsearch")
        reset = qt.check_and_reset()
        assert reset is False
        # requests_used should still be 50
        row = conn.execute(
            "SELECT requests_used FROM api_quota WHERE service = 'jsearch'"
        ).fetchone()
        assert row["requests_used"] == 50

    def test_reset_on_or_after_date(self):
        conn = _make_db()
        yesterday = (date.today() - timedelta(days=1)).isoformat()
        conn.execute(
            """
            INSERT INTO api_quota (service, requests_used, monthly_limit, reset_day, next_reset_date, is_exhausted)
            VALUES ('jsearch', 50, 100, 1, ?, TRUE)
            """,
            (yesterday,),
        )
        conn.commit()
        qt = QuotaTracker(conn, "jsearch")
        reset = qt.check_and_reset()
        assert reset is True
        row = conn.execute(
            "SELECT requests_used, is_exhausted FROM api_quota WHERE service = 'jsearch'"
        ).fetchone()
        assert row["requests_used"] == 0
        assert not bool(row["is_exhausted"])

    def test_reset_updates_next_reset_date(self):
        conn = _make_db()
        yesterday = (date.today() - timedelta(days=1)).isoformat()
        conn.execute(
            """
            INSERT INTO api_quota (service, requests_used, monthly_limit, reset_day, next_reset_date, is_exhausted)
            VALUES ('jsearch', 80, 100, 15, ?, FALSE)
            """,
            (yesterday,),
        )
        conn.commit()
        qt = QuotaTracker(conn, "jsearch")
        qt.check_and_reset()
        row = conn.execute(
            "SELECT next_reset_date FROM api_quota WHERE service = 'jsearch'"
        ).fetchone()
        new_reset = date.fromisoformat(row["next_reset_date"])
        assert new_reset > date.today()
        assert new_reset.day == 15

    def test_no_reset_if_no_row(self):
        conn = _make_db()
        qt = QuotaTracker(conn, "jsearch")
        result = qt.check_and_reset()
        assert result is False
