"""
quota_tracker.py — JSearch (RapidAPI) monthly quota management.
Reads/writes the api_quota table in SQLite.
"""

import logging
from calendar import monthrange
from datetime import date, datetime, timezone

logger = logging.getLogger(__name__)


def _today() -> date:
    return datetime.now(tz=timezone.utc).date()


def _compute_next_reset(reset_day: int, from_date: date | None = None) -> date:
    """
    Compute the next reset date given a day-of-month reset_day.
    Advances to the next month. Clamps to the last day of the month if needed
    (e.g. reset_day=31 in a 30-day month → last day of month).
    """
    base = from_date or _today()
    # Move to the next month
    if base.month == 12:
        year, month = base.year + 1, 1
    else:
        year, month = base.year, base.month + 1

    # Clamp day to the last day of the target month
    last_day = monthrange(year, month)[1]
    day = min(reset_day, last_day)
    return date(year, month, day)


class QuotaTracker:
    def __init__(self, conn, service: str = "jsearch"):
        self.conn = conn
        self.service = service

    def _row(self):
        return self.conn.execute(
            "SELECT * FROM api_quota WHERE service = ?", (self.service,)
        ).fetchone()

    def ensure_initialized(self, monthly_limit: int, reset_day: int) -> None:
        """
        Insert the quota row if it doesn't exist yet.
        Called once at pipeline startup with values from config.
        """
        row = self._row()
        if row is None:
            next_reset = _compute_next_reset(reset_day)
            self.conn.execute(
                """
                INSERT OR IGNORE INTO api_quota
                  (service, requests_used, monthly_limit, reset_day, next_reset_date, is_exhausted)
                VALUES (?, 0, ?, ?, ?, FALSE)
                """,
                (self.service, monthly_limit, reset_day, next_reset.isoformat()),
            )
            self.conn.commit()
            logger.info(
                f"[quota] Initialized {self.service}: limit={monthly_limit}, "
                f"reset_day={reset_day}, next_reset={next_reset}"
            )

    def check_and_reset(self) -> bool:
        """
        If today >= next_reset_date, reset the counter.
        Returns True if a reset occurred.
        """
        row = self._row()
        if row is None:
            return False

        next_reset = date.fromisoformat(row["next_reset_date"])
        today = _today()

        if today >= next_reset:
            new_next_reset = _compute_next_reset(row["reset_day"], from_date=today)
            self.conn.execute(
                """
                UPDATE api_quota
                SET requests_used = 0,
                    is_exhausted = FALSE,
                    next_reset_date = ?,
                    last_updated = CURRENT_TIMESTAMP
                WHERE service = ?
                """,
                (new_next_reset.isoformat(), self.service),
            )
            self.conn.commit()
            logger.info(
                f"[quota] {self.service} quota reset. Next reset: {new_next_reset}"
            )
            return True
        return False

    def is_available(self) -> bool:
        row = self._row()
        if row is None:
            return False
        return not bool(row["is_exhausted"])

    def remaining(self) -> int:
        row = self._row()
        if row is None:
            return 0
        return max(0, row["monthly_limit"] - row["requests_used"])

    def consume(self, count: int = 1) -> None:
        """Record that `count` requests were used. Mark exhausted if limit reached."""
        row = self._row()
        if row is None:
            return
        new_used = row["requests_used"] + count
        exhausted = new_used >= row["monthly_limit"]
        self.conn.execute(
            """
            UPDATE api_quota
            SET requests_used = ?,
                is_exhausted = ?,
                last_updated = CURRENT_TIMESTAMP
            WHERE service = ?
            """,
            (new_used, exhausted, self.service),
        )
        self.conn.commit()
        logger.info(
            f"[quota] {self.service}: used {new_used}/{row['monthly_limit']} "
            f"({'EXHAUSTED' if exhausted else 'ok'})"
        )

    def mark_exhausted(self) -> None:
        self.conn.execute(
            "UPDATE api_quota SET is_exhausted = TRUE, last_updated = CURRENT_TIMESTAMP WHERE service = ?",
            (self.service,),
        )
        self.conn.commit()
        logger.warning(f"[quota] {self.service} marked exhausted.")

    def cycle_end(self) -> date | None:
        row = self._row()
        if row is None:
            return None
        return date.fromisoformat(row["next_reset_date"])
