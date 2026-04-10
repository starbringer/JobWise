"""
tests/test_deduplicator.py
~~~~~~~~~~~~~~~~~~~~~~~~~~
Unit tests for src/deduplicator.py — stable job key generation.

Covers:
- make_job_key(): normalization, idempotence, collision avoidance
- process(): dedup logic with mock DB connection

Run from the project root:
    pytest tests/test_deduplicator.py -v
"""

import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

PROJECT_ROOT = Path(__file__).parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.deduplicator import make_job_key, process


# ---------------------------------------------------------------------------
# make_job_key()
# ---------------------------------------------------------------------------

class TestMakeJobKey:
    def test_returns_16_char_hex(self):
        key = make_job_key("Software Engineer", "Google", "Seattle, WA")
        assert len(key) == 16
        assert all(c in "0123456789abcdef" for c in key)

    def test_idempotent_same_inputs(self):
        """Same inputs must always produce the same key."""
        k1 = make_job_key("Software Engineer", "Google", "Seattle, WA")
        k2 = make_job_key("Software Engineer", "Google", "Seattle, WA")
        assert k1 == k2

    def test_case_insensitive(self):
        """Keys must be equal regardless of input casing."""
        k1 = make_job_key("Software Engineer", "Google", "Seattle, WA")
        k2 = make_job_key("SOFTWARE ENGINEER", "GOOGLE", "SEATTLE, WA")
        assert k1 == k2

    def test_whitespace_collapsed(self):
        """Extra whitespace is normalized."""
        k1 = make_job_key("Software  Engineer", "Google", "Seattle, WA")
        k2 = make_job_key("Software Engineer", "Google", "Seattle, WA")
        assert k1 == k2

    def test_leading_trailing_whitespace_stripped(self):
        k1 = make_job_key("  Software Engineer  ", "  Google  ", "  Seattle, WA  ")
        k2 = make_job_key("Software Engineer", "Google", "Seattle, WA")
        assert k1 == k2

    def test_punctuation_stripped(self):
        """Punctuation (commas, periods, hyphens in non-pipe positions) is stripped."""
        k1 = make_job_key("Sr. Software Engineer", "Google, Inc.", "Seattle WA")
        k2 = make_job_key("Sr Software Engineer", "Google Inc", "Seattle WA")
        assert k1 == k2

    def test_different_companies_produce_different_keys(self):
        k1 = make_job_key("Software Engineer", "Google", "Seattle, WA")
        k2 = make_job_key("Software Engineer", "Microsoft", "Seattle, WA")
        assert k1 != k2

    def test_different_titles_produce_different_keys(self):
        k1 = make_job_key("Software Engineer", "Google", "Seattle, WA")
        k2 = make_job_key("Senior Software Engineer", "Google", "Seattle, WA")
        assert k1 != k2

    def test_different_locations_produce_different_keys(self):
        k1 = make_job_key("Software Engineer", "Google", "Seattle, WA")
        k2 = make_job_key("Software Engineer", "Google", "New York, NY")
        assert k1 != k2

    def test_empty_location_allowed(self):
        """Empty location is valid and should produce a consistent key."""
        k1 = make_job_key("Software Engineer", "Google", "")
        k2 = make_job_key("Software Engineer", "Google", "")
        assert k1 == k2
        assert len(k1) == 16

    def test_unicode_title_consistent(self):
        """Unicode characters are handled consistently."""
        k1 = make_job_key("Ingénieur Logiciel", "Société", "Paris")
        k2 = make_job_key("Ingénieur Logiciel", "Société", "Paris")
        assert k1 == k2


# ---------------------------------------------------------------------------
# process()
# ---------------------------------------------------------------------------

class TestProcess:
    def _make_job(self, title="Engineer", company="Acme", location="Remote", **kwargs):
        return {
            "title": title,
            "company": company,
            "location": location,
            "remote_type": "remote",
            "salary_min": None,
            "salary_max": None,
            "salary_currency": "USD",
            "salary_period": "annual",
            "salary_raw": None,
            "description": "A job",
            "apply_url": "https://example.com",
            "source": "test",
            "source_company_slug": None,
            "date_posted": None,
            "raw_data": None,
            **kwargs,
        }

    def test_inserts_new_jobs(self):
        """process() should call insert_job for each valid job."""
        conn = MagicMock()
        jobs = [self._make_job("Engineer", "Acme", "Remote")]

        with patch("src.deduplicator.database.insert_job", return_value=True) as mock_insert:
            total, new = process(conn, jobs)

        assert total == 1
        assert new == 1
        mock_insert.assert_called_once()

    def test_skips_job_with_missing_title(self):
        conn = MagicMock()
        jobs = [self._make_job(title="", company="Acme")]

        with patch("src.deduplicator.database.insert_job", return_value=True) as mock_insert:
            total, new = process(conn, jobs)

        assert total == 1
        assert new == 0
        mock_insert.assert_not_called()

    def test_skips_job_with_missing_company(self):
        conn = MagicMock()
        jobs = [self._make_job(title="Engineer", company="")]

        with patch("src.deduplicator.database.insert_job", return_value=True) as mock_insert:
            total, new = process(conn, jobs)

        assert total == 1
        assert new == 0
        mock_insert.assert_not_called()

    def test_counts_duplicates(self):
        """When insert_job returns False (duplicate), new_inserted stays 0."""
        conn = MagicMock()
        jobs = [self._make_job("Engineer", "Acme", "Remote")]

        with patch("src.deduplicator.database.insert_job", return_value=False):
            total, new = process(conn, jobs)

        assert total == 1
        assert new == 0

    def test_adds_job_key_to_job_dict(self):
        """process() must set job['job_key'] before inserting."""
        conn = MagicMock()
        job = self._make_job("Engineer", "Acme", "Remote")

        captured = []

        def capture_insert(conn, j):
            captured.append(j.copy())
            return True

        with patch("src.deduplicator.database.insert_job", side_effect=capture_insert):
            process(conn, [job])

        assert "job_key" in captured[0]
        assert len(captured[0]["job_key"]) == 16

    def test_empty_list_returns_zero_zero(self):
        conn = MagicMock()
        total, new = process(conn, [])
        assert total == 0
        assert new == 0

    def test_multiple_jobs_correct_counts(self):
        conn = MagicMock()
        jobs = [
            self._make_job("Engineer A", "Acme", "Remote"),
            self._make_job("Engineer B", "Acme", "Remote"),
            self._make_job("Engineer C", "Acme", "Remote"),
        ]

        # First two are new, third is a duplicate
        insert_results = [True, True, False]
        with patch("src.deduplicator.database.insert_job", side_effect=insert_results):
            total, new = process(conn, jobs)

        assert total == 3
        assert new == 2
