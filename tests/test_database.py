"""
tests/test_database.py
~~~~~~~~~~~~~~~~~~~~~~
Unit tests for src/database.py — SQLite read/write operations.

Uses an in-memory SQLite DB initialised with the full migration stack.

Covers:
- upsert_profile(): idempotent row creation
- insert_job(): deduplication via job_key, fetched_for_profile_id stamping
- update_profile_field(): add/remove/set actions, dot-notation paths, type coercion
- get_profile() / update_profile_structured_content()
- deduplicator.process(): profile_id stamping, first-discoverer wins on duplicates

Run from the project root:
    pytest tests/test_database.py -v
"""

import json
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

import pytest
from src.database import (
    init_db,
    upsert_profile,
    get_profile,
    update_profile_structured_content,
    update_profile_field,
    insert_job,
)
from src.deduplicator import process as dedup_process


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def conn():
    """In-memory DB with full schema applied."""
    c = init_db(":memory:")
    yield c
    c.close()


def _make_job(job_key="abc123def456", title="Software Engineer",
              company="Acme", location="Remote", **kwargs):
    base = {
        "job_key": job_key,
        "title": title,
        "company": company,
        "location": location,
        "remote_type": "remote",
        "salary_min": None,
        "salary_max": None,
        "salary_currency": "USD",
        "salary_period": "annual",
        "salary_raw": None,
        "description": "A job description.",
        "apply_url": "https://example.com/job",
        "source": "test",
        "source_company_slug": None,
        "date_posted": None,
        "raw_data": None,
    }
    base.update(kwargs)
    return base


def _seed_profile(conn, name="alice", structured=None):
    """Insert a profile row and optionally a structured_content JSON."""
    profile_id = upsert_profile(conn, name, f"/profiles/{name}.txt")
    if structured is not None:
        update_profile_structured_content(
            conn,
            profile_id=profile_id,
            structured_content=json.dumps(structured),
            input_hash="abc123",
            input_modified_at="2024-01-01 00:00:00",
        )
    return profile_id


# ---------------------------------------------------------------------------
# upsert_profile()
# ---------------------------------------------------------------------------

class TestUpsertProfile:
    def test_creates_new_profile(self, conn):
        pid = upsert_profile(conn, "alice", "/profiles/alice.txt")
        assert isinstance(pid, int)
        assert pid > 0

    def test_idempotent_second_call(self, conn):
        pid1 = upsert_profile(conn, "alice", "/profiles/alice.txt")
        pid2 = upsert_profile(conn, "alice", "/profiles/alice.txt")
        assert pid1 == pid2

    def test_different_names_different_ids(self, conn):
        pid1 = upsert_profile(conn, "alice", "/profiles/alice.txt")
        pid2 = upsert_profile(conn, "bob", "/profiles/bob.txt")
        assert pid1 != pid2

    def test_get_profile_returns_row(self, conn):
        upsert_profile(conn, "alice", "/profiles/alice.txt")
        row = get_profile(conn, "alice")
        assert row is not None
        assert row["name"] == "alice"

    def test_get_profile_returns_none_for_unknown(self, conn):
        row = get_profile(conn, "nobody")
        assert row is None


# ---------------------------------------------------------------------------
# insert_job()
# ---------------------------------------------------------------------------

class TestInsertJob:
    def test_new_job_inserted_returns_true(self, conn):
        job = _make_job()
        result = insert_job(conn, job)
        assert result is True

    def test_duplicate_job_key_returns_false(self, conn):
        job = _make_job()
        insert_job(conn, job)
        result = insert_job(conn, job)
        assert result is False

    def test_different_keys_both_inserted(self, conn):
        j1 = _make_job(job_key="key0000000000001", title="Engineer A")
        j2 = _make_job(job_key="key0000000000002", title="Engineer B")
        assert insert_job(conn, j1) is True
        assert insert_job(conn, j2) is True

    def test_inserted_job_readable(self, conn):
        job = _make_job(title="Staff Engineer", company="BigCo")
        insert_job(conn, job)
        row = conn.execute(
            "SELECT title, company FROM jobs WHERE job_key = ?", (job["job_key"],)
        ).fetchone()
        assert row["title"] == "Staff Engineer"
        assert row["company"] == "BigCo"

    def test_job_with_salary_stored(self, conn):
        job = _make_job(salary_min=120_000, salary_max=160_000)
        insert_job(conn, job)
        row = conn.execute(
            "SELECT salary_min, salary_max FROM jobs WHERE job_key = ?", (job["job_key"],)
        ).fetchone()
        assert row["salary_min"] == 120_000
        assert row["salary_max"] == 160_000

    def test_fetched_for_profile_id_stored(self, conn):
        """fetched_for_profile_id is saved when provided, visible in the jobs row."""
        pid = upsert_profile(conn, "alice", "/profiles/alice.txt")
        job = _make_job(fetched_for_profile_id=pid)
        insert_job(conn, job)
        row = conn.execute(
            "SELECT fetched_for_profile_id FROM jobs WHERE job_key = ?", (job["job_key"],)
        ).fetchone()
        assert row["fetched_for_profile_id"] == pid

    def test_fetched_for_profile_id_null_when_not_provided(self, conn):
        """fetched_for_profile_id is NULL when not supplied (backwards-compatible)."""
        job = _make_job()
        insert_job(conn, job)
        row = conn.execute(
            "SELECT fetched_for_profile_id FROM jobs WHERE job_key = ?", (job["job_key"],)
        ).fetchone()
        assert row["fetched_for_profile_id"] is None

    def test_duplicate_insert_keeps_original_fetched_for(self, conn):
        """INSERT OR IGNORE: second insert with a different profile_id is ignored —
        the first discoverer wins and keeps its fetched_for_profile_id."""
        pid1 = upsert_profile(conn, "alice", "/profiles/alice.txt")
        pid2 = upsert_profile(conn, "bob",   "/profiles/bob.txt")
        job = _make_job(fetched_for_profile_id=pid1)
        insert_job(conn, job)
        # Second insert with different profile — should be ignored
        job2 = {**job, "fetched_for_profile_id": pid2}
        result = insert_job(conn, job2)
        assert result is False
        row = conn.execute(
            "SELECT fetched_for_profile_id FROM jobs WHERE job_key = ?", (job["job_key"],)
        ).fetchone()
        assert row["fetched_for_profile_id"] == pid1  # first discoverer preserved


# ---------------------------------------------------------------------------
# deduplicator.process() — fetched_for stamping
# ---------------------------------------------------------------------------

class TestDeduplicatorFetchedFor:
    """
    Issue 6 — fetched_for_profile_id: jobs fetched during a profile's pipeline
    must be stamped with that profile's ID so the database view can show the
    association even before scoring runs.  First discoverer wins on duplicates.
    """

    def test_profile_id_stamped_on_new_job(self, conn):
        """process() with profile_id stamps fetched_for_profile_id on the inserted row."""
        pid = upsert_profile(conn, "alice", "/profiles/alice.txt")
        jobs = [_make_job(title="Unique Job Alpha", company="CompanyA")]
        dedup_process(conn, jobs, profile_id=pid)
        row = conn.execute(
            "SELECT fetched_for_profile_id FROM jobs WHERE job_key = ?", (jobs[0]["job_key"],)
        ).fetchone()
        assert row["fetched_for_profile_id"] == pid

    def test_no_profile_id_leaves_null(self, conn):
        """process() without profile_id leaves fetched_for_profile_id as NULL."""
        jobs = [_make_job(title="Unique Job Beta", company="CompanyB")]
        dedup_process(conn, jobs)
        row = conn.execute(
            "SELECT fetched_for_profile_id FROM jobs WHERE job_key = ?", (jobs[0]["job_key"],)
        ).fetchone()
        assert row["fetched_for_profile_id"] is None

    def test_first_discoverer_wins_on_duplicate(self, conn):
        """When a duplicate job is submitted by a second profile, the original
        fetched_for_profile_id is preserved (INSERT OR IGNORE)."""
        pid1 = upsert_profile(conn, "alice", "/profiles/alice.txt")
        pid2 = upsert_profile(conn, "bob",   "/profiles/bob.txt")
        jobs = [_make_job(title="Unique Job Gamma", company="CompanyC")]
        dedup_process(conn, jobs, profile_id=pid1)
        key = jobs[0]["job_key"]
        dedup_process(conn, [_make_job(title="Unique Job Gamma", company="CompanyC")], profile_id=pid2)
        row = conn.execute(
            "SELECT fetched_for_profile_id FROM jobs WHERE job_key = ?", (key,)
        ).fetchone()
        assert row["fetched_for_profile_id"] == pid1


# ---------------------------------------------------------------------------
# update_profile_field() — "add" action
# ---------------------------------------------------------------------------

class TestUpdateProfileFieldAdd:
    def _profile_with(self, conn, data: dict, name="alice"):
        _seed_profile(conn, name=name, structured=data)
        return name

    def test_add_to_list_field(self, conn):
        name = self._profile_with(conn, {"technical_skills": ["Python"]})
        update_profile_field(conn, name, "technical_skills", "add", "Go")
        row = get_profile(conn, name)
        content = json.loads(row["structured_content"])
        assert "Go" in content["technical_skills"]
        assert "Python" in content["technical_skills"]

    def test_add_no_duplicate(self, conn):
        name = self._profile_with(conn, {"technical_skills": ["Python"]})
        update_profile_field(conn, name, "technical_skills", "add", "Python")
        row = get_profile(conn, name)
        content = json.loads(row["structured_content"])
        assert content["technical_skills"].count("Python") == 1

    def test_add_to_empty_list(self, conn):
        name = self._profile_with(conn, {"must_haves": []})
        update_profile_field(conn, name, "must_haves", "add", "Remote")
        row = get_profile(conn, name)
        content = json.loads(row["structured_content"])
        assert "Remote" in content["must_haves"]

    def test_add_to_nested_field(self, conn):
        name = self._profile_with(conn, {
            "hard_requirements": {"company_exclude": []}
        })
        update_profile_field(conn, name, "hard_requirements.company_exclude", "add", "BadCorp")
        row = get_profile(conn, name)
        content = json.loads(row["structured_content"])
        assert "BadCorp" in content["hard_requirements"]["company_exclude"]


# ---------------------------------------------------------------------------
# update_profile_field() — "remove" action
# ---------------------------------------------------------------------------

class TestUpdateProfileFieldRemove:
    def _profile_with(self, conn, data: dict, name="alice"):
        _seed_profile(conn, name=name, structured=data)
        return name

    def test_remove_existing_item(self, conn):
        name = self._profile_with(conn, {"technical_skills": ["Python", "Go"]})
        update_profile_field(conn, name, "technical_skills", "remove", "Go")
        row = get_profile(conn, name)
        content = json.loads(row["structured_content"])
        assert "Go" not in content["technical_skills"]
        assert "Python" in content["technical_skills"]

    def test_remove_non_existent_is_noop(self, conn):
        name = self._profile_with(conn, {"technical_skills": ["Python"]})
        update_profile_field(conn, name, "technical_skills", "remove", "Rust")
        row = get_profile(conn, name)
        content = json.loads(row["structured_content"])
        assert content["technical_skills"] == ["Python"]

    def test_remove_from_nested_field(self, conn):
        name = self._profile_with(conn, {
            "hard_requirements": {"locations": ["Virginia", "Maryland"]}
        })
        update_profile_field(conn, name, "hard_requirements.locations", "remove", "Maryland")
        row = get_profile(conn, name)
        content = json.loads(row["structured_content"])
        assert "Maryland" not in content["hard_requirements"]["locations"]
        assert "Virginia" in content["hard_requirements"]["locations"]


# ---------------------------------------------------------------------------
# update_profile_field() — "set" action
# ---------------------------------------------------------------------------

class TestUpdateProfileFieldSet:
    def _profile_with(self, conn, data: dict, name="alice"):
        _seed_profile(conn, name=name, structured=data)
        return name

    def test_set_scalar_string(self, conn):
        name = self._profile_with(conn, {"work_style": "remote"})
        update_profile_field(conn, name, "work_style", "set", "hybrid")
        row = get_profile(conn, name)
        content = json.loads(row["structured_content"])
        assert content["work_style"] == "hybrid"

    def test_set_numeric_field_coerces_string_to_int(self, conn):
        name = self._profile_with(conn, {"salary_min": None})
        update_profile_field(conn, name, "salary_min", "set", "120000")
        row = get_profile(conn, name)
        content = json.loads(row["structured_content"])
        assert content["salary_min"] == 120_000
        assert isinstance(content["salary_min"], int)

    def test_set_numeric_field_null_string_becomes_none(self, conn):
        name = self._profile_with(conn, {"salary_min": 100_000})
        update_profile_field(conn, name, "salary_min", "set", "null")
        row = get_profile(conn, name)
        content = json.loads(row["structured_content"])
        assert content["salary_min"] is None

    def test_set_bool_field_true(self, conn):
        name = self._profile_with(conn, {
            "hard_requirements": {"has_clearance": None}
        })
        update_profile_field(conn, name, "hard_requirements.has_clearance", "set", "true")
        row = get_profile(conn, name)
        content = json.loads(row["structured_content"])
        assert content["hard_requirements"]["has_clearance"] is True

    def test_set_bool_field_false(self, conn):
        name = self._profile_with(conn, {
            "hard_requirements": {"has_clearance": True}
        })
        update_profile_field(conn, name, "hard_requirements.has_clearance", "set", "false")
        row = get_profile(conn, name)
        content = json.loads(row["structured_content"])
        assert content["hard_requirements"]["has_clearance"] is False

    def test_set_nested_scalar(self, conn):
        name = self._profile_with(conn, {
            "hard_requirements": {"remote_type": None}
        })
        update_profile_field(conn, name, "hard_requirements.remote_type", "set", "remote")
        row = get_profile(conn, name)
        content = json.loads(row["structured_content"])
        assert content["hard_requirements"]["remote_type"] == "remote"

    def test_set_null_string_becomes_none(self, conn):
        name = self._profile_with(conn, {"work_style": "remote"})
        update_profile_field(conn, name, "work_style", "set", "null")
        row = get_profile(conn, name)
        content = json.loads(row["structured_content"])
        assert content["work_style"] is None

    def test_set_no_structured_content_is_noop(self, conn):
        """update_profile_field on a profile with no structured_content must not raise."""
        upsert_profile(conn, "empty", "/profiles/empty.txt")
        # Should silently no-op
        update_profile_field(conn, "empty", "technical_skills", "add", "Python")
