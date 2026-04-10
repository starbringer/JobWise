"""
tests/test_migrations.py
~~~~~~~~~~~~~~~~~~~~~~~~
Tests for database schema creation and migration correctness.

The consolidated v1_initial_schema now includes all columns from v10/v11 and
the LOWER(name) unique index from v12, and registers schema versions 1–12 so
incremental migrations are skipped on fresh installs.  These tests verify:

  1. Fresh install — tables, columns, indexes, ATS seed data, and schema
     version registration all match the expected final state.
  2. Case-insensitive profile name uniqueness — idx_profiles_name_lower
     prevents "Alice" / "alice" duplicates; upsert_profile and get_profile
     both operate case-insensitively.

Run from the project root:
    pytest tests/test_migrations.py -v
"""

import sqlite3
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

import pytest
from src.database import init_db, migrate, upsert_profile, get_profile


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _columns(conn, table: str) -> set[str]:
    """Return the set of column names for a table."""
    rows = conn.execute(f"PRAGMA table_info({table})").fetchall()
    return {row[1] for row in rows}


def _indexes(conn, table: str) -> set[str]:
    """Return the set of index names on a table."""
    rows = conn.execute(f"PRAGMA index_list({table})").fetchall()
    return {row[1] for row in rows}


def _tables(conn) -> set[str]:
    """Return all user-created table names."""
    rows = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'"
    ).fetchall()
    return {row[0] for row in rows}


def _schema_versions(conn) -> set[int]:
    return {row[0] for row in conn.execute("SELECT version FROM schema_version").fetchall()}


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def conn():
    """In-memory DB initialised with the full migration stack (fresh install)."""
    c = init_db(":memory:")
    yield c
    c.close()


# ---------------------------------------------------------------------------
# Fresh install — tables
# ---------------------------------------------------------------------------

class TestFreshInstallTables:
    def test_all_expected_tables_exist(self, conn):
        expected = {
            "schema_version", "profiles", "jobs", "profile_jobs",
            "ats_companies", "api_quota", "search_runs",
        }
        assert expected <= _tables(conn)

    def test_profiles_has_required_columns(self, conn):
        cols = _columns(conn, "profiles")
        for col in ("id", "name", "input_file", "structured_content",
                    "custom_job_titles", "ideal_cand_pairs", "created_at", "updated_at"):
            assert col in cols, f"profiles missing column: {col}"

    def test_jobs_has_fetched_for_profile_id(self, conn):
        """Column added in v10 must be present on a fresh install."""
        assert "fetched_for_profile_id" in _columns(conn, "jobs")

    def test_profile_jobs_has_saved_column(self, conn):
        """Column added in v11 must be present on a fresh install."""
        assert "saved" in _columns(conn, "profile_jobs")

    def test_profile_jobs_saved_defaults_to_false(self, conn):
        """saved DEFAULT FALSE must hold for newly inserted rows."""
        upsert_profile(conn, "alice", "/profiles/alice.txt")
        conn.execute(
            "INSERT INTO jobs (job_key, title, company, source) VALUES ('k1', 'T', 'C', 's')"
        )
        conn.execute(
            "INSERT INTO profile_jobs (profile_id, job_key) "
            "SELECT id, 'k1' FROM profiles WHERE LOWER(name)='alice'"
        )
        conn.commit()
        row = conn.execute("SELECT saved FROM profile_jobs WHERE job_key='k1'").fetchone()
        assert row[0] == 0  # FALSE / 0


# ---------------------------------------------------------------------------
# Fresh install — LOWER(name) unique index
# ---------------------------------------------------------------------------

class TestFreshInstallIndex:
    def test_lower_name_index_exists(self, conn):
        assert "idx_profiles_name_lower" in _indexes(conn, "profiles")

    def test_lower_name_index_is_unique(self, conn):
        row = conn.execute(
            "SELECT \"unique\" FROM pragma_index_list('profiles') "
            "WHERE name='idx_profiles_name_lower'"
        ).fetchone()
        assert row is not None
        assert row[0] == 1  # 1 = unique


# ---------------------------------------------------------------------------
# Fresh install — schema version registration
# ---------------------------------------------------------------------------

class TestFreshInstallSchemaVersions:
    def test_all_migration_versions_registered(self, conn):
        """Versions 1, 10, 11, 12 must all be registered so incremental
        migrations are skipped on the next startup."""
        versions = _schema_versions(conn)
        for v in (1, 10, 11, 12):
            assert v in versions, f"schema_version missing v{v}"

    def test_migrate_is_idempotent(self, conn):
        """Calling migrate() again on an already-initialised DB must be a no-op."""
        migrate(conn)  # should not raise


# ---------------------------------------------------------------------------
# Fresh install — ATS seed data
# ---------------------------------------------------------------------------

class TestFreshInstallSeedData:
    def test_greenhouse_seeds_populated(self, conn):
        count = conn.execute(
            "SELECT COUNT(*) FROM ats_companies WHERE ats='greenhouse'"
        ).fetchone()[0]
        assert count >= 10

    def test_lever_seeds_populated(self, conn):
        count = conn.execute(
            "SELECT COUNT(*) FROM ats_companies WHERE ats='lever'"
        ).fetchone()[0]
        assert count >= 5

    def test_known_greenhouse_slug_present(self, conn):
        row = conn.execute(
            "SELECT slug FROM ats_companies WHERE ats='greenhouse' AND company='Stripe'"
        ).fetchone()
        assert row is not None
        assert row[0] == "stripe"

    def test_known_lever_slug_present(self, conn):
        row = conn.execute(
            "SELECT slug FROM ats_companies WHERE ats='lever' AND company='Netflix'"
        ).fetchone()
        assert row is not None
        assert row[0] == "netflix"

    def test_seed_data_idempotent_on_second_init(self, conn):
        """Running migrate() again must not duplicate ATS seed rows."""
        before = conn.execute("SELECT COUNT(*) FROM ats_companies").fetchone()[0]
        migrate(conn)
        after = conn.execute("SELECT COUNT(*) FROM ats_companies").fetchone()[0]
        assert before == after


# ---------------------------------------------------------------------------
# Case-insensitive profile name uniqueness
# ---------------------------------------------------------------------------

class TestCaseInsensitiveProfileNames:
    def test_duplicate_name_different_case_raises(self, conn):
        """Inserting 'alice' then 'Alice' must fail with an IntegrityError."""
        conn.execute("INSERT INTO profiles (name, input_file) VALUES ('alice', '/f.txt')")
        conn.commit()
        with pytest.raises(sqlite3.IntegrityError):
            conn.execute("INSERT INTO profiles (name, input_file) VALUES ('Alice', '/f.txt')")
            conn.commit()

    def test_insert_or_ignore_silently_ignores_case_variant(self, conn):
        """INSERT OR IGNORE must not raise; row count must stay at 1."""
        conn.execute(
            "INSERT OR IGNORE INTO profiles (name, input_file) VALUES ('bob', '/f.txt')"
        )
        conn.execute(
            "INSERT OR IGNORE INTO profiles (name, input_file) VALUES ('BOB', '/f.txt')"
        )
        conn.commit()
        count = conn.execute(
            "SELECT COUNT(*) FROM profiles WHERE LOWER(name)='bob'"
        ).fetchone()[0]
        assert count == 1

    def test_upsert_profile_case_insensitive_idempotent(self, conn):
        """upsert_profile('alice') then upsert_profile('Alice') must return the same id."""
        pid1 = upsert_profile(conn, "alice", "/f.txt")
        pid2 = upsert_profile(conn, "Alice", "/f.txt")
        assert pid1 == pid2

    def test_get_profile_case_insensitive(self, conn):
        """get_profile must find a row regardless of name casing."""
        upsert_profile(conn, "charlie", "/f.txt")
        assert get_profile(conn, "charlie") is not None
        assert get_profile(conn, "CHARLIE") is not None
        assert get_profile(conn, "Charlie") is not None

    def test_get_profile_returns_none_for_unknown(self, conn):
        assert get_profile(conn, "nobody") is None
