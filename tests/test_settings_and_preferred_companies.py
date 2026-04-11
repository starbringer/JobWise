"""
tests/test_settings_and_preferred_companies.py
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Unit/integration tests for the Settings tab and the per-profile Preferred
Companies feature introduced alongside it.

Covers:

1. load_preferred_companies()
   - Always reads from the profile's preferred_companies list.
   - Returns [] when the profile has no list, an empty list, or profile=None.
   - Whitespace and blank entries are cleaned.

2. GET /settings
   - Returns 200 and renders key fields from config.yaml.
   - Settings nav link is marked active.

3. POST /api/settings/save  (all sections)
   - ai, pipeline, sources, ranker, scheduler, logging, web sections are
     written to config.yaml and visible on reload.
   - Numeric coercion: string inputs accepted (as HTML forms send them).
   - Bad input (non-numeric for an int field) returns 500 with ok=false.
   - Partial payload leaves unmentioned sections unchanged.

4. POST /profile/<name>/import_companies
   - Copies every non-comment line from preferred_companies.txt into the
     profile's preferred_companies field in the DB.
   - Idempotent: a second call adds 0 duplicates.
   - Missing file → added=0, no crash.
   - Unknown profile → 404.

Run from the project root:
    pytest tests/test_settings_and_preferred_companies.py -v
"""

import copy
import json
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

import pytest
import yaml

from src.pipeline import load_preferred_companies
from src.database import (
    init_db, get_profile,
    upsert_profile, update_profile_structured_content,
)


# ─────────────────────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────────────────────

_MINIMAL_CONFIG = {
    "ai": {
        "provider": "claude_cli",
        "claude_cli": {"model": "claude-sonnet-4-6"},
        "anthropic":  {"model": "claude-haiku-4-5-20251001"},
        "openai":     {"model": "gpt-4o"},
        "gemini":     {"model": "gemini-2.0-flash"},
        "ollama":     {"model": "gemma2:9b", "host": "http://localhost", "port": 11434},
    },
    "top_n": 50,
    "top_n_display": 50,
    "jsearch_queries_per_run": 10,
    "sources": {
        "greenhouse": True, "lever": True, "jsearch": True, "jobspy": True,
        "max_ats_companies_per_run": 20,
    },
    "jobspy": {"sites": ["linkedin", "indeed"], "results_per_site": 25},
    "scheduler": {"enabled": False, "run_times": ["11:00", "18:00"], "profiles": []},
    "api": {"jsearch_reset_day": 18},
    "web": {"host": "0.0.0.0", "port": 5000, "debug": False},
    "database": {"path": "data/jobs.db"},
    "profiles_dir": "profiles",
    "preferred_companies_file": "config/preferred_companies.txt",
    "job_retention_days": 30,
    "logging": {
        "level": "INFO", "file": "logs/pipeline.log",
        "max_bytes": 5242880, "backup_count": 3,
    },
    "ranker": {
        "batch_size": 50, "min_match_score": 0.4, "description_max_chars": 3500,
        "scoring": {
            "manager": {
                "required": 10, "preferred": 5, "nice_to_have": 5,
                "unknown": 1, "extra_skill": 2,
            },
            "candidate": {"must_have": 10, "nice_to_have": 7, "unknown": 5},
        },
    },
}


def _write_config(path: Path, cfg: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        yaml.dump(cfg, f, default_flow_style=False, allow_unicode=True, sort_keys=False)


def _read_config(path: Path) -> dict:
    with open(path, encoding="utf-8") as f:
        return yaml.safe_load(f)


def _seed_profile(conn, name: str = "testuser") -> None:
    """Insert a minimal profile with an empty preferred_companies list."""
    upsert_profile(conn, name, input_file=f"profiles/{name}.txt")
    p = get_profile(conn, name)
    structured = {
        "profile_name": name,
        "preferred_companies": [],
        "target_companies": [],
        "technical_skills": [],
    }
    update_profile_structured_content(
        conn, p["id"], json.dumps(structured), "hash1", "2025-01-01"
    )


def _get_profile_companies(conn, name: str = "testuser") -> list:
    p = get_profile(conn, name)
    return json.loads(p["structured_content"] or "{}").get("preferred_companies", [])


# ─────────────────────────────────────────────────────────────────────────────
# Fixture: isolated Flask app backed by a tmp config + in-memory DB
# ─────────────────────────────────────────────────────────────────────────────

@pytest.fixture
def app_client(tmp_path, monkeypatch):
    """
    Minimal isolated test environment:
    - config.yaml written to tmp_path/config/
    - file-based SQLite DB in tmp_path/data/jobs.db — routes get a fresh
      connection per call (matching production), while the fixture keeps its
      own separate helper connection for seeding/reading.
    - web.app.PROJECT_ROOT monkeypatched to tmp_path
    Yields (flask_test_client, tmp_path, helper_db_conn).
    """
    import web.app as webapp

    cfg_path = tmp_path / "config" / "config.yaml"
    _write_config(cfg_path, copy.deepcopy(_MINIMAL_CONFIG))

    db_path = tmp_path / "data" / "jobs.db"
    db_path.parent.mkdir()
    # Initialise schema once, then close so routes can open their own connections
    seed = init_db(str(db_path))
    seed.close()

    monkeypatch.setattr(webapp, "PROJECT_ROOT", tmp_path)
    # Each call to get_db() gets a fresh connection, just like production
    monkeypatch.setattr(webapp, "get_db", lambda: init_db(str(db_path)))

    webapp.app.config["TESTING"] = True
    client = webapp.app.test_client()

    # Separate helper connection for test seeding and assertions
    helper_conn = init_db(str(db_path))
    yield client, tmp_path, helper_conn
    helper_conn.close()


# ─────────────────────────────────────────────────────────────────────────────
# 1. load_preferred_companies()
# ─────────────────────────────────────────────────────────────────────────────

class TestLoadPreferredCompanies:
    """Preferred companies come exclusively from the profile dict."""

    _CFG = {}  # config arg is unused for company loading; kept for API compat

    # ── Returns from profile list ──────────────────────────────────────────

    def test_returns_profile_list(self):
        profile = {"preferred_companies": ["Acme", "Beta Corp"]}
        assert load_preferred_companies(self._CFG, profile) == ["Acme", "Beta Corp"]

    def test_returns_all_entries(self):
        companies = ["Google", "Microsoft", "Anthropic", "OpenAI"]
        profile = {"preferred_companies": companies}
        assert load_preferred_companies(self._CFG, profile) == companies

    # ── Empty / absent cases ───────────────────────────────────────────────

    def test_empty_profile_list_returns_empty(self):
        profile = {"preferred_companies": []}
        assert load_preferred_companies(self._CFG, profile) == []

    def test_missing_key_returns_empty(self):
        profile = {"target_companies": ["Other"]}
        assert load_preferred_companies(self._CFG, profile) == []

    def test_none_profile_returns_empty(self):
        assert load_preferred_companies(self._CFG, None) == []

    def test_none_value_returns_empty(self):
        profile = {"preferred_companies": None}
        assert load_preferred_companies(self._CFG, profile) == []

    # ── Cleaning ───────────────────────────────────────────────────────────

    def test_whitespace_stripped(self):
        profile = {"preferred_companies": ["  Acme Corp  ", " Beta Inc"]}
        assert load_preferred_companies(self._CFG, profile) == ["Acme Corp", "Beta Inc"]

    def test_blank_entries_skipped(self):
        profile = {"preferred_companies": ["Acme", "", "  ", "Beta"]}
        assert load_preferred_companies(self._CFG, profile) == ["Acme", "Beta"]

    def test_all_blank_returns_empty(self):
        profile = {"preferred_companies": ["", "  ", "\t"]}
        assert load_preferred_companies(self._CFG, profile) == []


# ─────────────────────────────────────────────────────────────────────────────
# 2. GET /settings
# ─────────────────────────────────────────────────────────────────────────────

class TestSettingsPage:
    def test_returns_200(self, app_client):
        client, _, _ = app_client
        assert client.get("/settings").status_code == 200

    def test_renders_current_ai_provider(self, app_client):
        client, _, _ = app_client
        assert b"claude_cli" in client.get("/settings").data

    def test_renders_top_n_value(self, app_client):
        client, _, _ = app_client
        assert b"50" in client.get("/settings").data

    def test_renders_all_section_headings(self, app_client):
        client, _, _ = app_client
        body = client.get("/settings").data
        for heading in (b"AI Provider", b"Pipeline", b"Job Sources",
                        b"Ranker", b"Scheduler", b"Logging", b"Web Server"):
            assert heading in body, f"Missing section: {heading}"

    def test_settings_tab_active_in_nav(self, app_client):
        client, _, _ = app_client
        body = client.get("/settings").data
        assert b'class="active"' in body

    def test_no_config_shows_first_run_banner(self, tmp_path, monkeypatch):
        """When config.yaml is absent the Settings page renders with defaults
        and shows the first-run warning banner."""
        import web.app as webapp
        import copy

        db_path = tmp_path / "data" / "jobs.db"
        db_path.parent.mkdir()
        init_db(str(db_path)).close()

        monkeypatch.setattr(webapp, "PROJECT_ROOT", tmp_path)
        monkeypatch.setattr(webapp, "get_db", lambda: init_db(str(db_path)))
        webapp.app.config["TESTING"] = True

        client = webapp.app.test_client()
        r = client.get("/settings")
        assert r.status_code == 200
        assert b"No config.yaml found" in r.data
        # Defaults must still be rendered (page is usable)
        assert b"claude_cli" in r.data


# ─────────────────────────────────────────────────────────────────────────────
# 3. POST /api/settings/save
# ─────────────────────────────────────────────────────────────────────────────

class TestLoadConfig:
    """load_config() must work correctly with missing, partial, and full config files."""

    def test_returns_defaults_when_file_missing(self, tmp_path, monkeypatch):
        import web.app as webapp
        monkeypatch.setattr(webapp, "PROJECT_ROOT", tmp_path)
        cfg = webapp.load_config()
        assert cfg["top_n"] == 50
        assert cfg["ai"]["provider"] == "claude_cli"

    def test_returns_file_values_when_present(self, tmp_path, monkeypatch):
        import web.app as webapp
        monkeypatch.setattr(webapp, "PROJECT_ROOT", tmp_path)
        _write_config(tmp_path / "config" / "config.yaml",
                      {**copy.deepcopy(_MINIMAL_CONFIG), "top_n": 99})
        assert webapp.load_config()["top_n"] == 99

    def test_missing_key_filled_from_defaults(self, tmp_path, monkeypatch):
        """A config.yaml that omits a key (e.g. added in a newer version) should
        still return the default value for that key."""
        import web.app as webapp
        monkeypatch.setattr(webapp, "PROJECT_ROOT", tmp_path)
        # Write a minimal config that lacks the 'ranker' section entirely
        sparse = {"top_n": 10, "database": {"path": "data/jobs.db"},
                  "ai": {"provider": "openai"}}
        _write_config(tmp_path / "config" / "config.yaml", sparse)
        cfg = webapp.load_config()
        assert cfg["top_n"] == 10                   # from file
        assert cfg["ai"]["provider"] == "openai"    # from file
        assert "batch_size" in cfg["ranker"]        # filled from defaults

    def test_nested_merge_preserves_file_values(self, tmp_path, monkeypatch):
        """Deep merge must not clobber nested values that exist in the file."""
        import web.app as webapp
        monkeypatch.setattr(webapp, "PROJECT_ROOT", tmp_path)
        partial = {
            "ai": {"provider": "gemini", "gemini": {"model": "gemini-ultra"}},
            "database": {"path": "data/jobs.db"},
        }
        _write_config(tmp_path / "config" / "config.yaml", partial)
        cfg = webapp.load_config()
        assert cfg["ai"]["gemini"]["model"] == "gemini-ultra"   # file wins
        assert "claude_cli" in cfg["ai"]                        # default filled in


class TestSaveSettings:
    def _post(self, client, payload: dict):
        return client.post(
            "/api/settings/save",
            data=json.dumps(payload),
            content_type="application/json",
        )

    def _reload(self, tmp_path: Path) -> dict:
        return _read_config(tmp_path / "config" / "config.yaml")

    # ── AI ────────────────────────────────────────────────────────────────

    def test_save_ai_provider(self, app_client):
        client, tmp, _ = app_client
        r = self._post(client, {"ai": {"provider": "openai"}})
        assert json.loads(r.data)["ok"] is True
        assert self._reload(tmp)["ai"]["provider"] == "openai"

    def test_save_ai_model(self, app_client):
        client, tmp, _ = app_client
        self._post(client, {"ai": {"anthropic": {"model": "claude-opus-4-6"}}})
        assert self._reload(tmp)["ai"]["anthropic"]["model"] == "claude-opus-4-6"

    def test_save_ollama_settings(self, app_client):
        client, tmp, _ = app_client
        self._post(client, {"ai": {"ollama": {
            "model": "llama3", "host": "http://192.168.1.1", "port": 11435
        }}})
        ollama = self._reload(tmp)["ai"]["ollama"]
        assert ollama["host"] == "http://192.168.1.1"
        assert ollama["port"] == 11435

    # ── Pipeline ──────────────────────────────────────────────────────────

    def test_save_pipeline_values(self, app_client):
        client, tmp, _ = app_client
        r = self._post(client, {
            "top_n": 25, "top_n_display": 25,
            "jsearch_queries_per_run": 5, "job_retention_days": 14,
        })
        assert json.loads(r.data)["ok"] is True
        cfg = self._reload(tmp)
        assert cfg["top_n"] == 25
        assert cfg["jsearch_queries_per_run"] == 5
        assert cfg["job_retention_days"] == 14

    def test_pipeline_accepts_string_numbers(self, app_client):
        """HTML form values arrive as strings; endpoint must coerce to int."""
        client, tmp, _ = app_client
        self._post(client, {
            "top_n": "30", "top_n_display": "30",
            "jsearch_queries_per_run": "8", "job_retention_days": "45",
        })
        assert self._reload(tmp)["top_n"] == 30

    # ── Sources ───────────────────────────────────────────────────────────

    def test_save_jsearch_reset_day(self, app_client):
        client, tmp, _ = app_client
        self._post(client, {"api": {"jsearch_reset_day": 22}})
        assert self._reload(tmp)["api"]["jsearch_reset_day"] == 22

    def test_save_sources_disable_jsearch(self, app_client):
        client, tmp, _ = app_client
        self._post(client, {"sources": {
            "greenhouse": True, "lever": True, "jsearch": False,
            "jobspy": True, "max_ats_companies_per_run": 15,
        }})
        assert self._reload(tmp)["sources"]["jsearch"] is False

    def test_save_sources_max_ats(self, app_client):
        client, tmp, _ = app_client
        self._post(client, {"sources": {
            "greenhouse": True, "lever": True, "jsearch": True,
            "jobspy": True, "max_ats_companies_per_run": 5,
        }})
        assert self._reload(tmp)["sources"]["max_ats_companies_per_run"] == 5

    def test_save_jobspy_sites(self, app_client):
        client, tmp, _ = app_client
        self._post(client, {"jobspy": {"sites": ["linkedin"], "results_per_site": 10}})
        cfg = self._reload(tmp)
        assert cfg["jobspy"]["sites"] == ["linkedin"]
        assert cfg["jobspy"]["results_per_site"] == 10

    # ── Ranker ────────────────────────────────────────────────────────────

    def test_save_ranker_core_fields(self, app_client):
        client, tmp, _ = app_client
        self._post(client, {"ranker": {
            "batch_size": 20, "min_match_score": 0.5, "description_max_chars": 3000,
        }})
        cfg = self._reload(tmp)["ranker"]
        assert cfg["batch_size"] == 20
        assert cfg["min_match_score"] == pytest.approx(0.5)
        assert cfg["description_max_chars"] == 3000

    def test_save_ranker_scoring_weights(self, app_client):
        client, tmp, _ = app_client
        self._post(client, {"ranker": {"scoring": {
            "manager":   {"required": 15, "preferred": 8, "nice_to_have": 4,
                          "unknown": 2, "extra_skill": 3},
            "candidate": {"must_have": 12, "nice_to_have": 8, "unknown": 4},
        }}})
        scoring = self._reload(tmp)["ranker"]["scoring"]
        assert scoring["manager"]["required"] == 15
        assert scoring["candidate"]["must_have"] == 12

    # ── Scheduler ─────────────────────────────────────────────────────────

    def test_save_scheduler(self, app_client):
        client, tmp, _ = app_client
        self._post(client, {"scheduler": {
            "enabled": True, "run_times": ["09:00", "17:00"], "profiles": ["alice"],
        }})
        sch = self._reload(tmp)["scheduler"]
        assert sch["enabled"] is True
        assert sch["run_times"] == ["09:00", "17:00"]
        assert sch["profiles"] == ["alice"]

    # ── Logging ───────────────────────────────────────────────────────────

    def test_save_logging(self, app_client):
        client, tmp, _ = app_client
        self._post(client, {"logging": {"level": "DEBUG", "file": "logs/debug.log"}})
        lg = self._reload(tmp)["logging"]
        assert lg["level"] == "DEBUG"
        assert lg["file"] == "logs/debug.log"

    # ── Web ───────────────────────────────────────────────────────────────

    def test_save_web(self, app_client):
        client, tmp, _ = app_client
        self._post(client, {"web": {"host": "127.0.0.1", "port": 8080}})
        web = self._reload(tmp)["web"]
        assert web["host"] == "127.0.0.1"
        assert web["port"] == 8080

    # ── Error handling ────────────────────────────────────────────────────

    def test_save_creates_config_when_missing(self, tmp_path, monkeypatch):
        """Saving settings when config.yaml doesn't exist yet must create it."""
        import web.app as webapp

        db_path = tmp_path / "data" / "jobs.db"
        db_path.parent.mkdir()
        init_db(str(db_path)).close()

        monkeypatch.setattr(webapp, "PROJECT_ROOT", tmp_path)
        monkeypatch.setattr(webapp, "get_db", lambda: init_db(str(db_path)))
        webapp.app.config["TESTING"] = True

        cfg_path = tmp_path / "config" / "config.yaml"
        assert not cfg_path.exists()

        client = webapp.app.test_client()
        r = client.post(
            "/api/settings/save",
            data=json.dumps({"ai": {"provider": "gemini"}}),
            content_type="application/json",
        )
        assert r.status_code == 200
        assert json.loads(r.data)["ok"] is True
        assert cfg_path.exists()
        assert _read_config(cfg_path)["ai"]["provider"] == "gemini"

    def test_save_creates_config_dir_when_missing(self, tmp_path, monkeypatch):
        """The config/ directory itself may not exist on first run."""
        import web.app as webapp

        db_path = tmp_path / "data" / "jobs.db"
        db_path.parent.mkdir()
        init_db(str(db_path)).close()

        monkeypatch.setattr(webapp, "PROJECT_ROOT", tmp_path)
        monkeypatch.setattr(webapp, "get_db", lambda: init_db(str(db_path)))
        webapp.app.config["TESTING"] = True

        assert not (tmp_path / "config").exists()

        client = webapp.app.test_client()
        r = client.post(
            "/api/settings/save",
            data=json.dumps({"top_n": 10}),
            content_type="application/json",
        )
        assert r.status_code == 200
        assert (tmp_path / "config" / "config.yaml").exists()

    def test_bad_numeric_input_returns_500(self, app_client):
        client, _, _ = app_client
        r = self._post(client, {"top_n": "not-a-number"})
        assert r.status_code == 500
        body = json.loads(r.data)
        assert body["ok"] is False
        assert "error" in body

    def test_empty_payload_is_noop(self, app_client):
        """Empty payload writes config back unchanged — no crash, top_n still 50."""
        client, tmp, _ = app_client
        r = self._post(client, {})
        assert r.status_code == 200
        assert self._reload(tmp)["top_n"] == 50

    def test_partial_payload_leaves_other_sections_unchanged(self, app_client):
        client, tmp, _ = app_client
        self._post(client, {"ai": {"provider": "gemini"}})
        cfg = self._reload(tmp)
        assert cfg["top_n"] == 50                   # pipeline untouched
        assert cfg["ai"]["provider"] == "gemini"    # ai updated


# ─────────────────────────────────────────────────────────────────────────────
# 4. POST /profile/<name>/import_companies
# ─────────────────────────────────────────────────────────────────────────────

class TestImportCompanies:
    _COMPANIES = ["Google", "Microsoft", "Anthropic"]

    def _write_pc_file(self, tmp_path: Path) -> None:
        pc_file = tmp_path / "config" / "preferred_companies.txt"
        pc_file.write_text(
            "# Top companies\n" + "\n".join(self._COMPANIES) + "\n",
            encoding="utf-8",
        )

    # ── Basic import ──────────────────────────────────────────────────────

    def test_import_returns_correct_counts(self, app_client):
        client, tmp, conn = app_client
        _seed_profile(conn)
        self._write_pc_file(tmp)

        body = json.loads(client.post("/profile/testuser/import_companies").data)
        assert body["ok"] is True
        assert body["added"] == len(self._COMPANIES)
        assert body["total"] == len(self._COMPANIES)

    def test_imported_companies_appear_in_profile(self, app_client):
        client, tmp, conn = app_client
        _seed_profile(conn)
        self._write_pc_file(tmp)

        client.post("/profile/testuser/import_companies")
        companies = _get_profile_companies(conn)
        for c in self._COMPANIES:
            assert c in companies

    # ── Idempotency ───────────────────────────────────────────────────────

    def test_second_import_adds_zero(self, app_client):
        client, tmp, conn = app_client
        _seed_profile(conn)
        self._write_pc_file(tmp)

        client.post("/profile/testuser/import_companies")
        body = json.loads(client.post("/profile/testuser/import_companies").data)
        assert body["added"] == 0
        assert body["total"] == len(self._COMPANIES)

    def test_no_duplicates_in_db_after_two_imports(self, app_client):
        client, tmp, conn = app_client
        _seed_profile(conn)
        self._write_pc_file(tmp)

        client.post("/profile/testuser/import_companies")
        client.post("/profile/testuser/import_companies")
        companies = _get_profile_companies(conn)
        assert len(companies) == len(set(companies))

    # ── Missing file ──────────────────────────────────────────────────────

    def test_missing_file_returns_zero_no_crash(self, app_client):
        client, tmp, conn = app_client
        _seed_profile(conn)
        # Don't create the file

        body = json.loads(client.post("/profile/testuser/import_companies").data)
        assert body["ok"] is True
        assert body["added"] == 0
        assert body["total"] == 0

    # ── Unknown profile ───────────────────────────────────────────────────

    def test_unknown_profile_returns_404(self, app_client):
        client, tmp, conn = app_client
        self._write_pc_file(tmp)
        assert client.post("/profile/doesnotexist/import_companies").status_code == 404
