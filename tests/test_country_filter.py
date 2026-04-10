"""
tests/test_country_filter.py
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Unit tests for country-based job filtering in src/ranker.py.

Covers:
  _country_conflict()   — foreign-country name detection (word-boundary aware)
  pre_filter()          — end-to-end filter #7 (country) and interaction with
                          filter #8 (location) for remote jobs

Run from the project root:
    pytest tests/test_country_filter.py -v
"""

import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.ranker import _country_conflict, pre_filter


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _job(location: str, remote_type: str = "remote", title: str = "SWE",
         company: str = "Acme") -> dict:
    """Return a minimal job dict suitable for pre_filter()."""
    return {
        "title": title,
        "company": company,
        "location": location,
        "remote_type": remote_type,
        "salary_min": None,
        "salary_max": None,
        "description": "Python engineer with 5+ years experience. Requirements: ...",
        "job_key": f"{title}-{company}-{location}",
    }


def _profile(country: list[str], locations: list[str] | None = None) -> dict:
    return {
        "hard_requirements": {
            "country": country,
            "locations": locations or [],
        }
    }


# ---------------------------------------------------------------------------
# _country_conflict — should return True (conflict detected)
# ---------------------------------------------------------------------------

class TestCountryConflictTrue:
    """Locations that clearly belong to a foreign country."""

    # --- Dropbox-style "Remote - <Country>" format ---
    def test_remote_mexico(self):
        assert _country_conflict("us", "remote - mexico") is True

    def test_remote_poland(self):
        assert _country_conflict("us", "remote - poland") is True

    def test_remote_canada(self):
        assert _country_conflict("us", "remote - canada: select locations") is True

    def test_remote_germany(self):
        assert _country_conflict("us", "remote - germany") is True

    def test_remote_ireland(self):
        assert _country_conflict("us", "remote - ireland") is True

    def test_remote_india(self):
        assert _country_conflict("us", "remote - india") is True

    def test_remote_turkey(self):
        assert _country_conflict("us", "remote - turkey") is True

    def test_remote_australia(self):
        assert _country_conflict("us", "remote - australia") is True

    def test_remote_brazil(self):
        assert _country_conflict("us", "remote - brazil") is True

    def test_remote_dubai(self):
        """Dubai (UAE) must be detected even though UAE is the formal country name."""
        assert _country_conflict("us", "remote - dubai") is True

    # --- City, Country format ---
    def test_bangalore_india(self):
        assert _country_conflict("us", "bangalore, india") is True

    def test_mexico_city(self):
        assert _country_conflict("us", "mexico city") is True

    def test_london_uk(self):
        assert _country_conflict("us", "london, united kingdom") is True

    def test_toronto_canada(self):
        assert _country_conflict("us", "toronto, canada") is True

    # --- Countries not in the old explicit conflict list ---
    def test_romania(self):
        assert _country_conflict("us", "remote - romania") is True

    def test_ukraine(self):
        assert _country_conflict("us", "remote - ukraine") is True

    def test_south_korea(self):
        assert _country_conflict("us", "remote - south korea") is True

    def test_singapore(self):
        assert _country_conflict("us", "remote - singapore") is True

    def test_nigeria(self):
        assert _country_conflict("us", "remote - nigeria") is True

    def test_south_africa(self):
        assert _country_conflict("us", "remote - south africa") is True

    def test_new_zealand(self):
        assert _country_conflict("us", "remote - new zealand") is True

    def test_colombia(self):
        assert _country_conflict("us", "remote - colombia") is True

    def test_switzerland(self):
        assert _country_conflict("us", "remote - switzerland") is True

    # --- Multi-country location strings ---
    def test_multi_country_germany_ireland_uk(self):
        """Dropbox-style semicolon-separated foreign locations."""
        loc = "remote - germany; remote - ireland; remote - united kingdom"
        assert _country_conflict("us", loc) is True

    # --- Cross-country: non-US hard_country ---
    def test_canada_profile_sees_us_job(self):
        assert _country_conflict("canada", "remote - us: select locations") is True

    def test_canada_profile_sees_mexico_job(self):
        assert _country_conflict("canada", "remote - mexico") is True

    def test_uk_profile_sees_germany_job(self):
        assert _country_conflict("uk", "remote - germany") is True

    def test_australia_profile_sees_india_job(self):
        assert _country_conflict("australia", "remote - india") is True


# ---------------------------------------------------------------------------
# _country_conflict — should return False (no conflict)
# ---------------------------------------------------------------------------

class TestCountryConflictFalse:
    """Locations that are confirmed US or ambiguous — must NOT be rejected."""

    # --- Clearly US ---
    def test_remote_us_select(self):
        assert _country_conflict("us", "remote - us: select locations") is False

    def test_remote_us_all(self):
        assert _country_conflict("us", "remote - us: all locations") is False

    def test_remote_us_san_francisco(self):
        assert _country_conflict("us", "remote - us: san francisco, ca") is False

    def test_remote_usa(self):
        assert _country_conflict("us", "remote - usa") is False

    def test_united_states_explicit(self):
        assert _country_conflict("us", "new york, united states") is False

    def test_us_city_state(self):
        assert _country_conflict("us", "mclean, va") is False

    def test_us_california(self):
        assert _country_conflict("us", "san francisco, ca") is False

    # --- New Mexico must NOT be treated as Mexico (the country) ---
    def test_new_mexico_state(self):
        assert _country_conflict("us", "albuquerque, new mexico") is False

    def test_new_mexico_bare(self):
        assert _country_conflict("us", "new mexico") is False

    def test_remote_us_new_mexico(self):
        assert _country_conflict("us", "remote - us: new mexico") is False

    # --- indiana must NOT match india ---
    def test_indianapolis_indiana(self):
        assert _country_conflict("us", "indianapolis, indiana") is False

    # --- Benefit of the doubt: location-agnostic strings ---
    def test_bare_remote(self):
        assert _country_conflict("us", "remote") is False

    def test_anywhere(self):
        assert _country_conflict("us", "anywhere") is False

    def test_empty_string(self):
        assert _country_conflict("us", "") is False

    # --- Cross-country own-country ---
    def test_canada_profile_sees_canada_job(self):
        assert _country_conflict("canada", "remote - canada: select locations") is False

    def test_uk_profile_sees_uk_job(self):
        assert _country_conflict("uk", "london, england") is False

    def test_australia_profile_sees_australia_job(self):
        assert _country_conflict("australia", "sydney, australia") is False


# ---------------------------------------------------------------------------
# pre_filter — end-to-end country filter (filter #7)
# ---------------------------------------------------------------------------

class TestPreFilterCountry:
    """pre_filter() filter #7: jobs with foreign locations are rejected regardless
    of remote_type.  Location-agnostic strings ("Remote", "Anywhere") pass."""

    def test_remote_mexico_rejected_for_us_profile(self):
        jobs = [_job("Remote - Mexico", remote_type="remote")]
        passed, rejected = pre_filter(jobs, _profile(["US"]))
        assert len(passed) == 0
        assert len(rejected) == 1
        assert "outside required country" in rejected[0][1]

    def test_remote_poland_rejected_for_us_profile(self):
        jobs = [_job("Remote - Poland", remote_type="remote")]
        passed, rejected = pre_filter(jobs, _profile(["US"]))
        assert len(passed) == 0

    def test_remote_canada_rejected_for_us_profile(self):
        jobs = [_job("Remote - Canada: Select locations", remote_type="remote")]
        passed, rejected = pre_filter(jobs, _profile(["US"]))
        assert len(passed) == 0

    def test_remote_dubai_rejected_for_us_profile(self):
        jobs = [_job("Remote - Dubai", remote_type="remote")]
        passed, rejected = pre_filter(jobs, _profile(["US"]))
        assert len(passed) == 0

    def test_remote_turkey_rejected_for_us_profile(self):
        jobs = [_job("Remote - Turkey", remote_type="remote")]
        passed, rejected = pre_filter(jobs, _profile(["US"]))
        assert len(passed) == 0

    def test_remote_us_passes_for_us_profile(self):
        jobs = [_job("Remote - US: Select locations", remote_type="remote")]
        passed, rejected = pre_filter(jobs, _profile(["US"]))
        assert len(passed) == 1
        assert len(rejected) == 0

    def test_bare_remote_passes_for_us_profile(self):
        """'Remote' with no country qualifier must not be filtered."""
        jobs = [_job("Remote", remote_type="remote")]
        passed, rejected = pre_filter(jobs, _profile(["US"]))
        assert len(passed) == 1

    def test_anywhere_passes_for_us_profile(self):
        jobs = [_job("Anywhere", remote_type="remote")]
        passed, rejected = pre_filter(jobs, _profile(["US"]))
        assert len(passed) == 1

    def test_empty_location_passes_for_us_profile(self):
        """No location → benefit of the doubt."""
        jobs = [_job("", remote_type="remote")]
        passed, rejected = pre_filter(jobs, _profile(["US"]))
        assert len(passed) == 1

    def test_new_mexico_not_filtered(self):
        jobs = [_job("Albuquerque, New Mexico", remote_type="hybrid")]
        passed, rejected = pre_filter(jobs, _profile(["US"]))
        assert len(passed) == 1

    def test_no_country_requirement_passes_all(self):
        """When no country is set, all jobs pass the country filter."""
        jobs = [
            _job("Remote - Mexico", remote_type="remote"),
            _job("Remote - Germany", remote_type="remote"),
            _job("Remote - US: Select locations", remote_type="remote"),
        ]
        passed, rejected = pre_filter(jobs, _profile([]))
        assert len(passed) == 3

    def test_multiple_jobs_mixed(self):
        jobs = [
            _job("Remote - US: Select locations", remote_type="remote"),
            _job("Remote - Mexico", remote_type="remote"),
            _job("Remote - Poland", remote_type="remote"),
            _job("Remote", remote_type="remote"),
        ]
        passed, rejected = pre_filter(jobs, _profile(["US"]))
        assert len(passed) == 2   # US + bare Remote
        assert len(rejected) == 2  # Mexico + Poland

    def test_onsite_foreign_city_rejected(self):
        """Non-remote foreign location is also rejected."""
        jobs = [_job("Mexico City, Mexico", remote_type="onsite")]
        passed, rejected = pre_filter(jobs, _profile(["US"]))
        assert len(passed) == 0
