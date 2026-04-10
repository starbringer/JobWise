"""
tests/test_location.py
~~~~~~~~~~~~~~~~~~~~~~
Unit tests for src/location.py — US state name/abbreviation matching utilities.

Covers:
- expand(): bidirectional state↔abbr expansion
- matches(): word-boundary-aware location matching, false-positive prevention
- matches_any(): multi-area matching

Run from the project root:
    pytest tests/test_location.py -v
"""

import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.location import expand, matches, matches_any


# ---------------------------------------------------------------------------
# expand()
# ---------------------------------------------------------------------------

class TestExpand:
    def test_full_state_name_returns_name_and_abbr(self):
        result = expand("Virginia")
        assert result == {"virginia", "va"}

    def test_abbr_returns_abbr_and_full_name(self):
        result = expand("VA")
        assert result == {"va", "virginia"}

    def test_case_insensitive_full_name(self):
        assert expand("VIRGINIA") == {"virginia", "va"}
        assert expand("virginia") == {"virginia", "va"}

    def test_case_insensitive_abbr(self):
        assert expand("va") == {"va", "virginia"}
        assert expand("Va") == {"va", "virginia"}

    def test_maryland(self):
        assert expand("Maryland") == {"maryland", "md"}
        assert expand("MD") == {"md", "maryland"}

    def test_dc_abbreviation(self):
        result = expand("DC")
        assert "dc" in result

    def test_washington_dc_full(self):
        result = expand("washington dc")
        assert "dc" in result

    def test_non_state_returns_single_element(self):
        assert expand("Northern Virginia") == {"northern virginia"}
        assert expand("Seattle") == {"seattle"}
        assert expand("Remote") == {"remote"}

    def test_two_word_state(self):
        assert expand("New York") == {"new york", "ny"}
        assert expand("NY") == {"ny", "new york"}

    def test_west_virginia(self):
        assert expand("West Virginia") == {"west virginia", "wv"}
        assert expand("WV") == {"wv", "west virginia"}

    def test_north_carolina(self):
        assert expand("North Carolina") == {"north carolina", "nc"}

    def test_california(self):
        assert expand("California") == {"california", "ca"}
        assert expand("CA") == {"ca", "california"}


# ---------------------------------------------------------------------------
# matches()
# ---------------------------------------------------------------------------

class TestMatches:
    # --- Basic positive cases ---

    def test_abbr_match_in_city_state(self):
        assert matches("McLean, VA", "Virginia") is True
        assert matches("McLean, VA", "VA") is True

    def test_full_name_in_location(self):
        assert matches("Northern Virginia", "VA") is True
        assert matches("Northern Virginia", "Virginia") is True

    def test_maryland_city(self):
        assert matches("Bethesda, MD", "Maryland") is True
        assert matches("Bethesda, MD", "MD") is True

    def test_washington_dc(self):
        assert matches("Washington, DC", "DC") is True
        assert matches("Washington, DC", "dc") is True
        assert matches("Washington DC", "DC") is True

    def test_new_york(self):
        assert matches("New York, NY", "New York") is True
        assert matches("New York, NY", "NY") is True

    def test_multi_word_state(self):
        assert matches("Portland, OR", "Oregon") is True

    # --- Critical false-positive prevention cases ---

    def test_savannah_ga_does_not_match_va(self):
        """'Savannah, GA' must NOT match Virginia — 'va' appears as substring in 'savannah'."""
        assert matches("Savannah, GA", "Virginia") is False
        assert matches("Savannah, GA", "VA") is False

    def test_washington_state_does_not_match_wa_for_dc_profile(self):
        """'Seattle, WA' must NOT match 'DC'."""
        assert matches("Seattle, WA", "DC") is False

    def test_indiana_does_not_match_indiana_for_wrong_abbr(self):
        """'Indianapolis, IN' should match Indiana but not Iowa."""
        assert matches("Indianapolis, IN", "Indiana") is True
        assert matches("Indianapolis, IN", "Iowa") is False

    def test_maine_does_not_match_maryland(self):
        """ME (Maine) must not match MD (Maryland)."""
        assert matches("Portland, ME", "Maryland") is False
        assert matches("Portland, ME", "ME") is True

    def test_seattle_does_not_match_virginia(self):
        assert matches("Seattle, WA", "Virginia") is False

    def test_san_francisco_ca_does_not_match_colorado(self):
        assert matches("San Francisco, CA", "Colorado") is False

    # --- Edge cases ---

    def test_empty_job_location_returns_false(self):
        assert matches("", "Virginia") is False

    def test_empty_area_returns_false(self):
        assert matches("McLean, VA", "") is False

    def test_both_empty_returns_false(self):
        assert matches("", "") is False

    def test_none_job_location_returns_false(self):
        assert matches(None, "Virginia") is False

    def test_none_area_returns_false(self):
        assert matches("McLean, VA", None) is False

    def test_remote_location(self):
        """'Remote' should match 'remote'."""
        assert matches("Remote", "remote") is True

    def test_case_insensitive_matching(self):
        assert matches("mclean, va", "Virginia") is True
        assert matches("MCLEAN, VA", "virginia") is True


# ---------------------------------------------------------------------------
# matches_any()
# ---------------------------------------------------------------------------

class TestMatchesAny:
    def test_matches_first_in_list(self):
        assert matches_any("McLean, VA", ["VA", "MD", "DC"]) is True

    def test_matches_middle_in_list(self):
        assert matches_any("Bethesda, MD", ["VA", "MD", "DC"]) is True

    def test_matches_last_in_list(self):
        assert matches_any("Washington, DC", ["VA", "MD", "DC"]) is True

    def test_no_match_returns_false(self):
        assert matches_any("Seattle, WA", ["VA", "MD", "DC"]) is False

    def test_empty_list_returns_false(self):
        assert matches_any("McLean, VA", []) is False

    def test_single_item_list_match(self):
        assert matches_any("Austin, TX", ["Texas"]) is True

    def test_single_item_list_no_match(self):
        assert matches_any("Austin, TX", ["California"]) is False

    def test_savannah_ga_against_va_list(self):
        """Regression: Savannah, GA must not match a VA/MD/DC list."""
        assert matches_any("Savannah, GA", ["VA", "MD", "DC"]) is False
