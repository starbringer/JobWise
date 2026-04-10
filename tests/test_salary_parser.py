"""
tests/test_salary_parser.py
~~~~~~~~~~~~~~~~~~~~~~~~~~~
Unit tests for src/salary_parser.py — salary range extraction from job descriptions.

Covers:
- _to_int(): normalises "150k", "1.5M", "150,000" → integer
- _is_plausible(): validates salary ranges
- _location_zone(): maps text to zone tags
- parse_salary(): end-to-end extraction with zone-based selection

Run from the project root:
    pytest tests/test_salary_parser.py -v
"""

import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.salary_parser import _to_int, _is_plausible, _location_zone, parse_salary


# ---------------------------------------------------------------------------
# _to_int()
# ---------------------------------------------------------------------------

class TestToInt:
    def test_plain_integer(self):
        assert _to_int("150000") == 150000

    def test_comma_separated(self):
        assert _to_int("150,000") == 150000

    def test_k_suffix_lowercase(self):
        assert _to_int("150k") == 150000

    def test_k_suffix_uppercase(self):
        assert _to_int("150K") == 150000

    def test_m_suffix_lowercase(self):
        assert _to_int("1m") == 1_000_000

    def test_m_suffix_uppercase(self):
        assert _to_int("2M") == 2_000_000

    def test_decimal_k(self):
        assert _to_int("1.5k") == 1500

    def test_decimal_m(self):
        assert _to_int("1.5M") == 1_500_000

    def test_decimal_no_suffix(self):
        assert _to_int("150000.0") == 150000

    def test_invalid_returns_none(self):
        assert _to_int("abc") is None

    def test_whitespace_stripped(self):
        assert _to_int("  150k  ") == 150000


# ---------------------------------------------------------------------------
# _is_plausible()
# ---------------------------------------------------------------------------

class TestIsPlausible:
    def test_typical_software_range(self):
        assert _is_plausible(120_000, 180_000) is True

    def test_lower_bound_50k(self):
        assert _is_plausible(50_000, 80_000) is True

    def test_just_above_20k_minimum(self):
        assert _is_plausible(21_000, 40_000) is True

    def test_below_20k_rejected(self):
        assert _is_plausible(15_000, 30_000) is False

    def test_hi_equals_lo_rejected(self):
        assert _is_plausible(100_000, 100_000) is False

    def test_hi_below_lo_rejected(self):
        assert _is_plausible(180_000, 120_000) is False

    def test_ratio_too_large_rejected(self):
        """hi/lo >= 5 is implausible (typo or non-salary number)."""
        assert _is_plausible(20_000, 120_000) is False

    def test_ratio_just_below_5_accepted(self):
        assert _is_plausible(50_000, 200_000) is True  # ratio = 4

    def test_upper_bound_near_5m(self):
        # hi/lo = 4 < 5, within the plausible ratio
        assert _is_plausible(1_000_000, 4_000_000) is True

    def test_ratio_exactly_5_rejected(self):
        # hi/lo = 5 is NOT < 5, so rejected
        assert _is_plausible(1_000_000, 5_000_000) is False


# ---------------------------------------------------------------------------
# _location_zone()
# ---------------------------------------------------------------------------

class TestLocationZone:
    def test_san_francisco(self):
        assert _location_zone("San Francisco, CA") == "sf"

    def test_sf_abbreviation(self):
        assert _location_zone("SF office") == "sf"

    def test_bay_area(self):
        assert _location_zone("Bay Area") == "sf"

    def test_new_york(self):
        assert _location_zone("New York, NY") == "ny"

    def test_nyc(self):
        assert _location_zone("NYC") == "ny"

    def test_seattle(self):
        assert _location_zone("Seattle, WA") == "seattle"

    def test_bellevue(self):
        assert _location_zone("Bellevue, WA") == "seattle"

    def test_dc_abbreviation_zone(self):
        # "dc" token unambiguously matches the dc zone
        assert _location_zone("DC") == "dc"

    def test_virginia_zone(self):
        # "virginia" matches dc zone (no ambiguous substring)
        assert _location_zone("Virginia") == "dc"

    def test_maryland_zone(self):
        assert _location_zone("Maryland") == "dc"

    def test_reston(self):
        # "reston" in dc zone keywords
        assert _location_zone("Reston, VA") == "dc"

    def test_boston(self):
        assert _location_zone("Boston, MA") == "boston"

    def test_remote(self):
        assert _location_zone("Remote") == "remote"

    def test_remote_keyword(self):
        # "remote" unambiguously matches the remote zone
        assert _location_zone("remote") == "remote"

    def test_all_us(self):
        assert _location_zone("all us") == "remote"

    def test_unknown_location_returns_none(self):
        assert _location_zone("Timbuktu") is None

    def test_case_insensitive(self):
        assert _location_zone("SAN FRANCISCO") == "sf"
        assert _location_zone("NEW YORK") == "ny"


# ---------------------------------------------------------------------------
# parse_salary()
# ---------------------------------------------------------------------------

class TestParseSalary:
    def test_none_description_returns_none_none(self):
        assert parse_salary(None) == (None, None)

    def test_empty_description_returns_none_none(self):
        assert parse_salary("") == (None, None)

    def test_no_salary_in_description(self):
        desc = "We are a great company looking for talented engineers."
        assert parse_salary(desc) == (None, None)

    def test_simple_dollar_range(self):
        desc = "The salary range is $150,000 – $180,000."
        lo, hi = parse_salary(desc)
        assert lo == 150_000
        assert hi == 180_000

    def test_k_notation_range(self):
        desc = "Compensation: $150k-$180k per year."
        lo, hi = parse_salary(desc)
        assert lo == 150_000
        assert hi == 180_000

    def test_to_separator(self):
        desc = "Salary: $130,000 to $160,000"
        lo, hi = parse_salary(desc)
        assert lo == 130_000
        assert hi == 160_000

    def test_single_figure_annually(self):
        desc = "Base pay is $150,000 per year for this role."
        lo, hi = parse_salary(desc)
        assert lo == 150_000
        assert hi == 150_000

    def test_single_figure_annually_k(self):
        desc = "Salary: $150k annually"
        lo, hi = parse_salary(desc)
        assert lo == 150_000
        assert hi == 150_000

    def test_implausible_range_ignored(self):
        """Very small salary range (below $20k threshold) must not be extracted."""
        desc = "The team has 5 to 10 members and salary is undisclosed."
        lo, hi = parse_salary(desc)
        assert lo is None
        assert hi is None

    def test_single_range_no_location_preference(self):
        desc = "$140,000 – $170,000"
        lo, hi = parse_salary(desc, preferred_locations=None)
        assert lo == 140_000
        assert hi == 170_000

    def test_single_range_with_location_preference(self):
        """With a single range, preferred_locations doesn't change the result."""
        desc = "$140,000 – $170,000"
        lo, hi = parse_salary(desc, preferred_locations=["Virginia"])
        assert lo == 140_000
        assert hi == 170_000

    def test_zone_based_multiple_ranges_picks_matching_zone(self):
        """
        Zone table: higher range for SF, lower for DC/Virginia.
        Profile is in Virginia → should pick the Virginia range.
        Ranges are separated by >400 chars of padding so ±200-char snippets don't overlap.
        """
        padding = " " * 450
        desc = (
            f"San Francisco compensation: $200,000 – $240,000.{padding}"
            "Virginia / Maryland compensation: $150,000 – $180,000."
        )
        lo, hi = parse_salary(desc, preferred_locations=["Virginia"])
        assert lo == 150_000
        assert hi == 180_000

    def test_zone_based_picks_sf_when_profile_is_sf(self):
        padding = " " * 450
        desc = (
            f"San Francisco compensation: $200,000 – $240,000.{padding}"
            "Virginia / Maryland compensation: $150,000 – $180,000."
        )
        lo, hi = parse_salary(desc, preferred_locations=["San Francisco"])
        assert lo == 200_000
        assert hi == 240_000

    def test_no_zone_match_falls_back_to_first_range(self):
        """When no zone matches, the first valid range is returned."""
        desc = "$120,000 – $150,000 for SF; $100,000 – $130,000 for Seattle."
        # Profile in a zone not mentioned in desc
        lo, hi = parse_salary(desc, preferred_locations=["Boston"])
        assert lo == 120_000
        assert hi == 150_000

    def test_slash_separator(self):
        desc = "$150,000/$180,000 per year"
        lo, hi = parse_salary(desc)
        assert lo == 150_000
        assert hi == 180_000

    def test_m_notation(self):
        desc = "Total compensation: $1.2M – $1.5M"
        lo, hi = parse_salary(desc)
        assert lo == 1_200_000
        assert hi == 1_500_000

    def test_em_dash_separator(self):
        """Em dash (U+2014) is a common separator in real job postings."""
        desc = "Base salary: $119,000 \u2014 $125,500 USD"
        lo, hi = parse_salary(desc)
        assert lo == 119_000
        assert hi == 125_500

    def test_html_entity_em_dash_separator(self):
        """&mdash; entity in HTML descriptions must be decoded before matching."""
        desc = "Salary: $119,000 &mdash; $125,500 USD"
        lo, hi = parse_salary(desc)
        assert lo == 119_000
        assert hi == 125_500

    def test_html_entity_encoded_tags_with_em_dash(self):
        """Real-world case: Instacart-style descriptions with entity-encoded HTML
        tags (e.g. &lt;span&gt;$119,000&lt;/span&gt;&lt;span&gt;&mdash;&lt;/span&gt;).
        Tags and entities must both be decoded to extract the salary range."""
        # Reproduces the Instacart Senior Accountant job that was incorrectly
        # passing the salary pre-filter: salary stored in double-encoded HTML
        # with em dash in a separate &lt;span&gt; element.
        desc = (
            '&lt;span&gt;$109,000&lt;/span&gt;'
            '&lt;span class="divider"&gt;&mdash;&lt;/span&gt;'
            '&lt;span&gt;$115,000 USD&lt;/span&gt;'
        )
        lo, hi = parse_salary(desc)
        assert lo == 109_000
        assert hi == 115_000
