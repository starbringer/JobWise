"""
salary_parser.py — Extract salary ranges from job description text.

Used as a fallback when the job source does not provide structured salary fields.
Supports zone-based salary tables (e.g. "$150k–$170k in SF/NYC, $130k–$150k in Seattle")
and picks the range that best matches the candidate's preferred locations.
"""

import html as _html
import re
from typing import Optional

# ---------------------------------------------------------------------------
# Salary range extraction
# ---------------------------------------------------------------------------

# Matches ranges like:
#   $150,000 – $180,000   $150k-$180k   150,000 to 180,000   $1.5M–$2M
_RANGE_RE = re.compile(
    r"""
    \$\s*(?P<lo>\d{1,3}(?:,\d{3})*(?:\.\d+)?[kKmM]?   # lower bound, must start with $
         |\d+(?:\.\d+)?[kKmM])
    \s*(?:—|–|-|to|\/)\s*                         # separator (em dash, en dash, hyphen, to, /)
    \$?\s*(?P<hi>\d{1,3}(?:,\d{3})*(?:\.\d+)?[kKmM]?  # upper bound
              |\d+(?:\.\d+)?[kKmM])
    (?:\s*(?:per\s+year|annually|\/\s*(?:yr|year)|USD|\s*a\s+year))?
    """,
    re.VERBOSE | re.IGNORECASE,
)

# Matches a single figure: "$150,000 per year" / "$150k annually"
_SINGLE_RE = re.compile(
    r"""\$\s*(?P<val>\d{1,3}(?:,\d{3})*(?:\.\d+)?[kKmM]?|\d+(?:\.\d+)?[kKmM])
        \s*(?:per\s+year|annually|\/\s*(?:yr|year)|a\s+year)""",
    re.VERBOSE | re.IGNORECASE,
)


def _to_int(raw: str) -> Optional[int]:
    """Normalise a salary token (e.g. '150k', '1.5M', '150,000') to an integer."""
    raw = raw.replace(",", "").strip()
    multiplier = 1
    if raw[-1].lower() == "k":
        multiplier = 1_000
        raw = raw[:-1]
    elif raw[-1].lower() == "m":
        multiplier = 1_000_000
        raw = raw[:-1]
    try:
        return int(float(raw) * multiplier)
    except ValueError:
        return None


def _is_plausible(lo: int, hi: int) -> bool:
    """Reject obviously wrong figures (typos, non-salary numbers)."""
    # Annual salary: $20k–$5M is the credible range
    return 20_000 <= lo <= 5_000_000 and lo < hi and hi / lo < 5


# ---------------------------------------------------------------------------
# Location zone matching
# ---------------------------------------------------------------------------

# Maps lowercase location keywords → canonical zone tag
_ZONE_KEYWORDS: list[tuple[set[str], str]] = [
    ({"san francisco", "sf", "bay area", "california", "ca", "silicon valley", "palo alto",
      "mountain view", "menlo park", "san jose", "sunnyvale"}, "sf"),
    ({"new york", "nyc", "ny", "manhattan", "brooklyn", "new jersey", "nj"}, "ny"),
    ({"seattle", "washington state", "wa", "bellevue", "redmond", "kirkland"}, "seattle"),
    ({"washington dc", "washington, dc", "dc", "virginia", "va", "maryland", "md",
      "northern virginia", "nova", "arlington", "bethesda", "reston", "tysons",
      "dmv", "dmv area"}, "dc"),
    ({"boston", "massachusetts", "ma", "cambridge"}, "boston"),
    ({"chicago", "illinois", "il"}, "chicago"),
    ({"austin", "texas", "tx", "dallas", "houston"}, "texas"),
    ({"denver", "colorado", "co", "boulder"}, "denver"),
    ({"remote", "anywhere", "us-based", "united states", "nationwide", "all locations",
      "all us", "anywhere in the us", "other locations"}, "remote"),
]


def _location_zone(text: str) -> Optional[str]:
    """Return the best-matching zone tag for a snippet of text, or None."""
    t = text.lower()
    for keywords, zone in _ZONE_KEYWORDS:
        if any(kw in t for kw in keywords):
            return zone
    return None


def _profile_zones(preferred_locations: list[str]) -> set[str]:
    """Convert a profile's preferred_locations list into a set of zone tags."""
    zones: set[str] = set()
    for loc in preferred_locations or []:
        z = _location_zone(loc)
        if z:
            zones.add(z)
    return zones


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

# Matches Amazon-style ranges: "from $X [/year] ... up to $Y [/year]"
# where the gap between bounds can contain geographic qualifiers like
# "in our lowest geographic market" (up to ~200 chars, no dollar signs).
# Examples:
#   "from $129,300/year in our lowest geographic market up to $223,600/year"
#   "$80,000/year up to $120,000"
_FROM_UPTO_RE = re.compile(
    r'\$\s*(?P<lo>\d{1,3}(?:,\d{3})*(?:\.\d+)?[kKmM]?)'  # lower bound
    r'(?:/(?:yr|year))?\s*'                                  # optional /year
    r'[^$\n]{0,220}?'                                        # gap — no $, no newline
    r'\bup\s+to\b\s+'                                        # "up to"
    r'\$\s*(?P<hi>\d{1,3}(?:,\d{3})*(?:\.\d+)?[kKmM]?)',  # upper bound
    re.IGNORECASE,
)


def parse_salary(description: str, preferred_locations: list[str] | None = None
                 ) -> tuple[Optional[int], Optional[int]]:
    """
    Parse a salary range from a job description.

    If multiple ranges exist (zone-based salary tables), picks the one whose
    surrounding text best matches the candidate's preferred_locations.
    Falls back to the first valid range if no zone match is found.

    Returns (salary_min, salary_max) as annual USD integers, or (None, None).
    """
    if not description:
        return None, None

    # Descriptions are often stored as HTML (sometimes with entity-encoded tags, e.g.
    # &lt;span&gt;).  Decode entities first (revealing actual < > characters), then
    # strip the resulting HTML tags, then decode any remaining entities (e.g. &mdash;
    # inside a <span> that is now gone) to get clean plain text for regex matching.
    description = _html.unescape(description)
    if "<" in description:
        description = re.sub(r"<[^>]+>", " ", description)
        description = _html.unescape(description)

    # Find all ranges with their positions
    candidates: list[tuple[int, int, int]] = []  # (lo, hi, match_start)
    for m in _RANGE_RE.finditer(description):
        lo = _to_int(m.group("lo"))
        hi = _to_int(m.group("hi"))
        if lo and hi and _is_plausible(lo, hi):
            candidates.append((lo, hi, m.start()))

    # Fallback: try "from $X ... up to $Y" pattern (e.g. Amazon geographic ranges)
    if not candidates:
        for m in _FROM_UPTO_RE.finditer(description):
            lo = _to_int(m.group("lo"))
            hi = _to_int(m.group("hi"))
            if lo and hi and _is_plausible(lo, hi):
                candidates.append((lo, hi, m.start()))

    if not candidates:
        # Fall back to single-figure match (treat as both min and max)
        m = _SINGLE_RE.search(description)
        if m:
            val = _to_int(m.group("val"))
            if val and 20_000 <= val <= 5_000_000:
                return val, val
        return None, None

    if len(candidates) == 1 or not preferred_locations:
        lo, hi, _ = candidates[0]
        return lo, hi

    # Multiple ranges — try zone matching
    profile_zones = _profile_zones(preferred_locations)
    if not profile_zones:
        lo, hi, _ = candidates[0]
        return lo, hi

    # Score each candidate by how well its surrounding context matches the profile zones
    best_lo, best_hi = candidates[0][0], candidates[0][1]
    best_score = -1

    for lo, hi, pos in candidates:
        # Look at ±200 chars around the salary figure for location cues
        snippet = description[max(0, pos - 200): pos + 200].lower()
        score = 0
        for keywords, zone in _ZONE_KEYWORDS:
            if zone in profile_zones and any(kw in snippet for kw in keywords):
                score += 2   # direct profile zone hit
            elif zone == "remote" and "remote" in profile_zones and any(kw in snippet for kw in keywords):
                score += 1   # remote is also acceptable
        if score > best_score:
            best_score = score
            best_lo, best_hi = lo, hi

    return best_lo, best_hi
