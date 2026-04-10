"""
location.py — US-focused location matching utilities.

Provides bidirectional US state name ↔ abbreviation expansion so that
"Virginia" automatically matches "McLean, VA", "VA", "Northern Virginia", etc.

Public API
----------
expand(term: str) -> set[str]
    Return all equivalent forms of a location term (lowercased).
    e.g. "virginia" → {"virginia", "va"}
    e.g. "va"       → {"va", "virginia"}
    Non-state terms are returned as a single-element set.

matches(job_location: str, area: str) -> bool
    True if job_location contains any expanded form of area, or vice versa.

matches_any(job_location: str, areas: list[str]) -> bool
    True if matches() is True for at least one area in the list.
"""

from __future__ import annotations

# Canonical US state name → 2-letter abbreviation
_STATE_TO_ABBR: dict[str, str] = {
    "alabama": "al",
    "alaska": "ak",
    "arizona": "az",
    "arkansas": "ar",
    "california": "ca",
    "colorado": "co",
    "connecticut": "ct",
    "delaware": "de",
    "florida": "fl",
    "georgia": "ga",
    "hawaii": "hi",
    "idaho": "id",
    "illinois": "il",
    "indiana": "in",
    "iowa": "ia",
    "kansas": "ks",
    "kentucky": "ky",
    "louisiana": "la",
    "maine": "me",
    "maryland": "md",
    "massachusetts": "ma",
    "michigan": "mi",
    "minnesota": "mn",
    "mississippi": "ms",
    "missouri": "mo",
    "montana": "mt",
    "nebraska": "ne",
    "nevada": "nv",
    "new hampshire": "nh",
    "new jersey": "nj",
    "new mexico": "nm",
    "new york": "ny",
    "north carolina": "nc",
    "north dakota": "nd",
    "ohio": "oh",
    "oklahoma": "ok",
    "oregon": "or",
    "pennsylvania": "pa",
    "rhode island": "ri",
    "south carolina": "sc",
    "south dakota": "sd",
    "tennessee": "tn",
    "texas": "tx",
    "utah": "ut",
    "vermont": "vt",
    "virginia": "va",
    "washington": "wa",
    "west virginia": "wv",
    "wisconsin": "wi",
    "wyoming": "wy",
    # DC is treated as a state-equivalent
    "district of columbia": "dc",
    "washington dc": "dc",
    "washington d.c.": "dc",
}

# Reverse map: abbreviation → full name
_ABBR_TO_STATE: dict[str, str] = {v: k for k, v in _STATE_TO_ABBR.items()}


def expand(term: str) -> set[str]:
    """
    Return all known equivalent forms of *term* (all lowercased).

    Examples
    --------
    >>> expand("Virginia")
    {'virginia', 'va'}
    >>> expand("VA")
    {'va', 'virginia'}
    >>> expand("Northern Virginia")
    {'northern virginia'}   # partial phrases fall through as-is
    """
    t = term.strip().lower()
    if t in _STATE_TO_ABBR:
        return {t, _STATE_TO_ABBR[t]}
    if t in _ABBR_TO_STATE:
        return {t, _ABBR_TO_STATE[t]}
    return {t}


def matches(job_location: str, area: str) -> bool:
    """
    Return True if *job_location* contains any expanded form of *area*,
    or if any expanded form of *area* contains *job_location*.

    Matching is word-boundary–aware: "va" must appear as a whole word/token
    (comma-separated or space-separated segment), not as a substring inside
    another word, to avoid "savannah, ga" matching "va".

    Examples
    --------
    >>> matches("McLean, VA", "Virginia")
    True
    >>> matches("Virginia Beach, VA", "va")
    True
    >>> matches("Savannah, GA", "Virginia")
    False
    """
    if not job_location or not area:
        return False

    job_loc = job_location.strip().lower()
    forms = expand(area)

    # Split job location into tokens (comma/space separated)
    import re
    tokens = set(re.split(r"[\s,]+", job_loc))
    # Also keep the full string for multi-word state names like "west virginia"
    full_parts = [p.strip() for p in job_loc.split(",")]

    for form in forms:
        # 1. Exact token match (handles 2-char abbreviations like "va", "md", "wa")
        if form in tokens:
            return True

        # 2. Substring match — only safe for forms longer than 3 chars.
        #    Short abbreviations (e.g. "va", "wa") must NOT use substring matching
        #    because they appear as substrings in unrelated words ("savannah", "washington").
        if len(form) >= 4:
            # Form appears anywhere in the full location string
            # (handles multi-word names like "west virginia", "new york")
            if form in job_loc:
                return True
            # Comma-separated part contains the form, or the form contains
            # the part (e.g. "washington" in "washington d.c." for area=DC).
            # Guard against short parts re-introducing the abbreviation false positive.
            for part in full_parts:
                if form in part:
                    return True
                if len(part) >= 4 and part in form:
                    return True

    return False


def matches_any(job_location: str, areas: list[str]) -> bool:
    """Return True if *job_location* matches at least one area in *areas*."""
    return any(matches(job_location, area) for area in areas)
