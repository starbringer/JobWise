"""
query_builder.py — Builds the search plan for a pipeline run.

Search architecture:
  1. JSearch (quota-limited, non-ATS companies only):
     Priority 1: required companies NOT already in ats_companies table
     Priority 2: preferred companies NOT in ats_companies table
     Companies with free ATS boards are excluded — no point spending quota on them.

  2. Greenhouse / Lever (free, unlimited):
     - All companies in ats_companies table (unrestricted board scan)
     - Companies from the search list that have ATS boards but didn't fit JSearch budget

  3. JobSpy (general keyword discovery, always runs):
     - Profile-based keyword queries, no company targeting
     - Discovers jobs from companies not in any list
"""

import logging
import math

from src import database

logger = logging.getLogger(__name__)


def build_search_plan(
    conn,
    profile: dict,
    preferred_companies: list[str],
    jsearch_budget: int,
) -> dict:
    """
    Build the full search plan for one pipeline run.

    Returns:
      {
        "jsearch_queries":        list of {query, location, date_posted, extra_params} dicts,
        "ats_slugs":              list of slugs to fetch via Greenhouse/Lever,
        "jobspy_queries":         list of {query, location} dicts,
        "companies_not_covered":  list of company names that couldn't be searched,
      }
    """
    hard = profile.get("hard_requirements", {})
    company_exclude = {c.lower().strip() for c in (hard.get("company_exclude") or [])}

    # --- Build ordered search list ---
    required   = _clean_list(profile.get("target_companies") or [])
    preferred  = _clean_list(preferred_companies)
    search_list = _ordered_dedupe(required, preferred, company_exclude)

    logger.info(
        f"[query_builder] Search list: {len(required)} required + {len(preferred)} preferred "
        f"= {len(search_list)} unique (after excluding {len(company_exclude)} blocked companies)"
    )

    # --- Look up which companies have free ATS boards ---
    ats_lookup = database.get_all_ats_slugs(conn)  # company_lower / slug_lower → slug

    search_list_ats: list[str] = []   # slugs for search-list companies on ATS
    search_list_no_ats: list[str] = []  # company names without ATS boards

    for company in search_list:
        slug = ats_lookup.get(company.lower())
        if slug:
            search_list_ats.append(slug)
        else:
            search_list_no_ats.append(company)

    # Build query enrichment fragments from hard requirements
    title_fragment = _title_fragment(profile)
    query_suffix   = _hard_requirement_suffix(hard)
    location_param = _location_param(hard)
    extra_params   = _jsearch_extra_params(hard)

    # --- Allocate JSearch budget ---
    # JSearch is used ONLY for companies without a free ATS board (Greenhouse/Lever).
    # Companies already in ats_companies are fetched for free — spending JSearch credits
    # on them wastes quota and returns data we already have.
    #
    # Priority order for JSearch (non-ATS companies only):
    #   1. required companies not on any ATS
    #   2. preferred companies not on any ATS
    required_names = [c for c in required if not _is_excluded(c, company_exclude)]
    non_ats_preferred = [c for c in preferred
                         if c not in required_names
                         and not _is_excluded(c, company_exclude)
                         and ats_lookup.get(c.lower()) is None]

    # Split required companies: those with ATS boards are already free, skip for JSearch
    required_no_ats = [c for c in required_names if ats_lookup.get(c.lower()) is None]

    priority_list = required_no_ats + non_ats_preferred

    jsearch_queries, companies_not_covered = _batch_companies(
        priority_list, jsearch_budget, title_fragment, query_suffix, location_param, extra_params
    )

    # ATS slugs: all known ATS companies (unrestricted scan)
    all_ats_rows = conn.execute("SELECT slug FROM ats_companies").fetchall()
    ats_slugs = [row["slug"] for row in all_ats_rows]

    # JobSpy: general keyword discovery + targeted queries for companies not covered by JSearch
    jobspy_queries = _build_jobspy_queries(profile, hard, companies_not_covered)

    if companies_not_covered:
        logger.warning(
            f"[query_builder] {len(companies_not_covered)} companies couldn't fit in JSearch budget: "
            + ", ".join(companies_not_covered[:10])
            + ("..." if len(companies_not_covered) > 10 else "")
        )

    logger.info(
        f"[query_builder] Plan: {len(jsearch_queries)} JSearch queries, "
        f"{len(ats_slugs)} ATS slugs (GH/Lever), "
        f"{len(jobspy_queries)} JobSpy queries."
    )

    return {
        "jsearch_queries": jsearch_queries,
        "ats_slugs": ats_slugs,
        "jobspy_queries": jobspy_queries,
        "companies_not_covered": companies_not_covered,
    }


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _clean_list(items: list) -> list[str]:
    """Strip and deduplicate a list of strings, preserving order."""
    seen = set()
    result = []
    for item in items:
        s = str(item).strip()
        if s and s.lower() not in seen:
            seen.add(s.lower())
            result.append(s)
    return result


def _ordered_dedupe(
    required: list[str],
    preferred: list[str],
    excluded: set[str],
) -> list[str]:
    """Build ordered search list: required first, then preferred, excluding blocked."""
    seen = set()
    result = []
    for company in required + preferred:
        cl = company.lower()
        if cl not in excluded and cl not in seen:
            seen.add(cl)
            result.append(company)
    return result


def _is_excluded(company: str, excluded: set[str]) -> bool:
    return company.lower() in excluded


def _title_fragment(profile: dict) -> str:
    """
    Build the job title part of a query.
    Prefer explicit hard_requirements.job_titles, fall back to target_job_titles.
    Returns a short string like "Staff Engineer OR Principal Engineer".
    """
    hard_titles = (profile.get("hard_requirements") or {}).get("job_titles") or []
    soft_titles = profile.get("target_job_titles") or []
    titles = hard_titles or soft_titles

    if not titles:
        return "Software Engineer"

    # Use first 3 titles to keep query length reasonable
    return " OR ".join(titles[:3])


def _hard_requirement_suffix(hard: dict) -> str:
    """Build words to append to the query string (remote type)."""
    parts = []
    if hard.get("remote_type") == "remote":
        parts.append("remote")
    elif hard.get("remote_type") == "hybrid":
        parts.append("hybrid")
    return " ".join(parts)


def _location_param(hard: dict) -> str:
    """Return the JSearch location parameter value."""
    locs = hard.get("locations") or []
    if locs:
        return locs[0]
    if hard.get("remote_type") == "remote":
        return "remote"
    return ""


def _jsearch_extra_params(hard: dict) -> dict:
    """Build JSearch API extra parameters from hard requirements."""
    params = {}
    if hard.get("remote_type") == "remote":
        params["job_is_remote"] = "true"
    if hard.get("salary_min"):
        params["min_salary"] = str(hard["salary_min"])
    if hard.get("employment_type"):
        params["employment_type"] = hard["employment_type"]
    return params


def _batch_companies(
    companies: list[str],
    budget: int,
    title_fragment: str,
    query_suffix: str,
    location_param: str,
    extra_params: dict,
) -> tuple[list[dict], list[str]]:
    """
    Batch companies into at most `budget` JSearch queries.
    Returns (queries, companies_not_covered).
    """
    if not companies or budget <= 0:
        return [], companies

    per_batch = math.ceil(len(companies) / budget)
    queries = []
    companies_covered = []

    for i in range(0, len(companies), per_batch):
        if len(queries) >= budget:
            break
        batch = companies[i : i + per_batch]
        companies_str = " OR ".join(batch)
        query_parts = [title_fragment, "at", companies_str]
        if query_suffix:
            query_parts.append(query_suffix)
        query = " ".join(query_parts)

        queries.append({
            "query": query,
            "location": location_param,
            "extra_params": extra_params,
        })
        companies_covered.extend(batch)

    not_covered = [c for c in companies if c not in companies_covered]
    return queries, not_covered


def _build_jobspy_queries(profile: dict, hard: dict, companies_not_covered: list[str] | None = None) -> list[dict]:
    """
    Build JobSpy queries:
    - 1-2 general keyword queries for broad discovery (no company filter)
    - 1 targeted query per company in companies_not_covered (companies JSearch budget couldn't reach)
    """
    hard_titles = hard.get("job_titles") or []
    soft_titles = profile.get("target_job_titles") or []
    primary_title = (hard_titles or soft_titles or ["Software Engineer"])[0]

    location = _location_param(hard)

    queries = [{"query": primary_title, "location": location}]

    # Second general query: top keyword + title variation if available
    keywords = profile.get("extracted_keywords") or []
    top_kw = next((k for k in keywords if k.lower() not in primary_title.lower()), None)
    if top_kw and len(soft_titles) > 1:
        alt_title = soft_titles[1]
        queries.append({"query": f"{alt_title} {top_kw}", "location": location})

    # Company-targeted queries for companies not covered by JSearch
    for company in (companies_not_covered or []):
        queries.append({"query": f"{primary_title} at {company}", "location": location})

    return queries
