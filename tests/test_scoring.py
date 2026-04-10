"""
tests/test_scoring.py
~~~~~~~~~~~~~~~~~~~~~
Unit tests for scoring correctness, description completeness, and location filtering.

Covers five bugs fixed in the ranker:

Issue 1 — extract_description: section classifier missed "Required Qualifications"
  and similar common headers, causing the AI to receive only pay-range boilerplate.

Issue 2 — Location pre-filter: jobs outside the profile's hard_requirements.locations
  must be rejected before scoring.  Verifies pre_filter rejects Seattle, WA for a
  profile that only accepts VA/MD/DC.

Issue 3 — Description completeness: jobs whose description is pure company-overview
  boilerplate (no requirements/responsibilities sections) must be flagged as
  missing_info, not scored.  is_description_scoreable() is the gate.

Issue 4 — Salary pre-filter: jobs below the profile's minimum salary must be
  rejected even when the job's salary_min DB field is NULL.  pre_filter must fall
  back to parsing salary from the description text before giving benefit of the
  doubt.  Root cause: INSERT OR IGNORE in deduplicator means salary fields parsed
  from descriptions are never written back to already-existing DB rows, so jobs
  whose descriptions were fetched after initial insertion always had salary_min=NULL.

Issue 5 — sqlite3.Row in refilter: pre_filter used job.get("description") which
  fails on sqlite3.Row objects (they only support subscript access, not .get()).
  refilter() passes sqlite3.Row rows from the DB directly into pre_filter, so any
  .get() call crashes the refilter pipeline with AttributeError.

Run from the project root:
    pytest tests/test_scoring.py -v
"""

import sqlite3
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

import pytest
from src.ranker import (
    extract_description,
    is_description_scoreable,
    pre_filter,
    compute_scores_public,
)

# ---------------------------------------------------------------------------
# Shared fixtures
# ---------------------------------------------------------------------------

# Minimal profile matching laohu's hard constraints (VA/MD/DC locations).
_PROFILE_VA = {
    "profile_name": "test",
    "hard_requirements": {
        "locations": ["VA", "MD", "DC"],
        "salary_min": None,
        "remote_type": None,
        "company_exclude": [],
        "country": ["US"],
        "has_clearance": None,
        "exclude_industries": [],
        "exclude_company_types": [],
    },
    "target_job_titles": ["Software Engineer"],
    "must_haves": [],
    "nice_to_haves": [],
}

# Minimal job dict with all fields pre_filter reads.
def _job(location: str, remote_type: str = "unknown", **kwargs) -> dict:
    return {
        "job_key": "test-key",
        "title": "Senior Software Engineer",
        "company": "Test Corp",
        "location": location,
        "remote_type": remote_type,
        "salary_min": None,
        "salary_max": None,
        "description": "Some description.",
        **kwargs,
    }


# ---------------------------------------------------------------------------
# Issue 1 — extract_description section classifier
# ---------------------------------------------------------------------------

class TestExtractDescriptionSectionHeaders:
    """
    Verifies that the section classifier recognises a wide variety of common
    job-posting section headers — especially headers that were previously missed
    (e.g. 'Required Qualifications', 'Preferred Requirements', 'Role Description').
    """

    # --- "Required Qualifications" style (CVS Health, many enterprise employers) ---

    def test_required_qualifications_header_recognized(self):
        """
        A description using 'Required Qualifications' must keep the requirements
        content and NOT return only the pay-range section.
        """
        desc = """
We're a great company doing great things.

**Position Summary**

We are seeking an experienced engineer to join our team.

**Required Qualifications**

* 10+ years of software engineering experience
* Proficiency in Python and Go
* Experience with distributed systems

**Preferred Qualifications**

* Experience with Kubernetes
* Familiarity with observability tooling

**Pay Range**

The typical pay range for this role is $150,000 - $200,000.
"""
        extracted = extract_description(desc, max_chars=3500)
        assert "10+ years" in extracted, "Required Qualifications content must be kept"
        assert "Python and Go" in extracted, "Required Qualifications content must be kept"
        assert "Kubernetes" in extracted, "Preferred Qualifications content must be kept"
        # Company intro should be stripped
        assert "We're a great company" not in extracted

    def test_required_qualifications_not_pay_range_only(self):
        """
        The extracted result must NOT start with 'Pay Range' when the description
        contains 'Required Qualifications' — that was the original bug.
        """
        desc = """
About Us: We are a healthcare company.

**Required Qualifications**

* 5+ years backend experience
* Java, Go, Python

**Pay Range**

$120,000 - $180,000
"""
        extracted = extract_description(desc, max_chars=3500)
        assert not extracted.startswith("Pay Range"), (
            "Extraction must not start with Pay Range when requirements exist"
        )
        assert "5+ years backend" in extracted

    def test_preferred_requirements_header(self):
        desc = "**Preferred Requirements**\n\n* Kubernetes experience\n* AWS familiarity\n"
        extracted = extract_description(desc, max_chars=3500)
        assert "Kubernetes" in extracted

    def test_role_description_header(self):
        desc = "**Role Description**\n\n* Lead design of distributed systems\n* Mentor junior engineers\n"
        extracted = extract_description(desc, max_chars=3500)
        assert "Lead design" in extracted

    def test_position_summary_header(self):
        desc = "**Position Summary**\n\n* Build and scale backend services\n* Collaborate with product teams\n"
        extracted = extract_description(desc, max_chars=3500)
        assert "Build and scale" in extracted

    def test_standalone_required_colon(self):
        desc = "**Required:**\n\n* 8+ years Python\n* Distributed systems design\n"
        extracted = extract_description(desc, max_chars=3500)
        assert "8+ years" in extracted

    def test_standalone_skills_header(self):
        desc = "**Skills:**\n\n* Go, Python, Rust\n* Kubernetes, Terraform\n"
        extracted = extract_description(desc, max_chars=3500)
        assert "Go, Python" in extracted

    def test_what_we_are_looking_for_header(self):
        desc = "**What We Are Looking For:**\n\n* 5+ years of backend experience\n* Strong system design skills\n"
        extracted = extract_description(desc, max_chars=3500)
        assert "5+ years" in extracted

    def test_embedded_header_split(self):
        """
        A bold header embedded at the end of a bullet list (no blank line before it)
        must still be recognised as a section header.
        This was the 'Preferred Qualifications' bug in the CVS description.
        """
        desc = (
            "**Required Qualifications**\n\n"
            "* 8+ years of experience\n"
            "* Java, Go, Python\n"
            "**Preferred Qualifications**\n\n"  # no blank line before the header
            "* Kubernetes knowledge\n"
            "* Observability tools\n"
            "\n"
            "**Pay Range**\n\n"
            "$130,000 - $200,000\n"
        )
        extracted = extract_description(desc, max_chars=3500)
        assert "8+ years" in extracted
        assert "Kubernetes" in extracted

    def test_cvs_principal_fixture(self):
        """
        The actual CVS Health Principal SDE job description stored in the DB must
        produce an extraction that includes the Required Qualifications content and
        does NOT start with 'Pay Range'.
        """
        fixture = Path(__file__).parent / "fixtures" / "cvs_principal.html"
        if not fixture.exists():
            pytest.skip("CVS Principal fixture not found")
        desc = fixture.read_text(encoding="utf-8")

        extracted = extract_description(desc, max_chars=3500)
        # Must contain actual job requirements
        assert "12+" in extracted or "years" in extracted, (
            "CVS extraction must contain experience requirements, not just pay range"
        )
        assert not extracted.startswith("**Pay Range**"), (
            "CVS extraction must not start with Pay Range"
        )


# ---------------------------------------------------------------------------
# Issue 2 — pre_filter location enforcement
# ---------------------------------------------------------------------------

class TestPreFilterLocation:
    """
    Verifies that pre_filter correctly rejects jobs whose location falls outside
    the profile's hard_requirements.locations list.
    """

    def test_seattle_rejected_for_va_md_dc_profile(self):
        """Seattle, WA must be rejected when profile requires VA/MD/DC."""
        jobs = [_job("Seattle, WA")]
        passed, rejected = pre_filter(jobs, _PROFILE_VA)
        assert len(passed) == 0, "Seattle, WA must not pass VA/MD/DC location filter"
        assert len(rejected) == 1
        _, reason = rejected[0]
        assert "seattle" in reason.lower() or "location" in reason.lower()

    def test_mclean_va_passes(self):
        """McLean, VA must pass for a profile requiring VA."""
        jobs = [_job("McLean, VA")]
        passed, rejected = pre_filter(jobs, _PROFILE_VA)
        assert len(passed) == 1, "McLean, VA must pass VA/MD/DC location filter"

    def test_bethesda_md_passes(self):
        """Bethesda, MD must pass."""
        jobs = [_job("Bethesda, MD")]
        passed, rejected = pre_filter(jobs, _PROFILE_VA)
        assert len(passed) == 1

    def test_washington_dc_passes(self):
        """Washington, DC must pass."""
        jobs = [_job("Washington, DC")]
        passed, rejected = pre_filter(jobs, _PROFILE_VA)
        assert len(passed) == 1

    def test_fully_remote_passes_regardless_of_location(self):
        """
        A job listed as remote_type='remote' must pass even if its location field
        says Seattle — remote jobs are not location-constrained.
        """
        jobs = [_job("Seattle, WA", remote_type="remote")]
        passed, rejected = pre_filter(jobs, _PROFILE_VA)
        assert len(passed) == 1, "Fully-remote jobs must pass location filter"

    def test_new_york_rejected(self):
        """New York must be rejected for VA/MD/DC profile."""
        jobs = [_job("New York, NY")]
        passed, rejected = pre_filter(jobs, _PROFILE_VA)
        assert len(passed) == 0

    def test_san_francisco_rejected(self):
        """San Francisco, CA must be rejected."""
        jobs = [_job("San Francisco, CA")]
        passed, rejected = pre_filter(jobs, _PROFILE_VA)
        assert len(passed) == 0

    def test_no_location_constraint_passes_everything(self):
        """When profile has no locations constraint, all locations must pass."""
        profile_no_loc = {
            **_PROFILE_VA,
            "hard_requirements": {**_PROFILE_VA["hard_requirements"], "locations": []},
        }
        jobs = [_job("Seattle, WA"), _job("New York, NY"), _job("McLean, VA")]
        passed, rejected = pre_filter(jobs, profile_no_loc)
        assert len(passed) == 3, "No location constraint means all locations pass"

    def test_virginia_full_name_passes(self):
        """Location string 'Virginia' must match the 'VA' constraint."""
        jobs = [_job("Northern Virginia")]
        passed, rejected = pre_filter(jobs, _PROFILE_VA)
        assert len(passed) == 1, "Northern Virginia must match VA constraint"


# ---------------------------------------------------------------------------
# Issue 3 — is_description_scoreable() completeness gate
# ---------------------------------------------------------------------------

class TestIsDescriptionScoreable:
    """
    Verifies that is_description_scoreable() correctly distinguishes substantive
    job descriptions from company-overview boilerplate.
    """

    # Pure company overview — the exact Plaid description that triggered the bug.
    PLAID_BOILERPLATE = (
        "We believe that the way people interact with their finances will drastically "
        "improve in the next few years. We're dedicated to empowering this transformation "
        "by building the tools and experiences that thousands of developers use to create "
        "their own products. Plaid powers the tools millions of people rely on to live a "
        "healthier financial life. We work with thousands of companies like Venmo, SoFi, "
        "several of the Fortune 500, and many of the largest banks to make it easy for "
        "people to connect their financial accounts to the apps and services they want to "
        "use. Plaid's network covers 12,000 financial institutions across the US, Canada, "
        "UK and Europe. Founded in 2013, the company is headquartered in San Francisco "
        "with offices in New York, Washington D.C., London and Amsterdam."
    )

    def test_plaid_boilerplate_not_scoreable(self):
        """Pure company overview with no job requirements must NOT be scoreable."""
        assert not is_description_scoreable(self.PLAID_BOILERPLATE), (
            "Company overview boilerplate must not be considered scoreable"
        )

    def test_empty_description_not_scoreable(self):
        assert not is_description_scoreable("")
        assert not is_description_scoreable(None)

    def test_pay_range_only_not_scoreable(self):
        """A description that only has a pay range section must NOT be scoreable."""
        desc = "**Pay Range**\n\nThe typical pay range is $100,000 - $150,000 per year."
        assert not is_description_scoreable(desc), (
            "Pay range only must not be considered scoreable"
        )

    def test_description_with_requirements_is_scoreable(self):
        """A description with 'Required Qualifications' must be scoreable."""
        desc = (
            "**Required Qualifications**\n\n"
            "* 5+ years of software engineering experience\n"
            "* Proficiency in Python, Go\n"
        )
        assert is_description_scoreable(desc)

    def test_description_with_responsibilities_is_scoreable(self):
        """A description with 'Responsibilities' must be scoreable."""
        desc = (
            "**Responsibilities**\n\n"
            "* Design and build distributed systems\n"
            "* Lead technical initiatives\n"
        )
        assert is_description_scoreable(desc)

    def test_description_with_basic_qualifications_is_scoreable(self):
        """Amazon-style 'BASIC QUALIFICATIONS' must be scoreable."""
        desc = (
            "**BASIC QUALIFICATIONS**\n\n"
            "* 5+ years of non-internship professional software development experience\n"
        )
        assert is_description_scoreable(desc)

    def test_description_with_what_you_will_do_is_scoreable(self):
        """'What You Will Do' section must be scoreable."""
        desc = (
            "**What You Will Do**\n\n"
            "* Architect and scale our backend services\n"
            "* Collaborate with product managers\n"
        )
        assert is_description_scoreable(desc)

    def test_cvs_principal_is_scoreable_after_fix(self):
        """
        The CVS Principal description must be scoreable after the regex fix —
        it was previously misclassified as non-scoreable because 'Required Qualifications'
        was not recognised.
        """
        fixture = Path(__file__).parent / "fixtures" / "cvs_principal.html"
        if not fixture.exists():
            pytest.skip("CVS Principal fixture not found")
        desc = fixture.read_text(encoding="utf-8")
        assert is_description_scoreable(desc), (
            "CVS Principal description must be scoreable (contains 'Required Qualifications')"
        )


# ---------------------------------------------------------------------------
# Scoring formula regression tests (prevent future regressions)
# ---------------------------------------------------------------------------

class TestComputeScoresRegression:
    """
    Regression tests for the scoring formula to ensure known correct values
    remain stable after any future changes.
    """

    _SCORING_CFG = {
        "manager":   {"required": 10, "preferred": 6, "nice_to_have": 3, "unknown": 5, "extra_skill": 1},
        "candidate": {"must_have": 10, "nice_to_have": 5, "unknown": 7},
    }

    def test_cvs_lead_observability_manager_score(self):
        """
        CVS Lead Observability Platform Engineer: 7 matched pairs (5 required, 2 preferred),
        all matches.

        Previously this returned 128 because depth=7/25=0.275 compressed the score.
        That behaviour caused a real bug: the all-matched Director job (11 pairs, total=40,
        score=128) ranked below a PM Finance job with 2 preferred gaps (6 pairs, total=9,
        score=136) — an all-match job beaten by a partial-match job purely due to
        total_job_requirements inflation.

        Fix: mgr_depth is always 1.0; score = ratio_mgr directly.
        With all 7 pairs matched, ratio = 200, so score = 200.
        total_job_requirements no longer affects manager_score.
        """
        pairs = [
            {"category": "experience", "job_side": "7+ years SE", "candidate_side": "15+ yrs", "job_importance": "required"},
            {"category": "skill", "job_side": "Go, Java", "candidate_side": "Go, Java, Kotlin", "job_importance": "required"},
            {"category": "skill", "job_side": "Observability", "candidate_side": "OpenTelemetry", "job_importance": "required"},
            {"category": "skill", "job_side": "K8s, Docker, AWS", "candidate_side": "K8s, Docker, AWS", "job_importance": "required"},
            {"category": "skill", "job_side": "Data pipelines", "candidate_side": "Elasticsearch, SQL", "job_importance": "required"},
            {"category": "skill", "job_side": "Service mesh, Kafka", "candidate_side": "Service Mesh", "job_importance": "preferred"},
            {"category": "skill", "job_side": "Mentoring, incidents", "candidate_side": "Mentorship, P1", "job_importance": "preferred"},
        ]
        mgr, _, _ = compute_scores_public(pairs, self._SCORING_CFG, total_job_requirements=25, ideal_cand_pairs=6)
        assert mgr == 200, f"Expected 200 for CVS Lead Observability (all matched), got {mgr}"

    def test_all_matches_beats_partial_match(self):
        """
        A job where all extracted pairs are matches must always score higher than a job
        where some pairs are gaps, regardless of total_job_requirements.

        This replaces test_all_matches_low_coverage_not_200 which asserted the opposite:
        that low depth (few pairs / many reported requirements) should compress scores
        toward 100.  That design caused all-match jobs to rank below partial-match jobs
        when Claude over-counted total_job_requirements (e.g. 40 line-items vs 11 pairs).
        """
        all_match_pairs = [
            {"category": "experience", "job_side": "Senior BE", "candidate_side": "15+ yrs", "job_importance": "required"},
            {"category": "skill", "job_side": "Microservices", "candidate_side": "Go, microservices", "job_importance": "required"},
            {"category": "domain", "job_side": "Fintech", "candidate_side": "Fintech", "job_importance": "required"},
            {"category": "domain", "job_side": "Fortune 500", "candidate_side": "SAP", "job_importance": "required"},
        ]
        partial_match_pairs = all_match_pairs + [
            {"category": "skill", "job_side": "PMP cert", "job_importance": "preferred"},
            {"category": "skill", "job_side": "Change mgmt cert", "job_importance": "preferred"},
        ]
        mgr_all, _, _ = compute_scores_public(
            all_match_pairs, self._SCORING_CFG, total_job_requirements=20, ideal_cand_pairs=6
        )
        mgr_partial, _, _ = compute_scores_public(
            partial_match_pairs, self._SCORING_CFG, total_job_requirements=9, ideal_cand_pairs=6
        )
        assert mgr_all == 200, f"All-matched should score 200, got {mgr_all}"
        assert mgr_all > mgr_partial, (
            f"All-matched ({mgr_all}) must rank above partial-match ({mgr_partial})"
        )


# ---------------------------------------------------------------------------
# Issue 4 — pre_filter salary floor (recurring regression)
# ---------------------------------------------------------------------------

# Profile that enforces a $150,000 minimum salary (mirrors miao's real profile).
_PROFILE_SALARY = {
    "profile_name": "miao",
    "preferred_locations": ["Virginia", "DC", "Maryland"],
    "hard_requirements": {
        "salary_min": 150_000,
        "remote_type": None,
        "locations": [],
        "company_exclude": [],
        "country": ["US"],
        "has_clearance": None,
        "exclude_industries": [],
        "exclude_company_types": [],
        "exclude_titles": [],
    },
    "target_job_titles": ["Senior Accountant"],
    "must_haves": [],
    "nice_to_haves": [],
}

# Description that clearly states a salary range below the minimum.
_DESC_BELOW_MIN = (
    "Senior Accountant, Revenue Accounting.\n\n"
    "Responsibilities: reconcile revenue accounts, prepare journal entries.\n\n"
    "Compensation: $120,000 - $145,000 per year based on experience."
)

# Description that clearly states a salary range above the minimum.
_DESC_ABOVE_MIN = (
    "Senior Accountant, Revenue Accounting.\n\n"
    "Responsibilities: reconcile revenue accounts, prepare journal entries.\n\n"
    "Compensation: $155,000 - $185,000 per year based on experience."
)

# Description with no salary information at all.
_DESC_NO_SALARY = (
    "Senior Accountant, Revenue Accounting.\n\n"
    "Responsibilities: reconcile revenue accounts, prepare journal entries."
)

# Description with max above floor but min below — the exact failure case.
_DESC_MIN_BELOW_MAX_ABOVE = (
    "Senior Accountant, Revenue Accounting — Retailer\n\n"
    "Responsibilities: month-end close, revenue reconciliation.\n\n"
    "The expected salary range for this position is $140,000 - $165,000 annually."
)


def _salary_job(salary_min=None, salary_max=None, description="", title="Senior Accountant") -> dict:
    """Build a minimal job dict for salary pre-filter tests."""
    return {
        "job_key": "test-salary-key",
        "title": title,
        "company": "Retailer Inc",
        "location": "Remote",
        "remote_type": "remote",
        "salary_min": salary_min,
        "salary_max": salary_max,
        "description": description,
    }


class TestPreFilterSalaryFloor:
    """
    Issue 4 regression — salary floor filter must work even when salary_min is
    not stored in the DB field (salary_min=None).

    Root cause: deduplicator uses INSERT OR IGNORE, so salary data parsed from
    descriptions is never written back to existing rows.  On the next pipeline
    run, salary_min=NULL in DB, and the filter gave benefit of the doubt instead
    of falling back to description parsing.
    """

    # --- structured DB field present ---

    def test_db_salary_below_floor_rejected(self):
        """Job with DB salary_min below floor must be rejected."""
        jobs = [_salary_job(salary_min=140_000, salary_max=165_000)]
        passed, rejected = pre_filter(jobs, _PROFILE_SALARY)
        assert len(passed) == 0, "salary_min $140k must be rejected against $150k floor"
        assert len(rejected) == 1
        _, reason = rejected[0]
        assert "140,000" in reason and "150,000" in reason

    def test_db_salary_at_floor_passes(self):
        """Job with DB salary_min exactly at floor must pass."""
        jobs = [_salary_job(salary_min=150_000, salary_max=180_000)]
        passed, rejected = pre_filter(jobs, _PROFILE_SALARY)
        assert len(passed) == 1, "salary_min exactly at floor must pass"

    def test_db_salary_above_floor_passes(self):
        """Job with DB salary_min above floor must pass."""
        jobs = [_salary_job(salary_min=160_000, salary_max=200_000)]
        passed, rejected = pre_filter(jobs, _PROFILE_SALARY)
        assert len(passed) == 1

    # --- DB field absent; salary in description ---

    def test_desc_salary_below_floor_rejected_when_db_null(self):
        """
        THE RECURRING BUG: when DB salary_min is NULL but description states a
        salary range below the floor, the job must be rejected via description
        fallback — not passed with benefit of the doubt.
        """
        jobs = [_salary_job(salary_min=None, salary_max=None, description=_DESC_BELOW_MIN)]
        passed, rejected = pre_filter(jobs, _PROFILE_SALARY)
        assert len(passed) == 0, (
            "Job with salary $120k-$145k in description must be rejected against $150k floor "
            "even when DB salary_min is NULL"
        )
        assert len(rejected) == 1

    def test_retailer_exact_case_rejected(self):
        """
        Exact reproduction of 'Senior Accountant, Revenue Accounting - Retailer':
        description states $140k-$165k; DB salary_min=NULL; floor=$150k.
        Must be rejected because $140k < $150k.
        """
        jobs = [_salary_job(
            salary_min=None,
            salary_max=None,
            description=_DESC_MIN_BELOW_MAX_ABOVE,
            title="Senior Accountant, Revenue Accounting - Retailer",
        )]
        passed, rejected = pre_filter(jobs, _PROFILE_SALARY)
        assert len(passed) == 0, (
            "'Senior Accountant, Revenue Accounting - Retailer' ($140k min) must be rejected "
            "against $150k floor even when DB salary_min is NULL"
        )

    def test_html_encoded_description_salary_below_floor_rejected(self):
        """
        Regression: Instacart 'Senior Accountant, Revenue Accounting - Retailer' was
        shown in Miao's job list despite its salary being below $150k.

        Root cause: description is stored as double-encoded HTML
        (&lt;span&gt;$109,000&lt;/span&gt;&lt;span&gt;&mdash;&lt;/span&gt;…).
        parse_salary must decode HTML entities and strip tags before applying
        the salary regex; failing to do so returns (None, None), granting
        benefit-of-the-doubt and letting the job pass the filter incorrectly.
        """
        html_desc = (
            '&lt;span&gt;$109,000&lt;/span&gt;'
            '&lt;span class="divider"&gt;&mdash;&lt;/span&gt;'
            '&lt;span&gt;$115,000 USD&lt;/span&gt;'
        )
        jobs = [_salary_job(
            salary_min=None,
            salary_max=None,
            description=html_desc,
            title="Senior Accountant, Revenue Accounting - Retailer",
        )]
        passed, rejected = pre_filter(jobs, _PROFILE_SALARY)
        assert len(passed) == 0, (
            "HTML-encoded description with $109k salary must be rejected against $150k floor — "
            "salary_parser must decode HTML entities and strip tags before matching"
        )

    def test_desc_salary_above_floor_passes_when_db_null(self):
        """DB salary_min NULL but description states salary above floor → pass."""
        jobs = [_salary_job(salary_min=None, salary_max=None, description=_DESC_ABOVE_MIN)]
        passed, rejected = pre_filter(jobs, _PROFILE_SALARY)
        assert len(passed) == 1, (
            "Job with salary $155k-$185k in description must pass $150k floor"
        )

    def test_no_salary_anywhere_passes_benefit_of_doubt(self):
        """DB salary_min NULL and no salary in description → benefit of the doubt → pass."""
        jobs = [_salary_job(salary_min=None, salary_max=None, description=_DESC_NO_SALARY)]
        passed, rejected = pre_filter(jobs, _PROFILE_SALARY)
        assert len(passed) == 1, (
            "Job with no salary info anywhere must pass (benefit of the doubt)"
        )

    def test_no_salary_requirement_in_profile_passes_all(self):
        """When profile has no salary_min requirement, all salary situations pass."""
        profile_no_salary = {
            **_PROFILE_SALARY,
            "hard_requirements": {**_PROFILE_SALARY["hard_requirements"], "salary_min": None},
        }
        jobs = [
            _salary_job(salary_min=50_000),
            _salary_job(salary_min=None, description=_DESC_BELOW_MIN),
            _salary_job(salary_min=None, description=_DESC_NO_SALARY),
        ]
        passed, rejected = pre_filter(jobs, profile_no_salary)
        assert len(passed) == 3, "No salary floor means all jobs pass salary check"

    def test_empty_description_passes_benefit_of_doubt(self):
        """DB salary_min NULL and empty description → benefit of the doubt → pass."""
        jobs = [_salary_job(salary_min=None, salary_max=None, description="")]
        passed, rejected = pre_filter(jobs, _PROFILE_SALARY)
        assert len(passed) == 1


# ---------------------------------------------------------------------------
# Issue 5 — pre_filter must accept sqlite3.Row objects (refilter regression)
# ---------------------------------------------------------------------------

def _sqlite3_row(**kwargs) -> sqlite3.Row:
    """
    Create a real sqlite3.Row from an in-memory DB so pre_filter is tested
    with the exact type refilter() passes in.  sqlite3.Row supports subscript
    access (row["col"]) but NOT .get(), so any accidental .get() call raises
    AttributeError.
    """
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    cols = list(kwargs.keys())
    placeholders = ", ".join("?" for _ in cols)
    col_defs = ", ".join(cols)
    conn.execute(f"CREATE TABLE t ({col_defs})")
    conn.execute(f"INSERT INTO t VALUES ({placeholders})", list(kwargs.values()))
    return conn.execute("SELECT * FROM t").fetchone()


class TestPreFilterAcceptsSqlite3Row:
    """
    Issue 5 regression — refilter() passes sqlite3.Row objects directly into
    pre_filter().  sqlite3.Row has no .get() method, so any field access using
    .get() raises AttributeError and crashes the refilter pipeline.

    Root cause: ranker.py line 698 used job.get("description") instead of
    job["description"].  Fix: use subscript access consistently.
    """

    def _row(self, salary_min=None, salary_max=None, description="", location="Remote",
             remote_type="remote"):
        return _sqlite3_row(
            job_key="test-key",
            title="Senior Software Engineer",
            company="Test Corp",
            location=location,
            remote_type=remote_type,
            salary_min=salary_min,
            salary_max=salary_max,
            description=description,
        )

    def test_sqlite3_row_passes_without_error(self):
        """
        pre_filter must not raise AttributeError when given sqlite3.Row objects.
        This is the exact failure mode of the refilter pipeline before the fix.
        """
        row = self._row(salary_min=None, description=_DESC_NO_SALARY)
        # Must not raise AttributeError: 'sqlite3.Row' object has no attribute 'get'
        passed, rejected = pre_filter([row], _PROFILE_SALARY)
        assert len(passed) == 1  # no salary info → benefit of the doubt

    def test_sqlite3_row_salary_below_floor_rejected(self):
        """sqlite3.Row with low salary in description must still be rejected."""
        row = self._row(salary_min=None, description=_DESC_BELOW_MIN)
        passed, rejected = pre_filter([row], _PROFILE_SALARY)
        assert len(passed) == 0, (
            "sqlite3.Row with $120k-$145k description must be rejected against $150k floor"
        )

    def test_sqlite3_row_salary_above_floor_passes(self):
        """sqlite3.Row with salary above floor must pass."""
        row = self._row(salary_min=None, description=_DESC_ABOVE_MIN)
        passed, rejected = pre_filter([row], _PROFILE_SALARY)
        assert len(passed) == 1
