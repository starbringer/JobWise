"""
tests/test_extract_description.py
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Integration tests for ranker.extract_description using job descriptions saved
as HTML fixture files under tests/fixtures/.  The fixtures were captured from
the database so tests remain valid even after the DB is cleared.

Run from the project root:
    pytest tests/test_extract_description.py -v
    pytest tests/test_extract_description.py -v -s   # also prints snapshot output
"""

import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).parent.parent
FIXTURES_DIR = Path(__file__).parent / "fixtures"

if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

import pytest
from src.ranker import extract_description


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _load(filename: str) -> str:
    """Read a fixture HTML file."""
    path = FIXTURES_DIR / filename
    if not path.exists():
        pytest.skip(f"Fixture not found: {path}")
    return path.read_text(encoding="utf-8")


def _check(extracted: str, must_have: list[str], must_not_have: list[str] = ()):
    for phrase in must_have:
        assert phrase in extracted, (
            f"Expected phrase missing from extracted description:\n"
            f"  phrase : {phrase!r}\n"
            f"  extract: {extracted[:300]!r}..."
        )
    for phrase in must_not_have:
        assert phrase not in extracted, (
            f"Unwanted phrase found in extracted description:\n"
            f"  phrase : {phrase!r}\n"
            f"  extract: {extracted[:300]!r}..."
        )


# ---------------------------------------------------------------------------
# Assertion tests — one per company
# ---------------------------------------------------------------------------

class TestExtractDescription:
    """
    Each test:
      - Loads a fixture HTML file captured from the database.
      - Runs extract_description(desc, max_chars=3500).
      - Asserts that key requirement / compensation phrases ARE present.
      - Asserts that company-intro / EEO boilerplate is NOT present
        (only applicable when the raw description is long enough to trigger
        filtering — when the sanitised plain text is already ≤ 3 500 chars
        the function returns it unchanged by design).
    """

    def test_airbnb_account_executive(self):
        """
        Airbnb (12-month FTC): plain text > 3 500 chars → company-intro filtered.
        Responsibilities and salary data must be retained.
        """
        extracted = extract_description(_load("airbnb.html"), max_chars=3500)

        _check(
            extracted,
            must_have=[
                "A Typical Day",
                "6+ years of professional experience",
                "English and French",
                "€61.000",                          # French salary range
            ],
            must_not_have=[
                "Airbnb was born in 2007",           # company-intro copy
            ],
        )

    def test_anthropic_account_executive_medical(self):
        """
        Anthropic: 'About Anthropic' header triggers company-intro skip;
        'About the role' section is kept.  Requirements and salary must be present.
        """
        extracted = extract_description(_load("anthropic.html"), max_chars=3500)

        _check(
            extracted,
            must_have=[
                "5+ years of enterprise sales experience",
                "$290,000",                         # salary floor
                "Responsibilities",                 # first kept section header
            ],
            must_not_have=[
                "About Anthropic",                  # company-intro header
                "About the role",                   # role-context header — now skipped
            ],
        )

    def test_dropbox_account_executive_nordics(self):
        """
        Dropbox: plain text fits within budget — returned as-is.
        Requirements and salary data must be present.
        """
        extracted = extract_description(_load("dropbox.html"), max_chars=3500)

        _check(
            extracted,
            must_have=[
                "4+ years of B2B SaaS closing experience",
                "Swedish",                           # language requirement
                "£109,700",                         # UK salary range
            ],
        )

    def test_duolingo_ai_research_engineer(self):
        """
        Duolingo: plain text fits within budget — returned as-is.
        Degree requirement and location must be present.
        """
        extracted = extract_description(_load("duolingo.html"), max_chars=3500)

        _check(
            extracted,
            must_have=[
                "Ph.D. in computer science",
                "Pittsburgh, PA or New York, NY",   # location requirement
            ],
        )

    def test_instacart_account_manager(self):
        """
        Instacart: few recognisable section headers; salary data is preserved
        via the compensation section classifier.
        """
        extracted = extract_description(_load("instacart.html"), max_chars=3500)

        _check(
            extracted,
            must_have=[
                "$90,000",                          # CA/NY salary floor
            ],
        )

    def test_lyft_accounts_receivable_manager(self):
        """
        Lyft: Responsibilities and Experience sections kept in full.
        """
        extracted = extract_description(_load("lyft.html"), max_chars=3500)

        _check(
            extracted,
            must_have=[
                "Bachelor's degree in Accounting or Finance",
                "5-10+",                            # years of experience
            ],
        )

    def test_pinterest_account_manager(self):
        """
        Pinterest: 'What you'll do' and 'What we're looking for' sections
        drive the extraction.  Core qualifications must survive.
        """
        extracted = extract_description(_load("pinterest.html"), max_chars=3500)

        _check(
            extracted,
            must_have=[
                "3+ years in sales or account management",
            ],
        )

    def test_squarespace_account_executive_acquisition(self):
        """
        Squarespace: 'Who We're Looking For' triggers requirements keep;
        salary data must be present.
        """
        extracted = extract_description(_load("squarespace.html"), max_chars=3500)

        _check(
            extracted,
            must_have=[
                "5+ years of B2B SaaS sales experience",
                "$85,000",                          # salary floor
            ],
        )

    def test_stripe_account_executive_ai_sales(self):
        """
        Stripe: plain text ≤ 3 500 chars — returned as-is (no filtering needed).
        Core requirements must be present.
        """
        extracted = extract_description(_load("stripe.html"), max_chars=3500)

        _check(
            extracted,
            must_have=[
                "10+ years of sales experience",
                "Strong interest in technology",
            ],
        )

    def test_amazon_sr_sde_aws_proactive_security(self):
        """
        Amazon: markdown bold headers (**DESCRIPTION**, **BASIC QUALIFICATIONS**,
        **PREFERRED QUALIFICATIONS**) must be recognised after stripping ** markers.
        'Key job responsibilities' header (with 'job' between key and responsibilities)
        must be matched.  Salary in '168,100.00 - 227,400.00 USD' format (no $ prefix)
        must be captured via the content-level compensation override.
        """
        extracted = extract_description(_load("amazon.html"), max_chars=3500)

        _check(
            extracted,
            must_have=[
                "5+ years of non-internship professional software development experience",
                "168,100",   # salary range (Amazon format: digits only, no $ prefix)
                "Key job responsibilities",
            ],
        )


# ---------------------------------------------------------------------------
# Snapshot tests — visual review of what the AI receives
# ---------------------------------------------------------------------------

SNAPSHOT_CASES = [
    ("airbnb.html",      "Airbnb",       "Account Executive (12 Month FTC)"),
    ("anthropic.html",   "Anthropic",    "Account Executive, Academic Medical Centers"),
    ("dropbox.html",     "Dropbox",      "Account Executive Nordics"),
    ("duolingo.html",    "Duolingo",     "AI Research Engineer, New PhD Graduate"),
    ("instacart.html",   "Instacart",    "Account Manager"),
    ("lyft.html",        "Lyft",         "Accounts Receivable Manager, Billing"),
    ("pinterest.html",   "Pinterest",    "Account Manager, tvScientific"),
    ("squarespace.html", "Squarespace",  "Account Executive, Acquisition"),
    ("stripe.html",      "Stripe",       "Account Executive, AI Sales"),
    ("amazon.html",      "Amazon",       "Sr. Software Development Engineer, AWS Proactive Security"),
]

# Extracted text files are written here so you can inspect them directly.
EXTRACTED_DIR = FIXTURES_DIR / "extracted"


@pytest.mark.parametrize("filename,company,title", SNAPSHOT_CASES)
def test_extraction_snapshot(filename: str, company: str, title: str, capsys):
    """
    Visual snapshot: prints extracted description and writes it to
    tests/fixtures/extracted/<company>.txt so you can inspect what the AI
    actually receives.  Always passes — non-empty output is the only assertion.
    Run with -s to also see the output in the terminal.
    """
    extracted = extract_description(_load(filename), max_chars=3500)

    # Write to file — create directory on first run.
    EXTRACTED_DIR.mkdir(exist_ok=True)
    out_name = filename.replace(".html", ".txt")
    out_path = EXTRACTED_DIR / out_name
    header = f"{company} -- {title}\nExtracted: {len(extracted):,} chars\n{'='*60}\n\n"
    out_path.write_text(header + extracted, encoding="utf-8")

    def _safe(text: str) -> str:
        enc = sys.stdout.encoding or "utf-8"
        return text.encode(enc, errors="replace").decode(enc, errors="replace")

    with capsys.disabled():
        print(_safe(f"\n{'='*60}"))
        print(_safe(f"  {company} -- {title}"))
        print(_safe(f"  Extracted: {len(extracted):,} chars  ->  {out_path}"))
        print(_safe(f"{'='*60}"))
        print(_safe(extracted[:800]))
        if len(extracted) > 800:
            print(_safe(f"  ... [{len(extracted) - 800} more chars] ..."))

    assert len(extracted) > 0, f"{company}: extraction returned empty string"
    assert len(extracted) <= 3500, f"{company}: extraction exceeded max_chars"
