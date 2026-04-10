"""
ranker.py — Match-pair extraction and deterministic scoring of jobs against a profile.

AI extracts structured "match_pairs" — facts linking job requirements to candidate
attributes.  Python then applies weighted, auditable scoring rules to produce:
  1. manager_score  (0–200) — how well the candidate satisfies the job's requirements
  2. candidate_score (0–200) — how well the job satisfies the candidate's preferences

match_score (0.0–1.0) = fraction of *required* manager-category pairs that matched.
Used as a threshold gate before promotion (min_match_score in config).

Promotion ranking: combined (manager + candidate) DESC → manager DESC → candidate DESC.

Includes a Python pre-filter for hard requirements before any AI tokens are spent.
Only scores jobs not yet evaluated for this profile.
"""

import html
import json
import logging
import re as _re

from src import ai_client, database, location
from src.salary_parser import parse_salary as _parse_salary

# ---------------------------------------------------------------------------
# Description section extractor
# ---------------------------------------------------------------------------

# ---------------------------------------------------------------------------
# Section-classifier patterns
# Each pattern is matched against the first line of a paragraph (≤120 chars).
# Classification is used to decide which sections to keep, brief, or skip.
# ---------------------------------------------------------------------------

# Sections to SKIP entirely — company marketing copy and boilerplate.
_SEC_COMPANY_INTRO = _re.compile(
    r"^(about\s+(us|the\s+company|our\s+company|who\s+we\s+are|"
    # well-known company names as intro headers
    r"airbnb|stripe|google|amazon|apple|meta|netflix|uber|lyft|twitter|x\.com|"
    r"anthropic|openai|deepmind|microsoft|salesforce|hubspot|shopify|"
    r"discord|figma|coinbase|atlassian|dropbox|pinterest|instacart|doordash|"
    r"squarespace|duolingo|robinhood|plaid|gong|nerdwallet|wayfair)|"
    r"who\s+we\s+are|our\s+story|our\s+mission|company\s+overview|"
    r"introduction|background)\s*[:\-]?\s*$",
    _re.IGNORECASE,
)

# Legal/EEO boilerplate that appears at the end of most job descriptions.
_SEC_BOILERPLATE = _re.compile(
    r"(equal\s+(opportunity|employment)|eeo\b|affirmative\s+action|"
    r"committed\s+to\s+(diversity|equal|inclusion)|diversity.{0,20}inclusion|"
    r"commitment.{0,30}(inclusion|belonging|diversity)|inclusion.{0,20}belonging|"
    r"disability\s+(accommodation|accessible)|accommodation\s+request|"
    r"privacy\s+(notice|policy)|candidate\s+privacy|"
    r"pay\s+transparency|salary\s+transparency|"
    r"#LI-|we\s+will\s+never\s+solicit|never\s+ask\s+you\s+to\s+(transfer|pay)|"
    r"thank\s+you\s+in\s+advance\s+for\s+providing)",
    _re.IGNORECASE,
)

# Sections to SKIP — generic role/team narrative that adds no scoring signal.
# These headers introduce overview/context copy, not requirements or duties.
_SEC_ROLE_CONTEXT = _re.compile(
    r"^(about\s+(the\s+)?(role|position|opportunity|team)|"
    r"the\s+(role|job|position|opportunity)|"
    r"role\s+overview|position\s+overview|job\s+overview|"
    r"overview|the\s+opportunity|this\s+(role|position|job)|"
    r"what\s+you[''`\u2018\u2019]?ll?\s+join|"
    r"about\s+the\s+team|the\s+team|our\s+team)\s*[:\-]?\s*$",
    _re.IGNORECASE,
)

# Sections to keep in FULL — what the job requires.
_SEC_RESPONSIBILITIES = _re.compile(
    r"(^responsibilities|key\s+(job\s+)?responsibilities|primary\s+responsibilities|"
    r"core\s+responsibilities|job\s+duties|job\s+functions?|^description\s*[:\-]?\s*$|"
    r"what\s+you[''`\u2018\u2019]?ll?\s+(do|own|build|lead|drive|work\s+on|accomplish|be\s+doing)|"
    r"what\s+you[''`\u2018\u2019]?re\s+(responsible|expected)|"
    r"^you\s+will\b|you[''`\u2018\u2019]?ll?\s+be\s+responsible|"
    r"^you[''`\u2018\u2019]?ll?\s+get\s+to|"
    r"^in\s+this\s+(dynamic\s+)?role|"
    r"a\s+typical\s+day|day[\s\-]to[\s\-]day|your\s+(role|impact|day)|"
    r"^about\s+the\s+(job|position)\s*[:\-]?\s*$|"
    # Common summary / description headers kept for context
    r"role\s+(description|summary)|position\s+(summary|description)|job\s+summary|"
    r"work\s+you[''`\u2018\u2019]?ll?\s+do|"
    r"the\s+difference\s+you\s+will\s+make|"
    r"you[''`\u2018\u2019]?ll?\s+thrive\s+in\s+this\s+role|"
    r"what\s+you\s+will\s+do)",
    _re.IGNORECASE,
)

# Sections to keep in FULL — candidate requirements.
# Covers the wide variety of header styles used by ATS platforms and employers.
_SEC_REQUIREMENTS = _re.compile(
    r"(^requirements?|^qualifications?|"
    r"minimum\s+(requirements?|qualifications?)|"
    r"basic\s+qualifications?|preferred\s+qualifications?|"
    # Variants missed by the original patterns
    r"required\s+qualifications?|required\s+experience|required\s+skills?|"
    r"preferred\s+requirements?|preferred\s+experience|preferred\s+skills?|"
    r"desired\s+qualifications?|desired\s+skills?|"
    r"key\s+qualifications?|additional\s+qualifications?|core\s+qualifications?|"
    r"position\s+requirements?|job\s+requirements?|minimum\s+experience|"
    # Standalone single-word or short headers  (anchored to full line)
    r"^required\s*[:\-]?\s*$|^preferred\s*[:\-]?\s*$|"
    r"^skills?\s*[:\-]?\s*$|^experience\s*[:\-]?\s*$|"
    r"nice[\s\-]to[\s\-]have|technical\s+skills?|"
    r"must[\s\-]have|what\s+we[''`\u2018\u2019]?re\s+looking\s+for|"
    r"what\s+we\s+are\s+looking\s+for|"
    r"we[''`\u2018\u2019]?re\s+looking\s+for\s+someone|"
    r"who\s+we[''`\u2018\u2019]?re\s+looking\s+for|"
    r"who\s+you\s+are|your\s+expertise|"
    r"your\s+background|your\s+skills?(\s*[&+]\s*expertise)?|"
    r"what\s+you\s+(bring|need|have|offer)\s*[:\-]|"
    r"you\s+(have|bring|possess)\s*[:\-]|"
    r"you\s+might\s+(also\s+)?(be\s+a\s+good\s+fit|have|bring)|"
    r"you\s+may\s+be\s+a\s+good\s+fit|"
    r"strong\s+candidates?\s+(may\s+(also\s+)?)?(have|has)|"
    r"skills?\s+(and|&)\s+experience|experience\s+required|"
    r"candidate\s+requirements?|"
    r"^experience\s*[:\-]|^what\s+you\s+have\s*[:\-]|"
    r"^who\s+you[''`\u2018\u2019]?re\s*[:\-]?$|"
    r"^about\s+you\s*[:\-]?\s*$)",
    _re.IGNORECASE,
)

# Sections to keep in FULL — compensation / benefits (needed for candidate scoring).
_SEC_COMPENSATION = _re.compile(
    r"(^compensation|^benefits?|^perks?|^salary|^equity|"
    r"total\s+rewards?|what\s+we\s+offer|what[''`\u2018\u2019]?s\s+in\s+it|"
    r"our\s+(benefits?|package|offer|compensation)|we\s+offer|"
    r"pay\s+(range|band|scale)|stock\s+(options?|grants?|units?)|"
    r"^base\s+(salary|pay)|^total\s+(compensation|comp|pay)|"
    r"annual\s+salary|^the\s+annual\s+compensation\s+range|"
    r"what\s+you[''`\u2018\u2019]?ll?\s+(get|receive|earn)\s*[:\-]|"
    r"compensation\s+(range|package|details?|info)|"
    r"salary\s+(range|band|info|details?)|"
    r"^pay\s*[:\-]?\s*$|^rewards?\s*[:\-]?\s*$)",
    _re.IGNORECASE,
)

# Section types (string constants used as state in the classifier)
_ST_COMPANY  = "company"       # skip
_ST_BOILER   = "boilerplate"   # skip
_ST_ROLE     = "role_context"  # skip (overview/team narrative, no scoring signal)
_ST_WORK     = "work"          # keep full
_ST_REQS     = "reqs"          # keep full
_ST_COMP     = "comp"          # keep full
_ST_UNKNOWN  = None            # not yet classified


def _classify_first_line(line: str) -> str | None:
    """
    Return a section type string if the line matches a known section header,
    or None if it doesn't match anything recognisable.

    Boilerplate is checked before the 120-char length guard because EEO/legal
    disclaimers are often written as a single long run-on paragraph with no
    preceding section header — they must still be caught and skipped.
    """
    line = line.strip()
    if not line:
        return None
    # Strip markdown bold markers (**text**) before pattern matching so that
    # headers like "**DESCRIPTION**" and "**BASIC QUALIFICATIONS**" are recognised.
    line = _re.sub(r"\*\*", "", line).strip()
    if not line:
        return None
    # Boilerplate check applies to lines of any length.
    if _SEC_BOILERPLATE.search(line):
        return _ST_BOILER
    # All other classifiers only match short section-header lines.
    if len(line) > 120:
        return None
    if _SEC_COMPANY_INTRO.search(line):
        return _ST_COMPANY
    if _SEC_ROLE_CONTEXT.search(line):
        return _ST_ROLE
    if _SEC_RESPONSIBILITIES.search(line):
        return _ST_WORK
    if _SEC_REQUIREMENTS.search(line):
        return _ST_REQS
    if _SEC_COMPENSATION.search(line):
        return _ST_COMP
    return None


def _sanitize_html(text: str) -> str:
    """Decode HTML entities and strip HTML tags, then normalise whitespace."""
    # Decode entities repeatedly until stable — handles double-encoded text
    # e.g. &amp;mdash; → &mdash; → —
    prev = None
    while prev != text:
        prev = text
        text = html.unescape(text)
    # Remove <script> and <style> blocks entirely (content, not just tags)
    text = _re.sub(r"<script[^>]*>.*?</script>", " ", text, flags=_re.DOTALL | _re.IGNORECASE)
    text = _re.sub(r"<style[^>]*>.*?</style>", " ", text, flags=_re.DOTALL | _re.IGNORECASE)
    # Block-level closing tags and <br> → newline so paragraph structure is preserved
    text = _re.sub(r"</(p|div|li|h[1-6]|tr|section|article|header|footer)>",
                   "\n", text, flags=_re.IGNORECASE)
    text = _re.sub(r"<br\s*/?>", "\n", text, flags=_re.IGNORECASE)
    # Strip all remaining HTML/XML tags
    text = _re.sub(r"<[^>]+>", " ", text)
    # Unescape markdown backslash sequences (e.g. \+ \- \* \. used by Greenhouse/Amazon)
    text = _re.sub(r"\\([+\-*.()\[\]{}#!|`_~>])", r"\1", text)
    # Collapse runs of whitespace (but preserve newlines for section structure)
    text = _re.sub(r"[ \t\xa0]+", " ", text)   # \xa0 = &nbsp;
    text = _re.sub(r" *\n *", "\n", text)
    text = _re.sub(r"\n{3,}", "\n\n", text)
    return text.strip()


def _preprocess_headers(desc: str) -> str:
    """
    Ensure bold section-header lines are their own paragraphs (surrounded by double
    newlines) so the classifier always sees them as a first_line.

    Two passes are needed:
      1. ALL-CAPS bold headers (e.g. **BASIC QUALIFICATIONS**) — already handled
         but kept for explicitness.
      2. Title-Case bold headers (e.g. **Required Qualifications**) that ATS boards
         like Indeed often attach to the last bullet of the preceding section with
         only a single newline.  Pattern: any line whose entire content is **…**
         (up to 80 chars between the markers) that is glued to the line above it.
    """
    # Pass 1: ALL-CAPS bold headers at end of a paragraph (original logic)
    desc = _re.sub(r"(?<!\n\n)(\n\*\*[A-Z][A-Z\s]+\*\*)", r"\n\n\1", desc)
    # Pass 2: Any bold-only line (Title Case or mixed) glued to the line above.
    # Matches: non-newline char, newline, **Header text (≤80 chars)**, newline
    # Inserts an extra newline before the header to split it into its own paragraph.
    desc = _re.sub(
        r"([^\n])\n(\*\*[A-Z][^*\n]{0,80}\*\*[ \t]*)(?=\n)",
        r"\1\n\n\2",
        desc,
    )
    return desc


def _run_section_filter(desc: str, max_chars: int) -> tuple[str, set[str]]:
    """
    Core section-classifier state machine.  Always runs — no early-return bypass.

    Returns (extracted_text, found_section_types) where found_section_types is the
    set of _ST_* constants (work / reqs / comp) that were actually present in the
    description.  An empty set means no meaningful job-content sections were found
    (description is company overview boilerplate or otherwise unstructured).

    Caller uses found_section_types to decide whether the job is scoreable.

    Compensation sections are collected separately and appended with a guaranteed
    budget so they are never silently truncated by long qualification lists.
    """
    _SKIP_TYPES = {_ST_COMPANY, _ST_BOILER, _ST_ROLE}

    paragraphs = _re.split(r"\n{2,}|\n(?=#+\s)", desc)

    comp_parts: list[str] = []   # compensation / benefits paragraphs — kept separate
    other_parts: list[str] = []  # work / reqs / unknown paragraphs
    current_type: str | None = _ST_UNKNOWN
    found_sections: set[str] = set()

    for para in paragraphs:
        para = para.strip()
        if not para:
            continue

        # Classify by first line
        first_line = para.split("\n")[0]
        classified = _classify_first_line(first_line)
        if classified is not None:
            current_type = classified

        # Content-level compensation override: a paragraph containing a salary
        # figure (e.g. "$60,048 — $123,629 USD") is always compensation even
        # when it follows a boilerplate marker like #LI- that set the sticky
        # skip state.  Salary data must never be discarded.
        if current_type in _SKIP_TYPES and _re.search(
            r"([\$£€]\s*\d{2,3}(?:,\d{3})+|\d{3},\d{3}[.\d]*\s*(?:USD|GBP|EUR|CAD|AUD)|base\s+salary\s+range)", para
        ):
            current_type = _ST_COMP

        # Bucket paragraphs: comp goes to its own list so it can't be crowded out.
        if current_type in _SKIP_TYPES:
            continue
        elif current_type == _ST_COMP:
            comp_parts.append(para)
            found_sections.add(_ST_COMP)
        elif current_type in {_ST_WORK, _ST_REQS}:
            other_parts.append(para)
            found_sections.add(current_type)
        # _ST_UNKNOWN: include only after we've already started — avoids
        # emitting stray paragraphs before the first recognisable section.
        elif current_type is _ST_UNKNOWN and (other_parts or comp_parts):
            other_parts.append(para)

    # Assemble: give compensation a guaranteed tail-budget so it is never
    # truncated even when qualification lists are verbose.
    _COMP_BUDGET = 700  # chars reserved for comp/benefits — enough for a salary paragraph
    if comp_parts:
        other_text = _re.sub(r"\n{2,}", "\n", "\n".join(other_parts))
        comp_text = _re.sub(r"\n{2,}", "\n", "\n".join(comp_parts))[:_COMP_BUDGET]
        result = (other_text[: max_chars - _COMP_BUDGET] + "\n" + comp_text).strip()
    else:
        result = _re.sub(r"\n{2,}", "\n", "\n".join(other_parts)).strip()[:max_chars]

    if not result:
        # Fallback: no sections recognised — use head + tail to capture
        # both role info (head) and compensation (tail).
        tail_budget = min(800, max_chars // 4)
        head = _re.sub(r"\n{2,}", "\n", desc[: max_chars - tail_budget])
        tail = _re.sub(r"\n{2,}", "\n", desc[-tail_budget:])
        fallback = (head + "\n...\n" + tail) if tail.strip() and tail.strip() not in head else head
        return fallback, set()

    return result, found_sections


def sanitize_description(desc: str) -> str:
    """
    Return the full job description as plain text — HTML stripped, entities decoded,
    whitespace normalised — without any section filtering or truncation.

    Intended for the "View Full Description" toggle in the UI where the user wants
    to see everything the source page provided, not just the scored sections.
    """
    if not desc:
        return ""
    return _sanitize_html(desc)


def extract_description(desc: str, max_chars: int = 3500) -> str:
    """
    Extract signal-rich sections from a job description as plain text.

    Uses a section-classifier state machine:
      1. Sanitize HTML → plain text paragraphs.
      2. Ensure bold section-header lines are their own paragraphs.
      3. Classify each paragraph by scanning its first line for known section
         headers (company intro, boilerplate, role context, responsibilities,
         requirements, compensation).
      4. Unclassified paragraphs inherit the current active section type.
      5. Assembly rules:
           company_intro / boilerplate / role_context → skipped entirely
           work / reqs / comp                         → kept in full
      6. Apply max_chars as a safety cap.
      7. Fallback to head+tail when no sections are recognised (e.g. non-English,
         pure company overview boilerplate).

    Section filtering always runs regardless of description length — this ensures
    company intro and EEO boilerplate are stripped for both AI scoring (max_chars=3500)
    and web display (max_chars=10000).
    """
    if not desc:
        return ""
    desc = _sanitize_html(desc)
    desc = _preprocess_headers(desc)
    text, _ = _run_section_filter(desc, max_chars)
    return text


_LOGIN_WALL_PATTERNS = _re.compile(
    r"(sign\s+in\s+to\s+(view|see|access|apply)|"
    r"join\s+now\s+to\s+(view|see|apply)|"
    r"authwall|checkpoint/challenge|uas/authenticate|"
    r"log\s*in\s+to\s+(view|see|access|apply)|"
    r"you\s+must\s+(sign\s+in|log\s+in)\s+to|"
    r"please\s+(sign\s+in|log\s+in)\s+to\s+(view|see|access)|"
    r"create\s+(a\s+)?free\s+account\s+to\s+(see|view|access))",
    _re.IGNORECASE,
)


def is_description_scoreable(desc: str) -> bool:
    """
    Return True if the job description contains actual work responsibilities or
    requirements/qualifications — i.e. it is substantive enough to score fairly.

    Returns False for descriptions that are only company overview boilerplate,
    pure compensation/benefits text, login/auth walls, or otherwise lack
    job-content sections. Such jobs are flagged as 'missing_info' and skipped
    by the AI scorer.
    """
    if not desc:
        return False
    # Reject login/auth walls before the section check — these pages may contain
    # keywords like "qualifications" in job-preview snippets but provide no
    # scoreable content (e.g. LinkedIn sign-in page shown instead of the job).
    if _LOGIN_WALL_PATTERNS.search(desc):
        return False
    sanitized = _sanitize_html(desc)
    sanitized = _preprocess_headers(sanitized)
    # Use a very large cap so truncation never hides a section that appears late.
    _, found_sections = _run_section_filter(sanitized, max_chars=999_999)
    return bool(found_sections & {_ST_WORK, _ST_REQS})

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Ranker prompt — split into static system template + dynamic user template.
#
# _RANKER_SYSTEM_TMPL:  scoring rules + candidate profile.
#   This is identical across all batch calls within a single run, so it is
#   sent as a cached system message when using the anthropic provider
#   (Anthropic ephemeral cache, 5-min TTL → ~10% of normal input price on hits).
#   For claude_cli it is prepended to the prompt string (no native caching).
#
# _RANKER_USER_TMPL:  jobs to evaluate — changes every batch.
# ---------------------------------------------------------------------------

_RANKER_SYSTEM_TMPL = """You are extracting structured match facts between job postings and a candidate profile.
Your output drives automated scoring — accuracy and conciseness matter more than completeness.

Output ONLY valid JSON — no explanation, no markdown, no code fences.

Output format (array, one entry per job):
[
  {{
    "job_key": "<job_key>",
    "total_job_requirements": <integer — count of ALL requirements/qualifications in the job posting, hard AND soft, regardless of whether the candidate matches>,
    "match_pairs": [
      {{
        "category": "<see categories below>",
        "job_side": "<1-5 word label from job text>",
        "candidate_side": "<1-5 word label from profile>",
        "job_importance": "<required|preferred|nice_to_have>",
        "candidate_priority": "<must_have|nice_to_have>"
      }},
      ...
    ],
    "manager_notes": "<max 2 bullets, • prefix, \\n separated, max 15 words each>",
    "candidate_notes": "<max 2 bullets, • prefix, \\n separated, max 15 words each>"
  }},
  ...
]

Omit any key whose value would be null — do not include it in the object.

total_job_requirements: Count every distinct requirement, qualification, or expectation the job posting lists — required AND preferred AND nice-to-have. Include skills, experience, domain knowledge, certifications, anything. Count each item once even if restated. Do not count candidate-side preferences (compensation, benefits, etc.). This count is used to measure how completely the match_pairs cover the job's requirements.

=== CATEGORIES ===

Manager-perspective (does the candidate satisfy the job?):
  skill        — technical skills, tools, languages, frameworks, methodologies
  experience   — years of experience, seniority level, role type history
  domain       — industry or domain expertise the job targets

Candidate-perspective (does the job satisfy the candidate?):
  compensation   — salary / total comp vs candidate's stated target
  equity         — RSU, stock options, ESPP, any ownership compensation
  benefits       — health insurance, 401k, PTO, parental leave, learning budget, etc.
  work_arrangement — remote/hybrid/onsite, location, timezone requirements
  culture_growth — company culture, values, career growth, mentorship, advancement

=== PAIR CREATION RULES ===

MANAGER categories (skill, experience, domain):
  • Job requires X AND candidate has X → include BOTH sides (it is a match)
  • Job requires X AND candidate lacks evidence of X → include job_side only (gap)
  • Candidate has X not required by job → include candidate_side only (extra skill — use sparingly)
  • job_importance: infer from job language
      "required" / "must have" / "you need" / "you will need" → required
      "preferred" / "a plus" / "ideally" / "nice if" → preferred
      "bonus" / "nice to have" / "not required" → nice_to_have
      unclear → omit the field
  • Treat overqualification as a match (populate both sides)

CANDIDATE categories (compensation, equity, benefits, work_arrangement, culture_growth):
  • Job offers X AND it SATISFIES / is COMPATIBLE WITH what candidate wants → BOTH sides (match)
  • Job states something that CONTRADICTS candidate's preference (must_have OR nice_to_have) → candidate_side only
      (e.g. job: "onsite SF", candidate: "remote only" (must_have) → candidate_side only — NOT both sides)
      (e.g. job: "Seattle, WA onsite", candidate: "VA/MD/DC area only" (must_have) → candidate_side only — job location does not satisfy geographic preference)
      (e.g. job: "$90k", candidate: "min $150k" (must_have) → candidate_side only)
      (e.g. job: "salary not disclosed", candidate: "min $200k" (must_have) → candidate_side only, job_side OMITTED — undisclosed salary does not satisfy a stated minimum; NEVER populate job_side with phrases like "salary not disclosed" or "not stated")
      (e.g. job: "fast-paced startup environment", candidate: "work-life balance" (nice_to_have) → candidate_side only)
  • Candidate has a must_have that the job is SILENT on → candidate_side only
  • Candidate has a nice_to_have that the job is SILENT on (no relevant mention at all) → DO NOT create a pair
  • Candidate has a nice_to_have that the job ACTIVELY CONTRADICTS → candidate_side only (negative signal, not silence)
  • KEY RULE: "both sides" means the job actually satisfies the candidate's need —
      not merely that both parties mentioned something in the same category.
      If the job's stated value conflicts with the candidate's stated need, it is a gap.
  • candidate_priority: infer from profile sections
      profile "must have" / "required" / "will not accept" → must_have
      profile "prefer" / "nice to have" / "ideally" / "valued" → nice_to_have
      unstated priority → omit the field

=== LABEL GUIDELINES ===
  • Keep labels short: 1–5 words (e.g. "golang", "5+ yrs backend", "fully remote", "401k 5% match")
  • Labels are for human review only — the scoring engine never reads them
  • Match the wording to what the source actually says — avoid paraphrasing heavily

=== EXTERNAL KNOWLEDGE RULES ===

Use ONLY facts explicitly stated in the job posting and candidate profile, EXCEPT:

  domain pairs only: you may infer the company's primary industry/sector when not stated
    (e.g. Palantir → defense/intelligence, Stripe → fintech, Cloudflare → cybersecurity/infra)

  culture_growth pairs only: you may use well-known public facts:
    • Fortune 500 / Fortune 1000 membership
    • FAANG / Big Tech classification
    • Company size tier when not stated (startup / mid-size / large / enterprise)
    • Company stage when not stated (pre-IPO / recently public / established)
    • Widely documented culture traits (e.g. known high-performance culture, known long hours)

  ALL other categories: strictly source text only — if the information is not in the job
  posting or profile, do not create the pair. When uncertain, omit.
  Only apply external knowledge when you are highly confident.

=== NOTES ===
  manager_notes: max 2 bullets — key strengths, critical gaps if any, hiring call
  candidate_notes: max 2 bullets — comp/equity signals, lifestyle fit, any reservations
  Max 15 words per bullet. Start each with •. Separate with \\n.

Candidate profile:
{profile_json}"""

_RANKER_USER_TMPL = "Jobs to evaluate:\n{jobs_json}"


def _title_filter(jobs: list, profile: dict) -> tuple[list, list]:
    """
    Keep only jobs whose title contains at least one keyword derived from
    the profile's target job titles or hard_requirements.job_titles.

    Keywords are extracted by splitting title strings into individual words
    (≥4 chars) and common multi-word phrases. This prevents scoring thousands
    of irrelevant listings fetched from ATS boards (e.g. Stripe has 520 open roles).

    Jobs with no title are always passed through (can't filter what we can't see).
    """
    hard_titles = (profile.get("hard_requirements") or {}).get("job_titles") or []
    soft_titles = profile.get("target_job_titles") or []
    all_titles = hard_titles + soft_titles

    if not all_titles:
        return jobs, []  # no title info — can't filter, pass everything

    # Seniority/filler words stripped from both sides so "Lead Engineer" matches
    # "Engineering Lead" and "Senior Data Scientist" matches "Data Scientist".
    stopwords = {"senior", "junior", "lead", "staff", "principal", "associate",
                 "head", "director", "vice", "president", "level", "with", "from",
                 "this", "that", "will", "have", "been", "some", "more", "than"}

    def _words(text: str) -> set[str]:
        """Split text into significant words (≥4 chars, not stopwords)."""
        return {
            w.strip("().,") for w in _re.split(r"[\s/,\-]+", text.lower())
            if len(w.strip("().,")) >= 4 and w.strip("().,") not in stopwords
        }

    # Build keyword set from profile titles (all meaningful words)
    keywords: set[str] = set()
    full_phrases: list[str] = []
    for t in all_titles:
        keywords |= _words(t)
        full_phrases.append(t.lower())  # also keep full-phrase check

    passed, rejected = [], []
    for job in jobs:
        title = (job["title"] or "").lower() if job["title"] else ""
        if not title:
            passed.append(job)
            continue

        # Check 1: any full profile-title phrase is a substring of the job title
        if any(phrase in title for phrase in full_phrases):
            passed.append(job)
            continue

        # Check 2: word-set intersection (order-independent)
        # "Lead Engineer" matches "Engineering Lead", "Head of Data" matches "Data Lead"
        if _words(title) & keywords:
            passed.append(job)
        else:
            rejected.append((job, f"Title '{job['title']}' contains no target-role keywords"))

    return passed, rejected


# Patterns that unambiguously mean clearance is required — no negation check needed.
_CLEARANCE_DEFINITIVE = [
    "ts/sci", "ts-sci",
    "active clearance", "clearance required", "clearance eligible",
    "dod clearance", "doe clearance", "dhs clearance",
    "must have clearance", "must hold clearance", "requires clearance",
    "obtain a clearance", "obtain clearance",
    "polygraph",
    "must be a us citizen", "must be a u.s. citizen",
]

# Patterns that can appear in a "not required" context — check negation window.
_CLEARANCE_AMBIGUOUS = [
    "security clearance", "secret clearance", "top secret",
    "public trust", "able to obtain",
    "u.s. citizenship required", "us citizenship required",
]

# Phrases within ~80 chars of a match that mean clearance is NOT required.
_CLEARANCE_NEGATIONS = [
    "not required", "not needed", "no clearance", "clearance not required",
    "is a plus", "a plus", "preferred but not required",
]


def _job_requires_clearance(job: dict) -> bool:
    """Return True if the job posting contains federal clearance language.

    Scans the full description (not just the first N chars) and handles
    negation context (e.g. "clearance not required, but a plus") so jobs
    that merely mention clearance as optional are not incorrectly filtered.
    """
    text = " ".join([
        (job["title"] or ""),
        (job["description"] or ""),
    ]).lower()

    # Definitive patterns — always means clearance is required.
    if any(pat in text for pat in _CLEARANCE_DEFINITIVE):
        return True

    # Ambiguous patterns — only flag if no negation phrase is nearby.
    for pat in _CLEARANCE_AMBIGUOUS:
        idx = text.find(pat)
        while idx >= 0:
            window = text[max(0, idx - 80): idx + 80]
            if not any(neg in window for neg in _CLEARANCE_NEGATIONS):
                return True
            idx = text.find(pat, idx + 1)

    return False


def pre_filter(jobs: list, profile: dict) -> tuple[list, list]:
    """
    Apply hard requirements as binary pass/fail filters before sending to AI.
    Returns (passed, rejected) where rejected items are (job, reason) tuples.

    Hard filters applied:
      1. company_exclude      — HIGHEST PRIORITY
      2. clearance            — if has_clearance is False, reject jobs requiring federal clearance
      3. remote_type          — only if set and job remote_type is known and mismatches
      4. salary               — only if job's salary_min is known and below our floor
      5. exclude_industries   — keyword scan on company + description snippet
      6. exclude_company_types — keyword scan on company + title + description snippet
      7. exclude_titles       — substring match on job title
      8. country              — skip fully-remote; benefit of the doubt if location is ambiguous
      9. locations            — if set, job must be clearly remote OR in an allowed area;
                                empty/unset = no constraint (willing to relocate)
    """
    hard = profile.get("hard_requirements") or {}

    company_exclude = {c.lower().strip() for c in (hard.get("company_exclude") or [])}
    has_clearance = hard.get("has_clearance")  # True, False, or None
    hard_remote = (hard.get("remote_type") or "").lower()
    hard_salary_min = hard.get("salary_min")
    exclude_industries = [i.lower() for i in (hard.get("exclude_industries") or [])]
    exclude_company_types = [t.lower() for t in (hard.get("exclude_company_types") or [])]
    exclude_titles = [t.lower().strip() for t in (hard.get("exclude_titles") or [])]
    hard_countries = [c.strip().lower() for c in (hard.get("country") or []) if c.strip()]
    hard_locations = [l.strip() for l in (hard.get("locations") or []) if l.strip()]

    passed = []
    rejected = []

    for job in jobs:
        company_lower = (job["company"] or "").lower()
        desc_snippet = (job["description"] or "")[:300].lower()
        title_lower = (job["title"] or "").lower()

        # 1. Company exclude (highest priority)
        if company_exclude:
            if any(excl in company_lower or company_lower in excl for excl in company_exclude):
                rejected.append((job, f"Company '{job['company']}' is on the exclude list"))
                continue

        # 2. Clearance — reject jobs that require federal clearance when candidate has none
        if has_clearance is False and _job_requires_clearance(job):
            rejected.append((job, "Job requires federal security clearance but candidate has none"))
            continue

        # 3. Remote type
        if hard_remote and hard_remote != "flexible":
            job_remote = (job["remote_type"] or "unknown").lower()
            if job_remote != "unknown" and job_remote != hard_remote:
                rejected.append((job, f"Remote type '{job_remote}' does not match required '{hard_remote}'"))
                continue

        # 4. Salary floor — reject if the stated minimum salary is below our floor.
        # salary_max is irrelevant (more money is never a reason to reject).
        # When the DB field is NULL, fall back to parsing from description text;
        # only give benefit of the doubt when salary truly cannot be determined.
        if hard_salary_min:
            job_salary_min = job["salary_min"]
            if job_salary_min is None:
                desc = job["description"] or ""
                if desc:
                    preferred_locations = profile.get("preferred_locations")
                    lo, _hi = _parse_salary(desc, preferred_locations)
                    job_salary_min = lo  # remains None if parser finds nothing
            if job_salary_min and job_salary_min < hard_salary_min:
                rejected.append((
                    job,
                    f"Salary ${job_salary_min:,} is below required minimum ${hard_salary_min:,}"
                ))
                continue

        # 5. Industry exclusion
        if exclude_industries:
            scan_text = f"{company_lower} {desc_snippet}"
            matched = next((ind for ind in exclude_industries if ind in scan_text), None)
            if matched:
                rejected.append((job, f"Industry exclusion matched: '{matched}'"))
                continue

        # 6. Company type exclusion
        if exclude_company_types:
            scan_text = f"{company_lower} {title_lower} {desc_snippet}"
            matched = next((ct for ct in exclude_company_types if ct in scan_text), None)
            if matched:
                rejected.append((job, f"Company type exclusion matched: '{matched}'"))
                continue

        # 7. Title exclusion — whole-phrase word-boundary match on job title
        # "engineering manager" matches "senior engineering manager" but NOT "project manager"
        if exclude_titles:
            matched = next(
                (et for et in exclude_titles if _re.search(r'\b' + _re.escape(et) + r'\b', title_lower)),
                None,
            )
            if matched:
                rejected.append((job, f"Title exclusion matched: '{matched}'"))
                continue

        # 8. Country — remote jobs are NOT exempt: "Remote, Mexico" still restricts the
        # candidate to Mexico. Give benefit of the doubt only for location-agnostic strings
        # like "Remote", "Anywhere", "Worldwide", or an empty location field.
        if hard_countries:
            loc = (job["location"] or "").lower()
            if loc and loc not in ("remote", "anywhere", "worldwide", "global"):
                # Pass if ANY of the required countries matches; reject only if ALL conflict
                if all(_country_conflict(hc, loc) for hc in hard_countries):
                    rejected.append((job, f"Location '{job['location']}' is outside required country"))
                    continue

        # 9. Location constraint
        # If locations is set and non-empty, the user only accepts jobs that are either:
        #   (a) clearly remote — no physical presence required regardless of location, OR
        #   (b) in one of the allowed areas — any work style (on-site, hybrid) is fine locally
        # If locations is empty/unset, no constraint — user accepts any location (willing to relocate).
        # Jobs with no location string get benefit of the doubt and pass.
        if hard_locations:
            job_remote = (job["remote_type"] or "unknown").lower()
            if job_remote != "remote":
                loc = (job["location"] or "").lower()
                if loc and loc not in ("remote", "anywhere"):
                    if not location.matches_any(job["location"], hard_locations):
                        rejected.append((
                            job,
                            f"Job in '{job['location']}' is outside allowed locations and does not offer remote"
                        ))
                        continue

        passed.append(job)

    return passed, rejected


# ---------------------------------------------------------------------------
# Country own-indicators: if ANY appear in the location string, the job is
# confirmed to be in that country (no conflict).  Matching is substring-based
# on an already-lowercased location string.
# Rules:
#   - Leading spaces guard short tokens against substring collisions
#     (e.g. " va" won't match "savannah").
#   - "new mexico" is listed here so it is resolved as a US state BEFORE the
#     comprehensive foreign-name registry can match the bare word "mexico".
# ---------------------------------------------------------------------------
_COUNTRY_ALIASES: dict[str, list[str]] = {
    "us": [
        "united states", " usa,", " usa ", ",usa", "u.s.a", "u.s.,",
        " california", " new york", " texas", " washington,", " florida",
        " illinois", " massachusetts", " colorado", " georgia", " virginia",
        " arizona", " seattle", " chicago", " boston", " austin", " denver",
        " san francisco", " los angeles", " new york city", " silicon valley",
        "new mexico",
    ],
    "usa": [
        "united states", " usa,", " usa ", ",usa", "u.s.a", "u.s.,",
        " california", " new york", " texas", " washington,", " florida",
        "new mexico",
    ],
    "united states": [
        "united states", " usa,", " usa ", "u.s.a",
        " california", " new york", " texas", " washington,", " florida",
        "new mexico",
    ],
    "canada": [
        "canada", " ontario", " british columbia", " quebec", " alberta",
        " toronto", " vancouver", " montreal", " calgary",
    ],
    "uk": [
        "united kingdom", " england", " scotland", " wales", " london",
        " manchester", " birmingham", " edinburgh", " bristol",
    ],
    "united kingdom": [
        "united kingdom", " england", " scotland", " wales", " london",
        " manchester", " birmingham",
    ],
    "australia": [
        "australia", " sydney", " melbourne", " brisbane", " perth",
        " adelaide", " canberra", " new south wales", " victoria,",
    ],
    "germany": [
        "germany", "deutschland", " berlin", " munich", " hamburg",
        " frankfurt", " cologne", " stuttgart",
    ],
    "france": [
        "france", " paris", " lyon", " marseille", " toulouse", " bordeaux",
    ],
    "india": [
        "india", " bangalore", " bengaluru", " mumbai", " delhi",
        " hyderabad", " pune", " chennai", " kolkata",
    ],
    "singapore": ["singapore"],
    "netherlands": [
        "netherlands", " amsterdam", " rotterdam", " the hague", " utrecht",
    ],
}

# ---------------------------------------------------------------------------
# Comprehensive country name registry — used by _country_conflict for
# foreign-country detection.  For each country, lists the lowercase name
# strings that identify it.  _country_conflict does whole-word matching
# against these strings, so "mexico" won't match "new mexico" only if the
# own-indicator check runs first (which it always does).
#
# Deliberately excluded:
#   "georgia"   — collides with the US state of the same name
#   city names  — excluded to prevent false positives on US towns that share
#                 a name with a foreign city (e.g. Rome, GA; Naples, FL).
#                 Exceptions: a handful of unambiguous major business hubs
#                 added where the city name alone is commonly used in listings
#                 (dubai, bangalore/bengaluru, toronto).
# ---------------------------------------------------------------------------
_COUNTRY_NAME_REGISTRY: dict[str, frozenset[str]] = {
    # North America
    "canada":          frozenset(["canada", "toronto", "vancouver", "montreal"]),
    "mexico":          frozenset(["mexico"]),
    # Europe
    "united kingdom":  frozenset(["united kingdom", "england", "scotland", "wales",
                                   "london", "manchester", "birmingham", "edinburgh"]),
    "germany":         frozenset(["germany", "deutschland"]),
    "france":          frozenset(["france"]),
    "spain":           frozenset(["spain"]),
    "italy":           frozenset(["italy"]),
    "portugal":        frozenset(["portugal"]),
    "netherlands":     frozenset(["netherlands", "holland", "amsterdam"]),
    "poland":          frozenset(["poland"]),
    "ukraine":         frozenset(["ukraine"]),
    "romania":         frozenset(["romania"]),
    "hungary":         frozenset(["hungary"]),
    "czech republic":  frozenset(["czech republic", "czechia"]),
    "sweden":          frozenset(["sweden"]),
    "norway":          frozenset(["norway"]),
    "denmark":         frozenset(["denmark"]),
    "finland":         frozenset(["finland"]),
    "switzerland":     frozenset(["switzerland"]),
    "austria":         frozenset(["austria"]),
    "belgium":         frozenset(["belgium"]),
    "ireland":         frozenset(["ireland"]),
    "croatia":         frozenset(["croatia"]),
    "serbia":          frozenset(["serbia"]),
    "slovakia":        frozenset(["slovakia"]),
    "bulgaria":        frozenset(["bulgaria"]),
    "greece":          frozenset(["greece"]),
    "latvia":          frozenset(["latvia"]),
    "lithuania":       frozenset(["lithuania"]),
    "estonia":         frozenset(["estonia"]),
    # Asia-Pacific
    "india":           frozenset(["india", "bangalore", "bengaluru",
                                   "mumbai", "delhi", "hyderabad"]),
    "china":           frozenset(["china"]),
    "japan":           frozenset(["japan"]),
    "south korea":     frozenset(["south korea", "korea"]),
    "north korea":     frozenset(["north korea"]),
    "singapore":       frozenset(["singapore"]),
    "taiwan":          frozenset(["taiwan"]),
    "vietnam":         frozenset(["vietnam"]),
    "thailand":        frozenset(["thailand"]),
    "philippines":     frozenset(["philippines"]),
    "indonesia":       frozenset(["indonesia"]),
    "malaysia":        frozenset(["malaysia"]),
    "pakistan":        frozenset(["pakistan"]),
    "bangladesh":      frozenset(["bangladesh"]),
    "australia":       frozenset(["australia", "sydney", "melbourne",
                                   "brisbane", "perth"]),
    "new zealand":     frozenset(["new zealand"]),
    # Middle East
    "israel":          frozenset(["israel", "tel aviv"]),
    "turkey":          frozenset(["turkey", "turkiye"]),
    "uae":             frozenset(["uae", "united arab emirates", "dubai"]),
    "saudi arabia":    frozenset(["saudi arabia"]),
    "iran":            frozenset(["iran"]),
    # Africa
    "nigeria":         frozenset(["nigeria"]),
    "south africa":    frozenset(["south africa"]),
    "kenya":           frozenset(["kenya"]),
    "egypt":           frozenset(["egypt"]),
    "ghana":           frozenset(["ghana"]),
    "ethiopia":        frozenset(["ethiopia"]),
    "morocco":         frozenset(["morocco"]),
    # Latin America
    "brazil":          frozenset(["brazil"]),
    "argentina":       frozenset(["argentina"]),
    "colombia":        frozenset(["colombia"]),
    "chile":           frozenset(["chile"]),
    "peru":            frozenset(["peru"]),
    "venezuela":       frozenset(["venezuela"]),
    "ecuador":         frozenset(["ecuador"]),
    "costa rica":      frozenset(["costa rica"]),
    "panama":          frozenset(["panama"]),
    "guatemala":       frozenset(["guatemala"]),
    # US and aliases — needed so cross-country checks (e.g. hard_country=canada)
    # can detect US-restricted jobs as conflicts.  "us" is added as a whole-word
    # match (not substring) via _word_in_location, so it won't false-fire on
    # common English "us" in typical location strings like "Remote - US: Select".
    "us":              frozenset(["united states", "usa", "us"]),
    "usa":             frozenset(["united states", "usa", "us"]),
    "united states":   frozenset(["united states", "usa", "us"]),
}

# Groups of registry keys that refer to the same real-world country.
# When checking for conflicts, all keys in the same family as hard_country
# are skipped (they represent "our country", not a foreign one).
_COUNTRY_FAMILIES: list[frozenset[str]] = [
    frozenset({"us", "usa", "united states"}),
    frozenset({"uk", "united kingdom"}),
    frozenset({"south korea", "north korea", "korea"}),  # separate countries but grouped by peninsula name
    frozenset({"uae", "united arab emirates"}),
]
# Override: south/north korea are actually different countries, keep them separate.
_COUNTRY_FAMILIES = [
    frozenset({"us", "usa", "united states"}),
    frozenset({"uk", "united kingdom"}),
    frozenset({"uae", "united arab emirates"}),
]


def _get_country_family(key: str) -> frozenset[str]:
    """Return the set of registry keys that refer to the same country as *key*."""
    for family in _COUNTRY_FAMILIES:
        if key in family:
            return family
    return frozenset({key})


def _word_in_location(word: str, location: str) -> bool:
    """True if *word* appears as a complete token in *location*.

    Uses negative letter lookbehind/lookahead to avoid matching *word* when it
    is embedded inside a longer word (e.g. "india" must not match "indiana").
    """
    return bool(_re.search(r"(?<![a-z])" + _re.escape(word) + r"(?![a-z])", location))


def _country_conflict(hard_country: str, location: str) -> bool:
    """Return True if *location* clearly belongs to a country other than *hard_country*.

    Two-step logic:
      1. Own-indicators (substring match) — fast path: if the location contains
         any indicator from _COUNTRY_ALIASES for hard_country, it is confirmed
         to be our country and we return False immediately.
      2. Comprehensive registry scan — whole-word match against every country in
         _COUNTRY_NAME_REGISTRY that is not in the same family as hard_country.
         If any foreign country name is found, return True (conflict).
      3. Benefit of the doubt — no clear signal → return False (pass).
    """
    own_key = hard_country.lower()
    own_indicators = _COUNTRY_ALIASES.get(own_key) or [own_key]

    # Step 1: own-country indicators → definitely our country, no conflict.
    if any(ind in location for ind in own_indicators):
        return False

    # Step 2: comprehensive foreign-country detection.
    own_family = _get_country_family(own_key)
    for country_key, country_names in _COUNTRY_NAME_REGISTRY.items():
        if country_key in own_family:
            continue  # same country — skip
        for name in country_names:
            if _word_in_location(name, location):
                return True  # a known foreign country name found

    # Step 3: benefit of the doubt — no foreign country detected.
    return False


def refilter(conn, profile_id: int, profile: dict) -> dict:
    """
    Re-apply hard requirements to every job in the database without fetching new jobs or using AI.

    - Jobs that NOW fail and have no user action → marked hidden=[Hard filter]
    - Jobs that NOW fail but user has acted on them (saved/applied/…) → skipped (preserved)
    - Jobs previously [Hard filter] that NOW pass → deleted from profile_jobs so they are
      re-scored on the next regular pipeline run
    - Jobs not yet in profile_jobs that fail → inserted as [Hard filter] hidden
    """
    all_jobs = database.get_profile_jobs_full(conn, profile_id)
    passed, rejected = pre_filter(list(all_jobs), profile)

    newly_rejected = 0
    updated_existing = 0
    already_filtered = 0
    skipped_actioned = 0

    for job, reason in rejected:
        result = database.refilter_profile_job(conn, profile_id, job["job_key"], reason)
        if result == "inserted":
            newly_rejected += 1
        elif result == "updated":
            updated_existing += 1
        elif result == "already_filtered":
            already_filtered += 1
        elif result == "skipped":
            skipped_actioned += 1

    cleared = 0
    for job in passed:
        if database.unfilter_profile_job(conn, profile_id, job["job_key"]):
            cleared += 1

    logger.info(
        f"[refilter] {len(all_jobs)} total jobs | "
        f"{len(rejected)} rejected ({newly_rejected} new, {updated_existing} updated, "
        f"{already_filtered} already filtered, {skipped_actioned} skipped—user acted) | "
        f"{cleared} cleared for re-evaluation"
    )
    return {
        "total_jobs": len(all_jobs),
        "jobs_passed_filter": len(passed),
        "rejected_new": newly_rejected,
        "rejected_updated": updated_existing,
        "rejected_already_filtered": already_filtered,
        "skipped_actioned": skipped_actioned,
        "cleared_for_reeval": cleared,
    }


def rank(
    conn,
    profile_id: int,
    profile: dict,
    profile_changed: bool,
    top_n: int = 15,
    batch_size: int = 50,
    min_match_score: float = 0.4,
    scoring_cfg: dict | None = None,
    score_limit: int | None = None,
    ideal_cand_pairs: int | None = None,
    desc_max_chars: int = 3500,
    retention_days: int = 30,
    top_n_display: int | None = None,
    progress_callback=None,
) -> dict:
    """
    Score unscored jobs and insert qualifying ones into profile_jobs.

    scoring_cfg    — weights dict from config.yaml ranker.scoring (optional; defaults used if absent).
    score_limit    — if set, cap AI scoring to the first N jobs after pre-filters (for testing).
    ideal_cand_pairs — pre-computed from DB; computed fresh from profile if not supplied.
    desc_max_chars — safety cap on description chars sent to AI (from config ranker.description_max_chars).
    Returns summary dict: {jobs_pre_filtered, jobs_scored, jobs_added}
    """
    unscored = database.get_unscored_jobs(conn, profile_id, retention_days=retention_days)
    logger.info(
        f"[ranker] {'Full rescore' if profile_changed else 'Incremental score'}: "
        f"{len(unscored)} unscored jobs for profile '{profile.get('profile_name')}'"
    )

    # Stage 0: Re-validate existing scored profile_jobs against current hard requirements.
    # Jobs that were scored in a previous run may no longer pass hard constraints if the
    # profile's locations/remote_type/etc. were updated since.  Re-filter and hide them
    # so they no longer appear in the candidate's job list.
    existing_jobs = database.get_profile_jobs_full(conn, profile_id)
    if existing_jobs:
        _, stale_rejected = pre_filter(list(existing_jobs), profile)
        stale_hidden = 0
        for job, reason in stale_rejected:
            outcome = database.refilter_profile_job(conn, profile_id, job["job_key"], reason)
            if outcome in ("updated", "inserted"):
                stale_hidden += 1
        if stale_hidden:
            logger.info(
                f"[ranker] Stale-score re-filter: {stale_hidden} previously-scored job(s) "
                f"now hidden — profile hard requirements have changed."
            )

    if not unscored:
        return {"jobs_pre_filtered": 0, "jobs_scored": 0, "jobs_added": 0}

    # Stage 1: Python pre-filter for hard requirements
    if progress_callback:
        progress_callback("filtering")
    passed, rejected = pre_filter(list(unscored), profile)
    logger.info(
        f"[ranker] Pre-filter: {len(passed)} passed, {len(rejected)} discarded by hard requirements"
    )

    # Insert rejected jobs as hidden so they aren't re-evaluated next run
    for job, reason in rejected:
        database.insert_profile_job(
            conn,
            profile_id=profile_id,
            job_key=job["job_key"],
            match_score=0.0,
            match_notes=f"[Hard filter] {reason}",
            rank_at_discovery=None,
            hidden=True,
        )

    if not passed:
        return {"jobs_pre_filtered": len(rejected), "jobs_scored": 0, "jobs_added": 0}

    # Stage 2: Title keyword filter (Python, no AI tokens)
    # Keep jobs whose title contains at least one keyword from the profile's target titles.
    # This prevents scoring thousands of irrelevant GH/Lever listings (e.g. Stripe has 520 open roles).
    passed, title_rejected = _title_filter(passed, profile)
    logger.info(
        f"[ranker] Title filter: {len(passed)} passed, {len(title_rejected)} discarded by title mismatch"
    )
    for job, reason in title_rejected:
        database.insert_profile_job(
            conn,
            profile_id=profile_id,
            job_key=job["job_key"],
            match_score=0.0,
            match_notes=f"[Title filter] {reason}",
            rank_at_discovery=None,
            hidden=True,
        )

    if not passed:
        return {"jobs_pre_filtered": len(rejected) + len(title_rejected), "jobs_scored": 0, "jobs_added": 0}

    # Stage 2.5: Description completeness filter (Python, no AI tokens)
    # Jobs without substantive job-content sections (responsibilities or requirements)
    # cannot be fairly scored — they produce spurious perfect scores because Claude
    # falls back to external knowledge about the company and domain.
    # Two checks must both pass:
    #   (a) The extracted description must be at least 150 chars (not empty/trivial).
    #   (b) The description must contain at least one work or requirements section —
    #       pure company-overview boilerplate (e.g. Plaid's 782-char about-us text)
    #       fails this check even though it exceeds the char floor.
    # Failed jobs are marked 'missing_info' for manual review via the UI tab.
    _MIN_DESCRIPTION_CHARS = 150
    missing_info: list = []
    scoreable: list = []
    for job in passed:
        raw_desc = job["description"] or ""
        extracted = extract_description(raw_desc, max_chars=desc_max_chars)
        if len(extracted) < _MIN_DESCRIPTION_CHARS or not is_description_scoreable(raw_desc):
            missing_info.append(job)
        else:
            scoreable.append(job)

    logger.info(
        f"[ranker] Description check: {len(scoreable)} scoreable, "
        f"{len(missing_info)} flagged as missing_info"
    )
    for job in missing_info:
        database.insert_profile_job(
            conn,
            profile_id=profile_id,
            job_key=job["job_key"],
            match_score=0.0,
            match_notes="[Missing info] Job description absent or too short to evaluate. Check the apply link for full details.",
            rank_at_discovery=None,
            hidden=True,
            application_status="missing_info",
        )
    passed = scoreable

    if not passed:
        return {
            "jobs_pre_filtered": len(rejected) + len(title_rejected),
            "jobs_missing_info": len(missing_info),
            "jobs_scored": 0,
            "jobs_added": 0,
        }

    # Sort by posted date (newest first) before AI scoring so that the most recent
    # jobs are evaluated first.  Jobs without a valid YYYY-MM-DD date_posted fall back
    # to date_found so they still participate but rank behind explicitly-dated jobs.
    def _sort_key(j):
        dp = j["date_posted"] or ""
        if len(dp) >= 10 and dp[4] == "-" and dp[7] == "-":
            return dp[:10]
        return (j["date_found"] or "")[:10]

    passed = sorted(passed, key=_sort_key, reverse=True)
    logger.debug(f"[ranker] Sorted {len(passed)} jobs by posted date (newest first) before AI scoring")

    # Score limit: cap AI scoring for testing purposes
    if score_limit and score_limit > 0:
        passed = passed[:score_limit]
        logger.info(f"[ranker] score_limit={score_limit}: capped to {len(passed)} jobs for AI scoring")

    # Use caller-supplied ideal_cand_pairs (stored in DB) or compute fresh from profile.
    if ideal_cand_pairs is None:
        ideal_cand_pairs = _count_candidate_preferences(profile)
        logger.debug(f"[ranker] ideal_cand_pairs computed from profile: {ideal_cand_pairs}")
    else:
        logger.debug(f"[ranker] ideal_cand_pairs loaded from DB: {ideal_cand_pairs}")

    # Stage 3: AI dual-perspective scoring in batches
    if progress_callback:
        progress_callback("scoring", len(passed))
    # Each batch is saved to DB immediately as hidden=True so that a mid-run
    # interruption doesn't lose work — they won't be re-scored next run.
    #
    # Content-aware batching: fill each batch up to a token budget rather than
    # a fixed count. Job descriptions vary widely in length — a fixed count of
    # 50 can produce 150k+ token prompts that time out. We cap by estimated
    # tokens (~3.5 chars/token) AND by the configured batch_size ceiling.
    # Budget raised to 20k (from 15k) because section-filtered descriptions are
    # shorter on average, so more jobs fit per batch → fewer subprocess calls.
    _TOKEN_BUDGET = 20_000
    _CHARS_PER_TOKEN = 3.5

    def _est_tokens(job) -> int:
        text = " ".join(str(job[k]) for k in job.keys() if job[k])
        return max(1, round(len(text) / _CHARS_PER_TOKEN))

    batches: list[list] = []
    cur_batch: list = []
    cur_tokens: int = 0
    for job in passed:
        jt = _est_tokens(job)
        if cur_batch and (cur_tokens + jt > _TOKEN_BUDGET or len(cur_batch) >= batch_size):
            batches.append(cur_batch)
            cur_batch, cur_tokens = [job], jt
        else:
            cur_batch.append(job)
            cur_tokens += jt
    if cur_batch:
        batches.append(cur_batch)

    all_scores: list[dict] = []
    total_batches = len(batches)
    consecutive_failures = 0
    _MAX_CONSECUTIVE_FAILURES = 5
    for batch_num, batch in enumerate(batches, 1):
        logger.info(f"[ranker] Scoring batch {batch_num}/{total_batches}: {len(batch)} jobs (~{round(sum(_est_tokens(j) for j in batch)/1000)}k tokens)...")
        try:
            scores = _score_batch(profile, batch, scoring_cfg or {}, ideal_cand_pairs=ideal_cand_pairs, desc_max_chars=desc_max_chars)
            consecutive_failures = 0
        except ai_client.RateLimitError as e:
            logger.error(f"[ranker] Usage limit reached at batch {batch_num}/{total_batches} — aborting run. {e}")
            break
        except Exception:
            consecutive_failures += 1
            if consecutive_failures >= _MAX_CONSECUTIVE_FAILURES:
                logger.error(
                    f"[ranker] {consecutive_failures} consecutive batch failures at batch "
                    f"{batch_num}/{total_batches} — aborting run (likely rate limited)."
                )
                break
            logger.info(f"[ranker] Batch {batch_num}/{total_batches} saved (0 jobs).")
            continue

        # Persist immediately so interruptions don't lose progress
        for scored in scores:
            database.insert_profile_job(
                conn,
                profile_id=profile_id,
                job_key=scored["job_key"],
                match_score=scored["match_score"],
                match_notes=scored.get("manager_notes", ""),
                rank_at_discovery=None,
                hidden=True,
                manager_score=scored["manager_score"],
                candidate_score=scored["candidate_score"],
                candidate_notes=scored.get("candidate_notes"),
                match_pairs_json=json.dumps(scored.get("match_pairs", [])),
                total_job_requirements=scored.get("total_job_requirements"),
                ai_raw_response=scored.get("ai_raw_response"),
            )
        all_scores.extend(scores)
        logger.info(f"[ranker] Batch {batch_num}/{total_batches} saved ({len(scores)} jobs).")

    # Promote top-N qualifying jobs to visible (hidden=False).
    # min_match_score = fraction of required manager-category pairs matched (0.0–1.0).
    # Acts as a gate to exclude jobs where the candidate clearly lacks required skills.
    # Above this floor, jobs are ranked by combined total score so both perspectives
    # contribute equally — strong candidate fit can lift a borderline manager score.
    qualifying = sorted(
        [s for s in all_scores if s["match_score"] >= min_match_score],
        key=lambda x: (x["manager_score"] + x["candidate_score"], x["manager_score"], x["candidate_score"]),
        reverse=True,
    )
    top_candidates = qualifying[:top_n]

    jobs_added = 0
    for rank_pos, scored in enumerate(top_candidates, start=1):
        promoted = database.promote_profile_job(
            conn,
            profile_id=profile_id,
            job_key=scored["job_key"],
            rank_at_discovery=rank_pos,
        )
        if promoted:
            jobs_added += 1

    logger.info(
        f"[ranker] Scored {len(all_scores)} jobs, "
        f"{len(qualifying)} qualifying (score >= {min_match_score}), "
        f"{jobs_added} promoted to visible."
    )

    # Rebalance the global recommended list so it always shows the top
    # top_n_display jobs across all runs, not just those from this run.
    rebalance_result = {"promoted": 0, "demoted": 0}
    if top_n_display is not None:
        rebalance_result = database.rebalance_visible_jobs(
            conn, profile_id, top_n_display, min_match_score
        )
        if rebalance_result["promoted"] or rebalance_result["demoted"]:
            logger.info(
                f"[ranker] Rebalanced display list (top_n_display={top_n_display}): "
                f"+{rebalance_result['promoted']} promoted, "
                f"-{rebalance_result['demoted']} demoted."
            )

    return {
        "jobs_pre_filtered": len(rejected),
        "jobs_missing_info": len(missing_info),
        "jobs_scored": len(all_scores),
        "jobs_added": jobs_added,
    }


# ---------------------------------------------------------------------------
# Category sets for scoring direction
# ---------------------------------------------------------------------------
_MANAGER_CATEGORIES   = {"skill", "experience", "domain"}
_CANDIDATE_CATEGORIES = {"compensation", "equity", "benefits", "work_arrangement", "culture_growth"}


_SALARY_KEYWORDS    = frozenset({"salary", "pay", "compensation", "comp", "usd", "$", "k/yr", "k/year", "per year", "annual"})
_LOCATION_KEYWORDS  = frozenset({"remote", "hybrid", "onsite", "on-site", "in-person", "office", "location", "timezone", "relocat"})

# Phrases that indicate salary was not disclosed — used to prevent "Salary not
# disclosed" in job_side from scoring as a compensation match.
_COMP_NON_DISCLOSURE = _re.compile(
    r"\b(not\s+disclosed|not\s+stated|not\s+provided|undisclosed|"
    r"not\s+available|n/?a|salary\s+range\s+not\s+listed|"
    r"confidential|tbd|to\s+be\s+determined)\b",
    _re.IGNORECASE,
)


def _must_haves_contain(must_haves: list, keywords: frozenset) -> bool:
    """Return True if any must_have string contains at least one of the given keywords."""
    joined = " ".join(must_haves).lower()
    return any(kw in joined for kw in keywords)


def _count_candidate_preferences(profile: dict) -> int:
    """
    Count the number of candidate-side preferences that Claude may generate
    a pair for.  Used as the depth-factor denominator for the candidate score.

    Included:
      must_haves       — every item always generates a candidate pair (even job-silent ones)
      nice_to_haves    — each item may generate a pair; included so jobs that address
                         more preferences score higher than those that address fewer
      salary_min/max   — generates a compensation pair; skipped if already in must_haves
      work_style       — generates a work_arrangement pair; skipped if already in must_haves
      preferred_locations — counted as 1 location block when non-empty and not fully remote
      hard_requirements.locations — fallback location source when preferred_locations is
                         empty; Claude always generates a work_arrangement pair from it

    Excluded:
      target_companies — only generate a pair for jobs at those specific companies;
                         including them would penalise all other jobs in the depth calc
    """
    must_haves: list  = list(profile.get("must_haves")    or [])
    nice_to_haves: list = list(profile.get("nice_to_haves") or [])
    count = len(must_haves) + len(nice_to_haves)

    # Salary: add only if not already expressed in must_haves
    if (profile.get("salary_min") or profile.get("salary_max")) and \
            not _must_haves_contain(must_haves, _SALARY_KEYWORDS):
        count += 1

    # Work arrangement: add only if explicit and not already expressed in must_haves
    ws = (profile.get("work_style") or "").lower().strip()
    ws_explicit = ws and ws not in ("flexible", "unknown", "")
    if ws_explicit and not _must_haves_contain(must_haves, _LOCATION_KEYWORDS):
        count += 1

    # Location preference: count as 1 when any location constraint exists and not fully remote.
    # Claude generates a single work_arrangement pair for the location block regardless of
    # how many cities are listed. Falls back to hard_requirements.locations when
    # preferred_locations is empty (Claude receives the full profile JSON and uses both).
    if ws != "remote":
        preferred_locs = [loc for loc in (profile.get("preferred_locations") or [])
                          if loc and loc.lower() != "remote"]
        hard_locs = (profile.get("hard_requirements") or {}).get("locations") or []
        if preferred_locs or hard_locs:
            count += 1

    return max(count, 1)  # at least 1 to avoid division by zero


def compute_scores_public(
    match_pairs: list[dict],
    scoring_cfg: dict,
    total_job_requirements: int | None = None,
    ideal_cand_pairs: int | None = None,
) -> tuple[int, int, float]:
    """Public wrapper around _compute_scores for use outside the ranker module."""
    return _compute_scores(match_pairs, scoring_cfg, total_job_requirements, ideal_cand_pairs)


def get_ideal_cand_pairs(profile: dict) -> int:
    """Public wrapper around _count_candidate_preferences for use outside the ranker module."""
    return _count_candidate_preferences(profile)


def _compute_scores(
    match_pairs: list[dict],
    scoring_cfg: dict,
    total_job_requirements: int | None = None,
    ideal_cand_pairs: int | None = None,
) -> tuple[int, int, float]:
    """
    Derive manager_score (0–200), candidate_score (0–200), and match_score (0.0–1.0)
    from a list of match_pairs extracted by Claude.

    Scoring approach — ratio × depth:
      ratio    = raw / baseline × 200  (0–200; 100 if no signals)
      depth    = min(pairs_covering_requirements / ideal, 1.0)  (0–1 confidence)
      score    = clamp(100 + (ratio - 100) × depth, 0, 200)

    Manager depth denominator:
      total_job_requirements — reported by Claude from the job posting (preferred).
      Falls back to the count of manager pairs extracted (all methods are lower bounds).

    Candidate depth denominator:
      ideal_cand_pairs — computed by Python from the profile's must-have preferences
      before this call.  Falls back to cand_pair_count if not provided.

    match_score = required_matched / required_total for manager categories.
    Used as a pre-promotion threshold gate (min_match_score in config).
    If no required pairs exist, match_score = 1.0 (no hard gate needed).
    """
    mgr_cfg  = scoring_cfg.get("manager",   {})
    cand_cfg = scoring_cfg.get("candidate", {})

    # Points per job_importance level (manager categories)
    IMP: dict = {
        "required":     mgr_cfg.get("required",     10),
        "preferred":    mgr_cfg.get("preferred",      6),
        "nice_to_have": mgr_cfg.get("nice_to_have",   3),
        None:           mgr_cfg.get("unknown",         5),
    }
    EXTRA: int = mgr_cfg.get("extra_skill", 1)

    # Points per candidate_priority level (candidate categories)
    PRI: dict = {
        "must_have":    cand_cfg.get("must_have",    10),
        "nice_to_have": cand_cfg.get("nice_to_have",  5),
        None:           cand_cfg.get("unknown",        7),
    }

    mgr_raw       = 0
    mgr_baseline  = 0
    cand_raw      = 0
    cand_baseline = 0
    required_total   = 0
    required_matched = 0
    mgr_pairs_from_job = 0   # pairs where the job stated a requirement (match or gap)
    cand_pair_count    = 0

    for pair in match_pairs:
        if not isinstance(pair, dict):
            continue
        category = (pair.get("category") or "").lower().strip()
        job_side_raw = pair.get("job_side") or ""
        has_job  = bool(job_side_raw)
        has_cand = bool(pair.get("candidate_side"))
        job_imp  = pair.get("job_importance")    # required | preferred | nice_to_have | None
        cand_pri = pair.get("candidate_priority") # must_have | nice_to_have | None

        # Defensive: for compensation pairs, treat any "not disclosed / not stated"
        # job_side as absent.  The AI prompt instructs Claude to omit job_side in this
        # case, but occasionally it fills it with a phrase like "Salary not disclosed".
        # Scoring that as has_job=True would make an undisclosed salary count as a match.
        if category == "compensation" and has_job and _COMP_NON_DISCLOSURE.search(job_side_raw):
            has_job = False

        if category in _MANAGER_CATEGORIES:
            pts = IMP.get(job_imp, IMP[None])
            if has_job and has_cand:
                # Match — candidate satisfies this requirement
                mgr_raw            += pts
                mgr_baseline       += pts
                mgr_pairs_from_job += 1
                if job_imp == "required":
                    required_total   += 1
                    required_matched += 1
            elif has_job and not has_cand:
                # Gap — job requires X, candidate lacks it; scores 0, baseline still counts it
                mgr_baseline       += pts
                mgr_pairs_from_job += 1
                if job_imp == "required":
                    required_total += 1
            else:
                # Extra skill — candidate has something the job doesn't need; small bonus
                mgr_raw += EXTRA

        elif category in _CANDIDATE_CATEGORIES:
            cand_pair_count += 1
            pts = PRI.get(cand_pri, PRI[None])
            if has_job and has_cand:
                # Match — job satisfies this candidate preference
                cand_raw      += pts
                cand_baseline += pts
            elif not has_job and has_cand:
                # Unmet preference — job is silent or doesn't offer it.
                # Per AI rules only must_have generates this pair type, but be defensive.
                cand_baseline += pts   # what it could have scored
                # raw gets 0 (no points for not meeting the preference)

    # Manager depth: always treat extracted pairs as full coverage (depth=1.0).
    #
    # We previously used: depth = mgr_pairs_from_job / total_job_requirements
    # (where total_job_requirements is Claude's count of ALL requirements in the posting).
    # That caused a systematic bug: Claude counts raw line-items in the posting (~40) while
    # generating fewer consolidated pairs (~11 logical skill areas).  With depth=11/40=0.275
    # and ratio=200 (all matched), score was 128 — lower than a job with 2 gaps that had
    # higher depth (6/9=0.667), which scored 136.  An all-match job ranked below a
    # partial-match job because Claude's line-item count inflated the depth denominator.
    #
    # With depth=1.0, score = ratio_mgr directly: more matches → higher score, gaps → lower.
    # total_job_requirements is still stored in the DB for display, but no longer affects scoring.
    mgr_depth = 1.0 if mgr_pairs_from_job > 0 else 0.0

    # Candidate depth: how many of the profile's must-have preferences were addressed?
    # ideal_cand_pairs from Python profile count is the best denominator.
    if ideal_cand_pairs and ideal_cand_pairs > 0:
        cand_depth = min(cand_pair_count / ideal_cand_pairs, 1.0)
    else:
        cand_depth = 1.0 if cand_pair_count > 0 else 0.0

    ratio_mgr  = (mgr_raw  / mgr_baseline  * 200) if mgr_baseline  > 0 else 100.0
    ratio_cand = (cand_raw / cand_baseline * 200) if cand_baseline > 0 else 100.0

    manager_score   = round(100 + (ratio_mgr  - 100) * mgr_depth)
    candidate_score = round(100 + (ratio_cand - 100) * cand_depth)

    manager_score   = max(0, min(200, manager_score))
    candidate_score = max(0, min(200, candidate_score))

    match_score = (required_matched / required_total) if required_total > 0 else 1.0

    return manager_score, candidate_score, match_score


def _score_batch(
    profile: dict,
    jobs: list,
    scoring_cfg: dict | None = None,
    ideal_cand_pairs: int | None = None,
    desc_max_chars: int = 3500,
) -> list[dict]:
    """
    Send a batch of jobs to Claude for match_pair extraction.
    Returns a list of score dicts with manager_score, candidate_score, match_score, and notes.

    ideal_cand_pairs — pre-computed from the profile by _count_candidate_preferences().
      Passed through to _compute_scores() as the candidate depth denominator.
    desc_max_chars   — safety cap on description length (passed from ranker config).
    """
    if scoring_cfg is None:
        scoring_cfg = {}

    condensed = []
    for job in jobs:
        desc = job["description"] or ""
        snippet = extract_description(desc, max_chars=desc_max_chars)
        # Build job dict; omit keys with null/empty values to reduce token count
        job_entry: dict = {"job_key": job["job_key"]}
        if job["title"]:
            job_entry["title"] = job["title"]
        if job["company"]:
            job_entry["company"] = job["company"]
        if job["location"]:
            job_entry["location"] = job["location"]
        remote = job["remote_type"] or ""
        if remote and remote != "unknown":
            job_entry["remote_type"] = remote
        if job["salary_min"]:
            job_entry["salary_min"] = job["salary_min"]
        if job["salary_max"]:
            job_entry["salary_max"] = job["salary_max"]
        job_entry["description"] = snippet
        condensed.append(job_entry)

    # Static system prompt (rules + profile) — sent cached when using the anthropic provider.
    # Dynamic user message — just the jobs JSON, changes every batch.
    system_prompt = _RANKER_SYSTEM_TMPL.format(profile_json=json.dumps(profile))
    user_content  = _RANKER_USER_TMPL.format(jobs_json=json.dumps(condensed))

    try:
        results, ai_raw_response = ai_client.call_ai_for_json_with_raw(user_content, system=system_prompt)
    except ai_client.RateLimitError:
        raise  # propagate up so the batch loop aborts the entire run
    except Exception as e:
        logger.error(f"[ranker] Claude call failed for batch: {e}")
        raise  # propagate so the batch loop can count consecutive failures

    if not isinstance(results, list):
        logger.error(f"[ranker] Expected list from Claude, got {type(results)}")
        return []

    valid = []
    for r in results:
        if not isinstance(r, dict):
            continue
        job_key = r.get("job_key")
        if not job_key:
            continue

        match_pairs = [p for p in (r.get("match_pairs") or []) if isinstance(p, dict)]
        total_job_requirements = r.get("total_job_requirements")
        if isinstance(total_job_requirements, (int, float)) and total_job_requirements > 0:
            total_job_requirements = int(total_job_requirements)
        else:
            total_job_requirements = None
        manager_score, candidate_score, match_score = _compute_scores(
            match_pairs, scoring_cfg,
            total_job_requirements=total_job_requirements,
            ideal_cand_pairs=ideal_cand_pairs,
        )

        valid.append({
            "job_key":                job_key,
            "match_score":            match_score,
            "manager_score":          manager_score,
            "candidate_score":        candidate_score,
            "manager_notes":          str(r.get("manager_notes")   or ""),
            "candidate_notes":        str(r.get("candidate_notes") or ""),
            "match_pairs":            match_pairs,
            "total_job_requirements": total_job_requirements,
            "ai_raw_response":        ai_raw_response,
        })

    return valid
