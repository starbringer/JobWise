"""
profile_processor.py — Reads a profile file and generates structured JSON content via AI.
Supported formats: .txt, .md, .pdf, .docx
Stores the result in profiles.structured_content in the DB.
"""

import hashlib
import json
import logging
import os
from datetime import datetime, timezone
from pathlib import Path

from src import ai_client, database

logger = logging.getLogger(__name__)

STRUCTURED_CONTENT_PROMPT = """You are a career profile analyst. Read the following free-form career profile or resume text and produce a structured JSON object.

Your two main tasks are:
  A) Extract explicit AND implicit keywords from the profile text.
  B) Identify which requirements are HARD (deal-breakers) vs SOFT (preferences).

Output ONLY valid JSON — no explanation, no markdown, no code fences.

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
TASK A — KEYWORD EXTRACTION
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Include BOTH explicit and implicit keywords in extracted_keywords.

Explicit keywords: directly stated technologies, skills, certifications, tools.
  e.g. "Python", "Kubernetes", "CI/CD", "PostgreSQL"

Implicit keywords: skills or traits INFERRED from context but not stated verbatim.
Read every project, achievement, and responsibility and surface what it implies.
  Examples of inference:
  * "Led a team of 8 engineers across 3 time zones"
    → leadership, people management, remote team management, cross-functional collaboration
  * "Reduced API p99 latency from 800ms to 120ms"
    → performance optimization, systems reliability, backend engineering, profiling
  * "Communicated weekly sprint progress to VP of Engineering and CFO"
    → executive communication, stakeholder management, technical presentation
  * "Delivered migration of 10TB legacy database with zero downtime"
    → database migration, operational reliability, risk management, technical planning
  * "Owned the on-call rotation and resolved 3 P0 incidents in Q3"
    → incident response, on-call, production operations, systems ownership
  * "Mentored 4 junior engineers who were promoted within 18 months"
    → mentorship, coaching, engineering culture, team development

Be generous — it is better to include a keyword that might not apply than to miss
a genuine strength. Aim for 20-60 keywords total.

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
TASK B — HARD vs SOFT CLASSIFICATION
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
HARD requirements are absolute deal-breakers — the candidate will not accept a role
that violates them. Identify them by explicit language:
  Hard indicators: "only", "must", "require", "will not", "not interested in",
                   "won't consider", "deal-breaker", "no X", "exclusively"
  e.g. "I only want fully remote roles" → hard_remote_type = "remote"
  e.g. "I'm not interested in agencies or staffing firms" → hard_exclude_company_types
  e.g. "I only do on-site locally in California/Bay Area" → remote_type = "on-site", locations = ["California", "CA"]
  e.g. "I'm open to relocating" or no location restriction → locations = [] (no constraint)

SOFT requirements are preferences — good to have, but not deal-breakers.
  Soft indicators: "prefer", "ideally", "hoping for", "open to", "would like"
  e.g. "I prefer companies in the Bay Area" → soft, goes into preferred_locations only

For job titles specifically:
  If the profile EXPLICITLY states a target title ("I am looking for a Staff Engineer role"):
    → add to hard_requirements.job_titles (used to enrich search queries)
  If titles appear only as background ("I have worked as a software engineer"):
    → these become target_job_titles (soft preference) only

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
OUTPUT SCHEMA
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
{{
  "profile_name": "<string>",
  "generated_at": "<YYYY-MM-DD>",

  "experience_summary": "<1-2 sentence summary of their background and level>",
  "years_experience_total": <integer or null>,
  "years_experience_primary": <integer or null>,

  "target_job_titles": ["<soft preferred titles — be specific with seniority, 3-5 variations>"],
  "target_industries": ["<preferred industries>"],
  "target_companies": ["<specific company names they mentioned positively>"],
  "preferred_locations": ["<City, ST format, or 'remote'>"],
  "work_style": "<remote | hybrid | on-site | flexible>",
  "salary_min": <annual USD integer or null>,
  "salary_max": <annual USD integer or null>,
  "salary_currency": "USD",

  "technical_skills": ["<explicit technical skills>"],
  "soft_skills": ["<explicit soft skills>"],
  "must_haves": ["<non-negotiable requirements stated explicitly>"],
  "nice_to_haves": ["<preferred but optional>"],
  "exclusions": ["<job types, conditions, or contexts they want to avoid>"],

  "extracted_keywords": [
    "<all keywords, explicit AND implicit, as individual strings>"
  ],

  "hard_requirements": {{
    "job_titles":             ["<explicit target titles if stated as requirement, else []>"],
    "exclude_titles":         ["<job title keywords they explicitly do not want, e.g. 'engineering manager', 'director'; used as substring match — be specific to avoid false positives; else []>"],
    "company_exclude":        ["<company names they explicitly do not want, else []>"],
    "remote_type":            "<remote | hybrid | on-site | null>",
    "locations":              ["<areas where the user accepts physical presence, e.g. ['California', 'CA', 'Bay Area']; set only if they restrict on-site/hybrid to specific areas; empty [] if willing to relocate or fully remote only>"],
    "salary_min":             <annual USD integer or null>,
    "employment_type":        "<FULL_TIME | CONTRACT | PART_TIME | null>",
    "exclude_industries":     ["<industries they explicitly refuse, else []>"],
    "exclude_company_types":  ["<company types they explicitly refuse e.g. agency staffing, else []>"],
    "country":                ["<country name or code they require jobs to be in, e.g. US, Canada, UK — [] if not stated or if fully remote>"],
    "has_clearance":          <true | false | null — true if profile explicitly states they hold a US federal security clearance (Secret, TS, TS/SCI, etc.); false if they explicitly state they do NOT have clearance; null if not mentioned>
  }}
}}

Rules:
- Use null for missing scalar fields, [] for missing list fields.
- Do NOT invent information not present in the profile.
- salary fields: normalize to annual USD integers (hourly × 2080, monthly × 12).
- preferred_locations: if work_style is remote, put ["remote"]; otherwise format as "City, ST".
- extracted_keywords: flat list of strings, no nesting. Both explicit and implicit.
- hard_requirements: only populate fields where the profile uses clear hard language.
  When in doubt, leave null / [].
- has_clearance: set to false only when the profile explicitly says they lack clearance
  (e.g. "I do not have a clearance", "no clearance"). Set true only if they say they hold
  one (e.g. "I have an active TS/SCI"). Leave null when the topic is not mentioned.

Profile name: {profile_name}
Today's date: {today}

--- BEGIN PROFILE ---
{profile_text}
--- END PROFILE ---
"""


SUPPORTED_EXTENSIONS = (".txt", ".md", ".pdf", ".docx")


def _md5(text: str) -> str:
    return hashlib.md5(text.encode("utf-8")).hexdigest()


def _file_mtime_iso(path: Path) -> str:
    ts = os.path.getmtime(path)
    return datetime.fromtimestamp(ts, tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S")


def _find_profile_file(profiles_dir: Path, profile_name: str) -> Path | None:
    """Return the first matching profile file for profile_name, trying all supported extensions."""
    for ext in SUPPORTED_EXTENSIONS:
        path = profiles_dir / f"{profile_name}{ext}"
        if path.exists():
            return path
    return None


def _read_profile_text(input_file: Path) -> str:
    """Extract plain text from a profile file regardless of format."""
    ext = input_file.suffix.lower()
    if ext in (".txt", ".md"):
        return input_file.read_text(encoding="utf-8")
    elif ext == ".pdf":
        import pypdf
        reader = pypdf.PdfReader(input_file)
        return "\n".join(page.extract_text() or "" for page in reader.pages)
    elif ext == ".docx":
        import docx
        doc = docx.Document(input_file)
        return "\n".join(p.text for p in doc.paragraphs)
    else:
        raise ValueError(f"Unsupported profile file format: {ext}")


def _parse_from_file(profile_name: str, input_file: Path) -> dict:
    """Parse a profile file into structured JSON via AI. Returns the dict."""
    profile_text = _read_profile_text(input_file)
    today = datetime.now(tz=timezone.utc).strftime("%Y-%m-%d")
    prompt = STRUCTURED_CONTENT_PROMPT.format(
        profile_name=profile_name,
        today=today,
        profile_text=profile_text,
    )
    structured = ai_client.call_ai_for_json(prompt)
    structured["profile_name"] = profile_name
    structured["generated_at"] = today
    if "extracted_keywords" not in structured:
        structured["extracted_keywords"] = (
            structured.get("technical_skills", []) + structured.get("soft_skills", [])
        )
    if "hard_requirements" not in structured:
        structured["hard_requirements"] = {
            "job_titles": [], "company_exclude": [], "remote_type": None,
            "locations": [], "salary_min": None, "employment_type": None,
            "exclude_industries": [], "exclude_company_types": [],
            "has_clearance": None,
        }
    return structured


def _merge_profiles(existing: dict, new_parsed: dict) -> tuple[dict, int]:
    """
    Merge new_parsed into existing. Never removes — only adds missing items.
    - Lists: union (add items not already present, case-insensitive)
    - Scalars: only fill in if existing value is None/empty
    - Dicts: recurse
    Returns (merged_dict, additions_count).
    """
    result = dict(existing)
    additions = 0
    for key, new_val in new_parsed.items():
        if key in ("profile_name", "generated_at"):
            continue
        existing_val = result.get(key)
        if isinstance(new_val, list):
            existing_list = existing_val if isinstance(existing_val, list) else []
            existing_lower = {str(x).lower() for x in existing_list}
            to_add = [x for x in new_val if str(x).lower() not in existing_lower]
            if to_add:
                result[key] = existing_list + to_add
                additions += len(to_add)
        elif isinstance(new_val, dict):
            existing_dict = existing_val if isinstance(existing_val, dict) else {}
            merged_sub, sub_adds = _merge_profiles(existing_dict, new_val)
            result[key] = merged_sub
            additions += sub_adds
        else:
            if (existing_val is None or existing_val == "") and new_val is not None:
                result[key] = new_val
                additions += 1
    return result, additions


def process(
    conn,
    profile_name: str,
    profiles_dir: Path,
) -> dict:
    """
    Process a profile for the pipeline. Returns a dict with:
      - profile_id: int
      - profile_changed: bool (True if structured_content was freshly generated)
      - structured_content: dict (parsed JSON)

    If structured_content already exists in the DB, it is returned as-is
    (the pipeline trusts the DB; use sync_from_file() to pull in txt changes).

    Raises FileNotFoundError only if no structured_content exists in the DB AND no supported
    profile file is found. Once a profile has been initialized (structured_content set), no
    file is needed for subsequent runs.
    """
    # Check DB first — if the profile is already initialized, no file needed.
    db_profile = database.get_profile(conn, profile_name)
    if db_profile and db_profile["structured_content"]:
        logger.info(f"Profile '{profile_name}' already in DB — skipping file parsing.")
        return {
            "profile_id": db_profile["id"],
            "profile_changed": False,
            "structured_content": json.loads(db_profile["structured_content"]),
        }

    # Profile not yet initialized — require a file.
    found_file = _find_profile_file(profiles_dir, profile_name)
    if not found_file:
        raise FileNotFoundError(
            f"No profile file found for '{profile_name}' in {profiles_dir}\n"
            f"Create profiles/{profile_name}.txt (or .md / .pdf / .docx) with your experience, "
            f"skills, and ideal job description."
        )

    profile_id = database.upsert_profile(conn, profile_name, str(found_file))
    logger.info(f"Profile '{profile_name}' is new — parsing from {found_file.name}...")
    structured = _parse_from_file(profile_name, found_file)

    structured_json = json.dumps(structured, ensure_ascii=False)
    database.update_profile_structured_content(
        conn,
        profile_id=profile_id,
        structured_content=structured_json,
        input_hash=_md5(_read_profile_text(found_file)),
        input_modified_at=_file_mtime_iso(found_file),
    )
    logger.info(f"Profile '{profile_name}' created with {len(structured.get('extracted_keywords', []))} keywords.")

    return {
        "profile_id": profile_id,
        "profile_changed": True,
        "structured_content": structured,
    }


def sync_from_file(conn, profile_name: str, profiles_dir: Path) -> dict:
    """
    Re-parse the .txt file and merge new content into the existing DB profile.
    Never removes existing items — only adds what is missing.

    Returns:
      - structured_content: dict (merged result)
      - additions: int (number of new items added)
    """
    input_file = _find_profile_file(profiles_dir, profile_name)
    if not input_file:
        raise FileNotFoundError(
            f"No profile file found for '{profile_name}' in {profiles_dir}\n"
            f"Supported formats: {', '.join(SUPPORTED_EXTENSIONS)}"
        )

    db_profile = database.get_profile(conn, profile_name)
    if not db_profile:
        raise ValueError(f"Profile '{profile_name}' not found in DB.")

    logger.info(f"[sync] Parsing '{input_file.name}' for profile '{profile_name}'...")
    new_parsed = _parse_from_file(profile_name, input_file)

    existing = json.loads(db_profile["structured_content"]) if db_profile["structured_content"] else {}
    merged, additions = _merge_profiles(existing, new_parsed)

    structured_json = json.dumps(merged, ensure_ascii=False)
    profile_id = db_profile["id"]
    database.update_profile_structured_content(
        conn,
        profile_id=profile_id,
        structured_content=structured_json,
        input_hash=_md5(_read_profile_text(input_file)),
        input_modified_at=_file_mtime_iso(input_file),
    )
    logger.info(f"[sync] Profile '{profile_name}' synced — {additions} new item(s) added.")
    return {"structured_content": merged, "additions": additions}
