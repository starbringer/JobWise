"""
Migration v1: Full schema creation — current as of v12.

Creates all tables in their final form and seeds ats_companies with known
Greenhouse/Lever slugs. Registers schema versions 1–12 so that incremental
migrations (v10/v11/v12) are skipped on fresh installs.

Existing databases at version >= 1 skip this migration automatically; their
incremental migrations run as needed via v10/v11/v12.
"""


def migrate(conn):
    c = conn.cursor()

    # Create tables in dependency order (profiles before jobs/profile_jobs/search_runs)
    c.executescript("""
        CREATE TABLE IF NOT EXISTS schema_version (
            version     INTEGER PRIMARY KEY,
            applied_at  DATETIME DEFAULT CURRENT_TIMESTAMP,
            description TEXT
        );

        CREATE TABLE IF NOT EXISTS profiles (
            id                  INTEGER PRIMARY KEY AUTOINCREMENT,
            name                TEXT NOT NULL,
            input_file          TEXT NOT NULL,
            input_hash          TEXT,
            input_modified_at   DATETIME,
            structured_content  TEXT,
            custom_job_titles   TEXT DEFAULT '[]',
            ideal_cand_pairs    INTEGER,
            created_at          DATETIME DEFAULT CURRENT_TIMESTAMP,
            updated_at          DATETIME DEFAULT CURRENT_TIMESTAMP
        );

        CREATE UNIQUE INDEX IF NOT EXISTS idx_profiles_name_lower ON profiles(LOWER(name));

        CREATE TABLE IF NOT EXISTS jobs (
            job_key                 TEXT PRIMARY KEY,
            title                   TEXT NOT NULL,
            company                 TEXT NOT NULL,
            location                TEXT,
            remote_type             TEXT,
            salary_min              INTEGER,
            salary_max              INTEGER,
            salary_currency         TEXT DEFAULT 'USD',
            salary_period           TEXT DEFAULT 'annual',
            salary_raw              TEXT,
            description             TEXT,
            apply_url               TEXT,
            source                  TEXT NOT NULL,
            source_company_slug     TEXT,
            date_posted             DATE,
            date_found              DATETIME DEFAULT CURRENT_TIMESTAMP,
            raw_data                TEXT,
            fetched_for_profile_id  INTEGER REFERENCES profiles(id)
        );

        CREATE TABLE IF NOT EXISTS profile_jobs (
            id                      INTEGER PRIMARY KEY AUTOINCREMENT,
            profile_id              INTEGER NOT NULL REFERENCES profiles(id),
            job_key                 TEXT NOT NULL REFERENCES jobs(job_key),
            match_score             REAL,
            match_notes             TEXT,
            rank_at_discovery       INTEGER,
            application_status      TEXT DEFAULT 'new',
            status_updated_at       DATETIME,
            hidden                  BOOLEAN DEFAULT FALSE,
            notes                   TEXT,
            saved                   BOOLEAN DEFAULT FALSE,
            manager_score           INTEGER,
            candidate_score         INTEGER,
            candidate_notes         TEXT,
            match_pairs_json        TEXT,
            total_job_requirements  INTEGER,
            ai_raw_response         TEXT,
            added_at                DATETIME DEFAULT CURRENT_TIMESTAMP,

            UNIQUE(profile_id, job_key)
        );

        CREATE TABLE IF NOT EXISTS ats_companies (
            id            INTEGER PRIMARY KEY AUTOINCREMENT,
            company       TEXT NOT NULL,
            ats           TEXT NOT NULL,
            slug          TEXT NOT NULL,
            verified      BOOLEAN DEFAULT FALSE,
            discovered_at DATETIME DEFAULT CURRENT_TIMESTAMP,

            UNIQUE(ats, slug)
        );

        CREATE TABLE IF NOT EXISTS api_quota (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            service         TEXT UNIQUE NOT NULL,
            requests_used   INTEGER DEFAULT 0,
            monthly_limit   INTEGER NOT NULL,
            reset_day       INTEGER NOT NULL,
            next_reset_date DATE NOT NULL,
            is_exhausted    BOOLEAN DEFAULT FALSE,
            last_updated    DATETIME DEFAULT CURRENT_TIMESTAMP
        );

        CREATE TABLE IF NOT EXISTS search_runs (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            profile_id      INTEGER REFERENCES profiles(id),
            triggered_by    TEXT,
            sources_used    TEXT,
            jobs_found      INTEGER,
            jobs_added      INTEGER,
            jsearch_credits INTEGER DEFAULT 0,
            status          TEXT,
            error_message   TEXT,
            started_at      DATETIME DEFAULT CURRENT_TIMESTAMP,
            finished_at     DATETIME
        );
    """)

    # Seed Greenhouse companies
    greenhouse_seeds = [
        ("Airbnb", "airbnb"),
        ("Stripe", "stripe"),
        ("Snapchat", "snap"),
        ("Dropbox", "dropbox"),
        ("Pinterest", "pinterest"),
        ("Lyft", "lyft"),
        ("Instacart", "instacart"),
        ("DoorDash", "doordash"),
        ("HubSpot", "hubspot"),
        ("Duolingo", "duolingo"),
        ("Anthropic", "anthropic"),
        ("Gong", "gong"),
        ("NerdWallet", "nerdwallet"),
        ("Squarespace", "squarespace"),
        ("Wayfair", "wayfair"),
        ("Evernote", "evernote"),
        ("Foursquare", "foursquare"),
        ("Cisco", "cisco"),
        ("DocuSign", "docusign"),
        ("The New York Times", "nytimes"),
    ]

    # Seed Lever companies
    lever_seeds = [
        ("Netflix", "netflix"),
        ("Atlassian", "atlassian"),
        ("Discord", "discord"),
        ("Shopify", "shopify"),
        ("Figma", "figma"),
        ("Coinbase", "coinbase"),
        ("KPMG", "kpmg"),
        ("Plaid", "plaid"),
        ("Robinhood", "robinhood"),
        ("Brex", "brex"),
    ]

    for company, slug in greenhouse_seeds:
        c.execute(
            "INSERT OR IGNORE INTO ats_companies (company, ats, slug, verified) VALUES (?, 'greenhouse', ?, TRUE)",
            (company, slug),
        )

    for company, slug in lever_seeds:
        c.execute(
            "INSERT OR IGNORE INTO ats_companies (company, ats, slug, verified) VALUES (?, 'lever', ?, TRUE)",
            (company, slug),
        )

    # Register all versions so incremental migrations are skipped on fresh installs
    for version, desc in [
        (1,  "Full schema — all tables, columns, and ATS seed data"),
        (10, "Add fetched_for_profile_id to jobs"),
        (11, "Add saved flag separate from application_status"),
        (12, "Case-insensitive profile name dedup + LOWER(name) unique index"),
    ]:
        c.execute(
            "INSERT OR IGNORE INTO schema_version (version, description) VALUES (?, ?)",
            (version, desc),
        )

    conn.commit()
