<img src="docs/img/icon.png" alt="JobWise" width="80">

# JobWise

![Python](https://img.shields.io/badge/python-3.11%2B-blue)
![License](https://img.shields.io/badge/license-Apache%202.0-blue)
[![Buy Me a Coffee](https://img.shields.io/badge/Buy%20Me%20a%20Coffee-%E2%98%95-yellow)](https://buymeacoffee.com/starbringer)

A self-hosted, AI-powered job search assistant. It fetches postings from multiple sources, scores each one against your personal profile, and serves a local web UI where you review matches and track your applications.

> **Self-hosted and private** — your profile and job data stay on your machine. Only your profile text and job descriptions are sent to the AI provider you choose.

---

## ⚡ Quick Start

**New here? One-click setup — takes about 5 minutes.**

| Platform | What to do |
|----------|-----------|
| **Windows** | Double-click **`setup.bat`** in the project folder |
| **Mac / Linux** | Run `bash setup.sh` in the project folder |

The setup wizard walks you through everything: installing packages, getting API keys (with step-by-step instructions), setting up your profile, and opening the web app.

**After setup, to open JobWise:**
- **Windows** — double-click **`start.bat`**
- **Mac / Linux** — run `bash start.sh`

This opens a terminal window running the app. **To stop JobWise**, switch to that terminal and press `Ctrl+C` (or just close the terminal window). Closing the browser tab alone does not stop it.

> Already comfortable with Python? See [For Technical Users](#for-technical-users) below for the manual setup.

---

## Features

- **Multi-source fetching** — Greenhouse, Lever, JSearch, and JobSpy (LinkedIn/Indeed)
- **AI scoring** — each job scored from both a hiring-manager and candidate perspective
- **Hard-requirement filtering** — titles, companies, remote type, salary, country, clearance, and industry filters applied before any AI tokens are spent
- **Any AI provider** — Gemini, OpenAI, Anthropic, or local Ollama; no lock-in
- **Multiple profiles** — manage separate job searches side by side (e.g. different roles, markets, or seniority levels), each with independent scoring and history
- **Application tracking** — move jobs through a full pipeline (applied → phone screen → interviews → offer/rejected) and save bookmarks; saved and active-stage jobs are never auto-removed
- **Incremental runs** — only new jobs are fetched and scored on repeat runs
- **Resume as profile** — drop in your resume as `.txt`, `.md`, `.pdf`, or `.docx` to get started instantly
- **Preferred companies** — pin a list of companies that are always searched on every run
- **Web UI** — browse matches, edit your profile, view pipeline history, and choose from 9 color themes (Pine, Black, Forest, Midnight, Ocean, Sunset, Lavender, Linen, Fjord); mobile-friendly — accessible from any device on the same Wi-Fi network
- **Scheduler** — runs automatically via Windows Task Scheduler or cron

---

## Screenshots

**Job list** — ranked matches for your profile, with AI scores from both the hiring manager's and your own perspective, salary, location, and quick-action buttons.

![Job list](docs/img/job-list.png)

**Job detail** — full AI assessment, job description, salary range, and application stage tracker in one view.

![Job detail](docs/img/job-detail.png)

**Job database** — every raw job fetched across all sources and profiles, with filtering by source, remote type, and score status.

![Job database](docs/img/all-jobs.png)

**Profile editor** — structured profile with target titles, locations, work style, salary, and experience summary; editable directly in the browser.

![Profile editor](docs/img/profile.png)

---

## ☕ Support This Project

If JobWise has saved you hours of mindlessly scrolling job boards (or at least made the hunt slightly less soul-crushing), consider buying me a coffee. It helps keep the project alive and the caffeine levels stable.

**[☕ Buy Me a Coffee](https://buymeacoffee.com/starbringer)**

No pressure at all — the software is and always will be free. But if you land a great job with it, you know where to find me. 😄

---

## Getting Started

### What you'll need

- **Python 3.11+** — [download here](https://python.org)
- **A free RapidAPI account** — for the JSearch job source
- **An AI provider API key** — [Gemini](https://aistudio.google.com) (free tier available, recommended), [OpenAI](https://platform.openai.com), or [Anthropic](https://console.anthropic.com); **or** run fully locally with [Ollama](https://ollama.com) (no key, no cost)

---

## For Non-Technical Users

The easiest way to get started is the **setup wizard** — see the [Quick Start](#-quick-start) section above. It walks you through every step with instructions on screen and takes about 5 minutes.

**The only prerequisite you need to install yourself is Python:**

1. Download Python 3.11 or later from [python.org](https://python.org)
2. During installation, tick **"Add Python to PATH"** before clicking Install
3. Then double-click **`setup.bat`** (Windows) or run `bash setup.sh` (Mac/Linux)

The wizard handles everything else: packages, API keys (with step-by-step signup instructions), your profile, config files, and optionally the automatic scheduler.

**After setup, daily use:**
- **Windows** — double-click **`start.bat`**
- **Mac/Linux** — run `bash start.sh`

This opens a terminal window running the app. **To stop JobWise**, switch to that terminal and press `Ctrl+C` (or just close the terminal window). Closing the browser tab alone does not stop it.

Then click **Find New Jobs** on your profile to fetch and score the latest postings. The first search can take 10–30 minutes depending on how many jobs are found; repeat searches are much faster since only new postings are scored.

For help using the web UI, see the **[User Guide →](docs/user-guide.md)**

<details>
<summary>Manual setup steps (reference)</summary>

If you prefer to set up manually or something goes wrong with the wizard, here are the individual steps.

### Step 1 — Install Python

Download Python 3.11 or later from [python.org](https://python.org). During installation, check **"Add Python to PATH"**.

### Step 2 — Download the project

Download and unzip this project to any folder on your computer.

### Step 3 — Install dependencies

Open a terminal in the project folder:

```bash
python -m venv venv

# Windows
venv\Scripts\activate

# macOS / Linux
source venv/bin/activate

pip install -r requirements.txt
```

### Step 4 — Get your API keys

**JSearch (job data — free):**
1. Create a free account at [rapidapi.com](https://rapidapi.com)
2. Search for **JSearch** → Subscribe → select **BASIC (Free)** — 200 requests/month
3. Copy your `X-RapidAPI-Key` from the dashboard
4. Note the day-of-month you signed up (e.g. signed up on the 5th → you'll use `5` in config)

**AI provider — pick one:**

| Provider | Cost | Sign up |
|----------|------|---------|
| Google Gemini | Free tier available | [aistudio.google.com](https://aistudio.google.com) |
| OpenAI | Paid | [platform.openai.com](https://platform.openai.com) |
| Anthropic | Paid | [console.anthropic.com](https://console.anthropic.com) |
| Ollama | Free, runs locally | [ollama.com](https://ollama.com) |

### Step 5 — Create your `.env` file

Create a file named `.env` in the project root:

```
JSEARCH_API_KEY=your_rapidapi_key_here
GEMINI_API_KEY=your_gemini_key_here
```

### Step 6 — Configure the app

```bash
# Windows
copy config\config.sample.yaml config\config.yaml

# macOS / Linux
cp config/config.sample.yaml config/config.yaml
```

Open `config/config.yaml` and update:

```yaml
ai:
  provider: gemini

api:
  jsearch_reset_day: 5
```

### Step 7 — Create your profile

Place your resume in the `profiles/` folder (`.txt`, `.md`, `.pdf`, or `.docx`). The filename without the extension becomes your profile name (e.g. `profiles/alice.pdf` → profile name `alice`).

> ⚠️ **Privacy:** Remove personal identifiers (name, phone, address, email) before saving. The AI only needs your skills and preferences.

### Step 8 — Start the web app

```bash
python run_web.py
```

Open **http://localhost:5000**, then click **Find New Jobs** on your profile.

### Step 9 — Set up automatic scheduling (optional)

**Windows (run as Administrator):**
```
scheduler\install-task.bat
```

**macOS / Linux:**
```bash
bash scheduler/install-cron.sh
```

</details>

That's it! For help managing saved jobs, tracking your applications, and getting the most out of the web UI, see the **[User Guide →](docs/user-guide.md)**

---

## For Technical Users

If you're comfortable with Python and want the full picture — pipeline flags, advanced config, scheduler options, adding job sources, and more — see the **[Advanced Guide →](docs/advanced.md)**

---

## Search Sources

| Source | Type | Quota |
|--------|------|-------|
| **Greenhouse** | Company ATS board | Free, unlimited |
| **Lever** | Company ATS board | Free, unlimited |
| **JSearch** | RapidAPI | 200 req/month (free tier) |
| **JobSpy** | Web scraper | No API key needed |

JSearch credits are spent only on companies without a free Greenhouse/Lever board. General keyword searches use JobSpy. See [Legal Notice](#legal-notice) for JobSpy terms.

---

## Project Structure

```
jobwise/
├── setup.bat / setup.sh         # One-click setup wizard (start here!)
├── start.bat / start.sh         # Daily launcher — opens the web app
├── setup_wizard.py              # Setup wizard script (called by setup.bat/sh)
├── config/
│   ├── config.yaml              # All settings
│   └── preferred_companies.txt  # Always-searched companies
├── profiles/
│   └── yourname.txt             # Your career profile (plain text)
├── data/                        # Created automatically on first run
│   └── jobs.db                  # SQLite database
├── src/
│   ├── pipeline.py              # Main pipeline entry point
│   └── sources/                 # Job sources (Greenhouse, Lever, JSearch, JobSpy)
├── web/                         # Flask web app
├── scheduler/                   # Scheduling scripts
├── docs/                        # Extended documentation
│   ├── user-guide.md            # Web UI and application tracking guide
│   └── advanced.md              # Technical reference
├── run_web.py                   # Web app entry point
├── requirements.txt
└── .env                         # API keys (never commit)
```

---

## Legal Notice

This project is licensed under the [Apache License 2.0](LICENSE).

### JobSpy / web scraping

The JobSpy integration scrapes LinkedIn, Indeed, and similar job boards by simulating browser requests. **The Terms of Service of these platforms generally prohibit automated access.**

- This project does not endorse or encourage violating any platform's Terms of Service.
- JobSpy is **opt-out by default** — disable it at any time with `sources.jobspy: false` in `config/config.yaml`.
- **You are solely responsible** for how you use this software.

---

## Contributing

Contributions are welcome. To get started:

1. Fork the repository and create a branch from `master`
2. Make your changes — keep them focused and minimal
3. Test locally against a real profile and database
4. Open a pull request with a clear description of what changed and why

Please open an issue before starting significant changes.

---

## License

[Apache License 2.0](LICENSE) — free to use, modify, and distribute. Attribution required: retain the copyright notice and license file in all copies and derivative works.
