# Advanced Guide — Technical Reference

This guide covers all pipeline flags, configuration options, scheduler setup, and extension points. It assumes you're comfortable with the command line and have already completed the basic setup in the [README](../README.md).

---

## Pipeline Flags

All pipeline commands follow this pattern:

```bash
python src/pipeline.py --profile <name> [flags]
```

### Full run

```bash
# Standard incremental run — fetch new jobs, score them, promote top matches
python src/pipeline.py --profile john
```

`--full-search` is a modifier that can be added to any command below to ignore the incremental date window and re-fetch from all sources as if running for the first time. It does not change which steps run.

### Fetching only

```bash
# Fetch job listings only — no scoring or promotion
python src/pipeline.py --profile john --fetch-jobs

# Same, but force a full re-fetch (ignore last run date)
python src/pipeline.py --profile john --fetch-jobs --full-search

# Fetch descriptions for jobs flagged as "Missing Info" and re-score them
python src/pipeline.py --profile john --fetch-missing
```

### Scoring

```bash
# Score all unscored jobs in the DB (no fetching)
python src/pipeline.py --profile john --score

# Score only the next N unscored jobs
python src/pipeline.py --profile john --score --score-number 50

# Re-score all existing DB jobs with AI using the latest profile (no fetching)
python src/pipeline.py --profile john --rescore

# Re-score only the next N jobs
python src/pipeline.py --profile john --rescore --score-number 50
```

### Filtering and Promotion

```bash
# Re-apply hard requirements to all DB jobs without fetching or AI
python src/pipeline.py --profile john --refilter

# Re-apply promotion logic to already-scored jobs (no AI calls, instant)
python src/pipeline.py --profile john --repromote
```

### Maintenance

```bash
# List all profiles in the DB with their ID and initialization status
python src/pipeline.py --list-profiles

# Wipe all jobs and start fresh (profiles are preserved)
python src/pipeline.py --clear-jobs

# Wipe jobs, scores, and history for a single profile only
python src/pipeline.py --profile john --clear-jobs

# Permanently delete a profile and all its data from the DB
python src/pipeline.py --profile john --delete-profile
```

---

## Configuration Reference (`config/config.yaml`)

### AI Provider

```yaml
ai:
  provider: gemini   # gemini | openai | anthropic | ollama
  gemini:
    model: gemini-2.5-pro-preview-03-25
  openai:
    model: gpt-4o
  anthropic:
    model: claude-haiku-4-5-20251001
  ollama:
    model: llama3
    host: http://localhost
    port: 11434
```

**Using Ollama:** Make sure Ollama is running (`ollama serve`) and the model is pulled (`ollama pull llama3`). No API key needed.

### Pipeline Settings

| Setting | Default | Description |
|---------|---------|-------------|
| `top_n` | `50` | Max new jobs promoted to visible per run |
| `top_n_display` | `50` | Hard cap on the recommended list — all scored jobs are globally re-ranked after each run; saved and active-stage jobs are never hidden |
| `jsearch_queries_per_run` | `10` | JSearch API calls per pipeline run |
| `job_retention_days` | `30` | How long raw jobs are kept in the database |
| `ranker.min_match_score` | `0.4` | Required-skill coverage floor (0–1); jobs below this are never promoted |
| `ranker.batch_size` | `50` | Jobs sent to AI per scoring call |

### Sources

```yaml
sources:
  jobspy: true    # set to false to disable LinkedIn/Indeed scraping
```

### Scheduler

```yaml
scheduler:
  run_times: ["08:00", "18:00"]   # 24h local time; re-run install script after changing
  profiles: ["john"]              # empty list = all profiles
  enabled: false                  # only used by scheduler/scheduler.py daemon mode
```

### API

```yaml
api:
  jsearch_reset_day: 19   # day-of-month your RapidAPI subscription resets
```

---

## Environment Variables

| Variable | Required | Description |
|----------|----------|-------------|
| `JSEARCH_API_KEY` | Yes | Your RapidAPI key for JSearch |
| `GEMINI_API_KEY` | When `provider: gemini` | Google Gemini API key |
| `OPENAI_API_KEY` | When `provider: openai` | OpenAI API key |
| `ANTHROPIC_API_KEY` | When `provider: anthropic` | Anthropic API key |
| `OLLAMA_API_BASE` | No | Override Ollama base URL (alternative to `ollama.host`/`ollama.port`) |

---

## Preferred Companies

Edit `config/preferred_companies.txt` to list companies that are always searched on every run, regardless of other settings:

```
Google
Meta
Stripe
Anthropic
```

---

## Scheduling

The web app (`run_web.py`) and the scheduler are separate processes — run them independently.

### Option 1 — Windows Task Scheduler (recommended)

```bash
# Run as Administrator:
scheduler\install-task.bat
```

Creates one Windows scheduled task per entry in `run_times`. Each task:
- Wakes the PC from sleep at the scheduled time (`WakeToRun`)
- Runs `scheduler/run_scheduled.py` — fetches and scores jobs for all profiles, then exits
- No background daemon left running between jobs

Re-run `install-task.bat` after changing `run_times` in `config.yaml`.

```bash
# Uninstall:
scheduler\uninstall-task.bat

# Trigger a run immediately:
schtasks /run /tn "JobWise_0800"

# Check task status:
schtasks /query /tn "JobWise*"
```

### Option 2 — cron (macOS / Linux)

```bash
bash scheduler/install-cron.sh

# Uninstall:
bash scheduler/uninstall-cron.sh
```

Reads `run_times` from `config/config.yaml` and installs one cron entry per time. Re-running is safe — existing entries are replaced.

> **macOS note:** cron does not wake the machine from sleep. Enable **Power Nap** in System Settings → Battery to keep cron running with the lid closed. On Linux the machine must be awake at run time.

### Option 3 — Long-running daemon

For a persistent background process that waits for configured times and re-runs indefinitely:

```bash
python scheduler/scheduler.py
```

---

## Logs

| File | Contents |
|------|----------|
| `logs/scheduler.log` | Full output of every scheduled run — sources, jobs found, filter decisions, AI results, promotion summary. Rotated nightly, 7 days kept. |
| `logs/pipeline.log` | Same information, written by the pipeline itself |

When using Ollama, every AI call logs a throughput line:

```
Ollama (llama3): 1240 in / 312 out tokens, 8.3s, 37.6 tok/s
```

---

## JSearch Quota Management

The free tier gives **200 requests/month**. Each pipeline run uses up to `jsearch_queries_per_run` credits (default 10).

| Frequency | Runs/month | Credits used | Status |
|-----------|------------|--------------|--------|
| 3–4×/week | ~13–17 | 130–170 | ✓ Safe |
| 5×/week | ~22 | 220 | Reduce `jsearch_queries_per_run` to 9 |

Quota resets on the day-of-month you signed up (`api.jsearch_reset_day`).

JSearch credits are spent **only on companies without a free Greenhouse/Lever board**. General keyword discovery uses JobSpy (no quota).

---

## Adding a New Job Source

Sources live in [src/sources/](../src/sources/). Each source:

1. Extends `BaseSource` from [src/sources/base.py](../src/sources/base.py)
2. Implements `fetch(profile, queries)` returning a list of job dicts
3. Is registered and toggled via `config/config.yaml` under `sources`

Follow the existing source implementations as reference.

---

## Profile File Formats

Profile files live in `profiles/` and can be in any of the following formats:

| Format | Extension | Notes |
|--------|-----------|-------|
| Plain text | `.txt` | Simplest option |
| Markdown | `.md` | Rendered as plain text for AI parsing |
| PDF | `.pdf` | Text is extracted automatically |
| Word document | `.docx` | Text is extracted automatically |

The file is only read on the first pipeline run (or when you sync). After that, the profile lives in the database. If you have multiple files for the same profile name (e.g. `john.txt` and `john.pdf`), the first match wins in this order: `.txt` → `.md` → `.pdf` → `.docx`.

## Profile Sync from File

After the first pipeline run, the profile lives in the database and is managed from the web UI at `/profile/<name>/structured`.

To pull changes from your profile file back into the database, use the **Sync from file** button on the profile page. Sync is additive — it only adds items missing from the database profile and never removes anything. Deletions must be done manually in the UI.

---

## Web UI Themes

The web UI supports multiple color themes switchable via the navbar: **Pine** (default), Black, Forest, Midnight, Ocean, Sunset, Lavender, Linen, and Fjord. Themes affect only the palette — layout and card structure are the same across all themes.

---

## Job Database View

The global database view (`/database`) shows every raw job across all profiles. Each profile tag on a job shows:

- **Hard/title filter rejections** — muted chip with truncated rejection reason; hover for full reason
- **AI-scored jobs** — `MgrScore·CandScore` plus match% (required-skill coverage); hover for promotion details
- **Fetched-for profile** — jobs with no scored entry show a dotted chip for the profile that discovered them
- **Stale post date warning** — when `date_posted` is outside `job_retention_days`, explains why the ranker skipped it
