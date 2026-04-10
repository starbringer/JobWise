# User Guide — Managing Jobs and Tracking Applications

This guide covers everything you need to use the web UI day-to-day: browsing matched jobs, saving the ones you like, tracking your application progress, and keeping your profile up to date.

---

## Opening the Web App

**Easiest way:**
- **Windows** — double-click **`start.bat`** in the project folder
- **Mac/Linux** — run `bash start.sh` in the project folder

Your browser opens automatically at **http://localhost:5000**.

Or start it manually from a terminal (with the virtual environment active):

```bash
python run_web.py
```

---

## Finding New Jobs

Click the **Find New Jobs** button on any profile — either on the home page or inside the profile's job list — to search for and score the latest job postings.

**What happens when you click it:**

1. JobWise searches Greenhouse, Lever, JSearch, and Indeed/LinkedIn for open roles matching your profile
2. New postings are saved to the database (already-seen jobs are skipped)
3. Each new job is scored by the AI against your profile from two perspectives: how a hiring manager would see your fit, and how well the role meets your preferences
4. Qualified jobs are added to your list, ranked by score

The first search takes **2–5 minutes** depending on your AI provider. Repeat searches are faster because most jobs are already in the database.

**Status bar:**

While a search is running, a bar appears at the top of every page:

```
[━━━━━━━━━━━━━━━░░░░░░░]  Finding new jobs for alice…  12s
```

You can freely navigate to other pages — the search keeps running in the background. When it finishes, the bar updates to show a summary:

```
✓  Last search: 2:34 PM (alice) — 47 fetched · 12 new · 6 added to your list
```

- **fetched** — total job postings seen across all sources
- **new** — postings not previously in your database (deduplicated)
- **added to your list** — new jobs that passed hard filters and scored well enough to appear in your job list

Dismiss the summary bar with the **×** button. Only one search can run at a time — if you try to start a second, you'll see a message telling you to wait.

---

## Dashboard

The home page shows all your profiles with a count of matched jobs and active applications. Click a profile name to go to its job list.

---

## Browsing Jobs

The job list shows all jobs matched to your profile, sorted by score by default.

**Filtering and sorting:**
- Use the tabs at the top to filter by status: All, New, Saved, Applied, and so on
- Sort by **Score** (AI match score), **Date** (date posted), **Added** (when the job was added to your list), or **Company** using the sort controls

**Each job card shows:**
- Job title and company
- Two circular score meters — one from the hiring manager's perspective, one from yours
- A short AI summary of why the job is or isn't a good fit
- The source and date the job was added (e.g. `GH @ 4/5/2026 1:00PM`)
- **Read Full Description** — opens the full job posting inline without leaving the page

---

## Saving Jobs

Click the **☆** bookmark icon on any job card to save it. Saved jobs:
- Appear in the **Saved** tab for quick access
- Are never removed by the 30-day retention window or profile updates
- Remain visible regardless of their application stage

Click **★** again to unsave.

---

## Tracking Your Applications

The save flag and application stage are independent — you can save a job and move it through stages at the same time.

**Application stages:**

```
new → applied → phone_screen → interview_1 → interview_2 → interview_3 → offer
           ↘                                                            ↘
            rejected (from any stage)                               withdrawn
```

Update the stage from the job card or the full job detail page. Jobs in any active stage (applied and beyond) are protected and will never be hidden or cleared automatically.

---

## Job Detail Page

Click a job title to open its full detail page. Here you'll find:

- Full job description
- Complete AI analysis from both perspectives
- Score breakdown
- Status controls — update stage, add notes
- Direct link to the original job posting

---

## Adding Notes

On the job detail page, there's a notes field where you can record anything useful: recruiter contact, interview prep notes, salary discussed, next steps, etc.

---

## Updating Your Profile

Your profile is managed from the web UI at `/profile/yourname/structured`.

To make changes:
- Edit the fields directly in the browser and save
- Or update your `profiles/yourname.txt` file and click **Sync from file** to pull the changes in

> **Note:** Sync is additive — it only adds fields that are missing from the database profile and never removes anything. If you want to delete a skill or preference, do it manually in the UI.

After updating your profile, you can re-run the pipeline to re-score existing jobs against your new profile:

```bash
python src/pipeline.py --profile yourname --rescore
```

---

## Stats Page

Go to `/profile/yourname/stats` to see:
- Application status breakdown
- Job source breakdown (how many came from each source)
- Pipeline run history

---

## Job Database (Global View)

The **Job Database** link in the navbar shows every raw job in the database across all profiles. Useful for:
- Searching for a specific company or title across all your profiles
- Seeing why a job was filtered out or not promoted
- Checking how many jobs came from each source

Each job shows profile tags indicating how it was processed for each of your profiles.

---

## Tips

- **Search regularly** — the scheduler does this automatically twice a day, but you can always click **Find New Jobs** any time you want fresh results
- **Save jobs early** — saved jobs are protected and won't disappear
- **Use the notes field** — it's easy to lose track of where you are with 20+ applications
- **Check the stats page** — it tells you at a glance how active your pipeline has been
