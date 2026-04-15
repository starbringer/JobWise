"""
JobWise Setup Wizard
────────────────────
Runs with plain system Python — no venv or third-party packages needed.

What this wizard does:
  1. Checks your Python version
  2. Creates a virtual environment and installs packages (with progress bar)
  3. Walks you through getting AI and job-data API keys
  4. Creates your profile from a resume file (or a blank placeholder)
  5. Writes .env and config/config.yaml so you don't have to edit files manually
  6. Optionally sets up the automatic scheduler
  7. Launches the web app in your browser
"""

import os
import re
import shutil
import subprocess
import sys
import platform
import time
import threading
import webbrowser
import textwrap
from pathlib import Path

# ── Colour / Unicode helpers ─────────────────────────────────────────────────
IS_WINDOWS = platform.system() == "Windows"

def _enable_win_vt100():
    """Enable VT-100 escape codes on Windows 10+."""
    try:
        import ctypes
        k = ctypes.windll.kernel32
        k.SetConsoleMode(k.GetStdHandle(-11), 7)
        return True
    except Exception:
        return False

USE_COLOUR = _enable_win_vt100() if IS_WINDOWS else (
    hasattr(sys.stdout, "isatty") and sys.stdout.isatty()
)

# Detect whether the terminal can render Unicode block characters
_enc = (sys.stdout.encoding or "").lower().replace("-", "")
USE_UNICODE = _enc in ("utf8", "utf-8", "cp65001") or (not IS_WINDOWS)

FULL_BLOCK = "█" if USE_UNICODE else "#"
LITE_BLOCK = "░" if USE_UNICODE else "-"
SPINNER_FRAMES = (
    ["⠋", "⠙", "⠹", "⠸", "⠼", "⠴", "⠦", "⠧", "⠇", "⠏"] if USE_UNICODE
    else ["|", "/", "-", "\\"]
)

RESET = "\033[0m"
BOLD  = "\033[1m"
DIM   = "\033[2m"
GREEN = "\033[92m"
YELLOW= "\033[93m"
CYAN  = "\033[96m"
RED   = "\033[91m"

def c(code, text):
    return f"{code}{text}{RESET}" if USE_COLOUR else text

# ── UI helpers ────────────────────────────────────────────────────────────────
def clear():
    os.system("cls" if IS_WINDOWS else "clear")

def section(n, title):
    print(f"\n{c(BOLD+CYAN, f'── Step {n}:')} {c(BOLD, title)}")
    print(c(DIM, "   " + "─" * 50))

def ok(msg):   print(f"   {c(GREEN, '✓')} {msg}")
def warn(msg): print(f"   {c(YELLOW, '⚠')}  {msg}")
def err(msg):  print(f"   {c(RED, '✗')} {msg}")
def info(msg): print(f"   {c(DIM, msg)}")
def blank():   print()

def banner(lines):
    width = 58
    print(c(CYAN, "─" * width))
    for line in lines:
        print(f"  {line}")
    print(c(CYAN, "─" * width))

def ask(prompt, default=None):
    suffix = f"  [{c(DIM, str(default))}]" if default else ""
    try:
        val = input(f"\n   {c(BOLD, '→')} {prompt}{suffix}\n     ").strip()
    except (EOFError, KeyboardInterrupt):
        raise KeyboardInterrupt
    return val if val else default

def ask_yn(prompt, default=True):
    hint = c(DIM, "[Y/n]") if default else c(DIM, "[y/N]")
    try:
        raw = input(f"\n   {c(BOLD, '→')} {prompt} {hint}: ").strip().lower()
    except (EOFError, KeyboardInterrupt):
        raise KeyboardInterrupt
    if not raw:
        return default
    return raw in ("y", "yes")

def ask_menu(prompt, choices, default_key=None):
    """
    choices: list of (key, label) tuples
    Returns the chosen key.
    """
    blank()
    print(f"   {c(BOLD, '→')} {prompt}")
    for i, (key, label) in enumerate(choices, 1):
        marker = c(CYAN, "►") if key == default_key else " "
        print(f"     {marker} {c(BOLD, str(i))}) {label}")
    default_n = next((i for i, (k, _) in enumerate(choices, 1) if k == default_key), None)
    hint = f"  [{c(DIM, str(default_n))}]" if default_n else ""
    while True:
        try:
            raw = input(f"\n     Enter number{hint}: ").strip()
        except (EOFError, KeyboardInterrupt):
            raise KeyboardInterrupt
        if not raw and default_n:
            return choices[default_n - 1][0]
        try:
            idx = int(raw) - 1
            if 0 <= idx < len(choices):
                return choices[idx][0]
        except ValueError:
            pass
        warn("Please enter a number from the list.")

# ── Progress bar (animated, runs while pip installs) ─────────────────────────
def _draw_bar(done, total, label, width=28):
    """Overwrite current line with an animated progress bar."""
    filled = int(width * min(done, total) / max(total, 1))
    bar = FULL_BLOCK * filled + LITE_BLOCK * (width - filled)
    pct = int(100 * min(done, total) / max(total, 1))
    print(f"\r   [{bar}] {pct:3d}%  {label:<35}", end="", flush=True)

def install_packages_with_progress():
    """
    Install requirements.txt into the venv with a live progress bar.
    Streams pip stdout line-by-line and ticks the bar on each 'Collecting' line.
    """
    req = ROOT / "requirements.txt"
    if not req.exists():
        warn("requirements.txt not found — skipping package installation.")
        return

    # Count installable lines in requirements.txt (non-blank, non-comment)
    req_lines = [
        l.strip() for l in req.read_text().splitlines()
        if l.strip() and not l.strip().startswith("#")
    ]
    total_pkgs = max(len(req_lines), 1)

    state = {"done": False, "error": None, "count": 0, "current": ""}

    def _pip_thread():
        # Silently upgrade pip first
        subprocess.run(
            [str(VENV_PYTHON), "-m", "pip", "install", "--quiet", "--upgrade", "pip"],
            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
        )
        # Now install requirements with streaming output
        proc = subprocess.Popen(
            [
                str(VENV_PYTHON), "-m", "pip", "install",
                "--no-cache-dir", "--progress-bar", "off",
                "-r", str(req),
            ],
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            encoding="utf-8",
            errors="replace",
            cwd=ROOT,
        )
        for line in proc.stdout:
            line = line.strip()
            if line.startswith("Collecting "):
                pkg = line.split()[1].split("[")[0]
                state["count"] += 1
                state["current"] = pkg
            elif line.startswith("Downloading ") or line.startswith("Installing "):
                # minor progress nudge for large downloads
                pass
        proc.wait()
        if proc.returncode != 0:
            state["error"] = f"pip exited with code {proc.returncode}"
        state["done"] = True

    t = threading.Thread(target=_pip_thread, daemon=True)
    t.start()

    frame_idx = 0
    while not state["done"]:
        frame = SPINNER_FRAMES[frame_idx % len(SPINNER_FRAMES)]
        done  = state["count"]
        label = f"{frame} {state['current'] or 'starting…'}"
        _draw_bar(done, total_pkgs, label)
        time.sleep(0.12)
        frame_idx += 1

    # Final bar — 100% done
    if state["error"]:
        print()  # newline after \r
        err(f"Package installation failed: {state['error']}")
        info("Try running manually: pip install -r requirements.txt")
        sys.exit(1)
    else:
        print(f"\r   [{FULL_BLOCK * 28}] 100%  {c(GREEN, 'All packages installed')}          ")


# ── Paths ─────────────────────────────────────────────────────────────────────
ROOT         = Path(__file__).parent.resolve()
VENV         = ROOT / "venv"
VENV_PYTHON  = VENV / ("Scripts/python.exe" if IS_WINDOWS else "bin/python")
VENV_PIP     = VENV / ("Scripts/pip.exe"    if IS_WINDOWS else "bin/pip")
DOTENV       = ROOT / ".env"
CONFIG       = ROOT / "config" / "config.yaml"
SAMPLE       = ROOT / "config" / "config.sample.yaml"
PROFILES_DIR = ROOT / "profiles"


# ── Config / env helpers ──────────────────────────────────────────────────────
def create_venv():
    if VENV_PYTHON.exists():
        ok("Virtual environment already exists — reusing it.")
        return
    print("   Creating virtual environment…", end="", flush=True)
    result = subprocess.run(
        [sys.executable, "-m", "venv", str(VENV)],
        stdout=subprocess.PIPE, stderr=subprocess.PIPE,
    )
    if result.returncode != 0:
        print()
        err("Could not create virtual environment.")
        err(result.stderr.decode(errors="replace"))
        sys.exit(1)
    print(f" {c(GREEN, 'done')}")


def write_dotenv(keys: dict):
    """Merge keys into .env, preserving any existing keys not being overwritten."""
    existing: dict = {}
    if DOTENV.exists():
        for line in DOTENV.read_text(encoding="utf-8").splitlines():
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                k, _, v = line.partition("=")
                existing[k.strip()] = v.strip()
    existing.update({k: v for k, v in keys.items() if v})
    with DOTENV.open("w", encoding="utf-8") as f:
        for k, v in existing.items():
            if v:
                f.write(f"{k}={v}\n")
    ok(f".env saved  ({', '.join(k for k in keys if keys[k])})")


def write_config(provider: str, jsearch_reset_day: int, profile_name: str):
    if not SAMPLE.exists():
        warn("config.sample.yaml not found — skipping config write.")
        return
    text = SAMPLE.read_text(encoding="utf-8")
    # Update provider
    text = re.sub(
        r'^(\s*provider:\s*)"[^"]*"',
        lambda m: f'{m.group(1)}"{provider}"',
        text, flags=re.MULTILINE,
    )
    # Update jsearch_reset_day
    text = re.sub(
        r'^(\s*jsearch_reset_day:\s*)\d+',
        lambda m: f"{m.group(1)}{jsearch_reset_day}",
        text, flags=re.MULTILINE,
    )
    # Set scheduler profiles list
    text = re.sub(
        r'^(\s*profiles:\s*)\[\]',
        lambda m: f'{m.group(1)}["{profile_name}"]',
        text, flags=re.MULTILINE,
    )
    CONFIG.write_text(text, encoding="utf-8")
    ok(f"config/config.yaml written  (provider: {provider})")


# ── Wizard steps ──────────────────────────────────────────────────────────────

def step1_python():
    section(1, "Checking Python version")
    vi = sys.version_info
    if vi < (3, 11):
        err(f"Python 3.11 or later is required. You have {vi.major}.{vi.minor}.{vi.micro}.")
        blank()
        print(textwrap.dedent("""\
            Download the latest Python from:
              https://www.python.org/downloads/

            During installation, tick the box that says "Add Python to PATH"
            before clicking Install. Then run setup again.
        """))
        sys.exit(1)
    ok(f"Python {vi.major}.{vi.minor}.{vi.micro}")


def step2_packages():
    section(2, "Setting up environment & installing packages")
    info("This step only runs once and usually takes about a minute.")
    blank()
    create_venv()
    install_packages_with_progress()


def step3_ai_provider():
    section(3, "Choose your AI provider")
    info("The AI reads your profile and scores each job for relevance.")
    blank()

    provider = ask_menu(
        "Which AI provider do you want to use?",
        [
            ("claude_cli", "claude.ai  — free with a claude.ai Pro or Max subscription, no API key needed"),
            ("gemini",     "Google Gemini  — free tier available, recommended for most users"),
            ("openai",     "OpenAI GPT-4o  — paid, requires a credit card"),
            ("anthropic",  "Anthropic Claude API  — paid, requires a credit card"),
            ("ollama",     "Ollama  — completely free, runs on your computer (no internet needed)"),
        ],
        default_key="gemini",
    )

    env_keys = {}

    if provider == "claude_cli":
        blank()
        print(c(BOLD, "   Using claude.ai — no API key needed!"))
        blank()
        print("   You'll need Claude Code installed and signed in.")
        print("   Here's how to set it up:")
        blank()
        print(c(BOLD, "   Step 1 — Install Claude Code:"))
        print("     Go to:  https://claude.ai/code")
        print("     Download and install it for your platform (Windows or Mac).")
        blank()
        print(c(BOLD, "   Step 2 — Sign in:"))
        print("     Open a terminal (Command Prompt on Windows, Terminal on Mac) and run:")
        print(c(CYAN, "       claude login"))
        print("     A browser window will open — sign in with your claude.ai account.")
        print("     You only need to do this once.")
        blank()
        print(c(BOLD, "   Step 3 — Verify it works:"))
        print("     In the same terminal, run:")
        print(c(CYAN, '       claude -p "Say hello"'))
        print("     If you see a response, you're all set.")
        blank()
        input("   Press Enter once you've completed the steps above (or to skip for now): ")
        ok("claude.ai selected — no API key needed.")

    elif provider == "gemini":
        blank()
        print(c(BOLD, "   How to get a FREE Gemini API key (takes ~2 minutes):"))
        print("     1. Go to:  https://aistudio.google.com")
        print("     2. Sign in with any Google account")
        print('     3. Click "Get API key" → "Create API key in new project"')
        print('     4. Copy the key shown — it starts with "AIza..."')
        blank()
        key = ask('Paste your Gemini API key here\n     (or press Enter to add it later):')
        if key:
            env_keys["GEMINI_API_KEY"] = key
            ok("Gemini API key saved.")
        else:
            warn("Skipped. Add  GEMINI_API_KEY=your_key  to the .env file later.")

    elif provider == "openai":
        blank()
        print(c(BOLD, "   How to get an OpenAI API key:"))
        print("     1. Go to:  https://platform.openai.com")
        print('     2. Sign in → click your profile icon (top-right) → "API keys"')
        print('     3. Click "Create new secret key" — it starts with "sk-..."')
        print("     Note: You must add a payment method before the key will work.")
        blank()
        key = ask("Paste your OpenAI API key (or press Enter to add it later):")
        if key:
            env_keys["OPENAI_API_KEY"] = key
            ok("OpenAI API key saved.")
        else:
            warn("Skipped. Add  OPENAI_API_KEY=your_key  to the .env file later.")

    elif provider == "anthropic":
        blank()
        print(c(BOLD, "   How to get an Anthropic API key:"))
        print("     1. Go to:  https://console.anthropic.com")
        print('     2. Sign in → click "API Keys" in the left sidebar')
        print('     3. Click "Create Key" — it starts with "sk-ant-..."')
        print("     Note: You must add a payment method before the key will work.")
        blank()
        key = ask("Paste your Anthropic API key (or press Enter to add it later):")
        if key:
            env_keys["ANTHROPIC_API_KEY"] = key
            ok("Anthropic API key saved.")
        else:
            warn("Skipped. Add  ANTHROPIC_API_KEY=your_key  to the .env file later.")

    elif provider == "ollama":
        blank()
        print(c(BOLD, "   How to set up Ollama (free, no account needed):"))
        print("     1. Download and install from:  https://ollama.com")
        print("     2. Open a terminal and run:    ollama pull llama3")
        print("        (Downloads the AI model — about 4 GB, takes a few minutes)")
        print("     3. Leave Ollama running in the background.")
        blank()
        input("   Press Enter once Ollama is installed and running: ")
        ok("Ollama selected — no API key needed.")

    return provider, env_keys


def step4_jsearch():
    section(4, "Job data API key  (JSearch via RapidAPI)")
    info("JSearch gives you jobs from LinkedIn, Indeed, and 20+ boards via a free API.")
    info("Free tier: 200 requests/month — plenty for several runs per week.")
    blank()
    print(c(BOLD, "   How to get your free JSearch key (takes ~3 minutes):"))
    print("     1. Go to:  https://rapidapi.com  and create a free account")
    print('     2. In the search bar at the top, search for:  JSearch')
    print('     3. Click on JSearch → click the blue "Subscribe to Test" button')
    print('     4. Select the BASIC (Free) plan → click Subscribe')
    print('     5. Click the "Endpoints" tab')
    print('     6. Your key appears on the right under "X-RapidAPI-Key" — copy it')
    blank()
    key = ask(
        "Paste your JSearch (RapidAPI) key here\n"
        "     (or press Enter to skip — you'll get fewer job sources):"
    )

    reset_day = 1
    if key:
        ok("JSearch API key saved.")
        raw = ask(
            "What day of the month did you create your RapidAPI account?\n"
            "     (e.g. type 15 if you signed up on the 15th — this tracks your monthly quota):",
            default="1",
        )
        try:
            reset_day = max(1, min(28, int(raw)))
        except (ValueError, TypeError):
            reset_day = 1
    else:
        warn("Skipped. You can add  JSEARCH_API_KEY=your_key  to the .env file later.")

    return {"JSEARCH_API_KEY": key} if key else {}, reset_day


def step5_profile_name():
    section(5, "Name your profile")
    info("Your profile is what the AI reads to understand your background and what job you want.")
    info("If you have multiple job searches (e.g. two different roles), you can create")
    info("separate profiles for each. For now, one profile is all you need.")
    blank()
    raw = ask(
        "Enter a short name for your profile  (e.g. your first name, like  alice):",
        default="me",
    )
    # Sanitise: letters, numbers, hyphens, underscores only
    name = re.sub(r"[^a-zA-Z0-9_-]", "_", raw or "me").strip("_").lower() or "me"
    if name != raw:
        info(f"Profile name sanitised to: {name}")
    return name


def step6_profile_file(profile_name: str) -> tuple:
    """Returns (profile_path, is_real_file).

    is_real_file=True  → real content was provided (PDF, docx, txt, md).
    is_real_file=False → blank placeholder was created; no extraction makes sense yet.
    """
    section(6, "Set up your profile")
    blank()
    print(textwrap.dedent("""\
        The easiest way to get started is to use your resume.
        Supported formats: PDF (.pdf), Word (.docx), or plain text (.txt or .md)

        Privacy note:
          Your resume text is sent to the AI provider you chose to score jobs.
          Before using it, remove personal details the AI doesn't need:
            your full name, phone number, home address, email, government ID numbers.
          Skills, experience, and job preferences are all the AI needs.
    """))

    PROFILES_DIR.mkdir(exist_ok=True)

    # Check if a profile file already exists
    existing = sorted(PROFILES_DIR.glob(f"{profile_name}.*"))
    if existing:
        ok(f"Profile '{profile_name}' already exists ({existing[0].name}) — keeping it.")
        return existing[0], True

    choice = ask_menu(
        "How would you like to set up your profile?",
        [
            ("file",    "I have a resume file — I'll enter the path below"),
            ("blank",   "Create a blank profile — I'll fill it in later via the web UI"),
        ],
        default_key="file",
    )

    if choice == "file":
        return _collect_resume_path(profile_name), True
    else:
        return _create_blank_profile(profile_name), False


def _collect_resume_path(profile_name: str, attempts: int = 3) -> Path:
    blank()
    if IS_WINDOWS:
        print("   Tip: Open File Explorer, right-click your resume file,")
        print('        choose "Copy as path", then paste it here.')
    else:
        print("   Tip: Drag your resume file from Finder/Files into this")
        print("        terminal window — it will paste the path automatically.")
    blank()

    for attempt in range(attempts):
        raw = ask("Full path to your resume file:")
        if not raw:
            warn("No path entered.")
        else:
            # Strip surrounding quotes (Windows "Copy as path" adds them)
            cleaned = raw.strip('"').strip("'").strip()
            path = Path(cleaned)
            if not path.exists():
                err(f"File not found: {cleaned}")
                if attempt < attempts - 1:
                    info("Please check the path and try again.")
            elif path.suffix.lower() not in (".pdf", ".docx", ".txt", ".md"):
                err(f"Unsupported file type: {path.suffix}")
                info("Please use a .pdf, .docx, .txt, or .md file.")
            else:
                dest = PROFILES_DIR / f"{profile_name}{path.suffix.lower()}"
                try:
                    shutil.copy2(path, dest)
                except PermissionError:
                    home = Path.home()
                    err(f"Permission denied reading: {cleaned}")
                    info("The file is in a folder this script cannot read.")
                    info(f"Move it directly into your home folder:  {home}")
                    info("Then paste the updated path and try again.")
                    if attempt < attempts - 1:
                        info("Please try again with the updated path.")
                    continue
                ok(f"Resume copied → profiles/{dest.name}")
                return dest

    warn("Could not use the provided file path. Creating a blank profile instead.")
    return _create_blank_profile(profile_name)


def _create_blank_profile(profile_name: str) -> Path:
    dest = PROFILES_DIR / f"{profile_name}.txt"
    dest.write_text(
        "# JobWise Profile\n"
        "# Fill in your details below, then click 'Save' in the web UI.\n"
        "# The more detail you provide, the better the AI can score jobs for you.\n\n"
        "I am looking for a [ROLE] position.\n\n"
        "Skills: [list your key skills, e.g. Python, SQL, project management]\n\n"
        "Experience: [brief summary of your background]\n\n"
        "Location: [your city / country]\n"
        "Work style: [remote / hybrid / on-site]\n\n"
        "Salary expectation: [e.g. $80,000+]\n\n"
        "Preferences:\n"
        "- I prefer [e.g. startups / large companies]\n"
        "- I do not want [e.g. agencies, fully on-site roles]\n",
        encoding="utf-8",
    )
    blank()
    print(c(YELLOW + BOLD, "   ⚠  Important:"))
    print("      Without a completed profile, the job search cannot score jobs for you.")
    print("      After setup, go to the web UI → Profile tab and fill in your details.")
    blank()
    ok(f"Blank profile created → profiles/{dest.name}")
    return dest


def _check_ai_ready_wizard(provider: str, env_keys: dict) -> bool:
    """Return True if the chosen AI provider has credentials ready for use."""
    if provider in ("claude_cli", "ollama"):
        return True
    key_map = {
        "gemini":    "GEMINI_API_KEY",
        "openai":    "OPENAI_API_KEY",
        "anthropic": "ANTHROPIC_API_KEY",
    }
    env_var = key_map.get(provider)
    if not env_var:
        return True
    # Key was just supplied in this wizard session
    if env_keys.get(env_var):
        return True
    # Key may exist in .env from a previous run
    if DOTENV.exists():
        for line in DOTENV.read_text(encoding="utf-8").splitlines():
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                k, _, v = line.partition("=")
                if k.strip() == env_var and v.strip():
                    return True
    return False


def _build_extract_script(profile_name: str, profile_file: Path) -> str:
    """Build a self-contained Python script to run in the venv for profile extraction."""
    root_r   = repr(str(ROOT))
    config_r = repr(str(CONFIG))
    name_r   = repr(profile_name)
    file_r   = repr(str(profile_file))
    return "\n".join([
        "import sys",
        "sys.path.insert(0, " + root_r + ")",
        "from pathlib import Path",
        "import yaml",
        "from src import database, profile_processor",
        "config_path = Path(" + config_r + ")",
        "config = yaml.safe_load(open(config_path, encoding='utf-8')) if config_path.exists() else {}",
        "config = config or {}",
        "db_path = Path(" + root_r + ") / (config.get('database') or {}).get('path', 'data/jobs.db')",
        "profiles_dir = Path(" + root_r + ") / config.get('profiles_dir', 'profiles')",
        "db_path.parent.mkdir(parents=True, exist_ok=True)",
        "conn = database.init_db(db_path)",
        "database.upsert_profile(conn, " + name_r + ", " + file_r + ")",
        "profile_processor.process(conn, " + name_r + ", profiles_dir)",
        "conn.close()",
        "print('__DONE__')",
    ])


def _run_extract_with_spinner(profile_name: str, profile_file: Path) -> bool:
    """Run AI profile extraction inside the venv with an animated spinner.

    Returns True on success, False on failure.
    """
    script = _build_extract_script(profile_name, profile_file)
    state: dict = {"done": False, "error": None}

    def _bg():
        try:
            proc = subprocess.run(
                [str(VENV_PYTHON), "-c", script],
                capture_output=True, text=True,
                encoding="utf-8", errors="replace",
                cwd=ROOT,
            )
            if proc.returncode != 0:
                lines = (proc.stderr or proc.stdout or "unknown error").strip().splitlines()
                state["error"] = lines[-1] if lines else "unknown error"
            elif "__DONE__" not in proc.stdout:
                state["error"] = "Extraction did not complete"
        except Exception as exc:
            state["error"] = str(exc)
        state["done"] = True

    t = threading.Thread(target=_bg, daemon=True)
    t.start()

    frame_idx = 0
    while not state["done"]:
        frame = SPINNER_FRAMES[frame_idx % len(SPINNER_FRAMES)]
        print(
            f"\r   {frame}  Reading your profile \u2014 building a picture of what you\u2019re looking for\u2026",
            end="", flush=True,
        )
        time.sleep(0.12)
        frame_idx += 1
    print()  # newline after spinner

    if state["error"]:
        err(f"Profile import failed: {state['error'][:200]}")
        return False
    ok("Profile imported \u2014 your profile is ready.")
    return True


def step8_import_profile(profile_name: str, profile_file: Path, is_real_file: bool,
                          provider: str, env_keys: dict) -> bool:
    """Offer to import the profile into the database with AI extraction.

    Returns True if the profile was successfully imported.
    """
    if not is_real_file:
        return False  # blank placeholder — nothing meaningful to extract yet

    section(8, "Import your profile")
    blank()
    print("   The AI will read your profile file and extract the key details")
    print("   (target roles, location, skills, salary preferences).")
    print("   This takes about 30\u201360 seconds and only needs to happen once.")
    blank()

    if not _check_ai_ready_wizard(provider, env_keys):
        warn("Your AI provider isn\u2019t configured with an API key yet.")
        info("You can set up the key later from the Settings tab in the web UI,")
        info("then load your profile from the Profiles page.")
        return False

    want = ask_yn("Import your profile now?", default=True)
    if not want:
        blank()
        info("No problem. When you open the web UI, use the \u2018Load Profile File\u2019")
        info("form on the Profiles page to import your profile.")
        return False

    blank()
    success = _run_extract_with_spinner(profile_name, profile_file)
    if not success:
        blank()
        info("You can try again from the web UI \u2014 use the \u2018Load Profile File\u2019")
        info("form on the Profiles page and select your file.")
    return success


def step7_write_config(provider: str, env_keys: dict, jsearch_day: int, profile_name: str):
    section(7, "Writing configuration files")
    write_dotenv(env_keys)
    write_config(provider, jsearch_day, profile_name)


def step9_scheduler(profile_name: str) -> bool:
    """Returns True if user wants scheduler but still needs to install it manually."""
    section(9, "Automatic scheduling  (optional)")
    blank()
    print(c(BOLD, "   Two ways to get fresh jobs:"))
    blank()
    print("   Without scheduler  (totally fine):")
    print("     Open the app, click 'Find New Jobs', and wait a few minutes.")
    print("     Simple, works great, just requires you to remember to do it.")
    blank()
    print("   With scheduler  (a bit more setup, zero effort after that):")
    print("     The app fetches new jobs automatically at 8am and 6pm every day.")
    print("     By the time you open the app, fresh jobs are already waiting for you.")
    blank()

    want = ask_yn("Set up the automatic scheduler?", default=False)

    if not want:
        blank()
        info("No problem — just click 'Find New Jobs' in the app whenever you want fresh results.")
        blank()
        info("Changed your mind later? You can set up the scheduler at any time:")
        if IS_WINDOWS:
            info("  Right-click  scheduler\\install-task.bat  → 'Run as administrator'")
        else:
            info("  Run:  bash scheduler/install-cron.sh")
        return False

    if IS_WINDOWS:
        blank()
        print(c(BOLD, "   The scheduler requires Administrator permission."))
        print("   Here's how to run it after this wizard finishes:")
        blank()
        print("     1. Open File Explorer and go to this project's folder")
        print('     2. Open the  scheduler  folder inside it')
        print('     3. Right-click  install-task.bat')
        print('     4. Click "Run as administrator"')
        print('     5. Click "Yes" if Windows asks for permission')
        blank()
        print("   We'll remind you of this at the end.")
        return True  # pending — user must do it manually
    else:
        # Mac/Linux: user-level crontab, no sudo needed
        sh = ROOT / "scheduler" / "install-cron.sh"
        if sh.exists():
            blank()
            info("Installing cron jobs…")
            result = subprocess.run(["bash", str(sh)], cwd=ROOT)
            if result.returncode == 0:
                ok("Scheduler installed.")
                if platform.system() == "Darwin":
                    blank()
                    info("macOS tip: Enable 'Power Nap' in System Settings → Battery")
                    info("so the job search can run even while your Mac is sleeping.")
            else:
                warn("Scheduler installation returned an error.")
                warn("You can try again later with:  bash scheduler/install-cron.sh")
        else:
            warn("scheduler/install-cron.sh not found — skipping.")
        return False


def step9_launch(provider, profile_name, profile_file, scheduler_pending):
    blank()
    summary_lines = [
        c(BOLD + GREEN, "Setup complete!"),
        "",
        f"  Profile:   {profile_name}  ({profile_file.name})",
        f"  Provider:  {provider}",
        "",
        f"  {c(GREEN, '✓')} Virtual environment ready",
        f"  {c(GREEN, '✓')} Packages installed",
        f"  {c(GREEN, '✓')} API keys saved",
        f"  {c(GREEN, '✓')} Config file written",
    ]
    if scheduler_pending:
        summary_lines += [
            "",
            f"  {c(YELLOW, '⚠')}  Still to do: install the scheduler",
            "     1. Open the  scheduler  folder in this project",
            "     2. Right-click  install-task.bat",
            '     3. Choose "Run as administrator"',
        ]
    summary_lines += [
        "",
        "  What to do next:",
        '  1. The web UI will open in your browser.',
        '  2. Click "Find New Jobs" on your profile to kick off your first search.',
        "     (Takes 2–5 minutes — you can browse the app while it runs.)",
        "  3. Your matched jobs will appear in the Jobs tab when it's done.",
        "",
        f"  Daily use: double-click  {'start.bat' if IS_WINDOWS else 'start.sh'}  to open the app.",
        "  Then click 'Find New Jobs' to check for the latest postings.",
    ]
    banner(summary_lines)

    want_launch = ask_yn("Open the web UI now?", default=True)
    if want_launch:
        _launch_web()
    else:
        blank()
        info("Run  python run_web.py  whenever you're ready.")
        if IS_WINDOWS:
            info("Or double-click  start.bat")


def _launch_web():
    blank()
    info("Starting web server…")
    cmd = [str(VENV_PYTHON), str(ROOT / "run_web.py")]
    kwargs: dict = {"cwd": ROOT}
    if IS_WINDOWS:
        kwargs["creationflags"] = (
            subprocess.DETACHED_PROCESS | subprocess.CREATE_NEW_PROCESS_GROUP
        )
    else:
        kwargs["start_new_session"] = True
    subprocess.Popen(cmd, **kwargs)
    time.sleep(2)
    webbrowser.open("http://localhost:6868")
    ok("Web UI opened at http://localhost:6868")
    blank()


# ── Main ──────────────────────────────────────────────────────────────────────
def main():
    clear()

    # ASCII art logo (works on all terminals)
    print(c(CYAN + BOLD, r"""
     _ _    _   __        ___
    | (_)  | |  \ \      / (_)___  ___
 _  | | |__| |___\ \ /\ / /| / __|/ _ \
| |_| | |  _  |___\ V  V / | \__ \  __/
 \___/|_|_| |_|    \_/\_/  |_|___/\___|
"""))
    print(c(BOLD, "  Welcome to the JobWise Setup Wizard"))
    print(c(DIM,  "  This wizard sets up everything so you can start finding jobs."))
    print(c(DIM,  "  It takes about 5 minutes. You can press Ctrl+C at any time to stop."))
    blank()

    step1_python()
    step2_packages()
    provider, env_keys = step3_ai_provider()
    jsearch_keys, jsearch_day = step4_jsearch()
    env_keys.update(jsearch_keys)
    profile_name = step5_profile_name()
    profile_file, is_real_file = step6_profile_file(profile_name)
    step7_write_config(provider, env_keys, jsearch_day, profile_name)
    step8_import_profile(profile_name, profile_file, is_real_file, provider, env_keys)
    scheduler_pending = step9_scheduler(profile_name)
    step9_launch(provider, profile_name, profile_file, scheduler_pending)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print(f"\n\n{c(YELLOW, '  Setup cancelled.')}  Run setup again whenever you like.\n")
        sys.exit(0)
