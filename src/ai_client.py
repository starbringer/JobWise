"""
ai_client.py — Abstraction over AI API calls.
Supports both legacy Claude Code CLI (`claude -p`) and modern multi-provider via `litellm`.
"""

import json
import logging
import os
import subprocess
import time
from pathlib import Path

import yaml
try:
    import litellm
except ImportError:
    litellm = None

logger = logging.getLogger(__name__)

PROJECT_ROOT = Path(__file__).parent.parent


class RateLimitError(RuntimeError):
    """Raised when usage limit is exhausted. Caller should abort the run."""
    pass


# ---------------------------------------------------------------------------
# Configuration & Token tracking
# ---------------------------------------------------------------------------
_CHARS_PER_TOKEN = 3.5

_session: dict = {"input_tokens": 0, "output_tokens": 0, "calls": 0}
_config_cache: dict | None = None


def _get_config() -> dict:
    global _config_cache
    if _config_cache is None:
        config_path = PROJECT_ROOT / "config" / "config.yaml"
        if config_path.exists():
            with open(config_path, encoding="utf-8") as f:
                _config_cache = yaml.safe_load(f) or {}
        else:
            _config_cache = {}
    return _config_cache


def _estimate_tokens(text: str) -> int:
    return max(1, round(len(text) / _CHARS_PER_TOKEN))


def reset_usage() -> None:
    """Reset the session token counter. Call at the start of each pipeline run."""
    _session.update({"input_tokens": 0, "output_tokens": 0, "calls": 0})


def get_usage() -> dict:
    """Return cumulative token usage for the current session."""
    total = _session["input_tokens"] + _session["output_tokens"]
    config = _get_config()
    provider = config.get("ai", {}).get("provider", "claude_cli")
    note = "exact API counts" if provider != "claude_cli" else "estimated (~3.5 chars/token)"
    return {
        "input_tokens": _session["input_tokens"],
        "output_tokens": _session["output_tokens"],
        "total_tokens": total,
        "calls": _session["calls"],
        "note": note,
    }


# ---------------------------------------------------------------------------
# Legacy Claude CLI Logic
# ---------------------------------------------------------------------------

def _find_claude_exe() -> str:
    override = os.environ.get("CLAUDE_EXE")
    if override:
        return override

    import shutil
    on_path = shutil.which("claude")
    if on_path:
        return on_path

    for ext_dir in [Path.home() / ".vscode" / "extensions",
                    Path.home() / ".cursor" / "extensions"]:
        if not ext_dir.exists():
            continue
        candidates = sorted(
            ext_dir.glob("anthropic.claude-code-*/resources/native-binary/claude.exe"),
            key=lambda p: p.parts[-4],
            reverse=True,
        )
        candidates = [p for p in candidates if p.exists()]
        if candidates:
            return str(candidates[0])

    return "claude"


def _call_claude_cli(prompt: str, expect_json: bool = True, system: str | None = None) -> str:
    claude_exe = _find_claude_exe()

    # Optional model override from config (e.g. "claude-haiku-4-5-20251001")
    config = _get_config()
    cli_model = (config.get("ai", {}).get("claude_cli", {}) or {}).get("model", "").strip()

    # CLI has no separate system param — prepend it to the prompt
    full_prompt = (system + "\n\n" + prompt) if system else prompt

    env = os.environ.copy()
    if "CLAUDE_CODE_GIT_BASH_PATH" not in env:
        for candidate in [
            r"D:\apps\Git\bin\bash.exe",
            r"C:\Program Files\Git\bin\bash.exe",
            r"C:\Git\bin\bash.exe",
        ]:
            if Path(candidate).exists():
                env["CLAUDE_CODE_GIT_BASH_PATH"] = candidate
                break

    def _run(exe):
        cmd = [exe, "-p"]
        if cli_model:
            cmd += ["--model", cli_model]
        return subprocess.run(
            cmd,
            input=full_prompt,
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="replace",
            cwd=str(PROJECT_ROOT),
            timeout=600,
            env=env,
        )

    try:
        result = _run(claude_exe)
    except FileNotFoundError:
        import time
        logger.warning(
            f"claude.exe failed to launch from '{claude_exe}' (transient error), "
            "retrying in 10s..."
        )
        time.sleep(10)
        try:
            result = _run(claude_exe)
        except FileNotFoundError:
            claude_exe = _find_claude_exe()
            try:
                result = _run(claude_exe)
            except FileNotFoundError:
                raise RuntimeError(
                    f"Claude Code CLI not found. Last tried: '{claude_exe}'."
                )
    except subprocess.TimeoutExpired:
        raise RuntimeError("Claude CLI call timed out after 600 seconds.")

    if result.returncode != 0:
        combined = (result.stdout + result.stderr).lower()
        if "usage limit" in combined or "rate limit" in combined or "quota" in combined:
            raise RateLimitError(
                "Claude usage limit reached.\n"
                f"CLI output: {(result.stdout + result.stderr).strip()[:300]}"
            )
        if result.returncode == 1 and not combined.strip():
            import time
            for delay in (30, 60):
                logger.warning(f"Claude CLI exited 1 with no output, retrying in {delay}s...")
                time.sleep(delay)
                result = _run(claude_exe)
                if result.returncode == 0:
                    break
                new_combined = (result.stdout + result.stderr).lower()
                if "usage limit" in new_combined or "rate limit" in new_combined or "quota" in new_combined:
                    raise RateLimitError(
                        "Claude usage limit reached.\n"
                        f"CLI output: {(result.stdout + result.stderr).strip()[:300]}"
                    )
            else:
                raise RateLimitError("Claude CLI returned exit code 1 with no output after retries.")
        if result.returncode != 0:
            raise RuntimeError(
                f"Claude CLI returned exit code {result.returncode}.\n"
                f"stderr: {result.stderr.strip()}"
            )

    output = result.stdout.strip()
    _session["input_tokens"] += _estimate_tokens(full_prompt)
    _session["output_tokens"] += _estimate_tokens(output)
    _session["calls"] += 1
    logger.debug(f"Claude raw output ({len(output)} chars): {output[:200]}...")
    return output


# ---------------------------------------------------------------------------
# LiteLLM Logic
# ---------------------------------------------------------------------------

def _call_ollama(prompt: str, system: str | None = None) -> str:
    if litellm is None:
        raise RuntimeError("litellm is not installed. Run `pip install litellm`")

    config = _get_config()
    ollama_cfg = config.get("ai", {}).get("ollama", {})
    host = ollama_cfg.get("host", "http://localhost")
    port = ollama_cfg.get("port", 11434)
    model = ollama_cfg.get("model", "llama3")
    api_base = f"{host}:{port}"

    # litellm expects "ollama/modelname"
    litellm_model = model if model.startswith("ollama/") else f"ollama/{model}"

    messages = []
    if system:
        messages.append({"role": "system", "content": system})
    messages.append({"role": "user", "content": prompt})

    try:
        t0 = time.monotonic()
        response = litellm.completion(
            model=litellm_model,
            messages=messages,
            api_base=api_base,
        )
        elapsed = time.monotonic() - t0

        output = response.choices[0].message.content

        if hasattr(response, "usage") and response.usage:
            in_tok = getattr(response.usage, "prompt_tokens", 0)
            out_tok = getattr(response.usage, "completion_tokens", 0)
            _session["input_tokens"] += in_tok
            _session["output_tokens"] += out_tok
        else:
            in_tok = _estimate_tokens((system or "") + prompt)
            out_tok = _estimate_tokens(output)
            _session["input_tokens"] += in_tok
            _session["output_tokens"] += out_tok

        tps = out_tok / elapsed if elapsed > 0 else 0.0
        logger.info(
            f"Ollama ({model}): {in_tok} in / {out_tok} out tokens, "
            f"{elapsed:.1f}s, {tps:.1f} tok/s"
        )
        _session["calls"] += 1
        logger.debug(f"Ollama raw output ({len(output)} chars): {output[:200]}...")
        return output

    except Exception as e:
        raise RuntimeError(f"Ollama call failed: {e}")


def _call_litellm(prompt: str, model: str, system: str | None = None) -> str:
    if litellm is None:
        raise RuntimeError("litellm is not installed. Run `pip install litellm`")

    # Build messages list. For Anthropic/Claude models, send system as a cached
    # content block so the static scoring rules + profile are only charged once
    # per cache window (5-min TTL, ~10% of normal input price on cache hits).
    is_claude = model.startswith("claude") or "anthropic/" in model
    messages = []
    if system:
        if is_claude:
            messages.append({
                "role": "system",
                "content": [{"type": "text", "text": system, "cache_control": {"type": "ephemeral"}}],
            })
        else:
            messages.append({"role": "system", "content": system})
    messages.append({"role": "user", "content": prompt})

    try:
        response = litellm.completion(model=model, messages=messages)

        output = response.choices[0].message.content

        if hasattr(response, 'usage') and response.usage:
            _session["input_tokens"] += getattr(response.usage, 'prompt_tokens', 0)
            _session["output_tokens"] += getattr(response.usage, 'completion_tokens', 0)
        else:
            _session["input_tokens"] += _estimate_tokens((system or "") + prompt)
            _session["output_tokens"] += _estimate_tokens(output)

        _session["calls"] += 1
        logger.debug(f"LiteLLM raw output ({len(output)} chars): {output[:200]}...")
        return output

    except litellm.exceptions.RateLimitError as e:
        raise RateLimitError(f"LiteLLM API Rate limit reached: {e}")
    except litellm.exceptions.ContextWindowExceededError as e:
        raise RuntimeError(f"LiteLLM Context Window Exceeded: {e}")
    except Exception as e:
        raise RuntimeError(f"LiteLLM API call failed: {e}")


# ---------------------------------------------------------------------------
# Public Orchestration Layer
# ---------------------------------------------------------------------------

_LITELLM_PREFIXES = {
    "gemini": "gemini/",
    "openai": "",          # litellm accepts openai model names as-is
    "anthropic": "",       # litellm accepts claude model names as-is
}


def call_ai(prompt: str, expect_json: bool = True, system: str | None = None) -> str:
    """
    Send a prompt to the configured AI provider and return the response text.

    system — optional static context sent separately from the user prompt.
      For the anthropic provider this is delivered as a cached system message,
      reducing input token cost by ~60-70% on repeated calls within the same run.
      For claude_cli it is prepended to the prompt (no native caching).
    """
    config = _get_config()
    ai_cfg = config.get("ai", {})
    provider = ai_cfg.get("provider", "claude_cli")

    if provider == "ollama":
        return _call_ollama(prompt, system=system)
    elif provider == "claude_cli":
        return _call_claude_cli(prompt, expect_json, system=system)
    elif provider in _LITELLM_PREFIXES:
        provider_cfg = ai_cfg.get(provider, {})
        model = provider_cfg.get("model")
        if not model:
            raise RuntimeError(
                f"No model configured for provider '{provider}'. "
                f"Add an 'ai.{provider}.model' entry in config.yaml."
            )
        prefix = _LITELLM_PREFIXES[provider]
        litellm_model = f"{prefix}{model}" if prefix and not model.startswith(prefix) else model
        return _call_litellm(prompt, litellm_model, system=system)
    else:
        raise RuntimeError(
            f"Unknown ai.provider '{provider}'. "
            "Valid options: gemini, openai, anthropic, ollama, claude_cli."
        )


def _strip_code_fences(raw: str) -> str:
    """Strip markdown code fences from an AI response if present."""
    if not raw.startswith("```"):
        return raw
    inner_lines = []
    in_block = False
    for line in raw.splitlines():
        if line.startswith("```") and not in_block:
            in_block = True
            continue
        if line.startswith("```") and in_block:
            break
        if in_block:
            inner_lines.append(line)
    return "\n".join(inner_lines)


def call_ai_for_json(prompt: str, system: str | None = None) -> dict | list:
    """
    Call the configured AI provider and parse the response as JSON.
    Strips markdown code fences if present.
    """
    raw = call_ai(prompt, expect_json=True, system=system)
    cleaned = _strip_code_fences(raw)
    try:
        return json.loads(cleaned)
    except json.JSONDecodeError as e:
        raise RuntimeError(
            f"AI returned invalid JSON: {e}\nRaw output:\n{raw[:500]}"
        )


def call_ai_for_json_with_raw(
    prompt: str, system: str | None = None
) -> tuple[dict | list, str]:
    """
    Like call_ai_for_json but also returns the raw response text as the second
    element of a tuple, for storage and debugging purposes.
    Returns (parsed_json, raw_text).
    """
    raw = call_ai(prompt, expect_json=True, system=system)
    cleaned = _strip_code_fences(raw)
    try:
        return json.loads(cleaned), raw
    except json.JSONDecodeError as e:
        raise RuntimeError(
            f"AI returned invalid JSON: {e}\nRaw output:\n{raw[:500]}"
        )


# Aliases for backward compatibility
call_claude = call_ai
call_claude_for_json = call_ai_for_json
