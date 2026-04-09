"""Local auth persistence for AppGrowing CLI."""

from __future__ import annotations

import json
import importlib
import os
import re
import shutil
import subprocess
from pathlib import Path
from typing import Any


AUTH_DIR = Path.home() / ".config" / "appgrowing-cli"
AUTH_FILE = AUTH_DIR / "auth.json"
BROWSER_NAMES = ("auto", "chrome", "firefox", "edge", "brave")


def save_auth(cookie: str, endpoint: str, language: str) -> Path:
    """Persist auth session data locally."""
    AUTH_DIR.mkdir(parents=True, exist_ok=True)
    payload = {
        "cookie": cookie,
        "endpoint": endpoint,
        "language": language,
    }
    AUTH_FILE.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    return AUTH_FILE


def load_auth() -> dict[str, Any]:
    """Load auth session data from env or local file."""
    env_cookie = os.getenv("APPGROWING_COOKIE", "").strip()
    env_endpoint = os.getenv("APPGROWING_ENDPOINT", "").strip()
    env_language = os.getenv("APPGROWING_LANGUAGE", "").strip()
    if env_cookie:
        return {
            "cookie": env_cookie,
            "endpoint": env_endpoint or "https://api-appgrowing-global.youcloud.com/graphql",
            "language": env_language or "en",
            "source": "env",
        }
    if AUTH_FILE.exists():
        data = json.loads(AUTH_FILE.read_text(encoding="utf-8"))
        data["source"] = "file"
        return data
    return {
        "cookie": "",
        "endpoint": "https://api-appgrowing-global.youcloud.com/graphql",
        "language": "en",
        "source": "none",
    }


def parse_curl_auth(raw_command: str) -> dict[str, str]:
    """Extract cookie/endpoint/language from a curl command string."""
    cookie = ""
    endpoint = ""
    language = ""

    cookie_match = re.search(r"-H\s+['\"]cookie:\s*([^'\"]+)['\"]", raw_command, flags=re.IGNORECASE)
    if cookie_match:
        cookie = cookie_match.group(1).strip()

    lang_match = re.search(
        r"-H\s+['\"]accept-language:\s*([^'\"]+)['\"]",
        raw_command,
        flags=re.IGNORECASE,
    )
    if lang_match:
        language = lang_match.group(1).strip().split(",")[0]

    url_match = re.search(r"(https?://[^\s'\"\\]+)", raw_command)
    if url_match:
        endpoint = url_match.group(1).strip()

    return {
        "cookie": cookie,
        "endpoint": endpoint,
        "language": language,
    }


def _cookie_dict_to_header(cookie_map: dict[str, str]) -> str:
    return "; ".join(f"{k}={v}" for k, v in cookie_map.items() if k and v)


def _extract_direct(browser: str, domain: str) -> dict[str, str]:
    try:
        browser_cookie3 = importlib.import_module("browser_cookie3")
    except ImportError:
        return {}

    fn_map = {
        "chrome": browser_cookie3.chrome,
        "firefox": browser_cookie3.firefox,
        "edge": browser_cookie3.edge,
        "brave": browser_cookie3.brave,
    }
    if browser == "auto":
        browser_order = ("chrome", "firefox", "edge", "brave")
    else:
        browser_order = (browser,)

    for b in browser_order:
        fn = fn_map[b]
        try:
            jar = fn(domain_name=domain)
            cookies = {c.name: c.value for c in jar if c.value}
            if cookies:
                return cookies
        except Exception:
            continue
    return {}


def _extract_subprocess(browser: str, domain: str) -> dict[str, str]:
    if not shutil.which("uv"):
        return {}
    script = r"""
import json
import browser_cookie3

browser = "__BROWSER__"
domain = "__DOMAIN__"

fn_map = {
    "chrome": browser_cookie3.chrome,
    "firefox": browser_cookie3.firefox,
    "edge": browser_cookie3.edge,
    "brave": browser_cookie3.brave,
}
order = ["chrome", "firefox", "edge", "brave"] if browser == "auto" else [browser]

out = {}
for b in order:
    try:
        jar = fn_map[b](domain_name=domain)
        out = {c.name: c.value for c in jar if c.value}
        if out:
            break
    except Exception:
        continue

if out:
    print(json.dumps(out, ensure_ascii=False))
"""
    script = script.replace("__BROWSER__", browser).replace("__DOMAIN__", domain)
    try:
        result = subprocess.run(
            ["uv", "run", "--with", "browser-cookie3", "python3", "-c", script],
            capture_output=True,
            text=True,
            timeout=30,
        )
    except Exception:
        return {}
    if result.returncode != 0 or not result.stdout.strip():
        return {}
    try:
        parsed = json.loads(result.stdout.strip())
    except json.JSONDecodeError:
        return {}
    if not isinstance(parsed, dict):
        return {}
    return {str(k): str(v) for k, v in parsed.items() if k and v}


def extract_browser_auth(
    *,
    browser: str = "auto",
    domain: str = ".youcloud.com",
) -> dict[str, str]:
    """Extract browser cookies and return auth payload.

    Returns empty dict when extraction fails.
    """
    browser = browser.lower().strip()
    if browser not in BROWSER_NAMES:
        return {}
    domain = domain.strip() or ".youcloud.com"

    cookies = _extract_subprocess(browser, domain)
    if not cookies:
        cookies = _extract_direct(browser, domain)
    if not cookies:
        return {}

    return {
        "cookie": _cookie_dict_to_header(cookies),
        "endpoint": "https://api-appgrowing-global.youcloud.com/graphql",
        "language": "en",
        "cookie_count": str(len(cookies)),
    }
