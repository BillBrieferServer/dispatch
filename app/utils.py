"""Shared utility functions and constants for the Bill Briefer application."""

import os
import csv
import json
import logging
import re
import shutil
from pathlib import Path
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional
from zoneinfo import ZoneInfo

logger = logging.getLogger(__name__)

# --- Timezone ---
BOISE_TZ = ZoneInfo("America/Boise")

# --- Directory layout ---
DATA_DIR = Path("/app/data")
DATA_DIR.mkdir(parents=True, exist_ok=True)
JOBS_DIR = DATA_DIR / "jobs"
JOBS_DIR.mkdir(parents=True, exist_ok=True)
AUTH_DIR = DATA_DIR / "auth"
AUTH_DIR.mkdir(parents=True, exist_ok=True)
REPORTS_DIR = DATA_DIR / "reports"
REPORTS_DIR.mkdir(parents=True, exist_ok=True)

# --- Auth file paths ---
ALLOWLIST_PATH = DATA_DIR / "allowlist_emails.txt"
MANUAL_USERS_PATH = DATA_DIR / "manual_users.json"
ADMIN_ALLOWLIST_PATH = AUTH_DIR / "admin_emails.txt"

# --- Usage log ---
USAGE_LOG_PATH = DATA_DIR / "usage_log.csv"
USAGE_LOG_MAX_BYTES = 10 * 1024 * 1024  # 10MB rotation threshold
USAGE_LOG_FIELDS = [
    "job_id",
    "created_at_utc",
    "started_at_utc",
    "finished_at_utc",
    "created_at_boise",
    "started_at_boise",
    "finished_at_boise",
    "requestor",
    "bill_input",
    "bill_resolved",
    "bill_id",
    "status",
    "error",
    "bill_text_doc_id",
    "bill_text_mime",
    "bill_text_chars",
    "bill_text_ocr_used",
    "bill_text_ocr_pages",
    "bill_text_sent_to_ai_chars",
    "ai_model",
]

# --- Token cost estimation (per million tokens) ---
TOKEN_COSTS = {
    "anthropic": {
        "claude-sonnet-4-20250514": {"input": 3.00, "output": 15.00},
        "claude-sonnet-4-0": {"input": 3.00, "output": 15.00},
        "claude-haiku-4-5-20251001": {"input": 0.80, "output": 4.00},
        "default": {"input": 3.00, "output": 15.00},
    },
    "openai": {
        "gpt-4o-mini": {"input": 0.15, "output": 0.60},
        "default": {"input": 0.15, "output": 0.60},
    },
}

# --- Session ID mapping ---
SESSION_ID_MAP = {
    "2026": 2246,
    "2025": 2168,
    "2024": 2119,
    "2023": 2011,
    "2022": 1954,
    "2021": 1800,
    "2020": 1725,
}
DEFAULT_SESSION_YEAR = "2026"
AVAILABLE_SESSIONS = list(range(int(DEFAULT_SESSION_YEAR), 2019, -1))

# --- Email config ---
FROM_EMAIL = os.getenv("FROM_EMAIL", "").strip()
ALERT_EMAIL = os.getenv("ALERT_EMAIL", "").strip()
NTFY_TOPIC = os.getenv("NTFY_TOPIC", "").strip()
JOB_TIMEOUT_MINUTES = int(os.getenv("JOB_TIMEOUT_MINUTES", "5"))
SMTP_HOST = os.getenv("SMTP_HOST", "").strip()
SMTP_PORT = int(os.getenv("SMTP_PORT", "587").strip() or "587")
SMTP_USER = os.getenv("SMTP_USER", "").strip()
SMTP_PASS = os.getenv("SMTP_PASS", "").strip()

# --- Worker config ---
JOB_WORKER_INTERVAL_SECONDS = int(os.getenv("JOB_WORKER_INTERVAL_SECONDS", "10").strip() or "10")

# --- LegiScan (kept for legiscan_call) ---
LEGISCAN_API_KEY = os.getenv("LEGISCAN_API_KEY", "").strip()
LEGISCAN_BASE_URL = "https://api.legiscan.com/"
LEGISCAN_STATE = os.getenv("LEGISCAN_STATE", "ID").strip().upper()
LEGISCAN_SESSION_YEAR = os.getenv("LEGISCAN_SESSION_YEAR", "").strip()
LEGISCAN_SESSION_ID_OVERRIDE = os.getenv("LEGISCAN_SESSION_ID", "").strip()

# --- App URL ---
APP_BASE_URL = os.getenv("APP_BASE_URL", "https://sand.billbriefer.com").strip().rstrip("/")


def _estimate_cost(token_usage: dict) -> float:
    provider = token_usage.get("provider", "anthropic")
    model = token_usage.get("model", "default")
    costs = TOKEN_COSTS.get(provider, {})
    rates = costs.get(model, costs.get("default", {"input": 3.00, "output": 15.00}))
    input_cost = (token_usage.get("input_tokens", 0) / 1_000_000) * rates["input"]
    output_cost = (token_usage.get("output_tokens", 0) / 1_000_000) * rates["output"]
    return round(input_cost + output_cost, 6)


def _read_json(path: Path) -> Optional[Dict[str, Any]]:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return None


def _write_json(path: Path, payload: Dict[str, Any]) -> None:
    path.write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")


def _norm_text(x: Any) -> str:
    if x is None:
        return ""
    if isinstance(x, str):
        return x.strip()
    return str(x).strip()


def _iso_utc(ts: Any) -> str:
    try:
        t = float(ts)
    except Exception:
        return ""
    return datetime.fromtimestamp(t, tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _iso_boise(ts: Any) -> str:
    try:
        t = float(ts)
    except Exception:
        return ""
    dt = datetime.fromtimestamp(t, tz=timezone.utc).astimezone(BOISE_TZ)
    return dt.isoformat(timespec="seconds")


def _rotate_usage_log():
    """Rotate usage_log.csv if it exceeds size threshold."""
    try:
        if USAGE_LOG_PATH.exists() and USAGE_LOG_PATH.stat().st_size > USAGE_LOG_MAX_BYTES:
            suffix = datetime.now().strftime("%Y_%m")
            archive = DATA_DIR / f"usage_log_{suffix}.csv"
            if not archive.exists():
                shutil.move(str(USAGE_LOG_PATH), str(archive))
                logger.info(f"Rotated usage_log.csv to {archive.name}")
    except Exception as e:
        logger.warning(f"Usage log rotation failed: {e}")


def append_usage_log(job: Dict[str, Any]) -> None:
    _rotate_usage_log()
    """Append one row to the master usage log CSV (never breaks job flow)."""
    try:
        status = (job.get("status") or "").lower()
        if status not in ("done", "failed"):
            return

        row = {
            "job_id": job.get("job_id", ""),
            "created_at_utc": _iso_utc(job.get("created_at")),
            "started_at_utc": _iso_utc(job.get("started_at")),
            "finished_at_utc": _iso_utc(job.get("finished_at")),
            "created_at_boise": _iso_boise(job.get("created_at")),
            "started_at_boise": _iso_boise(job.get("started_at")),
            "finished_at_boise": _iso_boise(job.get("finished_at")),
            "requestor": job.get("email", ""),
            "bill_input": job.get("bill_input", ""),
            "bill_resolved": job.get("bill_resolved", ""),
            "bill_id": job.get("bill_id", ""),
            "status": job.get("status", ""),
            "error": job.get("error", ""),
            "bill_text_doc_id": job.get("bill_text_doc_id", ""),
            "bill_text_mime": job.get("bill_text_mime", ""),
            "bill_text_chars": job.get("bill_text_chars", ""),
            "bill_text_ocr_used": job.get("bill_text_ocr_used", ""),
            "bill_text_ocr_pages": job.get("bill_text_ocr_pages", ""),
            "bill_text_sent_to_ai_chars": job.get("bill_text_sent_to_ai_chars", ""),
            "ai_model": job.get("ai_model_used", ""),
        }

        file_exists = USAGE_LOG_PATH.exists()
        with USAGE_LOG_PATH.open("a", encoding="utf-8", newline="") as f:
            w = csv.DictWriter(f, fieldnames=USAGE_LOG_FIELDS)
            if not file_exists:
                w.writeheader()
            w.writerow(row)
    except Exception:
        return


def _load_allowlist() -> set:
    if not ALLOWLIST_PATH.exists():
        return set()
    out = set()
    for raw in ALLOWLIST_PATH.read_text(encoding="utf-8", errors="ignore").splitlines():
        s = (raw or "").strip().lower()
        if s and "@" in s:
            out.add(s)
    return out


def _load_manual_users() -> dict:
    """Load manual users dict: {email: {name: str, district: int|None}}"""
    if not MANUAL_USERS_PATH.exists():
        return {}
    try:
        data = json.loads(MANUAL_USERS_PATH.read_text(encoding="utf-8"))
        out = {}
        for email, val in data.items():
            if isinstance(val, str):
                out[email] = {"name": val, "district": None}
            elif isinstance(val, dict):
                out[email] = val
            else:
                out[email] = {"name": str(val), "district": None}
        return out
    except Exception:
        return {}


def _save_manual_users(users: dict):
    """Save manual users dict: {email: {name: str, district: int|None}}"""
    MANUAL_USERS_PATH.write_text(json.dumps(users, indent=2), encoding="utf-8")


def _load_admin_allowlist() -> set:
    if not ADMIN_ALLOWLIST_PATH.exists():
        return set()
    out = set()
    for raw in ADMIN_ALLOWLIST_PATH.read_text(encoding="utf-8", errors="ignore").splitlines():
        s = (raw or "").strip().lower()
        if s and "@" in s:
            out.add(s)
    return out


def _html_to_text(s: str) -> str:
    """Very simple HTML -> text fallback (good enough for plain-text email clients)."""
    s = s or ""
    s = s.replace("<br>", "\n").replace("<br/>", "\n").replace("<br />", "\n")
    s = s.replace("</p>", "\n\n").replace("</div>", "\n")
    s = re.sub(r"<[^>]+>", "", s)
    import html as _html
    s = _html.unescape(s)
    s = re.sub(r"\n{3,}", "\n\n", s).strip()
    return s
