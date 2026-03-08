"""
ai_cache.py

SQLite cache for AI-generated briefing content.
Goal: Avoid redundant AI API calls by caching analysis per bill.

Caching strategy:
- Sections 2-5, 8: Cache per bill (generic analysis)
- Sections 6, 7, 9: Generated fresh each time (not cached)

Cache invalidation: When bill_change_hash changes (bill was updated)
"""

from __future__ import annotations

import json

import sqlite3
import time
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Dict, List, Optional

DATA_DIR = Path("/app/data")
AI_CACHE_DB = DATA_DIR / "ai_cache.sqlite"


def _now() -> float:
    return time.time()


@contextmanager
def _conn():
    """Get database connection with proper settings."""
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    con = sqlite3.connect(str(AI_CACHE_DB), timeout=30)
    con.row_factory = sqlite3.Row
    try:
        con.execute("PRAGMA journal_mode=WAL;")
        con.execute("PRAGMA synchronous=NORMAL;")
        con.execute("PRAGMA foreign_keys=ON;")
        yield con
        con.commit()
    finally:
        con.close()


def init_ai_cache_db() -> None:
    """Create tables/indexes if they don't exist."""
    with _conn() as con:
        # Main AI briefing cache (sections 2-5, 8)
        con.execute("""
            CREATE TABLE IF NOT EXISTS ai_briefing_cache (
                bill_id INTEGER PRIMARY KEY,
                session_id INTEGER NOT NULL,
                bill_number TEXT NOT NULL,
                bill_change_hash TEXT,
                ai_json TEXT NOT NULL,
                model_used TEXT,
                created_at REAL NOT NULL,
                updated_at REAL NOT NULL,
                input_token_count INTEGER,
                output_token_count INTEGER,
                UNIQUE(session_id, bill_number)
            )
        """)
        con.execute("CREATE INDEX IF NOT EXISTS idx_ai_cache_session ON ai_briefing_cache(session_id);")
        con.execute("CREATE INDEX IF NOT EXISTS idx_ai_cache_hash ON ai_briefing_cache(bill_id, bill_change_hash);")

        # Usage statistics
        con.execute("""
            CREATE TABLE IF NOT EXISTS ai_cache_stats (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                event_type TEXT NOT NULL,
                cache_type TEXT NOT NULL,
                bill_id INTEGER,
                district_code TEXT,
                timestamp REAL NOT NULL
            )
        """)
        con.execute("CREATE INDEX IF NOT EXISTS idx_stats_type ON ai_cache_stats(event_type, timestamp);")


def _to_json(x: Any) -> str:
    try:
        return json.dumps(x, ensure_ascii=False)
    except Exception:
        return "{}"


def _from_json(s: str) -> Any:
    try:
        return json.loads(s or "")
    except Exception:
        return None


def _log_stat(event_type: str, cache_type: str, bill_id: int = None, district_code: str = None):
    """Log cache usage statistics."""
    try:
        with _conn() as con:
            con.execute(
                "INSERT INTO ai_cache_stats (event_type, cache_type, bill_id, district_code, timestamp) VALUES (?, ?, ?, ?, ?)",
                (event_type, cache_type, bill_id, district_code, _now())
            )
    except Exception:
        pass  # Don't fail on stats logging


# ============================================================
# AI Briefing Cache (sections 2-5, 8)
# ============================================================

def get_cached_briefing(bill_id: int, current_change_hash: str = None) -> Optional[Dict[str, Any]]:
    """
    Get cached AI briefing for a bill.

    Args:
        bill_id: LegiScan bill ID
        current_change_hash: If provided, only return if hash matches (bill unchanged)

    Returns:
        Dict with ai_json (parsed), model_used, etc. or None if not cached/stale
    """
    with _conn() as con:
        cur = con.execute(
            "SELECT * FROM ai_briefing_cache WHERE bill_id = ?",
            (int(bill_id),)
        )
        row = cur.fetchone()

        if not row:
            _log_stat("cache_miss", "briefing", bill_id)
            return None

        # Check if bill has changed
        if current_change_hash and row["bill_change_hash"] != current_change_hash:
            _log_stat("cache_invalidated", "briefing", bill_id)
            return None

        _log_stat("cache_hit", "briefing", bill_id)
        return {
            "bill_id": row["bill_id"],
            "session_id": row["session_id"],
            "bill_number": row["bill_number"],
            "ai_json": _from_json(row["ai_json"]) or {},
            "model_used": row["model_used"],
            "created_at": row["created_at"],
            "updated_at": row["updated_at"],
        }


def cache_briefing(
    bill_id: int,
    session_id: int,
    bill_number: str,
    ai_json: Dict[str, Any],
    model_used: str,
    bill_change_hash: str = None,
    input_tokens: int = None,
    output_tokens: int = None,
) -> None:
    """
    Cache AI briefing result for a bill.

    Args:
        bill_id: LegiScan bill ID
        session_id: LegiScan session ID
        bill_number: Bill identifier (e.g., "H0416")
        ai_json: Parsed AI output
        model_used: "anthropic" or "openai"
        bill_change_hash: Current hash for invalidation tracking
        input_tokens: Token count for cost tracking
        output_tokens: Token count for cost tracking
    """
    now = _now()
    with _conn() as con:
        con.execute(
            """
            INSERT INTO ai_briefing_cache
                (bill_id, session_id, bill_number, bill_change_hash, ai_json, model_used,
                 created_at, updated_at, input_token_count, output_token_count)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(bill_id) DO UPDATE SET
                bill_change_hash = excluded.bill_change_hash,
                ai_json = excluded.ai_json,
                model_used = excluded.model_used,
                updated_at = excluded.updated_at,
                input_token_count = excluded.input_token_count,
                output_token_count = excluded.output_token_count
            """,
            (bill_id, session_id, bill_number, bill_change_hash, _to_json(ai_json),
             model_used, now, now, input_tokens, output_tokens)
        )

