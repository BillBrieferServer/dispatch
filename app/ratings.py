"""
ratings.py

SQLite storage for legislator interest ratings.
Each rating is per-tenant, per-session, per-legislator.
"""

import sqlite3
import time
from contextlib import contextmanager
from pathlib import Path
from typing import Dict, Optional

DATA_DIR = Path("/app/data")
RATINGS_DB = DATA_DIR / "ratings.sqlite"


@contextmanager
def _conn():
    """Get database connection with proper settings."""
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    con = sqlite3.connect(str(RATINGS_DB), timeout=30)
    con.row_factory = sqlite3.Row
    try:
        con.execute("PRAGMA journal_mode=WAL;")
        con.execute("PRAGMA synchronous=NORMAL;")
        yield con
        con.commit()
    finally:
        con.close()


def init_ratings_db() -> None:
    """Create the ratings table if it doesn't exist."""
    with _conn() as con:
        con.execute("""
            CREATE TABLE IF NOT EXISTS legislator_ratings (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                tenant TEXT NOT NULL DEFAULT '',
                session_year TEXT NOT NULL,
                legislator_email TEXT NOT NULL,
                rating INTEGER NOT NULL CHECK(rating BETWEEN 1 AND 5),
                created_at REAL NOT NULL,
                updated_at REAL NOT NULL,
                UNIQUE(tenant, session_year, legislator_email)
            )
        """)
        con.execute("CREATE INDEX IF NOT EXISTS idx_ratings_session ON legislator_ratings(tenant, session_year);")


def get_ratings(tenant: str, session_year: str) -> Dict[str, int]:
    """
    Get all ratings for a tenant+session.
    Returns dict of {legislator_email: rating}.
    """
    with _conn() as con:
        cur = con.execute(
            "SELECT legislator_email, rating FROM legislator_ratings WHERE tenant = ? AND session_year = ?",
            (tenant, session_year)
        )
        return {row["legislator_email"]: row["rating"] for row in cur.fetchall()}


def set_rating(tenant: str, session_year: str, legislator_email: str, rating: int) -> None:
    """Upsert a rating (1-5) for a legislator."""
    now = time.time()
    with _conn() as con:
        con.execute("""
            INSERT INTO legislator_ratings (tenant, session_year, legislator_email, rating, created_at, updated_at)
            VALUES (?, ?, ?, ?, ?, ?)
            ON CONFLICT(tenant, session_year, legislator_email) DO UPDATE SET
                rating = excluded.rating,
                updated_at = excluded.updated_at
        """, (tenant, session_year, legislator_email, rating, now, now))


def clear_rating(tenant: str, session_year: str, legislator_email: str) -> None:
    """Remove a rating (set back to unrated)."""
    with _conn() as con:
        con.execute(
            "DELETE FROM legislator_ratings WHERE tenant = ? AND session_year = ? AND legislator_email = ?",
            (tenant, session_year, legislator_email)
        )
