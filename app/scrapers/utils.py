"""
Shared utilities for Dispatch advocacy scrapers.
- Bill number normalization (QIBrain format)
- Database helpers (lookup, upsert)
"""
import logging
import re
from datetime import datetime, timezone

import psycopg2
from psycopg2.extras import execute_values

logger = logging.getLogger(__name__)

DB_URL = "postgresql://quietimpact_user:ezj9QfukEXaShHcBpqN92WM4KREvvlWA@localhost:5432/qibrain"


def get_db():
    """Get a psycopg2 connection to QIBrain."""
    return psycopg2.connect(DB_URL)


def normalize_bill_number(raw: str) -> str | None:
    """
    Normalize a raw bill number string to QIBrain format.

    Examples:
        "H500" → "H0500"
        "HB 781" → "H0781"
        "House Bill 840" → "H0840"
        "Senate Bill 1263" → "S1263"
        "HJR 103" → "HJR103"
        "HJM9" → "HJM009"
    """
    if not raw or not isinstance(raw, str):
        return None

    s = raw.strip()
    if not s:
        return None

    # Full text form: "House Bill 840", "Senate Joint Resolution 103", etc.
    type_map = {
        'House Bill': 'H',
        'Senate Bill': 'S',
        'House Joint Resolution': 'HJR',
        'Senate Joint Resolution': 'SJR',
        'House Concurrent Resolution': 'HCR',
        'Senate Concurrent Resolution': 'SCR',
        'House Joint Memorial': 'HJM',
        'Senate Joint Memorial': 'SJM',
    }

    for full_name, prefix in type_map.items():
        pattern = re.compile(rf'^{re.escape(full_name)}\s+(\d+)', re.IGNORECASE)
        m = pattern.match(s)
        if m:
            num = m.group(1)
            return _format_bill(prefix, num)

    # Abbreviation form: "HB 781", "SB 1303", etc.
    abbrev_map = {
        'HB': 'H',
        'SB': 'S',
    }
    m = re.match(r'^(HB|SB)\s*(\d+)$', s, re.IGNORECASE)
    if m:
        prefix = abbrev_map[m.group(1).upper()]
        num = m.group(2)
        return _format_bill(prefix, num)

    # Short form: "H500", "S1263", "HJR103", "HJM9", etc.
    m = re.match(r'^(HCR|SCR|HJR|SJR|HJM|SJM|H|S)\s*(\d+)$', s, re.IGNORECASE)
    if m:
        prefix = m.group(1).upper()
        num = m.group(2)
        return _format_bill(prefix, num)

    logger.warning(f"Could not normalize bill number: {raw!r}")
    return None


def _format_bill(prefix: str, num_str: str) -> str:
    """Format prefix + number with correct zero-padding."""
    num = int(num_str)
    if prefix in ('H', 'S'):
        return f"{prefix}{num:04d}"
    else:
        # HJR, SJR, HCR, SCR, HJM, SJM — 3-digit padding
        return f"{prefix}{num:03d}"


def lookup_bill_id(conn, bill_number: str) -> int | None:
    """Look up bill_id from idaho.bills by bill_number."""
    with conn.cursor() as cur:
        cur.execute(
            "SELECT bill_id FROM idaho.bills WHERE bill_number = %s LIMIT 1",
            (bill_number,)
        )
        row = cur.fetchone()
        return row[0] if row else None


def upsert_position(conn, bill_id: int, org_name: str, position: str,
                     position_detail: str = None, source_url: str = None):
    """Insert or update an advocacy position."""
    now = datetime.now(timezone.utc)
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO dispatch.advocacy_positions
                (bill_id, org_name, position, position_detail, source_url, noted_at, entered_by)
            VALUES (%s, %s, %s, %s, %s, %s, 'scraper')
            ON CONFLICT (bill_id, org_name) DO UPDATE SET
                position = EXCLUDED.position,
                position_detail = EXCLUDED.position_detail,
                source_url = EXCLUDED.source_url,
                noted_at = EXCLUDED.noted_at
        """, (bill_id, org_name, position, position_detail, source_url, now))


def upsert_legislator_score(conn, legislator_name: str, chamber: str,
                             district: str, org_name: str, year: int,
                             score: float, possible_score: float,
                             vote_index: float, source_url: str = None):
    """Insert or update a legislator score."""
    now = datetime.now(timezone.utc)
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO dispatch.legislator_scores
                (legislator_name, chamber, district, org_name, year,
                 score, possible_score, vote_index, source_url, updated_at)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (legislator_name, org_name, year) DO UPDATE SET
                chamber = EXCLUDED.chamber,
                district = EXCLUDED.district,
                score = EXCLUDED.score,
                possible_score = EXCLUDED.possible_score,
                vote_index = EXCLUDED.vote_index,
                source_url = EXCLUDED.source_url,
                updated_at = EXCLUDED.updated_at
        """, (legislator_name, chamber, district, org_name, year,
              score, possible_score, vote_index, source_url, now))
