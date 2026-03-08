#!/usr/bin/env python3
"""
Scrape ACLU of Idaho bill positions.

Source: https://www.acluidaho.org
Method: WordPress REST API with ACF fields — no auth

Legislation CPT: GET /wp-json/wp/v2/legislation
Position taxonomy: GET /wp-json/wp/v2/position
Session filter: acf.session == 295 for 2026
"""
import argparse
import logging
import sys

import requests

sys.path.insert(0, __file__.rsplit('/', 1)[0])
from utils import get_db, normalize_bill_number, lookup_bill_id, upsert_position

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
logger = logging.getLogger(__name__)

LEGISLATION_URL = "https://www.acluidaho.org/wp-json/wp/v2/legislation"
POSITION_TAX_URL = "https://www.acluidaho.org/wp-json/wp/v2/position"
ORG_NAME = "ACLU Idaho"
SOURCE_URL = "https://www.acluidaho.org"
SESSION_2026 = 295

# Taxonomy ID → position mapping
POSITION_MAP = {
    56: ("oppose", "Oppose"),
    59: ("support", "Support"),
    304: ("monitor", "Monitor"),
    60: ("neutral", "Neutral"),
    62: ("oppose", "Oppose Unless Amended"),
}


def fetch_all_legislation():
    """Fetch all legislation posts, handling pagination."""
    all_items = []
    page = 1
    per_page = 100

    while True:
        logger.info(f"Fetching ACLU legislation page {page}")
        resp = requests.get(LEGISLATION_URL, params={
            "per_page": per_page,
            "page": page,
            "_fields": "title,acf",
        }, timeout=30)

        if resp.status_code == 400:
            break

        resp.raise_for_status()
        items = resp.json()

        if not items:
            break

        all_items.extend(items)

        total_pages = int(resp.headers.get("X-WP-TotalPages", 1))
        if page >= total_pages:
            break
        page += 1

    return all_items


def scrape(dry_run=False):
    """Scrape ACLU Idaho bill positions."""
    items = fetch_all_legislation()
    logger.info(f"ACLU: {len(items)} legislation items found (all sessions)")

    stats = {"found": 0, "matched": 0, "upserted": 0, "skipped": 0}
    conn = None if dry_run else get_db()

    try:
        for item in items:
            acf = item.get("acf", {}) or {}

            # Filter to 2026 session
            session = acf.get("session")
            if isinstance(session, list):
                session_ids = session
            elif isinstance(session, (int, str)):
                session_ids = [session]
            else:
                continue

            if SESSION_2026 not in [int(s) if isinstance(s, (int, str)) and str(s).isdigit() else s for s in session_ids]:
                continue

            stats["found"] += 1

            # Bill number from ACF (already in QIBrain format per spec)
            raw_bill = (acf.get("bill_number") or "").strip()
            if not raw_bill:
                stats["skipped"] += 1
                continue

            bill_number = normalize_bill_number(raw_bill)
            if not bill_number:
                logger.warning(f"ACLU: Could not normalize {raw_bill!r}")
                stats["skipped"] += 1
                continue

            # Position from taxonomy ID
            pos_ids = acf.get("position", [])
            if isinstance(pos_ids, (int, str)):
                pos_ids = [pos_ids]
            elif not isinstance(pos_ids, list):
                pos_ids = []

            position = None
            position_detail = None

            for pid in pos_ids:
                try:
                    pid = int(pid)
                except (ValueError, TypeError):
                    continue
                if pid in POSITION_MAP:
                    position, position_detail = POSITION_MAP[pid]
                    break

            if not position:
                stats["skipped"] += 1
                continue

            if dry_run:
                print(f"  {bill_number:8s} | {position:8s} | {position_detail}")
                stats["matched"] += 1
                stats["upserted"] += 1
                continue

            bill_id = lookup_bill_id(conn, bill_number)
            if bill_id is None:
                logger.warning(f"ACLU: Bill {bill_number} not found in QIBrain — skipping")
                stats["skipped"] += 1
                continue

            stats["matched"] += 1
            upsert_position(conn, bill_id, ORG_NAME, position, position_detail, SOURCE_URL)
            stats["upserted"] += 1

        if conn:
            conn.commit()
    finally:
        if conn:
            conn.close()

    return stats


def main():
    parser = argparse.ArgumentParser(description="Scrape ACLU Idaho bill positions")
    parser.add_argument("--dry-run", action="store_true", help="Print results without inserting")
    args = parser.parse_args()

    result = scrape(dry_run=args.dry_run)
    logger.info(f"ACLU: {result}")


if __name__ == "__main__":
    main()
