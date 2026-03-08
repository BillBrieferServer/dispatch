#!/usr/bin/env python3
"""
Scrape IWF (Idaho Wildlife Federation) bill positions.

Source: BillTrack50 public stakeholder API
Method: POST — no auth
"""
import argparse
import logging
import sys

import requests

sys.path.insert(0, __file__.rsplit('/', 1)[0])
from utils import get_db, normalize_bill_number, lookup_bill_id, upsert_position

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
logger = logging.getLogger(__name__)

API_URL = "https://www.billtrack50.com/webapi/public-stakeholder/Ntdo9EbzpEKaZJuycAIUDQ/items"
ORG_NAME = "IWF"
SOURCE_URL = "https://www.billtrack50.com"

POSITION_MAP = {
    "support": "support",
    "oppose": "oppose",
    "neutral": "neutral",
}


def scrape(dry_run=False):
    """Scrape IWF bill positions from BillTrack50."""
    logger.info(f"Fetching IWF positions from {API_URL}")
    resp = requests.post(API_URL, json={}, timeout=30)
    resp.raise_for_status()
    items = resp.json()

    if isinstance(items, dict):
        items = items.get("items", items.get("data", []))

    logger.info(f"IWF: {len(items)} items found")

    stats = {"found": len(items), "matched": 0, "upserted": 0, "skipped": 0}
    conn = None if dry_run else get_db()

    try:
        for item in items:
            bill_number_raw = (item.get("stateBillID") or "").strip()
            position_col = (item.get("positionColumn") or "").strip()
            comments = (item.get("customText39978") or "").strip()

            if not bill_number_raw:
                stats["skipped"] += 1
                continue

            # Skip bills with no position
            if not position_col:
                stats["skipped"] += 1
                continue

            bill_number = normalize_bill_number(bill_number_raw)
            if not bill_number:
                logger.warning(f"IWF: Could not normalize {bill_number_raw!r}")
                stats["skipped"] += 1
                continue

            # Map position
            pos_lower = position_col.lower().strip()
            position = POSITION_MAP.get(pos_lower)
            if not position:
                logger.warning(f"IWF: Unknown position {position_col!r} for {bill_number}")
                stats["skipped"] += 1
                continue

            position_detail = comments if comments else position_col

            if dry_run:
                print(f"  {bill_number:8s} | {position:8s} | {position_detail}")
                stats["matched"] += 1
                stats["upserted"] += 1
                continue

            bill_id = lookup_bill_id(conn, bill_number)
            if bill_id is None:
                logger.warning(f"IWF: Bill {bill_number} not found in QIBrain — skipping")
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
    parser = argparse.ArgumentParser(description="Scrape IWF bill positions")
    parser.add_argument("--dry-run", action="store_true", help="Print results without inserting")
    args = parser.parse_args()

    result = scrape(dry_run=args.dry_run)
    logger.info(f"IWF: {result}")


if __name__ == "__main__":
    main()
