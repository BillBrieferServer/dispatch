#!/usr/bin/env python3
"""
Scrape CAI (Citizens Alliance of Idaho) bill tags.

Source: Two public Google Sheets (House + Senate) published as TSV.
Method: Direct HTTP GET — no auth.

CAI uses issue taxonomy tags, not Support/Oppose.
Store as: position="tag", position_detail="CAI: [category_tag]"
"""
import argparse
import csv
import io
import logging
import sys

import requests

sys.path.insert(0, __file__.rsplit('/', 1)[0])
from utils import get_db, normalize_bill_number, lookup_bill_id, upsert_position

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
logger = logging.getLogger(__name__)

HOUSE_URL = "https://docs.google.com/spreadsheets/d/e/2PACX-1vRJsN2hpbMAbczSVarp-5N00L66TA__n5nPM8kGxj9NVgEbf7C4CTkW4GzlxFXmoWsHfNvsKZgRy6qs/pub?gid=0&single=true&output=tsv"
SENATE_URL = "https://docs.google.com/spreadsheets/d/e/2PACX-1vRJsN2hpbMAbczSVarp-5N00L66TA__n5nPM8kGxj9NVgEbf7C4CTkW4GzlxFXmoWsHfNvsKZgRy6qs/pub?gid=2105129202&single=true&output=tsv"
ORG_NAME = "CAI"
SOURCE_URL = "https://docs.google.com/spreadsheets/d/e/2PACX-1vRJsN2hpbMAbczSVarp-5N00L66TA__n5nPM8kGxj9NVgEbf7C4CTkW4GzlxFXmoWsHfNvsKZgRy6qs/pubhtml"


def fetch_sheet(url: str) -> list[dict]:
    """Fetch and parse a Google Sheets TSV export."""
    resp = requests.get(url, timeout=30)
    resp.raise_for_status()

    reader = csv.reader(io.StringIO(resp.text), delimiter='\t')
    rows = list(reader)

    if not rows:
        return []

    # First row is header; columns: status | bill_id | category_tag | sponsors | summary | SOP/fiscal_note
    results = []
    for row in rows[1:]:  # Skip header
        if len(row) < 3:
            continue
        results.append({
            "status": row[0].strip() if len(row) > 0 else "",
            "bill_id": row[1].strip() if len(row) > 1 else "",
            "category_tag": row[2].strip() if len(row) > 2 else "",
            "sponsors": row[3].strip() if len(row) > 3 else "",
            "summary": row[4].strip() if len(row) > 4 else "",
        })

    return results


def scrape(dry_run=False):
    """Scrape CAI bill tags from both House and Senate sheets."""
    logger.info("Fetching CAI House sheet")
    house_rows = fetch_sheet(HOUSE_URL)
    logger.info(f"CAI House: {len(house_rows)} rows")

    logger.info("Fetching CAI Senate sheet")
    senate_rows = fetch_sheet(SENATE_URL)
    logger.info(f"CAI Senate: {len(senate_rows)} rows")

    all_rows = house_rows + senate_rows
    logger.info(f"CAI: {len(all_rows)} total rows")

    stats = {"found": 0, "matched": 0, "upserted": 0, "skipped": 0}
    conn = None if dry_run else get_db()

    try:
        for row in all_rows:
            raw_bill = row["bill_id"]
            tag = row["category_tag"]

            if not raw_bill:
                continue

            stats["found"] += 1

            # Skip rows without a meaningful tag
            if not tag or tag.upper() == "N/A":
                stats["skipped"] += 1
                continue

            bill_number = normalize_bill_number(raw_bill)
            if not bill_number:
                logger.warning(f"CAI: Could not normalize {raw_bill!r}")
                stats["skipped"] += 1
                continue

            position = "tag"
            position_detail = f"CAI: {tag}"

            if dry_run:
                print(f"  {bill_number:8s} | {position:8s} | {position_detail}")
                stats["matched"] += 1
                stats["upserted"] += 1
                continue

            bill_id = lookup_bill_id(conn, bill_number)
            if bill_id is None:
                logger.warning(f"CAI: Bill {bill_number} not found in QIBrain — skipping")
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
    parser = argparse.ArgumentParser(description="Scrape CAI bill tags")
    parser.add_argument("--dry-run", action="store_true", help="Print results without inserting")
    args = parser.parse_args()

    result = scrape(dry_run=args.dry_run)
    logger.info(f"CAI: {result}")


if __name__ == "__main__":
    main()
