#!/usr/bin/env python3
"""
Scrape CVI (Conservation Voters for Idaho) bill tracker.

Source: https://cvidaho.org/2026-bill-tracker/
Method: jQuery DataTables — parse JSON data from script tags in page HTML.
Fallback: extract bill IDs from legislature URLs.
"""
import argparse
import json
import logging
import re
import sys

import requests

sys.path.insert(0, __file__.rsplit('/', 1)[0])
from utils import get_db, normalize_bill_number, lookup_bill_id, upsert_position

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
logger = logging.getLogger(__name__)

TRACKER_URL = "https://cvidaho.org/2026-bill-tracker/"
ORG_NAME = "CVI"
HEADERS = {"User-Agent": "Mozilla/5.0 (compatible; DispatchBriefer/1.0)"}

# Position mapping
POSITION_MAP = {
    "support": "support",
    "oppose": "oppose",
    "monitor": "monitor",
    "neutral": "neutral",
}


def extract_bill_from_url(url: str) -> str | None:
    """Extract bill number from legislature.idaho.gov URL."""
    m = re.search(r'/legislation/([A-Z]+\d+)/?', url)
    if m:
        return m.group(1)
    return None


def scrape(dry_run=False):
    """Scrape CVI bill tracker."""
    logger.info(f"Fetching CVI tracker from {TRACKER_URL}")
    resp = requests.get(TRACKER_URL, headers=HEADERS, timeout=30)
    resp.raise_for_status()
    html = resp.text

    # Try to find DataTables initialization data in script tags
    # Look for JSON arrays that contain bill data
    rows = []

    # Pattern 1: DataTables data array in script
    dt_match = re.search(r'(?:data|aaData|aoData)\s*[:=]\s*(\[\s*\[.+?\]\s*\])', html, re.DOTALL)
    if dt_match:
        try:
            data = json.loads(dt_match.group(1))
            for row in data:
                if isinstance(row, list) and len(row) >= 4:
                    rows.append(row)
        except json.JSONDecodeError:
            pass

    # Pattern 2: Look for a var with bill data
    if not rows:
        var_match = re.search(r'var\s+\w+\s*=\s*(\[\s*\[.+?\]\s*\]);', html, re.DOTALL)
        if var_match:
            try:
                data = json.loads(var_match.group(1))
                for row in data:
                    if isinstance(row, list) and len(row) >= 4:
                        rows.append(row)
            except json.JSONDecodeError:
                pass

    # Pattern 3: Parse table rows directly from HTML
    if not rows:
        logger.info("CVI: No DataTables JSON found, parsing HTML table rows")
        try:
            from lxml import html as lxml_html
            tree = lxml_html.fromstring(html)
            # Find the main data table
            for table in tree.xpath('//table'):
                trs = table.xpath('.//tbody/tr')
                for tr in trs:
                    tds = tr.xpath('.//td')
                    if len(tds) >= 4:
                        # Extract text and links
                        cells = []
                        for td in tds:
                            text = td.text_content().strip()
                            # Check for links
                            links = td.xpath('.//a/@href')
                            cells.append((text, links[0] if links else ""))
                        rows.append(cells)
        except Exception as e:
            logger.error(f"CVI: HTML parsing failed: {e}")

    if not rows:
        logger.error("CVI: Could not extract any bill data from page")
        return {"found": 0, "matched": 0, "upserted": 0, "skipped": 0}

    logger.info(f"CVI: {len(rows)} rows found")

    stats = {"found": len(rows), "matched": 0, "upserted": 0, "skipped": 0}
    conn = None if dry_run else get_db()

    try:
        for row in rows:
            # Handle both list-of-strings and list-of-tuples formats
            if isinstance(row[0], tuple):
                # (text, link) format from HTML parsing
                raw_bill = row[0][0]
                position_text = row[3][0] if len(row) > 3 else ""
                leg_url = row[5][1] if len(row) > 5 and row[5][1] else ""
            else:
                # Plain string format from DataTables JSON
                # Strip HTML tags
                raw_bill = re.sub(r'<[^>]+>', '', str(row[0])).strip()
                position_text = re.sub(r'<[^>]+>', '', str(row[3])).strip() if len(row) > 3 else ""
                leg_url = ""
                # Try to extract URL from HTML in any column
                for cell in row:
                    url_m = re.search(r'href=["\']([^"\']*legislature\.idaho\.gov[^"\']*)["\']', str(cell))
                    if url_m:
                        leg_url = url_m.group(1)
                        break

            if not raw_bill:
                stats["skipped"] += 1
                continue

            # Normalize bill number — try direct first, then from URL
            bill_number = normalize_bill_number(raw_bill)
            if not bill_number and leg_url:
                fallback = extract_bill_from_url(leg_url)
                if fallback:
                    bill_number = normalize_bill_number(fallback)

            if not bill_number:
                logger.warning(f"CVI: Could not normalize {raw_bill!r}")
                stats["skipped"] += 1
                continue

            # Map position
            pos_lower = position_text.lower().strip()
            position = POSITION_MAP.get(pos_lower)
            if not position:
                if pos_lower:
                    logger.warning(f"CVI: Unknown position {position_text!r} for {bill_number}")
                stats["skipped"] += 1
                continue

            source_url = leg_url or TRACKER_URL

            if dry_run:
                print(f"  {bill_number:8s} | {position:8s} | {source_url}")
                stats["matched"] += 1
                stats["upserted"] += 1
                continue

            bill_id = lookup_bill_id(conn, bill_number)
            if bill_id is None:
                logger.warning(f"CVI: Bill {bill_number} not found in QIBrain — skipping")
                stats["skipped"] += 1
                continue

            stats["matched"] += 1
            upsert_position(conn, bill_id, ORG_NAME, position, position_text, source_url)
            stats["upserted"] += 1

        if conn:
            conn.commit()
    finally:
        if conn:
            conn.close()

    return stats


def main():
    parser = argparse.ArgumentParser(description="Scrape CVI bill positions")
    parser.add_argument("--dry-run", action="store_true", help="Print results without inserting")
    args = parser.parse_args()

    result = scrape(dry_run=args.dry_run)
    logger.info(f"CVI: {result}")


if __name__ == "__main__":
    main()
