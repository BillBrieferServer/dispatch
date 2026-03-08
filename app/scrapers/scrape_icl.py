#!/usr/bin/env python3
"""
Scrape ICL (Idaho Conservation League) bill tracker.

Source: https://idahoconservation.org/icl-bill-tracker
Method: Squarespace server-rendered HTML — requests + lxml, paginated.
"""
import argparse
import logging
import re
import sys

import requests
from lxml import html as lxml_html

sys.path.insert(0, __file__.rsplit('/', 1)[0])
from utils import get_db, normalize_bill_number, lookup_bill_id, upsert_position

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
logger = logging.getLogger(__name__)

TRACKER_URL = "https://idahoconservation.org/icl-bill-tracker"
ORG_NAME = "ICL"
HEADERS = {"User-Agent": "Mozilla/5.0 (compatible; DispatchBriefer/1.0)"}

# Position mapping
POSITION_MAP = {
    "support": "support",
    "oppose": "oppose",
    "neutral": "neutral",
    "under review": "monitor",
    "review": "monitor",
    "monitor": "monitor",
}

# Extract bill number from anywhere in text (HMJ is ICL's spelling of HJM)
BILL_RE = re.compile(r'(HB|SB|HJR|SJR|HCR|SCR|HJM|SJM|HMJ|SMJ)\s*(\d+)', re.IGNORECASE)

# Position: one of the known values, followed by non-alpha (e.g. "NeutralCurrent")
POSITION_RE = re.compile(r'Position:\s*(Support|Oppose|Neutral|Under Review|Monitor)', re.IGNORECASE)


def fetch_all_cards():
    """Fetch all bill cards, following pagination."""
    all_cards = []
    url = TRACKER_URL

    while url:
        logger.info(f"Fetching ICL page: {url}")
        resp = requests.get(url, headers=HEADERS, timeout=30)
        resp.raise_for_status()
        tree = lxml_html.fromstring(resp.text)

        # Squarespace blog collection items
        cards = tree.xpath('//article')

        for card in cards:
            text = card.text_content()
            if not text:
                continue

            bill_match = BILL_RE.search(text)
            if not bill_match:
                continue

            # Normalize HMJ -> HJM, SMJ -> SJM
            prefix = bill_match.group(1).upper()
            if prefix == "HMJ":
                prefix = "HJM"
            elif prefix == "SMJ":
                prefix = "SJM"

            pos_match = POSITION_RE.search(text)
            position_raw = pos_match.group(1).strip() if pos_match else ""

            # Get link
            links = card.xpath('.//a/@href')
            card_url = links[0] if links else ""
            if card_url and not card_url.startswith("http"):
                card_url = "https://idahoconservation.org" + card_url

            all_cards.append({
                "raw_bill": f"{prefix} {bill_match.group(2)}",
                "position_raw": position_raw,
                "url": card_url,
            })

        # Pagination
        next_link = tree.xpath('//a[contains(text(), "Older")]/@href | //a[contains(@class, "next")]/@href')
        if next_link:
            next_url = next_link[0]
            if not next_url.startswith("http"):
                next_url = "https://idahoconservation.org" + next_url
            if next_url != url:
                url = next_url
                continue

        url = None

    return all_cards


def scrape(dry_run=False):
    """Scrape ICL bill positions."""
    cards = fetch_all_cards()
    logger.info(f"ICL: {len(cards)} bill cards found")

    stats = {"found": len(cards), "matched": 0, "upserted": 0, "skipped": 0}
    conn = None if dry_run else get_db()

    try:
        for card in cards:
            bill_number = normalize_bill_number(card["raw_bill"])
            if not bill_number:
                logger.warning(f"ICL: Could not normalize {card['raw_bill']!r}")
                stats["skipped"] += 1
                continue

            pos_lower = card["position_raw"].lower().strip()
            position = POSITION_MAP.get(pos_lower)
            if not position:
                if pos_lower:
                    logger.warning(f"ICL: Unknown position {card['position_raw']!r} for {bill_number}")
                stats["skipped"] += 1
                continue

            source_url = card["url"] or TRACKER_URL

            if dry_run:
                print(f"  {bill_number:8s} | {position:8s} | {card['position_raw']}")
                stats["matched"] += 1
                stats["upserted"] += 1
                continue

            bill_id = lookup_bill_id(conn, bill_number)
            if bill_id is None:
                logger.warning(f"ICL: Bill {bill_number} not found in QIBrain — skipping")
                stats["skipped"] += 1
                continue

            stats["matched"] += 1
            upsert_position(conn, bill_id, ORG_NAME, position, card["position_raw"], source_url)
            stats["upserted"] += 1

        if conn:
            conn.commit()
    finally:
        if conn:
            conn.close()

    return stats


def main():
    parser = argparse.ArgumentParser(description="Scrape ICL bill positions")
    parser.add_argument("--dry-run", action="store_true", help="Print results without inserting")
    args = parser.parse_args()

    result = scrape(dry_run=args.dry_run)
    logger.info(f"ICL: {result}")


if __name__ == "__main__":
    main()
