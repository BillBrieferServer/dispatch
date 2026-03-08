#!/usr/bin/env python3
"""
Scrape IFF (Idaho Freedom Foundation) bill ratings.

Source: https://idahofreedom.org
Method: WordPress REST API — no auth

Categories 1469 (House), 1470 (Senate), 1472 (Senate spending).
Score is in post title only.
"""
import argparse
import logging
import re
import sys

import requests

sys.path.insert(0, __file__.rsplit('/', 1)[0])
from utils import get_db, normalize_bill_number, lookup_bill_id, upsert_position

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
logger = logging.getLogger(__name__)

API_URL = "https://idahofreedom.org/wp-json/wp/v2/posts"
CATEGORIES = "1469,1470,1472"
ORG_NAME = "IFF"
HEADERS = {"User-Agent": "DispatchBriefer/1.0 (bill tracker)"}

# Full text → prefix mapping
TYPE_MAP = {
    'House Bill': 'H',
    'Senate Bill': 'S',
    'House Joint Resolution': 'HJR',
    'Senate Joint Resolution': 'SJR',
    'House Concurrent Resolution': 'HCR',
    'Senate Concurrent Resolution': 'SCR',
    'House Joint Memorial': 'HJM',
    'Senate Joint Memorial': 'SJM',
}

# Build regex: "House Bill 840" etc.
BILL_RE = re.compile(
    r'(' + '|'.join(re.escape(k) for k in TYPE_MAP) + r')\s+(\d+)',
    re.IGNORECASE
)
SCORE_RE = re.compile(r'\(([+-]?\d+)\)')


def fetch_all_posts():
    """Fetch all IFF bill rating posts, handling pagination."""
    all_posts = []
    page = 1
    per_page = 100

    while True:
        logger.info(f"Fetching IFF posts page {page}")
        resp = requests.get(API_URL, params={
            "categories": CATEGORIES,
            "per_page": per_page,
            "page": page,
            "_fields": "title,link,date",
        }, headers=HEADERS, timeout=30)

        if resp.status_code == 400:
            # No more pages
            break

        resp.raise_for_status()
        posts = resp.json()

        if not posts:
            break

        all_posts.extend(posts)

        # Check if there are more pages
        total_pages = int(resp.headers.get("X-WP-TotalPages", 1))
        if page >= total_pages:
            break
        page += 1

    return all_posts


def parse_post(post):
    """Parse a post title into (bill_number, score, link)."""
    title = post.get("title", {})
    if isinstance(title, dict):
        title = title.get("rendered", "")
    title = (title or "").strip()
    link = post.get("link", "")

    # Extract bill number
    m = BILL_RE.search(title)
    if not m:
        return None, None, link

    bill_type = m.group(1)
    bill_num = m.group(2)

    # Normalize prefix
    prefix = None
    for full_name, pfx in TYPE_MAP.items():
        if full_name.lower() == bill_type.lower():
            prefix = pfx
            break

    if not prefix:
        return None, None, link

    bill_number = normalize_bill_number(f"{prefix}{bill_num}")

    # Extract score
    score_m = SCORE_RE.search(title)
    score = int(score_m.group(1)) if score_m else None

    return bill_number, score, link


def scrape(dry_run=False):
    """Scrape IFF bill ratings."""
    posts = fetch_all_posts()
    logger.info(f"IFF: {len(posts)} posts found")

    stats = {"found": len(posts), "matched": 0, "upserted": 0, "skipped": 0}
    conn = None if dry_run else get_db()

    try:
        for post in posts:
            bill_number, score, link = parse_post(post)

            if not bill_number:
                stats["skipped"] += 1
                continue

            if score is None:
                stats["skipped"] += 1
                continue

            # Map score to position
            if score > 0:
                position = "support"
            elif score < 0:
                position = "oppose"
            else:
                position = "neutral"

            position_detail = f"IFF: {'+' if score > 0 else ''}{score}"

            if dry_run:
                print(f"  {bill_number:8s} | {position:8s} | {position_detail:10s} | {link}")
                stats["matched"] += 1
                stats["upserted"] += 1
                continue

            bill_id = lookup_bill_id(conn, bill_number)
            if bill_id is None:
                logger.warning(f"IFF: Bill {bill_number} not found in QIBrain — skipping")
                stats["skipped"] += 1
                continue

            stats["matched"] += 1
            upsert_position(conn, bill_id, ORG_NAME, position, position_detail, link)
            stats["upserted"] += 1

        if conn:
            conn.commit()
    finally:
        if conn:
            conn.close()

    return stats


def main():
    parser = argparse.ArgumentParser(description="Scrape IFF bill ratings")
    parser.add_argument("--dry-run", action="store_true", help="Print results without inserting")
    args = parser.parse_args()

    result = scrape(dry_run=args.dry_run)
    logger.info(f"IFF: {result}")


if __name__ == "__main__":
    main()
