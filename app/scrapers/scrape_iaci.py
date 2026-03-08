#!/usr/bin/env python3
"""
Scrape IACI (Idaho Association of Commerce & Industry) bill positions and legislator scores.

Source: https://scorecard.iaci.org
Method: Custom WordPress REST plugin — no auth required

Bill positions: GET /wp-json/rds-bt50-scorecard/v1/bills
  Response: {"scorecards": [{"scorecard": {...}, "bills": [...]}, ...]}
  We use only the first (current year) scorecard.

Legislator scores: GET /wp-json/rds-bt50-scorecard/v1/legislators
  Response: list of legislator objects with score_sets array.
"""
import argparse
import logging
import sys

import requests

sys.path.insert(0, __file__.rsplit('/', 1)[0])
from utils import get_db, normalize_bill_number, lookup_bill_id, upsert_position, upsert_legislator_score, resolve_legislator_id

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
logger = logging.getLogger(__name__)

BILLS_URL = "https://scorecard.iaci.org/wp-json/rds-bt50-scorecard/v1/bills"
LEGISLATORS_URL = "https://scorecard.iaci.org/wp-json/rds-bt50-scorecard/v1/legislators"
ORG_NAME = "IACI"
SOURCE_URL = "https://scorecard.iaci.org"

# Position mapping from comments field
POSITION_MAP = {
    "support and score": ("support", "Support and Score"),
    "oppose and score": ("oppose", "Oppose and Score"),
    "oppose and score*": ("oppose", "Oppose and Score*"),
    "oppose, not scored": ("oppose", "Oppose, not Scored"),
    "opposed, not scored": ("oppose", "Oppose, not Scored"),
    "opposed, not scored.": ("oppose", "Oppose, not Scored"),
    "support, not scored": ("support", "Support, not Scored"),
    "monitor": ("monitor", "Monitor"),
}


def scrape_bills(dry_run=False):
    """Scrape IACI bill positions (current year scorecard only)."""
    logger.info(f"Fetching IACI bills from {BILLS_URL}")
    resp = requests.get(BILLS_URL, timeout=30)
    resp.raise_for_status()
    data = resp.json()

    # Navigate nested structure
    scorecards = data.get("scorecards", [])
    if not scorecards:
        logger.error("IACI: No scorecards found")
        return {"found": 0, "matched": 0, "upserted": 0, "skipped": 0}

    # Use first (most recent / current year) scorecard
    current = scorecards[0]
    sc_info = current.get("scorecard", {})
    bills = current.get("bills", [])
    logger.info(f"IACI: Using scorecard '{sc_info.get('scorecardName', '?')}' — {len(bills)} bills")

    stats = {"found": len(bills), "matched": 0, "upserted": 0, "skipped": 0}
    conn = None if dry_run else get_db()

    try:
        for bill in bills:
            bill_number_raw = (bill.get("stateBillID") or "").strip()
            comments = (bill.get("comments") or "").strip()

            if not bill_number_raw:
                stats["skipped"] += 1
                continue

            normalized = normalize_bill_number(bill_number_raw)
            if not normalized:
                logger.warning(f"IACI: Could not normalize {bill_number_raw!r}")
                stats["skipped"] += 1
                continue

            # Map position (case-insensitive, strip trailing whitespace/periods)
            comments_key = comments.lower().rstrip(". ")
            if comments_key in POSITION_MAP:
                position, position_detail = POSITION_MAP[comments_key]
            elif comments:
                logger.warning(f"IACI: Unknown position comment {comments!r} for {normalized}")
                position = "monitor"
                position_detail = comments
            else:
                stats["skipped"] += 1
                continue

            if dry_run:
                print(f"  {normalized:8s} | {position:8s} | {position_detail}")
                stats["matched"] += 1
                stats["upserted"] += 1
                continue

            bill_id = lookup_bill_id(conn, normalized)
            if bill_id is None:
                logger.warning(f"IACI: Bill {normalized} not found in QIBrain — skipping")
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


def scrape_legislators(dry_run=False):
    """Scrape IACI legislator scores."""
    logger.info(f"Fetching IACI legislators from {LEGISLATORS_URL}")
    resp = requests.get(LEGISLATORS_URL, timeout=30)
    resp.raise_for_status()
    legislators = resp.json()

    if not isinstance(legislators, list):
        logger.error(f"IACI: Expected list, got {type(legislators)}")
        return {"found": 0, "scores_upserted": 0}

    logger.info(f"IACI: {len(legislators)} legislators found")

    stats = {"found": len(legislators), "scores_upserted": 0}
    conn = None if dry_run else get_db()

    try:
        for leg in legislators:
            first = (leg.get("first_name") or "").strip()
            last = (leg.get("last_name") or "").strip()
            name = f"{first} {last}".strip()
            chamber = (leg.get("chamber") or "").strip()
            district = (leg.get("district") or "").strip()

            if not name:
                continue

            # Process score_sets (multi-year)
            score_sets = leg.get("score_sets", [])
            if not isinstance(score_sets, list):
                continue

            for ss in score_sets:
                year_str = ss.get("niceTitle", "")
                try:
                    year = int(year_str)
                except (ValueError, TypeError):
                    continue

                try:
                    score = float(ss.get("score")) if ss.get("score") is not None else None
                    possible = float(ss.get("possible_score")) if ss.get("possible_score") is not None else None
                    vi = float(ss.get("vote_index")) if ss.get("vote_index") is not None else None
                except (ValueError, TypeError):
                    continue

                if dry_run:
                    print(f"  {name:30s} | {chamber:6s} | {district:8s} | {year} | score={score} poss={possible} vi={vi}")
                    stats["scores_upserted"] += 1
                    continue

                leg_id = resolve_legislator_id(conn, name, chamber, district)
                upsert_legislator_score(
                    conn, name, chamber, district, ORG_NAME, year,
                    score, possible, vi, SOURCE_URL, legislator_id=leg_id
                )
                stats["scores_upserted"] += 1

        if conn:
            conn.commit()
    finally:
        if conn:
            conn.close()

    return stats


def main():
    parser = argparse.ArgumentParser(description="Scrape IACI bill positions and legislator scores")
    parser.add_argument("--dry-run", action="store_true", help="Print results without inserting")
    parser.add_argument("--bills-only", action="store_true", help="Only scrape bill positions")
    parser.add_argument("--scores-only", action="store_true", help="Only scrape legislator scores")
    args = parser.parse_args()

    if args.scores_only:
        stats = scrape_legislators(dry_run=args.dry_run)
        logger.info(f"IACI Legislators: {stats}")
        return stats

    bill_stats = scrape_bills(dry_run=args.dry_run)
    logger.info(f"IACI Bills: {bill_stats}")

    if not args.bills_only:
        leg_stats = scrape_legislators(dry_run=args.dry_run)
        logger.info(f"IACI Legislators: {leg_stats}")
        return {**bill_stats, **leg_stats}

    return bill_stats


if __name__ == "__main__":
    main()
