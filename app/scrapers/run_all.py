#!/usr/bin/env python3
"""
Run all Dispatch advocacy scrapers.

Usage:
    python run_all.py                          # Run all 7 scrapers
    python run_all.py --sources iaci,iff,aclu  # Run specific scrapers
    python run_all.py --dry-run                # Dry run all scrapers
"""
import argparse
import logging
import sys
import traceback
from datetime import datetime, timezone

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
logger = logging.getLogger(__name__)

# Import all scrapers
sys.path.insert(0, __file__.rsplit('/', 1)[0])

SCRAPER_REGISTRY = {
    "iaci": ("IACI", "scrape_iaci"),
    "iff": ("IFF", "scrape_iff"),
    "aclu": ("ACLU Idaho", "scrape_aclu"),
    "cvi": ("CVI", "scrape_cvi"),
    "iwf": ("IWF", "scrape_iwf"),
    "cai": ("CAI", "scrape_cai"),
    "icl": ("ICL", "scrape_icl"),
}

# Default run order
ALL_SOURCES = ["iaci", "iff", "aclu", "cvi", "iwf", "cai", "icl"]

# Daily sources (Mon-Sat)
DAILY_SOURCES = ["iaci", "iff", "aclu", "icl", "cvi"]


def run_scraper(key: str, dry_run: bool = False) -> dict:
    """Import and run a single scraper, return its stats."""
    display_name, module_name = SCRAPER_REGISTRY[key]
    mod = __import__(module_name)

    if key == "iaci":
        # IACI has separate bills + legislators
        bill_stats = mod.scrape_bills(dry_run=dry_run)
        leg_stats = mod.scrape_legislators(dry_run=dry_run)
        return {**bill_stats, "leg_scores": leg_stats.get("scores_upserted", 0)}
    else:
        return mod.scrape(dry_run=dry_run)


def main():
    parser = argparse.ArgumentParser(description="Run Dispatch advocacy scrapers")
    parser.add_argument("--dry-run", action="store_true", help="Print results without inserting")
    parser.add_argument("--sources", type=str, default=None,
                        help="Comma-separated list of sources (default: all)")
    args = parser.parse_args()

    if args.sources:
        sources = [s.strip().lower() for s in args.sources.split(",")]
        # Validate
        for s in sources:
            if s not in SCRAPER_REGISTRY:
                print(f"ERROR: Unknown source '{s}'. Valid: {', '.join(ALL_SOURCES)}")
                sys.exit(1)
    else:
        sources = ALL_SOURCES

    timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    print(f"\nADVOCACY SCRAPER RUN — {timestamp}")
    print("=" * 60)

    total_positions = 0
    results = {}

    for key in sources:
        display_name = SCRAPER_REGISTRY[key][0]
        try:
            stats = run_scraper(key, dry_run=args.dry_run)
            results[key] = stats

            found = stats.get("found", 0)
            matched = stats.get("matched", 0)
            upserted = stats.get("upserted", 0)
            skipped = stats.get("skipped", 0)
            total_positions += upserted

            line = f"{display_name:12s} {found:4d} found, {matched:4d} matched, {upserted:4d} upserted, {skipped:4d} skipped"
            if "leg_scores" in stats:
                line += f", {stats['leg_scores']} legislator scores"
            print(line)

        except Exception as e:
            logger.error(f"{display_name}: FAILED — {e}")
            traceback.print_exc()
            results[key] = {"error": str(e)}
            print(f"{display_name:12s} FAILED: {e}")

    print("=" * 60)
    prefix = "[DRY RUN] " if args.dry_run else ""
    print(f"{prefix}TOTAL: {total_positions} positions loaded")
    print()


if __name__ == "__main__":
    main()
