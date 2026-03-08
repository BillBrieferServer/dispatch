#!/usr/bin/env python3
"""
bls_to_qibrain.py - Ingest BLS economic data into QIBrain database.

Fetches LAUS, QCEW, CPI, JOLTS data from BLS API and stores in
bls_economic_data table. Computes district-level aggregates via
population-weighted county mapping.

Usage:
    cd /opt/billbriefer-sand
    /root/quietimpact/venv/bin/python -m app.etl.bls_to_qibrain
    /root/quietimpact/venv/bin/python -m app.etl.bls_to_qibrain --state ID
    /root/quietimpact/venv/bin/python -m app.etl.bls_to_qibrain --from-cache
"""
import argparse
import json
import os
import sys
from datetime import datetime, date
from pathlib import Path

import psycopg2
from psycopg2.extras import execute_values
from dotenv import load_dotenv

# Load QIBrain database URL
load_dotenv('/root/quietimpact/.env')
# Load BLS API key from billbriefer env
load_dotenv('/opt/billbriefer-sand/.env', override=False)

# Import BLS fetch functions from the app
from app.bls_data_fetch import (
    fetch_state_laus,
    fetch_county_laus,
    fetch_qcew_industry_data,
    fetch_cpi_west,
    fetch_jolts_state,
    LAUS_STATE_SERIES,
    IDAHO_COUNTIES,
    IDAHO_FIPS,
    CPI_WEST_SERIES,
    JOLTS_STATE_SERIES,
    NAICS_SECTORS,
    load_cache,
    CACHE_FILE,
)
from app.district_county_mapping import (
    DISTRICT_COUNTY_MAPPING,
    get_district_bls_estimate,
    get_primary_county,
)
# Patch cache file path for host execution (container path is /app/data/)
import app.bls_data_fetch as _bls_mod
_bls_mod.CACHE_FILE = Path("/opt/billbriefer-sand/data/bls_idaho_cache.json")

# State FIPS codes for --state parameter
STATE_FIPS = {
    'ID': '16',
}


def parse_period_to_date(period_str):
    """Convert BLS period string like 'December 2025' to a date."""
    if not period_str:
        return None
    try:
        dt = datetime.strptime(period_str, '%B %Y')
        return dt.date()
    except ValueError:
        # Try other formats
        try:
            dt = datetime.strptime(period_str, '%Y')
            return date(int(period_str), 1, 1)
        except ValueError:
            return None


def connect_qibrain():
    """Connect to QIBrain database."""
    db_url = os.environ.get('DATABASE_URL')
    if not db_url:
        print("ERROR: DATABASE_URL not set")
        sys.exit(1)
    return psycopg2.connect(db_url)


def upsert_rows(cur, rows):
    """UPSERT rows into bls_economic_data. Returns count of rows upserted."""
    if not rows:
        return 0

    # Deduplicate by unique key (last value wins)
    # Key: (series_type, geography_type, geography_id, state_fips, period_date, metric_name)
    seen = {}
    for row in rows:
        key = (row[0], row[1], row[2], row[3], row[5], row[6])  # series, geo_type, geo_id, fips, period_date, metric_name
        seen[key] = row
    deduped = list(seen.values())

    if len(deduped) < len(rows):
        print(f"  Deduplicated: {len(rows)} -> {len(deduped)} rows")

    sql = """
        INSERT INTO bls_economic_data (
            series_type, geography_type, geography_id, state_fips,
            period_type, period_date, metric_name, metric_value,
            metric_unit, source_series_id, sector_code, sector_name,
            is_derived, fetched_at
        ) VALUES %s
        ON CONFLICT (series_type, geography_type, geography_id, state_fips, period_date, metric_name)
        DO UPDATE SET
            metric_value = EXCLUDED.metric_value,
            metric_unit = EXCLUDED.metric_unit,
            source_series_id = EXCLUDED.source_series_id,
            sector_code = EXCLUDED.sector_code,
            sector_name = EXCLUDED.sector_name,
            is_derived = EXCLUDED.is_derived,
            fetched_at = EXCLUDED.fetched_at
    """

    execute_values(cur, sql, deduped)
    return len(deduped)


def ingest_state_laus(state_data, state_fips):
    """Convert state LAUS data to rows."""
    rows = []
    now = datetime.now()

    metrics = {
        'unemployment_rate': ('percent', 'LASST{fips}0000000000003'),
        'unemployment_count': ('count', 'LASST{fips}0000000000004'),
        'employment_count': ('count', 'LASST{fips}0000000000005'),
        'labor_force': ('count', 'LASST{fips}0000000000006'),
    }

    for metric_name, (unit, series_tmpl) in metrics.items():
        value = state_data.get(metric_name)
        period_str = state_data.get(f'{metric_name}_period')
        if value is not None and period_str:
            period_date = parse_period_to_date(period_str)
            if period_date:
                series_id = series_tmpl.format(fips=state_fips)
                rows.append((
                    'laus', 'state', state_fips, state_fips,
                    'monthly', period_date, metric_name, value,
                    unit, series_id, None, None,
                    False, now
                ))

    # Derived: unemployed_per_opening
    upo = state_data.get('unemployed_per_opening')
    if upo is not None:
        # Use the unemployment_rate period as proxy
        period_str = state_data.get('unemployment_rate_period')
        period_date = parse_period_to_date(period_str) if period_str else None
        if period_date:
            rows.append((
                'laus', 'state', state_fips, state_fips,
                'monthly', period_date, 'unemployed_per_opening', upo,
                'ratio', None, None, None,
                True, now
            ))

    return rows


def ingest_county_laus(county_data, state_fips):
    """Convert county LAUS data to rows."""
    rows = []
    now = datetime.now()

    for county_name, data in county_data.items():
        fips = data.get('fips', '')
        county_fips = fips[2:] if len(fips) == 5 else fips  # strip state prefix
        rate = data.get('unemployment_rate')
        period_str = data.get('period')

        if rate is not None and period_str:
            period_date = parse_period_to_date(period_str)
            if period_date:
                series_id = f"LAUCN{state_fips}{county_fips}0000000003"
                rows.append((
                    'laus', 'county', county_fips, state_fips,
                    'monthly', period_date, 'unemployment_rate', rate,
                    'percent', series_id, None, None,
                    False, now
                ))

    return rows


def ingest_qcew(industry_data, state_fips):
    """Convert QCEW industry data to rows.

    Metric names include sector to avoid UNIQUE constraint collisions
    (sector_code is not part of the unique key).
    Format: employment_agriculture, avg_weekly_wage_manufacturing, etc.
    """
    rows = []
    now = datetime.now()

    for sector_name, data in industry_data.items():
        naics = data.get('naics', '')
        year = data.get('year')
        employment = data.get('employment')
        wage = data.get('avg_weekly_wage')

        if year:
            period_date = date(year, 1, 1)  # annual data

            if employment is not None:
                rows.append((
                    'qcew', 'state', state_fips, state_fips,
                    'annual', period_date, f'employment_{sector_name}', employment,
                    'count', None, naics, sector_name,
                    False, now
                ))

            if wage is not None:
                rows.append((
                    'qcew', 'state', state_fips, state_fips,
                    'annual', period_date, f'avg_weekly_wage_{sector_name}', wage,
                    'dollars', None, naics, sector_name,
                    False, now
                ))

    return rows


def ingest_cpi(cpi_data, state_fips):
    """Convert CPI West Region data to rows."""
    rows = []
    now = datetime.now()

    categories = ['all_items', 'food', 'housing', 'medical_care', 'transportation', 'energy']

    for cat in categories:
        value = cpi_data.get(cat)
        period_str = cpi_data.get(f'{cat}_period')
        series_id = CPI_WEST_SERIES.get(cat)

        if value is not None and period_str:
            period_date = parse_period_to_date(period_str)
            if period_date:
                rows.append((
                    'cpi', 'state', state_fips, state_fips,
                    'monthly', period_date, f'cpi_{cat}', value,
                    'index', series_id, None, None,
                    False, now
                ))

        # YoY change
        yoy = cpi_data.get(f'{cat}_yoy_change')
        if yoy is not None and period_str:
            period_date = parse_period_to_date(period_str)
            if period_date:
                rows.append((
                    'cpi', 'state', state_fips, state_fips,
                    'monthly', period_date, f'cpi_{cat}_yoy_change', yoy,
                    'percent', series_id, None, None,
                    True, now
                ))

    return rows


def ingest_jolts(jolts_data, state_fips):
    """Convert JOLTS data to rows."""
    rows = []
    now = datetime.now()

    metrics = {
        'job_openings': ('count', 'thousands'),
        'job_openings_rate': ('percent', None),
        'hires': ('count', 'thousands'),
        'hires_rate': ('percent', None),
        'quits': ('count', 'thousands'),
        'quits_rate': ('percent', None),
        'layoffs': ('count', 'thousands'),
        'layoffs_rate': ('percent', None),
    }

    for metric_name, (unit, note) in metrics.items():
        value = jolts_data.get(metric_name)
        period_str = jolts_data.get(f'{metric_name}_period')
        series_id = JOLTS_STATE_SERIES.get(metric_name)

        if value is not None and period_str:
            period_date = parse_period_to_date(period_str)
            if period_date:
                # For JOLTS counts in thousands, store as-is with unit 'thousands'
                actual_unit = unit
                if note == 'thousands':
                    actual_unit = 'thousands'
                rows.append((
                    'jolts', 'state', state_fips, state_fips,
                    'monthly', period_date, metric_name, value,
                    actual_unit, series_id, None, None,
                    False, now
                ))

    return rows


def ingest_district_estimates(county_data, state_fips):
    """Compute and ingest district-level unemployment estimates."""
    rows = []
    now = datetime.now()

    # Get a representative period from any county
    period_date = None
    for cdata in county_data.values():
        period_str = cdata.get('period')
        if period_str:
            period_date = parse_period_to_date(period_str)
            break

    if not period_date:
        print("  WARNING: No county period found, skipping district estimates")
        return rows

    for district in DISTRICT_COUNTY_MAPPING:
        district_num = district.replace('LD', '')
        rate = get_district_bls_estimate(district, county_data, 'unemployment_rate')

        if rate is not None:
            rows.append((
                'laus', 'sld_upper', district_num, state_fips,
                'monthly', period_date, 'unemployment_rate', rate,
                'percent', None, None, None,
                True, now
            ))

    return rows


def ingest_from_cache(cache_path, state_fips):
    """Ingest BLS data from existing cache file instead of API."""
    print(f"Loading from cache: {cache_path}")
    with open(cache_path) as f:
        cache = json.load(f)

    all_rows = []
    all_rows.extend(ingest_state_laus(cache.get('state', {}), state_fips))
    all_rows.extend(ingest_county_laus(cache.get('counties', {}), state_fips))
    all_rows.extend(ingest_qcew(cache.get('industries', {}), state_fips))
    all_rows.extend(ingest_cpi(cache.get('cpi', {}), state_fips))
    all_rows.extend(ingest_jolts(cache.get('jolts', {}), state_fips))
    all_rows.extend(ingest_district_estimates(cache.get('counties', {}), state_fips))

    return all_rows, cache.get('metadata', {})


def ingest_from_api(state_fips):
    """Fetch fresh BLS data from API and prepare rows."""
    print("Fetching BLS data from API...")

    state_data = fetch_state_laus()
    county_data = fetch_county_laus()
    industry_data = fetch_qcew_industry_data()
    cpi_data = fetch_cpi_west()
    jolts_data = fetch_jolts_state()
    # OEWS skipped — known blocked by BLS

    # Compute unemployed_per_opening if we have both pieces
    if jolts_data.get('job_openings') and state_data.get('unemployment_count'):
        try:
            job_k = jolts_data['job_openings']
            unemp_k = state_data['unemployment_count'] / 1000
            state_data['unemployed_per_opening'] = round(unemp_k / job_k, 2)
        except (ZeroDivisionError, TypeError):
            pass

    all_rows = []
    all_rows.extend(ingest_state_laus(state_data, state_fips))
    all_rows.extend(ingest_county_laus(county_data, state_fips))
    all_rows.extend(ingest_qcew(industry_data, state_fips))
    all_rows.extend(ingest_cpi(cpi_data, state_fips))
    all_rows.extend(ingest_jolts(jolts_data, state_fips))
    all_rows.extend(ingest_district_estimates(county_data, state_fips))

    metadata = {
        'state_fields': len(state_data),
        'counties_count': len(county_data),
        'industries_count': len(industry_data),
        'cpi_categories': len([k for k in cpi_data if not k.endswith('_period') and not k.endswith('_change')]),
        'jolts_metrics': len([k for k in jolts_data if not k.endswith('_period')]),
    }

    return all_rows, metadata


def update_data_freshness(cur):
    """Update data_freshness entry for BLS data."""
    cur.execute("""
        SELECT source_name FROM data_freshness WHERE source_name = 'bls_economic'
    """)
    exists = cur.fetchone()

    if exists:
        cur.execute("""
            UPDATE data_freshness
            SET last_updated = now(),
                source_date = (SELECT max(period_date) FROM bls_economic_data),
                row_count = (SELECT count(*) FROM bls_economic_data),
                table_names = ARRAY['bls_economic_data'],
                refresh_method = 'etl_script',
                refresh_instructions = 'cd /opt/billbriefer-sand && /root/quietimpact/venv/bin/python -m app.etl.bls_to_qibrain',
                freshness_target_days = 30,
                notes = 'BLS data stored in QIBrain. Not yet wired into briefer generation pipeline. Section 9 uses Census ACS for employment data.'
            WHERE source_name = 'bls_economic'
        """)
    else:
        cur.execute("""
            INSERT INTO data_freshness (
                source_name, last_updated, source_date, row_count,
                table_names, refresh_method, refresh_instructions,
                freshness_target_days, notes
            ) VALUES (
                'bls_economic', now(),
                (SELECT max(period_date) FROM bls_economic_data),
                (SELECT count(*) FROM bls_economic_data),
                ARRAY['bls_economic_data'], 'etl_script',
                'cd /opt/billbriefer-sand && /root/quietimpact/venv/bin/python -m app.etl.bls_to_qibrain',
                30,
                'BLS data stored in QIBrain. Not yet wired into briefer generation pipeline. Section 9 uses Census ACS for employment data.'
            )
        """)


def main():
    parser = argparse.ArgumentParser(description='Ingest BLS data into QIBrain')
    parser.add_argument('--state', default='ID', help='State abbreviation (default: ID)')
    parser.add_argument('--from-cache', action='store_true',
                        help='Load from existing cache file instead of fetching from API')
    parser.add_argument('--cache-file', default=None,
                        help='Path to cache file (default: container cache)')
    args = parser.parse_args()

    state_fips = STATE_FIPS.get(args.state)
    if not state_fips:
        print(f"ERROR: Unknown state '{args.state}'. Supported: {', '.join(STATE_FIPS.keys())}")
        sys.exit(1)

    print(f"=== BLS to QIBrain Ingestion ===")
    print(f"State: {args.state} (FIPS: {state_fips})")
    print(f"Time: {datetime.now().isoformat()}")
    print()

    # Get data
    if args.from_cache:
        cache_path = args.cache_file or '/opt/billbriefer-sand/data/bls_idaho_cache.json'
        if not Path(cache_path).exists():
            # Try container volume mount path
            cache_path = '/opt/billbriefer-sand/app/data/bls_idaho_cache.json'
        if not Path(cache_path).exists():
            print(f"ERROR: Cache file not found at {cache_path}")
            sys.exit(1)
        all_rows, metadata = ingest_from_cache(cache_path, state_fips)
    else:
        all_rows, metadata = ingest_from_api(state_fips)

    print(f"\nPrepared {len(all_rows)} rows for ingestion")

    # Count by series type
    series_counts = {}
    geo_counts = {}
    for row in all_rows:
        st = row[0]
        gt = row[1]
        series_counts[st] = series_counts.get(st, 0) + 1
        geo_counts[gt] = geo_counts.get(gt, 0) + 1

    print("\nBy series type:")
    for st, count in sorted(series_counts.items()):
        print(f"  {st:10s}: {count} rows")
    print("\nBy geography:")
    for gt, count in sorted(geo_counts.items()):
        print(f"  {gt:10s}: {count} rows")

    # Write to QIBrain
    conn = connect_qibrain()
    try:
        cur = conn.cursor()

        count = upsert_rows(cur, all_rows)
        print(f"\nUpserted {count} rows into bls_economic_data")

        # Update data_freshness
        update_data_freshness(cur)
        print("Updated data_freshness entry")

        conn.commit()
        print("\nIngestion complete!")

        # Verify
        cur.execute("""
            SELECT series_type, geography_type, is_derived, count(*),
                   min(period_date), max(period_date)
            FROM bls_economic_data
            WHERE state_fips = %s
            GROUP BY series_type, geography_type, is_derived
            ORDER BY series_type, geography_type
        """, (state_fips,))

        print("\n=== Verification ===")
        print(f"{'Series':10s} {'Geography':10s} {'Derived':8s} {'Count':6s} {'Min Date':12s} {'Max Date':12s}")
        print("-" * 60)
        for row in cur.fetchall():
            print(f"{row[0]:10s} {row[1]:10s} {'yes' if row[2] else 'no':8s} {row[3]:<6d} {str(row[4]):12s} {str(row[5]):12s}")

        cur.execute("""
            SELECT count(DISTINCT geography_id) FROM bls_economic_data
            WHERE geography_type = 'county' AND state_fips = %s
        """, (state_fips,))
        county_count = cur.fetchone()[0]
        print(f"\nCounty coverage: {county_count} (expected 44)")

        cur.execute("""
            SELECT count(DISTINCT geography_id) FROM bls_economic_data
            WHERE geography_type = 'sld_upper' AND state_fips = %s
        """, (state_fips,))
        district_count = cur.fetchone()[0]
        print(f"District coverage: {district_count} (expected 35)")

        cur.close()
    except Exception as e:
        conn.rollback()
        print(f"ERROR: {e}")
        raise
    finally:
        conn.close()


if __name__ == '__main__':
    main()
