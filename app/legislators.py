"""
legislators.py

Load Idaho legislator data from QIBrain and provide lookup functions.
Generates personalized headers and unique briefer IDs.
"""

import os
import hashlib
from datetime import datetime
from typing import Dict, Any, Optional


# Configuration
STATE_CODE = os.getenv("STATE_CODE", "ID").upper()
SESSION_YEAR = os.getenv("SESSION_YEAR", "2026").strip()

# Module-level cache
LEGISLATORS: Dict[str, Dict[str, Any]] = {}


def _load_from_qibrain() -> Dict[str, Dict[str, Any]]:
    """
    Load legislators from QIBrain PostgreSQL.
    Returns dict keyed by lowercase email, or empty dict on failure.
    """
    try:
        from app.services.qibrain_data import get_all_legislators
        qibrain_legs = get_all_legislators()
        if not qibrain_legs:
            return {}

        legislators = {}
        for leg in qibrain_legs:
            email = (leg.get("email") or "").strip().lower()
            if not email or "@" not in email:
                continue

            title = leg.get("title", "")
            first_name = leg.get("first_name", "")
            last_name = leg.get("last_name", "")
            district = int(leg.get("district") or 0)
            seat = (leg.get("seat") or "").strip().upper()
            party = leg.get("party", "")
            chamber = leg.get("chamber", "")

            # Build derived fields matching Excel-loaded structure
            first_initial = first_name[0] if first_name else ""
            ld_code = f"LD{district:02d}{seat}"
            display_name = f"{title} {first_initial} {last_name}".strip()

            legislators[email] = {
                "chamber": chamber,
                "title": title,
                "first_name": first_name,
                "first_initial": first_initial,
                "last_name": last_name,
                "district": district,
                "seat": seat,
                "party": party,
                "email": leg.get("email", "").strip(),  # Original case
                "committees": "",  # Not in QIBrain yet
                "full_name": f"{first_name} {last_name}",
                "display_name": display_name,
                "ld_code": ld_code,
            }

        return legislators
    except Exception as e:
        print(f"QIBrain legislator load failed: {e}")
        return {}


def load_legislators() -> Dict[str, Dict[str, Any]]:
    """
    Load all legislators from QIBrain.

    Returns:
        Dictionary keyed by lowercase email with legislator data.
    """
    result = _load_from_qibrain()
    if result:
        print(f"\u2713 Loaded {len(result)} legislators from QIBrain")
        return result

    print("ERROR: Could not load legislators from QIBrain")
    return {}


def get_legislator_by_email(email: str) -> Optional[Dict[str, Any]]:
    """
    Look up a legislator by email address (case-insensitive).

    Args:
        email: Legislator's email address

    Returns:
        Legislator dictionary if found, None otherwise
    """
    if not email:
        return None
    return LEGISLATORS.get(email.strip().lower())


def get_all_legislators() -> Dict[str, Dict[str, Any]]:
    """
    Get the complete dictionary of all legislators.

    Returns:
        Dictionary of all legislators keyed by email
    """
    return LEGISLATORS


def get_legislator_count() -> int:
    """
    Count how many legislators are loaded.

    Returns:
        Number of legislators
    """
    return len(LEGISLATORS)


def get_district_count() -> int:
    """
    Count how many unique districts exist.

    Returns:
        Number of unique districts
    """
    districts = set()
    for leg in LEGISLATORS.values():
        districts.add(leg.get('district'))
    return len(districts)


def generate_briefer_id(email: str, bill_number: str) -> str:
    """
    Generate a unique briefer ID in format #ID2026-XXXX.

    The ID is deterministic - same email + bill always produces same ID.

    Args:
        email: Legislator email address
        bill_number: Bill number (e.g., "S1212")

    Returns:
        Briefer ID string like "#ID2026-7A4E"
    """
    # Combine email (lowercase) + bill_number (uppercase)
    data = f"{email.lower()}{bill_number.upper()}"

    # Create SHA256 hash
    hash_obj = hashlib.sha256(data.encode())

    # Take first 4 chars of hex, uppercase
    hash_hex = hash_obj.hexdigest()[:4].upper()

    # Format as #[STATE_CODE][SESSION_YEAR]-[4chars]
    return f"#{STATE_CODE}{SESSION_YEAR}-{hash_hex}"


def format_briefer_header(legislator: Dict[str, Any], bill_number: str) -> Dict[str, str]:
    """
    Create all header components for a briefer.

    Args:
        legislator: Legislator dictionary from get_legislator_by_email()
        bill_number: Bill number (e.g., "S1212")

    Returns:
        Dictionary with header components:
        - prepared_for: "Title FirstInitial LastName, LD##"
        - briefer_id: Unique briefer ID
        - generated: Timestamp "MM/DD/YYYY HH:MM(AM/PM)"
        - ld_code: District code
        - state_code: State code
        - session_year: Session year
    """
    # Build "prepared_for" with NO period after initial
    # Format: "Senator B Adams, LD12"
    prepared_for = f"{legislator['display_name']}, {legislator['ld_code']}"

    # Generate briefer ID
    briefer_id = generate_briefer_id(legislator['email'], bill_number)

    # Generate timestamp in 12-hour format
    now = datetime.now()
    generated = now.strftime("%m/%d/%Y %I:%M%p")

    return {
        'prepared_for': prepared_for,
        'briefer_id': briefer_id,
        'generated': generated,
        'ld_code': legislator['ld_code'],
        'state_code': STATE_CODE,
        'session_year': SESSION_YEAR,
    }
LEGISLATORS = load_legislators()
if LEGISLATORS:
    district_count = get_district_count()
    print(f"✓ Loaded {len(LEGISLATORS)} legislators from {district_count} districts")
else:
    print("⚠ No legislators loaded")


# Test code
if __name__ == "__main__":
    print("\n=== Legislators Module Test ===\n")

    print(f"Legislators loaded: {get_legislator_count()}")
    print(f"Districts: {get_district_count()}")
    print()

    # Test lookup
    test_email = "badams@senate.idaho.gov"
    print(f"Looking up: {test_email}")
    leg = get_legislator_by_email(test_email)

    if leg:
        print(f"  Found: {leg['display_name']}")
        print(f"  District: {leg['ld_code']}")
        print(f"  Title: {leg['title']}")
        print()

        # Test header generation
        header = format_briefer_header(leg, "S1212")
        print("Generated header:")
        for key, value in header.items():
            print(f"  {key}: {value}")
    else:
        print(f"  NOT FOUND")
        print("\nAvailable emails (first 5):")
        for email in list(LEGISLATORS.keys())[:5]:
            print(f"  - {email}")
