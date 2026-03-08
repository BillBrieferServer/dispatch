"""
ai_brief.py

Dispatch Leadership Briefer — AI orchestration.

Sections:
  1. Bill Summary (AI)
  2. Sponsor Profile (AI + deterministic data)
  3. Unintended Consequences (module)
  4. Power Flag (module)
  5. Momentum (AI + deterministic data)
  6. Advocacy Positions (deterministic — dispatch.advocacy_positions)
  7. Legislative Activity (deterministic — carried from briefer_format.py)
"""

from __future__ import annotations

import json
import logging
import re
import os
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from typing import Any, Dict, Optional, Tuple

# Import Anthropic client
try:
    from app.services.anthropic_client import generate_briefing as anthropic_generate
    ANTHROPIC_AVAILABLE = True
except ImportError:
    ANTHROPIC_AVAILABLE = False
    anthropic_generate = None

# Full org names for score key display
ORG_FULL_NAMES = {
    "IACI": "Idaho Association of Commerce & Industry",
    "IFF": "Idaho Freedom Foundation",
    "IFBF": "Idaho Farm Bureau Federation",
    "ACLU Idaho": "ACLU of Idaho",
    "CPAC": "CPAC Center for Legislative Accountability",
}

# Import modules
try:
    from app.sections.section_unintended import generate_unintended_consequences
    UNINTENDED_AVAILABLE = True
except ImportError:
    UNINTENDED_AVAILABLE = False
    generate_unintended_consequences = None

try:
    from app.sections.section_powerflag import generate_power_flag
    POWERFLAG_AVAILABLE = True
except ImportError:
    POWERFLAG_AVAILABLE = False
    generate_power_flag = None

# Import AI cache
try:
    from app.ai_cache import (
        get_cached_briefing,
        cache_briefing,
        init_ai_cache_db,
    )
    AI_CACHE_AVAILABLE = True
except ImportError:
    AI_CACHE_AVAILABLE = False
    get_cached_briefing = None
    cache_briefing = None
    init_ai_cache_db = None

logger = logging.getLogger(__name__)

# Configuration
MAX_BILL_TEXT_CHARS = int(os.getenv("MAX_BILL_TEXT_CHARS", "30000"))
AI_PROVIDER = os.getenv("AI_PROVIDER", "anthropic").strip().lower()


def _truncate(text: str, max_chars: int) -> str:
    text = text or ""
    if len(text) <= max_chars:
        return text
    return text[:max_chars] + f"\n\n[Truncated — {len(text)} chars total, showing first {max_chars}]"


# ---------------------------------------------------------------------------
# Deterministic data assembly
# ---------------------------------------------------------------------------

def _parse_sop_contacts(sop_text: str) -> list:
    """Extract legislator names from SOP Contact block."""
    if not sop_text:
        return []
    # Find the Contact: section
    match = re.search(r'Contact:\s*\n(.*?)(?:\nDISCLAIMER|\Z)', sop_text, re.DOTALL)
    if not match:
        return []
    block = match.group(1)
    names = []
    for line in block.split('\n'):
        line = line.strip()
        if not line:
            continue
        # Skip phone numbers, agency names, dates
        if re.match(r'^\(?\d{3}\)?[\s\-]', line):
            continue
        if any(skip in line.lower() for skip in [
            'department', 'division', 'commission', 'bureau', 'office of',
            'association', 'council', 'board of', 'statement of purpose',
            'bill sop', 'idaho state',
        ]):
            continue
        # Strip title prefix
        clean = re.sub(r'^(Representative|Senator)\s+', '', line).strip()
        if clean and len(clean) > 2 and not clean[0].isdigit():
            title = 'Rep.' if line.startswith('Representative') else 'Sen.' if line.startswith('Senator') else ''
            names.append({'title': title, 'full_name': clean, 'raw_line': line})
    return names


def _extract_district_num(district_str: str) -> str:
    """Extract numeric district from formats like 'HD-011B', 'SD-019', '11', etc."""
    if not district_str:
        return ''
    m = re.search(r'[HS]D-?(\d+)', district_str)
    if m:
        return str(int(m.group(1)))
    m = re.search(r'(\d+)', district_str)
    if m:
        return str(int(m.group(1)))
    return district_str


def _build_sponsor_context(bill_id: int) -> str:
    """Assemble sponsor data from QIBrain for AI context."""
    try:
        from app.services.qibrain_data import get_bill_sponsors, get_qibrain_connection
        sponsors = get_bill_sponsors(bill_id)
        if not sponsors:
            return "Committee-sponsored bill \u2014 no named individual sponsor."

        primary = sponsors[0]
        name = primary.get("name", "Unknown")
        first_name = primary.get("first_name", "")
        last_name = primary.get("last_name", "")
        party = primary.get("party", "")
        district = primary.get("district", "")

        is_committee = not first_name and not party

        if not is_committee:
            # Individual sponsor — existing logic
            district_num = _extract_district_num(district)
            lines = [f"Primary sponsor: {name} ({party}), District {district_num or district}"]
            conn = get_qibrain_connection()
            try:
                with conn.cursor() as cur:
                    cur.execute("""
                        SELECT count(*) as cnt FROM idaho.bills b
                        JOIN bill_sponsors bs ON b.bill_id = bs.bill_id
                        WHERE bs.name = %s AND bs.sponsor_order = 1
                        AND b.legiscan_session_id = 2246
                    """, (name,))
                    row = cur.fetchone()
                    bill_count = row["cnt"] if row else 0
                    lines.append(f"Bills sponsored this session: {bill_count}")

                    cur.execute("""
                        SELECT year, score, possible_score, vote_index
                        FROM dispatch.legislator_scores
                        WHERE legislator_id = (
                            SELECT legislator_id FROM idaho.legislators
                            WHERE last_name = %s AND district_id = %s AND is_active = true
                            LIMIT 1
                        )
                        AND org_name = 'IACI'
                        ORDER BY year DESC
                    """, (last_name, district_num))
                    scores = cur.fetchall()
                    if scores:
                        score_lines = []
                        for s in scores:
                            score_lines.append(
                                f"  {s['year']}: {s['score']}/{s['possible_score']} "
                                f"(index: {s['vote_index']})"
                            )
                        lines.append("IACI scores (multi-year):")
                        lines.extend(score_lines)
                    else:
                        lines.append("IACI scores: not available")
            finally:
                conn.close()
            return "\n".join(lines)

        # Committee sponsor — find individual contacts
        lines = [f"Committee: {name}"]
        conn = get_qibrain_connection()
        try:
            with conn.cursor() as cur:
                # Check bill_cosponsors first
                cur.execute("""
                    SELECT bc.legislator_name, l.party, l.district_id
                    FROM idaho.bill_cosponsors bc
                    LEFT JOIN idaho.legislators l
                        ON l.legislator_id = bc.legislator_id
                    WHERE bc.bill_id = %s
                    ORDER BY bc.id
                """, (bill_id,))
                cosponsors = cur.fetchall()

                individuals = []
                if cosponsors:
                    for cs in cosponsors:
                        individuals.append({
                            'name': cs['legislator_name'],
                            'party': cs.get('party', ''),
                            'district': str(cs.get('district_id', '')),
                        })
                else:
                    # Parse SOP text for contacts
                    cur.execute("SELECT sop_text FROM idaho.bills WHERE bill_id = %s", (bill_id,))
                    row = cur.fetchone()
                    sop_text = row['sop_text'] if row and row.get('sop_text') else ''
                    sop_contacts = _parse_sop_contacts(sop_text)
                    for sc in sop_contacts:
                        # Resolve to legislator for party/district
                        full = sc['full_name']
                        parts = full.split()
                        if len(parts) >= 2:
                            search_first = parts[0]
                            # Try progressively longer last names for multi-word surnames
                            matches = []
                            for split_pos in range(len(parts)-1, 0, -1):
                                candidate_last = ' '.join(parts[split_pos:])
                                cur.execute("""
                                    SELECT first_name, last_name, party, district_id
                                    FROM idaho.legislators
                                    WHERE last_name = %s AND is_active = true
                                    ORDER BY district_id
                                """, (candidate_last,))
                                matches = cur.fetchall()
                                if matches:
                                    break
                            matches = matches
                            if len(matches) == 1:
                                m = matches[0]
                                individuals.append({
                                    'name': f"{m['first_name']} {m['last_name']}",
                                    'party': m.get('party', ''),
                                    'district': str(m.get('district_id', '')),
                                })
                            elif len(matches) > 1:
                                # Multiple matches — try first name
                                for m in matches:
                                    if m['first_name'] and m['first_name'].lower().startswith(search_first.lower()):
                                        individuals.append({
                                            'name': f"{m['first_name']} {m['last_name']}",
                                            'party': m.get('party', ''),
                                            'district': str(m.get('district_id', '')),
                                        })
                                        break
                                else:
                                    # Use raw SOP name without party/district
                                    individuals.append({
                                        'name': sc['raw_line'],
                                        'party': '',
                                        'district': '',
                                    })
                            else:
                                # No match found — include raw name
                                individuals.append({
                                    'name': sc['raw_line'],
                                    'party': '',
                                    'district': '',
                                })

                if individuals:
                    contact_strs = []
                    for ind in individuals:
                        s = ind['name']
                        if ind['party']:
                            s += f" ({ind['party']})"
                        if ind['district']:
                            s += f", District {ind['district']}"
                        contact_strs.append(s)
                    lines.append(f"Primary contacts: {', '.join(contact_strs)}")

                    # IACI scores for each individual
                    for ind in individuals:
                        ind_name = ind['name']
                        ind_parts = ind_name.replace('Representative ', '').replace('Senator ', '').split()
                        if not ind_parts:
                            continue
                        ind_last = ind_parts[-1]
                        ind_district = ind.get('district', '')

                        if ind_district:
                            cur.execute("""
                                SELECT year, score, possible_score, vote_index
                                FROM dispatch.legislator_scores
                                WHERE legislator_id = (
                                    SELECT legislator_id FROM idaho.legislators
                                    WHERE last_name = %s AND district_id = %s AND is_active = true
                                    LIMIT 1
                                )
                                AND org_name = 'IACI'
                                ORDER BY year DESC
                            """, (ind_last, ind_district))
                        else:
                            cur.execute("""
                                SELECT year, score, possible_score, vote_index
                                FROM dispatch.legislator_scores ls
                                JOIN idaho.legislators l ON l.legislator_id = ls.legislator_id
                                WHERE l.last_name = %s AND l.is_active = true
                                AND ls.org_name = 'IACI'
                                ORDER BY ls.year DESC
                            """, (ind_last,))
                        scores = cur.fetchall()
                        if scores:
                            lines.append(f"IACI scores for {ind_name}:")
                            for s in scores:
                                lines.append(
                                    f"  {s['year']}: {s['score']}/{s['possible_score']} "
                                    f"(index: {s['vote_index']})"
                                )

                    # Bill count for each individual
                    for ind in individuals:
                        ind_name = ind['name'].replace('Representative ', '').replace('Senator ', '')
                        cur.execute("""
                            SELECT count(*) as cnt FROM idaho.bills b
                            JOIN bill_sponsors bs ON b.bill_id = bs.bill_id
                            WHERE (bs.name = %s OR bs.name LIKE %s)
                            AND bs.sponsor_order = 1
                            AND b.legiscan_session_id = 2246
                        """, (ind_name, f'%{ind_name}%'))
                        row = cur.fetchone()
                        cnt = row['cnt'] if row else 0
                        if cnt > 0:
                            lines.append(f"Bills sponsored by {ind_name} this session: {cnt}")
                else:
                    lines.append("No individual contacts identified.")
        finally:
            conn.close()

        return "\n".join(lines)
    except Exception as e:
        logger.warning(f"Failed to build sponsor context: {e}")
        return "Sponsor data unavailable."



def _build_momentum_context(bill_id: int) -> str:
    """Assemble momentum data from QIBrain for AI context."""
    try:
        from app.services.qibrain_data import get_bill_actions, get_qibrain_connection

        conn = get_qibrain_connection()
        try:
            with conn.cursor() as cur:
                # Get bill_number for this bill_id
                cur.execute("SELECT bill_number FROM idaho.bills WHERE bill_id = %s", (bill_id,))
                row = cur.fetchone()
                bill_number = row["bill_number"] if row else None

                # Full event timeline from bill_events (richer than bill_actions)
                cur.execute("""
                    SELECT event_date, event_text
                    FROM idaho.bill_events
                    WHERE bill_id = %s
                    ORDER BY event_date ASC, sequence_order ASC
                """, (bill_id,))
                events = cur.fetchall()

                # Hearing status from committee_agendas
                hearing = None
                if bill_number:
                    cur.execute("""
                        SELECT hearing_date, committee_name
                        FROM idaho.committee_agendas
                        WHERE bill_number = %s
                        ORDER BY hearing_date DESC LIMIT 1
                    """, (bill_number,))
                    hearing = cur.fetchone()

                # Reading calendar status
                reading_cal = None
                if bill_number:
                    cur.execute("""
                        SELECT reading_type, chamber, calendar_date
                        FROM idaho.reading_calendars
                        WHERE bill_number = %s
                        ORDER BY calendar_date DESC LIMIT 1
                    """, (bill_number,))
                    reading_cal = cur.fetchone()

                # Co-sponsor count
                cur.execute("""
                    SELECT COUNT(*) as cnt
                    FROM idaho.bill_cosponsors
                    WHERE bill_id = %s
                """, (bill_id,))
                cosponsor_row = cur.fetchone()
                cosponsor_count = cosponsor_row["cnt"] if cosponsor_row else 0
        finally:
            conn.close()

        if not events:
            # Fall back to get_bill_actions if no bill_events
            actions = get_bill_actions(bill_id)
            if not actions:
                return "No bill actions found."
            events = [{"event_date": a.get("action_date", ""), "event_text": a.get("action", "")} for a in reversed(actions)]

        # Days since introduction
        days_since = "unknown"
        for e in events:
            if "introduced" in (e.get("event_text", "").lower()):
                try:
                    intro_date = datetime.strptime(e["event_date"], "%Y-%m-%d")
                    days_since = (datetime.now() - intro_date).days
                except (ValueError, TypeError):
                    pass
                break
        if days_since == "unknown" and events:
            try:
                intro_date = datetime.strptime(events[0]["event_date"], "%Y-%m-%d")
                days_since = (datetime.now() - intro_date).days
            except (ValueError, TypeError):
                pass

        # Detect chamber crossover
        crossed_chamber = False
        for e in events:
            text_lower = (e.get("event_text") or "").lower()
            if "received from the house" in text_lower or "received from the senate" in text_lower:
                crossed_chamber = True
                break

        # Detect if bill has passed a chamber
        passed_chamber = None
        for e in events:
            text_lower = (e.get("event_text") or "").lower()
            if "passed" in text_lower and "read third time" in text_lower:
                if bill_number and bill_number.startswith("H"):
                    passed_chamber = "House"
                elif bill_number and bill_number.startswith("S"):
                    passed_chamber = "Senate"
                break

        # Build context lines
        lines = [
            f"Days since introduction: {days_since}",
            f"Total events: {len(events)}",
        ]

        if cosponsor_count > 0:
            lines.append(f"Co-sponsors: {cosponsor_count}")

        if crossed_chamber:
            lines.append(f"Chamber crossover: YES (originated in {'House' if bill_number and bill_number.startswith('H') else 'Senate'})")
        elif passed_chamber:
            lines.append(f"Passed {passed_chamber}, awaiting crossover")

        # Full event timeline (truncated event_text to keep context manageable)
        lines.append("")
        lines.append("EVENT TIMELINE (oldest to newest):")
        for e in events:
            date = e.get("event_date", "")
            text = (e.get("event_text") or "")[:200]  # Truncate long vote lists
            lines.append(f"  {date}: {text}")

        # Hearing and reading calendar
        lines.append("")
        if hearing:
            lines.append(f"Hearing scheduled: {hearing['hearing_date']} in {hearing['committee_name']}")
        else:
            lines.append("Hearing status: No hearing scheduled")

        if reading_cal:
            lines.append(f"Reading calendar: {reading_cal['reading_type']} in {reading_cal['chamber']} on {reading_cal['calendar_date']}")

        return "\n".join(lines)
    except Exception as e:
        logger.warning(f"Failed to build momentum context: {e}")
        return "Momentum data unavailable."


def _get_advocacy_positions(bill_id: int) -> dict:
    """Pull advocacy positions from dispatch.advocacy_positions. Never cached."""
    try:
        from app.services.qibrain_data import get_qibrain_connection
        conn = get_qibrain_connection()
        try:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT org_name, position, position_detail, source_url
                    FROM dispatch.advocacy_positions
                    WHERE bill_id = %s
                    ORDER BY org_name
                """, (bill_id,))
                positions = [dict(r) for r in cur.fetchall()]
        finally:
            conn.close()

        # Check for IFF vs IACI divergence
        org_positions = {p["org_name"]: p["position"] for p in positions}
        coalition_alert = None
        iff_pos = org_positions.get("IFF")
        iaci_pos = org_positions.get("IACI")
        if iff_pos and iaci_pos:
            if (iff_pos == "support" and iaci_pos == "oppose") or \
               (iff_pos == "oppose" and iaci_pos == "support"):
                coalition_alert = (
                    "IFF and IACI diverge \u2014 "
                    "Freedom Caucus vs. business community split."
                )

        return {
            "positions": positions,
            "coalition_alert": coalition_alert,
            "count": len(positions),
        }
    except Exception as e:
        logger.warning(f"Failed to get advocacy positions: {e}")
        return {"positions": [], "coalition_alert": None, "count": 0}


# ---------------------------------------------------------------------------
# Main orchestrator
# ---------------------------------------------------------------------------



def _build_sponsor_display(bill_id: int) -> dict:
    """
    Build deterministic sponsor display data for Section 2 rendering.

    Returns: {
        "contacts": [
            {"name": "Jordan Redman", "title": "Rep.", "ld": "LD3",
             "bills_this_session": 4,
             "scores": [{"org": "IACI", "pct": 38.2, "year": 2026}, ...]},
            ...
        ],
        "committee": "Business Committee" or None,
        "chamber": "House" or "Senate",
        "cosponsors": [{"name": "...", "title": "Rep.", "ld": "LD4"}, ...],
    }
    """
    if not bill_id:
        return {"contacts": [], "committee": None, "chamber": "", "cosponsors": []}

    try:
        from app.services.qibrain_data import get_bill_sponsors, get_qibrain_connection
        sponsors = get_bill_sponsors(bill_id)

        primary = sponsors[0] if sponsors else {}
        name = primary.get("name", "Unknown")
        first_name = primary.get("first_name", "")
        party = primary.get("party", "")
        district = primary.get("district", "")
        role = primary.get("role", "")

        # Determine chamber from role field
        chamber = ""
        if role:
            role_lower = role.lower()
            if "rep" in role_lower:
                chamber = "House"
            elif "sen" in role_lower:
                chamber = "Senate"

        is_committee = not first_name and not party

        conn = get_qibrain_connection()
        try:
            with conn.cursor() as cur:
                individuals = []

                if sponsors and not is_committee:
                    # Individual sponsor from bill_sponsors
                    district_num = _extract_district_num(district)
                    individuals.append({
                        "raw_name": name,
                        "last_name": primary.get("last_name", ""),
                        "district_num": district_num,
                        "chamber": chamber,
                    })
                else:
                    # Committee or no bill_sponsors — resolve from SOP text
                    cur.execute("SELECT sop_text FROM idaho.bills WHERE bill_id = %s", (bill_id,))
                    row = cur.fetchone()
                    sop_text = row['sop_text'] if row and row.get('sop_text') else ''
                    sop_contacts = _parse_sop_contacts(sop_text)
                    for sc in sop_contacts:
                        full = sc['full_name']
                        parts = full.split()
                        search_first = parts[0] if parts else ''
                        # Try progressively longer last names for multi-word surnames
                        # e.g., "Lori Den Hartog" -> try "Hartog", then "Den Hartog"
                        matches = []
                        search_last = parts[-1] if parts else ''
                        for split_pos in range(len(parts)-1, 0, -1):
                            candidate_last = ' '.join(parts[split_pos:])
                            cur.execute("""
                                SELECT first_name, last_name, district_id,
                                       CASE WHEN chamber = 'House' THEN 'House' ELSE 'Senate' END as chamber
                                FROM idaho.legislators
                                WHERE last_name = %s AND is_active = true
                                ORDER BY district_id
                            """, (candidate_last,))
                            matches = cur.fetchall()
                            if matches:
                                search_last = candidate_last
                                break
                        resolved = None
                        if len(matches) == 1:
                            resolved = matches[0]
                        elif len(matches) > 1:
                            for m in matches:
                                if m['first_name'] and m['first_name'].lower().startswith(search_first.lower()):
                                    resolved = m
                                    break
                        if resolved:
                            individuals.append({
                                "raw_name": f"{resolved['first_name']} {resolved['last_name']}",
                                "last_name": resolved['last_name'],
                                "district_num": str(resolved.get('district_id', '')),
                                "chamber": resolved.get('chamber', '') or sc.get('title', '').replace('.', ''),
                            })
                        else:
                            title = sc.get('title', '')
                            ch = 'House' if 'Rep' in title else 'Senate' if 'Sen' in title else ''
                            individuals.append({
                                "raw_name": sc['raw_line'],
                                "last_name": search_last,
                                "district_num": '',
                                "chamber": ch,
                            })

                # Build contacts with scores
                contacts = []
                for ind in individuals:
                    title = "Rep." if ind.get("chamber") == "House" else "Sen." if ind.get("chamber") == "Senate" else ""
                    ld = f"LD{ind['district_num']}" if ind.get('district_num') else ""

                    # Count bills this session
                    bills_count = 0
                    cur.execute("""
                        SELECT count(*) as cnt FROM idaho.bills b
                        JOIN bill_sponsors bs ON b.bill_id = bs.bill_id
                        WHERE (bs.name = %s OR bs.name LIKE %s)
                        AND bs.sponsor_order = 1
                        AND b.legiscan_session_id = 2246
                    """, (ind['raw_name'], f"%{ind['raw_name']}%"))
                    row = cur.fetchone()
                    if row:
                        bills_count = row['cnt']

                    # Get ALL org scores for this legislator (most recent year only)
                    scores = []
                    if ind.get('district_num'):
                        cur.execute("""
                            SELECT ls.org_name, ls.vote_index, ls.year
                            FROM dispatch.legislator_scores ls
                            JOIN idaho.legislators l ON l.legislator_id = ls.legislator_id
                            WHERE l.last_name = %s AND l.district_id = %s AND l.is_active = true
                            ORDER BY ls.org_name, ls.year DESC
                        """, (ind['last_name'], ind['district_num']))
                    else:
                        # No district — query by name only
                        cur.execute("""
                            SELECT ls.org_name, ls.vote_index, ls.year
                            FROM dispatch.legislator_scores ls
                            JOIN idaho.legislators l ON l.legislator_id = ls.legislator_id
                            WHERE l.last_name = %s AND l.is_active = true
                            ORDER BY ls.org_name, ls.year DESC
                        """, (ind['last_name'],))
                    score_rows = cur.fetchall()
                    if not score_rows:
                        # Fallback: district mismatch (seat vs legislative district)
                        cur.execute("""
                            SELECT ls.org_name, ls.vote_index, ls.year
                            FROM dispatch.legislator_scores ls
                            JOIN idaho.legislators l ON l.legislator_id = ls.legislator_id
                            WHERE l.last_name = %s AND l.is_active = true
                            ORDER BY ls.org_name, ls.year DESC
                        """, (ind['last_name'],))
                        score_rows = cur.fetchall()

                    # Take most recent year per org
                    seen_orgs = set()
                    for sr in score_rows:
                        org = sr['org_name']
                        if org not in seen_orgs:
                            seen_orgs.add(org)
                            vi = sr.get('vote_index')
                            if vi is not None:
                                scores.append({
                                    "org": org,
                                    "pct": round(float(vi), 1),
                                    "year": sr.get('year'),
                                })

                    # Sort: IACI first, then alphabetically
                    scores.sort(key=lambda s: (0 if s['org'] == 'IACI' else 1, s['org']))

                    contacts.append({
                        "name": ind['raw_name'],
                        "title": title,
                        "ld": ld,
                        "bills_this_session": bills_count,
                        "scores": scores,
                    })

                # Get co-sponsors (from LegCo PDF, already in bill_cosponsors table)
                cosponsors_list = []
                cur.execute("""
                    SELECT bc.legislator_name, l.district_id, l.chamber
                    FROM idaho.bill_cosponsors bc
                    JOIN idaho.legislators l ON l.legislator_id = bc.legislator_id
                    WHERE bc.bill_id = %s
                    ORDER BY l.chamber, l.last_name
                """, (bill_id,))
                for cs in cur.fetchall():
                    cs_title = "Rep." if cs['chamber'] == 'House' else "Sen."
                    cosponsors_list.append({
                        "name": cs['legislator_name'],
                        "title": cs_title,
                        "ld": f"LD{cs['district_id']}" if cs.get('district_id') else "",
                    })

                committee_name = name if (sponsors and is_committee) else None

                return {
                    "contacts": contacts,
                    "committee": committee_name,
                    "chamber": chamber,
                    "cosponsors": cosponsors_list,
                }
        finally:
            conn.close()
    except Exception as e:
        logger.warning(f"Failed to build sponsor display: {e}")
        return {"contacts": [], "committee": None, "chamber": "", "cosponsors": []}

def build_ai_brief(
    *,
    bill_number: str,
    legiscan_bill: Dict[str, Any],
    bill_text: str,
    fiscal_note_text: str = "",
    bill_id: int = None,
    session_id: int = None,
    bill_change_hash: str = None,
    use_cache: bool = True,
    census_context: Optional[Dict[str, Any]] = None,
) -> Tuple[Optional[Dict[str, Any]], Optional[str], str, bool, Dict]:
    """
    Generate Dispatch briefing for a bill.

    Returns:
        (parsed_json, error_message, model_used, was_invalidated, token_usage)
    """
    bill_text = _truncate(bill_text or "", MAX_BILL_TEXT_CHARS)
    was_invalidated = False

    # --- Check cache (S1, S2, S5 + S3, S4) ---
    if use_cache and AI_CACHE_AVAILABLE and bill_id:
        cached = get_cached_briefing(bill_id, bill_change_hash)
        if cached:
            logger.info(f"[{bill_number}] CACHE HIT")
            result = cached["ai_json"]
            # Always pull fresh advocacy positions (S6)
            result["advocacy_positions"] = _get_advocacy_positions(bill_id)
            result["sponsor_display"] = _build_sponsor_display(bill_id)
            return result, None, "cached", False, {}
        else:
            stale = get_cached_briefing(bill_id, None)
            if stale:
                was_invalidated = True
                logger.info(f"[{bill_number}] Cache INVALIDATED — bill changed")

    if not ANTHROPIC_AVAILABLE:
        return None, "Anthropic not available", "none", False, {}

    # --- Assemble deterministic context ---
    sponsor_context = _build_sponsor_context(bill_id) if bill_id else ""
    momentum_context = _build_momentum_context(bill_id) if bill_id else ""

    # --- Generate AI sections (S1, S2, S5) ---
    logger.info(f"[{bill_number}] Generating Dispatch briefing")
    result, error, token_usage = anthropic_generate(
        bill_number=bill_number,
        bill_text=bill_text,
        legiscan_bill=legiscan_bill,
        fiscal_note_text=fiscal_note_text,
        sponsor_context=sponsor_context,
        momentum_context=momentum_context,
    )

    if result is None:
        return None, error, "none", False, {}

    # --- Generate S3 + S4 modules in parallel ---
    bill_title = legiscan_bill.get("title", "") or legiscan_bill.get("description", "")
    s3_result, s3_error, s3_tokens = None, None, {}
    s4_result, s4_error, s4_tokens = None, None, {}

    if UNINTENDED_AVAILABLE and POWERFLAG_AVAILABLE:
        logger.info(f"[{bill_number}] Generating S3 + S4 modules in parallel")
        with ThreadPoolExecutor(max_workers=2) as executor:
            future_s3 = executor.submit(
                generate_unintended_consequences,
                bill_number=bill_number,
                bill_title=bill_title,
                bill_text=bill_text,
                bill_summary=result.get("bill_summary", ""),
                fiscal_note_text=fiscal_note_text,
            )
            future_s4 = executor.submit(
                generate_power_flag,
                bill_number=bill_number,
                bill_title=bill_title,
                bill_text=bill_text,
                bill_summary=result.get("bill_summary", ""),
            )
            s3_result, s3_error, s3_tokens = future_s3.result()
            s4_result, s4_error, s4_tokens = future_s4.result()
    else:
        if UNINTENDED_AVAILABLE:
            s3_result, s3_error, s3_tokens = generate_unintended_consequences(
                bill_number=bill_number, bill_title=bill_title,
                bill_text=bill_text, bill_summary=result.get("bill_summary", ""),
                fiscal_note_text=fiscal_note_text,
            )
        if POWERFLAG_AVAILABLE:
            s4_result, s4_error, s4_tokens = generate_power_flag(
                bill_number=bill_number, bill_title=bill_title,
                bill_text=bill_text, bill_summary=result.get("bill_summary", ""),
            )

    # Merge S3
    if s3_result:
        result["unintended_consequences"] = s3_result["unintended_consequences"]
        logger.info(f"[{bill_number}] S3 (Unintended Consequences) merged")
        for k, v in s3_tokens.items():
            token_usage[f"section_3_{k}"] = v
    else:
        logger.warning(f"[{bill_number}] S3 failed: {s3_error}")
        result["unintended_consequences"] = ["Module generation failed — review manually."]

    # Merge S4
    if s4_result:
        result["power_flag"] = s4_result["power_flag"]
        logger.info(f"[{bill_number}] S4 (Power Flag) merged")
        for k, v in s4_tokens.items():
            token_usage[f"section_4_{k}"] = v
    else:
        logger.warning(f"[{bill_number}] S4 failed: {s4_error}")
        result["power_flag"] = {
            "flag_level": "none",
            "direction": "none",
            "explanation": "Module generation failed — review manually.",
        }

    # --- Sponsor Display (deterministic, always fresh) ---
    result["sponsor_display"] = _build_sponsor_display(bill_id) if bill_id else {
        "contacts": [], "committee": None, "chamber": "",
    }

    # --- S6: Advocacy Positions (always fresh) ---
    result["advocacy_positions"] = _get_advocacy_positions(bill_id) if bill_id else {
        "positions": [], "coalition_alert": None, "count": 0,
    }

    # --- Add disclaimer ---
    result["disclaimer"] = {
        "title": "ABOUT THIS BRIEFER",
        "content": (
            "This analysis was generated by Dispatch using Claude AI (Anthropic) "
            "from official Idaho legislative documents. Advocacy positions are pulled "
            "from public organizational trackers. All factual claims should be verified "
            "against primary sources before citing publicly."
        ),
    }

    # --- Cache result (S1, S2, S3, S4, S5 — NOT S6, S7) ---
    if AI_CACHE_AVAILABLE and bill_id and session_id:
        try:
            # Cache a copy without S6 (advocacy) since that's always fresh
            cache_copy = {k: v for k, v in result.items() if k != "advocacy_positions"}
            cache_briefing(
                bill_id=bill_id,
                session_id=session_id,
                bill_number=bill_number,
                ai_json=cache_copy,
                model_used="anthropic",
                bill_change_hash=bill_change_hash,
            )
            logger.info(f"[{bill_number}] Cached Dispatch briefing")
        except Exception as e:
            logger.warning(f"[{bill_number}] Cache write failed: {e}")

    return result, None, "anthropic", was_invalidated, token_usage
