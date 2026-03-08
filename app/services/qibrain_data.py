"""
QIBrain Data Access Module

Single module for all QIBrain PostgreSQL reads used by the Bill Briefer.
Every function returns the same data structure the current code produces,
so callers (section generators, formatters, etc.) don't need to change.

Replaces: legiscan_sync.sqlite, LegiScan API calls, Census JSON files,
          IdahoLegislature.xlsx
"""

import os
import json
import logging
import psycopg2
from psycopg2.extras import RealDictCursor

logger = logging.getLogger(__name__)


def get_qibrain_connection():
    """Get connection to QIBrain PostgreSQL."""
    return psycopg2.connect(
        os.environ["QIBRAIN_DATABASE_URL"],
        cursor_factory=RealDictCursor
    )


# ---------------------------------------------------------------------------
# Bill Data (replaces LegiScan API + legiscan_sync.sqlite)
# ---------------------------------------------------------------------------

def get_bill(bill_number, session_year=2026):
    """
    Get bill metadata by bill number and session year.
    Replaces: LegiScan API getBill / legiscan_sync.sqlite lookup.
    Returns dict matching the structure the briefer expects, or None.
    """
    conn = get_qibrain_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT b.bill_id, b.legiscan_bill_id, b.bill_number,
                       b.title, b.description, b.status, b.source_url AS url,
                       b.last_action, b.last_action_date::text AS last_action_date,
                       b.subjects, b.committee, b.bill_type, b.change_hash,
                       b.legiscan_session_id
                FROM bills b
                WHERE b.bill_number = %s
                  AND b.legiscan_session_id IN (
                      SELECT legiscan_session_id FROM sessions WHERE year = %s
                  )
                ORDER BY b.bill_id DESC
                LIMIT 1
            """, (bill_number.upper(), session_year))
            row = cur.fetchone()
            if row is None:
                return None
            result = dict(row)
            # subjects is stored as text — parse if JSON, else split on semicolons
            if result.get("subjects"):
                subj = result["subjects"]
                if isinstance(subj, str):
                    try:
                        result["subjects"] = json.loads(subj)
                    except (json.JSONDecodeError, TypeError):
                        result["subjects"] = [s.strip() for s in subj.split(";") if s.strip()]
            else:
                result["subjects"] = []
            return result
    finally:
        conn.close()


def get_bill_text(bill_id):
    """
    Get full bill text.
    Replaces: LegiScan getBillText -> base64 PDF decode -> text extraction.
    Returns str or None if not yet synced.
    """
    conn = get_qibrain_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT full_text FROM bills WHERE bill_id = %s", (bill_id,))
            row = cur.fetchone()
            if row and row["full_text"]:
                return row["full_text"]
            return None
    finally:
        conn.close()


def get_bill_sponsors(bill_id):
    """
    Get sponsors for a bill.
    Replaces: legiscan_sync.sqlite sponsors lookup.
    Returns list of dicts.
    """
    conn = get_qibrain_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT name, first_name, last_name, party, district,
                       sponsor_type, sponsor_order, role
                FROM bill_sponsors
                WHERE bill_id = %s
                ORDER BY sponsor_order
            """, (bill_id,))
            return [dict(r) for r in cur.fetchall()]
    finally:
        conn.close()


def get_bill_actions(bill_id):
    """
    Get actions/history for a bill, most recent first.
    Tries bill_actions (LegiScan-sourced) first, falls back to bill_events
    (scraped from legislature.idaho.gov) when bill_actions is empty or has
    fewer entries.
    Returns list of dicts with keys: action_date, action, chamber, importance.
    """
    conn = get_qibrain_connection()
    try:
        with conn.cursor() as cur:
            # Primary source: bill_actions (LegiScan)
            cur.execute("""
                SELECT action_date::text AS action_date, action, chamber, importance
                FROM bill_actions
                WHERE bill_id = %s
                ORDER BY action_date DESC, sequence DESC
            """, (bill_id,))
            actions = [dict(r) for r in cur.fetchall()]

            # Fallback: bill_events (legislature.idaho.gov scraper)
            # Use bill_events if it has more data than bill_actions
            cur.execute("""
                SELECT event_date::text AS action_date, event_text AS action
                FROM idaho.bill_events
                WHERE bill_id = %s
                ORDER BY event_date DESC, sequence_order DESC
            """, (bill_id,))
            events = cur.fetchall()

            if events and len(events) > len(actions):
                # bill_events has richer data — use it instead
                cur.execute("SELECT bill_number FROM idaho.bills WHERE bill_id = %s", (bill_id,))
                bill_row = cur.fetchone()
                bn = bill_row["bill_number"] if bill_row else ""
                chamber = "H" if bn.startswith("H") else "S" if bn.startswith("S") else ""
                result = []
                for e in events:
                    result.append({
                        "action_date": e["action_date"],
                        "action": e["action"],
                        "chamber": chamber,
                        "importance": 1,
                    })
                return result

            return actions
    finally:
        conn.close()


def get_bill_votes(bill_id):
    """
    Get roll call votes for a bill with individual legislator votes.
    Replaces: LegiScan getRollCall calls.
    Returns list of roll call dicts with nested individual_votes.
    """
    conn = get_qibrain_connection()
    try:
        with conn.cursor() as cur:
            # Get roll calls
            cur.execute("""
                SELECT vote_id, chamber, vote_date::text AS vote_date,
                       yeas, nays, absent, result
                FROM bill_votes
                WHERE bill_id = %s
                ORDER BY vote_date DESC
            """, (bill_id,))
            votes = [dict(r) for r in cur.fetchall()]

            # Get individual votes for each roll call
            for vote in votes:
                cur.execute("""
                    SELECT l.first_name || ' ' || l.last_name AS legislator_name,
                           l.party, lv.vote_cast
                    FROM legislator_votes lv
                    JOIN legislators l ON l.legislator_id = lv.legislator_id
                    WHERE lv.vote_id = %s
                    ORDER BY l.last_name, l.first_name
                """, (vote["vote_id"],))
                vote["individual_votes"] = [dict(r) for r in cur.fetchall()]

            return votes
    finally:
        conn.close()


def get_bill_fiscal_note(bill_id):
    """
    Get fiscal note text for a bill.
    Replaces: LegiScan getSupplement -> PDF decode -> text extraction.
    Returns str or None if not yet available.
    """
    conn = get_qibrain_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT fiscal_note_text FROM bills WHERE bill_id = %s", (bill_id,))
            row = cur.fetchone()
            if row and row["fiscal_note_text"]:
                return row["fiscal_note_text"]
            return None
    finally:
        conn.close()


def get_bill_sop(bill_id):
    """
    Get Statement of Purpose text for a bill.
    Replaces: LegiScan getSupplement -> PDF decode -> text extraction.
    Returns str or None if not yet available.
    """
    conn = get_qibrain_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT sop_text FROM bills WHERE bill_id = %s", (bill_id,))
            row = cur.fetchone()
            if row and row["sop_text"]:
                return row["sop_text"]
            return None
    finally:
        conn.close()



def _get_session_info(legiscan_session_id):
    """
    Look up session year and name from QIBrain sessions table.
    Falls back to deriving year from bill action dates if session not found.
    Returns dict with session_id, session_name, year_start, year_end.
    """
    if not legiscan_session_id:
        return None
    conn = get_qibrain_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT year, session_name FROM sessions WHERE legiscan_session_id = %s",
                (legiscan_session_id,)
            )
            row = cur.fetchone()
            if row:
                return {
                    "session_id": legiscan_session_id,
                    "session_name": row["session_name"],
                    "year_start": row["year"],
                    "year_end": row["year"],
                }
            # Fallback: derive year from earliest bill action in this session
            cur.execute(
                """SELECT EXTRACT(YEAR FROM MIN(last_action_date))::int AS yr
                   FROM bills WHERE legiscan_session_id = %s
                   AND last_action_date IS NOT NULL""",
                (legiscan_session_id,)
            )
            row = cur.fetchone()
            if row and row["yr"]:
                yr = row["yr"]
                return {
                    "session_id": legiscan_session_id,
                    "session_name": f"{yr} Regular Session",
                    "year_start": yr,
                    "year_end": yr,
                }
    finally:
        conn.close()
    return None


# ---------------------------------------------------------------------------
# LegiScan-format adapter (bridges QIBrain data to existing code expectations)
# ---------------------------------------------------------------------------

def get_bill_as_legiscan_format(bill_number, session_year=2026):
    """
    Assemble a complete bill dict in LegiScan API format.

    This is the key adapter function. The rest of the codebase (ai_brief.py,
    anthropic_client.py, briefer_format.py) expects data in LegiScan API format.
    This function queries QIBrain and assembles the same structure.

    Returns tuple: (bill_payload, bill_obj, qibrain_bill_id)
        - bill_payload: dict mimicking legiscan_call("getBill") response
          (passed to build_ai_brief as legiscan_bill)
        - bill_obj: the inner bill dict (bill_payload["bill"])
          (used in main.py for metadata access)
        - qibrain_bill_id: the QIBrain bills.bill_id (for cache/store operations)
        Returns (None, None, None) if bill not found.
    """
    bill = get_bill(bill_number, session_year)
    if not bill:
        return None, None, None

    qibrain_bill_id = bill["bill_id"]
    legiscan_bill_id = bill["legiscan_bill_id"]

    # Look up actual session info from QIBrain (not hardcoded)
    session_info = _get_session_info(bill.get("legiscan_session_id"))
    if not session_info:
        # Ultimate fallback: use the requested session_year
        session_info = {
            "session_id": bill.get("legiscan_session_id", 0),
            "session_name": f"{session_year} Regular Session",
            "year_start": session_year,
            "year_end": session_year,
        }

    # Get related data
    sponsors = get_bill_sponsors(qibrain_bill_id)
    actions = get_bill_actions(qibrain_bill_id)
    votes = get_bill_votes(qibrain_bill_id)
    bill_text = get_bill_text(qibrain_bill_id)

    # Format sponsors to match LegiScan structure
    formatted_sponsors = []
    for s in sponsors:
        formatted_sponsors.append({
            "people_id": s.get("legiscan_people_id", 0),
            "name": s.get("name", ""),
            "first_name": s.get("first_name", ""),
            "last_name": s.get("last_name", ""),
            "party": s.get("party", ""),
            "party_id": {"R": 2, "D": 1, "I": 3, "L": 4}.get(s.get("party", ""), 0),
            "district": s.get("district", ""),
            "role": s.get("role", ""),
            "role_id": 1 if s.get("sponsor_type", 0) == 0 else 2,
            "sponsor_type_id": s.get("sponsor_type", 0),
            "sponsor_order": s.get("sponsor_order", 0),
        })

    # Format history to match LegiScan structure
    formatted_history = []
    for a in actions:
        formatted_history.append({
            "date": a.get("action_date", ""),
            "action_date": a.get("action_date", ""),
            "action": a.get("action", ""),
            "chamber": a.get("chamber", ""),
            "chamber_id": 1 if a.get("chamber") == "H" else 2,
            "importance": a.get("importance", 0),
        })

    # Format votes to match LegiScan structure
    formatted_votes = []
    for v in votes:
        passed = 1 if v.get("result", "").lower() in ("passed", "pass") else 0
        formatted_votes.append({
            "roll_call_id": v.get("vote_id", 0),
            "date": v.get("vote_date", ""),
            "description": "",
            "yea": v.get("yeas", 0),
            "nay": v.get("nays", 0),
            "nv": v.get("absent", 0),
            "absent": v.get("absent", 0),
            "chamber": v.get("chamber", ""),
            "chamber_id": 1 if v.get("chamber") == "H" else 2,
            "passed": passed,
        })

    # Format texts entry (if bill text exists)
    formatted_texts = []
    if bill_text:
        formatted_texts.append({
            "doc_id": 0,
            "date": "",
            "type": "Introduced",
            "type_id": 1,
            "mime": "text/plain",
            "mime_id": 1,
            "text_size": len(bill_text),
        })

    # Format subjects
    subjects = bill.get("subjects", [])
    formatted_subjects = []
    for subj in subjects:
        if isinstance(subj, dict):
            formatted_subjects.append(subj)
        elif isinstance(subj, str):
            formatted_subjects.append({"subject_name": subj})

    # Build the inner bill object (matches LegiScan getBill response structure)
    bill_obj = {
        "bill_id": legiscan_bill_id or qibrain_bill_id,
        "session_id": bill.get("legiscan_session_id", 0),
        "session": session_info,
        "bill_number": bill.get("bill_number", ""),
        "bill_type": bill.get("bill_type", "B"),
        "bill_type_id": 1,
        "title": bill.get("title", ""),
        "description": bill.get("description", ""),
        "status": bill.get("status", 0),
        "status_desc": "",
        "status_date": bill.get("last_action_date", ""),
        "state": "ID",
        "state_id": 13,
        "state_link": bill.get("url", ""),
        "url": bill.get("url", ""),
        "change_hash": bill.get("change_hash", ""),
        "committee": {"name": bill.get("committee", "")} if bill.get("committee") else {},
        "last_action": bill.get("last_action", ""),
        "last_action_date": bill.get("last_action_date", ""),
        "sponsors": formatted_sponsors,
        "history": formatted_history,
        "votes": formatted_votes,
        "texts": formatted_texts,
        "supplements": [],  # SOPs/fiscal notes handled separately via lazy-load
        "subjects": formatted_subjects,
    }

    # bill_payload wraps like LegiScan API response — but also includes
    # top-level fields for code that accesses legiscan_bill.get("title") directly
    bill_payload = {
        "status": "OK",
        "bill": bill_obj,
        # Top-level fields for anthropic_client.py which does legiscan_bill.get("title")
        "title": bill_obj["title"],
        "description": bill_obj["description"],
        "state": "ID",
        "session": bill_obj["session"],
        "sponsors": bill_obj["sponsors"],
        "subjects": bill_obj["subjects"],
        "history": bill_obj["history"],
        "status_val": bill_obj["status"],
        "last_action": bill_obj["last_action"],
        "last_action_date": bill_obj["last_action_date"],
    }

    return bill_payload, bill_obj, qibrain_bill_id


def find_bill_id_qibrain(bill_number, session_year=2026):
    """
    Resolve a bill number to its QIBrain bill_id.
    Replaces: get_masterlist_raw() + find_bill_id() flow.
    Returns (qibrain_bill_id, legiscan_bill_id) or (None, None).
    """
    bill = get_bill(bill_number, session_year)
    if bill:
        return bill["bill_id"], bill["legiscan_bill_id"]
    return None, None


# ---------------------------------------------------------------------------
# Write-back functions for lazy-load pattern (SOP/fiscal note)
# ---------------------------------------------------------------------------

def store_fiscal_note(bill_id, text):
    """Store extracted fiscal note text in QIBrain for future requests."""
    conn = get_qibrain_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE bills SET fiscal_note_text = %s, updated_at = NOW() WHERE bill_id = %s",
                (text, bill_id)
            )
        conn.commit()
        logger.info(f"Stored fiscal note for bill_id={bill_id}")
    finally:
        conn.close()


def store_sop(bill_id, text):
    """Store extracted SOP text in QIBrain for future requests."""
    conn = get_qibrain_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE bills SET sop_text = %s, updated_at = NOW() WHERE bill_id = %s",
                (text, bill_id)
            )
        conn.commit()
        logger.info(f"Stored SOP for bill_id={bill_id}")
    finally:
        conn.close()


def store_bill_text(bill_id, text):
    """Store extracted bill text in QIBrain for future requests."""
    conn = get_qibrain_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE bills SET full_text = %s, updated_at = NOW() WHERE bill_id = %s",
                (text, bill_id)
            )
        conn.commit()
        logger.info(f"Stored bill text for bill_id={bill_id} ({len(text)} chars)")
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Census / Demographics (replaces JSON file loads + Census API)
# ---------------------------------------------------------------------------

def get_district_demographics(district_number):
    """
    Get full demographics for a legislative district.
    Replaces: JSON file load from census_ld_demographics_2023.json.
    Returns dict with 155+ fields matching the structure district_analysis.py expects,
    or None if not found.

    district_number: int or str, e.g. 27 or "27"
    """
    # QIBrain stores geography_id as zero-padded 2-digit string
    geo_id = str(int(district_number)).zfill(2)

    conn = get_qibrain_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT detail_data
                FROM census_districts
                WHERE geography_type = 'sld_upper'
                  AND geography_id = %s
            """, (geo_id,))
            row = cur.fetchone()
            if row and row["detail_data"]:
                data = row["detail_data"]
                # detail_data is JSONB — psycopg2 auto-deserializes to dict
                if isinstance(data, str):
                    return json.loads(data)
                return data
            return None
    finally:
        conn.close()


def get_state_demographics():
    """
    Get statewide demographics for Idaho.
    Replaces: JSON file load from census_idaho_state_2023.json.
    Returns dict matching the statewide totals structure, or None.
    """
    conn = get_qibrain_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT detail_data
                FROM census_districts
                WHERE geography_type = 'state'
                  AND geography_id = '16'
            """, ())
            row = cur.fetchone()
            if row and row["detail_data"]:
                data = row["detail_data"]
                if isinstance(data, str):
                    return json.loads(data)
                return data
            return None
    finally:
        conn.close()




def get_district_bls_data(district_number):
    """
    Get BLS economic data relevant to a legislative district.
    Returns flat dict keyed by bls_ prefixed field names, or empty dict.

    Data sources:
    - LAUS: district-level unemployment (sld_upper) + state comparison
    - QCEW: state-level industry employment and wages by sector
    - CPI: state-level cost of living changes (West Region proxy)
    - JOLTS: state-level labor market dynamics
    """
    geo_id = str(int(district_number)).zfill(2)
    result = {}

    conn = get_qibrain_connection()
    try:
        with conn.cursor() as cur:
            # --- LAUS: District unemployment ---
            cur.execute("""
                SELECT metric_name, metric_value, period_date
                FROM reference.bls_economic_data
                WHERE series_type = 'laus' AND geography_type = 'sld_upper'
                  AND geography_id = %s AND state_fips = '16'
                ORDER BY period_date DESC LIMIT 1
            """, (geo_id,))
            row = cur.fetchone()
            if row:
                result["bls_unemployment_rate"] = float(row["metric_value"])
                pd = row["period_date"]
                result["bls_unemployment_rate_period"] = pd.strftime("%b %Y") if pd else ""

            # --- LAUS: State unemployment for comparison ---
            cur.execute("""
                SELECT metric_value
                FROM reference.bls_economic_data
                WHERE series_type = 'laus' AND geography_type = 'state'
                  AND state_fips = '16' AND metric_name = 'unemployment_rate'
                ORDER BY period_date DESC LIMIT 1
            """)
            row = cur.fetchone()
            if row:
                result["bls_state_unemployment_rate"] = float(row["metric_value"])

            # --- QCEW: Industry employment & wages (state-level) ---
            cur.execute("""
                SELECT metric_name, metric_value, period_date, sector_name
                FROM reference.bls_economic_data
                WHERE series_type = 'qcew' AND geography_type = 'state'
                  AND state_fips = '16' AND metric_value > 0
                ORDER BY metric_name
            """)
            qcew_period = None
            for row in cur.fetchall():
                mn = row["metric_name"]
                sector = row["sector_name"]
                val = float(row["metric_value"])

                if mn.startswith("employment_"):
                    result[f"bls_qcew_{sector}_employment"] = int(val)
                elif mn.startswith("avg_weekly_wage_"):
                    result[f"bls_qcew_{sector}_wage"] = int(val)

                if qcew_period is None and row["period_date"]:
                    qcew_period = f"{row['period_date'].year} Annual"

            if qcew_period:
                result["bls_qcew_period"] = qcew_period

            # --- CPI: Cost of living (state-level, YoY changes only) ---
            cur.execute("""
                SELECT metric_name, metric_value, period_date
                FROM reference.bls_economic_data
                WHERE series_type = 'cpi' AND geography_type = 'state'
                  AND state_fips = '16' AND metric_name LIKE '%%yoy_change'
                ORDER BY metric_name
            """)
            cpi_period = None
            for row in cur.fetchall():
                mn = row["metric_name"]
                field = "bls_" + mn.replace("_yoy_change", "_yoy")
                result[field] = float(row["metric_value"])

                if cpi_period is None and row["period_date"]:
                    cpi_period = row["period_date"].strftime("%b %Y")

            if cpi_period:
                result["bls_cpi_period"] = cpi_period
                result["bls_cpi_region"] = "West"

            # --- JOLTS: Labor market dynamics (state-level, most recent) ---
            cur.execute("""
                SELECT metric_name, metric_value, period_date
                FROM reference.bls_economic_data
                WHERE series_type = 'jolts' AND geography_type = 'state'
                  AND state_fips = '16'
                ORDER BY period_date DESC
            """)
            jolts_seen = set()
            jolts_period = None
            for row in cur.fetchall():
                mn = row["metric_name"]
                if mn in jolts_seen:
                    continue
                jolts_seen.add(mn)

                val = float(row["metric_value"])
                if mn == "job_openings":
                    result["bls_jolts_job_openings"] = int(val * 1000)
                elif mn == "job_openings_rate":
                    result["bls_jolts_job_openings_rate"] = val
                elif mn == "hires_rate":
                    result["bls_jolts_hires_rate"] = val
                elif mn == "quits_rate":
                    result["bls_jolts_quits_rate"] = val
                elif mn == "layoffs_rate":
                    result["bls_jolts_layoffs_rate"] = val

                if jolts_period is None and row["period_date"]:
                    jolts_period = row["period_date"].strftime("%b %Y")

            if jolts_period:
                result["bls_jolts_period"] = jolts_period
    finally:
        conn.close()

    return result

# ---------------------------------------------------------------------------
# Legislator Roster (replaces IdahoLegislature.xlsx)
# ---------------------------------------------------------------------------

def get_legislator(email=None, district=None, chamber=None, name=None):
    """
    Flexible legislator lookup.
    Replaces: Excel file lookup from IdahoLegislature.xlsx.
    Supports lookup by email, by district+chamber, or by name.
    Returns dict or None.
    """
    conn = get_qibrain_connection()
    try:
        with conn.cursor() as cur:
            if email:
                cur.execute("""
                    SELECT email,
                           COALESCE(title, CASE WHEN chamber='Senate' THEN 'Senator'
                                               WHEN chamber='House' THEN 'Representative'
                                               ELSE '' END) AS title,
                           first_name, last_name,
                           district_id::text AS district,
                           COALESCE(seat, '') AS seat,
                           party, chamber
                    FROM legislators
                    WHERE LOWER(email) = LOWER(%s)
                      AND is_active = true
                    LIMIT 1
                """, (email,))
            elif district and chamber:
                # Map chamber names to codes stored in DB
                chamber_val = chamber
                if chamber and len(chamber) == 1:
                    chamber_val = "Senate" if chamber.upper() == "S" else "House"
                elif chamber and chamber.lower().startswith("sen"):
                    chamber_val = "Senate"
                elif chamber and (chamber.lower().startswith("rep") or chamber.lower().startswith("house")):
                    chamber_val = "House"
                cur.execute("""
                    SELECT email,
                           COALESCE(title, CASE WHEN chamber='Senate' THEN 'Senator'
                                               WHEN chamber='House' THEN 'Representative'
                                               ELSE '' END) AS title,
                           first_name, last_name,
                           district_id::text AS district,
                           COALESCE(seat, '') AS seat,
                           party, chamber
                    FROM legislators
                    WHERE district_id = %s AND chamber = %s
                      AND is_active = true
                    ORDER BY seat
                    LIMIT 1
                """, (int(district), chamber_val))
            elif name:
                # Try last name match first, then full name
                cur.execute("""
                    SELECT email,
                           COALESCE(title, CASE WHEN chamber='Senate' THEN 'Senator'
                                               WHEN chamber='House' THEN 'Representative'
                                               ELSE '' END) AS title,
                           first_name, last_name,
                           district_id::text AS district,
                           COALESCE(seat, '') AS seat,
                           party, chamber
                    FROM legislators
                    WHERE (LOWER(last_name) = LOWER(%s)
                           OR LOWER(first_name || ' ' || last_name) = LOWER(%s))
                      AND is_active = true
                    ORDER BY last_name
                    LIMIT 1
                """, (name, name))
            else:
                return None

            row = cur.fetchone()
            return dict(row) if row else None
    finally:
        conn.close()


def get_all_legislators():
    """
    Get all current legislators.
    Replaces: Full Excel roster load.
    Returns list of legislator dicts.
    """
    conn = get_qibrain_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT email,
                       COALESCE(title, CASE WHEN chamber='Senate' THEN 'Senator'
                                           WHEN chamber='House' THEN 'Representative'
                                           ELSE '' END) AS title,
                       first_name, last_name,
                       district_id::text AS district,
                       COALESCE(seat, '') AS seat,
                       party, chamber
                FROM legislators
                WHERE is_active = true
                ORDER BY chamber, district_id, seat
            """)
            return [dict(r) for r in cur.fetchall()]
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Bill Search (replaces LegiScan getSearch API)
# ---------------------------------------------------------------------------



def get_session_id(year=2026):
    """Get the current session's legiscan_session_id for a given year."""
    conn = get_qibrain_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT legiscan_session_id FROM sessions
                WHERE year = %s AND is_active = true
                ORDER BY legiscan_session_id DESC
                LIMIT 1
            """, (year,))
            row = cur.fetchone()
            return row["legiscan_session_id"] if row else None
    finally:
        conn.close()



def refresh_bill_from_legislature(bill_number, qibrain_bill_id):
    """
    On-demand check of a single bill against legislature.idaho.gov.
    Runs at briefer generation time to ensure freshest possible data.
    Updates QIBrain if status changed or fiscal note newly available.
    Timeout: 5 seconds. Non-blocking: returns silently on any error.
    """
    import requests
    import re
    import html as html_mod

    HEADERS = {
        'User-Agent': 'QuietImpact-BillBriefer/1.0 (Legislative analysis service for Idaho legislators; info@billbriefer.com)'
    }
    BASE = "https://legislature.idaho.gov/sessioninfo/2026/legislation"
    logger = logging.getLogger('qibrain_data')

    try:
        # Fetch the bill page (1 request, ~500ms)
        resp = requests.get(f"{BASE}/{bill_number}/", headers=HEADERS, timeout=5)
        if resp.status_code != 200:
            return

        # Parse current status from page
        text = html_mod.unescape(resp.text)
        text_clean = re.sub(r'<[^>]+>', ' ', text)
        text_clean = re.sub(r'\s+', ' ', text_clean).strip()

        conn = get_qibrain_connection()
        cur = conn.cursor()

        try:
            # Check if fiscal note is missing and SOP PDF exists
            cur.execute("SELECT fiscal_note_text, full_text FROM bills WHERE bill_id = %s", (qibrain_bill_id,))
            row = cur.fetchone()

            # Fetch bill text if missing
            if row and not row['full_text']:
                bill_pdf_url = f"https://legislature.idaho.gov/wp-content/uploads/sessioninfo/2026/legislation/{bill_number}.pdf"
                try:
                    pdf_resp = requests.get(bill_pdf_url, headers=HEADERS, timeout=5)
                    if pdf_resp.status_code == 200 and len(pdf_resp.content) > 100:
                        try:
                            import pdfplumber
                            pdf = pdfplumber.open(io.BytesIO(pdf_resp.content))
                            pages = [p.extract_text() for p in pdf.pages if p.extract_text()]
                            pdf.close()
                            bill_full_text = '\n\n'.join(pages)
                        except ImportError:
                            bill_full_text = None

                        if bill_full_text and len(bill_full_text.strip()) > 50:
                            cur.execute("UPDATE bills SET full_text = %s, updated_at = NOW() WHERE bill_id = %s", (bill_full_text, qibrain_bill_id))
                            conn.commit()
                            logger.info(f"On-demand refresh: fetched bill text for {bill_number} ({len(bill_full_text)} chars)")
                except Exception:
                    pass  # Bill text not available yet

            if row and not row['fiscal_note_text']:
                # Try to fetch SOP PDF
                sop_url = f"https://legislature.idaho.gov/wp-content/uploads/sessioninfo/2026/legislation/{bill_number}SOP.pdf"
                try:
                    sop_resp = requests.get(sop_url, headers=HEADERS, timeout=5)
                    if sop_resp.status_code == 200 and len(sop_resp.content) > 100:
                        import io
                        try:
                            import pdfplumber
                            pdf = pdfplumber.open(io.BytesIO(sop_resp.content))
                            pages = [p.extract_text() for p in pdf.pages if p.extract_text()]
                            pdf.close()
                            sop_text = '\n\n'.join(pages)
                        except ImportError:
                            from pypdf import PdfReader
                            reader = PdfReader(io.BytesIO(sop_resp.content))
                            pages = [p.extract_text() for p in reader.pages if p.extract_text()]
                            sop_text = '\n\n'.join(pages)

                        if sop_text and len(sop_text.strip()) > 50:
                            cur.execute("""
                                UPDATE bills SET fiscal_note_text = %s, sop_text = %s, updated_at = NOW()
                                WHERE bill_id = %s
                            """, (sop_text, sop_text, qibrain_bill_id))
                            conn.commit()
                            logger.info(f"On-demand refresh: fetched fiscal note for {bill_number} ({len(sop_text)} chars)")
                except Exception:
                    pass  # SOP not available, that's fine

            # Check for vote details we might be missing
            cur.execute("""
                SELECT bv.vote_id, bv.yeas, bv.nays, bv.absent
                FROM bill_votes bv
                LEFT JOIN legislator_votes lv ON lv.vote_id = bv.vote_id
                WHERE bv.bill_id = %s AND lv.id IS NULL
            """, (qibrain_bill_id,))
            missing_votes = cur.fetchall()

            if missing_votes:
                # Parse votes from the already-fetched bill page
                vote_pattern = re.compile(
                    r'(PASSED|FAILED)\s*[-\u2013\u2014]\s*(\d+)-(\d+)-(\d+)\s*'
                    r'AYES\s*[-\u2013\u2014]\s*(.*?)\s*'
                    r'NAYS\s*[-\u2013\u2014]\s*(.*?)\s*'
                    r'(?:Absent\s+and\s+Excused\s*[-\u2013\u2014]\s*(.*?))?'
                    r'(?:Floor\s+Sponsor|Subject:|Title:|$)',
                    re.DOTALL
                )

                # Build legislator lookup
                cur.execute("SELECT legislator_id, last_name FROM legislators WHERE is_active = true")
                leg_by_name = {ln.lower(): lid for lid, ln in cur.fetchall()}

                for match in vote_pattern.finditer(text_clean):
                    yeas = int(match.group(2))
                    nays = int(match.group(3))
                    absent = int(match.group(4))

                    # Find matching roll call
                    matched = None
                    for mv in missing_votes:
                        if mv[1] == yeas and mv[2] == nays and mv[3] == absent:
                            matched = mv[0]
                            break

                    if not matched:
                        continue

                    ayes_text = match.group(5) or ''
                    nays_text = match.group(6) or ''
                    absent_text = match.group(7) or ''

                    votes_added = 0
                    for cast_val, names_text in [('Yea', ayes_text), ('Nay', nays_text), ('Absent', absent_text)]:
                        for raw in names_text.split(','):
                            name = re.sub(r'\([^)]*\)', '', raw).strip()
                            name = re.sub(r'[^\w\s\'\-]', '', name).strip()
                            if not name or name.lower() == 'none':
                                continue
                            leg_id = leg_by_name.get(name.lower())
                            if leg_id:
                                try:
                                    cur.execute("""
                                        INSERT INTO legislator_votes (legislator_id, vote_id, vote_cast)
                                        VALUES (%s, %s, %s)
                                        ON CONFLICT (legislator_id, vote_id) DO NOTHING
                                    """, (leg_id, matched, cast_val))
                                    votes_added += 1
                                except Exception:
                                    pass

                    if votes_added > 0:
                        conn.commit()
                        logger.info(f"On-demand refresh: added {votes_added} individual votes for {bill_number}")

        finally:
            conn.close()

    except Exception as e:
        logger.warning(f"On-demand refresh failed for {bill_number}: {e} — using cached data")
