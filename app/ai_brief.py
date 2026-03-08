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

def _build_sponsor_context(bill_id: int) -> str:
    """Assemble sponsor data from QIBrain for AI context."""
    try:
        from app.services.qibrain_data import get_bill_sponsors, get_qibrain_connection
        sponsors = get_bill_sponsors(bill_id)
        if not sponsors:
            return "Committee-sponsored bill — no named individual sponsor."

        primary = sponsors[0]
        name = primary.get("name", "Unknown")
        first_name = primary.get("first_name", "")
        last_name = primary.get("last_name", "")
        party = primary.get("party", "")
        district = primary.get("district", "")

        lines = [f"Primary sponsor: {name} ({party}), District {district}"]

        # Count bills this session
        conn = get_qibrain_connection()
        try:
            with conn.cursor() as cur:
                # Count bills where this person is primary sponsor
                cur.execute("""
                    SELECT count(*) as cnt FROM idaho.bills b
                    JOIN bill_sponsors bs ON b.bill_id = bs.bill_id
                    WHERE bs.name = %s AND bs.sponsor_order = 1
                    AND b.legiscan_session_id = 2246
                """, (name,))
                row = cur.fetchone()
                bill_count = row["cnt"] if row else 0
                lines.append(f"Bills sponsored this session: {bill_count}")

                # IACI scores
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
                """, (last_name, district))
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
    except Exception as e:
        logger.warning(f"Failed to build sponsor context: {e}")
        return "Sponsor data unavailable."


def _build_momentum_context(bill_id: int) -> str:
    """Assemble momentum data from QIBrain for AI context."""
    try:
        from app.services.qibrain_data import get_bill_actions, get_qibrain_connection
        actions = get_bill_actions(bill_id)

        if not actions:
            return "No bill actions found."

        # Days since introduction
        intro_action = None
        for a in reversed(actions):  # oldest first
            if "introduced" in (a.get("action", "").lower()):
                intro_action = a
                break
        if not intro_action and actions:
            intro_action = actions[-1]  # oldest action as fallback

        intro_date_str = intro_action.get("action_date", "") if intro_action else ""
        days_since = "unknown"
        if intro_date_str:
            try:
                intro_date = datetime.strptime(intro_date_str, "%Y-%m-%d")
                days_since = (datetime.now() - intro_date).days
            except ValueError:
                pass

        # Last action
        last = actions[0] if actions else {}
        last_action = last.get("action", "Unknown")
        last_date = last.get("action_date", "Unknown")

        # Hearing status
        conn = get_qibrain_connection()
        try:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT hearing_date, committee_name
                    FROM idaho.committee_agendas
                    WHERE bill_number = (
                        SELECT bill_number FROM idaho.bills WHERE bill_id = %s
                    )
                    ORDER BY hearing_date DESC LIMIT 1
                """, (bill_id,))
                hearing = cur.fetchone()
        finally:
            conn.close()

        hearing_status = "No hearing scheduled"
        if hearing:
            hearing_status = f"Hearing: {hearing['hearing_date']} in {hearing['committee_name']}"

        lines = [
            f"Days since introduction: {days_since}",
            f"Total events: {len(actions)}",
            f"Last action: {last_action} ({last_date})",
            f"Hearing status: {hearing_status}",
        ]
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
