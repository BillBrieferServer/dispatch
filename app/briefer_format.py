"""
briefer_format.py
Contains the full sectioned email formatter for Idaho Bill Briefer.
Kept in its own module to avoid accidental loss when main.py is edited.
"""
from __future__ import annotations
from datetime import datetime
from zoneinfo import ZoneInfo
import re
from typing import Any, Dict, List, Optional, Tuple
from app.legislators import generate_briefer_id
from app.tenant_config import get_tenant_config


def _norm_text(x: Any) -> str:
    if x is None:
        return ""
    if isinstance(x, str):
        return x.strip()
    return str(x).strip()
def _history_sorted_desc(bill_obj: Dict[str, Any]) -> List[Dict[str, Any]]:
    history = bill_obj.get("history", []) or bill_obj.get("actions", []) or []
    if not isinstance(history, list):
        return []
    return sorted([h for h in history if isinstance(h, dict)], key=lambda h: str(h.get("date") or ""), reverse=True)


def _sponsors_lines(bill_obj: Dict[str, Any], max_items: int = 20) -> List[str]:
    sponsors = bill_obj.get("sponsors", []) or []
    lines: List[str] = []
    for sp in sponsors[:max_items]:
        if not isinstance(sp, dict):
            continue
        name = _norm_text(sp.get("name"))
        role = _norm_text(sp.get("role"))
        if not name:
            continue
        lines.append(f"• {name}" + (f" ({role})" if role else ""))
    return lines


def _history_lines(bill_obj: Dict[str, Any], max_items: int = 20) -> List[str]:
    lines: List[str] = []
    for h in _history_sorted_desc(bill_obj)[:max_items]:
        dt = _norm_text(h.get("date"))
        action = _norm_text(h.get("action") or h.get("action_desc"))
        if dt and action:
            lines.append(f"• {dt}: {action}")
        elif action:
            lines.append(f"• {action}")
    return lines


def _subjects_line(bill_obj: Dict[str, Any]) -> str:
    """Format bill subjects/topics as a single line."""
    subjects = bill_obj.get("subjects", []) or []
    if not isinstance(subjects, list) or not subjects:
        return ""
    names = []
    for subj in subjects:
        if isinstance(subj, dict):
            name = _norm_text(subj.get("subject_name"))
            if name:
                names.append(name)
        elif isinstance(subj, str):
            name = _norm_text(subj)
            if name:
                names.append(name)
    return " • ".join(names) if names else ""


def _committee_path_lines(bill_obj: Dict[str, Any]) -> List[str]:
    """Format committee referrals as a list of lines."""
    referrals = bill_obj.get("referrals", []) or []
    if not isinstance(referrals, list) or not referrals:
        return []
    lines: List[str] = []
    # Sort by date ascending to show chronological path
    sorted_refs = sorted(
        [r for r in referrals if isinstance(r, dict)],
        key=lambda r: str(r.get("date") or "")
    )
    for ref in sorted_refs:
        date = _norm_text(ref.get("date"))
        name = _norm_text(ref.get("name"))
        chamber = _norm_text(ref.get("chamber"))
        if not name:
            continue
        chamber_name = {"H": "House", "S": "Senate"}.get(chamber, chamber)
        if date:
            lines.append(f"• {date}: Referred to {name}" + (f" ({chamber_name})" if chamber_name else ""))
        else:
            lines.append(f"• Referred to {name}" + (f" ({chamber_name})" if chamber_name else ""))
    return lines


def _rollcall_items(bill_obj: Dict[str, Any]) -> List[Dict[str, Any]]:
    for key in ("votes", "roll_calls", "rollcalls"):
        v = bill_obj.get(key)
        if isinstance(v, list) and v:
            return [x for x in v if isinstance(x, dict)]
    return []


def _rollcall_summary_lines(bill_obj: Dict[str, Any]) -> List[str]:
    items = _rollcall_items(bill_obj)
    lines: List[str] = []
    for rc in items[:10]:
        date = _norm_text(rc.get("date"))
        desc = _norm_text(rc.get("desc") or rc.get("description"))
        yea = rc.get("yea")
        nay = rc.get("nay")
        nv = rc.get("nv")
        absent = rc.get("absent")

        counts = []
        if yea is not None:
            counts.append(f"Yea {yea}")
        if nay is not None:
            counts.append(f"Nay {nay}")
        if nv is not None:
            counts.append(f"NV {nv}")
        if absent is not None:
            counts.append(f"Absent {absent}")

        base = f"• {date}: {desc}" if date and desc else f"• {desc or 'Roll call'}"
        if counts:
            base += f" ({', '.join(counts)})"
        lines.append(base)
    return lines


def _format_vote_record(individual_votes: List[Dict[str, Any]], session_year: Optional[int] = None) -> str:
    """
    Format individual legislator votes by chamber.

    Args:
        individual_votes: List of roll call data with individual votes
        session_year: The legislative session year (e.g., 2025). Used to determine
                      appropriate messaging for bills without vote records.

    Format:
    Roll Call: Third Reading - H0245 (03/15/2025)
    Result: PASSED (Yea 52, Nay 17, Absent 1)

    HOUSE (52-17-1)
    Y  J. Smith (R)       Y  M. Jones (R)       N  T. Brown (D)
    ...

    SENATE (28-7-0)
    Y  R. Allen (R)       ...
    """
    if not individual_votes:
        # Determine if this is a current or prior session
        current_year = datetime.now().year
        if session_year and session_year < current_year:
            return "• No roll call votes were recorded for this bill."
        else:
            return "• No roll call votes recorded for this bill yet."

    # Filter to only the most recent roll call per chamber
    # This simplifies the briefer by showing final votes, not intermediate procedural votes
    chamber_latest: Dict[str, Dict] = {}
    for rc in individual_votes:
        # Determine chamber from description or chamber field
        desc_lower = (rc.get("description") or "").lower()
        chamber_field = (rc.get("chamber") or "").upper()

        if "house" in desc_lower or chamber_field == "H":
            chamber_key = "HOUSE"
        elif "senate" in desc_lower or chamber_field == "S":
            chamber_key = "SENATE"
        else:
            chamber_key = "OTHER"

        rc_date = rc.get("date") or ""
        existing = chamber_latest.get(chamber_key)

        # Keep the most recent (latest date) roll call for each chamber
        if not existing or rc_date > (existing.get("date") or ""):
            chamber_latest[chamber_key] = rc

    # Order: House first, then Senate
    # Skip "OTHER" classified roll calls - they're typically committee votes
    # that would create duplicate chamber sections (e.g., 6-person committee vote
    # creating a second SENATE section alongside the full 35-person floor vote)
    filtered_votes = []
    for key in ["HOUSE", "SENATE"]:
        if key in chamber_latest:
            # Store chamber key with the roll call so we know which votes to output
            rc_with_chamber = dict(chamber_latest[key])
            rc_with_chamber["_chamber_key"] = key
            filtered_votes.append(rc_with_chamber)

    parts: List[str] = []

    for rc in filtered_votes:
        rc_chamber_key = rc.get("_chamber_key", "OTHER")
        votes = rc.get("votes") or []
        if not votes:
            parts.append("• Individual legislator votes not available for this roll call.")
            parts.append("  (Vote details may not be recorded for older bills or voice votes.)")
            parts.append("")
            continue

        # Separate by chamber (role)
        house_votes: List[Dict] = []
        senate_votes: List[Dict] = []

        for v in votes:
            # Determine chamber from role or party info
            # LegiScan uses role like "Rep" for House, "Sen" for Senate
            # Or we can check the chamber field
            name = v.get("name", "") or v.get("last_name", "")
            first = v.get("first_name", "")
            party = v.get("party", "")
            vote_text = v.get("vote_text", "")

            # Extract last name if full name given
            if " " in name and not v.get("last_name"):
                parts_name = name.split()
                last_name = parts_name[-1]
                first_initial = parts_name[0][0] if parts_name else ""
            else:
                last_name = v.get("last_name", "") or name
                first_initial = first[0] if first else ""

            # Map vote_text to short form
            if vote_text in ("Yea", "Aye", "Yes"):
                vote_short = "Y"
            elif vote_text in ("Nay", "No"):
                vote_short = "N"
            elif vote_text in ("NV", "Not Voting", "Present"):
                vote_short = "-"
            elif vote_text in ("Absent", "Excused"):
                vote_short = "A"
            else:
                vote_short = "?"

            # Party short form
            party_short = ""
            if party:
                if party.upper().startswith("R"):
                    party_short = "R"
                elif party.upper().startswith("D"):
                    party_short = "D"
                elif party.upper().startswith("I"):
                    party_short = "I"
                else:
                    party_short = party[0].upper()

            vote_entry = {
                "last_name": last_name,
                "first_initial": first_initial,
                "party_short": party_short,
                "vote_short": vote_short,
            }

            # Separate into chambers - check role field from LegiScan
            role = str(v.get("role", "")).lower()
            if "sen" in role:
                senate_votes.append(vote_entry)
            elif "rep" in role:
                house_votes.append(vote_entry)
            else:
                # Fallback: use chamber from roll call if single chamber vote
                chamber_fallback = (rc.get("chamber") or "").upper()
                if chamber_fallback == "H":
                    house_votes.append(vote_entry)
                elif chamber_fallback == "S":
                    senate_votes.append(vote_entry)
                else:
                    # Default to house
                    house_votes.append(vote_entry)

        # Sort alphabetically by last name
        house_votes.sort(key=lambda x: x["last_name"].lower())
        senate_votes.sort(key=lambda x: x["last_name"].lower())

        # Get official vote totals from roll call data
        official_yea = rc.get("yea")
        official_nay = rc.get("nay")
        official_nv = rc.get("nv") or 0
        official_absent = rc.get("absent") or 0
        official_other = official_nv + official_absent

        def format_chamber_votes(chamber_name: str, votes_list: List[Dict],
                                  off_yea: int = None, off_nay: int = None, off_other: int = None) -> List[str]:
            if not votes_list:
                return []

            # Count votes from parsed individual data
            parsed_y = sum(1 for v in votes_list if v["vote_short"] == "Y")
            parsed_n = sum(1 for v in votes_list if v["vote_short"] == "N")
            parsed_other = len(votes_list) - parsed_y - parsed_n

            # Use official totals if available, otherwise fall back to parsed counts
            if off_yea is not None and off_nay is not None:
                y_count = off_yea
                n_count = off_nay
                a_count = off_other if off_other is not None else parsed_other
            else:
                y_count = parsed_y
                n_count = parsed_n
                a_count = parsed_other

            # Check if we have all individual votes - add note before header if some are missing
            official_total = (off_yea or 0) + (off_nay or 0) + (off_other or 0)
            parsed_total = len(votes_list)
            missing_count = official_total - parsed_total if off_yea is not None and parsed_total < official_total else 0

            lines = []
            # Add note about missing legislators before the header
            if missing_count > 0:
                lines.append(f"({missing_count} legislator name(s) unavailable in source data)")

            # Use ** markers for bold - renders as bold in PDF, visible emphasis in email
            lines.append(f"**{chamber_name} ({y_count}-{n_count}-{a_count})**")

            # Format each vote: "Y  J. Smith (R)"
            formatted = []
            for v in votes_list:
                first_init = v["first_initial"] + " " if v["first_initial"] else ""
                party_str = f" ({v['party_short']})" if v["party_short"] else ""
                entry = f"{first_init}{v['last_name']}{party_str} - {v['vote_short']}"
                formatted.append(entry)

            # Arrange in columns (3 columns for readability)
            # Sort DOWN columns (newspaper-style) instead of across rows
            import math
            num_cols = 3
            col_width = 24  # Fixed column width for alignment
            num_rows = math.ceil(len(formatted) / num_cols)
            row_lines = []
            for row_idx in range(num_rows):
                row_parts = []
                for col_idx in range(num_cols):
                    # Pick item from each column's position
                    item_idx = col_idx * num_rows + row_idx
                    if item_idx < len(formatted):
                        # Pad each column to fixed width for email alignment
                        row_parts.append(formatted[item_idx].ljust(col_width))
                    else:
                        row_parts.append("")
                # Use VOTE_ROW: prefix with pipe delimiter for PDF table parsing
                # The padded format also works for email display
                row_str = "VOTE_ROW:" + "|".join([p.strip() for p in row_parts if p.strip()])
                row_lines.append(row_str)

            lines.extend(row_lines)
            return lines

        # Only output votes matching the roll call's chamber classification
        # This prevents a HOUSE roll call from creating a SENATE section
        # if some senators happen to be in the roll call data
        if rc_chamber_key == "HOUSE" and house_votes:
            parts.extend(format_chamber_votes("HOUSE", house_votes, official_yea, official_nay, official_other))
            parts.append("")
        elif rc_chamber_key == "SENATE" and senate_votes:
            parts.extend(format_chamber_votes("SENATE", senate_votes, official_yea, official_nay, official_other))
            parts.append("")

    return "\n".join(parts).rstrip()


def _last_action_backfill(bill_obj: Dict[str, Any]) -> Tuple[str, str]:
    last_action = _norm_text(bill_obj.get("last_action"))
    last_action_date = _norm_text(bill_obj.get("last_action_date"))
    hist = _history_sorted_desc(bill_obj)
    if (not last_action or not last_action_date) and hist:
        h0 = hist[0]
        last_action_date = last_action_date or _norm_text(h0.get("date"))
        last_action = last_action or _norm_text(h0.get("action") or h0.get("action_desc"))
    return last_action_date, last_action


def _bullets(items: List[str], prefix: str = "• ", max_items: int = 12) -> str:
    out = []
    for x in items[:max_items]:
        x = _norm_text(x)
        if x:
            out.append(f"{prefix}{x}")
    return "\n".join(out) if out else f"{prefix}None listed"


def _format_questions_with_answers(q_items: Any, max_q: int = 8, max_a: int = 3) -> str:
    if not isinstance(q_items, list) or not q_items:
        return "• Questions not available for this bill."

    out: List[str] = []
    for item in q_items[:max_q]:
        if isinstance(item, str):
            q = _norm_text(item)
            if q:
                out.append(f"• Q: {q}")
            continue
        if isinstance(item, dict):
            q = _norm_text(item.get("question"))
            answers = item.get("sample_answers") or []
            if not q:
                continue
            out.append(f"• Q: {q}")
            if isinstance(answers, list) and answers:
                labels = ["Supportive", "Skeptical"]
                for idx, a in enumerate(answers[:max_a]):
                    a = _norm_text(a)
                    if a:
                        label = labels[idx] if idx < len(labels) else "Possible answer"
                        out.append(f"  {label}: {a}")
            continue

    return "\n".join(out) if out else "• None listed"


def _fallback_floor_statement(side: str, bullets: List[str]) -> str:
    b = [x for x in bullets if _norm_text(x)][:3]
    if not b:
        return "(Not available)"
    lead = "Supporters may argue this bill helps by:" if side == "pro" else "Critics may argue this bill raises concerns because it may:"
    lines = [lead] + [f"- {x}" for x in b] + ["(Verify against official bill text and fiscal note.)"]
    return "\n".join(lines)
def format_full_briefer(
    *,
    recipient_line: str,
    bill_number: str,
    bill_obj: Dict[str, Any],
    ai_json: Optional[Dict[str, Any]],
    census_text: Any,  # Dict with 'enabled', 'content', 'source' or empty string for backwards compat
    session_label: str,
    requester_name: str = "",
    requester_email: str = "",
    district_num: int = 0,
    individual_votes: Optional[List[Dict[str, Any]]] = None,
    update_note: str = "",
) -> str:
    title_official = _norm_text(bill_obj.get("title") or bill_obj.get("description"))
    description = _norm_text(bill_obj.get("description"))
    state_link = _norm_text(bill_obj.get("state_link") or bill_obj.get("url"))
    last_action_date, last_action = _last_action_backfill(bill_obj)

    sponsors_lines = _sponsors_lines(bill_obj)
    history_lines = _history_lines(bill_obj)
    roll_lines = _rollcall_summary_lines(bill_obj)

    # AI fields (graceful)
    one_para = ""
    key_points: List[str] = []
    who_affects: List[str] = []
    pros: List[str] = []
    cons: List[str] = []
    unknowns: List[str] = []
    questions_items: Any = []
    risk_flags: List[str] = []
    floor_pro = ""
    floor_con = ""
    tp_for: List[str] = []
    tp_against: List[str] = []

    if ai_json:
        one_para = _norm_text(ai_json.get("one_paragraph_summary"))
        key_points = ai_json.get("key_points") or []
        who_affects = ai_json.get("who_it_affects") or []
        # Filter out any Note: items from who_it_affects
        who_affects = [x for x in who_affects if not str(x).strip().startswith("Note:")]
        impacts = ai_json.get("potential_impacts") or {}
        if isinstance(impacts, dict):
            pros = impacts.get("pros") or []
            cons = impacts.get("cons") or []
            unknowns = impacts.get("unknowns") or []
        questions_items = ai_json.get("questions_to_ask") or []
        risk_flags = ai_json.get("risk_flags") or []
        floor_pro = _norm_text(ai_json.get("floor_statement_pro"))
        floor_con = _norm_text(ai_json.get("floor_statement_con"))
        tp_for = ai_json.get("talking_points_for") or []
        tp_against = ai_json.get("talking_points_against") or []

    # fallbacks
    if not tp_for:
        tp_for = [str(x) for x in pros]
    if not tp_against:
        tp_against = [str(x) for x in cons]
    if not floor_pro:
        floor_pro = _fallback_floor_statement("pro", [str(x) for x in tp_for])
    if not floor_con:
        floor_con = _fallback_floor_statement("con", [str(x) for x in tp_against])

    short_label = description or (one_para.split("\n")[0].strip() if one_para else "")
    if len(short_label) > 160:
        short_label = short_label[:157] + "..."
    parts: List[str] = []
    timestamp = datetime.now(ZoneInfo("America/Boise")).strftime("%m/%d/%Y %I:%M%p")
    if requester_name:
        parts.append(f"Prepared for {requester_name}")
    parts.append(f"Generated: {timestamp}")
    if requester_email and bill_number:
        briefer_id = generate_briefer_id(requester_email, bill_number)
        parts.append(f"Briefer ID: {briefer_id}")
    if update_note:
        parts.append(update_note)
    parts.append("")  # Extra spacing before header
    parts.append("")
    # Note: census_text is now handled in Section 4 (Who / What Is Affected)
    # Extract year from session_label (e.g., "ID Legislature — 2025 Session" -> "2025")
    session_year_match = re.search(r'(\d{4})', session_label)
    session_year = session_year_match.group(1) if session_year_match else "2026"
    parts.append(get_tenant_config()["header_format"].format(session_year=session_year))
    parts.append("")
    parts.append(f"{bill_number} — {short_label or title_official}")
    parts.append("")
    parts.append("1. Bill Snapshot")
    parts.append(f"Bill Number: {bill_number}")
    parts.append(f"Bill Title (Official): {title_official}")
    parts.append(f"Jurisdiction / Session: {session_label}")
    parts.append(f"Status / Last Action: {last_action_date} - {last_action}")
    topics_line = _subjects_line(bill_obj)
    if topics_line:
        parts.append(f"Topics: {topics_line}")
    parts.append(f"Link: {state_link}")
    parts.append("")

    parts.append("2. Plain-Language Summary")
    parts.append(one_para or "(AI summary unavailable; see bill snapshot and legislative activity below.)")
    parts.append("")

    parts.append("3. What the Bill Does")
    parts.append("Key Changes in Law/Policy")
    parts.append(_bullets([str(x) for x in key_points], prefix="• ", max_items=14))
    parts.append("")

    parts.append("4. Who / What Is Affected")
    parts.append(_bullets([str(x) for x in who_affects], prefix="• ", max_items=14))

    parts.append("")

    parts.append("5. Policy Considerations")
    parts.append("Potential Benefits")
    parts.append(_bullets([str(x) for x in pros], prefix="• ", max_items=10))
    parts.append("Potential Concerns")
    parts.append(_bullets([str(x) for x in cons], prefix="• ", max_items=10))
    parts.append("Key Unknowns / Data Needed")
    parts.append(_bullets([str(x) for x in unknowns], prefix="• ", max_items=12))
    parts.append("")

    parts.append("6. Debate Prep")
    parts.append("Pro Argument (Sample Statement):")
    parts.append(floor_pro)
    parts.append("")
    parts.append("Con Argument (Sample Statement):")
    parts.append(floor_con)
    parts.append("")
    parts.append("Talking Points FOR (what supporters may argue):")
    parts.append(_bullets([str(x) for x in tp_for], prefix="• ", max_items=8))
    parts.append("")
    parts.append("Talking Points AGAINST (what critics may argue):")
    parts.append(_bullets([str(x) for x in tp_against], prefix="• ", max_items=8))
    parts.append("")
    parts.append("Note: These arguments represent positions supporters and critics may take. They are provided to help you prepare, not to advocate for either position.")
    parts.append("")

    parts.append("7. Key Questions")
    parts.append(_format_questions_with_answers(questions_items, max_q=8, max_a=3))
    parts.append("")

    parts.append('8. Uncertainties to Watch')
    watch_items = [str(x) for x in (risk_flags or [])]
    if watch_items:
        parts.append(_bullets(watch_items, prefix="• ", max_items=14))
    else:
        parts.append("• No specific uncertainties identified.")
    parts.append("")

    parts.append("9. Legislative Activity")
    parts.append("Sponsors")
    parts.append("\n".join(sponsors_lines) if sponsors_lines else "• None listed")
    parts.append("")
    committee_lines = _committee_path_lines(bill_obj)
    if committee_lines:
        parts.append("Committee Path")
        parts.append("\n".join(committee_lines))
        parts.append("")
    parts.append("Bill History/Actions (most recent first)")
    parts.append("\n".join(history_lines) if history_lines else "• None listed")
    parts.append("")
    parts.append("Roll Calls")
    parts.append("\n".join(roll_lines) if roll_lines else "• None listed")
    parts.append("")

    # Vote Record - individual legislator votes
    parts.append("Vote Record")
    session_year_int = int(session_year) if session_year and session_year.isdigit() else None
    parts.append(_format_vote_record(individual_votes or [], session_year=session_year_int))
    parts.append("")

    parts.append("")

    # About This Briefer section
    parts.append("")
    parts.append("ABOUT THIS BRIEFER")
    parts.append("")
    parts.append("This analysis combines official legislative documents, fiscal data, and demographic information with artificial intelligence to produce a structured briefing. All source data comes from verified government sources. AI is used to analyze, summarize, and identify policy considerations — not to generate facts, statistics, or quotes.")
    parts.append("Data Sources")
    parts.append("Idaho Legislature, US Census Bureau (ACS 2023), US Bureau of Labor Statistics (v2.0)")
    parts.append("BLS_FOOTNOTE: BLS.gov cannot vouch for the data or analyses derived from these data after the data have been retrieved from BLS.gov.")
    parts.append("Disclaimer")
    parts.append("This briefer is an informational time-saver, not a replacement for personal judgment. It is not legal advice or a voting recommendation. Please verify any details you plan to cite by reviewing official sources at legislature.idaho.gov.")
    parts.append(get_tenant_config()["footer_line"])
    parts.append("")
    parts.append(f"_{get_tenant_config()['tagline']}_")

    return "\n".join(parts)
