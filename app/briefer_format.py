"""
briefer_format.py
Dispatch Leadership Briefer — 7-section text assembly.

Sections:
  1. Bill Summary (AI)
  2. Sponsor Profile (AI + data)
  3. Unintended Consequences (module)
  4. Power Flag (module)
  5. Momentum (AI + data)
  6. Advocacy Positions (deterministic)
  7. Legislative Activity (deterministic)
"""
from __future__ import annotations
from datetime import datetime
from zoneinfo import ZoneInfo
import math
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
        lines.append(f"\u2022 {name}" + (f" ({role})" if role else ""))
    return lines


def _history_lines(bill_obj: Dict[str, Any], max_items: int = 20) -> List[str]:
    lines: List[str] = []
    for h in _history_sorted_desc(bill_obj)[:max_items]:
        dt = _norm_text(h.get("date"))
        action = _norm_text(h.get("action") or h.get("action_desc"))
        if dt and action:
            lines.append(f"\u2022 {dt}: {action}")
        elif action:
            lines.append(f"\u2022 {action}")
    return lines


def _committee_path_lines(bill_obj: Dict[str, Any]) -> List[str]:
    referrals = bill_obj.get("referrals", []) or []
    if not isinstance(referrals, list) or not referrals:
        return []
    lines: List[str] = []
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
            lines.append(f"\u2022 {date}: Referred to {name}" + (f" ({chamber_name})" if chamber_name else ""))
        else:
            lines.append(f"\u2022 Referred to {name}" + (f" ({chamber_name})" if chamber_name else ""))
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
        base = f"\u2022 {date}: {desc}" if date and desc else f"\u2022 {desc or 'Roll call'}"
        if counts:
            base += f" ({', '.join(counts)})"
        lines.append(base)
    return lines


def _format_vote_record(individual_votes: List[Dict[str, Any]], session_year: Optional[int] = None) -> str:
    if not individual_votes:
        current_year = datetime.now().year
        if session_year and session_year < current_year:
            return "\u2022 No roll call votes were recorded for this bill."
        else:
            return "\u2022 No roll call votes recorded for this bill yet."

    chamber_latest: Dict[str, Dict] = {}
    for rc in individual_votes:
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
        if not existing or rc_date > (existing.get("date") or ""):
            chamber_latest[chamber_key] = rc

    filtered_votes = []
    for key in ["HOUSE", "SENATE"]:
        if key in chamber_latest:
            rc_with_chamber = dict(chamber_latest[key])
            rc_with_chamber["_chamber_key"] = key
            filtered_votes.append(rc_with_chamber)

    parts: List[str] = []

    for rc in filtered_votes:
        rc_chamber_key = rc.get("_chamber_key", "OTHER")
        votes = rc.get("votes") or []
        if not votes:
            parts.append("\u2022 Individual legislator votes not available for this roll call.")
            parts.append("  (Vote details may not be recorded for older bills or voice votes.)")
            parts.append("")
            continue

        house_votes: List[Dict] = []
        senate_votes: List[Dict] = []

        for v in votes:
            name = v.get("name", "") or v.get("last_name", "")
            first = v.get("first_name", "")
            party = v.get("party", "")
            vote_text = v.get("vote_text", "")

            if " " in name and not v.get("last_name"):
                parts_name = name.split()
                last_name = parts_name[-1]
                first_initial = parts_name[0][0] if parts_name else ""
            else:
                last_name = v.get("last_name", "") or name
                first_initial = first[0] if first else ""

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

            role = str(v.get("role", "")).lower()
            if "sen" in role:
                senate_votes.append(vote_entry)
            elif "rep" in role:
                house_votes.append(vote_entry)
            else:
                chamber_fallback = (rc.get("chamber") or "").upper()
                if chamber_fallback == "H":
                    house_votes.append(vote_entry)
                elif chamber_fallback == "S":
                    senate_votes.append(vote_entry)
                else:
                    house_votes.append(vote_entry)

        house_votes.sort(key=lambda x: x["last_name"].lower())
        senate_votes.sort(key=lambda x: x["last_name"].lower())

        official_yea = rc.get("yea")
        official_nay = rc.get("nay")
        official_nv = rc.get("nv") or 0
        official_absent = rc.get("absent") or 0
        official_other = official_nv + official_absent

        def format_chamber_votes(chamber_name: str, votes_list: List[Dict],
                                  off_yea=None, off_nay=None, off_other=None) -> List[str]:
            if not votes_list:
                return []
            parsed_y = sum(1 for v in votes_list if v["vote_short"] == "Y")
            parsed_n = sum(1 for v in votes_list if v["vote_short"] == "N")
            parsed_other = len(votes_list) - parsed_y - parsed_n
            if off_yea is not None and off_nay is not None:
                y_count = off_yea
                n_count = off_nay
                a_count = off_other if off_other is not None else parsed_other
            else:
                y_count = parsed_y
                n_count = parsed_n
                a_count = parsed_other
            official_total = (off_yea or 0) + (off_nay or 0) + (off_other or 0)
            parsed_total = len(votes_list)
            missing_count = official_total - parsed_total if off_yea is not None and parsed_total < official_total else 0
            lines = []
            if missing_count > 0:
                lines.append(f"({missing_count} legislator name(s) unavailable in source data)")
            lines.append(f"**{chamber_name} ({y_count}-{n_count}-{a_count})**")
            formatted = []
            for v in votes_list:
                first_init = v["first_initial"] + " " if v["first_initial"] else ""
                party_str = f" ({v['party_short']})" if v["party_short"] else ""
                entry = f"{first_init}{v['last_name']}{party_str} - {v['vote_short']}"
                formatted.append(entry)
            num_cols = 3
            col_width = 24
            num_rows = math.ceil(len(formatted) / num_cols)
            for row_idx in range(num_rows):
                row_parts = []
                for col_idx in range(num_cols):
                    item_idx = col_idx * num_rows + row_idx
                    if item_idx < len(formatted):
                        row_parts.append(formatted[item_idx].ljust(col_width))
                row_str = "VOTE_ROW:" + "|".join([p.strip() for p in row_parts if p.strip()])
                lines.append(row_str)
            return lines

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


# ---------------------------------------------------------------------------
# Main formatter — 7-section Dispatch Leadership Briefer
# ---------------------------------------------------------------------------

def format_full_briefer(
    *,
    recipient_line: str,
    bill_number: str,
    bill_obj: Dict[str, Any],
    ai_json: Optional[Dict[str, Any]],
    census_text: Any = None,
    session_label: str,
    requester_name: str = "",
    requester_email: str = "",
    district_num: int = 0,
    individual_votes: Optional[List[Dict[str, Any]]] = None,
    update_note: str = "",
) -> str:
    title_official = _norm_text(bill_obj.get("title") or bill_obj.get("description"))
    description = _norm_text(bill_obj.get("description"))
    last_action_date, last_action = _last_action_backfill(bill_obj)

    ai = ai_json or {}

    # --- Header ---
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
    parts.append("")
    parts.append("")

    session_year_match = re.search(r'(\d{4})', session_label)
    session_year = session_year_match.group(1) if session_year_match else "2026"
    parts.append(get_tenant_config()["header_format"].format(session_year=session_year))
    parts.append("")

    short_label = description or title_official
    if short_label and len(short_label) > 160:
        short_label = short_label[:157] + "..."
    parts.append(f"{bill_number} \u2014 {short_label or ''}")
    parts.append("")

    # --- Section 1: Bill Summary ---
    parts.append("1. Bill Summary")
    bill_summary = _norm_text(ai.get("bill_summary"))
    parts.append(bill_summary or "(Summary unavailable.)")
    parts.append("")

    # --- Section 2: Sponsor Profile ---
    parts.append("2. Sponsor Profile")
    sd = ai.get("sponsor_display") or {}
    contacts = sd.get("contacts", [])

    if contacts:
        from app.ai_brief import ORG_FULL_NAMES

        # One line per sponsor: "Rep. Name, LD4, IACI: 87%, IFF: 36%"
        for idx, c in enumerate(contacts):
            label = f"{c.get('title', '')} {c['name']}".strip()
            if c.get('ld'):
                label += f", {c['ld']}"
            scores = c.get("scores", [])
            if scores:
                score_strs = [f"{s['org']}: {round(s['pct'])}%" for s in scores]
                label += ", " + ", ".join(score_strs)
            # Bills count for single sponsor only
            if len(contacts) == 1 and c.get("bills_this_session"):
                bc = c['bills_this_session']
                label += f" | {bc} {'bill' if bc == 1 else 'bills'} this session"
            parts.append(label)

        parts.append("")

        # Score key: small italic, "(2025 Score)" format
        all_orgs = []
        seen_key_orgs = set()
        for c in contacts:
            for s in c.get("scores", []):
                org = s["org"]
                if org not in seen_key_orgs:
                    seen_key_orgs.add(org)
                    full_name = ORG_FULL_NAMES.get(org, org)
                    year = s.get("year")
                    if org == "CPAC":
                        year_label = "Lifetime"
                    elif year:
                        year_label = f"{year} Score"
                    else:
                        year_label = ""
                    entry = f"{org} = {full_name}"
                    if year_label:
                        entry += f" ({year_label})"
                    all_orgs.append(entry)
        if all_orgs:
            parts.append(f"SCORE_KEY: {' | '.join(all_orgs)}")
    else:
        parts.append("(Sponsor data unavailable.)")
    parts.append("")

    # --- Section 3: Unintended Consequences ---
    parts.append("3. Unintended Consequences")
    consequences = ai.get("unintended_consequences") or []
    if isinstance(consequences, list) and consequences:
        for item in consequences:
            parts.append(f"\u2022 {_norm_text(item)}")
    else:
        parts.append("\u2022 No unintended consequences identified.")
    parts.append("")

    # --- Section 4: Power Flag ---
    parts.append("4. Power Flag")
    pf = ai.get("power_flag") or {}
    if isinstance(pf, dict):
        flag_level = _norm_text(pf.get("flag_level"))
        direction = _norm_text(pf.get("direction"))
        explanation = _norm_text(pf.get("explanation"))

        display_level = flag_level.upper() if flag_level and flag_level != "none" else "NONE"
        parts.append(f"Authority Shift: {display_level}")
        if direction and direction != "none":
            parts.append(f"Direction: {direction}")
        if explanation:
            parts.append(f"Explanation: {explanation}")
    else:
        parts.append("Authority Shift: NONE")
        parts.append("No authority shift detected.")
    parts.append("")

    # --- Section 5: Momentum ---
    parts.append("5. Momentum")
    mom = ai.get("momentum") or {}
    if isinstance(mom, dict):
        trajectory = _norm_text(mom.get("trajectory"))
        days = mom.get("days_since_introduction")
        hearing = _norm_text(mom.get("hearing_status"))
        narrative = _norm_text(mom.get("narrative"))

        if trajectory:
            parts.append(f"Trajectory: {trajectory}")
        if days is not None:
            parts.append(f"Days Since Introduction: {days}")
        if hearing:
            parts.append(f"Hearing Status: {hearing}")
        if narrative:
            parts.append(narrative)
    else:
        parts.append("(Momentum data unavailable.)")
    parts.append("")

    # --- Section 6: Advocacy Positions ---
    parts.append("6. Advocacy Positions")
    adv = ai.get("advocacy_positions") or {}
    positions = adv.get("positions", []) if isinstance(adv, dict) else []
    coalition_alert = adv.get("coalition_alert") if isinstance(adv, dict) else None

    if positions:
        for pos in positions:
            if isinstance(pos, dict):
                org = _norm_text(pos.get("org_name"))
                position = _norm_text(pos.get("position"))
                detail = _norm_text(pos.get("position_detail"))
                if org and position:
                    line = f"{org}: {position}"
                    if detail:
                        line += f" \u2014 {detail}"
                    parts.append(line)
        if coalition_alert:
            parts.append("")
            parts.append(f"\u26a1 COALITION ALERT: {coalition_alert}")
    else:
        parts.append("No advocacy organizations are currently tracking this bill.")
    parts.append("")

    # --- Section 7: Legislative Activity ---
    parts.append("7. Legislative Activity")

    sponsors_lines = _sponsors_lines(bill_obj)
    parts.append("Sponsors")
    parts.append("\n".join(sponsors_lines) if sponsors_lines else "\u2022 None listed")
    parts.append("")

    committee_lines = _committee_path_lines(bill_obj)
    if committee_lines:
        parts.append("Committee Path")
        parts.append("\n".join(committee_lines))
        parts.append("")

    history_lines = _history_lines(bill_obj)
    parts.append("Bill History/Actions (most recent first)")
    parts.append("\n".join(history_lines) if history_lines else "\u2022 None listed")
    parts.append("")

    roll_lines = _rollcall_summary_lines(bill_obj)
    parts.append("Roll Calls")
    parts.append("\n".join(roll_lines) if roll_lines else "\u2022 None listed")
    parts.append("")

    parts.append("Vote Record")
    session_year_int = int(session_year) if session_year and session_year.isdigit() else None
    parts.append(_format_vote_record(individual_votes or [], session_year=session_year_int))
    parts.append("")

    # --- Disclaimer ---
    parts.append("")
    disclaimer = ai.get("disclaimer") or {}
    if isinstance(disclaimer, dict) and disclaimer.get("content"):
        parts.append(disclaimer.get("title", "ABOUT THIS BRIEFER"))
        parts.append("")
        parts.append(disclaimer["content"])
    else:
        parts.append("ABOUT THIS BRIEFER")
        parts.append("")
        parts.append(
            "This analysis was generated by Dispatch using Claude AI (Anthropic) "
            "from official Idaho legislative documents. Advocacy positions are pulled "
            "from public organizational trackers. All factual claims should be verified "
            "against primary sources before citing publicly."
        )
    parts.append(get_tenant_config()["footer_line"])
    parts.append("")
    parts.append(f"_{get_tenant_config()['tagline']}_")

    return "\n".join(parts)
