"""Email formatting and sending for the Bill Briefer application."""

import os
import re
from app.tenant_config import get_tenant_config
import html
import time
import logging
import smtplib
from email.message import EmailMessage
from typing import Optional

from app.utils import (
    _html_to_text,
    FROM_EMAIL, SMTP_HOST, SMTP_PORT, SMTP_USER, SMTP_PASS,
)

logger = logging.getLogger(__name__)

def plain_text_to_html(s: str) -> str:
    """Convert briefer plain text into structured, professional HTML.

    - '1. Title' => bold section header with number badge
    - 'Key: Value' runs => two-column table
    - bullets (-, *, •) => list
    """
    import re
    import html as _html

    s = (s or "").replace("\r\n", "\n").replace("\r", "\n")
    raw_lines = [ln.rstrip() for ln in s.split("\n")]

    # Remove duplicate consecutive 'Legislative Briefing'
    cleaned = []
    prev = None
    for ln in raw_lines:
        t = (ln or "").strip()
        if prev and t.lower() == prev.lower() == "legislative briefing":
            continue
        cleaned.append(ln)
        prev = t

    sec_re = re.compile(r"^(\d+)\.\s+(.+)$")
    kv_re  = re.compile(r"^([^:]{1,45}):\s*(.+)$")

    out = []
    in_ul = False
    in_sponsors = False
    in_snapshot_box = False  # SNAPSHOT_FULL_BOX_V1
    in_vote_table = False  # Vote record table

    kv_rows = []
    current_section_title = ""
    in_history_timeline = False  # Timeline: Bill history/actions

    def esc(x: str) -> str:
        return _html.escape(x or "")

    def close_ul():
        nonlocal in_ul
        if in_ul:
            out.append("</ul>")
            in_ul = False

    def flush_kv():
        nonlocal kv_rows
        if not kv_rows:
            return
        wrap_snapshot = (current_section_title == "bill snapshot") and (not in_snapshot_box)
        if wrap_snapshot:
            out.append("<div style='background:#f8fafc;border:1px solid #e6e8eb;border-radius:10px;padding:12px;margin:8px 0 14px 0;'>")
        out.append("<table style='width:100%;border-collapse:separate;border-spacing:0 8px;margin:6px 0 6px 0;'>")
        for k, v in kv_rows:
            vv = (v or "").strip()
            v_html = f"<a href='{esc(vv)}'>{esc(vv)}</a>" if (vv.startswith('http://') or vv.startswith('https://')) else esc(vv)
            out.append("<tr>")
            out.append("<td style='width:34%%;vertical-align:top;color:#555;font-size:13px;padding-right:10px;'><strong>%s</strong></td>" % esc(k))
            out.append("<td style='vertical-align:top;color:#222;font-size:14px;word-break:break-word;'>%s</td>" % v_html)
            out.append("</tr>")
        out.append("</table>")
        if wrap_snapshot:
            out.append("</div>")
        kv_rows = []

    for raw in cleaned:
        t = (raw or "").strip()

        if not t:
            flush_kv(); close_ul()
            if in_sponsors:
                continue
            if in_history_timeline:
                out.append("</div>")
                in_history_timeline = False
            if in_vote_table:
                out.append("</table>")
                in_vote_table = False
            out.append("<div style='height:10px'></div>")
            continue

        # Prepared for line (requester name) - comes first, minimal spacing
        if t.startswith("Prepared for "):
            flush_kv(); close_ul()
            out.append(f"<div style='text-align:left;font-size:11px;font-style:italic;color:#666;margin:0 0 2px 0;'>{esc(t)}</div>")
            continue
        # Briefer ID line - comes second, minimal spacing
        if t.startswith("#ID") or t.startswith("#MT") or t.startswith("Briefer ID:"):
            flush_kv(); close_ul()
            out.append(f"<div style='text-align:left;font-size:11px;font-style:italic;color:#666;margin:0 0 2px 0;'>{esc(t)}</div>")
            continue
        # Update note line - same style as header metadata
        if t.startswith("Updated:"):
            flush_kv(); close_ul()
            out.append(f"<div style='text-align:left;font-size:11px;font-style:italic;color:#666;margin:0 0 16px 0;'>{esc(t)}</div>")
            continue
        # Timestamp line: MM/DD/YYYY HH:MMAM/PM - comes third, normal spacing after
        import re as _timestamp_re
        if t.startswith("Generated:") or _timestamp_re.match(r'^\d{2}/\d{2}/\d{4}\s+\d{1,2}:\d{2}[AP]M$', t):
            flush_kv(); close_ul()
            out.append(f"<div style='text-align:left;font-size:11px;font-style:italic;color:#666;margin:0 0 2px 0;'>{esc(t)}</div>")
            continue
        # Section 5 subheaders - bold them
        if t in ("Potential Benefits", "Potential Concerns", "Key Unknowns / Data Needed"):
            flush_kv(); close_ul()
            out.append(f"<div style='font-weight:bold;color:#333;margin:12px 0 6px 0;'>{esc(t)}</div>")
            continue
        # Section 6 subheaders - bold them
        if t in ("Pro Argument (Sample Statement):", "Con Argument (Sample Statement):", "Talking Points FOR (what supporters may argue):", "Talking Points AGAINST (what critics may argue):"):
            flush_kv(); close_ul()
            out.append(f"<div style='font-weight:bold;color:#333;margin:12px 0 6px 0;'>{esc(t)}</div>")
            continue
        # Section 3 subheaders - bold them
        if t == "Key Changes in Law/Policy":
            flush_kv(); close_ul()
            out.append(f"<div style='font-weight:bold;color:#333;margin:12px 0 6px 0;'>{esc(t)}</div>")
            continue

        # Section 10 subheaders - match Bill History/Actions style
        if t in ("Roll Calls", "Vote Record", "Committee Path"):
            flush_kv(); close_ul()
            out.append(f"<h3 style='font-size:14px;margin:14px 0 8px 0;color:#222;'><strong>{esc(t)}</strong></h3>")
            continue
        # Section 10 similar bill headers - bold them (e.g., "California SB64 (2025) - SIGNED INTO LAW")
        similar_bill_re = re.compile(r"^([A-Za-z][A-Za-z ]+)\s+([A-Z]+\d+)\s+\((\d{4})\)\s+-\s+(.+)$")
        if similar_bill_re.match(t):
            flush_kv(); close_ul()
            out.append(f"<div style='font-weight:bold;color:#1a365d;margin:14px 0 4px 0;'>{esc(t)}</div>")
            continue
        # Section 10 "Similarity:" line
        if t.startswith("Similarity:"):
            flush_kv(); close_ul()
            out.append(f"<div style='margin:2px 0;color:#666;font-size:13px;'>{esc(t)}</div>")
            continue
        # Section 10 "Title:" line - bold the label
        if t.startswith("Title:"):
            flush_kv(); close_ul()
            rest = t[6:].strip()
            out.append(f"<div style='margin:2px 0;'><span style='font-weight:bold;'>Title:</span> {esc(rest)}</div>")
            continue
        # Section 10 "Additional Similar Bills Found" header - bold it
        if t.startswith("Additional Similar Bills Found"):
            flush_kv(); close_ul()
            out.append(f"<div style='font-weight:bold;color:#1a365d;margin:14px 0 4px 0;'>{esc(t)}</div>")
            continue
        # Section 10 factual labels - bold them
        section10_labels = [
            "Legislative Outcome:",
            "Comparison to Idaho Bill:",
            "Governor's Veto Statement:",
            "Committee/Floor Action:",
            "Governor:",
            "Final:",
            "States:",
        ]
        matched_label = False
        for label in section10_labels:
            if t.startswith(label):
                flush_kv(); close_ul()
                rest = t[len(label):].strip()
                out.append(f"<div style='margin:2px 0;'><span style='font-weight:bold;'>{label}</span> {esc(rest)}</div>")
                matched_label = True
                break
        if matched_label:
            continue
        # Section 10 comparison bullet points (- California bill: ..., - Idaho bill: ...)
        if t.startswith("- ") and (" bill:" in t):
            flush_kv(); close_ul()
            out.append(f"<div style='margin:2px 0 2px 16px;'>{esc(t)}</div>")
            continue
        # Section 10 outcome bullet points (- X bills passed...)
        if t.startswith("- ") and ("bills " in t):
            flush_kv(); close_ul()
            out.append(f"<div style='margin:2px 0 2px 16px;'>{esc(t)}</div>")
            continue
        # Section 10 vote results (House: Passed 92-8...)
        vote_line_re = re.compile(r"^(House|Senate|Assembly):\s+")
        if vote_line_re.match(t):
            flush_kv(); close_ul()
            out.append(f"<div style='margin:2px 0 2px 8px;'>{esc(t)}</div>")
            continue
        # Section 10 horizontal separator lines
        if t == "---":
            flush_kv(); close_ul()
            out.append("<hr style='border:none;border-top:1px solid #ccc;margin:16px 0;'>")
            continue
        # Section 10 note at end
        if t.startswith("Note: Analysis shows"):
            flush_kv(); close_ul()
            out.append(f"<div style='font-style:italic;color:#666;margin:8px 0 4px 0;'>{esc(t)}</div>")
            continue
        # Section 9 District Profile note - small font, italicized
        if t.startswith("Note: This analysis"):
            flush_kv(); close_ul()
            out.append(f"<div style='font-size:11px;font-style:italic;color:#666;margin:8px 0 4px 0;'>{esc(t)}</div>")
            continue
        # Section 6 Floor Statements note - small font, italicized (matches Section 9)
        if t.startswith("Note: These arguments") or t.startswith("Note    These arguments"):
            flush_kv(); close_ul()
            # Normalize "Note    " to "Note: "
            note_text = t.replace("Note    ", "Note: ")
            out.append(f"<div style='font-size:11px;font-style:italic;color:#666;margin:8px 0 4px 0;'>{esc(note_text)}</div>")
            continue
        # About This Briefer section - bold titles
        if t == "ABOUT THIS BRIEFER":
            flush_kv(); close_ul()
            out.append(f"<div style='font-weight:bold;font-size:12px;color:#333;margin:20px 0 6px 0;padding-top:14px;border-top:1px solid #e6e8eb;'>{esc(t)}</div>")
            continue
        about_section_titles = ["Data Sources", "Disclaimer"]
        if t in about_section_titles:
            flush_kv(); close_ul()
            out.append(f"<div style='font-weight:bold;font-size:10px;color:#333;margin:8px 0 0px 0;'>{esc(t)}</div>")
            continue
        # BLS footnote - smaller/lighter text
        if t.startswith("BLS_FOOTNOTE:"):
            flush_kv(); close_ul()
            footnote_text = t.replace("BLS_FOOTNOTE: ", "")
            out.append(f"<div style='font-size:9px;color:#888;margin:2px 0 0 0;font-style:italic;'>\u2020{esc(footnote_text)}</div>")
            continue
        # Org footer line - bold org name
        _org = get_tenant_config()["org_name"]
        if t.startswith(_org):
            flush_kv(); close_ul()
            rest = t.replace(_org, "").strip()
            out.append(f"<div style='font-size:10px;margin:12px 0 0 0;'><strong>{esc(_org)}</strong>{esc(rest)}</div>")
            continue
        # Tagline - italicized (marked with underscores in source)
        if t.startswith("_") and t.endswith("_") and len(t) > 10:
            flush_kv(); close_ul()
            tagline = t.strip("_")
            out.append(f"<div style='font-size:10px;margin:4px 0 0 0;font-style:italic;'>{esc(tagline)}</div>")
            continue
        # About section content paragraphs - 10pt font
        about_content_starts = [
            "This analysis combines official legislative documents",
            "Idaho Legislature, US Census Bureau",
            "This briefer is an informational time-saver",
        ]
        is_about_content = any(t.startswith(s) for s in about_content_starts)
        if is_about_content:
            flush_kv(); close_ul()
            out.append(f"<div style='font-size:10px;margin:0;'>{esc(t)}</div>")
            continue
        # Section 10 veto statement content (quoted text after "Governor's Veto Statement:")
        if t.startswith("Vetoed by Governor"):
            flush_kv(); close_ul()
            out.append(f"<div style='margin:2px 0 2px 8px;font-style:italic;'>{esc(t)}</div>")
            continue
        # Section 4 demographic context header - bold it
        if t == "Demographic Context":
            flush_kv(); close_ul()
            out.append(f"<div style='font-weight:bold;color:#1a365d;margin:12px 0 6px 0;'>{esc(t)}</div>")
            continue
        # Idaho Bill Briefer header
        if "BRIEFER" in t.upper() and "SESSION" in t.upper():
            flush_kv(); close_ul()
            out.append(f"<div style='font-size:18px;font-weight:bold;color:#111;margin:16px 0 2px 0;'>{esc(t)}</div>")
            continue
        # Disclaimer line
        if t.startswith("This is not legal advice"):
            flush_kv(); close_ul()
            out.append(f"<div style='font-size:11px;font-style:italic;color:#666;margin:0 0 16px 0;'>{esc(t)}</div>")
            continue
        # Bill title line: e.g., S1212 — CAMPAIGN FINANCE -- Long description
        m_title = re.match(r"^([A-Za-z]{1,5}\d{1,6})\s*—\s*(.+)$", t)
        if m_title:
            flush_kv(); close_ul()
            left, right = t.split("—", 1)
            billnum = left.strip()
            right = right.strip()

            short = right
            desc = ""
            if "--" in right:
                a, b = right.split("--", 1)
                short = a.strip(" -–—")
                desc = b.strip(" -–—")

            title_line = f"{billnum} — {short}".strip()
            out.append("<div style='font-size:22px;font-weight:800;color:#111;margin:4px 0 8px 0;'>%s</div>" % esc(title_line))
            if desc:
                out.append("<div style='font-size:14px;color:#444;margin:0 0 12px 0;'>%s</div>" % esc(desc))
            continue

        # Timeline block for: Bill history/actions
        if t.lower().startswith("bill history/actions"):
            flush_kv(); close_ul()
            if in_history_timeline:
                out.append("</div>")
                in_history_timeline = False

            # HISTORY_CLOSES_SPONSORS_SAFE_V1
            if in_sponsors:
                out.append("</ul></div>")
                in_sponsors = False

            # Keep the label as a subheader
            out.append("<h3 style='font-size:14px;margin:14px 0 8px 0;color:#222;'><strong>Bill History/Actions</strong></h3>")
            out.append("<div style='border-left:3px solid #e6e8eb;padding-left:12px;margin:8px 0 14px 0;'>")
            in_history_timeline = True
            continue

        if in_history_timeline:
            m_hist = re.match(r"^(\d{4}-\d{2}-\d{2}):\s*(.+)$", t)
            if m_hist:
                dt = m_hist.group(1)
                action = m_hist.group(2)
                out.append("<div style='margin:0 0 8px 0;'><strong>%s</strong>: %s</div>" % (esc(dt), esc(action)))
                continue
            else:
                # Exit timeline when the next non-date line appears (e.g., Roll calls)
                out.append("</div>")
                in_history_timeline = False

        # Numbered section headers: 1. Bill Snapshot, etc.
        # SPONSORS_RENDER_V1
        # If we are inside Sponsors, collect lines until the next section/label.
        if in_sponsors:
            low = t.lower().strip()
            if sec_re.match(t) or ("bill history/actions" in low) or ("roll calls" in low) or low in ("data source", "data sources", "disclaimer"):
                out.append("</ul></div>")
                in_sponsors = False
                continue
                # if this line is a new header/section, fall through and let normal logic handle it
            else:
                item = t
                if item.startswith("• "):
                    item = item[2:].strip()
                out.append("<li style='margin:4px 0;color:#222;'>%s</li>" % esc(item))
                continue

        # Sponsors header
        if t.strip().lower() == "sponsors":
            flush_kv(); close_ul()
            out.append("<h3 style='font-size:14px;margin:14px 0 8px 0;color:#222;'><strong>Sponsors</strong></h3>")
            out.append("<div style='background:#f8fafc;border:1px solid #e6e8eb;border-radius:10px;padding:10px 12px;margin:6px 0 14px 0;'>")
            out.append("<ul style='margin:0;padding-left:18px;'>")
            in_sponsors = True
            continue

        m = sec_re.match(t)
        if m:
            # Close Snapshot box before starting the next section header
            if in_snapshot_box:
                out.append("</div>")
                in_snapshot_box = False
            flush_kv(); close_ul()
            num, title = m.group(1), m.group(2)
            current_section_title = (title or "").strip().lower()
            out.append(
                "<h2 style='font-size:18px;font-weight:800;margin:18px 0 10px 0;padding-top:14px;border-top:1px solid #e6e8eb;'>"
                f"<span style='display:inline-block;min-width:22px;text-align:center;margin-right:8px;"
                f"background:#eef2ff;border:1px solid #dfe3ff;border-radius:6px;padding:2px 6px;font-size:12px;color:#334;'>"
                f"{esc(num)}</span>{esc(title)}</h2>"
            )

            # SNAPSHOT_FULL_BOX_V1: open/close unified box for section 1
            if (title or "").strip().lower() == "bill snapshot":
                out.append("<div style='background:#f8fafc;border:1px solid #e6e8eb;border-radius:10px;padding:12px;margin:8px 0 14px 0;'>")
                in_snapshot_box = True
            else:
                # Close snapshot box when leaving section 1
                if in_snapshot_box:
                    out.append("</div>")
                    in_snapshot_box = False

            continue

        # QA_RENDER_FIX_V1: render Q/A lines as Q blocks + indented answers (avoid KV-table formatting)
        m_q = re.match(r'^(?:[•\-*]\s*)?Q(?:[:\t ]+)\s*(.+)$', t)
        if m_q:
            flush_kv(); close_ul()
            q = m_q.group(1).strip()
            out.append("<div style='margin:0 0 8px 0;'><strong>Q:</strong> %s</div>" % esc(q))
            continue

        # S7_ANSWER_RENDER: Supportive/Skeptical answer lines from Section 7
        m_sa = re.match(r'^\s*(?:[\u2022\-*]\s*)?(Supportive|Skeptical)(?:[:\t ]+)\s*(.+)$', t, re.IGNORECASE)
        if m_sa:
            flush_kv(); close_ul()
            label = m_sa.group(1).capitalize()
            ans = m_sa.group(2).strip()
            border_color = "#4a9e6f" if label == "Supportive" else "#c0392b"
            out.append("<div style='margin:0 0 10px 18px;padding:8px 10px;border-left:3px solid %s;background:#f8fafc;border-radius:8px;color:#444;font-size:13px;'><em>%s:</em> %s</div>" % (border_color, esc(label), esc(ans)))
            continue

        m_a = re.match(r'^(?:[•\-*]\s*)?Possible answer(?:[:\t ]+)\s*(.+)$', t, re.IGNORECASE)
        if m_a:
            flush_kv(); close_ul()
            ans = m_a.group(1).strip()
            out.append("<div style='margin:0 0 10px 18px;padding:8px 10px;border-left:3px solid #e6e8eb;background:#f8fafc;border-radius:8px;color:#444;font-size:13px;'><em>Possible answer:</em> %s</div>" % esc(ans))
            continue

        # VOTE_ROW: lines - render as HTML table for proper column alignment
        # Must be checked BEFORE kv_re matching since VOTE_ROW: matches key-value pattern
        if t.startswith("VOTE_ROW:"):
            flush_kv(); close_ul()
            vote_data = t[9:]  # Remove "VOTE_ROW:" prefix
            cols = vote_data.split("|")
            # Pad to 3 columns
            while len(cols) < 3:
                cols.append("")
            out.append("<tr>")
            for col in cols:
                out.append(f"<td style='padding:2px 28px 2px 0;vertical-align:top;white-space:nowrap;'>{esc(col.strip())}</td>")
            out.append("</tr>")
            continue

        # Chamber vote headers (HOUSE (35-12-3) or SENATE (25-10-0))
        # Also handles **HOUSE (35-12-3)** bold format
        chamber_vote_re = re.compile(r'^\*{0,2}(HOUSE|SENATE)\s+\((\d+)-(\d+)-(\d+)\)\*{0,2}$')
        m_chamber = chamber_vote_re.match(t)
        if m_chamber:
            flush_kv(); close_ul()
            # Close any existing vote table before starting new one
            if in_vote_table:
                out.append("</table>")
            chamber = m_chamber.group(1)
            y_count = m_chamber.group(2)
            n_count = m_chamber.group(3)
            a_count = m_chamber.group(4)
            out.append(f"<div style='font-weight:bold;color:#333;margin:12px 0 6px 0;'>{esc(chamber)} ({y_count}-{n_count}-{a_count})</div>")
            out.append("<table style='border-collapse:collapse;margin:0 0 8px 0;'>")
            in_vote_table = True
            continue

        mkv = kv_re.match(t)
        if mkv and current_section_title != "fiscal note summary":
            k = mkv.group(1).strip()
            v = mkv.group(2).strip()
            # Skip bullet points - they should not be treated as key-value pairs
            if t.startswith("•") or t.startswith("-") or t.startswith("*"):
                pass  # Fall through to normal bullet processing
            elif len(k) <= 45 and len(v) <= 500:
                kv_rows.append((k, v))
                continue

        # QA_FORMAT_FIX_V1
        # Q lines may arrive as: "• Q<TAB>Question..." or "Q: Question..." or "Q Question..."
        m_q = re.match(r"^(?:[•\-*]\s*)?Q(?:[:\t]|\s+)\s*(.+)$", t)
        if m_q:
            flush_kv(); close_ul()
            q = m_q.group(1).strip()
            out.append("<div style='margin:0 0 8px 0;'><strong>Q:</strong> %s</div>" % esc(q))
            continue

        # Possible answer lines may arrive as: "Possible answer<TAB>..." or "Possible answer: ..."
        # S7_ANSWER_RENDER_V2: Supportive/Skeptical answer lines (second handler)
        m_sa2 = re.match(r'^\s*(?:[\u2022\-*]\s*)?(Supportive|Skeptical)(?:[:\t]|\s+)\s*(.+)$', t, re.IGNORECASE)
        if m_sa2:
            flush_kv(); close_ul()
            label = m_sa2.group(1).capitalize()
            ans = m_sa2.group(2).strip()
            border_color = "#4a9e6f" if label == "Supportive" else "#c0392b"
            out.append("<div style='margin:0 0 10px 18px;padding:8px 10px;border-left:3px solid %s;background:#f8fafc;border-radius:8px;color:#444;font-size:13px;'><em>%s:</em> %s</div>" % (border_color, esc(label), esc(ans)))
            continue

        m_a = re.match(r"^Possible answer(?:[:\t]|\s+)\s*(.+)$", t, re.IGNORECASE)
        if m_a:
            flush_kv(); close_ul()
            ans = m_a.group(1).strip()
            out.append("<div style='margin:0 0 10px 18px;padding:8px 10px;border-left:3px solid #e6e8eb;background:#f8fafc;border-radius:8px;color:#444;font-size:13px;'><em>Possible answer:</em> %s</div>" % esc(ans))
            continue

        if t.startswith('- ') or t.startswith('* ') or t.startswith('• '):
            flush_kv()
            if not in_ul:
                out.append("<ul style='margin:8px 0 12px 18px;padding:0;'>")
                in_ul = True
            out.append("<li style='margin:4px 0;color:#222;'>%s</li>" % esc(t[2:].strip()))
            continue

        flush_kv(); close_ul()
        out.append("<p style='margin:0 0 10px 0;color:#222;'>%s</p>" % esc(t))

    if in_sponsors:
        out.append("</ul></div>")
        in_sponsors = False
    flush_kv(); close_ul()
    if in_vote_table:
        out.append("</table>")
        in_vote_table = False
    if in_snapshot_box:
        out.append("</div>")
        in_snapshot_box = False
    return "\n".join(out)


def wrap_email_html(inner_html: str) -> str:
    # Wrap content in a clean, readable “card” (safe for most email clients)
    inner = (inner_html or "").strip()

    footer = ""  # Footer content now in About This Briefer section

    return f"""<!doctype html>
<html>
  <head>
    <meta charset=\"utf-8\">
    <meta name=\"viewport\" content=\"width=device-width,initial-scale=1\">
    <title>{get_tenant_config()["org_name"]}</title>
  </head>
  <body style=\"margin:0;padding:0;background:#f5f7fa;\">
    <div style=\"font-family:Arial,system-ui,-apple-system,Segoe UI,Roboto,sans-serif;background:#f5f7fa;padding:24px;\">
      <div style=\"max-width:760px;margin:0 auto;background:#ffffff;border:1px solid #e6e8eb;border-radius:12px;padding:24px;line-height:1.45;\">
        {inner}
        {footer}
      </div>
    </div>
  </body>
</html>"""


def send_email(to_email: str, subject: str, body: str, pdf_attachment: Optional[bytes] = None, pdf_filename: str = "briefing.pdf") -> None:
    if not (FROM_EMAIL and SMTP_HOST and SMTP_USER and SMTP_PASS):
        raise RuntimeError("Email settings missing in .env (FROM_EMAIL/SMTP_HOST/SMTP_USER/SMTP_PASS).")

    body = body or ""
    # QA_NORMALIZE_V2: normalize Q/A formatting (matches New_briefer_template)
    import re as _re
    body = _re.sub(r"(?m)^\s*[•\-*]?\s*Q[\t ]+\s*", "Q: ", body)
    body = _re.sub(r"(?mi)^\s*Possible answer[\t ]+\s*", "Possible answer: ", body)
    body = _re.sub(r"(?mi)^\s+(?=(?:Supportive|Skeptical)[:	 ])", "", body)


    # ---- SPECIAL CASE: SIGN-IN / LOGIN EMAILS ----
    # If the body contains the sign-in token URL, send PLAIN TEXT ONLY (no HTML wrapper/footer).
    # This preserves the simple format:
    #   Click this link to sign in:\n\nhttps://.../auth?token=...\n\nThis link expires in 15 minutes.
    if "/auth?token=" in body:
        msg = EmailMessage()
        msg["From"] = FROM_EMAIL
        msg["To"] = to_email
        msg["Subject"] = subject
        msg["Reply-To"] = "info@billbriefer.com"
        msg.set_content(body)
        with smtplib.SMTP(SMTP_HOST, SMTP_PORT, timeout=30) as s:
            s.starttls()
            s.login(SMTP_USER, SMTP_PASS)
            s.send_message(msg)
        return

    # ---- DEFAULT: BRIEFERS AND OTHER EMAILS ----
    lower = body.lower()
    looks_html = any(tag in lower for tag in (
        "<html", "<body", "<div", "<p", "<table", "<br", "<h1", "<h2", "<ul", "<ol", "<li"
    ))

    msg = EmailMessage()
    msg["From"] = FROM_EMAIL
    msg["To"] = to_email
    msg["Subject"] = subject
    msg["Reply-To"] = "info@billbriefer.com"

    # Always include a plain-text fallback
    msg.set_content(_html_to_text(body) if looks_html else body)

    # Always include HTML (wrap in our nicer layout)
    if looks_html:
        html_body = body
    else:
        html_body = plain_text_to_html(body)

    msg.add_alternative(wrap_email_html(html_body), subtype="html")

    # Add PDF attachment if provided
    if pdf_attachment:
        msg.add_attachment(pdf_attachment, maintype='application', subtype='pdf', filename=pdf_filename)

    last_error = None
    for attempt in range(3):
        try:
            with smtplib.SMTP(SMTP_HOST, SMTP_PORT, timeout=30) as s:
                s.starttls()
                s.login(SMTP_USER, SMTP_PASS)
                s.send_message(msg)
            return  # Success
        except smtplib.SMTPAuthenticationError:
            raise  # Don't retry auth errors
        except Exception as e:
            last_error = e
            if attempt < 2:
                time.sleep(2)
    if last_error:
        raise last_error

