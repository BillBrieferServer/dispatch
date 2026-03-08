"""
pdf_render.py
Renders bill briefer PDFs with formatting that mirrors the email template.
"""
from __future__ import annotations

import re
from io import BytesIO
from typing import Optional, List, Tuple

from reportlab.lib.pagesizes import letter
from reportlab.lib.colors import HexColor
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.units import inch
from reportlab.platypus import (
    SimpleDocTemplate,
    Paragraph,
    Spacer,
    Table,
    TableStyle,
    HRFlowable,
)
from reportlab.lib.enums import TA_LEFT, TA_CENTER
from reportlab.pdfgen import canvas as pdfgen_canvas
from app.tenant_config import get_tenant_config


class NumberedCanvas(pdfgen_canvas.Canvas):
    """Custom canvas that adds page numbers to each page."""
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._page_number = 0

    def showPage(self):
        self._page_number += 1
        self._draw_page_number()
        super().showPage()

    def _draw_page_number(self):
        self.saveState()
        self.setFont("Helvetica", 9)
        self.setFillColorRGB(0.4, 0.4, 0.4)
        text = f"{self._page_number}"
        self.drawCentredString(4.25 * 72, 0.4 * 72, text)  # 72 points per inch
        self.restoreState()


# Colors matching email template
COLOR_PRIMARY = HexColor("#111111")
COLOR_SECONDARY = HexColor("#444444")
COLOR_MUTED = HexColor("#666666")
COLOR_BORDER = HexColor("#e6e8eb")
COLOR_BG_LIGHT = HexColor("#f8fafc")
COLOR_BADGE_BG = HexColor("#eef2ff")
COLOR_BADGE_BORDER = HexColor("#dfe3ff")

# Census demographic context block colors
COLOR_CENSUS_BG = HexColor("#F0F4F8")      # Light blue background
COLOR_CENSUS_BORDER = HexColor("#CBD5E0")  # Gray border
COLOR_CENSUS_HEADER = HexColor("#1a365d")  # Dark blue header
COLOR_CENSUS_SOURCE = HexColor("#718096")  # Gray source text


def _create_styles():
    """Create paragraph styles matching email format."""
    styles = getSampleStyleSheet()

    # Main header (IDAHO BILL BRIEFER — 2025 SESSION)
    styles.add(ParagraphStyle(
        name='IBB_MainHeader',
        parent=styles['Normal'],
        fontName='Helvetica-Bold',
        fontSize=14,
        textColor=COLOR_PRIMARY,
        spaceAfter=4,
    ))

    # Disclaimer line
    styles.add(ParagraphStyle(
        name='IBB_Disclaimer',
        parent=styles['Normal'],
        fontName='Helvetica-Oblique',
        fontSize=9,
        textColor=COLOR_MUTED,
        spaceAfter=20,
    ))

    # Bill title (H0123 — TITLE)
    styles.add(ParagraphStyle(
        name='IBB_BillTitle',
        parent=styles['Normal'],
        fontName='Helvetica-Bold',
        fontSize=16,
        leading=22,
        textColor=COLOR_PRIMARY,
        spaceAfter=8,
        spaceBefore=4,
    ))

    # Bill description (longer subtitle)
    styles.add(ParagraphStyle(
        name='IBB_BillDescription',
        parent=styles['Normal'],
        fontName='Helvetica',
        fontSize=11,
        textColor=COLOR_SECONDARY,
        spaceAfter=20,
    ))

    # Section headers (1. Bill Snapshot)
    styles.add(ParagraphStyle(
        name='IBB_SectionHeader',
        parent=styles['Normal'],
        fontName='Helvetica-Bold',
        fontSize=13,
        textColor=COLOR_PRIMARY,
        leftIndent=0,
        spaceBefore=0,
        spaceAfter=8,
    ))

    # Sub-headers (Sponsors, Bill History/Actions)
    styles.add(ParagraphStyle(
        name='IBB_SubHeader',
        parent=styles['Normal'],
        fontName='Helvetica-Bold',
        fontSize=11,
        textColor=COLOR_PRIMARY,
        spaceBefore=10,
        spaceAfter=6,
    ))

    # Body text
    styles.add(ParagraphStyle(
        name='IBB_Body',
        parent=styles['Normal'],
        fontName='Helvetica',
        fontSize=10,
        textColor=COLOR_PRIMARY,
        spaceAfter=6,
        leading=14,
    ))

    # Bullet items - text style (bullet rendered separately via table)
    styles.add(ParagraphStyle(
        name='IBB_Bullet',
        parent=styles['Normal'],
        fontName='Helvetica',
        fontSize=10,
        textColor=COLOR_PRIMARY,
        spaceAfter=0,
        leading=13,
    ))

    # Bullet character style
    styles.add(ParagraphStyle(
        name='IBB_BulletChar',
        parent=styles['Normal'],
        fontName='Helvetica',
        fontSize=10,
        textColor=COLOR_PRIMARY,
        leading=13,
    ))

    # Q/A Question - hanging indent so wrapped text aligns with first letter after "Q: "
    styles.add(ParagraphStyle(
        name='IBB_Question',
        parent=styles['Normal'],
        fontName='Helvetica-Bold',
        fontSize=10,
        textColor=COLOR_PRIMARY,
        leftIndent=14,
        firstLineIndent=-14,
        spaceAfter=2,
    ))

    # Q/A Answer
    styles.add(ParagraphStyle(
        name='IBB_Answer',
        parent=styles['Normal'],
        fontName='Helvetica-Oblique',
        fontSize=9,
        textColor=COLOR_SECONDARY,
        leftIndent=18,
        spaceAfter=8,
        leading=12,
    ))

    # Key-value label
    styles.add(ParagraphStyle(
        name='IBB_KVLabel',
        parent=styles['Normal'],
        fontName='Helvetica-Bold',
        fontSize=9,
        textColor=COLOR_MUTED,
    ))

    # Key-value value
    styles.add(ParagraphStyle(
        name='IBB_KVValue',
        parent=styles['Normal'],
        fontName='Helvetica',
        fontSize=10,
        textColor=COLOR_PRIMARY,
        splitLongWords=False,  # Don't split words - wrap whole words to next line
    ))

    # Prepared for / briefer ID (left-aligned)
    styles.add(ParagraphStyle(
        name='IBB_PreparedFor',
        parent=styles['Normal'],
        fontName='Helvetica-Oblique',
        fontSize=9,
        textColor=COLOR_MUTED,
        alignment=TA_LEFT,
        spaceAfter=0,
    ))

    # Timestamp (left-aligned)
    styles.add(ParagraphStyle(
        name='IBB_Timestamp',
        parent=styles['Normal'],
        fontName='Helvetica-Oblique',
        fontSize=9,
        textColor=COLOR_MUTED,
        alignment=TA_LEFT,
        spaceAfter=0,
    ))

    # Briefer ID (left-aligned, space after before main header)
    styles.add(ParagraphStyle(
        name='IBB_BrieferId',
        parent=styles['Normal'],
        fontName='Helvetica-Oblique',
        fontSize=9,
        textColor=COLOR_MUTED,
        alignment=TA_LEFT,
        spaceAfter=2,
    ))

    # Update note (italic, muted, same size as header metadata)
    styles.add(ParagraphStyle(
        name='IBB_UpdateNote',
        parent=styles['Normal'],
        fontName='Helvetica-Oblique',
        fontSize=9,
        textColor=COLOR_MUTED,
        alignment=TA_LEFT,
        spaceAfter=12,
    ))

    # District Profile Note (small, italic)
    styles.add(ParagraphStyle(
        name='IBB_Note',
        parent=styles['Normal'],
        fontName='Helvetica-Oblique',
        fontSize=8,
        textColor=COLOR_MUTED,
        spaceBefore=6,
        leading=11,
    ))

    # Footer
    styles.add(ParagraphStyle(
        name='IBB_Footer',
        parent=styles['Normal'],
        fontName='Helvetica',
        fontSize=8,
        textColor=COLOR_MUTED,
        spaceBefore=12,
        leading=11,
    ))

    # Census demographic context block styles
    styles.add(ParagraphStyle(
        name='IBB_CensusHeader',
        parent=styles['Normal'],
        fontName='Helvetica-Bold',
        fontSize=10,
        textColor=COLOR_CENSUS_HEADER,
        spaceAfter=4,
    ))

    styles.add(ParagraphStyle(
        name='IBB_CensusBody',
        parent=styles['Normal'],
        fontName='Helvetica',
        fontSize=9,
        textColor=COLOR_SECONDARY,
        leading=12,
        spaceAfter=4,
    ))

    styles.add(ParagraphStyle(
        name='IBB_CensusSource',
        parent=styles['Normal'],
        fontName='Helvetica-Oblique',
        fontSize=8,
        textColor=COLOR_CENSUS_SOURCE,
    ))


    # About This Briefer section styles
    styles.add(ParagraphStyle(
        name='IBB_AboutHeader',
        parent=styles['Normal'],
        fontName='Helvetica-Bold',
        fontSize=8,
        textColor=HexColor('#1a365d'),
        spaceBefore=6,
        spaceAfter=4,
    ))

    styles.add(ParagraphStyle(
        name='IBB_AboutSubHeader',
        parent=styles['Normal'],
        fontName='Helvetica-Bold',
        fontSize=8,
        textColor=COLOR_SECONDARY,
        spaceBefore=6,
        spaceAfter=2,
    ))

    styles.add(ParagraphStyle(
        name='IBB_AboutBody',
        parent=styles['Normal'],
        fontName='Helvetica',
        fontSize=8,
        textColor=COLOR_SECONDARY,
        spaceAfter=3,
        leading=13,
    ))

    return styles


def _normalize_text(text: str) -> str:
    """Normalize Unicode characters to ASCII equivalents for Helvetica font compatibility."""
    # Dashes and hyphens
    text = text.replace("–", "-")   # en-dash
    text = text.replace("—", "-")   # em-dash
    text = text.replace("‐", "-")   # Unicode hyphen
    text = text.replace("‑", "-")   # non-breaking hyphen
    text = text.replace("−", "-")   # minus sign
    text = text.replace("⁃", "-")   # hyphen bullet

    # Quotes
    text = text.replace(""", '"')   # left double quote
    text = text.replace(""", '"')   # right double quote
    text = text.replace("'", "'")   # left single quote
    text = text.replace("'", "'")   # right single quote
    text = text.replace("„", '"')   # low double quote
    text = text.replace("‚", "'")   # low single quote

    # Other common problematic characters
    text = text.replace("…", "...")  # ellipsis
    text = text.replace("•", "*")    # bullet (we add our own)
    text = text.replace("·", "-")    # middle dot
    text = text.replace("°", " deg") # degree symbol
    text = text.replace("™", "(TM)") # trademark
    text = text.replace("®", "(R)")  # registered
    text = text.replace("©", "(C)")  # copyright

    return text


def _escape_html(text: str) -> str:
    """Normalize and escape special characters for ReportLab Paragraph."""
    text = _normalize_text(text)
    text = text.replace("&", "&amp;")
    text = text.replace("<", "&lt;")
    text = text.replace(">", "&gt;")
    return text


def _parse_briefer_text(body_text: str) -> List[Tuple[str, str]]:
    """
    Parse the plain text briefer output into typed segments.
    Returns list of (type, content) tuples.

    Types: prepared_for, timestamp, main_header, disclaimer, bill_title,
           section_header, sub_header, bullet, question, answer, kv_pair,
           census_header, census_body, census_source, vote_row, body, blank
    """
    segments = []
    lines = (body_text or "").split("\n")

    # Patterns
    section_re = re.compile(r"^(\d+[A-Za-z]?)\.\s+(.+)$")
    timestamp_re = re.compile(r"^\d{2}/\d{2}/\d{4}\s+\d{1,2}:\d{2}[AP]M$")

    sub_headers = {
        "key changes in law/policy",
        "potential benefits",
        "potential concerns",
        "key unknowns / data needed",
        "pro argument (sample statement):",
        "con argument (sample statement):",
        "talking points for (what supporters may argue):",
        "talking points against (what critics may argue):",
        "sponsors",
        "committee path",
        "bill history/actions",
        "bill history/actions (most recent first)",
        "roll calls",
        "vote record", }

    # About This Briefer section titles
    about_section_titles = {
        "data sources",
        "disclaimer",
    }

    # Track current section to handle context-specific parsing
    current_section = ""
    in_floor_statement = False  # Track if we're inside Pro/Con argument body

    i = 0
    while i < len(lines):
        line = lines[i].rstrip()
        stripped = line.strip()
        lower = stripped.lower()

        # Blank line
        if not stripped:
            segments.append(("blank", ""))
            in_floor_statement = False  # End floor statement on blank line
            i += 1
            continue

        # Prepared for line
        if stripped.startswith("Prepared for "):
            segments.append(("prepared_for", stripped))
            i += 1
            continue

        # Briefer ID line
        if stripped.startswith("Briefer ID:"):
            segments.append(("briefer_id", stripped))
            i += 1
            continue

        # Update note line
        if stripped.startswith("Updated:"):
            segments.append(("update_note", stripped))
            i += 1
            continue

        # Timestamp (with or without "Generated:" prefix)
        if stripped.startswith("Generated:") or timestamp_re.match(stripped):
            segments.append(("timestamp", stripped))
            i += 1
            continue

        # Census demographic context block
        if stripped == "Demographic Context":
            segments.append(("census_header", stripped))
            i += 1
            continue

        # Census source line - removed, source now in footer Data Sources section

        # Main header (both policy and fiscal briefers)
        if ("BRIEFER" in stripped.upper() and "SESSION" in stripped.upper()):
            segments.append(("main_header", stripped))
            i += 1
            continue

        # Multi-agency banner
        if stripped == "*** MULTI-AGENCY APPROPRIATION ***":
            segments.append(("multi_agency_banner", stripped))
            i += 1
            continue

        # Disclaimer
        if stripped.startswith("This is not legal advice"):
            segments.append(("disclaimer", stripped))
            i += 1
            continue

        # About This Briefer main header
        if stripped == "ABOUT THIS BRIEFER":
            segments.append(("about_header", stripped))
            i += 1
            continue

        # About This Briefer sub-headers
        if lower in about_section_titles:
            segments.append(("about_sub_header", stripped))
            i += 1
            continue

        # BLS footnote - render as smaller text
        if stripped.startswith("BLS_FOOTNOTE:"):
            footnote_text = stripped.replace("BLS_FOOTNOTE: ", "")
            segments.append(("about_footnote", footnote_text))
            i += 1
            continue

        # Org footer line
        _org = get_tenant_config()["org_name"]
        if stripped.startswith(_org):
            segments.append(("about_footer", stripped))
            i += 1
            continue

        # Tagline (italicized, marked with underscores)
        if stripped.startswith("_") and stripped.endswith("_") and len(stripped) > 10:
            segments.append(("about_tagline", stripped))
            i += 1
            continue

        # Bill title (e.g., "H0123 — TITLE")
        if re.match(r"^[A-Za-z]{1,5}\d{1,6}\s*[—\-]\s*.+$", stripped):
            segments.append(("bill_title", stripped))
            i += 1
            continue

        # Section header (1. Bill Snapshot)
        m = section_re.match(stripped)
        if m:
            num, title = m.group(1), m.group(2)
            segments.append(("section_header", f"{num}. {title}"))
            current_section = title.lower().strip()
            in_floor_statement = False
            i += 1
            continue

        # Sub-headers (strip ** bold markers before matching)
        lower_clean = lower.strip("*")
        stripped_clean = stripped.strip("*")
        if lower_clean.rstrip(":") in sub_headers or lower_clean in sub_headers:
            segments.append(("sub_header", stripped_clean))
            # Track if entering a floor statement section
            in_floor_statement = lower in ("pro argument (sample statement):", "con argument (sample statement):")
            i += 1
            continue

        # Q/A Question
        q_match = re.match(r"^[•\-*]?\s*Q[:\s]+(.+)$", stripped)
        if q_match:
            segments.append(("question", q_match.group(1).strip()))
            i += 1
            continue

        # Q/A Answer (Supportive/Skeptical labels or legacy "Possible answer")
        a_match = re.match(r"^\s*(Supportive|Skeptical|Possible answer)[:\s]+(.+)$", stripped, re.IGNORECASE)
        if a_match:
            label = a_match.group(1).strip().capitalize()
            segments.append(("answer", (label, a_match.group(2).strip())))
            i += 1
            continue

        # Key-value pairs in Bill Snapshot section (without bullet prefix)
        if current_section == "bill snapshot":
            kv_match = re.match(r"^([^:]{1,40}):\s*(.+)$", stripped)
            if kv_match:
                segments.append(("kv_pair", (kv_match.group(1).strip(), kv_match.group(2).strip())))
                i += 1
                continue

        # Bullet point - but NOT inside floor statements (they use - for internal formatting)
        if (stripped.startswith("• ") or stripped.startswith("- ") or stripped.startswith("* ")) and not in_floor_statement:
            bullet_text = stripped[2:].strip()
            # Only treat as kv_pair in Bill Snapshot section
            if current_section == "bill snapshot":
                kv_match = re.match(r"^([^:]{1,40}):\s*(.+)$", bullet_text)
                if kv_match:
                    segments.append(("kv_pair", (kv_match.group(1).strip(), kv_match.group(2).strip())))
                    i += 1
                    continue
            segments.append(("bullet", bullet_text))
            i += 1
            continue

        # Vote record chamber header (**HOUSE (45-17-1)**)
        if stripped.startswith("**") and stripped.endswith("**") and ("HOUSE" in stripped or "SENATE" in stripped):
            header_text = stripped[2:-2]  # Remove ** markers
            segments.append(("vote_header", header_text))
            i += 1
            continue

        # Vote record row (VOTE_ROW:col1|col2|col3)
        if stripped.startswith("VOTE_ROW:"):
            vote_data = stripped[9:]  # Remove "VOTE_ROW:" prefix
            cols = vote_data.split("|")
            segments.append(("vote_row", cols))
            i += 1
            continue

        # Regular body text
        segments.append(("body", stripped))
        i += 1

    return segments


def render_briefer_pdf(
    *,
    title: str,
    body_text: str,
    subtitle: Optional[str] = None,
) -> bytes:
    """
    Render a professional PDF that mirrors the email briefer format.

    Args:
        title: PDF document title (used in metadata)
        body_text: Plain text output from format_full_briefer()
        subtitle: Optional subtitle (session label)

    Returns:
        PDF file bytes
    """
    buf = BytesIO()

    doc = SimpleDocTemplate(
        buf,
        pagesize=letter,
        leftMargin=0.75 * inch,
        rightMargin=0.75 * inch,
        topMargin=0.6 * inch,
        bottomMargin=0.6 * inch,
        title=title,
    )

    styles = _create_styles()
    story = []

    # Parse the briefer text
    segments = _parse_briefer_text(body_text)

    # Track state for grouping
    in_snapshot = False
    kv_pairs = []

    # Census block state
    in_census_block = False
    census_content = []

    # Track About section
    in_about_section = False

    def flush_census_block():
        """Render accumulated census block as a styled table."""
        nonlocal in_census_block, census_content
        if not census_content:
            in_census_block = False
            return

        # Build content paragraphs
        block_elements = []
        for ctype, ctext in census_content:
            if ctype == "header":
                block_elements.append(Paragraph(f"<b>{_escape_html(ctext)}</b>", styles['IBB_CensusHeader']))
            elif ctype == "body":
                block_elements.append(Paragraph(_escape_html(ctext), styles['IBB_CensusBody']))
            elif ctype == "source":
                block_elements.append(Paragraph(f"<i>{_escape_html(ctext)}</i>", styles['IBB_CensusSource']))

        # Wrap in a table for background styling
        if block_elements:
            # Create inner table to hold all paragraphs
            inner_data = [[elem] for elem in block_elements]
            inner_table = Table(inner_data, colWidths=[6.3 * inch])
            inner_table.setStyle(TableStyle([
                ('VALIGN', (0, 0), (-1, -1), 'TOP'),
                ('LEFTPADDING', (0, 0), (-1, -1), 0),
                ('RIGHTPADDING', (0, 0), (-1, -1), 0),
                ('TOPPADDING', (0, 0), (-1, -1), 0),
                ('BOTTOMPADDING', (0, 0), (-1, -1), 0),
            ]))

            # Outer table for background and border
            outer_table = Table([[inner_table]], colWidths=[6.5 * inch])
            outer_table.setStyle(TableStyle([
                ('BACKGROUND', (0, 0), (-1, -1), COLOR_CENSUS_BG),
                ('BOX', (0, 0), (-1, -1), 0.5, COLOR_CENSUS_BORDER),
                ('LEFTPADDING', (0, 0), (-1, -1), 10),
                ('RIGHTPADDING', (0, 0), (-1, -1), 10),
                ('TOPPADDING', (0, 0), (-1, -1), 8),
                ('BOTTOMPADDING', (0, 0), (-1, -1), 8),
            ]))
            story.append(Spacer(1, 6))
            story.append(outer_table)
            story.append(Spacer(1, 6))

        census_content = []
        in_census_block = False

    def flush_kv_table():
        """Flush accumulated key-value pairs as a table."""
        nonlocal kv_pairs
        if not kv_pairs:
            return

        table_data = []
        for k, v in kv_pairs:
            # Handle URLs
            if v.startswith("http://") or v.startswith("https://"):
                v_display = f'<link href="{_escape_html(v)}">{_escape_html(v)}</link>'
            else:
                v_display = _escape_html(v)

            table_data.append([
                Paragraph(f"<b>{_escape_html(k)}</b>", styles['IBB_KVLabel']),
                Paragraph(v_display, styles['IBB_KVValue']),
            ])

        if table_data:
            t = Table(table_data, colWidths=[1.8 * inch, 4.7 * inch])
            t.setStyle(TableStyle([
                ('VALIGN', (0, 0), (-1, -1), 'TOP'),
                ('LEFTPADDING', (0, 0), (-1, -1), 0),
                ('RIGHTPADDING', (0, 0), (-1, -1), 6),
                ('TOPPADDING', (0, 0), (-1, -1), 3),
                ('BOTTOMPADDING', (0, 0), (-1, -1), 3),
            ]))
            story.append(t)

        kv_pairs = []

    for seg_type, content in segments:
        if seg_type == "blank":
            if kv_pairs:
                flush_kv_table()
            story.append(Spacer(1, 6))
            continue

        if seg_type == "prepared_for":
            story.append(Paragraph(_escape_html(content), styles['IBB_PreparedFor']))
            continue

        if seg_type == "briefer_id":
            story.append(Paragraph(_escape_html(content), styles['IBB_BrieferId']))
            continue

        if seg_type == "update_note":
            story.append(Paragraph(_escape_html(content), styles['IBB_UpdateNote']))
            continue

        if seg_type == "timestamp":
            story.append(Paragraph(_escape_html(content), styles['IBB_Timestamp']))
            continue

        if seg_type == "main_header":
            flush_kv_table()
            story.append(Paragraph(_escape_html(content), styles['IBB_MainHeader']))
            continue

        if seg_type == "multi_agency_banner":
            # Render as a warning banner with background
            banner_style = ParagraphStyle(
                'MultiAgencyBanner',
                parent=styles['IBB_Body'],
                fontName='Helvetica-Bold',
                fontSize=10,
                textColor=HexColor("#744210"),  # Dark amber
                backColor=HexColor("#FEF3C7"),  # Light amber
                borderPadding=6,
                alignment=TA_CENTER,
            )
            story.append(Spacer(1, 4))
            story.append(Paragraph(_escape_html(content.replace("***", "").strip()), banner_style))
            story.append(Spacer(1, 4))
            continue

        if seg_type == "disclaimer":
            story.append(Paragraph(_escape_html(content), styles['IBB_Disclaimer']))
            continue

        if seg_type == "bill_title":
            flush_kv_table()
            # Parse bill number and title
            parts = re.split(r"\s*[—\-]\s*", content, maxsplit=1)
            bill_num = parts[0].strip()
            bill_desc = parts[1].strip() if len(parts) > 1 else ""

            # Further split description if it has "--"
            short_title = bill_desc
            long_desc = ""
            if "--" in bill_desc:
                title_parts = bill_desc.split("--", 1)
                short_title = title_parts[0].strip(" -")
                long_desc = title_parts[1].strip(" -")

            story.append(Paragraph(
                f"<b>{_escape_html(bill_num)}</b> — {_escape_html(short_title)}",
                styles['IBB_BillTitle']
            ))
            if long_desc:
                story.append(Paragraph(_escape_html(long_desc), styles['IBB_BillDescription']))
            continue

        if seg_type == "section_header":
            flush_kv_table()
            flush_census_block()  # Flush census block before new section
            in_snapshot = "bill snapshot" in content.lower()

            # Add separator line before section (except first)
            if story:
                story.append(Spacer(1, 8))
                story.append(HRFlowable(
                    width="100%",
                    thickness=1,
                    color=COLOR_BORDER,
                    spaceBefore=4,
                    spaceAfter=8,
                ))

            # Extract number and title
            m = re.match(r"^(\d+[A-Za-z]?)\.\s+(.+)$", content)
            if m:
                num, sec_title = m.group(1), m.group(2)
                # Simple inline format: "1. Title" with styled number
                story.append(Paragraph(
                    f'<font face="Helvetica-Bold" size="13" color="#334">{_escape_html(num)}.</font>  '
                    f'<font face="Helvetica-Bold" size="13">{_escape_html(sec_title)}</font>',
                    styles['IBB_SectionHeader']
                ))
            else:
                story.append(Paragraph(_escape_html(content), styles['IBB_SectionHeader']))
            continue

        if seg_type == "sub_header":
            flush_kv_table()
            story.append(Paragraph(f"<b>{_escape_html(content)}</b>", styles['IBB_SubHeader']))
            continue

        if seg_type == "kv_pair":
            k, v = content
            kv_pairs.append((k, v))
            continue

        if seg_type == "bullet":
            flush_kv_table()
            # Use table for precise bullet alignment
            bullet_table = Table(
                [[
                    Paragraph("•", styles['IBB_BulletChar']),
                    Paragraph(_escape_html(content), styles['IBB_Bullet'])
                ]],
                colWidths=[0.15 * inch, 6.6 * inch],
                hAlign='LEFT'
            )
            bullet_table.setStyle(TableStyle([
                ('VALIGN', (0, 0), (-1, -1), 'TOP'),
                ('LEFTPADDING', (0, 0), (-1, -1), 0),
                ('RIGHTPADDING', (0, 0), (0, 0), 2),
                ('RIGHTPADDING', (1, 0), (1, 0), 0),
                ('TOPPADDING', (0, 0), (-1, -1), 0),
                ('BOTTOMPADDING', (0, 0), (-1, -1), 3),
            ]))
            story.append(bullet_table)
            continue

        if seg_type == "question":
            flush_kv_table()
            story.append(Paragraph(f"<b>Q:</b> {_escape_html(content)}", styles['IBB_Question']))
            continue

        if seg_type == "answer":
            if isinstance(content, tuple):
                label, text = content
            else:
                label, text = "Possible answer", content
            story.append(Paragraph(f"<i>{_escape_html(label)}:</i> {_escape_html(text)}", styles['IBB_Answer']))
            continue

        # Census block handling
        if seg_type == "census_header":
            flush_kv_table()
            in_census_block = True
            census_content.append(("header", content))
            continue

        # census_source handling removed - source now in footer

        if seg_type == "about_header":
            flush_kv_table()
            flush_census_block()
            in_about_section = True  # Start tracking About section
            # Add separator line before About section
            story.append(Spacer(1, 8))
            story.append(HRFlowable(
                width="100%",
                thickness=1,
                color=COLOR_BORDER,
                spaceBefore=4,
                spaceAfter=8,
            ))
            story.append(Paragraph(_escape_html(content), styles['IBB_AboutHeader']))
            continue

        if seg_type == "about_sub_header":
            flush_kv_table()
            story.append(Paragraph(f"<b>{_escape_html(content)}</b>", styles['IBB_AboutSubHeader']))
            continue

        if seg_type == "about_footnote":
            flush_kv_table()
            footnote_style = ParagraphStyle(
                'IBB_Footnote',
                parent=styles.get('IBB_AboutBody', styles['Normal']),
                fontSize=7,
                textColor=HexColor("#999999"),
                fontName='Helvetica-Oblique',
                spaceBefore=1,
                spaceAfter=2,
            )
            story.append(Paragraph(f"\u2020{_escape_html(content)}", footnote_style))
            continue

        if seg_type == "about_footer":
            flush_kv_table()
            story.append(Spacer(1, 8))
            _org = get_tenant_config()["org_name"]
            rest = content.replace(_org, "").strip()
            rest_escaped = _escape_html(rest).replace("*", "&#8226;")  # Restore bullet point
            story.append(Paragraph(f"<b>{_escape_html(_org)}</b> {rest_escaped}", styles['IBB_AboutBody']))
            continue

        if seg_type == "about_tagline":
            tagline = content.strip("_")
            story.append(Paragraph(f"<i>{_escape_html(tagline)}</i>", styles['IBB_AboutBody']))
            continue

        if seg_type == "vote_header":
            flush_kv_table()
            # Render vote chamber header (e.g., "HOUSE (45-17-1)") as bold
            story.append(Paragraph(f"<b>{_escape_html(content)}</b>", styles['IBB_SubHeader']))
            continue

        if seg_type == "vote_row":
            flush_kv_table()
            # Render vote record as a 3-column table for proper alignment
            cols = content  # content is already a list of columns
            # Ensure we have 3 columns
            while len(cols) < 3:
                cols.append("")
            table_data = [[
                Paragraph(_escape_html(cols[0]), styles['IBB_Body']),
                Paragraph(_escape_html(cols[1]), styles['IBB_Body']),
                Paragraph(_escape_html(cols[2]), styles['IBB_Body']),
            ]]
            vote_table = Table(table_data, colWidths=[2.2 * inch, 2.2 * inch, 2.2 * inch])
            vote_table.setStyle(TableStyle([
                ('VALIGN', (0, 0), (-1, -1), 'TOP'),
                ('LEFTPADDING', (0, 0), (-1, -1), 0),
                ('RIGHTPADDING', (0, 0), (-1, -1), 20),
                ('TOPPADDING', (0, 0), (-1, -1), 2),
                ('BOTTOMPADDING', (0, 0), (-1, -1), 2),
            ]))
            story.append(vote_table)
            continue

        if seg_type == "body":
            flush_kv_table()
            # Check if we're inside a census block
            if in_census_block:
                census_content.append(("body", content))
            # Check for Section 9 District Profile note
            elif content.startswith("Note: This analysis"):
                story.append(Paragraph(_escape_html(content), styles['IBB_Note']))
            # Check for Section 6 Floor Statements note (matches Section 9 style)
            elif content.startswith("Note: These arguments") or content.startswith("Note    These arguments"):
                note_text = content.replace("Note    ", "Note: ")
                story.append(Paragraph(_escape_html(note_text), styles['IBB_Note']))
            # Check if we're in About section - use About body style for all content
            elif in_about_section:
                # Bold "Sections X" references if present
                if content.startswith("Sections ") or content.startswith("Section "):
                    import re as _sections_re
                    def bold_sections(m):
                        return f"<b>{m.group(0)}</b>"
                    bolded = _sections_re.sub(r'Sections? \d+(?:[- &,]+\d+)*', bold_sections, _escape_html(content))
                    story.append(Paragraph(bolded, styles['IBB_AboutBody']))
                else:
                    story.append(Paragraph(_escape_html(content), styles['IBB_AboutBody']))
            else:
                story.append(Paragraph(_escape_html(content), styles['IBB_Body']))
            continue

    # Flush any remaining KV pairs and census blocks
    flush_kv_table()
    flush_census_block()

    # Footer spacing (About This Briefer section is now in the plain text)
    story.append(Spacer(1, 8))

    # Build PDF
    doc.build(story, canvasmaker=NumberedCanvas)
    return buf.getvalue()
