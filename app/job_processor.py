"""Job processing, bill resolution, and worker logic for the Bill Briefer application."""

import fcntl
import io
import logging
import os
import re
import requests
import subprocess
import tempfile
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

from app.utils import (
    _read_json, _write_json, _norm_text, _estimate_cost, append_usage_log,
    _load_manual_users,
    BOISE_TZ, JOBS_DIR, DATA_DIR,
    DEFAULT_SESSION_YEAR, SESSION_ID_MAP,
    ALERT_EMAIL, NTFY_TOPIC, JOB_TIMEOUT_MINUTES,
)
from app.branding import ORG_NAME
from app.email_sender import send_email
from app.ai_brief import build_ai_brief
from app.services.qibrain_data import (
    refresh_bill_from_legislature,
    get_bill_data, get_bill_text,
    get_bill_fiscal_note, get_bill_sop, store_fiscal_note, store_sop,
    store_bill_text,
    get_bill_votes as qibrain_get_bill_votes,
)
from app.briefer_format import format_full_briefer
from app.pdf_render import render_briefer_pdf
from app.legislators import LEGISLATORS

# Census data removed — not used in dispatch stack

logger = logging.getLogger(__name__)



def normalize_bill_number(user_input: str) -> str:
    s = _norm_text(user_input).upper()
    s = re.sub(r"\s+", "", s).replace("-", "")
    if s.startswith("HB"):
        s = "H" + s[2:]
    elif s.startswith("SB"):
        s = "S" + s[2:]
    m = re.match(r"^([A-Z]+)(\d+)$", s)
    if not m:
        return s
    prefix, digits = m.group(1), m.group(2)
    # Only pad H and S prefixes to 4 digits (standard House/Senate bills)
    # Other types (HCR, SCR, HJM, SJM, HJR, SJR) use 3-digit padding
    if prefix in ("H", "S") and len(digits) <= 4:
        digits = digits.zfill(4)
    return f"{prefix}{digits}"


def bill_candidates(user_input: str) -> List[str]:
    """Generate candidate bill number formats to match against masterlist."""
    n = normalize_bill_number(user_input)
    c = {n}

    # For H and S bills, add HB/SB alternatives
    m = re.match(r"^(H|S)(\d{4})$", n)
    if m:
        chamber, digits = m.group(1), m.group(2)
        if chamber == "H":
            c.add("HB" + digits)
        elif chamber == "S":
            c.add("SB" + digits)

    # For other bill types (HCR, SCR, HJM, SJM, HJR, SJR, etc.)
    # Add multiple padding variants since formats vary
    m2 = re.match(r"^([A-Z]{2,})(\d+)$", n)
    if m2:
        prefix, digits = m2.group(1), m2.group(2)
        stripped = digits.lstrip('0') or '0'
        # Add unpadded version
        c.add(f"{prefix}{stripped}")
        # Add 2-digit padded version
        if len(stripped) <= 2:
            c.add(f"{prefix}{stripped.zfill(2)}")
        # Add 3-digit padded version (common for resolutions)
        if len(stripped) <= 3:
            c.add(f"{prefix}{stripped.zfill(3)}")
        # Add 4-digit padded version
        if len(stripped) <= 4:
            c.add(f"{prefix}{stripped.zfill(4)}")

    return sorted(c)


def classify_bill_topic(ai_json: Optional[Dict[str, Any]], bill_obj: Dict[str, Any]) -> str:
    """
    Classify bill topic for demographic context decisions.

    CONSERVATIVE APPROACH: Only 4 topics get demographic context where
    Census data has a DIRECT line to who the bill affects:
    - veterans: veteran population data
    - k12_education: student enrollment data
    - healthcare_access: insurance coverage, age demographics
    - social_services: poverty, age, disability data

    All other topics return classifications that do NOT get context.
    """
    text_parts = []
    if ai_json:
        text_parts.append(ai_json.get("one_paragraph_summary", ""))
        text_parts.extend(ai_json.get("key_points", []))
    text_parts.append(bill_obj.get("title", ""))
    text_parts.append(bill_obj.get("description", ""))

    search_text = " ".join(str(p) for p in text_parts).lower()

    # === TOPICS THAT GET DEMOGRAPHIC CONTEXT (whitelist) ===

    # Veterans - clear demographic tie
    if any(kw in search_text for kw in [
        "veteran", "military service", "armed forces", "service member",
        "national guard", "military family", "gi bill"
    ]):
        return "veterans"

    # K-12 Education - must be K-12 specific, not higher ed or admin
    k12_keywords = ["k-12", "k12", "elementary", "middle school", "high school",
                    "public school student", "school district", "classroom",
                    "kindergarten", "grade school"]
    if any(kw in search_text for kw in k12_keywords):
        return "k12_education"

    # Healthcare ACCESS - must be about access/coverage, not admin/licensing
    healthcare_access_kw = ["medicaid", "medicare", "health insurance", "uninsured",
                           "healthcare access", "medical coverage", "health coverage",
                           "prescription drug cost", "hospital access"]
    if any(kw in search_text for kw in healthcare_access_kw):
        return "healthcare_access"

    # Social services - direct poverty/assistance programs
    social_kw = ["food stamp", "snap benefit", "tanf", "welfare", "public assistance",
                 "housing assistance", "rental assistance", "homeless", "poverty program",
                 "low-income assistance", "disability benefit", "child care assistance"]
    if any(kw in search_text for kw in social_kw):
        return "social_services"

    # Family law - child support, custody, marriage, adoption have direct demographic ties
    family_law_kw = ["child support", "child custody", "custody", "paternity",
                     "adoption", "foster", "divorce", "marriage", "alimony",
                     "parental rights", "guardianship", "family court"]
    if any(kw in search_text for kw in family_law_kw):
        return "family_law"

    # === TOPICS THAT DO NOT GET DEMOGRAPHIC CONTEXT ===
    # (Classified for logging/debugging but no census data shown)

    # Higher education / education admin - no direct K-12 student tie
    if any(kw in search_text for kw in ["university", "college", "higher education",
                                         "education board", "school administration"]):
        return "higher_education"

    # Healthcare admin/licensing - no direct patient population tie
    if any(kw in search_text for kw in ["health", "medical", "hospital", "pharmacy",
                                         "drug", "mental health"]):
        return "healthcare_admin"

    # Campaign/elections - political process, not demographics
    if any(kw in search_text for kw in ["campaign", "election", "vote", "ballot",
                                         "candidate", "political", "pac", "lobbying"]):
        return "political"

    # Criminal justice - procedure focused
    if any(kw in search_text for kw in ["criminal", "felony", "misdemeanor", "sentencing",
                                         "prison", "parole", "prosecution"]):
        return "criminal_justice"

    # Transportation - infrastructure focused
    if any(kw in search_text for kw in ["highway", "road", "bridge", "transportation",
                                         "vehicle", "traffic", "motor vehicle"]):
        return "transportation"

    # Business/economic - too broad for direct demographic tie
    if any(kw in search_text for kw in ["tax", "business", "economic", "budget",
                                         "appropriation", "fiscal", "license", "permit"]):
        return "economic"

    # Property/real estate - unless housing assistance (caught above)
    if any(kw in search_text for kw in ["property", "real estate", "land use", "zoning"]):
        return "property"

    # Administrative procedures
    if any(kw in search_text for kw in ["procedure", "filing", "administrative",
                                         "agency", "rule", "regulation"]):
        return "administrative"

    return "other"


def _job_path(job_id: str) -> Path:
    return JOBS_DIR / f"{job_id}.json"



def _check_rate_limit(email: str, hourly_limit: int = 50, daily_limit: int = 50):
    """Check if user has exceeded request limits. Returns error message or None."""
    now = time.time()
    one_hour_ago = now - 3600
    one_day_ago = now - 86400
    hourly_count = 0
    daily_count = 0

    for fname in JOBS_DIR.glob("*.json"):
        try:
            job = _read_json(fname)
            if not job or job.get("email", "").lower() != email.lower():
                continue
            created = job.get("created_at", 0)
            if created > one_hour_ago:
                hourly_count += 1
            if created > one_day_ago:
                daily_count += 1
        except Exception:
            continue

    if hourly_count >= hourly_limit:
        return f"Rate limit reached: {hourly_limit} briefers per hour. Please try again later."
    if daily_count >= daily_limit:
        return f"Daily limit reached: {daily_limit} briefers per day. Please try again tomorrow."
    return None


def _has_pending_job(email: str, bill: str, session_year: str):
    """Check if user already has a pending/running job for this bill. Returns job_id or None."""
    bill_norm = normalize_bill_number(bill).upper()
    for fname in JOBS_DIR.glob("*.json"):
        try:
            job = _read_json(fname)
            if not job:
                continue
            if (job.get("email", "").lower() == email.lower() and
                normalize_bill_number(job.get("bill_input", "")).upper() == bill_norm and
                job.get("session_year", "") == session_year and
                job.get("status", "") in ("queued", "processing")):
                return job.get("job_id")
        except Exception:
            continue
    return None


def enqueue_job(email: str, bill_input: str, recipient_line: str, session_year: str = "") -> str:
    job_id = uuid.uuid4().hex[:12]
    # Normalize session year and get session ID
    session_year = _norm_text(session_year) or DEFAULT_SESSION_YEAR
    session_id = SESSION_ID_MAP.get(session_year, SESSION_ID_MAP[DEFAULT_SESSION_YEAR])
    logger.info(f"ENQUEUE_JOB: normalized session_year={session_year!r}, session_id={session_id}")
    payload = {
        "job_id": job_id,
        "email": _norm_text(email).lower(),
        "bill_input": _norm_text(bill_input),
        "recipient_line": _norm_text(recipient_line),
        "session_year": session_year,
        "session_id": session_id,
        "created_at": time.time(),
        "status": "queued",
        "error": None,
        "bill_resolved": None,
    }
    logger.info(f"ENQUEUE_JOB: payload={payload}")
    _write_json(_job_path(job_id), payload)
    return job_id


def _list_jobs() -> List[Path]:
    return sorted(JOBS_DIR.glob("*.json"), key=lambda p: p.stat().st_mtime, reverse=True)


def _acquire_lock() -> Optional[int]:
    lock_path = JOBS_DIR / ".lock"
    try:
        return os.open(str(lock_path), os.O_CREAT | os.O_EXCL | os.O_RDWR)
    except FileExistsError:
        return None


def _release_lock(fd: Optional[int]) -> None:
    lock_path = JOBS_DIR / ".lock"
    try:
        if fd is not None:
            os.close(fd)
        if lock_path.exists():
            lock_path.unlink()
    except Exception as e:
        print('SCHEDULER_STARTUP_ERROR:', repr(e))


def cleanup_old_jobs() -> None:
    try:
        retention_days = int(os.getenv("JOB_RETENTION_DAYS", "14"))
    except Exception:
        retention_days = 14
    cutoff = time.time() - (retention_days * 86400)
    for job_file in JOBS_DIR.glob("*.json"):
        try:
            if job_file.stat().st_mtime > cutoff:
                continue
            d = _read_json(job_file) or {}
            status = (d.get("status") or "").lower()
            if status not in ("done", "failed"):
                continue
            job_file.unlink()
        except Exception:
            continue


def cleanup_stuck_jobs() -> int:
    """
    Mark any 'processing' jobs as failed on startup.
    These are jobs that were interrupted by a container restart.
    Returns the number of jobs cleaned up.
    """
    cleaned = 0
    for job_file in JOBS_DIR.glob("*.json"):
        try:
            d = _read_json(job_file)
            if not d:
                continue
            status = (d.get("status") or "").lower()
            if status == "processing":
                d["status"] = "failed"
                d["error"] = "Job interrupted by server restart"
                d["finished_at"] = time.time()
                _write_json(job_file, d)
                append_usage_log(d)
                cleaned += 1
        except Exception:
            continue
    return cleaned


def _pdf_bytes_to_text(raw_pdf: bytes) -> str:
    if not raw_pdf:
        return ""
    try:
        from pypdf import PdfReader  # type: ignore
    except Exception:
        return ""
    try:
        import io
        reader = PdfReader(io.BytesIO(raw_pdf))
    except Exception:
        return ""
    out: List[str] = []
    for page in getattr(reader, "pages", []) or []:
        try:
            txt = page.extract_text() or ""
        except Exception:
            txt = ""
        if txt.strip():
            out.append(txt)
    return "\n\n".join(out).strip()


def _ocr_pdf_bytes_to_text(raw_pdf: bytes, *, max_pages: int = 25) -> tuple[str, int]:
    if not raw_pdf:
        return "", 0
    if shutil.which("pdftoppm") is None or shutil.which("tesseract") is None:
        return "", 0
    try:
        with tempfile.TemporaryDirectory() as td:
            pdf_path = Path(td) / "doc.pdf"
            pdf_path.write_bytes(raw_pdf)
            prefix = str(Path(td) / "page")
            subprocess.run(
                ["pdftoppm", "-png", "-r", "200", str(pdf_path), prefix],
                check=True,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
                timeout=120,
            )
            images = sorted(Path(td).glob("page-*.png")) or sorted(Path(td).glob("*.png"))
            if not images:
                return "", 0
            images = images[:max_pages]
            chunks = []
            pages = 0
            for img in images:
                pages += 1
                try:
                    r = subprocess.run(
                        ["tesseract", str(img), "stdout", "-l", "eng"],
                        check=True,
                        capture_output=True,
                        text=True,
                        timeout=60,
                    )
                    txt = (r.stdout or "").strip()
                    if txt:
                        chunks.append(txt)
                except Exception:
                    continue
            return ("\n\n".join(chunks)).strip(), pages
    except Exception:
        return "", 0


def _reflow_pdf_text(text: str) -> str:
    """
    Reflow PDF-extracted text into proper paragraphs.
    PDF extraction often preserves original line breaks, resulting in
    choppy text with only a few words per line.
    """
    if not text:
        return ""

    # First, fix hyphenated words split across lines
    text = re.sub(r'(\w)-\n(\w)', r'\1\2', text)

    # Split into paragraphs (blank lines or multiple newlines)
    paragraphs = re.split(r'\n\s*\n', text)

    reflowed = []
    for para in paragraphs:
        para = para.strip()
        if not para:
            continue

        # Check if this looks like a header (all caps, short, or specific patterns)
        lines = para.split('\n')
        is_header = (
            len(lines) == 1 and (
                para.isupper() or
                para.startswith('FISCAL') or
                para.startswith('STATEMENT OF PURPOSE') or
                re.match(r'^[A-Z][A-Z\s]+:?$', para)
            )
        )

        if is_header:
            reflowed.append(para)
            continue

        # Check if this is a structured section (like a table or list)
        # These often have consistent short lines or bullet points
        if all(line.strip().startswith(('-', '•', '*', '○')) for line in lines if line.strip()):
            reflowed.append(para)  # Keep list formatting
            continue

        # Reflow: join lines within the paragraph
        # But preserve lines that look like they should be separate (headers, labels)
        joined_lines = []
        current = ""

        for line in lines:
            line = line.strip()
            if not line:
                continue

            # Check if line is a label/header (ends with colon, all caps, etc.)
            is_label = (
                line.endswith(':') or
                line.isupper() or
                re.match(r'^[A-Z][A-Za-z\s]+:$', line)
            )

            if is_label:
                if current:
                    joined_lines.append(current.strip())
                    current = ""
                joined_lines.append(line)
            else:
                # Join with previous content
                if current:
                    # Add space if current doesn't end with hyphen
                    if current.endswith('-'):
                        current = current[:-1] + line
                    else:
                        current = current + " " + line
                else:
                    current = line

        if current:
            joined_lines.append(current.strip())

        reflowed.append("\n".join(joined_lines))

    return "\n\n".join(reflowed)


def _fetch_fiscal_note_with_fallback(qibrain_bill_id, bill_obj, display_bill):
    """Fetch fiscal note from QIBrain. On-demand refresh already runs before this."""
    fiscal_note = {"found": False, "text": "", "error": ""}
    qibrain_fn = get_bill_fiscal_note(qibrain_bill_id)
    if qibrain_fn:
        fiscal_note = {"found": True, "text": qibrain_fn, "source": "qibrain"}
        logger.info(f"Fiscal note from QIBrain for {display_bill}, {len(qibrain_fn)} chars")
    else:
        logger.info(f"No fiscal note available for {display_bill}")
    return fiscal_note

def check_stuck_jobs() -> None:
    """Watchdog: Find jobs stuck in processing state and mark them failed."""
    try:
        cutoff = time.time() - (JOB_TIMEOUT_MINUTES * 60)
        stuck_jobs = []
        
        for pth in _list_jobs():
            d = _read_json(pth)
            if not d:
                continue
            if d.get("status") == "processing":
                started = d.get("started_at", 0)
                if started and started < cutoff:
                    stuck_jobs.append((pth, d))
        
        for pth, job_data in stuck_jobs:
            job_id = job_data.get("job_id", "unknown")
            bill = job_data.get("bill_input", "unknown")
            email = job_data.get("email", "unknown")
            started = job_data.get("started_at", 0)
            elapsed = int((time.time() - started) / 60) if started else 0
            
            # Mark as failed
            job_data["status"] = "failed"
            job_data["error"] = f"Job timed out after {elapsed} minutes"
            job_data["finished_at"] = time.time()
            _write_json(pth, job_data)
            
            logger.warning(f"WATCHDOG: Marked stuck job {job_id} as failed (bill={bill}, elapsed={elapsed}min)")
            

            # Notify user of failure
            try:
                if email and email != "unknown":
                    fail_session = job_data.get("session_year", "")
                    fail_subject = f"Dispatch: Unable to generate {bill}"
                    fail_body = (
                        f"We were unable to generate your briefer for {bill}"
                        f" ({fail_session} Session).\n\n"
                        f"This can happen when bill text is temporarily unavailable "
                        f"or our analysis service is experiencing high demand.\n\n"
                        f"Please try your request again. If the problem persists, "
                        f"contact info@billbriefer.com and reference job ID: "
                        f"{job_id}.\n\n"
                        f"Dispatch — Leadership Briefer\n"
                        f"A service of Quiet Impact"
                    )
                    send_email(email, fail_subject, fail_body)
                    logger.info(f"WATCHDOG: User failure notification sent to {email}")
            except Exception as e:
                logger.error(f"WATCHDOG: Failed to send user notification: {e}")

            # Send alert if configured
            if ALERT_EMAIL and FROM_EMAIL and SMTP_HOST:
                try:
                    alert_subject = f"[ALERT] Dispatch Job Stuck: {bill}"
                    alert_body = f"""A bill briefer job was stuck and has been automatically terminated.

Job ID: {job_id}
Bill: {bill}
Requester: {email}
Started: {time.strftime('%Y-%m-%d %H:%M:%S UTC', time.gmtime(started))}
Elapsed: {elapsed} minutes
Timeout: {JOB_TIMEOUT_MINUTES} minutes

The job has been marked as failed. Please check the logs for more details.

This is an automated alert from the Dispatch watchdog.
"""
                    send_email(ALERT_EMAIL, alert_subject, alert_body)
                    logger.info(f"WATCHDOG: Alert sent to {ALERT_EMAIL}")
                except Exception as e:
                    logger.error(f"WATCHDOG: Failed to send alert email: {e}")
            
            # Send ntfy push notification if configured
            if NTFY_TOPIC:
                try:
                    import requests
                    ntfy_url = f"https://ntfy.sh/{NTFY_TOPIC}"
                    ntfy_title = f"Job Stuck: {bill}"
                    ntfy_msg = f"Bill {bill} stuck for {elapsed}min. Job marked failed."
                    requests.post(
                        ntfy_url,
                        data=ntfy_msg.encode('utf-8'),
                        headers={
                            "Title": ntfy_title,
                            "Priority": "high",
                            "Tags": "warning,briefer"
                        },
                        timeout=10
                    )
                    logger.info(f"WATCHDOG: ntfy alert sent to {NTFY_TOPIC}")
                except Exception as e:
                    logger.error(f"WATCHDOG: Failed to send ntfy alert: {e}")
                    
    except Exception as e:
        logger.error(f"WATCHDOG: Error checking stuck jobs: {e}")


def process_one_job() -> None:
    try:
        _process_one_job_inner()
    except Exception as e:
        logger.error(f'WORKER SAFETY NET: Unhandled exception in process_one_job: {e}', exc_info=True)



def _cleanup_demo_files() -> None:
    """Delete demo job JSON + PDF files older than 24 hours."""
    cutoff = time.time() - 86400
    for jp in JOBS_DIR.glob("*.json"):
        try:
            jd = _read_json(jp)
            if not jd or not jd.get("is_demo"):
                continue
            created = jd.get("created_at", 0)
            if created and created < cutoff:
                # Delete the PDF if it exists
                pdf_path = jp.with_suffix(".pdf")
                if pdf_path.exists():
                    pdf_path.unlink()
                    logger.info(f"Cleaned up demo PDF: {pdf_path.name}")
                # Delete the job JSON
                jp.unlink()
                logger.info(f"Cleaned up demo job: {jp.name}")
        except Exception:
            pass



def _process_one_job_inner() -> None:
    fd = _acquire_lock()
    if fd is None:
        return

    job_file = None
    job_data = None

    try:
        for pth in _list_jobs():
            d = _read_json(pth)
            if not d:
                continue
            if d.get("status") == "queued":
                job_file = pth
                job_data = d
                break
        if not job_file or not job_data:
            # Cleanup: remove demo PDFs and demo job files older than 24 hours
            _cleanup_demo_files()
            return

        job_data["status"] = "processing"
        job_data["started_at"] = time.time()
        _write_json(job_file, job_data)

        email_clean = _norm_text(job_data.get("email")).lower()
        recipient_line = _norm_text(job_data.get("recipient_line"))
        bill_input = _norm_text(job_data.get("bill_input"))

        candidates = bill_candidates(bill_input)
        display_bill = candidates[0] if candidates else normalize_bill_number(bill_input)
        subject = f"{ORG_NAME} — {display_bill}"

        # Get session year from job data or default
        job_session_year = job_data.get("session_year")
        session_year = job_session_year or DEFAULT_SESSION_YEAR

        # QIBrain: resolve bill directly by number (replaces masterlist + find_bill_id)
        bill_payload, bill_obj, qibrain_bill_id = get_bill_data(
            display_bill, session_year=int(session_year)
        )
        bill_id = bill_obj.get("bill_id") if bill_obj else None
        session_id = bill_obj.get("session_id") if bill_obj else None

        # Override session_year with the bill's actual session year from QIBrain.
        # bill_obj['session'] is dynamically resolved, not hardcoded.
        if bill_obj and bill_obj.get("session", {}).get("year_start"):
            session_year = str(bill_obj["session"]["year_start"])

        if not bill_id:
            body = (
                f"{recipient_line}\n\n"
                f"{display_bill}\n\n"
                f"Status: Bill not found\n\n"
                f"We could not find '{bill_input}' in the {session_year} Idaho legislative session.\n\n"
                f"Please check the bill number and try again. Common formats:\n"
                f"• House bills: H0001, HB1, H 1\n"
                f"• Senate bills: S1001, SB1001, S 1001\n"
            )
            send_email(email_clean, subject, body)

            job_data["status"] = "done"
            job_data["bill_resolved"] = None
            job_data["finished_at"] = time.time()
            _write_json(job_file, job_data)
            append_usage_log(job_data)
            return

        # QIBrain: bill_payload and bill_obj already populated above
        # Get bill text from QIBrain
        job_data["bill_text_ocr_used"] = False
        job_data["bill_text_ocr_pages"] = 0

        # On-demand freshness check: hit legislature.idaho.gov for latest data
        # Runs in ~500ms, catches status changes and new fiscal notes between cron syncs
        try:
            refresh_bill_from_legislature(display_bill, qibrain_bill_id)
        except Exception as e:
            logger.warning(f"On-demand refresh failed for {display_bill}: {e}")

        bill_text = get_bill_text(qibrain_bill_id) or ""

        if not bill_text:
            # Bill text not in QIBrain — fetch PDF directly from legislature.idaho.gov
            logger.info(f"Bill text not in QIBrain for {display_bill}, fetching from legislature.idaho.gov")
            try:
                import io
                bill_pdf_url = f"https://legislature.idaho.gov/wp-content/uploads/sessioninfo/2026/legislation/{display_bill.replace(' ', '')}.pdf"
                pdf_resp = requests.get(bill_pdf_url, timeout=10, headers={
                    'User-Agent': 'QuietImpact-BillBriefer/1.0 (info@billbriefer.com)'
                })
                if pdf_resp.status_code == 200 and len(pdf_resp.content) > 100:
                    try:
                        import pdfplumber
                        pdf = pdfplumber.open(io.BytesIO(pdf_resp.content))
                        pages = [p.extract_text() for p in pdf.pages if p.extract_text()]
                        pdf.close()
                        bill_text = '\n\n'.join(pages).strip()
                    except ImportError:
                        bill_text = _pdf_bytes_to_text(pdf_resp.content).strip()

                    if bill_text:
                        try:
                            store_bill_text(qibrain_bill_id, bill_text)
                            logger.info(f"Stored bill text from legislature.idaho.gov for {display_bill} ({len(bill_text)} chars)")
                        except Exception as e:
                            logger.warning(f"Failed to store bill text in QIBrain: {e}")
            except Exception as e:
                logger.warning(f"Failed to fetch bill text from legislature.idaho.gov: {e}")
                bill_text = ""

        job_data["bill_text_chars"] = len(bill_text or "")
        _write_json(job_file, job_data)

        # Pre-classify bill topic to get census context BEFORE AI call
        # (So we can pass demographic context to Claude for better analysis)
        session_label = f"ID Legislature — {session_year} Session"

        # Build search text from bill metadata (before AI analysis)
        search_text_parts = [
            bill_obj.get("title", ""),
            bill_obj.get("description", ""),
        ]
        search_text = " ".join(str(p) for p in search_text_parts)

        # Fetch fiscal note from QIBrain
        fiscal_note = _fetch_fiscal_note_with_fallback(qibrain_bill_id, bill_obj, display_bill)

        # AI - now with census context and fiscal note passed to the model
        job_data["bill_text_sent_to_ai_chars"] = len(bill_text or "")
        job_data["fiscal_note_chars"] = len(fiscal_note.get("text", "")) if fiscal_note.get("found") else 0
        _write_json(job_file, job_data)

        # Extract fiscal note text for AI context
        fiscal_note_text = fiscal_note.get("text", "") if fiscal_note.get("found") else ""

        # Extract bill metadata for caching
        # Use qibrain_bill_id for data lookups
        session_id_for_cache = bill_obj.get("session", {}).get("session_id") if bill_obj else None
        change_hash_for_cache = bill_obj.get("change_hash", "") if bill_obj else None

        ai_json, ai_err, ai_model, ai_was_invalidated, ai_token_usage = build_ai_brief(
            bill_number=display_bill,
            bill_data=bill_payload,
            bill_text=bill_text,
            fiscal_note_text=fiscal_note_text,
            bill_id=qibrain_bill_id,
            session_id=session_id_for_cache,
            bill_change_hash=change_hash_for_cache,
        )

        # Build update note if cache was invalidated (bill changed since last briefer)
        update_note = "Updated: bill changed since previous briefer" if ai_was_invalidated else ""

        # Log which AI model was used
        job_data["ai_model_used"] = ai_model
        job_data["ai_cache_hit"] = (ai_model == "cached")
        if ai_token_usage:
            job_data["ai_token_usage"] = ai_token_usage
            cost = _estimate_cost(ai_token_usage)
            job_data["ai_estimated_cost_usd"] = cost
            logger.info(f"[{display_bill}] AI cost estimate: ${cost:.4f} ({ai_token_usage})")
        _write_json(job_file, job_data)
        # Step 3 test gate: save raw AI JSON for inspection
        if ai_json:
            job_data["ai_json_raw"] = ai_json
            _write_json(job_file, job_data)

        if not ai_json:
            ai_json = {
                "bill_summary": f"AI unavailable ({ai_model}): {ai_err or 'unknown error'}",
                "sponsor_profile": {},
                "momentum": {},
                "unintended_consequences": ["AI generation failed — review manually."],
                "power_flag": {"flag_level": "none", "direction": "none", "explanation": "Module generation failed."},
                "advocacy_positions": {"positions": [], "coalition_alert": None, "count": 0},
            }

        # Demo override: use demo_name and demo_district if present
        _is_demo = job_data.get("is_demo", False)
        _demo_name = job_data.get("demo_name", "")
        _demo_district = job_data.get("demo_district")

        # Get requester name from legislators data (use display_name format)
        leg_info = LEGISLATORS.get(email_clean.lower())
        if leg_info:
            display_name = leg_info.get("display_name", "")
            ld_code_val = leg_info.get("ld_code", "")
            requester_name = f"{display_name}, {ld_code_val}" if display_name and ld_code_val and ld_code_val != "LD00" else display_name
        else:
            # Non-legislator (staff/demo) — look up name from auth database
            try:
                from app.auth.auth_db import get_db_connection
                with get_db_connection() as _conn:
                    _row = _conn.execute("SELECT name FROM users WHERE LOWER(email) = ?", (email_clean,)).fetchone()
                    requester_name = _row[0] if _row else ""
            except Exception:
                requester_name = ""
            # Append district from manual_users if assigned
            _mu = _load_manual_users().get(email_clean.lower(), {})
            _mu_dist = _mu.get("district") if isinstance(_mu, dict) else None
            if requester_name and _mu_dist:
                requester_name = f"{requester_name}, LD{int(_mu_dist):02d}"

        # Demo override: use demo_name/demo_district
        if _is_demo and _demo_name:
            requester_name = f"{_demo_name}, LD{int(_demo_district):02d}" if _demo_district else _demo_name

        # District number resolution (used for requester name display)
        district_num = None
        ld_code = ""
        if _is_demo and _demo_district:
            district_num = int(_demo_district)
        elif leg_info and leg_info.get("district"):
            district_num = int(leg_info["district"])
        else:
            # Check manual_users for assigned sample district
            _manual = _load_manual_users().get(email_clean.lower(), {})
            if isinstance(_manual, dict) and _manual.get("district"):
                district_num = int(_manual["district"])

        if district_num:
            ld_code = f"LD{district_num:02d}"


        # Fetch fiscal note from QIBrain
        fiscal_note = _fetch_fiscal_note_with_fallback(qibrain_bill_id, bill_obj, display_bill)

        # Fetch individual legislator votes from QIBrain
        individual_votes = []
        try:
            qibrain_votes = qibrain_get_bill_votes(qibrain_bill_id)
            for v in qibrain_votes:
                rc = {
                    "roll_call_id": v.get("vote_id", 0),
                    "date": v.get("vote_date", ""),
                    "description": "",
                    "chamber": v.get("chamber", ""),
                    "yea": v.get("yeas", 0),
                    "nay": v.get("nays", 0),
                    "nv": v.get("absent", 0),
                    "absent": v.get("absent", 0),
                    "passed": 1 if v.get("result", "").lower() in ("passed", "pass") else 0,
                    "votes": [
                        {
                            "name": iv.get("legislator_name", ""),
                            "party": iv.get("party", ""),
                            "vote_text": iv.get("vote_cast", ""),
                        }
                        for iv in v.get("individual_votes", [])
                    ],
                }
                individual_votes.append(rc)
            if individual_votes:
                total_voters = sum(len(rc.get("votes", [])) for rc in individual_votes)
                logger.info(f"Fetched {len(individual_votes)} roll calls with {total_voters} individual votes from QIBrain for {display_bill}")
        except Exception as e:
            logger.warning(f"Failed to fetch individual votes from QIBrain for {display_bill}: {e}")

        # Determine if this is an appropriation bill (from AI-extracted budget data)
        budget_data = ai_json.get("budget_extracted") if ai_json else None
        is_appropriation = budget_data and budget_data.get("is_appropriation", False)
        
        # Fallback detection: check bill subjects, title, and sponsor
        if not is_appropriation:
            subjects = bill_obj.get("subjects", [])
            subject_names = [s.get("subject_name", "").upper() if isinstance(s, dict) else str(s).upper() for s in subjects]
            if "APPROPRIATIONS" in subject_names:
                is_appropriation = True
                logger.info(f"APPROPRIATION detected via subject for {display_bill}")
            elif "appropriat" in (bill_obj.get("title") or "").lower():
                is_appropriation = True
                logger.info(f"APPROPRIATION detected via title for {display_bill}")
            else:
                # Check if sponsored by Finance Committee (JFAC bills are almost always appropriations)
                sponsors = bill_obj.get("sponsors", [])
                for sponsor in sponsors:
                    sponsor_name = sponsor.get("name", "").lower() if isinstance(sponsor, dict) else str(sponsor).lower()
                    if "finance committee" in sponsor_name or "finance comm" in sponsor_name:
                        is_appropriation = True
                        logger.info(f"APPROPRIATION detected via Finance Committee sponsor for {display_bill}")
                        break
        
        briefer_type = "appropriation" if is_appropriation else "policy"

        if is_appropriation:
            logger.info(f"APPROPRIATION BRIEFER for {display_bill}")
        else:
            logger.info(f"POLICY BRIEFER for {display_bill}")

        body = format_full_briefer(
            recipient_line=recipient_line or "Legislative Briefing",
            bill_number=display_bill,
            bill_obj=bill_obj,
            ai_json=ai_json,
    
            session_label=session_label,
            requester_name=requester_name,
            requester_email=email_clean,
            district_num=district_num or 0,
            individual_votes=individual_votes,
            update_note=update_note,
        )

        if is_appropriation:
            pdf_title = f"{display_bill} — Appropriation Briefing"
        else:
            pdf_title = f"{display_bill} — Legislative Briefing"

        # Generate PDF attachment
        pdf_bytes = render_briefer_pdf(title=pdf_title, body_text=body, subtitle=session_label)
        pdf_filename = f"{display_bill.replace(' ', '_')}_briefing.pdf"

        if _is_demo:
            # Demo job: save PDF to disk for download, don't email
            demo_pdf_path = JOBS_DIR / f"{job_data['job_id']}.pdf"
            demo_pdf_path.write_bytes(pdf_bytes)
            job_data["demo_pdf_path"] = str(demo_pdf_path)
            logger.info(f"[{display_bill}] Demo PDF saved: {demo_pdf_path}")
        else:
            send_email(email_clean, subject, body, pdf_attachment=pdf_bytes, pdf_filename=pdf_filename)

        
        job_data["status"] = "done"
        job_data["bill_resolved"] = display_bill
        job_data["bill_id"] = bill_id
        job_data["briefer_type"] = briefer_type
        job_data["finished_at"] = time.time()
        _write_json(job_file, job_data)
        append_usage_log(job_data)

    except Exception as e:
        try:
            if job_file:
                d = _read_json(job_file) or {}
                d["status"] = "failed"
                d["error"] = str(e)
                d["finished_at"] = time.time()
                _write_json(job_file, d)
                append_usage_log(d)

                # Notify user of failure
                try:
                    fail_email = d.get("email", "")
                    fail_bill = d.get("bill_input", d.get("bill", "unknown"))
                    fail_session = d.get("session_year", "")
                    if fail_email:
                        fail_subject = f"Dispatch: Unable to generate {fail_bill}"
                        fail_body = (
                            f"We were unable to generate your briefer for {fail_bill}"
                            f" ({fail_session} Session).\n\n"
                            f"This can happen when bill text is temporarily unavailable "
                            f"or our analysis service is experiencing high demand.\n\n"
                            f"Please try your request again. If the problem persists, "
                            f"contact info@billbriefer.com and reference job ID: "
                            f"{d.get('job_id', 'unknown')}.\n\n"
                            f"Dispatch — Leadership Briefer\n"
                            f"A service of Quiet Impact"
                        )
                        send_email(fail_email, fail_subject, fail_body)
                except Exception:
                    pass  # Don't let failure email errors prevent job cleanup
        except Exception:
            pass
    finally:
        _release_lock(fd)

