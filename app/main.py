import os
import logging
logger = logging.getLogger(__name__)
import json
import time
import secrets
from pathlib import Path
from typing import Any, Dict, Optional, List, Tuple
from datetime import datetime, timezone
from zoneinfo import ZoneInfo

from apscheduler.schedulers.background import BackgroundScheduler
from dotenv import load_dotenv
from fastapi import FastAPI, Form, Request
from fastapi.staticfiles import StaticFiles
from fastapi.responses import HTMLResponse, PlainTextResponse, RedirectResponse, FileResponse, Response
from fastapi.templating import Jinja2Templates

from app.services.qibrain_data import get_bill as qibrain_get_bill

from app.auth import auth_router, set_templates as set_auth_templates, init_auth_db, run_cleanup_jobs as auth_cleanup_jobs, get_session_by_token_hash, hash_token, update_session_last_used

# AI output cache
try:
    from app.ai_cache import init_ai_cache_db
    AI_CACHE_ENABLED = True
except ImportError:
    AI_CACHE_ENABLED = False
    init_ai_cache_db = None

from app.legislators import LEGISLATORS
from app.ratings import init_ratings_db, get_ratings, set_rating, clear_rating
from app.bill_status import classify_status, is_procedural_stage, normalize_committee_name

load_dotenv()

from app.tenant_config import get_tenant_config
from app.utils import (
    _estimate_cost,
    _read_json,
    _write_json,
    _norm_text,
    _iso_utc,
    _iso_boise,
    append_usage_log,
    _load_allowlist,
    _load_manual_users,
    _save_manual_users,
    _load_admin_allowlist,
    _html_to_text,
    BOISE_TZ,
    DATA_DIR,
    JOBS_DIR,
    AUTH_DIR,
    REPORTS_DIR,
    USAGE_LOG_PATH,
    USAGE_LOG_FIELDS,
    TOKEN_COSTS,
    SESSION_ID_MAP,
    DEFAULT_SESSION_YEAR,
    AVAILABLE_SESSIONS,
    FROM_EMAIL,
    ALERT_EMAIL,
    NTFY_TOPIC,
    JOB_TIMEOUT_MINUTES,
    SMTP_HOST,
    SMTP_PORT,
    SMTP_USER,
    SMTP_PASS,
    JOB_WORKER_INTERVAL_SECONDS,
    LEGISCAN_API_KEY,
    LEGISCAN_BASE_URL,
    LEGISCAN_STATE,
    LEGISCAN_SESSION_YEAR,
    LEGISCAN_SESSION_ID_OVERRIDE,
    APP_BASE_URL,
)

ALLOWLIST = _load_allowlist()

from app.job_processor import (
    normalize_bill_number,
    _job_path, _check_rate_limit, _has_pending_job, enqueue_job, _list_jobs,
    cleanup_old_jobs, cleanup_stuck_jobs, check_stuck_jobs,
    process_one_job,
)
app = FastAPI(docs_url=None, redoc_url=None, openapi_url=None)
# CSRF session cookie middleware (sets cookie for unauthenticated users)
from starlette.middleware.base import BaseHTTPMiddleware

class CSRFSessionMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request, call_next):
        response = await call_next(request)
        # Set csrf_session cookie if one was generated during this request
        new_csrf = getattr(request.state, "new_csrf_session", None)
        if new_csrf:
            response.set_cookie("csrf_session", new_csrf, max_age=3600, httponly=True, samesite="lax")
        elif not request.cookies.get("bb_session") and not request.cookies.get("csrf_session"):
            csrf_session = secrets.token_hex(32)
            response.set_cookie("csrf_session", csrf_session, max_age=3600, httponly=True, samesite="lax")
        return response

app.add_middleware(CSRFSessionMiddleware)

def _require_csrf(request, csrf_token: str) -> bool:
    """Validate CSRF token. Returns True if valid, False if invalid."""
    if not csrf_token:
        return False
    return _validate_csrf_token(request, csrf_token)

app.mount("/static", StaticFiles(directory="static"), name="static")

TEMPLATES_DIR = Path("/app/templates")

templates = Jinja2Templates(directory=str(TEMPLATES_DIR))

# Make csrf_token available in all templates via context processor
_original_template_response = templates.TemplateResponse

def _csrf_template_response(name, context, **kwargs):
    request = context.get("request")
    if request and "csrf_token" not in context:
        context["csrf_token"] = _get_csrf_token(request)
    # Inject tenant branding into all templates
    tc = get_tenant_config()
    context.setdefault("org_name", tc["org_name"])
    context.setdefault("org_full_name", tc["org_full_name"])
    return _original_template_response(name, context, **kwargs)

templates.TemplateResponse = _csrf_template_response
# Auth router integration
set_auth_templates(templates)
from app.admin_routes import router as admin_router, set_admin_templates, log_buffer
set_admin_templates(templates)
app.include_router(auth_router)
app.include_router(admin_router)

# --- CSRF Protection ---
_csrf_tokens: dict = {}  # session_hash -> token

def _get_csrf_token(request) -> str:
    """Generate or retrieve CSRF token for current session."""
    session_cookie = request.cookies.get("bb_session", "")
    if not session_cookie:
        session_cookie = request.cookies.get("csrf_session", "")
    if not session_cookie:
        # First visit - generate a csrf_session value and store in request.state
        # The middleware will set this as a cookie on the response
        session_cookie = secrets.token_hex(32)
        request.state.new_csrf_session = session_cookie
    import hashlib
    key = hashlib.sha256(session_cookie.encode()).hexdigest()[:32]
    if key not in _csrf_tokens:
        _csrf_tokens[key] = secrets.token_hex(32)
    return _csrf_tokens[key]

def _validate_csrf_token(request, token: str) -> bool:
    """Validate CSRF token matches session."""
    session_cookie = request.cookies.get("bb_session", "")
    if not session_cookie:
        session_cookie = request.cookies.get("csrf_session", "")
    if not session_cookie:
        # Check if a new csrf_session was generated on this request
        session_cookie = getattr(request.state, "new_csrf_session", "")
    if not session_cookie:
        return False
    import hashlib
    key = hashlib.sha256(session_cookie.encode()).hexdigest()[:32]
    expected = _csrf_tokens.get(key, "")
    if not expected:
        return False
    return secrets.compare_digest(expected, token)

def _cleanup_csrf_tokens():
    """Limit CSRF token store size."""
    if len(_csrf_tokens) > 10000:
        # Keep most recent half
        keys = list(_csrf_tokens.keys())
        for k in keys[:len(keys)//2]:
            del _csrf_tokens[k]

from starlette.exceptions import HTTPException as StarletteHTTPException

@app.exception_handler(StarletteHTTPException)
async def custom_http_exception_handler(request: Request, exc: StarletteHTTPException):
    if exc.status_code == 404:
        return templates.TemplateResponse("404.html", {"request": request}, status_code=404)
    from starlette.responses import JSONResponse
    return JSONResponse({"detail": exc.detail}, status_code=exc.status_code)

def current_user(request: Request) -> Optional[Dict[str, Any]]:
    """Get current authenticated user from password+MFA auth (bb_session cookie)."""
    # First, check new auth system (bb_session cookie)
    new_auth_token = request.cookies.get("bb_session")
    if new_auth_token:
        token_hash = hash_token(new_auth_token)
        session = get_session_by_token_hash(token_hash)
        if session:
            # Check expiration
            from datetime import datetime
            expires_at = session.get("expires_at")
            if expires_at:
                if isinstance(expires_at, str):
                    expires_at = datetime.fromisoformat(expires_at)
                if expires_at > datetime.utcnow():
                    # Valid new auth session
                    update_session_last_used(token_hash)
                    email = session.get("email", "").lower()
                    user = {
                        "email": email,
                        "user_id": session.get("user_id"),
                        "name": session.get("name"),
                        "district": session.get("district"),
                        "chamber": session.get("chamber"),
                        "auth_type": "password"
                    }
                    # Check allowlist — user must be currently authorized
                    current_allow = _load_allowlist()
                    manual_users = _load_manual_users()
                    manual_emails = {k.strip().lower() for k in manual_users.keys()}
                    if email not in current_allow and email not in manual_emails:
                        # Not on allowlist — treat as not logged in
                        return None

                    # Enrich with legislator data
                    leg_data = LEGISLATORS.get(email)
                    if leg_data:
                        user["display_name"] = leg_data.get("display_name", user.get("name", ""))
                        user["ld_code"] = leg_data.get("ld_code", user.get("district", ""))
                    return user

    return None

def require_login(request: Request) -> Optional[RedirectResponse]:
    if current_user(request):
        return None
    return RedirectResponse(url="/auth/login", status_code=302)

def require_admin(request: Request) -> Optional[RedirectResponse]:
    redir = require_login(request)
    if redir:
        return redir
    user = current_user(request) or {}
    email_clean = _norm_text(user.get("email")).lower()
    admins = _load_admin_allowlist()
    if email_clean not in admins:
        return RedirectResponse(url="/not-authorized", status_code=302)
    return None

scheduler = BackgroundScheduler(daemon=True)

# FORCE_SCHEDULER_START_V1: start the scheduler even if FastAPI startup event fails
try:
    # Initialize AI output cache
    if AI_CACHE_ENABLED and init_ai_cache_db:
        init_ai_cache_db()
        print("AI_CACHE_DB_INITIALIZED")
    init_auth_db()
    init_ratings_db()
    # Clean up any jobs stuck in 'processing' from previous container restart
    stuck_count = cleanup_stuck_jobs()
    # Run data retention cleanup on startup
    try:
        _cleanup_result = auth_cleanup_jobs()
        _cleaned_total = sum(_cleanup_result.values())
        if _cleaned_total > 0:
            print(f"DATA_CLEANUP: {_cleanup_result}")
        cleanup_old_jobs()
    except Exception as _e:
        print(f"DATA_CLEANUP_ERROR: {_e}")
    if stuck_count > 0:
        print(f"STARTUP_CLEANUP: Marked {stuck_count} stuck job(s) as failed")
    # Clean up stale lock file from previous container
    lock_path = JOBS_DIR / ".lock"
    if lock_path.exists():
        try:
            lock_path.unlink()
            print("STARTUP_CLEANUP: Removed stale lock file")
        except Exception:
            pass
    scheduler.add_job(
        process_one_job,
        "interval",
        seconds=JOB_WORKER_INTERVAL_SECONDS,
        id="job_worker",
        replace_existing=True,
        max_instances=1,
    )
    scheduler.add_job(cleanup_old_jobs, "interval", hours=6, id="job_cleanup", replace_existing=True, max_instances=1)
    scheduler.add_job(check_stuck_jobs, "interval", minutes=1, id="stuck_job_watchdog", replace_existing=True, max_instances=1)
    scheduler.add_job(auth_cleanup_jobs, "interval", hours=6, id="auth_cleanup", replace_existing=True, max_instances=1)
    if not scheduler.running:
        scheduler.start()
        print("SCHEDULER_STARTED_V1")
except Exception as e:
    print("SCHEDULER_FORCE_START_ERROR_V1:", repr(e))

@app.on_event("startup")
def _start_scheduler():
    """Ensure scheduler is running on FastAPI startup (backup for module-level start)."""
    try:
        init_auth_db()
        if not scheduler.running:
            scheduler.add_job(
                process_one_job,
                "interval",
                seconds=JOB_WORKER_INTERVAL_SECONDS,
                id="job_worker",
                replace_existing=True,
                max_instances=1,
            )
            scheduler.add_job(cleanup_old_jobs, "interval", hours=6, id="job_cleanup", replace_existing=True, max_instances=1)
            scheduler.add_job(check_stuck_jobs, "interval", minutes=1, id="stuck_job_watchdog", replace_existing=True, max_instances=1)
            scheduler.add_job(auth_cleanup_jobs, "interval", hours=6, id="auth_cleanup", replace_existing=True, max_instances=1)
            scheduler.start()
    except Exception:
        pass

@app.get("/health")
def health_check():
    """Health check endpoint for Docker and monitoring."""
    # Count job states for worker health visibility
    queued = 0
    processing = 0
    stuck = 0
    failed_1h = 0
    now = time.time()
    cutoff = now - (JOB_TIMEOUT_MINUTES * 60)
    one_hour_ago = now - 3600
    for pth in _list_jobs():
        d = _read_json(pth)
        if not d:
            continue
        s = d.get("status")
        if s == "queued":
            queued += 1
        elif s == "processing":
            processing += 1
            if d.get("started_at", 0) < cutoff:
                stuck += 1
        elif s == "failed" and d.get("finished_at", 0) > one_hour_ago:
            failed_1h += 1
    return {
        "status": "degraded" if stuck > 0 or failed_1h > 0 else "ok",
        "service": "idaho-bill-briefer",
        "timestamp": time.time(),
        "jobs_queued": queued,
        "jobs_processing": processing,
        "jobs_stuck": stuck,
        "jobs_failed_1h": failed_1h,
    }

@app.get("/about", response_class=HTMLResponse)
def about_page(request: Request):
    return templates.TemplateResponse("about.html", {"request": request})

@app.get("/privacy", response_class=HTMLResponse)
def privacy_page(request: Request):
    return templates.TemplateResponse("privacy.html", {"request": request})

@app.get("/robots.txt", response_class=PlainTextResponse)
def robots_txt():
    return "User-agent: *\nDisallow: /\n"

@app.get("/", response_class=HTMLResponse)
def home(request: Request):
    redir = require_login(request)
    if redir:
        return redir
    user = current_user(request)
    return templates.TemplateResponse("home.html", {"request": request, "user": user, "sessions": AVAILABLE_SESSIONS, "default_session": DEFAULT_SESSION_YEAR})
@app.get("/dashboard", response_class=HTMLResponse)
def dashboard(request: Request):
    redir = require_login(request)
    if redir:
        return redir

    from app.services.qibrain_data import get_qibrain_connection
    from datetime import date

    tenant = os.getenv('TENANT_ID', '')
    session_year = DEFAULT_SESSION_YEAR
    today = date.today()

    existing_ratings = get_ratings(tenant, session_year)

    # Load demo sponsor assignments
    import json as _json
    _demo_path = os.path.join(os.path.dirname(__file__), '..', 'data', 'demo_sponsors.json')
    try:
        with open(_demo_path) as _f:
            demo_sponsors = _json.load(_f)
    except FileNotFoundError:
        demo_sponsors = {}

    conn = get_qibrain_connection()
    cur = conn.cursor()
    cur.execute("""
        SELECT b.bill_id, b.bill_number, b.title, b.subjects,
               b.introduced_date, b.last_action_date, b.last_action,
               b.committee,
               ba.attributed_name AS sponsor_name,
               COALESCE(b.introduced_date,
                   (SELECT MIN(be2.event_date) FROM idaho.bill_events be2
                    WHERE be2.bill_id = b.bill_id)) AS effective_intro_date,
               ba.attribution_source,
               ba.committee_name AS attribution_committee,
               lg.email AS sponsor_email,
               -- First committee referral from bill events
               (SELECT be.event_text FROM idaho.bill_events be
                WHERE be.bill_id = b.bill_id
                  AND be.event_text ILIKE '%%Referred to%%'
                  AND be.event_text NOT ILIKE '%%JR%%'
                  AND be.event_text NOT ILIKE '%%printing%%'
                ORDER BY be.sequence_order LIMIT 1) AS committee_event
        FROM idaho.bills b
        JOIN idaho.sessions s ON b.legiscan_session_id = s.legiscan_session_id
        LEFT JOIN idaho.bill_attribution ba ON ba.bill_id = b.bill_id
        LEFT JOIN idaho.legislators lg
            ON lg.legislator_id = ba.attributed_legislator_id
        WHERE s.year = %s
        ORDER BY b.last_action_date DESC NULLS LAST, b.bill_number
    """, (int(session_year),))
    rows = cur.fetchall()
    conn.close()

    bills = []
    for r in rows:
        # Topic: first element of subjects JSONB array, fallback to title prefix
        subj = r['subjects']
        if isinstance(subj, list) and subj:
            topic = str(subj[0]).upper()
        elif isinstance(subj, str):
            import json as _json
            try:
                parsed = _json.loads(subj)
                topic = str(parsed[0]).upper() if parsed else ''
            except Exception:
                topic = subj.upper() if subj else ''
        else:
            topic = ''
        if not topic:
            import re as _re2
            _tm = _re2.match(r'^([A-Z][A-Z\s,/&\-\(\)]+?)(?:\s+(?:Adds?|Amends?|Relates?|Provides?|Repeals?|Establishes?|Revises?|Appropriat|States|A JOINT|AN ACT)[\s,]|\s+[-–]\s)', r['title'] or '')
            if _tm:
                topic = _tm.group(1).strip().rstrip(' -')

        # Status classification
        status_label, status_color = classify_status(r.get('committee'), r.get('last_action'))

        # Days calculation
        intro = r.get('effective_intro_date') or r.get('introduced_date')
        days = (today - intro).days if intro else None

        # Last action date formatted MM/DD
        lad = r['last_action_date']
        last_action_fmt = lad.strftime('%m/%d') if lad else ''

        # Sponsor: demo override, then bill_attribution
        bn = r['bill_number'] or ''
        demo = demo_sponsors.get(bn)
        if demo:
            sponsor_name = demo['name']
            sponsor_email = demo['email'].lower()
        else:
            sponsor_name = (r.get('sponsor_name') or '').replace('Representative ', '').replace('Senator ', '')
            sponsor_email = (r.get('sponsor_email') or '').lower()
        rating = existing_ratings.get(sponsor_email) if sponsor_email else None

        # Committee: show actual committee name, not procedural stages or short codes
        committee = ''
        raw_committee = r.get('committee') or ''
        if raw_committee and not is_procedural_stage(raw_committee):
            # Current location IS a committee — normalize it
            committee = normalize_committee_name(raw_committee)
        else:
            # Bill has moved past committee — get committee from referral event
            _evt = r.get('committee_event') or ''
            if _evt:
                import re as _re
                m = _re.search(r'[Rr]eferred to\s+(.+?)(?:\s*Committee)?$', _evt)
                if m:
                    committee = normalize_committee_name(m.group(1).strip())
            # Third fallback: introducing committee from bill_attribution
            if not committee and r.get('attribution_committee'):
                committee = normalize_committee_name(r['attribution_committee'])
        if not committee:
            committee = 'Filed / No Committee'

        # Chamber from bill number prefix
        bn = r['bill_number'] or ''
        chamber = 'House' if bn.upper().startswith('H') else 'Senate' if bn.upper().startswith('S') else ''

        bills.append({
            'bill_number': bn,
            'topic': topic,
            'status_label': status_label,
            'status_color': status_color,
            'sponsor_name': sponsor_name,
            'committee': committee,
            'rating': rating,
            'days': days,
            'last_action_fmt': last_action_fmt,
            'chamber': chamber,
            'introduced_date': str(intro) if intro else '',
            'last_action_date': str(lad) if lad else '',
        })

    return templates.TemplateResponse("dashboard.html", {
        "request": request,
        "bills": bills,
        "session_year": session_year,
    })


# ─── Group Watch ─────────────────────────────────────────
@app.get("/group-watch", response_class=HTMLResponse)
async def group_watch(request: Request):
    """Advocacy positions & legislator scorecards cross-reference."""
    user = current_user(request)
    if not user:
        return RedirectResponse("/auth/login", status_code=302)

    from app.services.qibrain_data import get_qibrain_connection

    conn = get_qibrain_connection()
    try:
        with conn.cursor() as cur:
            # --- Table 1: Bill Positions ---
            # Get all bills with at least one advocacy position
            cur.execute("""
                SELECT DISTINCT ap.bill_id
                FROM dispatch.advocacy_positions ap
                JOIN idaho.bills b ON b.bill_id = ap.bill_id
                WHERE b.legiscan_session_id = 2246
            """)
            position_bill_ids = [r[0] for r in cur.fetchall()]

            bill_positions = []
            if position_bill_ids:
                placeholders = ','.join(['%s'] * len(position_bill_ids))
                cur.execute(f"""
                    SELECT b.bill_id, b.bill_number, b.title, b.committee,
                           ba.attributed_name,
                           COALESCE(b.introduced_date,
                               (SELECT MIN(be2.event_date) FROM idaho.bill_events be2
                                WHERE be2.bill_id = b.bill_id)) AS effective_intro_date
                    FROM idaho.bills b
                    LEFT JOIN idaho.bill_attribution ba ON ba.bill_id = b.bill_id
                    WHERE b.bill_id IN ({placeholders})
                    ORDER BY b.bill_number
                """, position_bill_ids)
                bills_data = cur.fetchall()

                # Get all positions in one query
                cur.execute(f"""
                    SELECT bill_id, org_name, position
                    FROM dispatch.advocacy_positions
                    WHERE bill_id IN ({placeholders})
                """, position_bill_ids)
                pos_rows = cur.fetchall()

                # Build position lookup: bill_id -> {org: position}
                pos_map = {}
                for r in pos_rows:
                    if r[0] not in pos_map:
                        pos_map[r[0]] = {}
                    pos_map[r[0]][r[1]] = r[2]

                from app.bill_status import classify_status, normalize_committee_name, is_procedural_stage

                for row in bills_data:
                    bill_id = row[0]
                    bill_number = row[1]
                    committee_raw = row[3] or ''
                    status_label, status_color = classify_status(committee_raw)
                    sponsor = row[4] or ''
                    # Strip title prefix
                    for pfx in ('Representative ', 'Senator '):
                        if sponsor.startswith(pfx):
                            sponsor = sponsor[len(pfx):]
                    chamber = 'House' if bill_number.startswith('H') else 'Senate'
                    positions = pos_map.get(bill_id, {})

                    # Check IFF/IACI divergence
                    iff_pos = positions.get('IFF', '')
                    iaci_pos = positions.get('IACI', '')
                    divergence = False
                    if iff_pos and iaci_pos:
                        sup_opp = {'support', 'oppose'}
                        if {iff_pos, iaci_pos} == sup_opp:
                            divergence = True

                    bill_positions.append({
                        'bill_number': bill_number,
                        'topic': (row[2] or '')[:80],
                        'chamber': chamber,
                        'status_label': status_label,
                        'status_color': status_color,
                        'sponsor': sponsor,
                        'positions': positions,
                        'divergence': divergence,
                    })

            # Determine which orgs have position data
            position_orgs = sorted(set(
                org for bp in bill_positions for org in bp['positions'].keys()
            ))
            # Preferred order
            org_order = ['IFF', 'IACI', 'CVI', 'ICL', 'CAI', 'ACLU Idaho', 'IWF']
            position_orgs = [o for o in org_order if o in position_orgs] + \
                            [o for o in position_orgs if o not in org_order]

            # --- Table 2: Legislator Scorecards ---
            cur.execute("""
                SELECT l.legislator_id, l.first_name, l.last_name, l.party,
                       l.district_id, l.chamber
                FROM idaho.legislators l
                WHERE l.is_active = true
                ORDER BY l.chamber, l.district_id
            """)
            legislators = cur.fetchall()

            # Get all scores
            cur.execute("""
                SELECT ls.legislator_id, ls.org_name, ls.vote_index, ls.year
                FROM dispatch.legislator_scores ls
                JOIN idaho.legislators l ON l.legislator_id = ls.legislator_id
                WHERE l.is_active = true
                ORDER BY ls.org_name, ls.year DESC
            """)
            score_rows = cur.fetchall()

            # Build score lookup: leg_id -> {org: {pct, year}}
            score_map = {}
            for r in score_rows:
                leg_id = r[0]
                org = r[1]
                if leg_id not in score_map:
                    score_map[leg_id] = {}
                if org not in score_map[leg_id]:  # first = most recent year
                    score_map[leg_id][org] = {
                        'pct': round(float(r[2]), 1) if r[2] is not None else None,
                        'year': r[3],
                    }

            score_orgs = sorted(set(
                org for sm in score_map.values() for org in sm.keys()
            ))
            score_org_order = ['IFF', 'IACI', 'CPAC', 'IFBF', 'ACLU Idaho']
            score_orgs = [o for o in score_org_order if o in score_orgs] + \
                         [o for o in score_orgs if o not in score_org_order]

            legislator_scores = []
            for leg in legislators:
                leg_id = leg[0]
                scores = score_map.get(leg_id, {})
                legislator_scores.append({
                    'name': f"{leg[1]} {leg[2]}",
                    'party': leg[3] or '',
                    'district': leg[4] or '',
                    'chamber': leg[5] or '',
                    'scores': scores,
                })

            # Build score legend
            org_full = {
                "IACI": "Idaho Association of Commerce & Industry",
                "IFF": "Idaho Freedom Foundation",
                "IFBF": "Idaho Farm Bureau Federation",
                "ACLU Idaho": "ACLU of Idaho",
                "CPAC": "CPAC Center for Legislative Accountability",
                "CVI": "Conservation Voters of Idaho",
                "ICL": "Idaho Conservation League",
                "CAI": "Citizens Alliance of Idaho",
                "IWF": "Idaho Wildlife Federation",
            }
            score_legend_parts = []
            for org in score_orgs:
                # Find most common year for this org
                years = [score_map[lid][org]['year']
                         for lid in score_map if org in score_map[lid]
                         and score_map[lid][org]['year']]
                year = max(set(years), key=years.count) if years else ''
                full = org_full.get(org, org)
                score_legend_parts.append(f"{org} = {full} ({year})")
            score_legend = ' | '.join(score_legend_parts)

    finally:
        conn.close()

    return templates.TemplateResponse("group_watch.html", {
        "request": request,
        "user": user,
        "bill_positions": bill_positions,
        "position_orgs": position_orgs,
        "legislator_scores": legislator_scores,
        "score_orgs": score_orgs,
        "score_legend": score_legend,
    })


@app.get("/ratings", response_class=HTMLResponse)
def ratings_page(request: Request):
    redir = require_login(request)
    if redir:
        return redir
    tenant = os.getenv('TENANT_ID', '')
    session_year = DEFAULT_SESSION_YEAR
    existing_ratings = get_ratings(tenant, session_year)
    # Build legislator list with ratings attached
    legs = []
    for email, leg in LEGISLATORS.items():
        legs.append({
            'email': email,
            'first_name': leg.get('first_name', ''),
            'last_name': leg.get('last_name', ''),
            'chamber': leg.get('chamber', ''),
            'district': leg.get('district', ''),
            'seat': leg.get('seat', ''),
            'party': leg.get('party', ''),
            'rating': existing_ratings.get(email),
        })
    legs.sort(key=lambda x: (x['chamber'], x['last_name'], x['first_name']))
    return templates.TemplateResponse("ratings.html", {
        "request": request,
        "legislators": legs,
        "session_year": session_year,
    })

@app.post("/ratings/save")
async def ratings_save(request: Request):
    redir = require_login(request)
    if redir:
        from fastapi.responses import JSONResponse
        return JSONResponse({"error": "unauthorized"}, status_code=401)
    body = await request.json()
    tenant = os.getenv('TENANT_ID', '')
    email = body.get('email', '').strip().lower()
    session_year = body.get('session_year', DEFAULT_SESSION_YEAR)
    rating = body.get('rating')
    if not email:
        from fastapi.responses import JSONResponse
        return JSONResponse({"error": "missing email"}, status_code=400)
    if rating is None:
        clear_rating(tenant, session_year, email)
    else:
        rating = int(rating)
        if rating < 1 or rating > 5:
            from fastapi.responses import JSONResponse
            return JSONResponse({"error": "rating must be 1-5"}, status_code=400)
        set_rating(tenant, session_year, email, rating)
    from fastapi.responses import JSONResponse
    return JSONResponse({"ok": True})

@app.post("/request", response_class=HTMLResponse)
def request_bill(request: Request, bill: str = Form(...), session: str = Form(DEFAULT_SESSION_YEAR), csrf_token: str = Form("")):
    redir = require_login(request)
    if redir:
        return redir
    user = current_user(request) or {}

    # CSRF validation
    if not _require_csrf(request, csrf_token):
        return templates.TemplateResponse("home.html", {
            "request": request,
            "user": user,
            "error": "Invalid request. Please refresh the page and try again.",
            "sessions": AVAILABLE_SESSIONS,
            "default_session": DEFAULT_SESSION_YEAR
        }, status_code=403)

    email_clean = _norm_text(user.get("email")).lower()

    # Rate limit check
    rate_error = _check_rate_limit(email_clean)
    if rate_error:
        return templates.TemplateResponse("home.html", {
            "request": request,
            "user": user,
            "error": rate_error,
            "sessions": AVAILABLE_SESSIONS,
            "default_session": DEFAULT_SESSION_YEAR
        })

    display_bill = normalize_bill_number(bill)

    # Duplicate detection
    pending = _has_pending_job(email_clean, bill, session)
    if pending:
        return templates.TemplateResponse("submitted.html", {
            "request": request,
            "email": email_clean,
            "bill": display_bill,
            "status": "queued",
            "job_id": pending,
            "session_year": session,
            "note": "This bill is already being processed. You will receive an email when it is ready."
        })

    # Look up bill title for confirmation display
    bill_title = None
    try:
        bill_info = qibrain_get_bill(display_bill, session_year=int(session))
        if bill_info:
            bill_title = bill_info.get("title")
    except Exception:
        pass  # Title is optional — don't fail the request

    logger.info(f"REQUEST_BILL: Creating job with session_year={session!r}")
    job_id = enqueue_job(email_clean, bill, "Legislative Briefing", session_year=session)
    logger.info(f"REQUEST_BILL: Created job {job_id}")
    return templates.TemplateResponse("submitted.html", {"request": request, "bill": display_bill, "status": "queued", "email": email_clean, "job_id": job_id, "session_year": session, "bill_title": bill_title})
