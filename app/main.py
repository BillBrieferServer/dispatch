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
