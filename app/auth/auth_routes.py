"""
auth_routes.py
FastAPI routes for Idaho Bill Briefer authentication.
Handles registration, login, MFA verification, password reset, and logout.
"""

import logging
import secrets
from datetime import datetime, timedelta
from typing import Optional

from fastapi import APIRouter, Request, Form, Response, HTTPException
from fastapi.responses import HTMLResponse, RedirectResponse
from starlette.templating import Jinja2Templates

from . import (
    # Database
    init_auth_db,
    create_user,
    get_user_by_email,
    get_user_by_id,
    update_user_last_login,
    update_user_password,
    update_user_status,
    check_user_locked,
    # MFA
    create_mfa_code as db_create_mfa_code,
    get_latest_mfa_code,
    mark_mfa_code_used,
    increment_mfa_attempts,
    invalidate_mfa_codes,
    count_recent_mfa_requests,
    # Sessions
    create_session_token as db_create_session_token,
    get_session_by_token_hash,
    update_session_last_used,
    delete_session_token,
    delete_all_user_sessions,
    get_user_sessions,
    # Trusted devices
    create_trusted_device as db_create_trusted_device,
    get_user_trusted_devices,
    delete_all_user_trusted_devices,
    get_trusted_device,
    update_trusted_device_last_used,
    delete_trusted_device,
    delete_all_user_trusted_devices,
    # Login attempts
    log_login_attempt,
    count_failed_attempts,
    count_failed_attempts_by_ip,
    # Security events
    log_security_event,
    # Security functions
    hash_password,
    verify_password,
    validate_password,
    generate_mfa_code,
    verify_mfa_code,
    validate_mfa_code_format,
    generate_session_token,
    hash_token,
    generate_device_fingerprint,
    validate_email,
    check_rate_limit,
    RATE_LIMITS,
    calculate_lockout_duration,
    mask_email,
    # Email
    send_signup_verification_code,
    send_welcome_email,
    send_login_mfa_code,
    send_password_reset_code,
    send_password_changed_notification,
    send_account_locked_notification,
)


# --- CSRF validation (lazy import from main app) ---
def _get_csrf_functions():
    """Lazy import to avoid circular imports."""
    try:
        from app.main import _get_csrf_token, _validate_csrf_token, _require_csrf
        return _get_csrf_token, _validate_csrf_token, _require_csrf
    except ImportError:
        return None, None, None


# --- Registration gate: QIBrain legislator lookup ---
from pathlib import Path
import json as _json

def is_authorized_email(email: str) -> bool:
    """Check if email is authorized to register.

    Checks (in order):
    1. QIBrain legislators table (active members only)
    2. Manual allowlist file (data/allowlist_emails.txt)
    3. Manual users JSON (data/manual_users.json)
    """
    email_lower = email.strip().lower()

    # Check 1: QIBrain legislators table (active members only)
    try:
        from app.services.qibrain_data import get_legislator
        legislator = get_legislator(email=email_lower)
        if legislator:
            return True
    except Exception:
        pass  # QIBrain unavailable — fall through to local checks

    # Check 2: Manual allowlist file
    allowlist_path = Path("/app/data/allowlist_emails.txt")
    if allowlist_path.exists():
        allowlist = {
            line.strip().lower()
            for line in allowlist_path.read_text().splitlines()
            if line.strip() and not line.startswith("#")
        }
        if email_lower in allowlist:
            return True

    # Check 3: Manual users JSON
    manual_path = Path("/app/data/manual_users.json")
    if manual_path.exists():
        try:
            manual = _json.loads(manual_path.read_text())
            # manual_users.json is {email: name} dict
            manual_emails = {k.strip().lower() for k in manual.keys()}
            if email_lower in manual_emails:
                return True
        except Exception:
            pass

    return False

logger = logging.getLogger(__name__)

# Create router
router = APIRouter(prefix="/auth", tags=["authentication"])

# Session cookie name
SESSION_COOKIE_NAME = "bb_session"

# Trusted device cookie (for "Remember this device" feature)
TRUSTED_DEVICE_COOKIE_NAME = "bb_trusted_device"
TRUSTED_DEVICE_DAYS = 30  # Remember device for 90 days

# Templates directory (will be set from main.py)
templates: Optional[Jinja2Templates] = None


def set_templates(t: Jinja2Templates):
    """Set templates from main app."""
    global templates
    templates = t


def get_client_ip(request: Request) -> str:
    """Get client IP from request, handling proxies."""
    forwarded = request.headers.get("X-Forwarded-For")
    if forwarded:
        return forwarded.split(",")[0].strip()
    return request.client.host if request.client else "unknown"


def get_user_agent(request: Request) -> str:
    """Get user agent from request."""
    return request.headers.get("User-Agent", "")[:500]


# ============================================================================
# SESSION MANAGEMENT HELPERS
# ============================================================================

def get_current_user(request: Request) -> Optional[dict]:
    """
    Get current authenticated user from session cookie.
    Returns user dict or None if not authenticated.
    """
    token = request.cookies.get(SESSION_COOKIE_NAME)
    if not token:
        return None

    token_hash = hash_token(token)
    session = get_session_by_token_hash(token_hash)

    if not session:
        return None

    # Check expiration
    expires_at = session.get('expires_at')
    if expires_at:
        if isinstance(expires_at, str):
            expires_at = datetime.fromisoformat(expires_at)
        if expires_at < datetime.utcnow():
            delete_session_token(token_hash)
            return None

    # Check account status
    if session.get('account_status') != 'active':
        return None

    # Update last used
    update_session_last_used(token_hash)

    return session


def create_session_response(
    response: Response,
    user_id: int,
    remember_days: int = 7,
    request: Optional[Request] = None
) -> str:
    """
    Create session and set cookie on response.
    Returns plain token.
    """
    # Generate session token
    plain_token, hashed_token, expires_at = generate_session_token(remember_days)

    # Get device info
    ip_address = get_client_ip(request) if request else None
    user_agent = get_user_agent(request) if request else None
    device_fingerprint = generate_device_fingerprint(user_agent or "", "", "", "")

    # Store in database
    db_create_session_token(
        user_id=user_id,
        token_hash=hashed_token,
        expires_at=expires_at,
        device_fingerprint=device_fingerprint,
        ip_address=ip_address,
        user_agent=user_agent
    )

    # Set cookie
    response.set_cookie(
        key=SESSION_COOKIE_NAME,
        value=plain_token,
        max_age=remember_days * 24 * 60 * 60,
        httponly=True,
        secure=True,  # HTTPS only
        samesite="lax"
    )

    return plain_token


def clear_session_cookie(response: Response):
    """Clear session cookie."""
    response.delete_cookie(SESSION_COOKIE_NAME)


# ============================================================================
# TRUSTED DEVICE HELPERS
# ============================================================================

def generate_trusted_device_token() -> str:
    """Generate a secure random token for trusted device."""
    return secrets.token_urlsafe(32)


def check_trusted_device(request: Request, user_id: int) -> bool:
    """
    Check if this device is trusted for the given user.
    Returns True if device is trusted and valid.
    """
    token = request.cookies.get(TRUSTED_DEVICE_COOKIE_NAME)
    if not token:
        return False

    token_hash = hash_token(token)
    device = get_trusted_device(token_hash)

    if not device:
        return False

    # Verify it belongs to this user and account is active
    if device.get('user_id') != user_id:
        return False

    if device.get('account_status') != 'active':
        return False

    # Update last used
    update_trusted_device_last_used(device['id'])

    return True


def create_trusted_device_response(
    response: Response,
    user_id: int,
    request: Optional[Request] = None
) -> None:
    """
    Create a trusted device record and set cookie on response.
    """
    # Generate token
    plain_token = generate_trusted_device_token()
    token_hash = hash_token(plain_token)
    expires_at = datetime.utcnow() + timedelta(days=TRUSTED_DEVICE_DAYS)

    # Get device info
    ip_address = get_client_ip(request) if request else None
    user_agent = get_user_agent(request) if request else None

    # Derive a device name from user agent
    device_name = _get_device_name(user_agent)

    # Store in database
    db_create_trusted_device(
        user_id=user_id,
        device_token_hash=token_hash,
        expires_at=expires_at,
        device_name=device_name,
        ip_address=ip_address,
        user_agent=user_agent
    )

    # Set cookie
    response.set_cookie(
        key=TRUSTED_DEVICE_COOKIE_NAME,
        value=plain_token,
        max_age=TRUSTED_DEVICE_DAYS * 24 * 60 * 60,
        httponly=True,
        secure=True,  # HTTPS only
        samesite="lax",
        path="/"
    )


def _get_device_name(user_agent: str) -> str:
    """Extract a friendly device name from user agent string."""
    if not user_agent:
        return "Unknown Device"

    ua_lower = user_agent.lower()

    # Determine OS
    if 'windows' in ua_lower:
        os_name = "Windows"
    elif 'macintosh' in ua_lower or 'mac os' in ua_lower:
        os_name = "Mac"
    elif 'linux' in ua_lower:
        os_name = "Linux"
    elif 'iphone' in ua_lower:
        os_name = "iPhone"
    elif 'ipad' in ua_lower:
        os_name = "iPad"
    elif 'android' in ua_lower:
        os_name = "Android"
    else:
        os_name = "Unknown"

    # Determine browser
    if 'chrome' in ua_lower and 'edg' not in ua_lower:
        browser = "Chrome"
    elif 'firefox' in ua_lower:
        browser = "Firefox"
    elif 'safari' in ua_lower and 'chrome' not in ua_lower:
        browser = "Safari"
    elif 'edg' in ua_lower:
        browser = "Edge"
    else:
        browser = "Browser"

    return f"{os_name} - {browser}"
# ============================================================================
# REGISTRATION ROUTES
# ============================================================================

@router.get("/register", response_class=HTMLResponse)
async def register_page(request: Request):
    """Display registration page."""
    # Check if already logged in
    user = get_current_user(request)
    if user:
        return RedirectResponse(url="/", status_code=302)

    return templates.TemplateResponse("auth/register.html", {
        "request": request,
        "error": None
    })


@router.post("/register")
async def register_submit(
    request: Request,
    email: str = Form(...),
    csrf_token: str = Form("")
):
    """Handle registration email submission."""
    _, _, require_csrf = _get_csrf_functions()
    if require_csrf and not require_csrf(request, csrf_token):
        return templates.TemplateResponse("auth/register.html", {
            "request": request,
            "error": "Invalid request. Please refresh the page and try again."
        })

    email = email.strip().lower()

    # Validate email format
    is_valid, error_msg = validate_email(email)
    if not is_valid:
        return templates.TemplateResponse("auth/register.html", {
            "request": request,
            "error": error_msg,
            "email": email
        })

    # Check if email already registered
    existing_user = get_user_by_email(email)
    if existing_user:
        return templates.TemplateResponse("auth/register.html", {
            "request": request,
            "error": "This email is already registered. Please log in.",
            "email": email
        })

    # --- Registration gate: only legislators and approved users ---
    if not is_authorized_email(email):
        logger.info(f"Registration denied for unauthorized email: {mask_email(email)}")
        return templates.TemplateResponse("auth/register.html", {
            "request": request,
            "error": "Registration is limited to Idaho legislators and approved users. "
                     "If you believe you should have access, contact info@billbriefer.com.",
            "email": email
        })

    # Check rate limit for MFA code requests
    recent_requests = count_recent_mfa_requests(email, "signup", 15)
    allowed, remaining, window = check_rate_limit(
        recent_requests,
        RATE_LIMITS['mfa_request_per_email']['limit'],
        RATE_LIMITS['mfa_request_per_email']['window_minutes']
    )
    if not allowed:
        return templates.TemplateResponse("auth/register.html", {
            "request": request,
            "error": f"Too many verification requests. Please try again in {window} minutes.",
            "email": email
        })

    # Generate verification code
    plain_code, hashed_code, expires_at = generate_mfa_code(expiry_minutes=15)

    # Store code in database
    db_create_mfa_code(
        email=email,
        code_hash=hashed_code,
        code_type="signup",
        expires_at=expires_at,
        ip_address=get_client_ip(request),
        user_agent=get_user_agent(request)
    )

    # Send verification email
    success, _ = send_signup_verification_code(email, plain_code)
    if not success:
        logger.error(f"Failed to send verification email to {email}")
        # Continue anyway - don't reveal email delivery issues

    # Store email in session for verification page
    # Using a simple approach - store in response cookies temporarily
    response = RedirectResponse(url="/auth/register/verify", status_code=302)
    response.set_cookie("pending_email", email, max_age=900, httponly=True)  # 15 min

    return response


@router.get("/register/verify", response_class=HTMLResponse)
async def register_verify_page(request: Request):
    """Display email verification page."""
    email = request.cookies.get("pending_email")
    if not email:
        return RedirectResponse(url="/auth/register", status_code=302)

    return templates.TemplateResponse("auth/register_verify.html", {
        "request": request,
        "email": email,
        "masked_email": mask_email(email),
        "error": None
    })


@router.post("/register/verify")
async def register_verify_submit(
    request: Request,
    code: str = Form(...)
):
    """Handle verification code submission."""
    email = request.cookies.get("pending_email")
    if not email:
        return RedirectResponse(url="/auth/register", status_code=302)

    code = code.strip()

    # Validate code format
    is_valid, error_msg = validate_mfa_code_format(code)
    if not is_valid:
        return templates.TemplateResponse("auth/register_verify.html", {
            "request": request,
            "email": email,
            "masked_email": mask_email(email),
            "error": error_msg
        })

    # Get latest code for this email
    mfa_record = get_latest_mfa_code(email, "signup")
    if not mfa_record:
        return templates.TemplateResponse("auth/register_verify.html", {
            "request": request,
            "email": email,
            "masked_email": mask_email(email),
            "error": "Code expired. Please request a new one."
        })

    # Check attempts
    if mfa_record['attempts'] >= 5:
        invalidate_mfa_codes(email, "signup")
        return templates.TemplateResponse("auth/register_verify.html", {
            "request": request,
            "email": email,
            "masked_email": mask_email(email),
            "error": "Too many failed attempts. Please request a new code."
        })

    # Verify code
    if not verify_mfa_code(code, mfa_record['code_hash']):
        increment_mfa_attempts(mfa_record['id'])
        attempts_left = 5 - mfa_record['attempts'] - 1
        return templates.TemplateResponse("auth/register_verify.html", {
            "request": request,
            "email": email,
            "masked_email": mask_email(email),
            "error": f"Invalid code. {attempts_left} attempts remaining."
        })

    # Mark code as used
    mark_mfa_code_used(mfa_record['id'])

    # Redirect to password creation
    response = RedirectResponse(url="/auth/register/password", status_code=302)
    response.set_cookie("verified_email", email, max_age=900, httponly=True)  # 15 min
    response.delete_cookie("pending_email")

    return response


@router.get("/register/password", response_class=HTMLResponse)
async def register_password_page(request: Request):
    """Display password creation page."""
    email = request.cookies.get("verified_email")
    if not email:
        return RedirectResponse(url="/auth/register", status_code=302)

    return templates.TemplateResponse("auth/register_password.html", {
        "request": request,
        "email": email,
        "error": None
    })


@router.post("/register/password")
async def register_password_submit(
    request: Request,
    password: str = Form(...),
    confirm_password: str = Form(...),
    remember_me: bool = Form(False)
):
    """Handle password creation and complete registration."""
    email = request.cookies.get("verified_email")
    if not email:
        return RedirectResponse(url="/auth/register", status_code=302)

    # Validate passwords match
    if password != confirm_password:
        return templates.TemplateResponse("auth/register_password.html", {
            "request": request,
            "email": email,
            "error": "Passwords do not match"
        })

    # Validate password strength
    is_valid, error_msg = validate_password(password)
    if not is_valid:
        return templates.TemplateResponse("auth/register_password.html", {
            "request": request,
            "email": email,
            "error": error_msg
        })

    # Look up legislator info from existing legislators database
    # This will be integrated with the main app's LEGISLATORS dict
    legislator_info = _lookup_legislator(email)

    if not legislator_info:
        return templates.TemplateResponse("auth/register_password.html", {
            "request": request,
            "email": email,
            "error": "We couldn't find this email in our Idaho Legislators database. Please contact support."
        })

    # Hash password
    password_hash = hash_password(password)

    # Create user
    user_id = create_user(
        email=email,
        password_hash=password_hash,
        name=legislator_info.get('name', ''),
        district=legislator_info.get('district'),
        chamber=legislator_info.get('chamber'),
        party=legislator_info.get('party'),
        email_verified=True
    )

    if not user_id:
        return templates.TemplateResponse("auth/register_password.html", {
            "request": request,
            "email": email,
            "error": "Account creation failed. This email may already be registered."
        })

    # Log security event
    log_security_event(
        user_id=user_id,
        event_type="account_created",
        description=f"Account created for {email}",
        ip_address=get_client_ip(request),
        user_agent=get_user_agent(request)
    )

    # Send welcome email
    send_welcome_email(
        email,
        legislator_info.get('name', 'Legislator'),
        legislator_info.get('district', 'your district')
    )

    # Create session and redirect
    remember_days = 7 if remember_me else 1
    response = RedirectResponse(url="/", status_code=302)
    response.delete_cookie("verified_email")
    create_session_response(response, user_id, remember_days, request)

    # Update last login
    update_user_last_login(user_id)

    return response


def _lookup_legislator(email: str) -> Optional[dict]:
    """
    Look up legislator info from the main app's LEGISLATORS database.
    Returns dict with name, district, chamber, party or None if not found.

    NOTE: Non-legislative emails (not @legislature/house/senate.idaho.gov)
    that pass the allowlist/manual_users gate are assigned Staff/Demo profile.
    This allows authorized staff and demo accounts to register without
    needing a legislator record. The gate check happens in register_submit()
    before this function is called.
    """
    # Import here to avoid circular imports
    try:
        from app.main import LEGISLATORS
    except ImportError:
        logger.error("Could not import LEGISLATORS from main")
        return None

    email_lower = email.lower()

    # Try LEGISLATORS first (keyed by email)
    leg_info = LEGISLATORS.get(email_lower)
    if leg_info:
        return {
            'name': leg_info.get('display_name', leg_info.get('full_name', '')),
            'district': leg_info.get('ld_code', ''),
            'chamber': leg_info.get('chamber', _get_chamber_from_title(leg_info.get('title', ''))),
            'party': leg_info.get('party', '')
        }

    # Staff/demo accounts: non-legislative emails that passed the allowlist gate
    if not email.endswith('@legislature.idaho.gov') and \
       not email.endswith('@house.idaho.gov') and \
       not email.endswith('@senate.idaho.gov'):
        logger.info(f"STAFF_REGISTER: Non-legislative email assigned Staff/Demo profile: {email}")
        return {
            'name': email.split('@')[0].replace('.', ' ').title(),
            'district': 'Staff/Demo',
            'chamber': 'Staff',
            'party': ''
        }

    return None


def _get_chamber_from_title(title: str) -> str:
    """Extract chamber from title."""
    title_lower = title.lower()
    if 'senator' in title_lower:
        return 'Senate'
    elif 'representative' in title_lower:
        return 'House'
    return ''


# ============================================================================
# LOGIN ROUTES
# ============================================================================

@router.get("/login", response_class=HTMLResponse)
async def login_page(request: Request):
    """Display login page."""
    # Check if already logged in
    user = get_current_user(request)
    if user:
        return RedirectResponse(url="/", status_code=302)

    return templates.TemplateResponse("auth/login.html", {
        "request": request,
        "error": None
    })


@router.post("/login")
async def login_submit(
    request: Request,
    email: str = Form(...),
    password: str = Form(...),
    csrf_token: str = Form("")
):
    """Handle login form submission."""
    _, _, require_csrf = _get_csrf_functions()
    if require_csrf and not require_csrf(request, csrf_token):
        return templates.TemplateResponse("auth/login.html", {
            "request": request,
            "error": "Invalid request. Please refresh the page and try again."
        })

    email = email.strip().lower()
    ip_address = get_client_ip(request)
    user_agent = get_user_agent(request)

    # Check rate limit by email
    email_failures = count_failed_attempts(email, 15)
    allowed, _, _ = check_rate_limit(
        email_failures,
        RATE_LIMITS['login_per_email']['limit'],
        RATE_LIMITS['login_per_email']['window_minutes']
    )
    if not allowed:
        return templates.TemplateResponse("auth/login.html", {
            "request": request,
            "error": "Too many login attempts. Please try again in 15 minutes.",
            "email": email
        })

    # Check rate limit by IP
    ip_failures = count_failed_attempts_by_ip(ip_address, 15)
    allowed, _, _ = check_rate_limit(
        ip_failures,
        RATE_LIMITS['login_per_ip']['limit'],
        RATE_LIMITS['login_per_ip']['window_minutes']
    )
    if not allowed:
        return templates.TemplateResponse("auth/login.html", {
            "request": request,
            "error": "Too many login attempts from this location. Please try again later.",
            "email": email
        })

    # Look up user
    user = get_user_by_email(email)

    if not user:
        # Log failed attempt (don't reveal user doesn't exist)
        log_login_attempt(email, False, "invalid_email", "password", None, ip_address, user_agent)
        return templates.TemplateResponse("auth/login.html", {
            "request": request,
            "error": "Invalid email or password.",
            "email": email
        })

    # Check if account is locked
    is_locked, minutes_remaining = check_user_locked(user['id'])
    if is_locked:
        return templates.TemplateResponse("auth/login.html", {
            "request": request,
            "error": f"Account temporarily locked. Try again in {minutes_remaining} minutes.",
            "email": email
        })

    # Check account status
    if user['account_status'] == 'suspended':
        log_login_attempt(email, False, "account_suspended", "password", user['id'], ip_address, user_agent)
        return templates.TemplateResponse("auth/login.html", {
            "request": request,
            "error": "Account suspended. Please contact support.",
            "email": email
        })

    # Verify password
    if not verify_password(password, user['password_hash']):
        log_login_attempt(email, False, "wrong_password", "password", user['id'], ip_address, user_agent)

        # Check if should lock account
        recent_failures = count_failed_attempts(email, 15)
        if recent_failures >= 5:
            lock_duration = calculate_lockout_duration(1)
            update_user_status(user['id'], 'locked', datetime.utcnow() + timedelta(minutes=lock_duration))
            log_security_event(user['id'], "account_locked", f"Locked for {lock_duration} min due to failed attempts", ip_address, user_agent)
            send_account_locked_notification(email, lock_duration)

            return templates.TemplateResponse("auth/login.html", {
                "request": request,
                "error": f"Account locked due to multiple failed attempts. Try again in {lock_duration} minutes.",
                "email": email
            })

        return templates.TemplateResponse("auth/login.html", {
            "request": request,
            "error": "Invalid email or password.",
            "email": email
        })

    # Password correct - check for existing valid session with Remember Me
    existing_token = request.cookies.get(SESSION_COOKIE_NAME)
    if existing_token:
        token_hash = hash_token(existing_token)
        existing_session = get_session_by_token_hash(token_hash)
        if existing_session and existing_session.get('user_id') == user['id']:
            # Valid session exists, skip MFA
            log_login_attempt(email, True, None, "session_reuse", user['id'], ip_address, user_agent)
            update_user_last_login(user['id'])
            return RedirectResponse(url="/", status_code=302)

    # Check if this is a trusted device - if so, skip MFA
    if check_trusted_device(request, user['id']):
        log_login_attempt(email, True, None, "trusted_device", user['id'], ip_address, user_agent)
        log_security_event(user['id'], "login_success", "Login via trusted device (MFA skipped)", ip_address, user_agent)

        # Create session
        response = RedirectResponse(url="/", status_code=302)
        create_session_response(response, user['id'], 7, request)  # 7 day session
        update_user_last_login(user['id'])

        return response

    # No valid session and not trusted device - proceed to MFA
    # Store pending login info
    response = RedirectResponse(url="/auth/login/verify", status_code=302)
    response.set_cookie("pending_login_email", email, max_age=600, httponly=True)  # 10 min

    # Generate and send MFA code
    plain_code, hashed_code, expires_at = generate_mfa_code(expiry_minutes=10)
    db_create_mfa_code(
        email=email,
        code_hash=hashed_code,
        code_type="login",
        expires_at=expires_at,
        user_id=user['id'],
        ip_address=ip_address,
        user_agent=user_agent
    )
    send_login_mfa_code(email, plain_code)

    return response


@router.get("/login/verify", response_class=HTMLResponse)
async def login_verify_page(request: Request):
    """Display MFA verification page."""
    email = request.cookies.get("pending_login_email")
    if not email:
        return RedirectResponse(url="/auth/login", status_code=302)

    return templates.TemplateResponse("auth/login_verify.html", {
        "request": request,
        "email": email,
        "masked_email": mask_email(email),
        "error": None
    })


@router.post("/login/verify")
async def login_verify_submit(
    request: Request,
    code: str = Form(...),
    remember_me: bool = Form(False),
    remember_days: int = Form(7),
    trust_device: bool = Form(False)
):
    """Handle MFA code verification."""
    email = request.cookies.get("pending_login_email")
    if not email:
        return RedirectResponse(url="/auth/login", status_code=302)

    ip_address = get_client_ip(request)
    user_agent = get_user_agent(request)
    code = code.strip()

    # Get user
    user = get_user_by_email(email)
    if not user:
        return RedirectResponse(url="/auth/login", status_code=302)

    # Validate code format
    is_valid, error_msg = validate_mfa_code_format(code)
    if not is_valid:
        return templates.TemplateResponse("auth/login_verify.html", {
            "request": request,
            "email": email,
            "masked_email": mask_email(email),
            "error": error_msg
        })

    # Get latest MFA code
    mfa_record = get_latest_mfa_code(email, "login")
    if not mfa_record:
        return templates.TemplateResponse("auth/login_verify.html", {
            "request": request,
            "email": email,
            "masked_email": mask_email(email),
            "error": "Code expired. Please log in again."
        })

    # Check attempts
    if mfa_record['attempts'] >= 5:
        invalidate_mfa_codes(email, "login")
        log_login_attempt(email, False, "mfa_max_attempts", "mfa", user['id'], ip_address, user_agent)
        return templates.TemplateResponse("auth/login_verify.html", {
            "request": request,
            "email": email,
            "masked_email": mask_email(email),
            "error": "Too many failed attempts. Please log in again."
        })

    # Verify code
    if not verify_mfa_code(code, mfa_record['code_hash']):
        attempts = increment_mfa_attempts(mfa_record['id'])
        log_login_attempt(email, False, "wrong_mfa_code", "mfa", user['id'], ip_address, user_agent)

        # Check if should lock account (3 failed MFA attempts)
        if attempts >= 3:
            lock_duration = calculate_lockout_duration(1)
            update_user_status(user['id'], 'locked', datetime.utcnow() + timedelta(minutes=lock_duration))
            log_security_event(user['id'], "account_locked", f"Locked for {lock_duration} min due to failed MFA", ip_address, user_agent)
            send_account_locked_notification(email, lock_duration)
            invalidate_mfa_codes(email, "login")

            response = RedirectResponse(url="/auth/login", status_code=302)
            response.delete_cookie("pending_login_email")
            return response

        attempts_left = 5 - attempts
        return templates.TemplateResponse("auth/login_verify.html", {
            "request": request,
            "email": email,
            "masked_email": mask_email(email),
            "error": f"Invalid code. {attempts_left} attempts remaining."
        })

    # MFA verified - create session
    mark_mfa_code_used(mfa_record['id'])
    log_login_attempt(email, True, None, "mfa", user['id'], ip_address, user_agent)
    log_security_event(user['id'], "login_success", "Successful login with MFA", ip_address, user_agent)

    # Create session
    session_days = remember_days if remember_me else 1
    response = RedirectResponse(url="/", status_code=302)
    response.delete_cookie("pending_login_email")
    create_session_response(response, user['id'], session_days, request)

    # If user checked "Trust this device", create trusted device record
    if trust_device:
        create_trusted_device_response(response, user['id'], request)
        log_security_event(user['id'], "trusted_device_added", f"Device trusted for {TRUSTED_DEVICE_DAYS} days", ip_address, user_agent)

    update_user_last_login(user['id'])

    return response


# ============================================================================
# LOGOUT
# ============================================================================

@router.get("/logout")
async def logout(request: Request):
    """Handle logout."""
    token = request.cookies.get(SESSION_COOKIE_NAME)
    if token:
        token_hash = hash_token(token)
        session = get_session_by_token_hash(token_hash)
        if session:
            log_security_event(
                session['user_id'],
                "logout",
                "User logged out",
                get_client_ip(request),
                get_user_agent(request)
            )
        delete_session_token(token_hash)

    response = RedirectResponse(url="/auth/login", status_code=302)
    clear_session_cookie(response)
    return response


# ============================================================================
# PASSWORD RESET ROUTES
# ============================================================================

@router.get("/reset-password", response_class=HTMLResponse)
async def reset_password_page(request: Request):
    """Display password reset request page."""
    return templates.TemplateResponse("auth/reset_password.html", {
        "request": request,
        "error": None,
        "success": None
    })


@router.post("/reset-password")
async def reset_password_submit(
    request: Request,
    email: str = Form(...),
    csrf_token: str = Form("")
):
    """Handle password reset request."""
    _, _, require_csrf = _get_csrf_functions()
    if require_csrf and not require_csrf(request, csrf_token):
        return templates.TemplateResponse("auth/reset_password.html", {
            "request": request,
            "error": "Invalid request. Please refresh the page and try again.",
            "success": None
        })

    email = email.strip().lower()
    ip_address = get_client_ip(request)

    # Always show success message (don't reveal if email exists)
    success_msg = "If this email is registered, you'll receive a reset code shortly."

    # Check rate limit
    recent_requests = count_recent_mfa_requests(email, "password_reset", 1440)  # 24 hours
    if recent_requests >= 3:
        return templates.TemplateResponse("auth/reset_password.html", {
            "request": request,
            "error": None,
            "success": success_msg  # Don't reveal rate limit
        })

    # Look up user
    user = get_user_by_email(email)
    if user:
        # Generate and send code
        plain_code, hashed_code, expires_at = generate_mfa_code(expiry_minutes=10)
        db_create_mfa_code(
            email=email,
            code_hash=hashed_code,
            code_type="password_reset",
            expires_at=expires_at,
            user_id=user['id'],
            ip_address=ip_address,
            user_agent=get_user_agent(request)
        )
        send_password_reset_code(email, plain_code)

        log_security_event(user['id'], "password_reset_requested", "Password reset code sent", ip_address)

    # Redirect to verification page
    response = RedirectResponse(url="/auth/reset-password/verify", status_code=302)
    response.set_cookie("reset_email", email, max_age=600, httponly=True)
    return response


@router.get("/reset-password/verify", response_class=HTMLResponse)
async def reset_password_verify_page(request: Request):
    """Display password reset verification page."""
    email = request.cookies.get("reset_email")
    if not email:
        return RedirectResponse(url="/auth/reset-password", status_code=302)

    return templates.TemplateResponse("auth/reset_password_verify.html", {
        "request": request,
        "email": email,
        "masked_email": mask_email(email),
        "error": None
    })


@router.post("/reset-password/verify")
async def reset_password_verify_submit(
    request: Request,
    code: str = Form(...)
):
    """Handle password reset code verification."""
    email = request.cookies.get("reset_email")
    if not email:
        return RedirectResponse(url="/auth/reset-password", status_code=302)

    code = code.strip()

    # Validate code format
    is_valid, error_msg = validate_mfa_code_format(code)
    if not is_valid:
        return templates.TemplateResponse("auth/reset_password_verify.html", {
            "request": request,
            "email": email,
            "masked_email": mask_email(email),
            "error": error_msg
        })

    # Get latest code
    mfa_record = get_latest_mfa_code(email, "password_reset")
    if not mfa_record:
        return templates.TemplateResponse("auth/reset_password_verify.html", {
            "request": request,
            "email": email,
            "masked_email": mask_email(email),
            "error": "Code expired. Please request a new one."
        })

    # Check attempts
    if mfa_record['attempts'] >= 5:
        invalidate_mfa_codes(email, "password_reset")
        return templates.TemplateResponse("auth/reset_password_verify.html", {
            "request": request,
            "email": email,
            "masked_email": mask_email(email),
            "error": "Too many failed attempts. Please request a new code."
        })

    # Verify code
    if not verify_mfa_code(code, mfa_record['code_hash']):
        increment_mfa_attempts(mfa_record['id'])
        attempts_left = 5 - mfa_record['attempts'] - 1
        return templates.TemplateResponse("auth/reset_password_verify.html", {
            "request": request,
            "email": email,
            "masked_email": mask_email(email),
            "error": f"Invalid code. {attempts_left} attempts remaining."
        })

    # Mark code as used
    mark_mfa_code_used(mfa_record['id'])

    # Redirect to new password page
    response = RedirectResponse(url="/auth/reset-password/new", status_code=302)
    response.set_cookie("verified_reset_email", email, max_age=600, httponly=True)
    response.delete_cookie("reset_email")
    return response


@router.get("/reset-password/new", response_class=HTMLResponse)
async def reset_password_new_page(request: Request):
    """Display new password page."""
    email = request.cookies.get("verified_reset_email")
    if not email:
        return RedirectResponse(url="/auth/reset-password", status_code=302)

    return templates.TemplateResponse("auth/reset_password_new.html", {
        "request": request,
        "error": None
    })


@router.post("/reset-password/new")
async def reset_password_new_submit(
    request: Request,
    password: str = Form(...),
    confirm_password: str = Form(...)
):
    """Handle new password submission."""
    email = request.cookies.get("verified_reset_email")
    if not email:
        return RedirectResponse(url="/auth/reset-password", status_code=302)

    # Validate passwords match
    if password != confirm_password:
        return templates.TemplateResponse("auth/reset_password_new.html", {
            "request": request,
            "error": "Passwords do not match"
        })

    # Validate password strength
    is_valid, error_msg = validate_password(password)
    if not is_valid:
        return templates.TemplateResponse("auth/reset_password_new.html", {
            "request": request,
            "error": error_msg
        })

    # Get user
    user = get_user_by_email(email)
    if not user:
        return RedirectResponse(url="/auth/reset-password", status_code=302)

    # Update password
    password_hash = hash_password(password)
    update_user_password(user['id'], password_hash)

    # Invalidate all sessions
    delete_all_user_sessions(user['id'])

    # Log event and send notification
    log_security_event(user['id'], "password_changed", "Password reset completed", get_client_ip(request))
    send_password_changed_notification(email)

    # Redirect to login
    response = RedirectResponse(url="/auth/login?reset=success", status_code=302)
    response.delete_cookie("verified_reset_email")
    clear_session_cookie(response)
    return response


# ============================================================================
# RESEND CODE ROUTES
# ============================================================================

@router.post("/resend-code/{code_type}")
async def resend_code(
    request: Request,
    code_type: str
):
    """Resend verification code."""
    if code_type == "signup":
        email = request.cookies.get("pending_email")
        expiry_minutes = 15
        send_func = send_signup_verification_code
    elif code_type == "login":
        email = request.cookies.get("pending_login_email")
        expiry_minutes = 10
        send_func = send_login_mfa_code
    elif code_type == "reset":
        email = request.cookies.get("reset_email")
        expiry_minutes = 10
        send_func = send_password_reset_code
    else:
        raise HTTPException(status_code=400, detail="Invalid code type")

    if not email:
        raise HTTPException(status_code=400, detail="No pending verification")

    # Check rate limit
    recent_requests = count_recent_mfa_requests(email, code_type, 15)
    if recent_requests >= 3:
        raise HTTPException(status_code=429, detail="Too many requests. Please wait.")

    # Invalidate old codes
    invalidate_mfa_codes(email, code_type)

    # Generate new code
    plain_code, hashed_code, expires_at = generate_mfa_code(expiry_minutes=expiry_minutes)

    # Store in database
    user = get_user_by_email(email)
    db_create_mfa_code(
        email=email,
        code_hash=hashed_code,
        code_type=code_type,
        expires_at=expires_at,
        user_id=user['id'] if user else None,
        ip_address=get_client_ip(request),
        user_agent=get_user_agent(request)
    )

    # Send email
    send_func(email, plain_code)

    return {"success": True, "message": "Code sent"}


# ============================================================================
# ACCOUNT SETTINGS ROUTES
# ============================================================================

@router.get("/settings", response_class=HTMLResponse)
async def settings_page(request: Request):
    """Display account settings page."""
    user = get_current_user(request)
    if not user:
        return RedirectResponse(url="/auth/login", status_code=302)

    # Get user active sessions
    sessions = get_user_sessions(user["user_id"])

    # Get user trusted devices
    trusted_devices = get_user_trusted_devices(user["user_id"])

    # Get current session token to highlight it
    current_token = request.cookies.get(SESSION_COOKIE_NAME)
    current_token_hash = hash_token(current_token) if current_token else None

    return templates.TemplateResponse("auth/settings.html", {
        "request": request,
        "user": user,
        "sessions": sessions,
        "trusted_devices": trusted_devices,
        "current_token_hash": current_token_hash,
        "error": None,
        "success": None
    })


@router.post("/settings/change-password")
async def change_password_submit(
    request: Request,
    current_password: str = Form(...),
    new_password: str = Form(...),
    confirm_password: str = Form(...),
    csrf_token: str = Form("")
):
    """Handle password change."""
    user = get_current_user(request)
    if not user:
        return RedirectResponse(url="/auth/login", status_code=302)

    user_record = get_user_by_id(user["user_id"])
    if not user_record:
        return RedirectResponse(url="/auth/login", status_code=302)

    sessions = get_user_sessions(user["user_id"])
    trusted_devices = get_user_trusted_devices(user["user_id"])
    current_token = request.cookies.get(SESSION_COOKIE_NAME)
    current_token_hash = hash_token(current_token) if current_token else None

    if not verify_password(current_password, user_record["password_hash"]):
        return templates.TemplateResponse("auth/settings.html", {
            "request": request, "user": user, "sessions": sessions,
            "trusted_devices": trusted_devices, "current_token_hash": current_token_hash,
            "error": "Current password is incorrect", "success": None
        })

    if new_password != confirm_password:
        return templates.TemplateResponse("auth/settings.html", {
            "request": request, "user": user, "sessions": sessions,
            "trusted_devices": trusted_devices, "current_token_hash": current_token_hash,
            "error": "New passwords do not match", "success": None
        })

    is_valid, error_msg = validate_password(new_password)
    if not is_valid:
        return templates.TemplateResponse("auth/settings.html", {
            "request": request, "user": user, "sessions": sessions,
            "trusted_devices": trusted_devices, "current_token_hash": current_token_hash,
            "error": error_msg, "success": None
        })

    new_password_hash = hash_password(new_password)
    update_user_password(user["user_id"], new_password_hash)

    log_security_event(user["user_id"], "password_changed", "Password changed from settings",
                       get_client_ip(request), get_user_agent(request))
    send_password_changed_notification(user["email"])

    for session in sessions:
        if session["token_hash"] != current_token_hash:
            delete_session_token(session["token_hash"])

    sessions = get_user_sessions(user["user_id"])
    return templates.TemplateResponse("auth/settings.html", {
        "request": request, "user": user, "sessions": sessions,
        "trusted_devices": trusted_devices, "current_token_hash": current_token_hash,
        "error": None, "success": "Password changed successfully. All other sessions have been logged out."
    })


@router.post("/settings/sessions/revoke/{session_id}")
async def revoke_session(request: Request, session_id: int):
    """Revoke a specific session."""
    user = get_current_user(request)
    if not user:
        return RedirectResponse(url="/auth/login", status_code=302)

    sessions = get_user_sessions(user["user_id"])
    session_to_revoke = next((s for s in sessions if s["id"] == session_id), None)

    if not session_to_revoke:
        return RedirectResponse(url="/auth/settings", status_code=302)

    current_token = request.cookies.get(SESSION_COOKIE_NAME)
    current_token_hash = hash_token(current_token) if current_token else None

    if session_to_revoke["token_hash"] == current_token_hash:
        delete_session_token(current_token_hash)
        response = RedirectResponse(url="/auth/login", status_code=302)
        clear_session_cookie(response)
        return response

    delete_session_token(session_to_revoke["token_hash"])
    log_security_event(user["user_id"], "session_revoked",
                       f"Session revoked: {session_to_revoke.get(user_agent, Unknown)[:50]}",
                       get_client_ip(request), get_user_agent(request))

    return RedirectResponse(url="/auth/settings", status_code=302)


@router.post("/settings/sessions/revoke-all")
async def revoke_all_other_sessions(request: Request):
    """Revoke all sessions except current one."""
    user = get_current_user(request)
    if not user:
        return RedirectResponse(url="/auth/login", status_code=302)

    current_token = request.cookies.get(SESSION_COOKIE_NAME)
    current_token_hash = hash_token(current_token) if current_token else None

    sessions = get_user_sessions(user["user_id"])
    revoked_count = 0
    for session in sessions:
        if session["token_hash"] != current_token_hash:
            delete_session_token(session["token_hash"])
            revoked_count += 1

    if revoked_count > 0:
        log_security_event(user["user_id"], "sessions_revoked_all",
                           f"Revoked {revoked_count} other session(s)",
                           get_client_ip(request), get_user_agent(request))

    return RedirectResponse(url="/auth/settings", status_code=302)


@router.post("/settings/trusted-devices/remove/{device_id}")
async def remove_trusted_device(request: Request, device_id: int):
    """Remove a trusted device."""
    user = get_current_user(request)
    if not user:
        return RedirectResponse(url="/auth/login", status_code=302)

    trusted_devices = get_user_trusted_devices(user["user_id"])
    device_to_remove = next((d for d in trusted_devices if d["id"] == device_id), None)

    if not device_to_remove:
        return RedirectResponse(url="/auth/settings", status_code=302)

    delete_trusted_device(device_to_remove["device_token_hash"])
    log_security_event(user["user_id"], "trusted_device_removed",
                       f"Trusted device removed: {device_to_remove.get(device_name, Unknown)}",
                       get_client_ip(request), get_user_agent(request))

    return RedirectResponse(url="/auth/settings", status_code=302)


@router.post("/settings/trusted-devices/remove-all")
async def remove_all_trusted_devices(request: Request):
    """Remove all trusted devices."""
    user = get_current_user(request)
    if not user:
        return RedirectResponse(url="/auth/login", status_code=302)

    trusted_devices = get_user_trusted_devices(user["user_id"])
    count = len(trusted_devices)

    if count > 0:
        delete_all_user_trusted_devices(user["user_id"])
        log_security_event(user["user_id"], "trusted_devices_removed_all",
                           f"Removed {count} trusted device(s)",
                           get_client_ip(request), get_user_agent(request))

    return RedirectResponse(url="/auth/settings", status_code=302)
