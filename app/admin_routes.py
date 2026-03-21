"""Admin route handlers for the Bill Briefer application."""

import collections
import logging
import threading
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import List

from fastapi import APIRouter, Form, Request
from fastapi.responses import (
    FileResponse, HTMLResponse, JSONResponse, PlainTextResponse,
    RedirectResponse, Response,
)

from app.utils import (
    _read_json, _write_json, _norm_text,
    _load_allowlist, _load_manual_users, _save_manual_users, _load_admin_allowlist,
    BOISE_TZ, JOBS_DIR, REPORTS_DIR,
    ALLOWLIST_PATH, ADMIN_ALLOWLIST_PATH,
    JOB_TIMEOUT_MINUTES, DEFAULT_SESSION_YEAR, SESSION_ID_MAP,
)
from app.usage_report import generate_report
from app.legislators import LEGISLATORS
from app.auth.auth_db import get_db_connection

logger = logging.getLogger(__name__)

router = APIRouter()

# --- Templates (set by main.py at startup) ---
_templates = None

def set_admin_templates(t):
    global _templates
    _templates = t

# Alias for convenience
@property
def templates():
    return _templates


def _get_templates():
    return _templates


# --- Lazy imports from main (avoid circular dependency) ---
def _current_user(request):
    from app.main import current_user
    return current_user(request)


def _require_login(request):
    from app.main import require_login
    return require_login(request)


def _require_admin(request):
    from app.main import require_admin
    return require_admin(request)


def _validate_csrf(request, token):
    from app.main import _validate_csrf_token
    return _validate_csrf_token(request, token)


def _get_job_path(job_id):
    from app.job_processor import _job_path
    return _job_path(job_id)


def _get_allowlist():
    """Get current ALLOWLIST from main module."""
    from app.main import ALLOWLIST
    return ALLOWLIST


def _set_allowlist(new_val):
    """Update ALLOWLIST in main module."""
    import app.main as main_mod
    main_mod.ALLOWLIST = new_val




def _get_chamber_access(email: str, default_title: str = '') -> dict:
    """Look up chamber access flags for a user from auth DB."""
    with get_db_connection() as conn:
        cur = conn.cursor()
        cur.execute("SELECT can_view_house, can_view_senate FROM users WHERE email = ?", (email.lower(),))
        row = cur.fetchone()
        if row:
            return {"can_view_house": row[0], "can_view_senate": row[1], "has_account": True}
    # No account yet — default based on title/email
    is_senator = 'senator' in default_title.lower() or email.lower().endswith('@senate.idaho.gov')
    is_rep = 'representative' in default_title.lower() or email.lower().endswith('@house.idaho.gov')
    if is_senator:
        return {"can_view_house": 0, "can_view_senate": 1, "has_account": False}
    elif is_rep:
        return {"can_view_house": 1, "can_view_senate": 0, "has_account": False}
    return {"can_view_house": 1, "can_view_senate": 1, "has_account": False}

@router.get("/not-authorized", response_class=HTMLResponse)
def not_authorized_page(request: Request):
    return _get_templates().TemplateResponse("not_authorized.html", {"request": request})



@router.get("/admin/reports", response_class=HTMLResponse)
def admin_reports_page(request: Request):
    redir = _require_admin(request)
    if redir:
        return redir

    now_boise = datetime.now(BOISE_TZ)
    default_end = now_boise.date().isoformat()
    default_start = (now_boise.date().fromordinal(now_boise.date().toordinal() - 6)).isoformat()

    reports = []
    try:
        files = sorted(REPORTS_DIR.glob("usage_report_*.pdf"), key=lambda x: x.stat().st_mtime, reverse=True)[:25]
        for f in files:
            mtime = datetime.fromtimestamp(f.stat().st_mtime, tz=timezone.utc).astimezone(BOISE_TZ)
            reports.append({"name": f.name, "mtime": mtime.isoformat(timespec="seconds"), "size": f"{f.stat().st_size:,} bytes"})
    except Exception:
        reports = []

    return _get_templates().TemplateResponse(
        "admin_reports.html",
        {"request": request, "default_start": default_start, "default_end": default_end, "reports": reports, "error": None},
    )



@router.post("/admin/reports/generate")
def admin_reports_generate(request: Request, start: str = Form(...), end: str = Form(...)):
    redir = _require_admin(request)
    if redir:
        return redir
    out_path, _total = generate_report(start, end)
    return FileResponse(path=str(out_path), media_type="application/pdf", filename=Path(out_path).name)



@router.get("/admin/reports/download/{filename}")
def admin_reports_download(request: Request, filename: str):
    redir = _require_admin(request)
    if redir:
        return redir
    safe = Path(filename).name
    fpath = REPORTS_DIR / safe
    if (not fpath.exists()) or (not fpath.is_file()):
        return PlainTextResponse("Not found\n", status_code=404)
    return FileResponse(path=str(fpath), media_type="application/pdf", filename=safe)


# --- Admin: Manage Users (allowlist + names) ---

# --- Admin: Manage Users (allowlist + names) ---
def _reload_auth_caches() -> None:
    # Refresh in-memory caches so changes apply immediately (no restart needed)
    _set_allowlist(_load_allowlist())

@router.get("/admin/users", response_class=HTMLResponse)
def admin_users_page(request: Request):
    redir = _require_admin(request)
    if redir:
        return redir

    # Load current allowlist
    allow = _load_allowlist()
    
    # Build legislator lists with authorization status
    senators = []
    representatives = []
    
    for email, leg in sorted(LEGISLATORS.items(), key=lambda x: x[1]['last_name']):
        ca = _get_chamber_access(email, leg.get('title', ''))
        leg_data = {
            'email': leg['email'],
            'display_name': leg['display_name'],
            'district': leg['district'],
            'authorized': email in allow,
            'can_view_house': ca['can_view_house'],
            'can_view_senate': ca['can_view_senate'],
            'has_account': ca['has_account'],
        }
        if 'senator' in leg['title'].lower():
            senators.append(leg_data)
        else:
            representatives.append(leg_data)
    
    # Find manual users (in allowlist but not legislators)
    legislator_emails = set(LEGISLATORS.keys())
    manual_names_dict = _load_manual_users()
    manual_users = []
    for email in sorted(allow):
        if email not in legislator_emails:
            mu = manual_names_dict.get(email, {})
            mu_name = mu.get("name") if isinstance(mu, dict) else mu
            mu_dist = mu.get("district") if isinstance(mu, dict) else None
            ca = _get_chamber_access(email)
            manual_users.append({
                'email': email, 'name': mu_name, 'district': mu_dist,
                'can_view_house': ca['can_view_house'],
                'can_view_senate': ca['can_view_senate'],
                'has_account': ca['has_account'],
            })

    return _get_templates().TemplateResponse(
        "admin_users.html",
        {
            "request": request,
            "senators": senators,
            "representatives": representatives,
            "manual_users": manual_users,
            "message": None,
            "error": None
        },
    )


@router.post("/admin/users", response_class=HTMLResponse)
def admin_users_save(request: Request, emails: List[str] = Form(default=[]), manual_names: List[str] = Form(default=[])):
    redir = _require_admin(request)
    if redir:
        return redir

    # Normalize emails
    allow_set = set()
    for email in emails:
        email_clean = email.strip().lower()
        if email_clean and "@" in email_clean:
            allow_set.add(email_clean)

    # Save allowlist
    ALLOWLIST_PATH.write_text("\n".join(sorted(allow_set)) + ("\n" if allow_set else ""), encoding="utf-8")

    # Save manual user names
    manual_users_dict = {}
    for entry in manual_names:
        if "|" in entry:
            parts = entry.split("|")
            email = parts[0].strip().lower()
            name = parts[1].strip() if len(parts) > 1 else ""
            district_str = parts[2].strip() if len(parts) > 2 else ""
            district = int(district_str) if district_str.isdigit() and 1 <= int(district_str) <= 35 else None
            if email and "@" in email:
                manual_users_dict[email] = {"name": name, "district": district}
    _save_manual_users(manual_users_dict)

    # Refresh in-memory caches
    _reload_auth_caches()

    # Rebuild page with updated data
    senators = []
    representatives = []
    
    for email, leg in sorted(LEGISLATORS.items(), key=lambda x: x[1]['last_name']):
        ca = _get_chamber_access(email, leg.get('title', ''))
        leg_data = {
            'email': leg['email'],
            'display_name': leg['display_name'],
            'district': leg['district'],
            'authorized': email.lower() in allow_set,
            'can_view_house': ca['can_view_house'],
            'can_view_senate': ca['can_view_senate'],
            'has_account': ca['has_account'],
        }
        if 'senator' in leg['title'].lower():
            senators.append(leg_data)
        else:
            representatives.append(leg_data)

    # Find manual users (in allowlist but not legislators)
    legislator_emails = set(LEGISLATORS.keys())
    manual_names_dict = _load_manual_users()
    manual_users = []
    for email in sorted(allow_set):
        if email not in legislator_emails:
            mu = manual_names_dict.get(email, {})
            mu_name = mu.get("name") if isinstance(mu, dict) else mu
            mu_dist = mu.get("district") if isinstance(mu, dict) else None
            ca = _get_chamber_access(email)
            manual_users.append({
                'email': email, 'name': mu_name, 'district': mu_dist,
                'can_view_house': ca['can_view_house'],
                'can_view_senate': ca['can_view_senate'],
                'has_account': ca['has_account'],
            })

    return _get_templates().TemplateResponse(
        "admin_users.html",
        {
            "request": request,
            "senators": senators,
            "representatives": representatives,
            "manual_users": manual_users,
            "message": f"Saved. {len(allow_set)} users authorized.",
            "error": None
        },
    )


@router.post("/admin/users/chamber-toggle")
async def admin_chamber_toggle(request: Request):
    """Toggle chamber access for a user via AJAX."""
    redir = _require_admin(request)
    if redir:
        return JSONResponse({"error": "unauthorized"}, status_code=401)

    form = await request.form()
    email = (form.get("email") or "").strip().lower()
    field = form.get("field", "")
    value = int(form.get("value", "1"))

    if field not in ("can_view_house", "can_view_senate"):
        return JSONResponse({"error": "invalid field"}, status_code=400)
    if not email:
        return JSONResponse({"error": "no email"}, status_code=400)

    with get_db_connection() as conn:
        cursor = conn.cursor()
        cursor.execute(f"UPDATE users SET {field} = ? WHERE email = ?", (value, email))
        conn.commit()
        updated = cursor.rowcount

    return JSONResponse({"ok": True, "updated": updated})



# ── Admin Activity Dashboard ──

@router.get("/admin/activity", response_class=HTMLResponse)
def admin_activity_page(request: Request):
    redir = _require_admin(request)
    if redir:
        return redir

    from datetime import datetime, timedelta, timezone
    import time as _time

    now_utc = datetime.utcnow()
    online_cutoff = now_utc - timedelta(minutes=30)
    today_cutoff = now_utc - timedelta(hours=24)
    week_cutoff = now_utc - timedelta(days=7)

    with get_db_connection() as conn:
        cur = conn.cursor()

        # All users
        cur.execute("""
            SELECT id, email, name, chamber, can_view_house, can_view_senate,
                   created_at, last_login
            FROM users
            ORDER BY last_login DESC NULLS LAST
        """)
        db_users = [dict(r) for r in cur.fetchall()]

        # All active sessions
        cur.execute("""
            SELECT st.user_id, u.email, u.name, st.last_used, st.ip_address
            FROM session_tokens st
            JOIN users u ON st.user_id = u.id
            WHERE st.expires_at > ?
            ORDER BY st.last_used DESC
        """, (now_utc.isoformat(),))
        sessions = [dict(r) for r in cur.fetchall()]

    # Count briefer jobs per user
    briefer_counts = {}
    for jf in JOBS_DIR.glob("*.json"):
        try:
            d = _read_json(jf)
            if d:
                em = (d.get("email") or "").lower()
                if em:
                    briefer_counts[em] = briefer_counts.get(em, 0) + 1
        except Exception:
            pass

    def parse_dt(s):
        if not s:
            return None
        try:
            return datetime.fromisoformat(s.replace("Z", "+00:00").replace("+00:00", ""))
        except Exception:
            return None

    def fmt_date(s):
        dt = parse_dt(s)
        if not dt:
            return "never"
        return dt.strftime("%b %d, %Y")

    def fmt_ago(s):
        dt = parse_dt(s)
        if not dt:
            return "never"
        diff = (now_utc - dt).total_seconds()
        if diff < 60:
            return f"{int(diff)}s ago"
        if diff < 3600:
            return f"{int(diff/60)}m ago"
        if diff < 86400:
            return f"{int(diff/3600)}h ago"
        return f"{int(diff/86400)}d ago"

    # Build online sessions list
    online_sessions = []
    online_user_ids = set()
    today_user_ids = set()
    week_user_ids = set()

    for s in sessions:
        last_used = parse_dt(s["last_used"])
        if last_used and last_used > online_cutoff:
            online_sessions.append({
                "name": s["name"] or s["email"],
                "email": s["email"],
                "ip": s["ip_address"] or "unknown",
                "last_active_ago": fmt_ago(s["last_used"]),
            })
            online_user_ids.add(s["user_id"])
        if last_used and last_used > today_cutoff:
            today_user_ids.add(s["user_id"])
        if last_used and last_used > week_cutoff:
            week_user_ids.add(s["user_id"])

    # Also check last_login for today/week counts
    for u in db_users:
        ll = parse_dt(u["last_login"])
        if ll and ll > today_cutoff:
            today_user_ids.add(u["id"])
        if ll and ll > week_cutoff:
            week_user_ids.add(u["id"])

    # Build user list
    users = []
    for u in db_users:
        is_online = u["id"] in online_user_ids
        active_today = u["id"] in today_user_ids
        status_class = "online" if is_online else "recent" if active_today else "inactive"
        users.append({
            "name": u["name"] or u["email"],
            "email": u["email"],
            "chamber": u["chamber"] or "",
            "can_view_house": u["can_view_house"],
            "can_view_senate": u["can_view_senate"],
            "created_fmt": fmt_date(u["created_at"]),
            "last_login_fmt": fmt_ago(u["last_login"]),
            "briefers_count": briefer_counts.get(u["email"].lower(), 0),
            "is_online": is_online,
            "active_today": active_today,
            "status_class": status_class,
        })

    return _get_templates().TemplateResponse("admin_activity.html", {
        "request": request,
        "online_count": len(online_user_ids),
        "today_count": len(today_user_ids),
        "week_count": len(week_user_ids),
        "total_users": len(db_users),
        "online_sessions": online_sessions,
        "users": users,
    })

# ── Admin Demo Briefer Generator ──


# ── Admin Demo Briefer Generator ──

@router.get("/admin/demo", response_class=HTMLResponse)
def admin_demo_page(request: Request):
    redir = _require_admin(request)
    if redir:
        return redir
    return _get_templates().TemplateResponse("admin_demo.html", {
        "request": request,
        "message": None,
        "error": None,
    })


@router.post("/admin/demo", response_class=HTMLResponse)
async def admin_demo_submit(request: Request):
    redir = _require_admin(request)
    if redir:
        return redir

    form = await request.form()
    csrf_token = form.get("csrf_token", "")
    if not _validate_csrf(request, csrf_token):
        return _get_templates().TemplateResponse("admin_demo.html", {
            "request": request, "message": None,
            "error": "Invalid session. Please refresh and try again.",
        })

    name = (form.get("demo_name") or "").strip()
    district_raw = (form.get("demo_district") or "").strip()
    bill_input = (form.get("bill") or "").strip()

    # Validate name
    if not name:
        return _get_templates().TemplateResponse("admin_demo.html", {
            "request": request, "message": None,
            "error": "Legislator name is required.",
        })

    # Validate district
    district_raw = district_raw.upper().replace("LD", "").strip()
    try:
        district_num = int(district_raw)
        if district_num < 1 or district_num > 35:
            raise ValueError
    except (ValueError, TypeError):
        return _get_templates().TemplateResponse("admin_demo.html", {
            "request": request, "message": None,
            "error": "District not found. Enter a number between 1 and 35.",
        })

    # Validate bill
    if not bill_input:
        return _get_templates().TemplateResponse("admin_demo.html", {
            "request": request, "message": None,
            "error": "Bill number is required.",
        })

    # Check demo rate limit (10/day)
    demo_jobs = list(JOBS_DIR.glob("*.json"))
    demo_today = 0
    cutoff = time.time() - 86400
    for jp in demo_jobs:
        try:
            jd = _read_json(jp)
            if jd and jd.get("is_demo") and jd.get("created_at", 0) > cutoff:
                demo_today += 1
        except Exception:
            pass
    if demo_today >= 10:
        return _get_templates().TemplateResponse("admin_demo.html", {
            "request": request, "message": None,
            "error": "Demo rate limit reached (10/day). Try again tomorrow.",
        })

    # Enqueue demo job
    user = _current_user(request)
    admin_email = user.get("email", "demo@billbriefer.com") if user else "demo@billbriefer.com"

    job_id = uuid.uuid4().hex[:12]
    session_year = DEFAULT_SESSION_YEAR
    session_id = SESSION_ID_MAP.get(session_year, SESSION_ID_MAP[DEFAULT_SESSION_YEAR])
    payload = {
        "job_id": job_id,
        "email": admin_email,
        "bill_input": _norm_text(bill_input),
        "recipient_line": "Legislative Briefing",
        "session_year": session_year,
        "session_id": session_id,
        "created_at": time.time(),
        "status": "queued",
        "error": None,
        "bill_resolved": None,
        "is_demo": True,
        "demo_name": name,
        "demo_district": district_num,
    }
    _write_json(_get_job_path(job_id), payload)
    logger.info(f"DEMO_JOB: {job_id} for {name}, LD{district_num:02d}, bill={bill_input}")

    return _get_templates().TemplateResponse("admin_demo.html", {
        "request": request,
        "message": f"Generating briefer for {name}, LD{district_num:02d}...",
        "error": None,
        "job_id": job_id,
        "demo_name": name,
        "demo_district": district_num,
    })


@router.get("/admin/demo/status/{job_id}")
def admin_demo_status(request: Request, job_id: str):
    redir = _require_admin(request)
    if redir:
        return JSONResponse({"error": "unauthorized"}, status_code=401)

    job_file = _get_job_path(job_id)
    if not job_file.exists():
        return JSONResponse({"error": "not found"}, status_code=404)

    job_data = _read_json(job_file)
    if not job_data or not job_data.get("is_demo"):
        return JSONResponse({"error": "not a demo job"}, status_code=404)

    return JSONResponse({
        "status": job_data.get("status"),
        "error": job_data.get("error"),
        "bill_resolved": job_data.get("bill_resolved"),
        "demo_name": job_data.get("demo_name", ""),
        "demo_district": job_data.get("demo_district"),
    })


@router.get("/admin/demo/download/{job_id}")
def admin_demo_download(request: Request, job_id: str):
    redir = _require_admin(request)
    if redir:
        return RedirectResponse(url="/admin/demo", status_code=302)

    job_file = _get_job_path(job_id)
    if not job_file.exists():
        return RedirectResponse(url="/admin/demo", status_code=302)

    job_data = _read_json(job_file)
    if not job_data or not job_data.get("is_demo"):
        return RedirectResponse(url="/admin/demo", status_code=302)

    pdf_path = Path(job_data.get("demo_pdf_path", ""))
    if not pdf_path.exists():
        return RedirectResponse(url="/admin/demo", status_code=302)

    bill = job_data.get("bill_resolved", "bill")
    demo_name = job_data.get("demo_name", "demo")
    filename = f"{bill}_{demo_name.replace(' ', '_')}_briefing.pdf"

    pdf_content = pdf_path.read_bytes()

    # Clean up: delete the PDF file after reading (it's a throwaway)
    try:
        pdf_path.unlink()
    except Exception:
        pass

    return Response(
        content=pdf_content,
        media_type="application/pdf",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )



# --- Admin management for admin emails ---
@router.get("/admin/admins", response_class=HTMLResponse)
def admin_admins_page(request: Request, message: str = "", message_type: str = ""):
    redir = _require_admin(request)
    if redir:
        return redir
    admins = sorted(_load_admin_allowlist())
    return _get_templates().TemplateResponse(
        "admin_admins.html",
        {"request": request, "admins": admins, "message": message, "message_type": message_type}
    )

@router.post("/admin/admins/add")
def admin_admins_add(request: Request, email: str = Form(...)):
    redir = _require_admin(request)
    if redir:
        return redir
    email_clean = email.strip().lower()
    if not email_clean or "@" not in email_clean:
        return RedirectResponse(url="/admin/admins?message=Invalid email&message_type=error", status_code=302)
    admins = _load_admin_allowlist()
    if email_clean in admins:
        return RedirectResponse(url="/admin/admins?message=Email already an admin&message_type=error", status_code=302)
    with open(ADMIN_ALLOWLIST_PATH, "a") as f:
        f.write(f"\n{email_clean}")
    return RedirectResponse(url="/admin/admins?message=Admin added successfully&message_type=success", status_code=302)

@router.post("/admin/admins/remove")
def admin_admins_remove(request: Request, email: str = Form(...)):
    redir = _require_admin(request)
    if redir:
        return redir
    email_clean = email.strip().lower()
    admins = _load_admin_allowlist()
    if email_clean not in admins:
        return RedirectResponse(url="/admin/admins?message=Email not found&message_type=error", status_code=302)
    # Don't allow removing yourself
    user = _current_user(request)
    user_email = _norm_text(user.get("email")).lower() if user else ""
    if user_email == email_clean:
        return RedirectResponse(url="/admin/admins?message=Cannot remove yourself&message_type=error", status_code=302)
    admins.discard(email_clean)
    with open(ADMIN_ALLOWLIST_PATH, "w") as f:
        f.write("\n".join(sorted(admins)))
    return RedirectResponse(url="/admin/admins?message=Admin removed successfully&message_type=success", status_code=302)

# -- Admin Ops Dashboard --------------------------------------------------

# -- Admin Ops Dashboard --------------------------------------------------

class RingBufferHandler(logging.Handler):
    """In-memory ring buffer that stores recent log entries."""
    def __init__(self, capacity=200):
        super().__init__()
        self._buf = collections.deque(maxlen=capacity)
        self._lock = threading.Lock()

    def emit(self, record):
        try:
            entry = {
                "ts": record.created,
                "iso": datetime.fromtimestamp(record.created, tz=BOISE_TZ).strftime("%Y-%m-%d %H:%M:%S"),
                "level": record.levelname,
                "msg": self.format(record),
            }
            with self._lock:
                self._buf.append(entry)
        except Exception:
            pass

    def get_recent(self, limit=100):
        with self._lock:
            items = list(self._buf)
        return items[-limit:]

log_buffer = RingBufferHandler(capacity=200)
log_buffer.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(name)s: %(message)s"))
logging.getLogger().addHandler(log_buffer)
logger.addHandler(log_buffer)


def _ops_get_system_status():
    now = time.time()
    one_hour_ago = now - 3600
    queued = processing = stuck = failed_1h = done_1h = 0
    for jf in JOBS_DIR.glob("*.json"):
        try:
            d = _read_json(jf)
            if not d:
                continue
            st = (d.get("status") or "").lower()
            if st == "queued":
                queued += 1
            elif st == "processing":
                if d.get("started_at") and (now - d["started_at"]) > JOB_TIMEOUT_MINUTES * 60:
                    stuck += 1
                else:
                    processing += 1
            elif st == "failed" and d.get("finished_at", 0) > one_hour_ago:
                failed_1h += 1
            elif st == "done" and d.get("finished_at", 0) > one_hour_ago:
                done_1h += 1
        except Exception:
            continue
    if stuck > 0 or failed_1h > 3:
        health, color = "critical", "#ef4444"
    elif failed_1h > 0 or queued > 5:
        health, color = "degraded", "#f59e0b"
    elif processing > 0 or queued > 0:
        health, color = "active", "#3b82f6"
    else:
        health, color = "healthy", "#22c55e"
    return {"health": health, "color": color, "queued": queued, "processing": processing,
            "stuck": stuck, "failed_1h": failed_1h, "done_1h": done_1h}


def _ops_format_time_ago(ts):
    if not ts:
        return "--"
    diff = time.time() - ts
    if diff < 60:
        return f"{int(diff)}s ago"
    if diff < 3600:
        return f"{int(diff/60)}m ago"
    if diff < 86400:
        return f"{int(diff/3600)}h ago"
    return f"{int(diff/86400)}d ago"


def _ops_get_recent_jobs(limit=20):
    jobs = []
    for jf in sorted(JOBS_DIR.glob("*.json"), key=lambda p: p.stat().st_mtime, reverse=True)[:limit]:
        try:
            d = _read_json(jf)
            if not d:
                continue
            created = d.get("created_at")
            started = d.get("started_at")
            finished = d.get("finished_at")
            wait_s = (started - created) if (started and created) else None
            proc_s = (finished - started) if (finished and started) else None
            raw_st = (d.get("status") or "unknown").lower()
            is_stuck = raw_st == "processing" and started and (time.time() - started) > JOB_TIMEOUT_MINUTES * 60
            effective_status = "stuck" if is_stuck else raw_st
            jobs.append({
                "job_id": d.get("job_id", ""),
                "bill": d.get("bill_resolved") or d.get("bill_input", "?"),
                "status": effective_status,
                "email": (d.get("email") or ""),
                "time_ago": _ops_format_time_ago(created),
                "wait_s": f"{wait_s:.0f}s" if wait_s is not None else "--",
                "proc_s": f"{proc_s:.0f}s" if proc_s is not None else "--",
                "cost": f"${d.get('ai_estimated_cost_usd', 0):.4f}" if d.get("ai_estimated_cost_usd") else "--",
                "error": (d.get("error") or "")[:120],
            })
        except Exception:
            continue
    return jobs


def _ops_retry_job(job_id: str) -> bool:
    path = _get_job_path(job_id)
    if not path.exists():
        return False
    d = _read_json(path)
    st = (d.get("status") or "").lower()
    if not d or st not in ("failed", "processing"):
        return False
    d["status"] = "queued"
    d["error"] = None
    d["started_at"] = None
    d["finished_at"] = None
    _write_json(path, d)
    logger.info(f"Admin retried job {job_id}")
    return True


@router.get("/admin/ops", response_class=HTMLResponse)
def admin_ops_page(request: Request):
    redir = _require_admin(request)
    if redir:
        return redir
    status = _ops_get_system_status()
    jobs = _ops_get_recent_jobs()
    return _get_templates().TemplateResponse("admin_ops.html", {"request": request, "status": status, "jobs": jobs})


@router.post("/admin/ops/retry-job")
def admin_ops_retry_job(request: Request, job_id: str = Form(...)):
    redir = _require_admin(request)
    if redir:
        return redir
    _ops_retry_job(job_id)
    return RedirectResponse(url="/admin/ops", status_code=302)



@router.get("/admin/ops/jobs")
def admin_ops_jobs_api(request: Request, limit: int = 20):
    redir = _require_admin(request)
    if redir:
        return JSONResponse([], status_code=401)
    jobs = _ops_get_recent_jobs(limit)
    return JSONResponse(jobs)


@router.get("/admin/ops/logs")
def admin_ops_logs(request: Request, limit: int = 100, level: str = ""):
    redir = _require_admin(request)
    if redir:
        return redir
    entries = log_buffer.get_recent(limit)
    if level:
        entries = [e for e in entries if e["level"] == level.upper()]
    return JSONResponse(content=entries)


