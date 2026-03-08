"""
auth_db.py
Authentication database schema and connection management for Idaho Bill Briefer.
Uses SQLite for simplicity and consistency with existing infrastructure.
"""

import sqlite3
import logging
from pathlib import Path
from datetime import datetime
from typing import Any, Dict, List, Optional
from contextlib import contextmanager

logger = logging.getLogger(__name__)

# Database path
DATA_DIR = Path("/app/data")
AUTH_DB_PATH = DATA_DIR / "auth.sqlite"


@contextmanager
def get_db_connection():
    """Get a database connection with proper cleanup."""
    conn = sqlite3.connect(str(AUTH_DB_PATH), timeout=30)
    conn.row_factory = sqlite3.Row
    try:
        yield conn
    finally:
        conn.close()


def init_auth_db():
    """Initialize the authentication database with all required tables."""
    with get_db_connection() as conn:
        cursor = conn.cursor()

        # Users table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS users (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                email TEXT UNIQUE NOT NULL,
                password_hash TEXT NOT NULL,
                name TEXT NOT NULL,
                district TEXT,
                chamber TEXT,
                party TEXT,

                -- Account status
                email_verified INTEGER DEFAULT 0,
                account_status TEXT DEFAULT 'active',
                locked_until TIMESTAMP,

                -- Timestamps
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                last_login TIMESTAMP,
                password_changed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)

        # MFA codes table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS mfa_codes (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER,
                email TEXT NOT NULL,
                code_hash TEXT NOT NULL,
                code_type TEXT NOT NULL,

                -- Expiration and usage
                expires_at TIMESTAMP NOT NULL,
                used INTEGER DEFAULT 0,
                used_at TIMESTAMP,
                attempts INTEGER DEFAULT 0,

                -- Tracking
                ip_address TEXT,
                user_agent TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

                FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE
            )
        """)

        # Session tokens table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS session_tokens (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER NOT NULL,
                token_hash TEXT UNIQUE NOT NULL,

                -- Device fingerprinting
                device_fingerprint TEXT,
                ip_address TEXT,
                user_agent TEXT,

                -- Expiration
                expires_at TIMESTAMP NOT NULL,
                last_used TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

                -- Timestamps
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

                FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE
            )
        """)

        # Login attempts table (for rate limiting)
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS login_attempts (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                email TEXT NOT NULL,
                user_id INTEGER,

                -- Attempt details
                success INTEGER NOT NULL,
                failure_reason TEXT,
                attempt_type TEXT DEFAULT 'password',

                -- Tracking
                ip_address TEXT,
                user_agent TEXT,
                timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

                FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE SET NULL
            )
        """)

        # Security events table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS security_events (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER,
                event_type TEXT NOT NULL,

                -- Event details
                description TEXT,
                ip_address TEXT,
                user_agent TEXT,

                -- Email notification
                email_sent INTEGER DEFAULT 0,
                email_sent_at TIMESTAMP,

                -- Timestamp
                timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

                FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE
            )
        """)

        # Trusted devices table (for "Remember this device" feature)
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS trusted_devices (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER NOT NULL,
                device_token_hash TEXT UNIQUE NOT NULL,
                device_name TEXT,
                ip_address TEXT,
                user_agent TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                last_used TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                expires_at TIMESTAMP NOT NULL,
                FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE
            )
        """)

        # Create indexes
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_users_email ON users(email)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_users_account_status ON users(account_status)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_mfa_codes_email ON mfa_codes(email)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_mfa_codes_expires_at ON mfa_codes(expires_at)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_session_tokens_user_id ON session_tokens(user_id)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_session_tokens_token_hash ON session_tokens(token_hash)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_session_tokens_expires_at ON session_tokens(expires_at)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_login_attempts_email ON login_attempts(email)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_login_attempts_timestamp ON login_attempts(timestamp)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_login_attempts_ip_address ON login_attempts(ip_address)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_security_events_user_id ON security_events(user_id)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_security_events_timestamp ON security_events(timestamp)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_trusted_devices_user_id ON trusted_devices(user_id)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_trusted_devices_token_hash ON trusted_devices(device_token_hash)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_trusted_devices_expires_at ON trusted_devices(expires_at)")

        conn.commit()
        logger.info("Auth database initialized successfully")


# ============================================================================
# USER OPERATIONS
# ============================================================================

def create_user(
    email: str,
    password_hash: str,
    name: str,
    district: Optional[str] = None,
    chamber: Optional[str] = None,
    party: Optional[str] = None,
    email_verified: bool = True
) -> Optional[int]:
    """Create a new user and return user_id."""
    with get_db_connection() as conn:
        cursor = conn.cursor()
        try:
            cursor.execute("""
                INSERT INTO users (email, password_hash, name, district, chamber, party, email_verified)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            """, (email.lower(), password_hash, name, district, chamber, party, 1 if email_verified else 0))
            conn.commit()
            return cursor.lastrowid
        except sqlite3.IntegrityError:
            logger.warning(f"User already exists: {email}")
            return None


def get_user_by_email(email: str) -> Optional[Dict[str, Any]]:
    """Get user by email address."""
    with get_db_connection() as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM users WHERE email = ?", (email.lower(),))
        row = cursor.fetchone()
        return dict(row) if row else None


def get_user_by_id(user_id: int) -> Optional[Dict[str, Any]]:
    """Get user by ID."""
    with get_db_connection() as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM users WHERE id = ?", (user_id,))
        row = cursor.fetchone()
        return dict(row) if row else None


def update_user_last_login(user_id: int) -> None:
    """Update user's last login timestamp."""
    with get_db_connection() as conn:
        cursor = conn.cursor()
        cursor.execute(
            "UPDATE users SET last_login = CURRENT_TIMESTAMP WHERE id = ?",
            (user_id,)
        )
        conn.commit()


def update_user_password(user_id: int, password_hash: str) -> None:
    """Update user's password hash."""
    with get_db_connection() as conn:
        cursor = conn.cursor()
        cursor.execute("""
            UPDATE users
            SET password_hash = ?, password_changed_at = CURRENT_TIMESTAMP
            WHERE id = ?
        """, (password_hash, user_id))
        conn.commit()


def update_user_status(user_id: int, status: str, locked_until: Optional[datetime] = None) -> None:
    """Update user account status."""
    with get_db_connection() as conn:
        cursor = conn.cursor()
        cursor.execute("""
            UPDATE users
            SET account_status = ?, locked_until = ?
            WHERE id = ?
        """, (status, locked_until, user_id))
        conn.commit()


def check_user_locked(user_id: int) -> tuple[bool, Optional[int]]:
    """
    Check if user account is locked.
    Returns: (is_locked, minutes_remaining)
    """
    user = get_user_by_id(user_id)
    if not user:
        return False, None

    if user['account_status'] != 'locked':
        return False, None

    if user['locked_until']:
        locked_until = datetime.fromisoformat(user['locked_until']) if isinstance(user['locked_until'], str) else user['locked_until']
        now = datetime.utcnow()
        if locked_until > now:
            minutes_remaining = int((locked_until - now).total_seconds() / 60)
            return True, minutes_remaining
        else:
            # Automatically unlock
            update_user_status(user_id, 'active', None)
            return False, None

    return True, None


# ============================================================================
# MFA CODE OPERATIONS
# ============================================================================

def create_mfa_code(
    email: str,
    code_hash: str,
    code_type: str,
    expires_at: datetime,
    user_id: Optional[int] = None,
    ip_address: Optional[str] = None,
    user_agent: Optional[str] = None
) -> int:
    """Create a new MFA code and return code_id."""
    with get_db_connection() as conn:
        cursor = conn.cursor()
        cursor.execute("""
            INSERT INTO mfa_codes (user_id, email, code_hash, code_type, expires_at, ip_address, user_agent)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """, (user_id, email.lower(), code_hash, code_type, expires_at, ip_address, user_agent))
        conn.commit()
        return cursor.lastrowid


def get_mfa_code(code_id: int) -> Optional[Dict[str, Any]]:
    """Get MFA code by ID."""
    with get_db_connection() as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM mfa_codes WHERE id = ?", (code_id,))
        row = cursor.fetchone()
        return dict(row) if row else None


def get_latest_mfa_code(email: str, code_type: str) -> Optional[Dict[str, Any]]:
    """Get the latest unused MFA code for an email and type."""
    with get_db_connection() as conn:
        cursor = conn.cursor()
        cursor.execute("""
            SELECT * FROM mfa_codes
            WHERE email = ? AND code_type = ? AND used = 0 AND expires_at > CURRENT_TIMESTAMP
            ORDER BY created_at DESC LIMIT 1
        """, (email.lower(), code_type))
        row = cursor.fetchone()
        return dict(row) if row else None


def mark_mfa_code_used(code_id: int) -> None:
    """Mark MFA code as used."""
    with get_db_connection() as conn:
        cursor = conn.cursor()
        cursor.execute("""
            UPDATE mfa_codes
            SET used = 1, used_at = CURRENT_TIMESTAMP
            WHERE id = ?
        """, (code_id,))
        conn.commit()


def increment_mfa_attempts(code_id: int) -> int:
    """Increment MFA code attempts and return new count."""
    with get_db_connection() as conn:
        cursor = conn.cursor()
        cursor.execute("""
            UPDATE mfa_codes
            SET attempts = attempts + 1
            WHERE id = ?
        """, (code_id,))
        conn.commit()
        cursor.execute("SELECT attempts FROM mfa_codes WHERE id = ?", (code_id,))
        row = cursor.fetchone()
        return row['attempts'] if row else 0


def invalidate_mfa_codes(email: str, code_type: str) -> None:
    """Invalidate all MFA codes for an email and type."""
    with get_db_connection() as conn:
        cursor = conn.cursor()
        cursor.execute("""
            UPDATE mfa_codes
            SET used = 1
            WHERE email = ? AND code_type = ? AND used = 0
        """, (email.lower(), code_type))
        conn.commit()


def count_recent_mfa_requests(email: str, code_type: str, minutes: int = 15) -> int:
    """Count recent MFA code requests for rate limiting."""
    with get_db_connection() as conn:
        cursor = conn.cursor()
        cursor.execute("""
            SELECT COUNT(*) as count FROM mfa_codes
            WHERE email = ? AND code_type = ?
            AND created_at > datetime('now', ?)
        """, (email.lower(), code_type, f'-{minutes} minutes'))
        row = cursor.fetchone()
        return row['count'] if row else 0


def cleanup_expired_mfa_codes() -> int:
    """Delete MFA codes older than 24 hours. Returns count deleted."""
    with get_db_connection() as conn:
        cursor = conn.cursor()
        cursor.execute("""
            DELETE FROM mfa_codes
            WHERE created_at < datetime('now', '-24 hours')
        """)
        conn.commit()
        return cursor.rowcount


# ============================================================================
# SESSION TOKEN OPERATIONS
# ============================================================================

def create_session_token(
    user_id: int,
    token_hash: str,
    expires_at: datetime,
    device_fingerprint: Optional[str] = None,
    ip_address: Optional[str] = None,
    user_agent: Optional[str] = None
) -> int:
    """Create a new session token and return token_id."""
    with get_db_connection() as conn:
        cursor = conn.cursor()
        cursor.execute("""
            INSERT INTO session_tokens (user_id, token_hash, expires_at, device_fingerprint, ip_address, user_agent)
            VALUES (?, ?, ?, ?, ?, ?)
        """, (user_id, token_hash, expires_at, device_fingerprint, ip_address, user_agent))
        conn.commit()
        return cursor.lastrowid


def get_session_by_token_hash(token_hash: str) -> Optional[Dict[str, Any]]:
    """Get session by token hash."""
    with get_db_connection() as conn:
        cursor = conn.cursor()
        cursor.execute("""
            SELECT st.*, u.email, u.name, u.district, u.chamber, u.account_status
            FROM session_tokens st
            JOIN users u ON st.user_id = u.id
            WHERE st.token_hash = ?
        """, (token_hash,))
        row = cursor.fetchone()
        return dict(row) if row else None


def update_session_last_used(token_hash: str) -> None:
    """Update session's last used timestamp."""
    with get_db_connection() as conn:
        cursor = conn.cursor()
        cursor.execute("""
            UPDATE session_tokens
            SET last_used = CURRENT_TIMESTAMP
            WHERE token_hash = ?
        """, (token_hash,))
        conn.commit()


def delete_session_token(token_hash: str) -> None:
    """Delete a session token (logout)."""
    with get_db_connection() as conn:
        cursor = conn.cursor()
        cursor.execute("DELETE FROM session_tokens WHERE token_hash = ?", (token_hash,))
        conn.commit()


def delete_all_user_sessions(user_id: int, except_token_hash: Optional[str] = None) -> int:
    """Delete all sessions for a user, optionally except one. Returns count deleted."""
    with get_db_connection() as conn:
        cursor = conn.cursor()
        if except_token_hash:
            cursor.execute("""
                DELETE FROM session_tokens
                WHERE user_id = ? AND token_hash != ?
            """, (user_id, except_token_hash))
        else:
            cursor.execute("DELETE FROM session_tokens WHERE user_id = ?", (user_id,))
        conn.commit()
        return cursor.rowcount


def get_user_sessions(user_id: int) -> List[Dict[str, Any]]:
    """Get all active sessions for a user."""
    with get_db_connection() as conn:
        cursor = conn.cursor()
        cursor.execute("""
            SELECT * FROM session_tokens
            WHERE user_id = ? AND expires_at > CURRENT_TIMESTAMP
            ORDER BY last_used DESC
        """, (user_id,))
        return [dict(row) for row in cursor.fetchall()]


def is_known_device(user_id: int, device_fingerprint: str) -> bool:
    """Check if this device fingerprint has been seen before for this user."""
    if not device_fingerprint:
        return True  # Don't alert for unknown fingerprints

    with get_db_connection() as conn:
        cursor = conn.cursor()
        cursor.execute('''
            SELECT COUNT(*) FROM session_tokens
            WHERE user_id = ? AND device_fingerprint = ?
        ''', (user_id, device_fingerprint))
        count = cursor.fetchone()[0]
        return count > 0


def get_approximate_location(ip_address: str) -> str:
    """Get approximate location from IP address (placeholder)."""
    # In production, you would use a GeoIP service
    # For now, return a placeholder
    if not ip_address:
        return "Unknown location"
    return f"IP: {ip_address}"


def cleanup_expired_sessions() -> int:
    """Delete expired session tokens. Returns count deleted."""
    with get_db_connection() as conn:
        cursor = conn.cursor()
        cursor.execute("DELETE FROM session_tokens WHERE expires_at < CURRENT_TIMESTAMP")
        conn.commit()
        return cursor.rowcount


# ============================================================================
# LOGIN ATTEMPTS (Rate Limiting)
# ============================================================================

def log_login_attempt(
    email: str,
    success: bool,
    failure_reason: Optional[str] = None,
    attempt_type: str = 'password',
    user_id: Optional[int] = None,
    ip_address: Optional[str] = None,
    user_agent: Optional[str] = None
) -> None:
    """Log a login attempt."""
    with get_db_connection() as conn:
        cursor = conn.cursor()
        cursor.execute("""
            INSERT INTO login_attempts (email, user_id, success, failure_reason, attempt_type, ip_address, user_agent)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """, (email.lower(), user_id, 1 if success else 0, failure_reason, attempt_type, ip_address, user_agent))
        conn.commit()


def count_failed_attempts(email: str, minutes: int = 15) -> int:
    """Count failed login attempts for an email in recent minutes."""
    with get_db_connection() as conn:
        cursor = conn.cursor()
        cursor.execute("""
            SELECT COUNT(*) as count FROM login_attempts
            WHERE email = ? AND success = 0
            AND timestamp > datetime('now', ?)
        """, (email.lower(), f'-{minutes} minutes'))
        row = cursor.fetchone()
        return row['count'] if row else 0


def count_failed_attempts_by_ip(ip_address: str, minutes: int = 15) -> int:
    """Count failed login attempts from an IP in recent minutes."""
    with get_db_connection() as conn:
        cursor = conn.cursor()
        cursor.execute("""
            SELECT COUNT(*) as count FROM login_attempts
            WHERE ip_address = ? AND success = 0
            AND timestamp > datetime('now', ?)
        """, (ip_address, f'-{minutes} minutes'))
        row = cursor.fetchone()
        return row['count'] if row else 0


def cleanup_old_login_attempts(days: int = 90) -> int:
    """Archive/delete login attempts older than specified days."""
    with get_db_connection() as conn:
        cursor = conn.cursor()
        cursor.execute(f"""
            DELETE FROM login_attempts
            WHERE timestamp < datetime('now', '-{days} days')
        """)
        conn.commit()
        return cursor.rowcount


# ============================================================================
# SECURITY EVENTS
# ============================================================================

def log_security_event(
    event_type: str,
    user_id: Optional[int] = None,
    description: Optional[str] = None,
    ip_address: Optional[str] = None,
    user_agent: Optional[str] = None
) -> int:
    """Log a security event and return event_id."""
    with get_db_connection() as conn:
        cursor = conn.cursor()
        cursor.execute("""
            INSERT INTO security_events (user_id, event_type, description, ip_address, user_agent)
            VALUES (?, ?, ?, ?, ?)
        """, (user_id, event_type, description, ip_address, user_agent))
        conn.commit()
        return cursor.lastrowid


def mark_security_event_emailed(event_id: int) -> None:
    """Mark security event as having email notification sent."""
    with get_db_connection() as conn:
        cursor = conn.cursor()
        cursor.execute("""
            UPDATE security_events
            SET email_sent = 1, email_sent_at = CURRENT_TIMESTAMP
            WHERE id = ?
        """, (event_id,))
        conn.commit()


def get_recent_security_events(user_id: int, limit: int = 20) -> List[Dict[str, Any]]:
    """Get recent security events for a user."""
    with get_db_connection() as conn:
        cursor = conn.cursor()
        cursor.execute("""
            SELECT * FROM security_events
            WHERE user_id = ?
            ORDER BY timestamp DESC LIMIT ?
        """, (user_id, limit))
        return [dict(row) for row in cursor.fetchall()]


# ============================================================================
# TRUSTED DEVICES
# ============================================================================

def create_trusted_device(
    user_id: int,
    device_token_hash: str,
    expires_at: datetime,
    device_name: Optional[str] = None,
    ip_address: Optional[str] = None,
    user_agent: Optional[str] = None
) -> int:
    """Create a trusted device record. Returns the device ID."""
    with get_db_connection() as conn:
        cursor = conn.cursor()
        cursor.execute("""
            INSERT INTO trusted_devices
            (user_id, device_token_hash, device_name, ip_address, user_agent, expires_at)
            VALUES (?, ?, ?, ?, ?, ?)
        """, (user_id, device_token_hash, device_name, ip_address, user_agent, expires_at))
        conn.commit()
        return cursor.lastrowid


def get_trusted_device(device_token_hash: str) -> Optional[Dict[str, Any]]:
    """Get trusted device by token hash, including user info."""
    with get_db_connection() as conn:
        cursor = conn.cursor()
        cursor.execute("""
            SELECT td.*, u.email, u.account_status
            FROM trusted_devices td
            JOIN users u ON td.user_id = u.id
            WHERE td.device_token_hash = ? AND td.expires_at > CURRENT_TIMESTAMP
        """, (device_token_hash,))
        row = cursor.fetchone()
        return dict(row) if row else None


def update_trusted_device_last_used(device_id: int) -> None:
    """Update last_used timestamp for trusted device."""
    with get_db_connection() as conn:
        cursor = conn.cursor()
        cursor.execute("""
            UPDATE trusted_devices
            SET last_used = CURRENT_TIMESTAMP
            WHERE id = ?
        """, (device_id,))
        conn.commit()


def delete_trusted_device(device_token_hash: str) -> None:
    """Delete a trusted device by token hash."""
    with get_db_connection() as conn:
        cursor = conn.cursor()
        cursor.execute("DELETE FROM trusted_devices WHERE device_token_hash = ?", (device_token_hash,))
        conn.commit()


def delete_all_user_trusted_devices(user_id: int) -> int:
    """Delete all trusted devices for a user. Returns count deleted."""
    with get_db_connection() as conn:
        cursor = conn.cursor()
        cursor.execute("DELETE FROM trusted_devices WHERE user_id = ?", (user_id,))
        conn.commit()
        return cursor.rowcount


def get_user_trusted_devices(user_id: int) -> List[Dict[str, Any]]:
    """Get all trusted devices for a user."""
    with get_db_connection() as conn:
        cursor = conn.cursor()
        cursor.execute("""
            SELECT * FROM trusted_devices
            WHERE user_id = ? AND expires_at > CURRENT_TIMESTAMP
            ORDER BY last_used DESC
        """, (user_id,))
        return [dict(row) for row in cursor.fetchall()]


def cleanup_expired_trusted_devices() -> int:
    """Remove expired trusted devices. Returns count deleted."""
    with get_db_connection() as conn:
        cursor = conn.cursor()
        cursor.execute("DELETE FROM trusted_devices WHERE expires_at < CURRENT_TIMESTAMP")
        conn.commit()
        return cursor.rowcount


# ============================================================================
# DATABASE MAINTENANCE
# ============================================================================


def run_cleanup_jobs() -> Dict[str, int]:
    """Run all cleanup jobs. Returns counts of items cleaned up."""
    return {
        'expired_mfa_codes': cleanup_expired_mfa_codes(),
        'expired_sessions': cleanup_expired_sessions(),
        'old_login_attempts': cleanup_old_login_attempts(90),
        'expired_trusted_devices': cleanup_expired_trusted_devices(),
    }


# Initialize database on import
if __name__ == "__main__":
    init_auth_db()
    print("Auth database initialized!")
