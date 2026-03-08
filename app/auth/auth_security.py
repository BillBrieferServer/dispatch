"""
auth_security.py
Security functions for Idaho Bill Briefer authentication.
Handles password hashing, MFA code generation, session tokens, and validation.
"""

import secrets
import hashlib
import re
import logging
from datetime import datetime, timedelta
from typing import Tuple

import bcrypt

logger = logging.getLogger(__name__)


# ============================================================================
# PASSWORD SECURITY
# ============================================================================

def hash_password(password: str) -> str:
    """
    Hash password with bcrypt.
    Uses cost factor 12 for security/performance balance.
    """
    salt = bcrypt.gensalt(rounds=12)
    password_hash = bcrypt.hashpw(password.encode('utf-8'), salt)
    return password_hash.decode('utf-8')


def verify_password(password: str, password_hash: str) -> bool:
    """Verify password against bcrypt hash."""
    try:
        return bcrypt.checkpw(
            password.encode('utf-8'),
            password_hash.encode('utf-8')
        )
    except Exception as e:
        logger.error(f"Password verification error: {e}")
        return False


def validate_password(password: str) -> Tuple[bool, str]:
    """
    Validate password meets requirements.
    Returns: (is_valid, error_message)
    """
    if len(password) < 8:
        return False, "Password must be at least 8 characters"

    has_letter = any(c.isalpha() for c in password)
    has_number = any(c.isdigit() for c in password)

    if not (has_letter and has_number):
        return False, "Password must contain at least one letter and one number"

    return True, ""


# ============================================================================
# MFA CODE SECURITY
# ============================================================================

def generate_mfa_code(expiry_minutes: int = 10) -> Tuple[str, str, datetime]:
    """
    Generate MFA code.
    Returns: (plain_code, hashed_code, expiration_time)
    """
    # Generate 6-digit code using cryptographically secure random
    code_int = secrets.randbelow(1000000)
    plain_code = f"{code_int:06d}"  # Pad with zeros: "000123"

    # Hash the code with SHA-256
    hashed_code = hashlib.sha256(plain_code.encode()).hexdigest()

    # Set expiration
    expires_at = datetime.utcnow() + timedelta(minutes=expiry_minutes)

    return plain_code, hashed_code, expires_at


def verify_mfa_code(submitted_code: str, stored_hash: str) -> bool:
    """Verify MFA code against stored hash using timing-safe comparison."""
    # Clean and validate input
    submitted_code = submitted_code.strip()
    if not submitted_code.isdigit() or len(submitted_code) != 6:
        return False

    submitted_hash = hashlib.sha256(submitted_code.encode()).hexdigest()
    return secrets.compare_digest(submitted_hash, stored_hash)


def validate_mfa_code_format(code: str) -> Tuple[bool, str]:
    """
    Validate MFA code format.
    Returns: (is_valid, error_message)
    """
    code = code.strip()

    if not code.isdigit():
        return False, "Code must contain only numbers"

    if len(code) != 6:
        return False, "Code must be exactly 6 digits"

    return True, ""


# ============================================================================
# SESSION TOKEN SECURITY
# ============================================================================

def generate_session_token(remember_days: int = 7) -> Tuple[str, str, datetime]:
    """
    Generate session token.
    Returns: (plain_token, hashed_token, expiration_time)
    """
    # Generate 32-byte secure random token
    plain_token = secrets.token_urlsafe(32)

    # Hash the token
    hashed_token = hashlib.sha256(plain_token.encode()).hexdigest()

    # Set expiration
    expires_at = datetime.utcnow() + timedelta(days=remember_days)

    return plain_token, hashed_token, expires_at


def hash_token(token: str) -> str:
    """Hash a token with SHA-256."""
    return hashlib.sha256(token.encode()).hexdigest()


def verify_session_token(submitted_token: str, stored_hash: str) -> bool:
    """Verify session token against stored hash using timing-safe comparison."""
    submitted_hash = hashlib.sha256(submitted_token.encode()).hexdigest()
    return secrets.compare_digest(submitted_hash, stored_hash)


def generate_device_fingerprint(
    user_agent: str,
    accept_language: str = "",
    screen_info: str = "",
    timezone: str = ""
) -> str:
    """
    Generate device fingerprint from request components.
    Returns SHA-256 hash of combined components.
    """
    components = [
        user_agent or '',
        accept_language or '',
        screen_info or '',
        timezone or '',
    ]
    fingerprint_string = '|'.join(components)
    return hashlib.sha256(fingerprint_string.encode()).hexdigest()


# ============================================================================
# EMAIL VALIDATION
# ============================================================================

def validate_email(email: str) -> Tuple[bool, str]:
    """
    Validate email address.
    Returns: (is_valid, error_message)
    """
    email = email.strip().lower()

    # Basic format check
    email_regex = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
    if not re.match(email_regex, email):
        return False, "Invalid email format"

    # Domain check removed - open registration
    # Length check
    if len(email) > 255:
        return False, "Email address too long"

    return True, ""


def is_legislative_email(email: str) -> bool:
    """Check if email is an official Idaho Legislature email."""
    email = email.strip().lower()
    legislative_domains = [
        '@legislature.idaho.gov',
        '@house.idaho.gov',
        '@senate.idaho.gov',
    ]
    return any(email.endswith(domain) for domain in legislative_domains)


# ============================================================================
# RATE LIMITING HELPERS
# ============================================================================

def check_rate_limit(
    current_count: int,
    limit: int,
    window_minutes: int
) -> Tuple[bool, int, int]:
    """
    Check rate limit.
    Returns: (is_allowed, attempts_remaining, window_minutes)
    """
    is_allowed = current_count < limit
    attempts_remaining = max(0, limit - current_count)
    return is_allowed, attempts_remaining, window_minutes


# Rate limit configurations
RATE_LIMITS = {
    'login_per_email': {'limit': 5, 'window_minutes': 15},
    'login_per_ip': {'limit': 10, 'window_minutes': 15},
    'mfa_request_per_email': {'limit': 3, 'window_minutes': 15},
    'mfa_verify_per_code': {'limit': 5, 'window_minutes': 15},
    'password_reset_per_email': {'limit': 3, 'window_minutes': 1440},  # 24 hours
    'password_reset_per_ip': {'limit': 5, 'window_minutes': 60},
}


# ============================================================================
# ACCOUNT LOCKOUT
# ============================================================================

def calculate_lockout_duration(consecutive_lockouts: int = 1) -> int:
    """
    Calculate lockout duration in minutes based on consecutive lockouts.
    First lockout: 15 minutes
    Second lockout (within 24 hours): 1 hour
    Third+ lockout (within 24 hours): 4 hours
    """
    if consecutive_lockouts <= 1:
        return 15
    elif consecutive_lockouts == 2:
        return 60
    else:
        return 240


# ============================================================================
# UTILITY FUNCTIONS
# ============================================================================

def mask_email(email: str) -> str:
    """Mask email for display (e.g., "j***@legislature.idaho.gov")."""
    if not email or '@' not in email:
        return '***'

    local, domain = email.split('@', 1)
    if len(local) <= 2:
        masked_local = local[0] + '***'
    else:
        masked_local = local[0] + '***' + local[-1]

    return f"{masked_local}@{domain}"


def generate_secure_random_string(length: int = 32) -> str:
    """Generate a secure random string."""
    return secrets.token_urlsafe(length)
