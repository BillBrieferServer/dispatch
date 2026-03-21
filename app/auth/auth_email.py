"""
auth_email.py
Email functions for Idaho Bill Briefer authentication.
Sends plain text emails via IONOS SMTP (no links - Idaho firewall requirement).
"""

import os
import smtplib
import logging
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from datetime import datetime
from typing import Optional, Tuple

logger = logging.getLogger(__name__)

from app.branding import ORG_NAME
def _org():
    return ORG_NAME


# SMTP Configuration from environment
SMTP_HOST = os.getenv('SMTP_HOST', 'smtp.ionos.com')
SMTP_PORT = int(os.getenv('SMTP_PORT', '587'))
SMTP_USER = os.getenv('SMTP_USER', 'briefers@billbriefer.com')
SMTP_PASS = os.getenv('SMTP_PASS', '')
SMTP_FROM_NAME = _org()
SMTP_FROM_EMAIL = os.getenv('FROM_EMAIL', SMTP_USER)

# Site domain for use in email text (derived from APP_BASE_URL)
APP_BASE_URL = os.getenv('APP_BASE_URL', 'https://sand.billbriefer.com')
SITE_DOMAIN = APP_BASE_URL.replace('https://', '').replace('http://', '').rstrip('/')

# Support contact info
SUPPORT_PHONE = os.getenv('SUPPORT_PHONE', '')
SUPPORT_EMAIL = SMTP_USER


def send_email(to_email: str, subject: str, body: str, max_retries: int = 3) -> Tuple[bool, str]:
    """
    Send plain text email via IONOS SMTP.
    Returns: (success, message)
    """
    if not SMTP_PASS:
        logger.error("SMTP_PASS not configured")
        return False, "Email configuration error"

    msg = MIMEMultipart()
    msg['From'] = f'{SMTP_FROM_NAME} <{SMTP_FROM_EMAIL}>'
    msg['To'] = to_email
    msg['Subject'] = subject
    msg['Reply-To'] = 'info@billbriefer.com'

    # Attach plain text body (NO HTML - firewall requirement)
    msg.attach(MIMEText(body, 'plain'))

    last_error = ""
    for attempt in range(max_retries):
        try:
            server = smtplib.SMTP(SMTP_HOST, SMTP_PORT, timeout=30)
            server.starttls()
            server.login(SMTP_USER, SMTP_PASS)
            server.send_message(msg)
            server.quit()

            logger.info(f"Email sent successfully to {to_email}: {subject}")
            return True, "Email sent successfully"

        except smtplib.SMTPAuthenticationError as e:
            last_error = f"SMTP authentication failed: {e}"
            logger.error(last_error)
            break  # Don't retry auth errors

        except smtplib.SMTPException as e:
            last_error = f"SMTP error: {e}"
            logger.warning(f"Email attempt {attempt + 1} failed: {last_error}")

        except Exception as e:
            last_error = f"Email send error: {e}"
            logger.warning(f"Email attempt {attempt + 1} failed: {last_error}")

    logger.error(f"Email failed after {max_retries} attempts to {to_email}: {last_error}")
    return False, last_error


# ============================================================================
# EMAIL TEMPLATES
# ============================================================================

def send_signup_verification_code(to_email: str, code: str) -> Tuple[bool, str]:
    """Send signup verification code email."""
    subject = f"Your {_org()} verification code"

    body = f"""Your verification code is: {code}

This code will expire in 15 minutes.

Enter this code on the {_org()} website to complete your registration.

If you didn't request this code, please ignore this email.

Questions? Reply to this email{f' or call {SUPPORT_PHONE}' if SUPPORT_PHONE else ''}

---
{_org()}
A service of Quiet Impact"""

    return send_email(to_email, subject, body)


def send_welcome_email(to_email: str, name: str, district: str) -> Tuple[bool, str]:
    """Send welcome email after successful registration."""
    subject = f"Welcome to {_org()}"

    body = f"""Welcome, {name}!

Your {_org()} account is now active.

To get started, log in at {SITE_DOMAIN} and select bills you'd like briefers for from your dashboard.

Your account includes:
- AI-powered bill analysis
- District-specific impact analysis for {district}
- Debate preparation materials
- Committee question suggestions

Need help getting started? Reply to this email{f' or call {SUPPORT_PHONE}' if SUPPORT_PHONE else ''}

---
{_org()}
A service of Quiet Impact"""

    return send_email(to_email, subject, body)


def send_login_mfa_code(to_email: str, code: str) -> Tuple[bool, str]:
    """Send login MFA verification code email."""
    subject = f"{_org()} login verification"

    body = f"""Your login verification code is: {code}

This code will expire in 10 minutes.

If you didn't attempt to log in, please change your password immediately by visiting {SITE_DOMAIN} and clicking "Forgot Password".

Questions? Reply to this email{f' or call {SUPPORT_PHONE}' if SUPPORT_PHONE else ''}

---
{_org()}
A service of Quiet Impact"""

    return send_email(to_email, subject, body)


def send_password_reset_code(to_email: str, code: str) -> Tuple[bool, str]:
    """Send password reset verification code email."""
    subject = f"Reset your {_org()} password"

    body = f"""Your password reset code is: {code}

This code will expire in 10 minutes.

Enter this code on the {_org()} website to reset your password.

If you didn't request a password reset, please ignore this email. Your password will remain unchanged.

Questions? Reply to this email{f' or call {SUPPORT_PHONE}' if SUPPORT_PHONE else ''}

---
{_org()}
A service of Quiet Impact"""

    return send_email(to_email, subject, body)


def send_password_changed_notification(to_email: str) -> Tuple[bool, str]:
    """Send password changed notification email."""
    subject = f"Your {_org()} password was changed"

    now = datetime.now()
    date_str = now.strftime("%B %d, %Y")
    time_str = now.strftime("%I:%M %p")

    body = f"""Your {_org()} password was successfully changed on {date_str} at {time_str}.

All active sessions have been logged out for security. You'll need to log in again with your new password.

If you didn't make this change, please contact us immediately{f' at {SUPPORT_PHONE}' if SUPPORT_PHONE else ''} or reply to this email.

---
{_org()}
A service of Quiet Impact"""

    return send_email(to_email, subject, body)


def send_new_device_login_alert(
    to_email: str,
    device_info: str,
    location: str,
    login_time: Optional[datetime] = None
) -> Tuple[bool, str]:
    """Send new device login alert email."""
    subject = f"New login to your {_org()} account"

    if login_time is None:
        login_time = datetime.now()

    date_str = login_time.strftime("%B %d, %Y")
    time_str = login_time.strftime("%I:%M %p")

    body = f"""A new login was detected on your {_org()} account:

Date/Time: {date_str} at {time_str}
Device: {device_info}
Location: {location} (approximate)

If this was you, no action is needed.

If this wasn't you, please secure your account immediately:
1. Visit {SITE_DOMAIN}
2. Click "Forgot Password" to reset your password
3. Contact us{f' at {SUPPORT_PHONE}' if SUPPORT_PHONE else ''}

---
{_org()}
A service of Quiet Impact"""

    return send_email(to_email, subject, body)


def send_account_locked_notification(to_email: str, lock_minutes: int) -> Tuple[bool, str]:
    """Send account locked notification email."""
    subject = f"Security Alert - {_org()} account temporarily locked"

    body = f"""Your {_org()} account has been temporarily locked due to multiple failed login attempts.

The account will unlock automatically in {lock_minutes} minutes.

If you didn't make these attempts, your password may have been compromised. After the lockout period, we recommend:
1. Changing your password immediately
2. Reviewing your recent account activity

Questions or concerns? Reply to this email{f' or call {SUPPORT_PHONE}' if SUPPORT_PHONE else ''}

---
{_org()}
A service of Quiet Impact"""

    return send_email(to_email, subject, body)


# ============================================================================
# EMAIL TESTING
# ============================================================================

def test_email_configuration() -> Tuple[bool, str]:
    """Test SMTP configuration by sending a test email."""
    if not SMTP_PASS:
        return False, "SMTP_PASS not configured"

    try:
        server = smtplib.SMTP(SMTP_HOST, SMTP_PORT, timeout=30)
        server.starttls()
        server.login(SMTP_USER, SMTP_PASS)
        server.quit()
        return True, "SMTP configuration valid"
    except Exception as e:
        return False, f"SMTP configuration error: {e}"


if __name__ == "__main__":
    # Test configuration
    success, message = test_email_configuration()
    print(f"SMTP Test: {message}")
