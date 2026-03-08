"""
auth module for Idaho Bill Briefer
Provides authentication, MFA, and session management.
"""

from .auth_db import (
    init_auth_db,
    get_db_connection,
    # User operations
    create_user,
    get_user_by_email,
    get_user_by_id,
    update_user_last_login,
    update_user_password,
    update_user_status,
    check_user_locked,
    # MFA operations
    create_mfa_code,
    get_mfa_code,
    get_latest_mfa_code,
    mark_mfa_code_used,
    increment_mfa_attempts,
    invalidate_mfa_codes,
    count_recent_mfa_requests,
    # Session operations
    create_session_token,
    get_session_by_token_hash,
    update_session_last_used,
    delete_session_token,
    delete_all_user_sessions,
    get_user_sessions,
    is_known_device,
    get_approximate_location,
    # Login attempts
    log_login_attempt,
    count_failed_attempts,
    count_failed_attempts_by_ip,
    # Security events
    log_security_event,
    mark_security_event_emailed,
    get_recent_security_events,
    # Cleanup
    run_cleanup_jobs,
    # Trusted devices
    create_trusted_device,
    get_trusted_device,
    update_trusted_device_last_used,
    delete_trusted_device,
    delete_all_user_trusted_devices,
    get_user_trusted_devices,
    cleanup_expired_trusted_devices,
)

from .auth_security import (
    # Password
    hash_password,
    verify_password,
    validate_password,
    # MFA
    generate_mfa_code,
    verify_mfa_code,
    validate_mfa_code_format,
    # Session tokens
    generate_session_token,
    hash_token,
    verify_session_token,
    generate_device_fingerprint,
    # Validation
    validate_email,
    is_legislative_email,
    # Rate limiting
    check_rate_limit,
    RATE_LIMITS,
    calculate_lockout_duration,
    # Utilities
    mask_email,
    generate_secure_random_string,
)

from .auth_email import (
    send_email,
    send_signup_verification_code,
    send_welcome_email,
    send_login_mfa_code,
    send_password_reset_code,
    send_password_changed_notification,
    send_new_device_login_alert,
    send_account_locked_notification,
    test_email_configuration,
)

from .auth_routes import router as auth_router, set_templates

__all__ = [
    # Router
    "auth_router",
    "set_templates",
    # Database
    "init_auth_db",
    "get_db_connection",
    # User operations
    "create_user",
    "get_user_by_email",
    "get_user_by_id",
    "update_user_last_login",
    "update_user_password",
    "update_user_status",
    "check_user_locked",
    # MFA
    "create_mfa_code",
    "get_mfa_code",
    "get_latest_mfa_code",
    "mark_mfa_code_used",
    "increment_mfa_attempts",
    "invalidate_mfa_codes",
    "count_recent_mfa_requests",
    "generate_mfa_code",
    "verify_mfa_code",
    "validate_mfa_code_format",
    # Sessions
    "create_session_token",
    "get_session_by_token_hash",
    "update_session_last_used",
    "delete_session_token",
    "delete_all_user_sessions",
    "get_user_sessions",
    "generate_session_token",
    "hash_token",
    "verify_session_token",
    "generate_device_fingerprint",
    # Security
    "hash_password",
    "verify_password",
    "validate_password",
    "validate_email",
    "is_legislative_email",
    "check_rate_limit",
    "RATE_LIMITS",
    "calculate_lockout_duration",
    "mask_email",
    "generate_secure_random_string",
    # Login attempts
    "log_login_attempt",
    "count_failed_attempts",
    "count_failed_attempts_by_ip",
    # Security events
    "log_security_event",
    "mark_security_event_emailed",
    "get_recent_security_events",
    # Email
    "send_email",
    "send_signup_verification_code",
    "send_welcome_email",
    "send_login_mfa_code",
    "send_password_reset_code",
    "send_password_changed_notification",
    "send_new_device_login_alert",
    "send_account_locked_notification",
    "test_email_configuration",
    # Cleanup
    "run_cleanup_jobs",
    # Trusted devices
    "create_trusted_device",
    "get_trusted_device",
    "update_trusted_device_last_used",
    "delete_trusted_device",
    "delete_all_user_trusted_devices",
    "get_user_trusted_devices",
    "cleanup_expired_trusted_devices",
]
