"""
    This module handles authentication, user registration, login history tracking,
    and session management for the Streamlit application.

    Main Responsibilities:
    ----------------------
    1. Database connection handling.
    2. Password hashing using SHA-256.
    3. User authentication and credential verification.
    4. User registration with inactive-by-default accounts.
    5. Login history tracking (login, logout, register events).
    6. Streamlit session state management.

    Database Used:
    --------------
    dn_diagnostics

    Tables Used:
    ------------
    - Users
    - login_history

    Security Features:
    ------------------
    - Passwords stored as SHA-256 hashes.
    - Only active users (is_active = TRUE) can log in.
    - Pending users (is_active = FALSE) can be detected separately.

    Designed For:
    -------------
    Streamlit-based authentication system with role-based access.

    Author: Your Name
    Purpose: Centralized authentication & session control layer.
"""

import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
import hashlib
import secrets
import smtplib
import os
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
import streamlit as st
from datetime import datetime, timedelta
from typing import Optional
from modules.logging_config import logger
from dotenv import load_dotenv
load_dotenv()  # auto load from root

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
env_path = os.path.join(BASE_DIR, ".env")
if os.path.exists(env_path):
    load_dotenv(env_path)

# ============================================
# DATABASE CONFIGURATION
# ============================================

DB_CONFIG = {
    "host": os.getenv("DB_HOST"),
    "database": os.getenv("DB_NAME"),
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD"),
    "port": int(os.getenv("DB_PORT", 5432))
}

# ============================================
# UAT / DEV CREDENTIALS
# ============================================
UAT_CREDENTIALS = {
    "username": os.getenv("DN_UAT_USERNAME"),
    "password": os.getenv("DN_UAT_PASSWORD")
}
# ============================================
# DEV MODE CONFIG
# ============================================

ENABLE_DEV_MODE = os.getenv("ENABLE_DEV_MODE", "false").lower() == "true"
# ============================================
# SMTP CONFIGURATION (SAFE)
# ============================================
SMTP_CONFIG = {
    "host": os.getenv("SMTP_HOST", "smtp.gmail.com"),
    "port": int(os.getenv("SMTP_PORT", 587)),
    "sender": os.getenv("SMTP_SENDER", "").strip(),
    "password": os.getenv("SMTP_PASSWORD", "").strip(),
    "display_name": os.getenv("SMTP_DISPLAY_NAME", "DN Diagnostics Platform")
}

APP_BASE_URL = os.getenv("APP_BASE_URL")
RESET_TOKEN_EXPIRY_MINUTES = int(os.getenv("RESET_TOKEN_EXPIRY_MINUTES", 30))

# Base URL of your Streamlit app.
# The reset link will be: APP_BASE_URL + ?reset_token=<token>
# Example local: "http://localhost:8501"
# APP_BASE_URL = os.getenv("APP_BASE_URL")

# # Token validity window (minutes)
# RESET_TOKEN_EXPIRY_MINUTES = int(os.getenv("RESET_TOKEN_EXPIRY_MINUTES", 30))

# ============================================
# DATABASE CONNECTION
# ============================================

def get_db_connection():
    """
        Establishes and returns a PostgreSQL database connection.

        Returns:
        --------
        connection object if successful.
        None if connection fails.

        Notes:
        ------
        Uses DB_CONFIG dictionary for connection parameters.
        Logs debug on success and error on failure.
    """
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        logger.debug("Database connection established")
        return conn
    except Exception as e:
        logger.error("Database connection failed")
        return None

# ============================================
# PASSWORD UTILS
# ============================================

def hash_password(password: str) -> str:
    """
        Generates SHA-256 hash for a given password.

        Parameters:
        -----------
        password : str
            Plain text password.

        Returns:
        --------
        str
            Hexadecimal SHA-256 hashed password.

        Purpose:
        --------
        Ensures passwords are never stored in plain text.
    """
    return hashlib.sha256(password.encode()).hexdigest()
def is_same_as_old_password(username: str, new_password: str) -> bool:
    """
    Checks if new password is same as old password (using SHA-256 hash).
    """
    conn = get_db_connection()
    if not conn:
        return False

    try:
        cursor = conn.cursor()

        # Get old hashed password
        cursor.execute("""
            SELECT password_hash
            FROM Users
            WHERE username = %s
        """, (username,))

        result = cursor.fetchone()
        cursor.close()
        conn.close()

        if not result:
            return False

        old_hashed_password = result[0]

        # Hash new password
        new_hashed_password = hash_password(new_password)

        # Compare hashes
        return new_hashed_password == old_hashed_password

    except Exception as e:
        conn.close()
        return False

# ============================================
# LOGIN HISTORY
# ============================================

def create_login_history_table():
    """
        Creates the login_history table if it does not exist.

        Table Structure:
        ----------------
        - serial_no (Auto Increment Primary Identifier)
        - user_id
        - timestamp
        - action (login, logout, register)

        Returns:
        --------
        True if table created successfully.
        False if creation fails.

        Purpose:
        --------
        Tracks user authentication activities for auditing.
    """
    conn = psycopg2.connect(**DB_CONFIG)
    if not conn:
        logger.error("Failed to create login_history table due to DB connection failure")
        return False

    try:
        cursor = conn.cursor()
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS login_history (
                serial_no SERIAL,
                user_id VARCHAR(100) NOT NULL,
                timestamp TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                action VARCHAR(20) NOT NULL
            );
        """)
        conn.commit()
        cursor.close()
        conn.close()
        return True
    except Exception as e:
        logger.error("Error creating login_history table")
        conn.rollback()
        conn.close()
        return False


def log_login_event(username: str, action: str):
    """
        Inserts a login-related event into login_history table.

        Parameters:
        -----------
        username : str
            User performing the action.
        action : str
            Type of action (e.g., 'login', 'logout', 'register').

        Returns:
        --------
        True if event logged successfully.
        False if logging fails.

        Purpose:
        --------
        Maintains audit trail of user activities.
    """
    conn = get_db_connection()
    if not conn:
        logger.error("Login event not logged. DB connection failed")
        return False

    try:
        cursor = conn.cursor()
        cursor.execute("""
            INSERT INTO login_history (user_id, timestamp, action)
            VALUES (%s, %s, %s)
        """, (username, datetime.now(), action))
        conn.commit()
        cursor.close()
        conn.close()
        return True
    except Exception as e:
        logger.error("Login history insert failed")
        conn.rollback()
        conn.close()
        return False


def get_login_history(username: str = None, limit: int = 50):
    """
        Fetches login history records.

        Parameters:
        -----------
        username : str (optional)
            If provided, filters history for that user only.
        limit : int
            Maximum number of records to return.

        Returns:
        --------
        list of tuples:
            (serial_no, user_id, timestamp, action)

        Notes:
        ------
        Records are ordered by most recent first.
    """
    conn = get_db_connection()
    if not conn:
        logger.error("Fetching login history failed due to DB connection failure")
        return []

    try:
        cursor = conn.cursor()
        if username:
            cursor.execute("""
                SELECT serial_no, user_id, timestamp, action
                FROM login_history
                WHERE user_id = %s
                ORDER BY timestamp DESC
                LIMIT %s
            """, (username, limit))
        else:
            cursor.execute("""
                SELECT serial_no, user_id, timestamp, action
                FROM login_history
                ORDER BY timestamp DESC
                LIMIT %s
            """, (limit,))
        rows = cursor.fetchall()
        cursor.close()
        conn.close()
        return rows
    except Exception as e:
        logger.error("Fetch login history failed")
        conn.close()
        return []

# ============================================
# AUTHENTICATION
# ============================================

def verify_credentials(username: str, password: str) -> Optional[str]:
    """
        Verifies user credentials against Users table.

        Parameters:
        -----------
        username : str
        password : str (plain text)

        Process:
        --------
        - Hashes password using SHA-256.
        - Checks username + password_hash + is_active = TRUE.

        Returns:
        --------
        dict:
            {
                "username": str,
                "employee_code": str,
                "role": str
            }
        OR
        None if authentication fails.

        Security:
        ---------
        Only active users are allowed to authenticate.
    """
    conn = get_db_connection()
    if not conn:
        logger.error("Login verification failed due to DB connection failure")
        return None

    try:
        cursor = conn.cursor()
        password_hash = hash_password(password)

        cursor.execute("""
            SELECT username,employee_code,role
            FROM Users
            WHERE username = %s AND password_hash = %s AND is_active = TRUE
        """, (username, password_hash))

        user = cursor.fetchone()
        cursor.close()
        conn.close()
        return {"username": user[0], "employee_code": user[1],"role": user[2]} if user else None

    except Exception as e:
        logger.error("Login verification failed")
        conn.close()
        return None

def authenticate_user_backend(username: str, password: str) -> dict | None:
    """
    Backend-safe authentication (used by FastAPI)
    """

    # ============================================
    #  DEV MODE LOGIN (FROM .env)
    # ============================================
    if ENABLE_DEV_MODE:
        for i in range(1, 4):  # supports 3 users
            env_user = os.getenv(f"DN_UAT_USERNAME_{i}")
            env_pass = os.getenv(f"DN_UAT_PASSWORD_{i}")

            if env_user and env_pass:
                if (username.strip() == env_user and password == env_pass):

                    logger.warning(
                        " DEV MODE LOGIN — bypassing DB for user '%s'",
                        username.strip()
                    )

                    return {
                        "username": username.strip(),
                        "name": "Dev User",
                        "employee_code": f"DEV00{i}",
                        "role": "DEV_MODE",
                    }

        # fallback for single user (your old setup)
        if (username.strip() == os.getenv("DN_UAT_USERNAME") and
                password == os.getenv("DN_UAT_PASSWORD")):

            logger.warning(
                " DEV MODE LOGIN — bypassing DB for user '%s'",
                username.strip()
            )

            return {
                "username": username.strip(),
                "name": "Dev User",
                "employee_code": "DEV000",
                "role": "DEV_MODE",
            }

    # ============================================
    #  NORMAL DB AUTHENTICATION
    # ============================================
    conn = get_db_connection()
    if not conn:
        logger.error("authenticate_user_backend: DB connection failed")
        return None

    try:
        cursor = conn.cursor()
        password_hash = hash_password(password)

        cursor.execute(
            """
            SELECT username, name, employee_code, role
            FROM Users
            WHERE username = %s
              AND password_hash = %s
              AND is_active = TRUE
            """,
            (username, password_hash),
        )

        row = cursor.fetchone()
        cursor.close()
        conn.close()

        if row:
            return {
                "username":      row[0],
                "name":          row[1],
                "employee_code": row[2],
                "role":          row[3],
            }

        return None

    except Exception:
        logger.exception("authenticate_user_backend query failed")
        conn.close()
        return None
    
def authenticate_user(username: str, password: str) -> bool:
    """
    Wrapper function (kept for backward compatibility).

    Now internally calls authenticate_user_backend()
    so both old Streamlit flow and new API flow work safely.
    """

    # Call new backend function
    user = authenticate_user_backend(username, password)

    if user:
        # Set session (same as old behavior)
        st.session_state.logged_in     = True
        st.session_state.username      = user["username"]
        st.session_state.employee_code = user.get("employee_code")
        st.session_state.role          = user.get("role")
        st.session_state.name          = user.get("name")

        log_login_event(username=user["username"], action="login")
        return True

    return False
# ============================================
# REGISTRATION
# ============================================

def user_exists(email: str, employee_code: str) -> bool:
    """
        Checks whether a user already exists.

        Parameters:
        -----------
        email : str
        employee_code : str

        Returns:
        --------
        True if user exists (username OR employee_code match).
        False otherwise.

        Purpose:
        --------
        Prevents duplicate user registrations.
    """
    try:
        conn = get_db_connection()
        with conn.cursor() as cursor:
            cursor.execute(
                """
                SELECT 1 FROM Users
                WHERE username = %s OR employee_code = %s
                """,
                (email, employee_code)
            )
            return cursor.fetchone() is not None

    except Exception:
        logger.exception("user_exists check failed")
        raise

    finally:
        if 'conn' in locals():
            conn.close()

def register_user(email, name, password, employee_code, role="USER") -> tuple[bool, str]:
    """
        Registers a new user account.

        Parameters:
        -----------
        email : str
        name : str
        password : str
        employee_code : str
        role : str (default = "USER")

        Behavior:
        ---------
        - Checks if user already exists.
        - Hashes password.
        - Inserts new user into Users table.
        - Sets is_active = FALSE (admin approval required).
        - Logs registration event.

        Returns:
        --------
        (True, success_message) if registration successful.
        (False, error_message) otherwise.

        Security:
        ---------
        New users must be activated by admin before login.
    """
    if user_exists(email, employee_code):
        logger.info(" User already exists")
        return False, "User with this email or employee code already exists."
    conn = get_db_connection()
    if not conn:
        logger.error(" DB connection failed")
        return False, "Database connection failed."
    try:
        cursor = conn.cursor()
        password_hash = hash_password(password)
        logger.info("Registering user | email=%s | employee_code=%s | role=%s",email, employee_code, role)
        cursor.execute("""
            INSERT INTO Users (username, name, password_hash, employee_code, role, is_active)
            VALUES (%s, %s, %s, %s, %s, %s)
        """, (email, name, password_hash, employee_code, role, False))  # inactive by default
        conn.commit()
        cursor.close()
        conn.close()
        log_login_event(username=email, action="register")
        return True, "Registration successful. Await admin activation."
    except Exception as e:
        logger.error("User registration failed")
        conn.rollback()
        conn.close()
        return False, "Registration failed."
    
def is_user_pending_approval(username: str, password: str) -> bool:
    """
        Checks whether user credentials are correct
        but account is not yet activated.

        Parameters:
        -----------
        username : str
        password : str

        Returns:
        --------
        True  -> If user exists and is_active = FALSE.
        False -> Otherwise.

        Purpose:
        --------
        Allows UI to display:
        "Your account is pending admin approval."
    """
    conn = get_db_connection()
    if not conn:
        logger.error("Pending approval check failed due to DB connection failure")
        return False
    try:
        cursor = conn.cursor()
        password_hash = hash_password(password)

        cursor.execute("""
            SELECT 1
            FROM Users
            WHERE username = %s
              AND password_hash = %s
              AND is_active = FALSE
        """, (username, password_hash))

        pending = cursor.fetchone() is not None
        cursor.close()
        conn.close()
        return pending

    except Exception as e:
        logger.error("Pending approval check failed")
        conn.close()
        return False


# ============================================
# FORGOT PASSWORD - IDENTITY VERIFICATION
# ============================================

def verify_reset_identity(username: str, employee_code: str) -> bool:
    """
        Verifies that the username + employee_code combination exists
        AND the account is active (is_active = TRUE).

        This is Step 1 of the password reset flow.
        Used to confirm the user's identity before sending a reset link.

        Parameters:
        -----------
        username : str
            The user's registered email / username.
        employee_code : str
            The user's 8-digit employee code.

        Returns:
        --------
        True  → If username + employee_code match and account is active.
        False → If no match found or account is inactive.

        Security:
        ---------
        Only active accounts are eligible for password reset.
        Inactive/pending accounts are blocked from reset flow.
    """
    conn = get_db_connection()
    if not conn:
        logger.error("verify_reset_identity: DB connection failed")
        return False

    try:
        cursor = conn.cursor()
        cursor.execute("""
            SELECT 1
            FROM Users
            WHERE username = %s
              AND employee_code = %s
              AND is_active = TRUE
        """, (username, employee_code))

        result = cursor.fetchone() is not None
        cursor.close()
        conn.close()

        if result:
            logger.info("Reset identity verified for user: %s", username)
        else:
            logger.warning("Reset identity verification failed for user: %s", username)

        return result

    except Exception as e:
        logger.error("verify_reset_identity query failed: %s", e)
        conn.close()
        return False


# ============================================
# FORGOT PASSWORD - STEP 2
# Create password_reset_tokens Table
# ============================================

def create_reset_tokens_table() -> bool:
    """
        Creates the password_reset_tokens table if it does not exist.

        Table Structure:
        ----------------
        - id           : Auto-increment primary key
        - username     : FK reference to Users.username
        - token        : Cryptographically secure URL-safe token (unique)
        - created_at   : Timestamp when token was generated
        - expires_at   : Timestamp when token becomes invalid (30 min window)
        - is_used      : Flag to mark token as consumed after password reset

        Security:
        ---------
        - Tokens are single-use (is_used = TRUE after reset).
        - Tokens expire after RESET_TOKEN_EXPIRY_MINUTES.
        - Old/used tokens are never deleted immediately; auditable.

        Returns:
        --------
        True  → Table created or already exists.
        False → DB error.
    """
    conn = get_db_connection()
    if not conn:
        logger.error("create_reset_tokens_table: DB connection failed")
        return False

    try:
        cursor = conn.cursor()
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS password_reset_tokens (
                id          SERIAL PRIMARY KEY,
                username    VARCHAR(150) NOT NULL,
                token       VARCHAR(256) UNIQUE NOT NULL,
                created_at  TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                expires_at  TIMESTAMP NOT NULL,
                is_used     BOOLEAN DEFAULT FALSE
            );
        """)
        conn.commit()
        cursor.close()
        conn.close()
        logger.info("password_reset_tokens table ready")
        return True

    except Exception as e:
        logger.error("create_reset_tokens_table failed: %s", e)
        conn.rollback()
        conn.close()
        return False


# ============================================
# FORGOT PASSWORD - STEP 2
# Generate Secure Reset Token & Store in DB
# ============================================

def generate_reset_token(username: str) -> str | None:
    """
        Generates a cryptographically secure password reset token
        for a given user and stores it in the password_reset_tokens table.

        Process:
        --------
        1. Invalidates (marks is_used=TRUE) any existing active tokens
           for this user to prevent multiple live reset links.
        2. Generates a URL-safe token using Python's secrets module.
        3. Calculates expiry timestamp (now + RESET_TOKEN_EXPIRY_MINUTES).
        4. Inserts the new token record into password_reset_tokens.

        Parameters:
        -----------
        username : str
            The username (email) whose token is being generated.

        Returns:
        --------
        str   → The generated token string if successful.
        None  → If DB operation fails.

        Security:
        ---------
        - Uses secrets.token_urlsafe(48) — 384 bits of entropy.
        - Expires in RESET_TOKEN_EXPIRY_MINUTES (default 30 min).
        - Previous tokens are invalidated before a new one is issued.
    """
    conn = get_db_connection()
    if not conn:
        logger.error("generate_reset_token: DB connection failed for user: %s", username)
        return None

    try:
        cursor = conn.cursor()

        # ── Invalidate any previously active tokens for this user ──
        cursor.execute("""
            UPDATE password_reset_tokens
            SET is_used = TRUE
            WHERE username = %s
              AND is_used = FALSE
              AND expires_at > NOW()
        """, (username,))

        # ── Generate new token ─────────────────────────────────────
        token = secrets.token_urlsafe(48)
        expires_at = datetime.now() + timedelta(minutes=RESET_TOKEN_EXPIRY_MINUTES)

        cursor.execute("""
            INSERT INTO password_reset_tokens
                (username, token, created_at, expires_at, is_used)
            VALUES
                (%s, %s, %s, %s, FALSE)
        """, (username, token, datetime.now(), expires_at))

        conn.commit()
        cursor.close()
        conn.close()

        logger.info(
            "Reset token generated for user: %s | expires: %s",
            username, expires_at.strftime("%Y-%m-%d %H:%M:%S")
        )
        return token

    except Exception as e:
        logger.error("generate_reset_token failed for user %s: %s", username, e)
        conn.rollback()
        conn.close()
        return None


# ============================================
# FORGOT PASSWORD - STEP 2
# Send Password Reset Email via SMTP
# ============================================

def send_reset_email(to_email: str, token: str, base_url: str = None) -> bool:
    """
        Composes and sends a password reset email to the user
        via Gmail SMTP using TLS encryption.

        The email contains:
        -------------------
        - A secure reset link: APP_BASE_URL + ?reset_token=<token>
        - Expiry notice (RESET_TOKEN_EXPIRY_MINUTES)
        - Plain-text and HTML versions (multipart/alternative)

        Parameters:
        -----------
        to_email : str
            Recipient email address (the user's username/email).
        token : str
            The reset token generated by generate_reset_token().

        Returns:
        --------
        True  → Email sent successfully.
        False → SMTP or connection error.

        SMTP Flow (matches flowchart):
        --------------------------------
        1. Connect to SMTP server (smtp.gmail.com:587)
        2. Establish secure TLS connection (starttls)
        3. Authenticate with sender email + App Password
        4. Send reset email with tokenised link

        Configuration:
        --------------
        Update SMTP_CONFIG and APP_BASE_URL at the top of this file
        before deployment.

        Security:
        ---------
        - TLS enforced via starttls().
        - App Password used (not plain Gmail password).
        - Reset link is single-use and time-limited.
    """
    # ── Pre-flight: catch unconfigured credentials immediately ─────
    if (SMTP_CONFIG["sender"] == "your-email@gmail.com"
            or SMTP_CONFIG["password"] == "your-app-password-here"):
        logger.error(
            "send_reset_email: SMTP_CONFIG is still using placeholder values!\n"
            "  → Open login.py and update SMTP_CONFIG:\n"
            "      'sender'   → your real Gmail address\n"
            "      'password' → your Gmail App Password\n"
            "  → To generate an App Password:\n"
            "      1. Go to myaccount.google.com/security\n"
            "      2. Enable 2-Step Verification\n"
            "      3. Go to myaccount.google.com/apppasswords\n"
            "      4. Create a password for 'Mail' and paste it here"
        )
        return False

    effective_base = (base_url or APP_BASE_URL).rstrip("/")
    reset_url = f"{effective_base}?reset_token={token}"
    expiry_mins = RESET_TOKEN_EXPIRY_MINUTES

    # ── Plain-text fallback ────────────────────────────────────────
    plain_body = (
        f"Hello,\n\n"
        f"You requested a password reset for your DN Diagnostics account.\n\n"
        f"Click the link below to reset your password:\n"
        f"{reset_url}\n\n"
        f"This link will expire in {expiry_mins} minutes.\n\n"
        f"If you did not request this reset, please ignore this email.\n\n"
        f"— DN Diagnostics Platform"
    )

    # ── HTML version ───────────────────────────────────────────────
    html_body = f"""
    <html>
    <body style="font-family: Arial, sans-serif; background:#f4f6f9; padding:30px;">
        <div style="max-width:480px; margin:auto; background:#ffffff;
                    border-radius:10px; padding:32px; box-shadow:0 2px 8px rgba(0,0,0,0.08);">

            <h2 style="color:#1e3a5f; margin-top:0;"> Password Reset Request</h2>

            <p style="color:#444; line-height:1.6;">
                You requested a password reset for your
                <strong>DN Diagnostics Platform</strong> account.
            </p>

            <p style="color:#444; line-height:1.6;">
                Click the button below to reset your password.
                This link will expire in <strong>{expiry_mins} minutes</strong>.
            </p>

            <div style="text-align:center; margin:28px 0;">
                <a href="{reset_url}"
                   style="background:#2563eb; color:#ffffff; padding:14px 32px;
                          border-radius:8px; text-decoration:none;
                          font-size:16px; font-weight:600;">
                    Reset Password
                </a>
            </div>

            <p style="color:#888; font-size:13px; line-height:1.5;">
                If the button doesn't work, copy and paste this link into your browser:<br>
                <a href="{reset_url}" style="color:#2563eb; word-break:break-all;">{reset_url}</a>
            </p>

            <hr style="border:none; border-top:1px solid #eee; margin:24px 0;">

            <p style="color:#aaa; font-size:12px;">
                If you did not request a password reset, you can safely ignore this email.
                Your account remains secure.
            </p>

            <p style="color:#aaa; font-size:12px; margin-bottom:0;">
                — DN Diagnostics Platform
            </p>
        </div>
    </body>
    </html>
    """

    try:
        # ── Build the email message ────────────────────────────────
        msg = MIMEMultipart("alternative")
        msg["Subject"] = "DN Diagnostics — Password Reset Request"
        msg["From"]    = f"{SMTP_CONFIG['display_name']} <{SMTP_CONFIG['sender']}>"
        msg["To"]      = to_email

        msg.attach(MIMEText(plain_body, "plain"))
        msg.attach(MIMEText(html_body, "html"))

        # ── Connect, secure, authenticate, send ───────────────────
        with smtplib.SMTP(SMTP_CONFIG["host"], SMTP_CONFIG["port"]) as server:
            server.ehlo()
            server.starttls()                          # enforce TLS
            server.ehlo()
            server.login(SMTP_CONFIG["sender"], SMTP_CONFIG["password"])
            server.sendmail(SMTP_CONFIG["sender"], to_email, msg.as_string())

        logger.info("Reset email sent successfully to: %s", to_email)
        return True

    except smtplib.SMTPAuthenticationError as e:
        logger.error(
            "send_reset_email: SMTP AUTHENTICATION FAILED for %s.\n"
            "  → Error code : %s\n"
            "  → Message    : %s\n"
            "  → Fix        : Make sure you are using a Gmail App Password,\n"
            "                 NOT your regular Gmail login password.\n"
            "                 Generate one at: myaccount.google.com/apppasswords\n"
            "                 (requires 2-Step Verification to be ON)",
            to_email, e.smtp_code, e.smtp_error
        )
        return False

    except smtplib.SMTPConnectError as e:
        logger.error(
            "send_reset_email: SMTP CONNECTION FAILED.\n"
            "  → Error  : %s\n"
            "  → Fix    : Check that SMTP host/port are correct and your\n"
            "             network/firewall allows outbound connections on port 587.",
            e
        )
        return False

    except smtplib.SMTPRecipientsRefused as e:
        logger.error(
            "send_reset_email: RECIPIENT REFUSED for %s.\n"
            "  → Error  : %s\n"
            "  → Fix    : Check the recipient email address is valid.",
            to_email, e
        )
        return False

    except smtplib.SMTPException as e:
        logger.error(
            "send_reset_email: SMTP ERROR for %s.\n"
            "  → Type    : %s\n"
            "  → Details : %s",
            to_email, type(e).__name__, e
        )
        return False

    except Exception as e:
        logger.exception(
            "send_reset_email: UNEXPECTED ERROR for %s — %s: %s",
            to_email, type(e).__name__, e
        )
        return False


# ============================================
# FORGOT PASSWORD - STEP 3a
# Validate Reset Token from URL
# ============================================

def validate_reset_token(token: str) -> str | None:
    """
        Validates a password reset token from the URL.

        Checks:
        -------
        1. Token exists in password_reset_tokens table.
        2. Token has not been used (is_used = FALSE).
        3. Token has not expired (expires_at > NOW()).

        Parameters:
        -----------
        token : str
            The URL-safe token string from the reset link.

        Returns:
        --------
        str  -> The username associated with the token if valid.
        None -> If token is invalid, expired, or already used.

        Security:
        ---------
        - Expired tokens are rejected regardless of is_used flag.
        - Used tokens are rejected regardless of expiry.
        - No information is revealed about WHY a token is invalid.
    """
    conn = get_db_connection()
    if not conn:
        logger.error("validate_reset_token: DB connection failed")
        return None

    try:
        cursor = conn.cursor()
        cursor.execute("""
            SELECT username
            FROM password_reset_tokens
            WHERE token      = %s
              AND is_used    = FALSE
              AND expires_at > NOW()
        """, (token,))

        row = cursor.fetchone()
        cursor.close()
        conn.close()

        if row:
            logger.info("validate_reset_token: valid token for user: %s", row[0])
            return row[0]
        else:
            logger.warning("validate_reset_token: invalid/expired/used token supplied")
            return None

    except Exception as e:
        logger.error("validate_reset_token: query failed: %s", e)
        conn.close()
        return None


# ============================================
# FORGOT PASSWORD - STEP 3b
# Reset Password in DB & Consume Token
# ============================================

def reset_user_password(token: str, new_password: str) -> tuple[bool, str]:
    """
        Resets the user's password after token validation.

        Process:
        --------
        1. Validates the token (not used, not expired).
        2. Checks new password does not match the current password.
        3. Updates password_hash in Users table.
        4. Marks token as is_used = TRUE (single-use enforcement).
        5. Logs a password_reset event in login_history.

        Parameters:
        -----------
        token : str
            The reset token from the URL / session.
        new_password : str
            Plain text new password (caller must validate strength first).

        Returns:
        --------
        (True,  "success message") -> Password reset successfully.
        (False, "error message")   -> Validation or DB failure.

        Security:
        ---------
        - New password is SHA-256 hashed before storage.
        - Token is consumed immediately after use.
        - Old password reuse is blocked.
        - All operations are wrapped in a single transaction.
    """
    # ── Step 1: Validate token ─────────────────────────────────────
    username = validate_reset_token(token)
    if not username:
        return False, "This reset link is invalid or has expired. Please request a new one."

    conn = get_db_connection()
    if not conn:
        logger.error("reset_user_password: DB connection failed for user: %s", username)
        return False, "Database connection failed. Please try again."

    try:
        cursor = conn.cursor()

        # ── Step 2: Check new password != current password ─────────
        new_hash = hash_password(new_password)
        cursor.execute(
            "SELECT password_hash FROM Users WHERE username = %s",
            (username,)
        )
        row = cursor.fetchone()
        if row and row[0] == new_hash:
            cursor.close()
            conn.close()
            return False, "New password must be different from your current password."

        # ── Step 3: Update password in Users table ──────────────────
        cursor.execute("""
            UPDATE Users
            SET password_hash = %s
            WHERE username = %s
        """, (new_hash, username))

        # ── Step 4: Mark token as used ──────────────────────────────
        cursor.execute("""
            UPDATE password_reset_tokens
            SET is_used = TRUE
            WHERE token = %s
        """, (token,))

        conn.commit()
        cursor.close()
        conn.close()

        # ── Step 5: Log the event ───────────────────────────────────
        log_login_event(username=username, action="password_reset")
        logger.info("reset_user_password: password reset successful for user: %s", username)

        return True, "Your password has been reset successfully. You can now log in."

    except Exception as e:
        logger.error("reset_user_password: failed for user %s: %s", username, e)
        conn.rollback()
        conn.close()
        return False, "Password reset failed due to an internal error. Please try again."
# ============================================
# PASSWORD STRENGTH VALIDATION
# ============================================

def is_valid_password(password: str) -> bool:
    """
        Validates password strength.

        Rules:
        ------
        - Minimum 8 characters
        - At least 1 uppercase letter (A-Z)
        - At least 1 lowercase letter (a-z)
        - At least 2 digit characters (0-9)
        - At least 1 special character (!@#$%^&* etc.)

        Parameters:
        -----------
        password : str
            Plain text password to validate.

        Returns:
        --------
        True  -> Password meets all strength requirements.
        False -> One or more rules are violated.
    """
    if len(password) < 8:
        return False
    if not any(c.isupper() for c in password):
        return False
    if not any(c.islower() for c in password):
        return False
    if sum(c.isdigit() for c in password) < 2:
        return False
    if not any(c in "!@#$%^&*()-_=+[]{}|;:'\",.<>?/`~" for c in password):
        return False
    return True


# ============================================
# SESSION MANAGEMENT
# ============================================

def initialize_session():
    """
        Initializes default Streamlit session variables.

        Sets:
        -----
        page
        logged_in
        username
        employee_code
        role

        Purpose:
        --------
        Ensures session keys exist before application logic runs.
        Prevents KeyError in Streamlit.
    """

    if "page" not in st.session_state:
        st.session_state.page = "login"

    if "logged_in" not in st.session_state:
        st.session_state.logged_in = False
    if "username" not in st.session_state:
        st.session_state.username = None
    if "employee_code" not in st.session_state:
        st.session_state.employee_code = None
    if "role" not in st.session_state:
        st.session_state.role = None

    # ── Developer mode bypass ──────────────────────────────────────
    # When True, any username/password combination grants access.
    # This flag is toggled from the login page UI and should NEVER
    # be enabled in a production deployment.
    if "dev_mode" not in st.session_state:
        st.session_state.dev_mode = False


def is_logged_in() -> bool:
    """
        Checks whether user is currently logged in.

        Returns:
        --------
        True if session_state.logged_in is True.
        False otherwise.
    """
    return st.session_state.get("logged_in", False)


def get_current_user():
    """
        Retrieves current logged-in user information.

        Returns:
        --------
        dict:
            {
                "username": str
            }

        Note:
        -----
        Returns None values if user is not logged in.
    """
    return {
        "username": st.session_state.get("username")
    }


def logout_user():
    """
        Logs out the current user.

        Behavior:
        ---------
        - Logs logout event into login_history.
        - Clears session_state variables:
            logged_in
            username
            employee_code
            role

        Purpose:
        --------
        Properly terminates authenticated session.
    """
    username = st.session_state.get("username")

    if username:
        log_login_event(username=username, action="logout")

    st.session_state.logged_in     = False
    st.session_state.username      = None
    st.session_state.employee_code = None
    st.session_state.role          = None
