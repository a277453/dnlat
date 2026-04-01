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
import streamlit as st
from datetime import datetime, timedelta, timezone
from typing import Optional
from jose import jwt, JWTError, ExpiredSignatureError
from fastapi import HTTPException, status
from modules.logging_config import logger
import os
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

# ============================================
# JWT CONFIGURATION
# ============================================
JWT_SECRET_KEY  = os.getenv("JWT_SECRET_KEY", "CHANGE_THIS_SECRET_IN_PRODUCTION")
JWT_ALGORITHM   = os.getenv("JWT_ALGORITHM", "HS256")
JWT_EXPIRY_HOURS = int(os.getenv("JWT_EXPIRY_HOURS", "8"))

# Paths that never need a token (checked by the middleware in main.py)
PUBLIC_PATHS = {
    "/",
    "/health",
    "/docs",
    "/redoc",
    "/openapi.json",
    "/api/v1/auth/login",
    "/api/v1/auth/register",
    "/api/v1/auth/initialize-db",
    "/api/v1/forgot-password",
    "/api/v1/verify-reset-identity",
    "/api/v1/reset-password",
}


def create_access_token(username: str, role: str, employee_code: str) -> str:
    """Create a signed JWT encoding username + role. Called at login."""
    payload = {
        "sub":  username,
        "role": role,
        "emp":  employee_code,
        "exp":  datetime.now(timezone.utc) + timedelta(hours=JWT_EXPIRY_HOURS),
    }
    token = jwt.encode(payload, JWT_SECRET_KEY, algorithm=JWT_ALGORITHM)
    logger.info("JWT issued: user='%s' role='%s'", username, role)
    return token


def decode_access_token(token: str) -> dict:
    """
    Decode and validate a JWT.
    Raises HTTPException 401 if token is missing, expired, or tampered.
    """
    try:
        return jwt.decode(token, JWT_SECRET_KEY, algorithms=[JWT_ALGORITHM])
    except ExpiredSignatureError:
        logger.warning("JWT decode failed: token expired")
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail={
                "status":  "error",
                "code":    401,
                "error":   "Unauthorized",
                "message": "Your session has expired. Please log in again.",
            },
            headers={"WWW-Authenticate": "Bearer"},
        )
    except JWTError as exc:
        logger.warning("JWT decode failed: %s", exc)
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail={
                "status":  "error",
                "code":    401,
                "error":   "Unauthorized",
                "message": "Invalid session token. Please log in again.",
            },
            headers={"WWW-Authenticate": "Bearer"},
        )

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
    if "session_token" not in st.session_state:
        st.session_state.session_token = None

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
    st.session_state.session_token = None
