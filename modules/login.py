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
from datetime import datetime
from typing import Optional
from modules.logging_config import logger

# ============================================
# DATABASE CONFIGURATION
# ============================================

DB_CONFIG = {
    "host": "localhost",
    "database": "dn_diagnostics",
    "user": "postgres",
    "password": "mise",
    "port": "5432"
}

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


def authenticate_user(username: str, password: str) -> bool:
    """
        Authenticates user and initializes session state.

        Parameters:
        -----------
        username : str
        password : str

        Behavior:
        ---------
        - Verifies credentials.
        - Sets Streamlit session variables:
            logged_in
            username
            employee_code
            role
        - Logs login event.

        Returns:
        --------
        True if authentication successful.
        False otherwise.
    """
    user = verify_credentials(username, password)

    if user:
        st.session_state.logged_in = True
        st.session_state.username = user["username"]
        st.session_state.employee_code = user["employee_code"]
        st.session_state.role          = user["role"]

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
