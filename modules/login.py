"""
login.py - Authentication and Database Logic
Updated to match admins table with ONLY:
- username
- password_hash
"""

import psycopg2
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
    "user": "dn_user",
    "password": "12345",
    "port": "5432"
}

# ============================================
# DATABASE CONNECTION
# ============================================

def get_db_connection():
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
    return hashlib.sha256(password.encode()).hexdigest()

# ============================================
# LOGIN HISTORY
# ============================================

def create_login_history_table():
    conn = get_db_connection()
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
    conn = get_db_connection()
    if not conn:
        logger.error("Login verification failed due to DB connection failure")
        return None

    try:
        cursor = conn.cursor()
        password_hash = hash_password(password)

        cursor.execute("""
            SELECT username
            FROM Users
            WHERE username = %s AND password_hash = %s AND is_active = TRUE
        """, (username, password_hash))

        user = cursor.fetchone()
        cursor.close()
        conn.close()
        return user[0] if user else None

    except Exception as e:
        logger.error("Login verification failed")
        conn.close()
        return None


def authenticate_user(username: str, password: str) -> bool:
    user = verify_credentials(username, password)

    if user:
        st.session_state.logged_in = True
        st.session_state.username = user

        log_login_event(username=user, action="login")
        return True

    return False

# ============================================
# REGISTRATION
# ============================================

def user_exists(email: str, employee_code: str) -> bool:
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
    Returns True if user exists, password is correct, but is_active = FALSE
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

    if "page" not in st.session_state:
        st.session_state.page = "login"

    if "logged_in" not in st.session_state:
        st.session_state.logged_in = False
    if "username" not in st.session_state:
        st.session_state.username = None


def is_logged_in() -> bool:
    return st.session_state.get("logged_in", False)


def get_current_user():
    return {
        "username": st.session_state.get("username")
    }


def logout_user():
    username = st.session_state.get("username")

    if username:
        log_login_event(username=username, action="logout")

    st.session_state.logged_in = False
    st.session_state.username = None
