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
        return psycopg2.connect(**DB_CONFIG)
    except Exception as e:
        print("âŒ DB connection failed:", e)
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
        print("âŒ Error creating login_history:", e)
        conn.rollback()
        conn.close()
        return False


def log_login_event(username: str, action: str):
    conn = get_db_connection()
    if not conn:
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
        print("âŒ Login history insert failed:", e)
        conn.rollback()
        conn.close()
        return False


def get_login_history(username: str = None, limit: int = 50):
    conn = get_db_connection()
    if not conn:
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
        print("âŒ Fetch login history failed:", e)
        conn.close()
        return []

# ============================================
# AUTHENTICATION
# ============================================

def verify_credentials(username: str, password: str) -> Optional[str]:
    conn = get_db_connection()
    if not conn:
        return None

    try:
        cursor = conn.cursor()
        password_hash = hash_password(password)

        cursor.execute("""
            SELECT username
            FROM admins
            WHERE username = %s AND password_hash = %s
        """, (username, password_hash))

        user = cursor.fetchone()
        cursor.close()
        conn.close()
        return user[0] if user else None

    except Exception as e:
        print("âŒ Login verification failed:", e)
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
# SESSION MANAGEMENT
# ============================================

def initialize_session():
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
