"""
Database Management Module for Transaction Analysis System.

This module is responsible for:

1. Creating and managing the 'userresponse' PostgreSQL database.
2. Creating required tables:
   - analysis_data (stores LLM analysis metadata)
   - feedback (stores user feedback on analysis)
3. Storing and retrieving transaction analysis records.
4. Handling user authentication (login verification).
5. Managing role-based access retrieval.

Key Concepts Used:
------------------
- psycopg2 for PostgreSQL connectivity.
- ISOLATION_LEVEL_AUTOCOMMIT:
    Executes SQL statements immediately without wrapping them
    inside a transaction block.
- SHA-256 hashing (via hashlib):
    Used to securely verify user passwords.
- Dictionary unpacking (**CONFIG):
    Used to pass database configuration parameters dynamically
    into psycopg2.connect().

Databases Used:
---------------
- postgres (default admin DB) → Used to create userresponse DB
- userresponse → Stores analysis + feedback data
- dn_diagnostics → Used for login validation

Author: Your Name
Purpose: Production-ready database utility layer
"""

import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
import hashlib
import os

# ============================================
# CONNECT TO DEFAULT POSTGRES DB
# ============================================

ADMIN_DB_CONFIG = {
    "host": "localhost",
    "database": "postgres",  # connect -> default postgres DB 
    "user": "postgres",      # postgres user
    "password": "mise",          #  postgres password 
    "port": "5432"
}

# ============================================
# CONNECT TO USERRESPONSE DB
# ============================================

USERRESPONSE_DB_CONFIG = {
    "host": "localhost",
    "database": "userresponse", 
    "user": "postgres",
    "password": "mise",
    "port": "5432"
}
# CREATE userresponse DATABASE
# ============================================

def create_userresponse_database():
    """
        Creates the 'userresponse' database if it does not already exist.

        Process:
        --------
        1. Connects to the default 'postgres' database.
        2. Checks whether 'userresponse' exists in pg_database.
        3. Creates the database if not found.

        Uses:
        -----
        - ISOLATION_LEVEL_AUTOCOMMIT to allow CREATE DATABASE
        (PostgreSQL does not allow CREATE DATABASE inside transactions).

        Raises:
        -------
        Prints error message if connection or creation fails.
    """
    try:
        # Connect to default postgres DB first
        conn = psycopg2.connect(**ADMIN_DB_CONFIG)
        conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        cursor = conn.cursor()
        print("Connected to default 'postgres' database successfully!")

        # Check if 'userresponse' already exists
        cursor.execute("SELECT 1 FROM pg_database WHERE datname = 'userresponse'")
        exists = cursor.fetchone()

        # Create if not exists
        if not exists:
            cursor.execute("CREATE DATABASE userresponse;")
            print("Database 'userresponse' created successfully!\n")
        else:
            print("Database 'userresponse' already exists — skipping.\n")

        cursor.close()
        conn.close()

    except Exception as e:
        print(f"Failed to create database: {e}")
        print("Make sure PostgreSQL is running and your password is correct.")

# ============================================
#  CREATE analysis_data TABLE
# ============================================

def create_analysis_table():
    """
        Creates the 'analysis_data' table inside the 'userresponse' database.

        Table Purpose:
        --------------
        Stores LLM transaction analysis metadata including:
        - Transaction details
        - Model used
        - Log statistics
        - Analysis duration
        - Raw LLM output
        - Timestamp of creation

        Primary Key:
        ------------
        (transaction_id, employee_code)

        Behavior:
        ---------
        Uses CREATE TABLE IF NOT EXISTS to prevent duplicate creation.
    """
    try:
        # Connect to userresponse DB
        conn = psycopg2.connect(**USERRESPONSE_DB_CONFIG)
        cursor = conn.cursor()
        print("Connected to 'userresponse' database successfully!")

        cursor.execute("""
            CREATE TABLE IF NOT EXISTS analysis_data (
                transaction_id        VARCHAR(255),
                employee_code         VARCHAR(8),
                model                 VARCHAR(100),
                transaction_type      VARCHAR(100),
                transaction_state     VARCHAR(100),
                source_file           VARCHAR(255),
                start_time            VARCHAR(100),
                end_time              VARCHAR(100),
                log_length            INTEGER,
                response_length       INTEGER,
                analysis_time_seconds FLOAT,
                llm_analysis          TEXT,
                created_at            TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (transaction_id, employee_code)
            );
        """)

        conn.commit()
        cursor.close()
        conn.close()
        print("Table 'analysis_data' created successfully!\n")

    except Exception as e:
        print(f"Failed to create table: {e}")

# ============================================
#  STORE OLLAMA METADATA
# Each field stored in its own column
# ============================================

def store_metadata(
    transaction_id: str,
    employee_code: str,            
    model: str,
    transaction_type: str,
    transaction_state: str,
    source_file: str,
    start_time: str,
    end_time: str,
    log_length: int,
    response_length: int,
    analysis_time_seconds: float,
    llm_analysis: str
):
    """
        Inserts or updates transaction analysis metadata in the database.

        Parameters:
        -----------
        transaction_id : str
        employee_code : str
        model : str
        transaction_type : str
        transaction_state : str
        source_file : str
        start_time : str
        end_time : str
        log_length : int
                        response_length : int
        analysis_time_seconds : float
        llm_analysis : str

        Behavior:
        ---------
        - Inserts new record into analysis_data table.
        - If record already exists (same transaction_id + employee_code),
        performs an UPDATE using ON CONFLICT.

        Returns:
        --------
        None (prints status message).

        Notes:
        ------
        Ensures idempotent storage of analysis results.
    """
    try:
        conn = psycopg2.connect(**USERRESPONSE_DB_CONFIG)
        cursor = conn.cursor()

        cursor.execute("""
            INSERT INTO analysis_data (
                transaction_id, employee_code, model,
                transaction_type, transaction_state,
                source_file, start_time, end_time,
                log_length, response_length,
                analysis_time_seconds, llm_analysis,
                created_at
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP)
            ON CONFLICT (transaction_id, employee_code) DO UPDATE SET
                model                 = EXCLUDED.model,
                transaction_type      = EXCLUDED.transaction_type,
                transaction_state     = EXCLUDED.transaction_state,
                source_file           = EXCLUDED.source_file,
                start_time            = EXCLUDED.start_time,
                end_time              = EXCLUDED.end_time,
                log_length            = EXCLUDED.log_length,
                response_length       = EXCLUDED.response_length,
                analysis_time_seconds = EXCLUDED.analysis_time_seconds,
                llm_analysis          = EXCLUDED.llm_analysis,
                created_at            = CURRENT_TIMESTAMP
        """, (
            transaction_id, employee_code, model,
            transaction_type, transaction_state,
            source_file, start_time, end_time,
            log_length, response_length,
            analysis_time_seconds, llm_analysis
        ))

        conn.commit()
        cursor.close()
        conn.close()
        print(f"Metadata stored! (transaction_id: {transaction_id}, employee_code: {employee_code})")

    except Exception as e:
        print(f"Failed to store metadata: {e}")

# ============================================
# dn_diagnostics DB CONFIG
# (needed to verify login and get employee_code)
# ============================================

DB_CONFIG = {
    "host": "localhost",
    "database": "dn_diagnostics",
    "user": "postgres",
    "password": "mise",
    "port": "5432"
}
# ============================================
# STEP 1: CHECK LOGIN
# Verifies username + password against dn_diagnostics.Users
# Returns employee_code if valid, None if not
# ============================================

def check_login(username: str, password: str):
    """
        Verifies user login credentials against dn_diagnostics.Users table.

        Parameters:
        -----------
        username : str
        password : str (plain text input)

        Process:
        --------
        1. Hashes the password using SHA-256.
        2. Matches username + password_hash + is_active = TRUE.
        3. Retrieves employee_code if valid.

        Returns:
        --------
        dict:
            {
                "username": str,
                "employee_code": str
            }
        OR
        None if authentication fails.

        Security:
        ---------
        Passwords are verified using hash comparison (not plain text).
    """
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cursor = conn.cursor()

        password_hash = hashlib.sha256(password.encode()).hexdigest()

        cursor.execute("""
            SELECT username, employee_code
            FROM Users
            WHERE username = %s AND password_hash = %s AND is_active = TRUE
        """, (username, password_hash))

        user = cursor.fetchone()
        cursor.close()
        conn.close()

        if user:
            print(f"Login OK — username: {user[0]}, employee_code: {user[1]}")
            return {"username": user[0], "employee_code": user[1]}
        else:
            print("Login failed — wrong credentials or inactive account.")
            return None

    except Exception as e:
        print(f"Login check error: {e}")
        return None


# ============================================
# STEP 2: RETRIEVE ANALYSIS
# Uses transaction_id + employee_code + date to fetch record
# ============================================

def retrieve_analysis(employee_code: str, transaction_id: str, date: str):
    """
        Retrieves a single transaction analysis record for a specific date.

        Parameters:
        -----------
        employee_code : str
        transaction_id : str
        date : str (format: 'YYYY-MM-DD')

        Returns:
        --------
        dict containing analysis fields if found.
        None if no record exists.

        Query Filter:
        -------------
        - transaction_id match
        - employee_code match
        - DATE(created_at) match

        Use Case:
        ---------
        Used to fetch analysis results from a specific day.
    """
    try:
        conn = psycopg2.connect(**USERRESPONSE_DB_CONFIG)
        cursor = conn.cursor()

        cursor.execute("""
            SELECT
                transaction_id, employee_code, model,
                transaction_type, transaction_state,
                source_file, start_time, end_time,
                log_length, response_length,
                analysis_time_seconds, llm_analysis, created_at
            FROM analysis_data
            WHERE transaction_id = %s
              AND employee_code  = %s
              AND DATE(created_at) = %s
        """, (transaction_id, employee_code, date))

        row = cursor.fetchone()
        cursor.close()
        conn.close()

        if not row:
            print(f"No record found for transaction_id='{transaction_id}', employee_code='{employee_code}', date='{date}'")
            return None

        columns = [
            "transaction_id", "employee_code", "model",
            "transaction_type", "transaction_state",
            "source_file", "start_time", "end_time",
            "log_length", "response_length",
            "analysis_time_seconds", "llm_analysis", "created_at"
        ]
        return dict(zip(columns, row))

    except Exception as e:
        print(f"Retrieve error: {e}")
        return None


# ============================================
# CREATE feedback TABLE
# Stores user feedback for each transaction analysis
# ============================================

def create_feedback_table():
    """
        Creates the 'feedback' table inside the 'userresponse' database.

        Table Purpose:
        --------------
        Stores user feedback for transaction analysis.

        Columns:
        --------
        - rating
        - alternative_cause
        - comment
        - model_version
        - submitted_at

        Primary Key:
        ------------
        (transaction_id, user_name)

        Ensures:
        --------
        One feedback per user per transaction.
    """
    try:
        conn = psycopg2.connect(**USERRESPONSE_DB_CONFIG)
        cursor = conn.cursor()

        cursor.execute("""
            CREATE TABLE IF NOT EXISTS feedback (
                transaction_id    VARCHAR(255),
                user_name         VARCHAR(150),
                rating            INTEGER,
                alternative_cause TEXT,
                comment           TEXT,
                model_version     VARCHAR(100),
                submitted_at      TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (transaction_id, user_name)
            );
        """)

        conn.commit()
        cursor.close()
        conn.close()
        print("Table 'feedback' created successfully!")

    except Exception as e:
        print(f"Failed to create feedback table: {e}")


# ============================================
# STORE FEEDBACK
# ============================================

def store_feedback(
    transaction_id: str,
    user_name: str,
    rating: int,
    alternative_cause: str,
    comment: str,
    model_version: str
):
    """
        Stores user feedback for a specific transaction.

        Parameters:
        -----------
        transaction_id : str
        user_name : str
        rating : int
        alternative_cause : str
        comment : str
        model_version : str

        Logic:
        ------
        - Checks if feedback already exists for the same
        (transaction_id, user_name).
        - Allows only one feedback entry per user per transaction.
        - If exists → returns "LIMIT_REACHED"
        - If stored → returns "SUCCESS"
        - If error → returns "ERROR"

        Returns:
        --------
        str status message.
    """
    try:
        conn = psycopg2.connect(**USERRESPONSE_DB_CONFIG)
        cursor = conn.cursor()

        # Check total feedbacks for this user + transaction (max 1 allowed)
        cursor.execute("""
            SELECT COUNT(*) FROM feedback
            WHERE transaction_id = %s
              AND user_name      = %s
        """, (transaction_id, user_name))

        count = cursor.fetchone()[0]

        if count >= 1:
            print(f"Feedback already exists — user: {user_name}, transaction: {transaction_id}")
            cursor.close()
            conn.close()
            return "LIMIT_REACHED"

        # Insert new feedback row
        cursor.execute("""
            INSERT INTO feedback (
                transaction_id,
                user_name,
                rating,
                alternative_cause,
                comment,
                model_version
            )
            VALUES (%s, %s, %s, %s, %s, %s)
        """, (
            transaction_id,
            user_name,
            rating,
            alternative_cause,
            comment,
            model_version
        ))

        conn.commit()
        cursor.close()
        conn.close()
        print(f"Feedback stored! (transaction_id: {transaction_id}, user: {user_name})")
        return "SUCCESS"

    except Exception as e:
        print(f"Failed to store feedback: {e}")
        return "ERROR"

def get_user_role(user_name: str) -> str:
    """
        Retrieves the role of a given user from dn_diagnostics.Users table.

        Parameters:
        -----------
        user_name : str

        Returns:
        --------
        str : role of user (e.g., Admin, User)
        None : if user not found or error occurs.

        Use Case:
        ---------
        Enables role-based authorization in application layer.
    """
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cursor = conn.cursor()
        cursor.execute("SELECT role FROM Users WHERE username = %s", (user_name,))
        row = cursor.fetchone()
        cursor.close()
        conn.close()
        return row[0] if row else None
    except Exception as e:
        print(f"Failed to get user role: {e}")
        return None
# ============================================
# RUN
# ============================================

if __name__ == "__main__":
    create_userresponse_database()
    create_analysis_table()
    create_feedback_table()

    # ---- STEP 1: check login ----
    user = check_login(
        username="Admin",
        password="dnadmin"
    )

    if user:
        # ---- STEP 2: retrieve analysis ----
        record = retrieve_analysis(
            employee_code  = user["employee_code"],
            transaction_id = "243XXXXXXXX",
            date           = "2026-02-25"
        )
        if record:
            for key, value in record.items():
                print(f"{key}: {value}")
        else:
            print("No record found.")
    else:
        print("Login failed — stopping.")


# ============================================
# GET ANALYSIS RECORDS
# Fetch records by transaction_id + employee_code + date
# ============================================

def get_analysis_records(transaction_id: str, employee_code: str):
    """
        Fetches all analysis records for a given transaction and employee.

        Parameters:
        -----------
        transaction_id : str
        employee_code : str

        Returns:
        --------
        list of dicts:
            Each dictionary represents one analysis row.

        Returns empty list if no records found.
    """
    try:
        conn = psycopg2.connect(**USERRESPONSE_DB_CONFIG)
        cursor = conn.cursor()

        cursor.execute("""
            SELECT
                transaction_id, employee_code, model,
                transaction_type, transaction_state,
                source_file, start_time, end_time,
                log_length, response_length,
                analysis_time_seconds, llm_analysis, created_at
            FROM analysis_data
            WHERE transaction_id = %s
              AND employee_code  = %s
        """, (transaction_id, employee_code))

        rows = cursor.fetchall()
        cursor.close()
        conn.close()

        if not rows:
            return []

        columns = [
            "transaction_id", "employee_code", "model",
            "transaction_type", "transaction_state",
            "source_file", "start_time", "end_time",
            "log_length", "response_length",
            "analysis_time_seconds", "llm_analysis", "created_at"
        ]
        return [dict(zip(columns, row)) for row in rows]

    except Exception as e:
        print(f"Failed to fetch records: {e}")
        return []


# ============================================
# GET FEEDBACK RECORDS
# Fetch feedback by transaction_id + user_name
# ============================================

def get_feedback_records(transaction_id: str, user_name: str):
    """
        Retrieves feedback records for a given transaction and user.

        Parameters:
        -----------
        transaction_id : str
        user_name : str

        Returns:
        --------
        list of dicts sorted by submission time (latest first).

        Returns empty list if no feedback exists.
    """
    try:
        conn = psycopg2.connect(**USERRESPONSE_DB_CONFIG)
        cursor = conn.cursor()

        cursor.execute("""
            SELECT
                transaction_id, user_name,
                rating, alternative_cause, comment,
                model_version, submitted_at
            FROM feedback
            WHERE transaction_id = %s
              AND user_name      = %s
            ORDER BY submitted_at DESC
        """, (transaction_id, user_name))

        rows = cursor.fetchall()
        cursor.close()
        conn.close()

        if not rows:
            return []

        columns = [
            "transaction_id", "user_name",
            "rating", "alternative_cause", "comment",
            "model_version", "submitted_at"
        ]
        return [dict(zip(columns, row)) for row in rows]

    except Exception as e:
        print(f"Failed to fetch feedback records: {e}")
        return []
