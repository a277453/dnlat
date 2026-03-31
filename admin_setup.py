"""
admin_setup.py - Initial Database & Admin Bootstrap Module

This module is responsible for initializing the application's
core database and administrative user setup.

Main Responsibilities:
----------------------
1. Create the 'dn_diagnostics' database if it does not exist.
2. Establish database connections.
3. Create the Users table (if missing).
4. Insert a default ADMIN user when the table is empty.
5. Provide password hashing utility.

Databases:
----------
- postgres        → Used only for database creation
- dn_diagnostics  → Main application database

Security Features:
------------------
- Passwords stored using SHA-256 hashing.
- Default admin account created only if Users table is empty.
- Users table enforces:
    - Primary key on username
    - Unique employee_code
    - Role-based access (ADMIN / USER)
    - Account activation flag (is_active)

Execution:
----------
This file is intended to be executed once during application setup.

Example:
--------
python admin_setup.py

Author: Your Name
Purpose: Application bootstrap and database initialization layer.
"""
import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
import hashlib
import os
from modules.streamlit_logger import logger as frontend_logger
import os
from dotenv import load_dotenv
load_dotenv()  # auto load from root

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
env_path = os.path.join(BASE_DIR, ".env")
if os.path.exists(env_path):
    load_dotenv(env_path)

def validate_env():
    required_vars = ["DB_HOST", "DB_NAME", "DB_USER", "DB_PASSWORD"]

    missing = [var for var in required_vars if not os.getenv(var)]

    if missing:
        raise Exception(f" Missing environment variables: {missing}")

validate_env()
# ============================================
# ADMIN (DEFAULT) DB CONFIG
# Used ONLY to create dn_diagnostics if missing
# ============================================
ADMIN_DB_CONFIG = {
    "host": os.getenv("DB_HOST", "localhost"),
    "database": "postgres",
    "user": os.getenv("Admin_DB_USER"),
    "password": os.getenv("Admin_DB_PASSWORD"),
    "port": int(os.getenv("Admin_DB_PORT", 5432))
}

# ============================================
# APP DB CONFIG
# ============================================
DB_CONFIG = {
    "host": os.getenv("DB_HOST"),
    "database": os.getenv("DB_NAME"),
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD"),
    "port": int(os.getenv("DB_PORT", 5432))
}

# ============================================
# CREATE dn_diagnostics DATABASE IF NOT EXISTS
# ============================================

def create_dn_diagnostics_database():
    """
        Creates the 'dn_diagnostics' database if it does not already exist.

        Process:
        --------
        1. Connects to the default 'postgres' database.
        2. Checks pg_database for 'dn_diagnostics'.
        3. Creates the database if not found.

        Uses:
        -----
        ISOLATION_LEVEL_AUTOCOMMIT because PostgreSQL does not allow
        CREATE DATABASE inside a transaction block.

        Logging:
        --------
        - Logs success if created.
        - Logs skip message if already exists.
        - Logs error if creation fails.

        Intended Usage:
        ---------------
        Should be executed once during application startup.
    """
    try:
        conn = psycopg2.connect(**ADMIN_DB_CONFIG)
        conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        cursor = conn.cursor()
        frontend_logger.info("Connected to default 'postgres' database")

        # Check if dn_diagnostics already exists
        cursor.execute("SELECT 1 FROM pg_database WHERE datname = 'dn_diagnostics'")
        exists = cursor.fetchone()

        if not exists:
            cursor.execute("CREATE DATABASE dn_diagnostics;")
            frontend_logger.info("Database 'dn_diagnostics' created successfully")
        else:
            frontend_logger.info("Database 'dn_diagnostics' already exists — skipping")

        cursor.close()
        conn.close()

    except Exception as e:
        frontend_logger.error(f"Failed to create 'dn_diagnostics' database: {e}")

def get_db_connection():
    """
        Establishes and returns a connection to the 'dn_diagnostics' database.

        Returns:
        --------
        connection object if successful.
        None if connection fails.

        Logging:
        --------
        Logs success message when connection is established.
        Logs error message if connection fails.

        Purpose:
        --------
        Centralized database connection handler for admin setup operations.
    """
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        frontend_logger.info("Database connection established successfully")
        return conn
    except Exception as e:
        frontend_logger.error("Database connection failed")
        return None

def hash_password(password: str) -> str:
    """
        Generates a SHA-256 hash of the provided password.

        Parameters:
        -----------
        password : str
            Plain text password.

        Returns:
        --------
        str
            Hexadecimal representation of the hashed password.

        Security:
        ---------
        Ensures passwords are not stored in plain text.
    """
    return hashlib.sha256(password.encode()).hexdigest()

def initialize_admin_table():
    """
        Generates a SHA-256 hash of the provided password.

        Parameters:
        -----------
        password : str
            Plain text password.

        Returns:
        --------
        str
            Hexadecimal representation of the hashed password.

        Security:
        ---------
        Ensures passwords are not stored in plain text.
    """
    frontend_logger.info(" initializing admin table")
    conn = get_db_connection()
    if not conn:
        frontend_logger.error("Admin table initialization aborted due to DB connection failure")
        return

    try:
        cursor = conn.cursor()

        #  Create table if not exists
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS Users (
                username VARCHAR(150) PRIMARY KEY,
                name VARCHAR(150) NOT NULL,
                password_hash TEXT NOT NULL,
                employee_code VARCHAR(8) UNIQUE NOT NULL,
                role VARCHAR(50) DEFAULT 'USER',
                is_active BOOLEAN DEFAULT FALSE,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        """)
    
        #  Check if table is empty
        cursor.execute("SELECT COUNT(*) FROM Users")
        count = cursor.fetchone()[0]

        if count == 0:
            default_users = [
                (os.getenv("ADMIN_USERNAME"),
                os.getenv("ADMIN_NAME"),
                os.getenv("ADMIN_PASSWORD"),
                os.getenv("ADMIN_EMPLOYEE_CODE"),
                os.getenv("ADMIN_ROLE"),
                os.getenv("ADMIN_IS_ACTIVE") == "True")
            ]

            for username, name, password, emp_code, role, is_active in default_users:
                cursor.execute("""
                    INSERT INTO Users (username, name, password_hash, employee_code, role, is_active)
                    VALUES (%s, %s, %s, %s, %s, %s)
                """, (username, name, hash_password(password), emp_code, role, is_active))
            frontend_logger.info(" default user created successfully")
        else:
            frontend_logger.info(" users already exist, skipping insert")

        conn.commit()
        
    except Exception as e:
        frontend_logger.exception("Error occurred while initializing admin table")
        conn.rollback()
    finally:
        conn.close()
            
        
if __name__ == "__main__":
    create_dn_diagnostics_database()
    initialize_admin_table()
