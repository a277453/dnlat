# admin_setup.py
import psycopg2
import hashlib
import os
from modules.streamlit_logger import logger as frontend_logger

DB_CONFIG = {
    "host": os.getenv("DB_HOST", "localhost"),
    "database": os.getenv("DB_NAME", "dn_diagnostics"),
    "user": os.getenv("DB_USER", "dn_user"),
    "password": os.getenv("DB_PASSWORD", "12345"),
    "port": os.getenv("DB_PORT", "5432")
}

def get_db_connection():
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        frontend_logger.info("Database connection established successfully")
        return conn
    except Exception as e:
        frontend_logger.error("Database connection failed")
        return None

def hash_password(password: str) -> str:
    return hashlib.sha256(password.encode()).hexdigest()

def initialize_admin_table():
    """Initializes the admin table and inserts default admin user if table is empty."""
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
                ("Admin", "Admin User", "dnadmin", "00000001", "ADMIN", True),
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
    initialize_admin_table()
