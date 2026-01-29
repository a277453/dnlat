# admin_setup.py
import psycopg2
import hashlib
from modules.logging_config import logger

DB_CONFIG = {
    "host": "localhost",
    "database": "dn_diagnostics",
    "user": "dn_user",
    "password": "12345",
    "port": "5432"
}

def get_db_connection():
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        logger.info("Database connection established successfully")
        return conn
    except Exception as e:
        logger.error("Database connection failed", exc_info=True)
        return None

def hash_password(password: str) -> str:
    return hashlib.sha256(password.encode()).hexdigest()

def initialize_admin_table():
    """Initializes the admin table and inserts default admin user if table is empty."""
    logger.info(" initializing admin table")
    conn = get_db_connection()
    if not conn:
        logger.error("Admin table initialization aborted due to DB connection failure")
        return

    try:
        cursor = conn.cursor()

        #  Create table if not exists
        #  Create table
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

            for email, name, password, emp_code, role, is_active in default_users:
                cursor.execute("""
                    INSERT INTO Users (username, name, password_hash, employee_code, role, is_active)
                    VALUES (%s, %s, %s, %s, %s, %s)
                """, (email, name, hash_password(password), emp_code, role, is_active))
            logger.info(" default user created with password")
        else:
            logger.info(" users already exist, skipping insert")

        conn.commit()
        
    except Exception as e:
        logger.error("Error occurred while initializing admin table", exc_info=True)
        conn.rollback()
    finally:
        if conn:
            conn.close()
            logger.info("Database connection closed")
        
if __name__ == "__main__":
    initialize_admin_table()
