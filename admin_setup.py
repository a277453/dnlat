# admin_setup.py
import psycopg2
import hashlib

DB_CONFIG = {
    "host": "localhost",
    "database": "dn_diagnostics",
    "user": "dn_user",
    "password": "12345",
    "port": "5432"
}

def get_db_connection():
    try:
        return psycopg2.connect(**DB_CONFIG)
    except Exception as e:
        print("  DB connection failed:", e)
        return None

def hash_password(password: str) -> str:
    return hashlib.sha256(password.encode()).hexdigest()

def initialize_admin_table():
    """Initializes the admin table and inserts default admin user if table is empty."""
    print("  initializing admin table")
    conn = get_db_connection()
    if not conn:
        return

    try:
        cursor = conn.cursor()

        # 1️ Create table if not exists
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
    
        #   Check if table is empty
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
            print("  default user created with password")
        else:
            print("  users already exist, skipping insert")

        conn.commit()
        cursor.execute("select * from Users;")
        print("currentusers:",cursor.fetchall())
    
        cursor.close()
        conn.close()

    except Exception as e:
        print("  Error initializing admin table:", e)
        conn.rollback()
        conn.close()
if __name__ == "__main__":
    initialize_admin_table()
