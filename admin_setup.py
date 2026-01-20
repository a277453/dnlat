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
        print("❌ DB connection failed:", e)
        return None

def hash_password(password: str) -> str:
    return hashlib.sha256(password.encode()).hexdigest()

def initialize_admin_table():
    """
    Creates admins table (username, password) and inserts 4 users once
    """
    conn = get_db_connection()
    if not conn:
        return

    try:
        cursor = conn.cursor()

        # 1️⃣ Create table if not exists
        # 1️⃣ Create table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS admins (
                username VARCHAR(150) PRIMARY KEY,
                name VARCHAR(150) NOT NULL,
                password_hash TEXT NOT NULL,
                employee_code VARCHAR(8) UNIQUE NOT NULL,
                role VARCHAR(50) DEFAULT 'USER',
                is_active BOOLEAN DEFAULT FALSE,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        """)

        # 2️⃣ Check if table is empty
        cursor.execute("SELECT COUNT(*) FROM admins")
        count = cursor.fetchone()[0]

        if count == 0:
            default_users = [
                ("Admin", "Admin User", "dnadmin", "00000001", "ADMIN","TRUE"),
            ]

            for email, name, password, emp_code, role, is_active in default_users:
                cursor.execute("""
                    INSERT INTO admins (username, name, password_hash, employee_code, role, is_active)
                    VALUES (%s, %s, %s, %s, %s, %s)
                """, (email, name, hash_password(password), emp_code, role, is_active))
            print("✅ default users created with password")
        else:
            print("ℹ️ Admin users already exist, skipping insert")

        conn.commit()
        cursor.close()
        conn.close()

    except Exception as e:
        print("❌ Error initializing admin table:", e)
        conn.rollback()
        conn.close()
