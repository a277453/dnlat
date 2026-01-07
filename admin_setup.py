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
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS admins (
                username VARCHAR(50) PRIMARY KEY,
                password_hash VARCHAR(256) NOT NULL
            );
        """)

        # 2️⃣ Check if table is empty
        cursor.execute("SELECT COUNT(*) FROM admins")
        count = cursor.fetchone()[0]

        if count == 0:
            users = ["Admin", "Atharv.Deshpande@dieboldnixdorf.com", "Ashish.Trivedi@dieboldnixdorf.com", "Test_user"]
            password_hash = hash_password("dnadmin")

            for user in users:
                cursor.execute("""
                    INSERT INTO admins (username, password_hash)
                    VALUES (%s, %s)
                """, (user, password_hash))

            print("✅ 4 default users created with password 'dnadmin'")
        else:
            print("ℹ️ Admin users already exist, skipping insert")

        conn.commit()
        cursor.close()
        conn.close()

    except Exception as e:
        print("❌ Error initializing admin table:", e)
        conn.rollback()
        conn.close()
