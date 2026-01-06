"""
login.py - Authentication and Database Logic
Complete version with all required functions
"""

import psycopg2
import hashlib
import streamlit as st
from typing import Optional, Tuple

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
    """
    Establish connection to PostgreSQL database
    
    Returns:
        psycopg2.connection: Database connection object or None if failed
    """
    try:
        conn = psycopg2.connect(
            host=DB_CONFIG["host"],
            database=DB_CONFIG["database"],
            user=DB_CONFIG["user"],
            password=DB_CONFIG["password"],
            port=DB_CONFIG["port"]
        )
        return conn
    except psycopg2.Error as e:
        print(f"❌ Database connection failed: {str(e)}")
        return None
    except Exception as e:
        print(f"❌ Unexpected error: {str(e)}")
        return None

def test_db_connection() -> bool:
    """
    Test if database connection is working
    
    Returns:
        bool: True if connection successful, False otherwise
    """
    conn = get_db_connection()
    if conn:
        try:
            conn.close()
            return True
        except:
            return False
    return False

# ============================================
# AUTHENTICATION FUNCTIONS
# ============================================

def hash_password(password: str) -> str:
    """
    Hash password using SHA-256
    
    Args:
        password (str): Plain text password
        
    Returns:
        str: Hashed password
    """
    return hashlib.sha256(password.encode()).hexdigest()

def verify_credentials(username: str, password: str) -> Optional[Tuple[str, str, str]]:
    """
    Verify user credentials against database
    
    Args:
        username (str): Username to verify
        password (str): Password to verify
        
    Returns:
        Tuple[str, str, str]: (username, name, role) if valid, None otherwise
    """
    conn = get_db_connection()
    if not conn:
        return None

    try:
        cursor = conn.cursor()
        password_hash = hash_password(password)

        """# 🔍 DEBUG PRINTS (ADD THESE)
        print("USERNAME ENTERED:", username)
        print("PASSWORD ENTERED:", password)
        print("HASH GENERATED :", password_hash)

        cursor.execute(
            "SELECT password_hash FROM admins WHERE username = %s",
            (username,)
        )
        db_hash = cursor.fetchone()
        print("HASH FROM DB  :", db_hash)"""

        # Query to verify credentials
        query = """
            SELECT username, name, role
            FROM admins
            WHERE username = %s AND password_hash = %s
        """
        
        cursor.execute(query, (username, password_hash))
        user = cursor.fetchone()
        
        cursor.close()
        conn.close()

        return user  # Returns (username, name, role) or None

    except psycopg2.Error as e:
        print(f"❌ Database error during login: {str(e)}")
        return None
    except Exception as e:
        print(f"❌ Unexpected error during login: {str(e)}")
        return None

def authenticate_user(username: str, password: str) -> bool:
    """
    Authenticate user and update session state
    
    Args:
        username (str): Username
        password (str): Password
        
    Returns:
        bool: True if authentication successful, False otherwise
    """
    user = verify_credentials(username, password)
    
    if user:
        # Store user information in session
        st.session_state.username = user[0]
        st.session_state.name = user[1]
        st.session_state.role = user[2]
        st.session_state.logged_in = True
        return True
    
    return False

# ============================================
# SESSION MANAGEMENT
# ============================================

def initialize_session():
    """
    Initialize session state variables for authentication
    THIS FUNCTION IS REQUIRED BY streamlit_app.py
    """
    if "logged_in" not in st.session_state:
        st.session_state.logged_in = False
    if "username" not in st.session_state:
        st.session_state.username = None
    if "name" not in st.session_state:
        st.session_state.name = None
    if "role" not in st.session_state:
        st.session_state.role = None

def is_logged_in() -> bool:
    """
    Check if user is logged in
    THIS FUNCTION IS REQUIRED BY streamlit_app.py
    
    Returns:
        bool: True if user is logged in, False otherwise
    """
    return st.session_state.get('logged_in', False)

def get_current_user() -> dict:
    """
    Get current logged in user information
    THIS FUNCTION IS REQUIRED BY streamlit_app.py
    
    Returns:
        dict: User information (username, name, role)
    """
    return {
        'username': st.session_state.get('username'),
        'name': st.session_state.get('name'),
        'role': st.session_state.get('role')
    }

def logout_user():
    """
    Logout user and clear session state
    THIS FUNCTION IS REQUIRED BY streamlit_app.py
    """
    st.session_state.logged_in = False
    st.session_state.username = None
    st.session_state.name = None
    st.session_state.role = None

# ============================================
# USER MANAGEMENT (Optional - for future use)
# ============================================

def create_user(username: str, password: str, name: str, role: str = 'user') -> bool:
    """
    Create a new user in the database
    
    Args:
        username (str): Username
        password (str): Plain text password (will be hashed)
        name (str): Full name
        role (str): User role (default: 'user')
        
    Returns:
        bool: True if user created successfully, False otherwise
    """
    conn = get_db_connection()
    if not conn:
        return False

    try:
        cursor = conn.cursor()
        password_hash = hash_password(password)

        query = """
            INSERT INTO admins (username, password_hash, name, role)
            VALUES (%s, %s, %s, %s)
        """
        
        cursor.execute(query, (username, password_hash, name, role))
        conn.commit()
        
        cursor.close()
        conn.close()
        
        return True

    except psycopg2.IntegrityError:
        print(f"❌ Username '{username}' already exists")
        return False
    except Exception as e:
        print(f"❌ Error creating user: {str(e)}")
        return False

def get_all_users() -> list:
    """
    Get all users from database (admin function)
    
    Returns:
        list: List of tuples (username, name, role)
    """
    conn = get_db_connection()
    if not conn:
        return []

    try:
        cursor = conn.cursor()
        cursor.execute("SELECT username, name, role FROM admins")
        users = cursor.fetchall()
        
        cursor.close()
        conn.close()
        
        return users

    except Exception as e:
        print(f"❌ Error fetching users: {str(e)}")
        return []

# ============================================
# TESTING / DEBUGGING
# ============================================

if __name__ == "__main__":
    """
    Test the authentication system
    Run: python login.py
    """
    print("\n" + "="*60)
    print("TESTING AUTHENTICATION SYSTEM")
    print("="*60 + "\n")
    
    # Test 1: Database connection
    print("Test 1: Database Connection")
    print("-" * 60)
    if test_db_connection():
        print("✅ Database connection successful")
    else:
        print("❌ Database connection failed")
        print("\nTroubleshooting:")
        print("  1. Check if PostgreSQL is running")
        print("  2. Verify credentials in DB_CONFIG (lines 15-21)")
        print("  3. Ensure database 'dn_diagnostics' exists")
    
    # Test 2: Check if all required functions exist
    print("\nTest 2: Required Functions")
    print("-" * 60)
    required_functions = [
        'get_db_connection',
        'authenticate_user',
        'initialize_session',
        'is_logged_in',
        'logout_user',
        'get_current_user'
    ]
    
    all_exist = True
    for func_name in required_functions:
        if func_name in dir():
            print(f"✅ {func_name}()")
        else:
            print(f"❌ {func_name}() - MISSING!")
            all_exist = False
    
    if not all_exist:
        print("\n⚠️  Some functions are missing! Check your login.py file.")
    
    # Test 3: Authentication (optional - update with real credentials)
    print("\nTest 3: Authentication")
    print("-" * 60)
    print("⚠️  Update test_username and test_password below to test login")
    print("    Lines 305-306 in this file")
    
    test_username = "dn_diagnostic"      
    test_password = "12345"   
    
    user = verify_credentials(test_username, test_password)
    if user:
        print(f"✅ Authentication successful for '{test_username}'")
        print(f"   Username: {user[0]}")
        print(f"   Name: {user[1]}")
        print(f"   Role: {user[2]}")
    else:
        print(f"❌ Authentication failed for '{test_username}'")
        print("   (This is expected if you haven't updated test credentials)")
    
    print("\n" + "="*60)
    print("TESTING COMPLETE")
    print("="*60 + "\n")
    
    if test_db_connection() and all_exist:
        print("✅ Ready to run: streamlit run streamlit_app.py")
    else:
        print("⚠️  Fix the issues above before running streamlit")
    print()