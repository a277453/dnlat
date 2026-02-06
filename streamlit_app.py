import streamlit as st
import requests
import json
import pandas as pd
from pathlib import Path
import io
import zipfile
import re
import plotly.express as px
import os  
from datetime import datetime  
import numpy as np
from fastapi.logger import logger
from modules.streamlit_logger import logger as frontend_logger
import time
from modules.login import register_user
from modules.newuser_constant import NewUserUI


# Import authentication functions
from admin_setup import initialize_admin_table
from modules.login import (
    create_login_history_table,
    initialize_session,
    is_logged_in,
    authenticate_user,
    is_user_pending_approval,
    logout_user,
    get_current_user
)
# IMPORTANT 
initialize_session()

frontend_logger.info("Streamlit app loaded")

# ============================================
# PAGE CONFIGURATION (MUST BE FIRST!)
# ============================================
st.set_page_config(
    page_title="DN Diagnostics Platform",
    page_icon="⚙️",
    layout="wide",
    initial_sidebar_state="expanded"
)

# ============================================
# THEME INITIALIZATION
# ============================================
# Initialize theme in session state
if 'theme' not in st.session_state:
    st.session_state.theme = 'dark'  # Default to dark theme

# ============================================
# THEME TOGGLE FUNCTION
# ============================================
def toggle_theme():
    """Toggle between light and dark theme"""
    st.session_state.theme = 'light' if st.session_state.theme == 'dark' else 'dark'

# ============================================
# THEME-SPECIFIC STYLES
# ============================================
def get_theme_styles():
    """Return CSS styles based on current theme"""
    
    if st.session_state.theme == 'dark':
        return """
        <style>
        /* Completely hide all Streamlit branding and deploy button */
        #MainMenu {visibility: hidden !important;}
        footer {visibility: hidden !important;}
        header {visibility: hidden !important;}
        
        [data-testid="stToolbar"] {
            display: none !important;
        }
        
        [data-testid="stDecoration"] {
            display: none !important;
        }
        
        button[kind="header"] {
            display: none !important;
        }
        
        .css-1dp5vir {
            display: none !important;
        }
        
        /* Theme Toggle Button Container */
        .theme-toggle-container {
            position: fixed;
            top: 1rem;
            right: 1rem;
            z-index: 999999;
        }
        
        /* Global Styles - DARK THEME */
        .main {
            background-color: #0a0a0a;
            color: #e0e0e0;
        }
        
        .block-container {
            padding-top: 2rem;
            padding-bottom: 2rem;
            max-width: 1400px;
        }
        
        /* Sidebar Styles */
        [data-testid="stSidebar"] {
            background: linear-gradient(180deg, #1a1a1a 0%, #0f0f0f 100%);
            border-right: 1px solid #2a2a2a;
        }
        
        /* Typography */
        h1 {
            color: #ffffff !important;
            font-size: 2.5rem !important;
            font-weight: 700 !important;
            margin-bottom: 0.5rem !important;
        }
        
        h2 {
            color: #ffffff !important;
            font-size: 1.75rem !important;
            font-weight: 600 !important;
            margin: 2rem 0 1rem 0 !important;
            border-bottom: 2px solid #2563eb;
            padding-bottom: 0.5rem;
        }
        
        h3 {
            color: #e0e0e0 !important;
            font-size: 1.25rem !important;
            font-weight: 600 !important;
        }
        
        /* Button Styles */
        .stButton > button {
            background: linear-gradient(135deg, #2563eb 0%, #1d4ed8 100%);
            color: #ffffff;
            border: none;
            padding: 0.75rem 2rem;
            border-radius: 8px;
            font-weight: 600;
            font-size: 0.95rem;
            transition: all 0.3s ease;
            box-shadow: 0 4px 6px rgba(37, 99, 235, 0.2);
            width: 100%;
            height: 48px;
        }
        
        .stButton > button:hover {
            background: linear-gradient(135deg, #1d4ed8 0%, #1e40af 100%);
            box-shadow: 0 6px 12px rgba(37, 99, 235, 0.35);
            transform: translateY(-1px);
        }
        
        /* File Uploader */
        [data-testid="stFileUploader"] {
            background-color: #1a1a1a;
            border: 2px dashed #404040;
            border-radius: 12px;
            padding: 2rem;
            transition: all 0.3s ease;
        }
        
        [data-testid="stFileUploader"]:hover {
            border-color: #2563eb;
            background-color: #1f1f1f;
        }
        
        /* Select Box */
        .stSelectbox > div > div {
            background-color: #1a1a1a;
            border: 1px solid #404040;
            border-radius: 8px;
            color: #e0e0e0;
            height: 48px;
        }
        
        /* Text Input */
        .stTextInput > div > div > input {
            background-color: #1a1a1a;
            border: 1px solid #404040;
            border-radius: 8px;
            color: #e0e0e0;
            height: 48px;
            padding: 0 1rem;
        }
        
        /* Metric Cards */
        [data-testid="stMetricValue"] {
            font-size: 1.75rem;
            font-weight: 700;
            color: #ffffff;
        }
        
        [data-testid="stMetricLabel"] {
            font-size: 0.875rem;
            color: #9ca3af;
            text-transform: uppercase;
            letter-spacing: 0.5px;
            font-weight: 500;
        }
        
        /* Data Tables */
        .dataframe {
            border: 1px solid #2a2a2a !important;
            border-radius: 8px;
            overflow: hidden;
        }
        
        .dataframe thead tr th {
            background-color: #1a1a1a !important;
            color: #ffffff !important;
            font-weight: 600 !important;
            text-transform: uppercase;
            font-size: 0.75rem;
            padding: 1rem !important;
        }
        
        .dataframe tbody tr:hover {
            background-color: #1f1f1f !important;
        }
        </style>
        """
    else:  # Light theme
        return """
        <style>
        /* Completely hide all Streamlit branding and deploy button */
        #MainMenu {visibility: hidden !important;}
        footer {visibility: hidden !important;}
        header {visibility: hidden !important;}
        
        [data-testid="stToolbar"] {
            display: none !important;
        }
        
        [data-testid="stDecoration"] {
            display: none !important;
        }
        
        button[kind="header"] {
            display: none !important;
        }
        
        .css-1dp5vir {
            display: none !important;
        }
        
        /* Theme Toggle Button Container */
        .theme-toggle-container {
            position: fixed;
            top: 1rem;
            right: 1rem;
            z-index: 999999;
        }
        
        /* Global Styles - LIGHT THEME */
        .stApp {
            background-color: #f8f9fa !important;
        }
        
        .main {
            background-color: #f8f9fa !important;
            color: #1f2937 !important;
        }
        
        .block-container {
            padding-top: 2rem;
            padding-bottom: 2rem;
            max-width: 1400px;
            background-color: #f8f9fa !important;
        }
        
        /* Sidebar Styles */
        [data-testid="stSidebar"] {
            background: linear-gradient(180deg, #ffffff 0%, #f3f4f6 100%) !important;
            border-right: 1px solid #e5e7eb !important;
        }
        
        [data-testid="stSidebar"] > div:first-child {
            background: linear-gradient(180deg, #ffffff 0%, #f3f4f6 100%) !important;
        }
        
        /* Typography */
        h1 {
            color: #111827 !important;
            font-size: 2.5rem !important;
            font-weight: 700 !important;
            margin-bottom: 0.5rem !important;
        }
        
        h2 {
            color: #111827 !important;
            font-size: 1.75rem !important;
            font-weight: 600 !important;
            margin: 2rem 0 1rem 0 !important;
            border-bottom: 2px solid #2563eb;
            padding-bottom: 0.5rem;
        }
        
        h3 {
            color: #374151 !important;
            font-size: 1.25rem !important;
            font-weight: 600 !important;
        }
        
        /* All text elements */
        p, span, div, label {
            color: #374151 !important;
        }
        
        /* Input labels */
        .stTextInput > label,
        [data-testid="stWidgetLabel"] {
            color: #111827 !important;
            font-weight: 500 !important;
            font-size: 0.95rem !important;
        }
        
        /* Form labels */
        label {
            color: #111827 !important;
        }
        
        [data-testid="stMarkdownContainer"] {
            color: #374151 !important;
        }
        
        [data-testid="stMarkdownContainer"] p {
            color: #374151 !important;
        }
        
        /* Button Styles */
        .stButton > button {
            background: linear-gradient(135deg, #3b82f6 0%, #2563eb 100%) !important;
            color: #ffffff !important;
            border: none !important;
            padding: 0.75rem 2rem;
            border-radius: 8px;
            font-weight: 600;
            font-size: 1rem;
            transition: all 0.3s ease;
            box-shadow: 0 4px 12px rgba(59, 130, 246, 0.4);
            width: 100%;
            height: 48px;
        }
        
        .stButton > button:hover {
            background: linear-gradient(135deg, #2563eb 0%, #1d4ed8 100%) !important;
            box-shadow: 0 6px 16px rgba(37, 99, 235, 0.5);
            transform: translateY(-2px);
        }
        
        /* Form submit button styling */
        .stButton > button[kind="primary"],
        .stButton > button[type="submit"] {
            background: linear-gradient(135deg, #3b82f6 0%, #2563eb 100%) !important;
            color: #ffffff !important;
            font-weight: 700 !important;
        }
        
        /* Form Elements */
        [data-baseweb="base-input"] {
            background-color: #ffffff !important;
        }
        
        /* File Uploader */
        [data-testid="stFileUploader"] {
            background-color: #ffffff !important;
            border: 2px dashed #d1d5db !important;
            border-radius: 12px;
            padding: 2rem;
            transition: all 0.3s ease;
        }
        
        [data-testid="stFileUploader"]:hover {
            border-color: #2563eb !important;
            background-color: #f9fafb !important;
        }
        
        [data-testid="stFileUploader"] section {
            background-color: #ffffff !important;
        }
        
        /* Select Box */
        .stSelectbox > div > div {
            background-color: #ffffff !important;
            border: 1px solid #d1d5db !important;
            border-radius: 8px;
            color: #1f2937 !important;
            height: 48px;
        }
        
        .stSelectbox [data-baseweb="select"] {
            background-color: #ffffff !important;
        }
        
        .stSelectbox [data-baseweb="select"] > div {
            background-color: #ffffff !important;
            border-color: #d1d5db !important;
        }
        
        /* Text Input */
        .stTextInput > div > div > input {
            background-color: #ffffff !important;
            border: 1px solid #d1d5db !important;
            border-radius: 8px;
            color: #1f2937 !important;
            height: 48px;
            padding: 0 1rem;
        }
        
        .stTextInput input {
            background-color: #ffffff !important;
            color: #1f2937 !important;
            border: 1px solid #d1d5db !important;
        }
        
        /* Placeholder text */
        .stTextInput input::placeholder {
            color: #6b7280 !important;
            opacity: 1 !important;
        }
        
        .stTextInput input::-webkit-input-placeholder {
            color: #6b7280 !important;
            opacity: 1 !important;
        }
        
        .stTextInput input::-moz-placeholder {
            color: #6b7280 !important;
            opacity: 1 !important;
        }
        
        /* Password Input */
        [data-testid="stTextInput"] input[type="password"] {
            background-color: #ffffff !important;
            color: #1f2937 !important;
            border: 1px solid #d1d5db !important;
        }
        
        [data-testid="stTextInput"] input[type="password"]::placeholder {
            color: #6b7280 !important;
            opacity: 1 !important;
        }
        
        /* Password visibility toggle button (eye icon) */
        button[data-testid="baseButton-header"] {
            background-color: #f3f4f6 !important;
            color: #1f2937 !important;
            border: 1px solid #d1d5db !important;
        }
        
        button[data-testid="baseButton-header"]:hover {
            background-color: #e5e7eb !important;
            border-color: #9ca3af !important;
        }
        
        button[data-testid="baseButton-header"] svg {
            color: #374151 !important;
            fill: #374151 !important;
        }
        
        /* Password input container */
        [data-testid="stTextInput"] button {
            background-color: #f3f4f6 !important;
            border-color: #d1d5db !important;
        }
        
        [data-testid="stTextInput"] button svg {
            color: #374151 !important;
        }
        
        [data-testid="stTextInput"] button:hover {
            background-color: #e5e7eb !important;
        }
        
        /* Metric Cards */
        [data-testid="stMetricValue"] {
            font-size: 1.75rem;
            font-weight: 700;
            color: #111827 !important;
        }
        
        [data-testid="stMetricLabel"] {
            font-size: 0.875rem;
            color: #6b7280 !important;
            text-transform: uppercase;
            letter-spacing: 0.5px;
            font-weight: 500;
        }
        
        /* Metric containers */
        [data-testid="metric-container"] {
            background-color: #ffffff !important;
            border: 1px solid #e5e7eb !important;
            border-radius: 8px;
            padding: 1rem;
        }
        
        /* Data Tables */
        .dataframe {
            border: 1px solid #e5e7eb !important;
            border-radius: 8px;
            overflow: hidden;
            background-color: #ffffff !important;
        }
        
        .dataframe thead tr th {
            background-color: #f3f4f6 !important;
            color: #111827 !important;
            font-weight: 600 !important;
            text-transform: uppercase;
            font-size: 0.75rem;
            padding: 1rem !important;
        }
        
        .dataframe tbody tr {
            background-color: #ffffff !important;
        }
        
        .dataframe tbody tr:hover {
            background-color: #f9fafb !important;
        }
        
        .dataframe tbody tr td {
            color: #374151 !important;
        }
        
        /* Expander */
        [data-testid="stExpander"] {
            background-color: #ffffff !important;
            border: 1px solid #e5e7eb !important;
            border-radius: 8px;
        }
        
        /* Tabs */
        .stTabs [data-baseweb="tab-list"] {
            background-color: #f3f4f6 !important;
        }
        
        .stTabs [data-baseweb="tab"] {
            background-color: #f3f4f6 !important;
            color: #374151 !important;
        }
        
        .stTabs [aria-selected="true"] {
            background-color: #ffffff !important;
            color: #2563eb !important;
        }
        
        /* Radio buttons */
        .stRadio > label {
            color: #374151 !important;
        }
        
        .stRadio [data-testid="stMarkdownContainer"] {
            color: #374151 !important;
        }
        
        /* Info/Warning/Error boxes */
        .stAlert {
            background-color: #ffffff !important;
            border: 1px solid #e5e7eb !important;
            color: #374151 !important;
        }
        
        /* Spinner */
        .stSpinner > div {
            border-top-color: #2563eb !important;
        }
        
        /* Code blocks */
        .stCodeBlock {
            background-color: #f3f4f6 !important;
        }
        
        code {
            background-color: #f3f4f6 !important;
            color: #1f2937 !important;
        }
        
        /* Container backgrounds */
        [data-testid="stVerticalBlock"] > div {
            background-color: transparent !important;
        }
        
        [data-testid="column"] {
            background-color: transparent !important;
        }
        
        /* Form container */
        [data-testid="stForm"] {
            background-color: #ffffff !important;
            border: 1px solid #e5e7eb !important;
            border-radius: 12px;
            padding: 2rem;
        }
        
        /* Caption text */
        .css-16huue1, .css-1om1ktf {
            color: #6b7280 !important;
        }
        </style>
        """

# Apply theme styles
st.markdown(get_theme_styles(), unsafe_allow_html=True)

# ============================================
# THEME TOGGLE BUTTON UI
# ============================================
# Create single toggle switch button
col1, col2 = st.columns([10, 1])
with col2:
    # Determine current state
    is_light = st.session_state.theme == 'light'
    toggle_bg = "#2196F3" if is_light else "#555555"
    circle_left = "33px" if is_light else "3px"
    
    # Single toggle switch with custom styling
    st.markdown(f"""
    <style>
    /* Style the single toggle button */
    .stButton[data-baseweb="button"] button {{
        background: {toggle_bg} !important;
        border: none !important;
        border-radius: 15px !important;
        width: 60px !important;
        height: 30px !important;
        padding: 0 !important;
        position: relative !important;
        cursor: pointer !important;
        transition: background 0.3s ease !important;
    }}
    
    .stButton[data-baseweb="button"] button:hover {{
        background: {toggle_bg} !important;
        opacity: 0.9 !important;
        box-shadow: none !important;
    }}
    
    /* White circle slider */
    .stButton[data-baseweb="button"] button::before {{
        content: '' !important;
        position: absolute !important;
        width: 24px !important;
        height: 24px !important;
        background: white !important;
        border-radius: 50% !important;
        top: 3px !important;
        left: {circle_left} !important;
        transition: left 0.3s ease !important;
        box-shadow: 0 1px 3px rgba(0,0,0,0.2) !important;
    }}
    
    /* Hide button text */
    .stButton[data-baseweb="button"] button div[data-testid="stMarkdownContainer"] {{
        display: none !important;
    }}
    </style>
    """, unsafe_allow_html=True)
    
    # Single button that acts as toggle
    if st.button("", key="theme_toggle", help="Dark/Light"):
        toggle_theme()
        st.rerun()

# ============================================
# GLOBAL VARIABLES
# ============================================
API_BASE_URL = "http://localhost:8000/api/v1"

# ============================================
# LOGIN PAGE UI
# ============================================

def show_login_page():
    """
    Display login page UI
    """
    # Center the login form
    col1, col2, col3 = st.columns([1, 2, 1])

    with col2:
        st.markdown(
            f"<h2 style='text-align:center; margin-top:100px;'>{NewUserUI.LOGIN_TITLE.value}</h2>", 
            unsafe_allow_html=True
        )
        st.markdown(NewUserUI.HTML_BREAK.value, unsafe_allow_html=True)

        # Login form
        with st.form(NewUserUI.LOGIN_FORM_KEY.value):
            username = st.text_input(
                NewUserUI.LOGIN_USERNAME_LABEL.value, 
                placeholder=NewUserUI.LOGIN_USERNAME_PLACEHOLDER.value,
                key=NewUserUI.LOGIN_USERNAME_KEY.value
            )
            password = st.text_input(
                NewUserUI.LOGIN_PASSWORD_LABEL.value, 
                type="password",
                placeholder=NewUserUI.LOGIN_PASSWORD_PLACEHOLDER.value,
                key=NewUserUI.LOGIN_PASSWORD_KEY.value
            )
            submit = st.form_submit_button(NewUserUI.LOGIN_BUTTON.value, use_container_width=True)

        # Handle login
        if submit:
            if not username or not password:
                st.error(NewUserUI.LOGIN_EMPTY_ERROR.value)
            else:
                with st.spinner(NewUserUI.AUTHENTICATING_SPINNER.value):
                    if authenticate_user(username, password):
                        user = get_current_user()
                        st.success(
                            NewUserUI.LOGIN_SUCCESS.value.format(
                                username=user["username"]
                            )
                        )

                        st.session_state[NewUserUI.SESSION_LOGIN_SUCCESS.value] = True
                        st.session_state[NewUserUI.SESSION_USERNAME.value] = user["username"]

                        st.rerun()  # Reload to show main app
                    elif is_user_pending_approval(username, password):
                        st.warning(
                            NewUserUI.LOGIN_PENDING_WARNING.value.format(username=username)
                        )

                    else:
                        st.error(NewUserUI.LOGIN_INVALID_ERROR.value)
        # ---------------------------
        # REGISTER BUTTON (NEW)
        # ---------------------------
        st.markdown(NewUserUI.HTML_BREAK.value, unsafe_allow_html=True)

        if st.button(NewUserUI.REGISTER_BUTTON.value, use_container_width=True):
            st.session_state[NewUserUI.SESSION_PAGE.value]= NewUserUI.PAGE_REGISTER.value
            st.rerun()
def is_invalid_emp_code(emp_code: str) -> bool:
    """
    Check if employee code is sequential or repeating pattern.
    Returns True if invalid.
    """
    if len(emp_code) != 8 or not emp_code.isdigit():
        return True

    # Check if all digits are same
    if len(set(emp_code)) == 1:
        return True

    # Check sequential ascending
    if emp_code in "01234567890123456789":
        return True

    # Check sequential descending
    if emp_code in "98765432109876543210":
        return True

    # Check repeating pairs like 11223344
    pairs = [emp_code[i:i+2] for i in range(0, 8, 2)]
    if all(pair[0] == pair[1] for pair in pairs):
        return True

    return False

def is_valid_password(password: str) -> bool:
    """
    Validates password strength.
    Rules:
    - Minimum 8 characters
    - At least 1 uppercase letter
    - At least 1 lowercase letter
    - At least 2 digits
    - At least 1 special character
    """
    if len(password) < 8:
        return False

    if not any(c.isupper() for c in password):
        return False

    if not any(c.islower() for c in password):
        return False

    #  MINIMUM 2 DIGITS CHECK
    if sum(c.isdigit() for c in password) < 2:
        return False

    if not any(c in "!@#$%^&*()-_=+[]{}|;:'\",.<>?/`~" for c in password):
        return False

    return True


def show_register_page():
    """
    Display registration page UI
    """
    col1, col2, col3 = st.columns([1, 2, 1])

    with col2:
        st.markdown(
            f"<h2 style='text-align:center; margin-top:100px;'>{NewUserUI.REGISTER_TITLE.value}</h2>",
            unsafe_allow_html=True
        )
        st.markdown(NewUserUI.HTML_BREAK.value, unsafe_allow_html=True)

        with st.form(NewUserUI.REGISTER_FORM_KEY.value):
            email = st.text_input(
                NewUserUI.REGISTER_EMAIL_LABEL.value,
                placeholder=NewUserUI.REGISTER_EMAIL_PLACEHOLDER.value,
                key=NewUserUI.REGISTER_EMAIL_KEY.value
            )
            name = st.text_input(
                NewUserUI.REGISTER_NAME_LABEL.value,
                placeholder=NewUserUI.REGISTER_NAME_PLACEHOLDER.value,
                key=NewUserUI.REGISTER_NAME_KEY.value
            )
            password = st.text_input(
                NewUserUI.REGISTER_PASSWORD_LABEL.value,
                type="password",
                placeholder=NewUserUI.REGISTER_PASSWORD_PLACEHOLDER.value,
                key=NewUserUI.REGISTER_PASSWORD_KEY.value
            )
            confirm_password = st.text_input(
                NewUserUI.REGISTER_CONFIRM_PASSWORD_LABEL.value,
                type="password",
                placeholder=NewUserUI.REGISTER_CONFIRM_PASSWORD_PLACEHOLDER.value,
                key=NewUserUI.REGISTER_CONFIRM_PASSWORD_KEY.value
            )
            employee_code = st.text_input(
                NewUserUI.REGISTER_EMP_CODE_LABEL.value,
                placeholder=NewUserUI.REGISTER_EMP_CODE_PLACEHOLDER.value,
                key=NewUserUI.REGISTER_EMP_CODE_KEY.value
            )
            role_type = st.text_input(
                NewUserUI.REGISTER_ROLE_LABEL.value,
                placeholder=NewUserUI.REGISTER_DEFAULT_ROLE.value,
                key=NewUserUI.REGISTER_ROLE_KEY.value,
                disabled=True
            )

            submit = st.form_submit_button(NewUserUI.REGISTER_SUBMIT_BUTTON.value, use_container_width=True)

        # ---------------------------
        # FORM SUBMIT HANDLING
        # ---------------------------
        if submit:
            email_pattern = NewUserUI.EMAIL_PATTERN.value
            name_pattern = NewUserUI.NAME_PATTERN.value

            email = email.strip().lower()
            name = name.strip().title()
            
            if not all([email, name, password, confirm_password, employee_code]):
                st.error(NewUserUI.ALL_FIELDS_REQUIRED.value)

            elif not re.match(email_pattern, email):
                st.error(NewUserUI.INVALID_EMAIL.value)

            elif not re.match(name_pattern, name):
                st.error(NewUserUI.INVALID_NAME.value)    
            
            elif not is_valid_password(password):
                st.error(NewUserUI.PASSWORD_RULES_ERROR.value)

            elif password != confirm_password:
                st.error(NewUserUI.PASSWORD_MISMATCH.value)

            elif is_invalid_emp_code(employee_code):
                st.error(NewUserUI.INVALID_EMP_CODE.value)

            else:
                # ---------------------------
                # BACKEND REGISTRATION
                # ---------------------------
                try:
                    success, message = register_user(
                        email, name, password, employee_code, role_type 
                    )

                    if success:
                        st.success(message)
                        time.sleep(3)
                        st.session_state[NewUserUI.SESSION_PAGE.value] = NewUserUI.PAGE_LOGIN.value
                        st.rerun()
                    else:
                        st.error(message)

                except RuntimeError:
                    # DB / infra issue
                    st.error(NewUserUI.SERVICE_UNAVAILABLE.value)

                except Exception:
                    # Unexpected failure
                    st.error(NewUserUI.REGISTRATION_INTERNAL_ERROR.value)
                
                
        # ---------------------------
        # BACK TO LOGIN
        # ---------------------------
        if st.button(NewUserUI.BACK_TO_LOGIN_BUTTON.value, use_container_width=True):
            st.session_state[NewUserUI.SESSION_PAGE.value] = NewUserUI.PAGE_LOGIN.value
            st.rerun()


def create_comparison_flow_plotly(txn1_id, txn1_state, txn1_flow_screens, txn1_matches,
                                   txn2_id, txn2_state, txn2_flow_screens, txn2_matches):
    """
    FUNCTION:
        create_comparison_flow_plotly
    DESCRIPTION:
        Creates a side-by-side Plotly visualization that compares the screen-flow
        of two transactions. Each screen is displayed as a colored box where:
            - Blue  : Matched screen between both transactions
            - Orange: Non-matched screen  
        The function generates a subplot with Transaction 1 on the left and 
        Transaction 2 on the right, showing the flow sequence visually.

    USAGE:
        fig = create_comparison_flow_plotly(
                txn1_id="TXN001",
                txn1_state="APPROVED",
                txn1_flow_screens=["Home", "PIN", "Withdraw"],
                txn1_matches=[True, False, True],
                txn2_id="TXN002",
                txn2_state="DECLINED",
                txn2_flow_screens=["Home", "PIN", "Amount"],
                txn2_matches=[True, False, False]
        )

    PARAMETERS:
        txn1_id (str) :
            Transaction 1 identifier.
        txn1_state (str) :
            Final state of Transaction 1 (e.g., APPROVED/DECLINED).
        txn1_flow_screens (list) :
            Ordered list of screen names representing Transaction 1 flow.
        txn1_matches (list[bool]) :
            List indicating whether each screen of Transaction 1 matches
            the corresponding screen of Transaction 2.

        txn2_id (str) :
            Transaction 2 identifier.
        txn2_state (str) :
            Final state of Transaction 2.
        txn2_flow_screens (list) :
            Ordered list of screen names representing Transaction 2 flow.
        txn2_matches (list[bool]) :
            Match indicators for Transaction 2 screens.

    RETURNS:
        fig (plotly.graph_objs._figure.Figure) :
            A Plotly figure object that renders a dual-column screen-flow
            comparison plot, ready to display in Streamlit or any frontend.

    RAISES:
        ValueError :
            If screen lists and match lists do not have equal lengths.
        TypeError  :
            If any parameter is passed with an incorrect data type.
    """
    import plotly.graph_objects as go
    from plotly.subplots import make_subplots
    
    # Create subplots: 1 row, 2 columns
    fig = make_subplots(
        rows=1, cols=2,
        subplot_titles=(f"Transaction 1: {txn1_id} ({txn1_state})", 
                       f"Transaction 2: {txn2_id} ({txn2_state})"),
        horizontal_spacing=0.1,
        specs=[[{"type": "xy"}, {"type": "xy"}]]
    )
    
    # Colors
    match_color = '#4A90E2'      # Blue for matches
    no_match_color = '#F5A623'   # Orange for non-matches
    
    max_screens = max(len(txn1_flow_screens), len(txn2_flow_screens))
    
    # Transaction 1 (left side)
    for i, (screen, is_match) in enumerate(zip(txn1_flow_screens, txn1_matches)):
        color = match_color if is_match else no_match_color
        y_pos = max_screens - 1 - i
        
        # Add box
        fig.add_shape(
            type="rect",
            x0=0.1, x1=0.9, y0=y_pos, y1=y_pos + 0.7,
            fillcolor=color,
            line=dict(color=color, width=2),
            row=1, col=1
        )
        
        # Add text
        fig.add_annotation(
            x=0.5, y=y_pos + 0.35,
            text=f"{i+1}. {screen}",
            showarrow=False,
            font=dict(color="white", size=10),
            xanchor="center", yanchor="middle",
            row=1, col=1
        )
    
    # Transaction 2 (right side)
    for i, (screen, is_match) in enumerate(zip(txn2_flow_screens, txn2_matches)):
        color = match_color if is_match else no_match_color
        y_pos = max_screens - 1 - i
        
        # Add box
        fig.add_shape(
            type="rect",
            x0=0.1, x1=0.9, y0=y_pos, y1=y_pos + 0.7,
            fillcolor=color,
            line=dict(color=color, width=2),
            row=1, col=2
        )
        
        # Add text
        fig.add_annotation(
            x=0.5, y=y_pos + 0.35,
            text=f"{i+1}. {screen}",
            showarrow=False,
            font=dict(color="white", size=10),
            xanchor="center", yanchor="middle",
            row=1, col=2
        )
    
    # Update layout
    height = max(600, max_screens * 100)
    
    fig.update_layout(
        height=height,
        showlegend=False,
        plot_bgcolor='#0E1117',
        paper_bgcolor='#0E1117',
        font=dict(color='white')
    )
    
    # Update axes
    for i in [1, 2]:
        fig.update_xaxes(
            showgrid=False, showticklabels=False, zeroline=False,
            range=[0, 1], row=1, col=i
        )
        fig.update_yaxes(
            showgrid=False, showticklabels=False, zeroline=False,
            range=[-0.5, max_screens], autorange=True, row=1, col=i
        )
    
    return fig

# ============================================
# CACHE HELPER FUNCTIONS
# ============================================

def init_cache():
    """Initialize cache in session state"""
    if 'api_cache' not in st.session_state:
        st.session_state.api_cache = {}
    if 'cache_hits' not in st.session_state:
        st.session_state.cache_hits = 0
    if 'cache_misses' not in st.session_state:
        st.session_state.cache_misses = 0

def get_cache_key(endpoint: str, **params) -> str:
    """Generate a unique cache key from endpoint and parameters"""
    import hashlib
    import json
    
    # Sort parameters for consistent keys
    sorted_params = json.dumps(params, sort_keys=True)
    cache_string = f"{endpoint}:{sorted_params}"
    
    # Create hash for the key
    return hashlib.md5(cache_string.encode()).hexdigest()

def get_from_cache(cache_key: str):
    """Get data from cache if it exists"""
    init_cache()
    
    if cache_key in st.session_state.api_cache:
        st.session_state.cache_hits += 1
        return st.session_state.api_cache[cache_key]
    
    st.session_state.cache_misses += 1
    return None

def save_to_cache(cache_key: str, data: dict):
    """Save data to cache"""
    init_cache()
    st.session_state.api_cache[cache_key] = data

def clear_cache():
    """Clear all cached data"""
    if 'api_cache' in st.session_state:
        st.session_state.api_cache = {}
        st.session_state.cache_hits = 0
        st.session_state.cache_misses = 0

def cached_request(method: str, url: str, cache_enabled: bool = True, **kwargs):
    """
    FUNCTION:
        init_session_state

    DESCRIPTION:
        Initializes required Streamlit session state variables used across
        the application. Ensures that essential session keys exist before
        any processing or UI interactions take place.

    USAGE:
        init_session_state()

    PARAMETERS:
        None

    RETURNS:
        None :
            This function modifies Streamlit's session_state directly and does
            not return any value.

    RAISES:
        None :
            No exceptions are raised explicitly. Streamlit handles any internal
            session state issues.
    """
    # Generate cache key
    cache_params = {
        'url': url,
        'json': kwargs.get('json', {}),
        'params': kwargs.get('params', {})
    }
    cache_key = get_cache_key(method, **cache_params)
    
    # Check cache first (if enabled)
    if cache_enabled:
        cached_data = get_from_cache(cache_key)
        if cached_data is not None:
            # Return a mock response object with cached data
            class CachedResponse:
                def __init__(self, data):
                    self.status_code = data['status_code']
                    self.headers = data.get('headers', {})
                    self._json = data['json']
                
                def json(self):
                    return self._json
            
            return CachedResponse(cached_data)
    
    # Make fresh request
    request_func = getattr(requests, method.lower())
    response = request_func(url, **kwargs)
    
    # Cache the response (if enabled and successful)
    if cache_enabled and response.status_code == 200:
        cache_data = {
            'status_code': response.status_code,
            'headers': dict(response.headers),
            'json': response.json()
        }
        save_to_cache(cache_key, cache_data)
    
    return response

# Initialize session state
def init_session_state():
    if 'zip_processed' not in st.session_state:
        st.session_state.zip_processed = False
    if 'processing_result' not in st.session_state:
        st.session_state.processing_result = None
    if 'selected_function' not in st.session_state:
        st.session_state.selected_function = None

init_session_state()

# ============================================
# UTILITY FUNCTIONS
# ============================================

def safe_decode(blob: bytes) -> str:
    """
    FUNCTION:
        safe_decode

    DESCRIPTION:
        Safely decodes a byte string into a readable text string by trying a
        sequence of common encodings. Prevents decoding failures by falling back
        to a safe replacement-based UTF-8 decode when all attempts fail.

    USAGE:
        text = safe_decode(byte_data)

    PARAMETERS:
        blob (bytes) :
            Raw byte content that needs to be converted into a string.

    RETURNS:
        str :
            A decoded text string. If no encoding works, characters that cannot
            be decoded are replaced with safe placeholder symbols.

    RAISES:
        None :
            All decoding errors are internally handled; no exceptions are raised.
    """
    encs = ["utf-8-sig", "utf-16", "utf-16-le", "utf-16-be", "cp1252", "latin-1", "utf-8"]
    for e in encs:
        try:
            return blob.decode(e)
        except Exception:
            continue
    return blob.decode("utf-8", errors="replace")

def parse_registry_file(content: bytes) -> pd.DataFrame:
    """
    FUNCTION:
        parse_registry_file

    DESCRIPTION:
        Parses the raw byte content of a Windows Registry (.reg) file and converts
        it into a structured pandas DataFrame. The parser extracts sections (paths),
        keys, and corresponding values using pattern matching for registry syntax.

    USAGE:
        df = parse_registry_file(reg_file_bytes)

    PARAMETERS:
        content (bytes) :
            Raw byte content of the registry file to be parsed.

    RETURNS:
        pd.DataFrame :
            A DataFrame containing parsed registry entries in the format:
                - Path  : Registry section header (e.g., HKEY_LOCAL_MACHINE\...\Run)
                - Key   : Registry key name (or '@' for default value)
                - Value : Raw string value assigned to the key

    RAISES:
        None :
            All decoding errors are handled by safe_decode; invalid lines are skipped.
    """
    lines = safe_decode(content).splitlines()
    rows = []
    current_section = None
    section_re = re.compile(r"^\s*\[(.+?)\]\s*$")
    kv_re = re.compile(r'^\s*(@|".+?"|[^=]+?)\s*=\s*(.+?)\s*$')
    
    for line in lines:
        line = line.strip()
        if not line:
            continue
            
        m = section_re.match(line)
        if m:
            current_section = m.group(1).strip()
            continue
        
        if current_section:
            mv = kv_re.match(line)
            if mv:
                key_raw, value_raw = mv.groups()
                key = key_raw.strip('"') if key_raw != "@" else "@"
                
                rows.append({
                    "Path": current_section,
                    "Key": key,
                    "Value": value_raw.strip()
                })
    
    return pd.DataFrame(rows)

def detect_line_difference(line1: str, line2: str) -> str:
    """
    FUNCTION:
        detect_line_difference

    DESCRIPTION:
        Compares two text lines and determines the type of difference between them.
        It classifies the difference as:
            - "identical"  : Exact text match
            - "whitespace" : Text matches after removing spaces and tabs
            - "content"    : Actual text/content differs

    USAGE:
        diff_type = detect_line_difference("abc", "a b c")

    PARAMETERS:
        line1 (str) :
            First line of text to compare.
        line2 (str) :
            Second line of text to compare.

    RETURNS:
        str :
            A string describing the comparison result:
                - "identical"
                - "whitespace"
                - "content"

    RAISES:
        None :
            This function does not raise exceptions. All comparisons are safe.
    """
    if line1 == line2:
        return "identical"
    if line1.replace(' ', '').replace('\t', '') == line2.replace(' ', '').replace('\t', ''):
        return "whitespace"
    return "content"

def render_side_by_side_diff(content1: str, content2: str, filename1: str, filename2: str):
    """    
    FUNCTION:
        render_side_by_side_diff

    DESCRIPTION:
        Renders a side-by-side visual comparison of two text files using Streamlit.
        Each line is classified as:
            - Content change
            - Whitespace-only change
            - Identical
        The function color-codes differences and displays line numbers for easy
        comparison between the two files.

    USAGE:
        render_side_by_side_diff(file1_text, file2_text, "old.txt", "new.txt")

    PARAMETERS:
        content1 (str) :
            Text content of the first (original) file.

        content2 (str) :
            Text content of the second (modified) file.

        filename1 (str) :
            Display name for the first file.

        filename2 (str) :
            Display name for the second file.

    RETURNS:
        None :
            This function renders UI components directly in Streamlit and
            does not return any value.

    RAISES:
        None :
            Any internal comparison is safely handled. No exceptions are raised
            during diff rendering.
    """
    lines1 = content1.splitlines()
    lines2 = content2.splitlines()
    
    max_lines = max(len(lines1), len(lines2))
    
    st.markdown("### File Comparison")
    st.caption(f"Comparing: {filename1} vs {filename2}")
    
    # Legend
    col1, col2, col3 = st.columns(3)
    with col1:
        st.markdown('<div class="legend-item"><div class="legend-color" style="background-color: rgba(239, 68, 68, 0.15);"></div>Content Changes</div>', unsafe_allow_html=True)
    with col2:
        st.markdown('<div class="legend-item"><div class="legend-color" style="background-color: rgba(168, 85, 247, 0.12);"></div>Whitespace Only</div>', unsafe_allow_html=True)
    with col3:
        st.markdown('<div class="legend-item"><div class="legend-color" style="background-color: transparent; border: 1px solid #404040;"></div>Identical Lines</div>', unsafe_allow_html=True)
    
    st.markdown("---")
    
    col_left, col_right = st.columns(2)
    
    with col_left:
        st.markdown(f"#### {filename1}")
        html_left = '<div class="diff-pane"><div class="diff-pane-header">Original File</div>'
        
        for i in range(max_lines):
            line1 = lines1[i] if i < len(lines1) else ""
            line2 = lines2[i] if i < len(lines2) else ""
            
            diff_type = detect_line_difference(line1, line2)
            
            if diff_type == "content":
                css_class = "diff-content-change"
            elif diff_type == "whitespace":
                css_class = "diff-whitespace-change"
            else:
                css_class = "diff-identical"
            
            line1_escaped = line1.replace('&', '&amp;').replace('<', '&lt;').replace('>', '&gt;')
            
            html_left += f'<div class="diff-line {css_class}"><span class="diff-line-number">{i+1}</span>{line1_escaped}</div>'
        
        html_left += '</div>'
        st.markdown(html_left, unsafe_allow_html=True)
    
    with col_right:
        st.markdown(f"#### {filename2}")
        html_right = '<div class="diff-pane"><div class="diff-pane-header">Modified File</div>'
        
        for i in range(max_lines):
            line1 = lines1[i] if i < len(lines1) else ""
            line2 = lines2[i] if i < len(lines2) else ""
            
            diff_type = detect_line_difference(line1, line2)
            
            if diff_type == "content":
                css_class = "diff-content-change"
            elif diff_type == "whitespace":
                css_class = "diff-whitespace-change"
            else:
                css_class = "diff-identical"
            
            line2_escaped = line2.replace('&', '&amp;').replace('<', '&lt;').replace('>', '&gt;')
            
            html_right += f'<div class="diff-line {css_class}"><span class="diff-line-number">{i+1}</span>{line2_escaped}</div>'
        
        html_right += '</div>'
        st.markdown(html_right, unsafe_allow_html=True)

# ============================================
# ANALYSIS FUNCTIONS
# ============================================

def render_transaction_stats():
    """
    Render transaction statistics with source file filter
    """
    st.markdown("###   Transaction Type Statistics")
    
    # Initialize a flag to track if we need to analyze
    need_analysis = False
    
    try:
        # STEP 1: Check if transaction data exists
        check_response = requests.get(
            f"{API_BASE_URL}/transaction-statistics",
            timeout=30
        )
        
        # If we get 400, it means data hasn't been analyzed yet
        if check_response.status_code == 400:
            need_analysis = True
            
            with st.spinner("Analyzing customer journals... This may take a moment."):
                try:
                    # Automatically analyze the customer journals
                    analyze_response = requests.post(
                        f"{API_BASE_URL}/analyze-customer-journals",
                        timeout=120
                    )
                    
                    if analyze_response.status_code == 200:
                        analyze_data = analyze_response.json()
                        # Give a moment for the session to update
                        import time
                        time.sleep(0.5)
                    else:
                        error_detail = analyze_response.json().get('detail', 'Analysis failed')
                        st.error(f"  Failed to analyze customer journals: {error_detail}")
                        return
                        
                except requests.exceptions.Timeout:
                    st.error("⏱  Analysis timeout. The file may be too large.")
                    return
                except requests.exceptions.ConnectionError:
                    st.error("  Connection error. Ensure the API server is running on localhost:8000.")
                    return
                except Exception as e:
                    st.error(f"  Error during analysis: {str(e)}")
                    import traceback
                    st.code(traceback.format_exc())
                    return
        
        # STEP 2: Now get the statistics (either they existed or we just created them)
        response = requests.get(
            f"{API_BASE_URL}/transaction-statistics",
            timeout=30
        )
        
        if response.status_code == 200:
            data = response.json()
            
            # ========================================
            # SECTION 1: Overall Statistics
            # ========================================
            st.markdown("#### Overall Transaction Statistics")
            
            if 'statistics' in data:
                stats_df = pd.DataFrame(data['statistics'])

                st.dataframe(
                    stats_df,
                    use_container_width=True,
                    hide_index=True
                )
            # ========================================
            # SECTION 2: Source File Filter
            # ========================================
            st.markdown("---")
            
            try:
                # Get source files information
                sources_response = requests.get(
                    f"{API_BASE_URL}/get-transactions-with-sources",
                    timeout=30
                )
                
                if sources_response.status_code == 200:
                    sources_data = sources_response.json()
                    available_sources = sources_data.get('source_files', [])
                    
                    if available_sources:
                        
                        # Multi-select dropdown for source files
                        selected_sources = st.multiselect(
                            "Select source files to view their transactions",
                            options=available_sources,
                            default=None,
                            key="transaction_stats_sources",
                            help="Select one or more source files to filter transactions"
                        )
                        
                        if selected_sources:
                            
                            # Get filtered transactions
                            filter_response = requests.post(
                                f"{API_BASE_URL}/filter-transactions-by-sources",
                                json={"source_files": selected_sources},
                                timeout=30
                            )
                            
                            if filter_response.status_code == 200:
                                filtered_data = filter_response.json()
                                transactions = filtered_data.get('transactions', [])
                                
                                if transactions:
                                    
                                    # Create display DataFrame
                                    txn_display_data = []
                                    for txn in transactions:
                                        txn_display_data.append({
                                            'Transaction ID': txn.get('Transaction ID', 'N/A'),
                                            'Type': txn.get('Transaction Type', 'N/A'),
                                            'State': txn.get('End State', 'N/A'),
                                            'Duration (s)': txn.get('Duration (seconds)', 0),
                                            'Source File': txn.get('Source File', 'N/A'),
                                            'Start Time': txn.get('Start Time', 'N/A'),
                                            'End Time': txn.get('End Time', 'N/A')
                                        })
                                    
                                    txn_df = pd.DataFrame(txn_display_data)
                                    
                                    # Add additional filters
                                    col1, col2, col3 = st.columns(3)
                                    
                                    with col1:
                                        # Get unique transaction types
                                        unique_types = sorted(txn_df['Type'].unique().tolist())
                                        filter_type = st.selectbox(
                                            "Transaction Type",
                                            options=['All'] + unique_types,
                                            key="stats_type_filter"
                                        )
                                    
                                    with col2:
                                        # Get unique states
                                        unique_states = sorted(txn_df['State'].unique().tolist())
                                        filter_state = st.selectbox(
                                            "End State",
                                            options=['All'] + unique_states,
                                            key="stats_state_filter"
                                        )
                                    
                                    with col3:
                                        # Transaction ID search
                                        search_txn_id = st.text_input(
                                            "Transaction ID",
                                            placeholder="Search ID...",
                                            key="stats_txn_id_search"
                                        )
                                    
                                    # Apply filters
                                    display_df = txn_df.copy()
                                    
                                    if filter_type != 'All':
                                        display_df = display_df[display_df['Type'] == filter_type]
                                    
                                    if filter_state != 'All':
                                        display_df = display_df[display_df['State'] == filter_state]

                                    if search_txn_id:
                                        display_df = display_df[display_df['Transaction ID'].str.contains(search_txn_id, case=False, na=False)]
                                    
                                    # Display filtered count
                                    if len(display_df) != len(txn_df):
                                        st.info(f"Filtered to {len(display_df)} transaction(s)")
                                    
                            # Transaction ID search
                                    st.markdown("---")
                                    search_txn_id = st.text_input(
                                        "  Search Transaction ID",
                                        placeholder="Enter Transaction ID to search...",
                                        key="ui_flow_txn_search"
                                    )
                                    
                                    if search_txn_id:
                                        display_df = display_df[display_df['Transaction ID'].str.contains(search_txn_id, case=False, na=False)]
                                        if len(display_df) == 0:
                                            st.warning("  No transactions match the search term")
                                            return
                                        st.info(f"Search filtered to {len(display_df)} transaction(s)")
                                    
                                    
                                    # Display the transactions table
                                    st.dataframe(
                                        display_df,
                                        use_container_width=True,
                                        hide_index=True
                                    )
                                    
                                    # Statistics for filtered data
                                    st.markdown("#####   Statistics for Filtered Transactions")
                                    
                                    stat_col1, stat_col2, stat_col3, stat_col4 = st.columns(4)
                                    
                                    with stat_col1:
                                        st.metric("Total", len(display_df))
                                    
                                    with stat_col2:
                                        successful = len(display_df[display_df['State'] == 'Successful'])
                                        st.metric("Successful", successful)
                                    
                                    with stat_col3:
                                        unsuccessful = len(display_df[display_df['State'] == 'Unsuccessful'])
                                        st.metric("Unsuccessful", unsuccessful)
                                    
                                    with stat_col4:
                                        if len(display_df) > 0:
                                            success_rate = (successful / len(display_df)) * 100
                                            st.metric("Success Rate", f"{success_rate:.1f}%")
                                        else:
                                            st.metric("Success Rate", "0%")
                                    
                                    # Download button
                                    st.markdown("---")
                                    csv = display_df.to_csv(index=False)
                                    st.download_button(
                                        label="📥 Download Filtered Transactions as CSV",
                                        data=csv,
                                        file_name=f"transactions_filtered_{len(selected_sources)}_sources.csv",
                                        mime="text/csv",
                                        key="download_filtered_txns"
                                    )
                                    
                                else:
                                    st.warning("  No transactions found for the selected source files.")
                            
                            else:
                                st.error(f"Failed to filter transactions. Status: {filter_response.status_code}")
                    
                    else:
                        st.warning("  No source files available. Please ensure customer journals were analyzed.")
                
                else:
                    st.error(f"Failed to retrieve source file information. Status: {sources_response.status_code}")
            
            except requests.exceptions.Timeout:
                st.error("⏱  Request timeout while fetching source files. Please try again.")
            except requests.exceptions.ConnectionError:
                st.error("🔌 Connection error. Ensure the API server is running.")
            except Exception as e:
                st.error(f"  Error loading source file filter: {str(e)}")
        
        elif response.status_code == 400:
            # This shouldn't happen after our analysis, but just in case
            st.error("  Transaction data still not available after analysis. Please check the API logs.")
            error_detail = response.json().get('detail', 'Unknown error')
            st.info(f"Details: {error_detail}")
        
        else:
            st.error(f"Failed to load transaction statistics. Status code: {response.status_code}")
            try:
                error_detail = response.json().get('detail', 'Unknown error')
                st.info(f"Details: {error_detail}")
            except:
                pass
            
    except requests.exceptions.Timeout:
        st.error("⏱  Request timeout. Please try again.")
    except requests.exceptions.ConnectionError:
        st.error("  Connection error. Ensure the API server is running on localhost:8000.")
    except Exception as e:
        st.error(f"  Error loading transaction statistics: {str(e)}")
        import traceback
        with st.expander(" Debug Information"):
            st.code(traceback.format_exc())


def render_registry_single():
    """
FUNCTION: render_registry_single

DESCRIPTION:
    Renders a Streamlit interface to view a single registry file from an 
    in-memory session. Allows selecting a registry file, searching entries, 
    displaying key metrics, and downloading filtered results as CSV.

USAGE:
    render_registry_single()

PARAMETERS:
    This function does not take any parameters. It relies on:
        - API_BASE_URL : Base URL for backend API calls
        - Streamlit session_state for storing selected file and search input

RETURNS:
    None : The function directly renders Streamlit UI components 
           (dropdowns, dataframes, search input, metrics, download button).

RAISES:
    requests.exceptions.Timeout         : If an API request times out
    requests.exceptions.ConnectionError : If API server is not reachable
    Exception                           : For any unexpected errors during execution
"""

    st.markdown("###  Registry File Viewer")

    # Get registry contents from session via API
    try:
        response = requests.get(
            f"{API_BASE_URL}/get-registry-contents",
            params={"session_id": "current_session"},
            timeout=30
        )
        
        if response.status_code != 200:
            st.error("  Failed to load registry files from session")
            logger.error(f"API call failed with status: {response.status_code}")
            return
            
        registry_data = response.json()
        registry_contents = registry_data.get('registry_contents', {})
        
        if not registry_contents:
            st.warning("  No registry files found in the uploaded package.")
            return

        # Create file selection dropdown
        selected_file_name = st.selectbox(
            "Select Registry File",
            options=["Select a file"] + list(registry_contents.keys()),
            key="reg_single_select"
        )

        if selected_file_name != "Select a file":
            with st.spinner("Loading registry file..."):
                try:
                    # Get content from in-memory cache
                    content_b64 = registry_contents[selected_file_name]
                    
                    # Decode base64 to bytes
                    import base64
                    content = base64.b64decode(content_b64)
                    
                    # Parse registry file
                    df = parse_registry_file(content)
                    
                    if not df.empty:
                        # Display metrics
                        col1, col2, col3 = st.columns(3)
                        with col1:
                            st.metric("Total Entries", len(df))
                        with col2:
                            st.metric("Unique Paths", df['Path'].nunique())
                        with col3:
                            st.metric("Unique Keys", df['Key'].nunique())
                        
                        st.markdown("---")
                        
                        # Search functionality
                        search_term = st.text_input(
                            "Search Registry", 
                            placeholder="Search in path, key, or value", 
                            key="reg_search"
                        )
                        
                        display_df = df
                        if search_term:
                            mask = (
                                df['Path'].str.contains(search_term, case=False, na=False) |
                                df['Key'].str.contains(search_term, case=False, na=False) |
                                df['Value'].str.contains(search_term, case=False, na=False)
                            )
                            display_df = df[mask]
                            st.info(f"Found {len(display_df)} matching entries.")
                        
                        # Display table
                        st.dataframe(display_df, use_container_width=True, height=400)
                        
                        # Download button
                        csv = display_df.to_csv(index=False)
                        st.download_button(
                            label="Download as CSV",
                            data=csv,
                            file_name=f"{Path(selected_file_name).stem}.csv",
                            mime="text/csv",
                            use_container_width=True
                        )
                    else:
                        st.warning("No entries found in the registry file.")
                        
                except Exception as e:
                    st.error(f"Error loading file: {str(e)}")
                    logger.exception(f"Error parsing registry file {selected_file_name}")
                    import traceback
                    with st.expander("  Debug Information"):
                        st.code(traceback.format_exc())
                    
    except requests.exceptions.Timeout:
        st.error("⏱  Request timeout. Please try again.")
    except requests.exceptions.ConnectionError:
        st.error("  Connection error. Ensure the API server is running on localhost:8000.")
    except Exception as e:
        st.error(f"  Error: {str(e)}")
        logger.exception("Error in render_registry_single")
        import traceback
        with st.expander("  Debug Information"):
            st.code(traceback.format_exc())

def render_registry_compare():
    """
    Render registry file comparison interface with in-memory content loading
    """
    st.markdown("### Registry File Comparison")
    
    # Get registry contents from Package A (main session)
    try:
        response = requests.get(
            f"{API_BASE_URL}/get-registry-contents",
            params={"session_id": "current_session"},
            timeout=30
        )
        
        if response.status_code != 200:
            st.error("  Failed to load registry files from first package")
            return
            
        registry_data = response.json()
        registry_contents_a = registry_data.get('registry_contents', {})
        
        if not registry_contents_a:
            st.warning("  No registry files found in the first uploaded package.")
            return
        
        st.markdown("#### Step 1: First Package (Already Loaded)")
        st.success(f"  Loaded {len(registry_contents_a)} registry file(s) from main package")
        
        # Show available files from first package
        with st.expander("View files in first package"):
            for filename in registry_contents_a.keys():
                st.caption(f"• {filename}")
        
        st.markdown("---")
        st.markdown("#### Step 2: Upload Second Package for Comparison")
        
        # File uploader for second ZIP
        uploaded_file_b = st.file_uploader(
            "Select second ZIP archive",
            type=['zip'],
            help="Upload another ZIP file to compare registry files",
            key="compare_zip_upload"
        )
        
        if uploaded_file_b is not None:
            file_size_mb = len(uploaded_file_b.getvalue()) / (1024 * 1024)
            st.info(f"  File: {uploaded_file_b.name} ({file_size_mb:.2f} MB)")
            
            # Process second ZIP button
            if st.button("Process Second Package", use_container_width=True, key="process_second_zip"):
                with st.spinner("Processing second package..."):
                    try:
                        files = {"file": (uploaded_file_b.name, uploaded_file_b.getvalue(), "application/zip")}
                        response = requests.post(
                            f"{API_BASE_URL}/process-zip", 
                            files=files, 
                            timeout=300   # change time (increased time)
                        )
                        
                        if response.status_code == 200:
                            result_b = response.json()
                            
                            # Get registry contents from second package
                            # Note: The second package is now in the main session (it replaces Package A)
                            # So we need to fetch it immediately
                            reg_response = requests.get(
                                f"{API_BASE_URL}/get-registry-contents",
                                timeout=30
                            )
                            
                            if reg_response.status_code == 200:
                                reg_data = reg_response.json()
                                registry_contents_b = reg_data.get('registry_contents', {})
                                
                                if not registry_contents_b:
                                    st.error("  No registry files found in second package.")
                                    return
                                
                                # Store second package contents in session state
                                st.session_state['compare_package_b'] = {
                                    'zip_name': uploaded_file_b.name,
                                    'registry_contents': registry_contents_b
                                }
                                
                                st.success(f"  Second package processed: {len(registry_contents_b)} registry file(s) found")
                                st.rerun()
                            else:
                                st.error("Failed to load registry files from second package")
                        else:
                            st.error(f"Error processing second package: {response.json().get('detail')}")
                    
                    except requests.exceptions.Timeout:
                        st.error("⏱  Request timeout. Please try again.")
                    except requests.exceptions.ConnectionError:
                        st.error("  Connection error. Ensure the API server is running.")
                    except Exception as e:
                        st.error(f"Error: {str(e)}")
                        import traceback
                        with st.expander("  Debug Information"):
                            st.code(traceback.format_exc())
        
        # If second package is loaded, show comparison UI
        if 'compare_package_b' in st.session_state:
            package_b = st.session_state['compare_package_b']
            registry_contents_b = package_b['registry_contents']
            
            st.markdown("---")
            st.markdown("#### Step 3: Select Files to Compare")
            
            # Get file names from both packages
            files_a = set(registry_contents_a.keys())
            files_b = set(registry_contents_b.keys())
            
            # Find common file names
            common_names = files_a & files_b
            
            if not common_names:
                st.warning("  No files with matching names found in both packages.")
                
                col1, col2 = st.columns(2)
                with col1:
                    st.markdown("**Files in Package 1:**")
                    for name in sorted(files_a):
                        st.caption(f"• {name}")
                with col2:
                    st.markdown("**Files in Package 2:**")
                    for name in sorted(files_b):
                        st.caption(f"• {name}")
                
                return
            
            st.success(f"  Found {len(common_names)} file(s) with matching names")
            
            # Select which file to compare
            selected_filename = st.selectbox(
                "Select file to compare",
                options=sorted(list(common_names)),
                key="compare_file_select"
            )
            
            if selected_filename:
                if st.button("Compare Selected Files", use_container_width=True, key="do_compare"):
                    with st.spinner("Comparing files..."):
                        try:
                            # Get contents from both packages
                            import base64
                            
                            content_a_b64 = registry_contents_a[selected_filename]
                            content_b_b64 = registry_contents_b[selected_filename]
                            
                            # Decode base64 to bytes
                            content_a = base64.b64decode(content_a_b64)
                            content_b = base64.b64decode(content_b_b64)
                            
                            # Decode to text
                            text_a = safe_decode(content_a)
                            text_b = safe_decode(content_b)
                            
                            # Render side-by-side comparison
                            fname_a = f"Package 1: {selected_filename}"
                            fname_b = f"Package 2: {selected_filename}"
                            render_side_by_side_diff(text_a, text_b, fname_a, fname_b)

                        except Exception as e:
                            st.error(f"Error comparing files: {str(e)}")
                            import traceback
                            with st.expander("  Debug Information"):
                                st.code(traceback.format_exc())
    
    except requests.exceptions.Timeout:
        st.error("⏱  Request timeout. Please try again.")
    except requests.exceptions.ConnectionError:
        st.error("  Connection error. Ensure the API server is running on localhost:8000.")
    except Exception as e:
        st.error(f"  Error in comparison setup: {str(e)}")
        logger.exception("Error in render_registry_compare")
        import traceback
        with st.expander("  Debug Information"):
            st.code(traceback.format_exc())
    

def render_transaction_comparison():
    """
    FUNCTION:
        render_transaction_comparison

    DESCRIPTION:
        Renders a comprehensive transaction comparison interface in Streamlit.
        The function allows users to:
            - Analyze customer journals if analysis is not yet performed
            - Filter transactions based on source files, transaction type, and state
            - Select two transactions for comparison
            - Display side-by-side UI flow of both transactions with color-coded matches
            - Show transaction logs for each selected transaction
            - Provide detailed metrics, including duration differences and source file info
            - Highlight unique and common screens between transactions
        Supports visual exploration and side-by-side comparisons for better insight
        into transaction flows and differences.

    USAGE:
        render_transaction_comparison()

    PARAMETERS:
        None

    RETURNS:
        None :
            This function renders UI components directly in Streamlit and does
            not return any value.

    RAISES:
        Exception :
            Any unexpected error during API calls, analysis, or comparison is
            caught and displayed via Streamlit.
    """
    st.markdown("###   Transaction Comparison Analysis")
    
    need_analysis = False
    
    try:
        # ========================================
        # STEP 1: Check if analysis is needed
        # ========================================
        try:
            sources_response = requests.get(
                f"{API_BASE_URL}/get-transactions-with-sources",
                timeout=30
            )
            
            if sources_response.status_code == 200:
                sources_data = sources_response.json()
                available_sources = sources_data.get('source_files', [])
                
                if not available_sources:
                    need_analysis = True
            else:
                need_analysis = True
                
        except Exception as e:
            need_analysis = True
        
        # STEP 2: Perform analysis if needed
        if need_analysis:
            st.info("  Customer journals need to be analyzed first...")
            
            with st.spinner("Analyzing customer journals... This may take a moment."):
                try:
                    analyze_response = requests.post(
                        f"{API_BASE_URL}/analyze-customer-journals",
                        timeout=120
                    )
                    
                    if analyze_response.status_code == 200:
                        analyze_data = analyze_response.json()
                        st.success(f"  Analysis complete! Found {analyze_data.get('total_transactions', 0)} transactions")
                        import time
                        time.sleep(0.5)
                        st.rerun()
                    else:
                        error_detail = analyze_response.json().get('detail', 'Analysis failed')
                        st.error(f"  Failed to analyze customer journals: {error_detail}")
                        return
                        
                except requests.exceptions.Timeout:
                    st.error("⏱  Analysis timeout. The file may be too large.")
                    return
                except requests.exceptions.ConnectionError:
                    st.error("  Connection error. Ensure the API server is running on localhost:8000.")
                    return
                except Exception as e:
                    st.error(f"  Error during analysis: {str(e)}")
                    return
        
        # ========================================
        # SECTION 1: Source File Filter
        # ========================================
        # Get source files
        sources_response = requests.get(
            f"{API_BASE_URL}/get-transactions-with-sources",
            timeout=30
        )
        
        if sources_response.status_code != 200:
            st.error("Failed to retrieve source file information.")
            return
        
        sources_data = sources_response.json()
        available_sources = sources_data.get('source_files', [])
        all_txns_df = pd.DataFrame(sources_data.get('all_transactions', []))
        
        if not available_sources:
            st.warning("  No source files available. Please ensure customer journals were analyzed.")
            return
        
        if not all_txns_df.empty:
            sources_with_txns = all_txns_df['Source File'].unique().tolist()
            available_sources = [src for src in available_sources if src in sources_with_txns]
                        
        # Multi-select dropdown for source files
        selected_sources = st.multiselect(
            "Choose source files containing transactions to compare",
            options=available_sources,
            default=available_sources,  # Select all by default
            key="comparison_sources",
            help="Select source files to filter available transactions"
        )
        
        if not selected_sources:
            st.info("  Please select at least one source file to continue")
            return
        
        # Get filtered transactions
        filter_response = requests.post(
            f"{API_BASE_URL}/filter-transactions-by-sources",
            json={"source_files": selected_sources},
            timeout=30
        )
        
        if filter_response.status_code != 200:
            st.error("Failed to get filtered transactions.")
            return
        
        filtered_data = filter_response.json()
        filtered_transactions = filtered_data.get('transactions', [])
        
        if len(filtered_transactions) < 2:
            st.warning(
                f"  Need at least 2 transactions for comparison. "
                f"Found only {len(filtered_transactions)} transaction(s) in the selected source files."
            )
            return
        
        # ========================================
        # SECTION 2: Optional Filters
        # ========================================
        st.markdown("---")
        
        # Create DataFrame for easier filtering
        txn_df = pd.DataFrame(filtered_transactions)
        
        col1, col2 = st.columns(2)
        
        with col1:
            # Filter by transaction type
            unique_types = sorted(txn_df['Transaction Type'].unique().tolist())
            if len(unique_types) > 1:
                filter_type = st.selectbox(
                    "Filter by Transaction Type",
                    options=['All Types'] + unique_types,
                    key="comparison_type_filter",
                    help="Optionally filter to compare transactions of the same type"
                )
                
                if filter_type != 'All Types':
                    txn_df = txn_df[txn_df['Transaction Type'] == filter_type]
                    filtered_transactions = txn_df.to_dict('records')
                    
                    if len(filtered_transactions) < 2:
                        st.warning(f"Only {len(filtered_transactions)} transaction(s) of type '{filter_type}'")
                        return
        
        with col2:
            # Filter by state
            unique_states = sorted(txn_df['End State'].unique().tolist())
            if len(unique_states) > 1:
                filter_state = st.selectbox(
                    "Filter by State",
                    options=['All States'] + unique_states,
                    key="comparison_state_filter",
                    help="Optionally filter by transaction state"
                )
                
                if filter_state != 'All States':
                    txn_df = txn_df[txn_df['End State'] == filter_state]
                    filtered_transactions = txn_df.to_dict('records')
                    
                    if len(filtered_transactions) < 2:
                        st.warning(f"Only {len(filtered_transactions)} transaction(s) with state '{filter_state}'")
                        return
        
        # ========================================
        # SECTION 3: Transaction ID Search
        # ========================================
        st.markdown("---")
        st.markdown("####   Search Transactions by ID")
        
        search_col1, search_col2 = st.columns(2)
        
        with search_col1:
            search_txn1_id = st.text_input(
                "Search Transaction 1 ID",
                placeholder="Enter Transaction ID to search...",
                key="compare_search_txn1",
                help="Filter first transaction dropdown by ID"
            )
        
        with search_col2:
            search_txn2_id = st.text_input(
                "Search Transaction 2 ID",
                placeholder="Enter Transaction ID to search...",
                key="compare_search_txn2",
                help="Filter second transaction dropdown by ID"
            )
        
        # Apply search filters
        filtered_txn1_list = filtered_transactions.copy() if isinstance(filtered_transactions, list) else filtered_transactions
        filtered_txn2_list = filtered_transactions.copy() if isinstance(filtered_transactions, list) else filtered_transactions
        
        if search_txn1_id:
            filtered_txn1_list = [
                txn for txn in filtered_txn1_list 
                if search_txn1_id.lower() in str(txn.get('Transaction ID', '')).lower()
            ]
            if len(filtered_txn1_list) == 0:
                st.warning("  No transactions match Transaction 1 search term")
        
        if search_txn2_id:
            filtered_txn2_list = [
                txn for txn in filtered_txn2_list 
                if search_txn2_id.lower() in str(txn.get('Transaction ID', '')).lower()
            ]
            if len(filtered_txn2_list) == 0:
                st.warning("  No transactions match Transaction 2 search term")
        
        # ========================================
        # SECTION 4: Transaction Selection
        # ========================================
        st.markdown("---")
        st.markdown("####   Select Two Transactions to Compare")
        
        col1, col2 = st.columns(2)
        
        # Transaction 1 selector
        with col1:
            st.markdown("##### First Transaction")
            
            # Use filtered list for Transaction 1
            txn1_options = [
                f"{txn['Transaction ID']} - {txn['Transaction Type']} ({txn['End State']})"
                for txn in filtered_txn1_list
            ]
            
            if not txn1_options:
                st.warning("No transactions available after filtering")
                return
            
            txn1_selection = st.selectbox(
                "Transaction 1",
                options=txn1_options,
                key="compare_txn1",
                help="Select the first transaction to compare"
            )
            
            if txn1_selection:
                txn1_id = txn1_selection.split(' - ')[0]
                txn1_data = next(
                    (txn for txn in filtered_txn1_list if txn['Transaction ID'] == txn1_id),
                    None
                )
                
                if txn1_data:
                    st.info(
                        f"**ID:** {txn1_data['Transaction ID']}\n\n"
                        f"**Type:** {txn1_data['Transaction Type']}\n\n"
                        f"**State:** {txn1_data['End State']}\n\n"
                        f"**Duration:** {txn1_data.get('Duration (seconds)', 0)}s\n\n"
                        f"**Source:** {txn1_data.get('Source File', 'Unknown')}"
                    )
        
        # Transaction 2 selector
        with col2:
            st.markdown("##### Second Transaction")
            
            # Use filtered list for Transaction 2 AND exclude selected txn1
            txn2_options = [
                opt for opt in [
                    f"{txn['Transaction ID']} - {txn['Transaction Type']} ({txn['End State']})"
                    for txn in filtered_txn2_list
                ]
                if opt.split(' - ')[0] != (txn1_id if txn1_selection else None)
            ]
            
            if not txn2_options:
                st.warning("No other transactions available for comparison")
                return
            
            txn2_selection = st.selectbox(
                "Transaction 2",
                options=txn2_options,
                key="compare_txn2",
                help="Select the second transaction to compare"
            )
            
            if txn2_selection:
                txn2_id = txn2_selection.split(' - ')[0]
                txn2_data = next(
                    (txn for txn in filtered_txn2_list if txn['Transaction ID'] == txn2_id),
                    None
                )
                
                if txn2_data:
                    st.info(
                        f"**ID:** {txn2_data['Transaction ID']}\n\n"
                        f"**Type:** {txn2_data['Transaction Type']}\n\n"
                        f"**State:** {txn2_data['End State']}\n\n"
                        f"**Duration:** {txn2_data.get('Duration (seconds)', 0)}s\n\n"
                        f"**Source:** {txn2_data.get('Source File', 'Unknown')}"
                    )
        
        # Check if both transactions are selected
        if not (txn1_selection and txn2_selection):
            st.info("  Please select both transactions above to proceed with comparison")
            return
        
        # ========================================
        # SECTION 4: Perform Comparison
        # ========================================
        st.markdown("---")
        st.markdown("####   Comparison Results")
        
        with st.spinner(f"Comparing {txn1_id} and {txn2_id}..."):
            try:
                # Call comparison API with caching
                comparison_response = cached_request(
                    'post',
                    f"{API_BASE_URL}/compare-transactions-flow",
                    cache_enabled=True,
                    json={
                        "txn1_id": txn1_id,
                        "txn2_id": txn2_id
                    },
                    timeout=30
                )
                
                if comparison_response.status_code == 200:
                    comparison_data = comparison_response.json()

                    if hasattr(comparison_response, '_json'):  # This means it came from cache
                        st.caption("  Loaded from cache")
                    
                    # Create tabs for different views
                    tab1, tab2, tab3 = st.tabs([
                        "  Side-by-Side Flow",
                        "  Transaction Logs",
                        "  Detailed Analysis"
                    ])
                    
                    # ========================================
                    # TAB 1: Side-by-Side Flow Comparison
                    # ========================================
                    with tab1:
                        st.markdown("#### Side-by-Side UI Flow Comparison")
                        
                        txn1_flow = comparison_data.get('txn1_flow', [])
                        txn2_flow = comparison_data.get('txn2_flow', [])
                        txn1_matches = comparison_data.get('txn1_matches', [])
                        txn2_matches = comparison_data.get('txn2_matches', [])
                        
                        # Display flows side by side
                        flow_col1, flow_col2 = st.columns(2)
                        
                        with flow_col1:
                            st.markdown(f"##### Transaction 1: {txn1_id}")
                            st.caption(f"State: {comparison_data.get('txn1_state', 'Unknown')}")
                            st.caption(f"{len(txn1_flow)} screen(s)")
                            
                            for i, (screen, is_match) in enumerate(zip(txn1_flow, txn1_matches), 1):
                                # Handle both dict and string formats
                                if isinstance(screen, dict):
                                    screen_name = screen.get('screen', 'Unknown')
                                    duration = screen.get('duration')
                                    if duration is not None:
                                        screen_display = f"{screen_name} ({duration:.1f}s)"
                                    else:
                                        screen_display = screen_name
                                else:
                                    screen_display = str(screen)
                                
                                if is_match:
                                    st.success(f"**{i}.** {screen_display}")
                                else:
                                    st.warning(f"**{i}.** {screen_display}")
                        
                        with flow_col2:
                            st.markdown(f"##### Transaction 2: {txn2_id}")
                            st.caption(f"State: {comparison_data.get('txn2_state', 'Unknown')}")
                            st.caption(f"{len(txn2_flow)} screen(s)")
                            
                            for i, (screen, is_match) in enumerate(zip(txn2_flow, txn2_matches), 1):
                                # Handle both dict and string formats
                                if isinstance(screen, dict):
                                    screen_name = screen.get('screen', 'Unknown')
                                    duration = screen.get('duration')
                                    if duration is not None:
                                        screen_display = f"{screen_name} ({duration:.1f}s)"
                                    else:
                                        screen_display = screen_name
                                else:
                                    screen_display = str(screen)
                                
                                if is_match:
                                    st.success(f"**{i}.** {screen_display}")
                                else:
                                    st.warning(f"**{i}.** {screen_display}")
                        
                        # Legend
                        st.markdown("---")
                        legend_col1, legend_col2 = st.columns(2)
                        with legend_col1:
                            st.success("Screen appears in both transactions")
                        with legend_col2:
                            st.warning("Screen unique to this transaction")
                        
                        # Calculate and display similarity metrics
                        st.markdown("---")
                        st.markdown("#####   Flow Similarity Metrics")
                        
                        # Extract screen names for comparison
                        def get_screen_names(flow):
                            screens = []
                            for item in flow:
                                if isinstance(item, dict):
                                    screens.append(item.get('screen', str(item)))
                                else:
                                    screens.append(str(item))
                            return screens
                        
                        txn1_screens = get_screen_names(txn1_flow)
                        txn2_screens = get_screen_names(txn2_flow)
                        
                        common_screens = len(set(txn1_screens) & set(txn2_screens))
                        total_unique_screens = len(set(txn1_screens) | set(txn2_screens))
                        similarity = (common_screens / total_unique_screens * 100) if total_unique_screens > 0 else 0
                        
                        metric_col1, metric_col2, metric_col3, metric_col4 = st.columns(4)
                        
                        with metric_col1:
                            st.metric("Common Screens", common_screens)
                        with metric_col2:
                            st.metric("Different Screens", total_unique_screens - common_screens)
                        with metric_col3:
                            st.metric("Total Unique Screens", total_unique_screens)
                        with metric_col4:
                            st.metric("Similarity", f"{similarity:.1f}%")
                    
                    # ========================================
                    # TAB 2: Transaction Logs
                    # ========================================
                    with tab2:
                        st.markdown("#### Transaction Logs Comparison")
                        
                        log_col1, log_col2 = st.columns(2)
                        
                        with log_col1:
                            st.markdown(f"##### Transaction 1: {txn1_id}")
                            txn1_log = comparison_data.get('txn1_log', 'No log available')
                            st.code(txn1_log, language="log", line_numbers=True)
                        
                        with log_col2:
                            st.markdown(f"##### Transaction 2: {txn2_id}")
                            txn2_log = comparison_data.get('txn2_log', 'No log available')
                            st.code(txn2_log, language="log", line_numbers=True)
                    
                    # ========================================
                    # TAB 3: Detailed Analysis
                    # ========================================
                    with tab3:
                        st.markdown("#### Detailed Comparison Analysis")
                        
                        # Additional comparison metrics
                        st.markdown("#####   Detailed Metrics")
                        
                        # Duration comparison
                        if txn1_data and txn2_data:
                            duration_col1, duration_col2, duration_col3 = st.columns(3)
                            
                            with duration_col1:
                                txn1_duration = txn1_data.get('Duration (seconds)', 0)
                                st.metric("Transaction 1 Duration", f"{txn1_duration}s")
                            
                            with duration_col2:
                                txn2_duration = txn2_data.get('Duration (seconds)', 0)
                                st.metric("Transaction 2 Duration", f"{txn2_duration}s")
                            
                            with duration_col3:
                                duration_diff = txn2_duration - txn1_duration
                                st.metric(
                                    "Duration Difference",
                                    f"{abs(duration_diff)}s",
                                    delta=f"TXN2 {'slower' if duration_diff > 0 else 'faster'}"
                                )
                        
                        st.markdown("---")
                        
                        # Source file comparison
                        st.markdown("#####   Source File Information")
                        source_col1, source_col2 = st.columns(2)
                        
                        with source_col1:
                            st.info(f"**Transaction 1 Source:**\n\n{txn1_data.get('Source File', 'Unknown')}")
                        
                        with source_col2:
                            st.info(f"**Transaction 2 Source:**\n\n{txn2_data.get('Source File', 'Unknown')}")
                        
                        # Check if from same source
                        if txn1_data.get('Source File') == txn2_data.get('Source File'):
                            st.success("Both transactions are from the same source file")
                        else:
                            st.warning("Transactions are from different source files")
                        
                        st.markdown("---")
                        
                        # Screen-by-screen comparison
                        st.markdown("#####   Screen-by-Screen Breakdown")
                        
                        # Extract screen names for comparison
                        def get_screen_names(flow):
                            screens = []
                            for item in flow:
                                if isinstance(item, dict):
                                    screens.append(item.get('screen', str(item)))
                                else:
                                    screens.append(str(item))
                            return screens
                        
                        txn1_screens = get_screen_names(txn1_flow)
                        txn2_screens = get_screen_names(txn2_flow)
                        
                        # Unique to Transaction 1
                        unique_to_txn1 = set(txn1_screens) - set(txn2_screens)
                        if unique_to_txn1:
                            with st.expander(f"Screens unique to {txn1_id} ({len(unique_to_txn1)})"):
                                for screen in sorted(unique_to_txn1):
                                    st.markdown(f"- {screen}")
                        else:
                            st.info(f"No screens unique to {txn1_id}")
                        
                        # Unique to Transaction 2
                        unique_to_txn2 = set(txn2_screens) - set(txn1_screens)
                        if unique_to_txn2:
                            with st.expander(f"Screens unique to {txn2_id} ({len(unique_to_txn2)})"):
                                for screen in sorted(unique_to_txn2):
                                    st.markdown(f"- {screen}")
                        else:
                            st.info(f"No screens unique to {txn2_id}")
                        
                        # Common screens
                        common = set(txn1_screens) & set(txn2_screens)
                        if common:
                            with st.expander(f"Common screens ({len(common)})", expanded=True):
                                for screen in sorted(common):
                                    st.markdown(f"- {screen}")
                
                elif comparison_response.status_code == 404:
                    error_detail = comparison_response.json().get('detail', 'Transaction not found')
                    st.error(f"  {error_detail}")
                elif comparison_response.status_code == 400:
                    error_detail = comparison_response.json().get('detail', 'Bad request')
                    st.error(f"  {error_detail}")
                else:
                    st.error(f"Failed to compare transactions. Status code: {comparison_response.status_code}")
                    
            except requests.exceptions.Timeout:
                st.error("⏱  Request timeout while comparing transactions. Please try again.")
            except requests.exceptions.ConnectionError:
                st.error("  Connection error. Ensure the API server is running.")
            except Exception as e:
                st.error(f"  Error during comparison: {str(e)}")
                import traceback
                with st.expander("  Debug Information"):
                    st.code(traceback.format_exc())
    
    except requests.exceptions.Timeout:
        st.error("⏱  Request timeout. Please try again.")
    except requests.exceptions.ConnectionError:
        st.error("  Connection error. Ensure the API server is running on localhost:8000.")
    except Exception as e:
        st.error(f"  Error in transaction comparison: {str(e)}")
        import traceback
        with st.expander("  Debug Information"):
            st.code(traceback.format_exc())

def render_ui_flow_individual():
    """
    FUNCTION:
        render_ui_flow_individual

    DESCRIPTION:
        Renders an interactive visualization of the UI flow for a single transaction
        in Streamlit. The function performs the following steps:
            - Checks if customer journals have been analyzed; performs analysis if needed
            - Retrieves available source files and filters transactions by selected sources
            - Allows optional filtering by transaction type and end state
            - Lets the user select a specific transaction to visualize
            - Fetches transaction UI flow and displays:
                - Transaction metrics (Type, State, Start/End Time, Source File, UI Events)
                - UI flow visualization (via Plotly flowchart or fallback list)
                - Full transaction log
        Provides an intuitive interface to explore and debug individual transaction flows.

    USAGE:
        render_ui_flow_individual()

    PARAMETERS:
        None

    RETURNS:
        None :
            This function renders Streamlit UI elements directly and does not return a value.

    RAISES:
        Exception :
            Any unexpected errors during API calls, analysis, filtering, or visualization
            are caught and displayed via Streamlit.
    """
    st.markdown("###   UI Flow of Individual Transaction")
    
    need_analysis = False
    
    try:
        # STEP 1: Try to get source files - if it fails, we need to analyze
        try:
            sources_response = requests.get(
                f"{API_BASE_URL}/get-transactions-with-sources",
                timeout=30
            )
            
            if sources_response.status_code == 200:
                sources_data = sources_response.json()
                available_sources = sources_data.get('source_files', [])
                
                # Check if we actually have sources
                if not available_sources:
                    need_analysis = True
            else:
                need_analysis = True
                
        except Exception as e:
            st.error(f"Error checking for source files: {str(e)}")
            need_analysis = True
        
        # STEP 2: If we need analysis, do it now
        if need_analysis:
            st.info("  Customer journals need to be analyzed first...")
            
            with st.spinner("Analyzing customer journals... This may take a moment."):
                try:
                    analyze_response = requests.post(
                        f"{API_BASE_URL}/analyze-customer-journals",
                        timeout=120
                    )
                    
                    if analyze_response.status_code == 200:
                        analyze_data = analyze_response.json()
                        st.success(f"✓ Analysis complete! Found {analyze_data.get('total_transactions', 0)} transactions")
                        import time
                        time.sleep(0.5)
                        st.rerun()
                    else:
                        error_detail = analyze_response.json().get('detail', 'Analysis failed')
                        st.error(f"  Failed to analyze customer journals: {error_detail}")
                        return
                        
                except requests.exceptions.Timeout:
                    st.error("⏱  Analysis timeout. The file may be too large.")
                    return
                except requests.exceptions.ConnectionError:
                    st.error("  Connection error. Ensure the API server is running on localhost:8000.")
                    return
                except Exception as e:
                    st.error(f"  Error during analysis: {str(e)}")
                    import traceback
                    with st.expander("  Debug Information"):
                        st.code(traceback.format_exc())
                    return
        
        # STEP 3: Get source files again after potential analysis
        sources_response = requests.get(
            f"{API_BASE_URL}/get-transactions-with-sources",
            timeout=30
        )
        
        if sources_response.status_code != 200:
            st.error(f"Failed to retrieve source files. Status: {sources_response.status_code}")
            try:
                error_detail = sources_response.json().get('detail', 'Unknown error')
                st.info(f"Details: {error_detail}")
            except:
                pass
            return
        
        sources_data = sources_response.json()
        available_sources = sources_data.get('source_files', [])
        all_transactions = sources_data.get('all_transactions', [])
        
        if not available_sources:
            st.warning("  No source files found even after analysis.")
            st.info("Please check:\n1. ZIP file contains customer journal files\n2. Customer journal files contain valid transaction data")
            return
        
        # STEP 4: Display source file selection
        
        selected_sources = st.multiselect(
            "Select one or more source files to view their transactions",
            options=available_sources,
            default=None,
            key="ui_flow_sources",
            help="Select source files to filter transactions"
        )
        
        if not selected_sources:
            return
        
        # STEP 5: Filter transactions by selected sources
        
        filter_response = requests.post(
            f"{API_BASE_URL}/filter-transactions-by-sources",
            json={"source_files": selected_sources},
            timeout=30
        )
        
        if filter_response.status_code != 200:
            st.error(f"Failed to filter transactions. Status: {filter_response.status_code}")
            try:
                error_detail = filter_response.json().get('detail', 'Unknown error')
                st.info(f"Details: {error_detail}")
            except:
                pass
            return
        
        filtered_data = filter_response.json()
        filtered_transactions = filtered_data.get('transactions', [])
        
        if not filtered_transactions:
            st.warning("  No transactions found for the selected source files.")
            return
        
        # Convert to DataFrame for easier filtering
        txn_df = pd.DataFrame(filtered_transactions)
        
        # STEP 6: Add filters for Type and State
        st.markdown("---")
        
        col1, col2 = st.columns(2)
        
        with col1:
            # Get unique transaction types
            unique_types = sorted(txn_df['Transaction Type'].unique().tolist())
            filter_type = st.selectbox(
                "Transaction Type (Optional)",
                options=['All'] + unique_types,
                key="ui_flow_type_filter"
            )
        
        with col2:
            # Get unique states
            unique_states = sorted(txn_df['End State'].unique().tolist())
            filter_state = st.selectbox(
                "End State (Optional)",
                options=['All'] + unique_states,
                key="ui_flow_state_filter"
            )
        
        # Apply filters
        display_df = txn_df.copy()
        
        if filter_type != 'All':
            display_df = display_df[display_df['Transaction Type'] == filter_type]
        
        if filter_state != 'All':
            display_df = display_df[display_df['End State'] == filter_state]
        
        if len(display_df) == 0:
            st.warning("  No transactions match the selected filters.")
            return
        
        # Display filtered count
        if len(display_df) != len(txn_df):
            st.info(f"Filtered to {len(display_df)} transaction(s)")

        # Transaction ID search
        st.markdown("---")
        search_txn_id = st.text_input(
            "  Search Transaction ID",
            placeholder="Enter Transaction ID to search...",
            key="ui_flow_txn_search"
        )
        
        if search_txn_id:
            display_df = display_df[display_df['Transaction ID'].str.contains(search_txn_id, case=False, na=False)]
            if len(display_df) == 0:
                st.warning("  No transactions match the search term")
                return
            st.info(f"Search filtered to {len(display_df)} transaction(s)")
        
        # Create options for selectbox
        transaction_options = []
        for _, txn in display_df.iterrows():
            txn_id = txn.get('Transaction ID', 'N/A')
            txn_type = txn.get('Transaction Type', 'Unknown')
            txn_state = txn.get('End State', 'Unknown')
            source_file = txn.get('Source File', 'Unknown')
            start_time = txn.get('Start Time', 'N/A')
            transaction_options.append(f"{txn_id} | {txn_type} | {txn_state} | {start_time} | {source_file}")
        
        selected_option = st.selectbox(
            "Select a transaction to visualize",
            options=["Select a transaction..."] + transaction_options,
            key="ui_flow_transaction_select"
        )
        
        if selected_option == "Select a transaction...":
            return
        
        # Extract transaction ID from selected option
        selected_txn_id = selected_option.split(" | ")[0]
        
        # STEP 8: Get and display the UI flow
        st.markdown("---")
        
        with st.spinner(f"Loading UI flow for transaction {selected_txn_id}..."):
            try:
                viz_response = cached_request(
                    'post',
                    f"{API_BASE_URL}/visualize-individual-transaction-flow",
                    cache_enabled=True,
                    json={"transaction_id": selected_txn_id},
                    timeout=60
                )
                
                if viz_response.status_code == 200:
                    viz_data = viz_response.json()
                    
                    # Display transaction details
                    st.markdown(f"### Transaction: {viz_data['transaction_id']}")
                    
                    col1, col2, col3 = st.columns(3)
                    with col1:
                        st.metric("Type", viz_data['transaction_type'])
                    with col2:
                        st.metric("State", viz_data['end_state'])
                    with col3:
                        st.metric("UI Events", viz_data.get('num_events', 0))
                    
                    col4, col5, col6 = st.columns(3)
                    with col4:
                        st.metric("Start Time", viz_data.get('start_time', 'N/A'))
                    with col5:
                        st.metric("End Time", viz_data.get('end_time', 'N/A'))
                    with col6:
                        st.metric("Source File", viz_data.get('source_file', 'N/A'))
                    
                    # Display UI flow
                    st.markdown("---")
                    st.markdown("####   UI Flow Visualization")
                    
                    ui_flow = viz_data.get('ui_flow', [])
                    has_flow = viz_data.get('has_flow', False)
                    
                    if has_flow and ui_flow:
                        
                        # Create and display the flowchart
                        fig = create_individual_flow_plotly(
                            viz_data['transaction_id'],
                            viz_data['end_state'],
                            ui_flow
                        )
                        
                        if fig:
                            st.plotly_chart(fig, use_container_width=True)
                        else:
                            # Fallback to list view
                            for idx, screen in enumerate(ui_flow, 1):
                                st.markdown(f"**{idx}.** {screen}")
                    else:
                        st.warning("  No UI flow data available for this transaction")
                        st.info("This could mean:\n- No UI journal files were uploaded\n- The transaction time range doesn't match any UI events\n- UI journal data is incomplete")
                    
                    # Show transaction log
                    st.markdown("---")
                    st.markdown("####   Transaction Log")
                    with st.expander("View Full Transaction Log", expanded=False):
                        st.code(viz_data.get('transaction_log', 'No log available'), language="text")
                
                else:
                    error_detail = viz_response.json().get('detail', 'Visualization failed')
                    st.error(f"  {error_detail}")
                    
            except requests.exceptions.Timeout:
                st.error("⏱  Request timeout. Please try again.")
            except requests.exceptions.ConnectionError:
                st.error("  Connection error. Ensure the API server is running on localhost:8000.")
            except Exception as e:
                st.error(f"  Error in UI flow visualization: {str(e)}")
                import traceback
                with st.expander("  Debug Information"):
                    st.code(traceback.format_exc())
    
    except requests.exceptions.Timeout:
        st.error("⏱  Request timeout. Please try again.")
    except requests.exceptions.ConnectionError:
        st.error("  Connection error. Ensure the API server is running on localhost:8000.")
    except Exception as e:
        st.error(f"  Error loading UI flow: {str(e)}")
        import traceback
        with st.expander("  Debug Information"):
            st.code(traceback.format_exc())

def render_under_construction(function_name: str):
    """
    FUNCTION:
        render_under_construction

    DESCRIPTION:
        Displays an "Under Construction" message in Streamlit for a given feature or
        function. Useful for indicating features that are planned but not yet implemented.

    USAGE:
        render_under_construction("Feature Name")

    PARAMETERS:
        function_name (str) : 
            The name of the feature or function that is under development.

    RETURNS:
        None :
            This function renders Streamlit UI elements directly and does not return a value.

    NOTES:
        - This is purely a UI placeholder and does not perform any backend operations.
    """
    st.markdown(f"### {function_name}")
    st.warning("This feature is currently under development.")

def create_individual_flow_plotly(txn_id, txn_state, flow_screens):
    """
    FUNCTION:
        create_individual_flow_plotly

    DESCRIPTION:
        Creates a Plotly visualization representing the UI flow of an individual transaction.
        Each screen in the transaction flow is displayed as a colored box with arrows
        indicating the sequence of screens. The color of the boxes can reflect the
        transaction state.

    USAGE:
        fig = create_individual_flow_plotly(txn_id="TXN123", txn_state="Successful", flow_screens=["Login", "Main Menu", "Withdrawal"])

    PARAMETERS:
        txn_id (str) :
            The unique identifier of the transaction.
        
        txn_state (str) :
            The end state of the transaction (e.g., "Successful", "Unsuccessful").
            This may influence the color of the boxes in the visualization.
        
        flow_screens (list of str) :
            Ordered list of UI screens visited during the transaction.
            Each element represents a screen or step in the transaction flow.

    RETURNS:
        plotly.graph_objects.Figure or None :
            Returns a Plotly Figure object visualizing the transaction flow.
            Returns None if the `flow_screens` list is empty or contains no valid flow data.

    NOTES:
        - The visualization uses rectangular boxes for screens and arrows to show the flow.
        - The layout adapts its height based on the number of screens.
        - Background is dark-themed to match Streamlit dark mode aesthetics.
    """
    import plotly.graph_objects as go
    
    if not flow_screens or flow_screens[0] == 'No flow data':
        return None
    
    # Check if we have detailed flow data (dict) or simple flow (string)
    has_details = isinstance(flow_screens[0], dict)
    
    # Color based on transaction state
    if txn_state == 'Successful':
        box_color = '#2563eb'
    elif txn_state == 'Unsuccessful':
        box_color = '#2563eb'
    else:
        box_color = '#2563eb'
    
    fig = go.Figure()
    
    max_screens = len(flow_screens)
    
    # Add boxes for each screen
    for i, screen_data in enumerate(flow_screens):
        y_pos = max_screens - 1 - i
        
        # Extract screen name and duration
        if has_details:
            screen_name = screen_data.get('screen', 'Unknown')
            timestamp = screen_data.get('timestamp', '')
            duration = screen_data.get('duration')
            
            # Format the text with duration
            if duration is not None:
                text_label = f"{i+1}. {screen_name}\n({duration:.2f}s)"
            else:
                text_label = f"{i+1}. {screen_name}"
        else:
            # Old format: just screen name
            screen_name = screen_data
            text_label = f"{i+1}. {screen_name}"
        
        # Add box
        fig.add_shape(
            type="rect",
            x0=0.1, x1=0.9, y0=y_pos, y1=y_pos + 0.7,
            fillcolor=box_color,
            line=dict(color=box_color, width=2)
        )
        
        # Add text with duration
        fig.add_annotation(
            x=0.5, y=y_pos + 0.35,
            text=text_label,
            showarrow=False,
            font=dict(color="white", size=11, family="Arial"),
            xanchor="center", yanchor="middle"
        )
        
        # Add arrow to next screen (if not last)
        if i < len(flow_screens) - 1:
            fig.add_annotation(
                x=0.5, y=(y_pos - 1) + 0.7, # Arrow tip: top of the next box
                ax=0.5, ay=y_pos,           # Arrow tail: bottom of the current box
                xref='x', yref='y',
                axref='x', ayref='y',
                showarrow=True,
                arrowhead=2,
                arrowsize=1,
                arrowwidth=2,
                arrowcolor=box_color
            )
    
    # Calculate height based on number of screens
    height = max(400, max_screens * 100)
    
    # Update layout
    fig.update_layout(
        height=height,
        showlegend=False,
        plot_bgcolor='#0E1117',
        paper_bgcolor='#0E1117',
        font=dict(color='white'),
        title=dict(
            text=f"UI Flow: {txn_id}",
            font=dict(size=16, color='white'),
            x=0.5,
            xanchor='center'
        ),
        margin=dict(l=20, r=20, t=60, b=20)
    )
    
    # Update axes
    fig.update_xaxes(
        showgrid=False, 
        showticklabels=False, 
        zeroline=False,
        range=[0, 1]
    )
    fig.update_yaxes(
        showgrid=False, 
        showticklabels=False, 
        zeroline=False,
        range=[-0.5, max_screens]
    )
    
    return fig

def create_consolidated_flow_plotly(flow_data):
    """
    FUNCTION:
        create_consolidated_flow_plotly

    DESCRIPTION:
        Generates a Plotly visualization representing the consolidated UI flow for all 
        transactions of a specific type. Each screen is displayed as a colored box, 
        with arrows showing transitions between screens. The number of transactions 
        passing through each screen and transition is annotated for insight into usage patterns.

    USAGE:
        fig = create_consolidated_flow_plotly(flow_data)

    PARAMETERS:
        flow_data (dict) :
            A dictionary containing transaction flow information with the following keys:
            
            - 'screens' (list of str) :
                List of unique screens involved in the transaction type.
            
            - 'transitions' (list of dict) :
                Each dictionary represents a transition with keys:
                    'from' (str) : Source screen
                    'to' (str)   : Destination screen
                    'count' (int): Number of transactions that followed this transition
            
            - 'screen_transactions' (dict) :
                Maps screen names to a list of transactions passing through that screen. 
                Each transaction entry is a dictionary with keys like 'txn_id', 'start_time', and 'state'.
            
            - 'transaction_type' (str) :
                The type of transactions being visualized.
            
            - 'transactions_with_flow' (int) :
                Total number of transactions included in the flow visualization.

    RETURNS:
        plotly.graph_objects.Figure or None :
            Returns a Plotly Figure visualizing the consolidated flow of transactions.
            Returns None if there are no screens in the flow_data.

    NOTES:
        - Screens are color-coded: success/complete (green), error/failure (red), normal flow (blue).
        - Arrows represent transitions, annotated with transaction counts.
        - Layout adapts to the number of screens and columns.
        - Hovering over a screen shows sample transactions passing through it.
    """
    import plotly.graph_objects as go
    from collections import defaultdict
    
    screens = flow_data['screens']
    transitions = flow_data['transitions']
    screen_transactions = flow_data['screen_transactions']
    
    if not screens:
        return None
    
    # Define screen hierarchy for positioning
    screen_hierarchy = {
        'Login': 0, 'PIN': 0, 'Authentication': 0, 'CardInsert': 0,
        'MainMenu': 1, 'Menu': 1, 'MenuSelection': 1,
        'Balance': 2, 'BalanceInquiry': 2, 'Inquiry': 2,
        'Withdraw': 2, 'Transfer': 2, 'Deposit': 2,
        'Amount': 3, 'Account': 3, 'FromAccount': 3, 'ToAccount': 3,
        'Confirm': 4, 'Confirmation': 4, 'Verify': 4,
        'Authorize': 5, 'Authorization': 5, 'DMAuthorization': 5,
        'Processing': 6, 'Wait': 6, 'PleaseWait': 6,
        'Cash': 7, 'Card': 7, 'Dispense': 7,
        'Receipt': 8, 'ReceiptPrint': 8, 'Print': 8,
        'End': 9, 'Complete': 9, 'Success': 9, 'ThankYou': 9,
        'Error': 10, 'Cancel': 10, 'Failed': 10, 'Timeout': 10
    }
    
    # Sort screens by hierarchy
    screens_list = sorted(screens, key=lambda x: (screen_hierarchy.get(x, 99), x))
    
    # Calculate grid layout with more spacing
    cols = 3
    rows = (len(screens_list) + cols - 1) // cols
    
    cell_width, cell_height = 25, 16
    box_width, box_height = 16, 10
    
    positions = {}
    
    for i, screen in enumerate(screens_list):
        col = i % cols
        row = i // cols
        x = col * cell_width
        y = -row * cell_height
        positions[screen] = (x, y)
    
    fig = go.Figure()
    
    # Build a map of outgoing transitions for each screen
    outgoing_transitions = defaultdict(list)
    for transition in transitions:
        from_screen = transition['from']
        to_screen = transition['to']
        count = transition['count']
        outgoing_transitions[from_screen].append({
            'to': to_screen,
            'count': count
        })
    
    # Helper function to create curved path between two points
    def create_curved_arrow_path(x0, y0, x1, y1):
        """Create a smooth curved path using bezier curve"""
        dx = x1 - x0
        dy = y1 - y0
        
        # Determine curve direction based on relative position
        if abs(dx) > abs(dy):  # Horizontal connection
            cx1 = x0 + dx * 0.5
            cy1 = y0
            cx2 = x0 + dx * 0.5
            cy2 = y1
        else:  # Vertical connection
            cx1 = x0
            cy1 = y0 + dy * 0.5
            cx2 = x1
            cy2 = y0 + dy * 0.5
        
        # Generate points along bezier curve
        t = np.linspace(0, 1, 50)
        curve_x = (1-t)**3 * x0 + 3*(1-t)**2*t * cx1 + 3*(1-t)*t**2 * cx2 + t**3 * x1
        curve_y = (1-t)**3 * y0 + 3*(1-t)**2*t * cy1 + 3*(1-t)*t**2 * cy2 + t**3 * y1
        
        return curve_x, curve_y
    
    # Helper function to get anchor points
    def get_anchor_points(screen_name):
        """Get the anchor points for a screen box"""
        x, y = positions[screen_name]
        return {
            'top': (x, y + box_height / 2 + 0.5),
            'bottom': (x, y - box_height / 2 - 0.5),
            'left': (x - box_width / 2 - 0.5, y),
            'right': (x + box_width / 2 + 0.5, y)
        }
    
    # Add transitions with curved arrows
    for transition in transitions:
        from_screen = transition['from']
        to_screen = transition['to']
        count = transition['count']
        
        if from_screen in positions and to_screen in positions:
            from_anchors = get_anchor_points(from_screen)
            to_anchors = get_anchor_points(to_screen)
            
            from_x, from_y = positions[from_screen]
            to_x, to_y = positions[to_screen]
            
            # Determine best connection points
            start_point = from_anchors['bottom']
            end_point = to_anchors['top']
            
            # If on same row, use horizontal connections
            if abs(to_y - from_y) < 2:
                if to_x > from_x:
                    start_point = from_anchors['right']
                    end_point = to_anchors['left']
                else:
                    start_point = from_anchors['left']
                    end_point = to_anchors['right']
            elif to_y > from_y:  # Going up
                start_point = from_anchors['top']
                end_point = to_anchors['bottom']
            
            x0, y0 = start_point
            x1, y1 = end_point
            
            # Create curved path
            curve_x, curve_y = create_curved_arrow_path(x0, y0, x1, y1)
            
            # Draw the curved line
            fig.add_trace(go.Scatter(
                x=curve_x,
                y=curve_y,
                mode='lines',
                line=dict(color='#2E7D32', width=3.5),
                showlegend=False,
                hoverinfo='skip'
            ))
            
            # Calculate arrow direction from last few points
            dx = x1 - curve_x[-5]
            dy = y1 - curve_y[-5]
            angle = np.arctan2(dy, dx)
            
            # Create larger arrowhead using SVG path
            arrow_length = 1.2
            arrow_width = 0.8
            
            # Calculate arrowhead points
            tip_x = x1
            tip_y = y1
            
            # Base points of the triangle
            base_left_x = tip_x - arrow_length * np.cos(angle) - arrow_width * np.sin(angle)
            base_left_y = tip_y - arrow_length * np.sin(angle) + arrow_width * np.cos(angle)
            
            base_right_x = tip_x - arrow_length * np.cos(angle) + arrow_width * np.sin(angle)
            base_right_y = tip_y - arrow_length * np.sin(angle) - arrow_width * np.cos(angle)
            
            # Draw filled triangle arrowhead
            fig.add_trace(go.Scatter(
                x=[tip_x, base_left_x, base_right_x, tip_x],
                y=[tip_y, base_left_y, base_right_y, tip_y],
                fill='toself',
                fillcolor='#2E7D32',
                line=dict(color='#2E7D32', width=0),
                mode='lines',
                showlegend=False,
                hoverinfo='skip'
            ))
    
    # Add screen boxes
    for screen in screens_list:
        x, y = positions[screen]
        txn_list = screen_transactions.get(screen, [])
        
        # Create hover text with outgoing transition counts
        hover_text = f"<b>{screen}</b><br><br><b>{len(txn_list)} transactions</b><br><br>"
        
        # Add outgoing transitions info
        if screen in outgoing_transitions and outgoing_transitions[screen]:
            hover_text += "<b>Next screens:</b><br>"
            for out_trans in outgoing_transitions[screen]:
                hover_text += f"→ {out_trans['to']}: {out_trans['count']} time(s)<br>"
            hover_text += "<br>"
        
        # Add transaction IDs
        hover_text += "<b>Transactions:</b><br>"
        for i, txn_info in enumerate(txn_list[:5]):
            hover_text += f"• {txn_info['txn_id']} ({txn_info['state']})<br>"
        if len(txn_list) > 5:
            hover_text += f"...and {len(txn_list) - 5} more"
        
        # All boxes same color
        box_color = '#B3D9FF'
        
        # Add rectangle
        fig.add_shape(
            type="rect",
            x0=x - box_width / 2, 
            x1=x + box_width / 2,
            y0=y - box_height / 2, 
            y1=y + box_height / 2,
            line=dict(color='#1976D2', width=2),
            fillcolor=box_color,
            layer="above"
        )
        
        # Add screen name
        fig.add_annotation(
            x=x,
            y=y,
            text=f"<b>{screen}</b>",
            showarrow=False,
            font=dict(size=10, family='Arial', color='black'),
            xref='x',
            yref='y'
        )
        
        # Add invisible hover point
        fig.add_trace(go.Scatter(
            x=[x], 
            y=[y],
            mode='markers',
            marker=dict(size=0.1, opacity=0),
            hovertemplate=hover_text + '<extra></extra>',
            showlegend=False
        ))
    
    # Update layout
    all_x = [p[0] for p in positions.values()]
    all_y = [p[1] for p in positions.values()]
    
    fig.update_layout(
        title=dict(
            text=f"<b>Consolidated Flow:</b> {flow_data['transaction_type']}<br><sub>({flow_data['transactions_with_flow']} transactions)</sub>",
            font=dict(size=22, color='black'),
            x=0.5,
            xanchor='center'
        ),
        xaxis=dict(
            showgrid=False, 
            zeroline=False, 
            showticklabels=False, 
            range=[min(all_x) - cell_width / 2, max(all_x) + cell_width / 2]
        ),
        yaxis=dict(
            showgrid=False, 
            zeroline=False, 
            showticklabels=False, 
            range=[min(all_y) - cell_height, max(all_y) + cell_height / 2],
            scaleanchor="x",
            scaleratio=1
        ),
        height=rows * 260 + 120,
        width=cols * 380 + 120,
        plot_bgcolor='white',
        paper_bgcolor='white',
        margin=dict(t=120, l=60, r=60, b=60),
        hovermode='closest'
    )
    
    return fig

def render_consolidated_flow():
    """
    FUNCTION:
        render_consolidated_flow

    DESCRIPTION:
        Renders the consolidated UI flow analysis for a selected transaction type 
        within a source file. Retrieves transaction data, allows filtering by source 
        file and transaction type, and generates a Plotly visualization showing 
        screens as colored boxes and transitions with transaction counts. Provides 
        detailed metrics for successful and unsuccessful transactions and lists 
        individual transaction flows.

    USAGE:
        render_consolidated_flow()

    PARAMETERS:
        None : Uses Streamlit widgets for user input (source file and transaction type).

    RETURNS:
        None : The function renders the UI and visualizations directly in Streamlit.

    NOTES:
        - Requires prior analysis of customer journals; will trigger analysis if not done.
        - Screens in the consolidated flow are color-coded:
            • Normal flow: light blue
            • Successful/Complete screens: light green
            • Error/Failed screens: light red
        - Arrows represent transitions with transaction counts.
        - Hovering over a screen shows sample transactions passing through it.
        - Includes metrics for total, successful, and unsuccessful transactions.
        - Detailed transaction flows can be expanded to review each transaction's UI path.
    """

    st.markdown("###   Consolidated Transaction UI Flow and Analysis")
    
    need_analysis = False
    
    try:
        # STEP 1: Check if analysis is needed
        try:
            sources_response = requests.get(
                f"{API_BASE_URL}/get-transactions-with-sources",
                timeout=30
            )
            
            if sources_response.status_code == 200:
                sources_data = sources_response.json()
                available_sources = sources_data.get('source_files', [])
                
                if not available_sources:
                    need_analysis = True
            else:
                need_analysis = True
                
        except Exception as e:
            need_analysis = True
        
        # STEP 2: Perform analysis if needed
        if need_analysis:
            st.info("  Customer journals need to be analyzed first...")
            
            with st.spinner("Analyzing customer journals..."):
                try:
                    analyze_response = requests.post(
                        f"{API_BASE_URL}/analyze-customer-journals",
                        timeout=120
                    )
                    
                    if analyze_response.status_code == 200:
                        st.success("✓ Analysis complete!")
                        import time
                        time.sleep(0.5)
                        st.rerun()
                    else:
                        error_detail = analyze_response.json().get('detail', 'Analysis failed')
                        st.error(f"  {error_detail}")
                        return
                except Exception as e:
                    st.error(f"  Error during analysis: {str(e)}")
                    return
        
        # STEP 3: Get source files
        sources_response = requests.get(
            f"{API_BASE_URL}/get-transactions-with-sources",
            timeout=30
        )
        
        if sources_response.status_code != 200:
            st.error("Failed to retrieve source files")
            return
        
        sources_data = sources_response.json()
        available_sources = sources_data.get('source_files', [])
        all_transactions = sources_data.get('all_transactions', [])
        
        if not available_sources:
            st.warning("  No source files available")
            return
        
        selected_source = st.selectbox(
            "Source File",
            options=available_sources,
            key="consolidated_source"
        )
        
        if not selected_source:
            return
        
        # Filter transactions from this source
        source_transactions = [txn for txn in all_transactions if txn.get('Source File') == selected_source]
        
        if not source_transactions:
            st.warning(f"No transactions found in source '{selected_source}'")
            return
        
        # Get unique transaction types from this source
        txn_df = pd.DataFrame(source_transactions)
        unique_types = sorted(txn_df['Transaction Type'].dropna().unique().tolist())
        
        if not unique_types:
            st.warning("No transaction types found in this source")
            return
        
        # STEP 5: Select transaction type
        st.markdown("---")
        
        selected_type = st.selectbox(
            "Transaction Type",
            options=unique_types,
            key="consolidated_type"
        )
        
        # Show stats for selected type
        type_transactions = txn_df[txn_df['Transaction Type'] == selected_type]
        
        col1, col2, col3 = st.columns(3)
        with col1:
            st.metric("Total Transactions", len(type_transactions))
        with col2:
            successful = len(type_transactions[type_transactions['End State'] == 'Successful'])
            st.metric("Successful", successful)
        with col3:
            unsuccessful = len(type_transactions[type_transactions['End State'] == 'Unsuccessful'])
            st.metric("Unsuccessful", unsuccessful)
        
        # STEP 6: Generate consolidated flow
        st.markdown("---")
        
        if st.button("  Generate Consolidated Flow", use_container_width=True):
            with st.spinner(f"Generating consolidated flow for {selected_type}..."):
                try:
                    response = requests.post(
                        f"{API_BASE_URL}/generate-consolidated-flow",
                        json={
                            "source_file": selected_source,
                            "transaction_type": selected_type
                        },
                        timeout=60
                    )
                    
                    if response.status_code == 200:
                        flow_data = response.json()
                        
                        # Display the consolidated flow chart
                        st.markdown("---")
                        st.markdown("###   Consolidated Flow Visualization")
                        st.info("  Hover over screens to see transaction IDs. Arrows show flow direction with transaction counts.")
                        
                        fig = create_consolidated_flow_plotly(flow_data)
                        if fig:
                            st.plotly_chart(fig, use_container_width=True)
                        
                        # Show detailed flow information
                        with st.expander("  Transaction Flow Details"):
                            st.markdown(f"**Transactions with UI flow data:** {flow_data['transactions_with_flow']}/{flow_data['total_transactions']}")
                            
                            st.markdown("**Individual Transaction Flows:**")
                            for txn_id, flow_info in flow_data['transaction_flows'].items():
                                st.markdown(
                                    f"• **{txn_id}** ({flow_info['state']}) "
                                    f"[{flow_info['start_time']} - {flow_info['end_time']}]: "
                                    f"{' → '.join(flow_info['screens'])}"
                                )
                    
                    else:
                        error_detail = response.json().get('detail', 'Failed to generate flow')
                        st.error(f"  {error_detail}")
                        
                except requests.exceptions.Timeout:
                    st.error("⏱  Request timeout. Please try again.")
                except requests.exceptions.ConnectionError:
                    st.error("  Connection error. Ensure the API server is running.")
                except Exception as e:
                    st.error(f"  Error: {str(e)}")
                    import traceback
                    with st.expander("  Debug Information"):
                        st.code(traceback.format_exc())
    
    except Exception as e:
        st.error(f"  Error: {str(e)}")
        import traceback
        with st.expander("  Debug Information"):
            st.code(traceback.format_exc())

def render_individual_transaction_analysis():
    """
FUNCTION: render_individual_transaction_analysis

DESCRIPTION:
    Renders a Streamlit interface for analyzing individual transactions using LLM-based insights.
    Users can filter transactions by source file, type, and end state, select a transaction to view
    details, preview logs, and request an AI-driven analysis. Feedback on the analysis can also
    be submitted through a guided form with user authentication.

USAGE:
    render_individual_transaction_analysis()

PARAMETERS:
    None : Uses Streamlit widgets, session state, and API calls for interaction.

RETURNS:
    None : The function renders UI elements and handles API interactions directly.

RAISES:
    requests.exceptions.Timeout        : If API requests (analysis, feedback, or data retrieval) exceed the timeout.
    requests.exceptions.ConnectionError: If the API server is unreachable.
    Exception                         : For general errors during transaction retrieval, analysis, or feedback submission.
"""
    st.markdown("###  Individual Transaction Analysis")
    
    need_analysis = False
    
    try:
        # STEP 1: Check if analysis is needed
        try:
            sources_response = requests.get(
                f"{API_BASE_URL}/get-transactions-with-sources",
                timeout=30
            )
            
            if sources_response.status_code == 200:
                sources_data = sources_response.json()
                available_sources = sources_data.get('source_files', [])
                
                if not available_sources:
                    need_analysis = True
            else:
                need_analysis = True
                
        except Exception as e:
            need_analysis = True
        
        # STEP 2: Perform analysis if needed
        if need_analysis:
            st.info("  Customer journals need to be analyzed first...")
            
            with st.spinner("Analyzing customer journals..."):
                try:
                    analyze_response = requests.post(
                        f"{API_BASE_URL}/analyze-customer-journals",
                        timeout=120
                    )
                    
                    if analyze_response.status_code == 200:
                        st.success("  Analysis complete!")
                        import time
                        time.sleep(0.5)
                        st.rerun()
                    else:
                        error_detail = analyze_response.json().get('detail', 'Analysis failed')
                        st.error(f"  {error_detail}")
                        return
                except Exception as e:
                    st.error(f"  Error during analysis: {str(e)}")
                    return
        
        # STEP 3: Get source files and transactions
        sources_response = requests.get(
            f"{API_BASE_URL}/get-transactions-with-sources",
            timeout=30
        )
        
        if sources_response.status_code != 200:
            st.error("Failed to retrieve transaction data")
            return
        
        sources_data = sources_response.json()
        available_sources = sources_data.get('source_files', [])
        all_transactions = sources_data.get('all_transactions', [])
        
        if not available_sources:
            st.warning("  No source files available")
            return
        
        if not all_transactions:
            st.warning("  No transactions available")
            return
        
        # STEP 4: Filters
        st.markdown("####   Select Transaction")
        
        txn_df = pd.DataFrame(all_transactions)
        
        col1, col2, col3 = st.columns(3)
        
        with col1:
            # Source file filter
            selected_sources = st.multiselect(
                "Source Files",
                options=available_sources,
                default=available_sources,
                key="indiv_analysis_sources"
            )
        
        with col2:
            # Transaction type filter
            if selected_sources:
                filtered_df = txn_df[txn_df['Source File'].isin(selected_sources)]
                unique_types = sorted(filtered_df['Transaction Type'].dropna().unique().tolist())
                
                selected_type = st.selectbox(
                    "Transaction Type",
                    options=['All Types'] + unique_types,
                    key="indiv_analysis_type"
                )
                
                if selected_type != 'All Types':
                    filtered_df = filtered_df[filtered_df['Transaction Type'] == selected_type]
            else:
                filtered_df = pd.DataFrame()
        
        with col3:
            # State filter
            if not filtered_df.empty:
                unique_states = sorted(filtered_df['End State'].dropna().unique().tolist())
                
                selected_state = st.selectbox(
                    "End State",
                    options=['All States'] + unique_states,
                    key="indiv_analysis_state"
                )
                
                if selected_state != 'All States':
                    filtered_df = filtered_df[filtered_df['End State'] == selected_state]
        
        if filtered_df.empty:
            st.warning("  No transactions match the selected filters")
            return
        
        # Transaction ID search
        st.markdown("---")
        search_txn_id = st.text_input(
            "  Search Transaction ID",
            placeholder="Enter Transaction ID to search...",
            key="indiv_analysis_txn_search"
        )
        
        if search_txn_id and not filtered_df.empty:
            filtered_df = filtered_df[filtered_df['Transaction ID'].str.contains(search_txn_id, case=False, na=False)]
            if filtered_df.empty:
                st.warning("  No transactions match the search term")
                return
        
        # STEP 5: Transaction selection
        st.markdown("---")
        st.markdown("####   Select a Transaction to Analyze")
        
        # Create transaction options
        transaction_options = {}
        for _, txn in filtered_df.iterrows():
            txn_id = txn['Transaction ID']
            display = f"{txn_id} | {txn['Transaction Type']} | {txn['End State']} | {txn['Source File']} | {txn['Start Time']}"
            transaction_options[display] = txn_id
        
        selected_display = st.selectbox(
            "Transaction",
            options=list(transaction_options.keys()),
            key="indiv_analysis_txn_select"
        )
        
        selected_txn_id = transaction_options[selected_display]
        selected_txn_data = filtered_df[filtered_df['Transaction ID'] == selected_txn_id].iloc[0]
        
        # STEP 6: Display transaction details
        st.markdown("---")
        
        col1, col2 = st.columns(2)
        
        with col1:
            st.markdown("####   Transaction Details")
            st.markdown(f"**ID:** {selected_txn_data['Transaction ID']}")
            st.markdown(f"**Type:** {selected_txn_data['Transaction Type']}")
            st.markdown(f"**State:** {selected_txn_data['End State']}")
            st.markdown(f"**Start Time:** {selected_txn_data['Start Time']}")
            st.markdown(f"**End Time:** {selected_txn_data['End Time']}")
            st.markdown(f"**Source File:** {selected_txn_data['Source File']}")
        
        with col2:
            st.markdown("####   Transaction Log Preview")
            transaction_log = str(selected_txn_data.get('Transaction Log', 'No log available'))
            
            # Show first 700 characters as preview
            preview = transaction_log[:700] + "..." if len(transaction_log) > 700 else transaction_log
            st.subheader("Transaction Log Preview")
            st.code(transaction_log)            

        # STEP 7: LLM Analysis
        st.markdown("---")
        st.markdown("### DN Transaction Analysis")
        
        # Initialize session state for analysis
        if 'current_analysis_txn' not in st.session_state:
            st.session_state.current_analysis_txn = None
        if 'analysis_result' not in st.session_state:
            st.session_state.analysis_result = None
        
        # Check if we need to clear previous analysis
        if st.session_state.current_analysis_txn != selected_txn_id:
            st.session_state.analysis_result = None
            st.session_state.current_analysis_txn = selected_txn_id
        
        col1, col2 = st.columns([1, 3])
        
        with col1:
            analyze_button = st.button(
                "  Analyze Transaction",
                use_container_width=True,
                type="primary"
            )
        
        with col2:
            if st.session_state.analysis_result:
                print("")
        
        if analyze_button:
            with st.spinner("  DN Analyzer is analyzing the transaction log... This may take a moment."):
                try:
                    response = cached_request(
                        'post',
                        f"{API_BASE_URL}/analyze-transaction-llm",
                        cache_enabled=True,
                        json={"transaction_id": selected_txn_id},
                        timeout=120
                    )
                    
                    if response.status_code == 200:
                        st.session_state.analysis_result = response.json()
                        st.rerun()
                    else:
                        error_detail = response.json().get('detail', 'Analysis failed')
                        st.error(f"  {error_detail}")
                        
                except requests.exceptions.Timeout:
                    st.error("⏱  Analysis timeout. The model may be taking too long to respond.")
                except requests.exceptions.ConnectionError:
                    st.error("  Connection error. Ensure the API server and Ollama are running.")
                except Exception as e:
                    st.error(f"  Error: {str(e)}")
        
        # Display analysis results
        if st.session_state.analysis_result:
            st.markdown("---")
            st.markdown("###   Analysis Results")
            
            result = st.session_state.analysis_result
            
            # Metadata
            col1, col2, col3 = st.columns(3)
            with col1:
                analysis_time = result['metadata'].get('analysis_time_seconds', 'N/A')
                st.metric("Analysis Time", f"{analysis_time} sec")
            with col2:
                st.metric("Log Size", f"{result['metadata']['log_length']} chars")
            with col3:
                st.metric("Analyzed At", result['timestamp'])
            
            # Analysis content
            st.markdown("---")
            st.markdown("####   AI Analysis")
            
            analysis_text = result.get('analysis', 'No analysis available')
            
            # Display in a nice box
            st.markdown(
                f"""
                <div style='background-color: #1e1e1e; padding: 20px; border-radius: 10px; border-left: 5px solid #4CAF50;'>
                <pre style='color: #ffffff; white-space: pre-wrap; word-wrap: break-word; font-family: monospace; font-size: 14px;'>{analysis_text}</pre>
                </div>
                """,
                unsafe_allow_html=True
            )
            
            # Metadata details
            with st.expander("  Analysis Metadata"):
                st.json(result['metadata'])
            
            # STEP 8: LLM Response Feedback
            st.markdown("---")
            st.markdown("###   LLM Response Feedback")
            st.info("Help us improve our AI analysis by providing feedback on the results")
            
            with st.expander("  Provide Feedback", expanded=False):
                st.markdown("*Your feedback helps improve the accuracy of future analyses*")
                
                # Feedback key prefix
                feedback_key_prefix = f"feedback_{selected_txn_id}"
                
                # User authentication data
                users = {
                    "Select User": {"email": "", "passcode": ""},
                    "John Smith (john.smith@company.com)": {"email": "john.smith@company.com", "passcode": "1234"},
                    "Sarah Johnson (sarah.johnson@company.com)": {"email": "sarah.johnson@company.com", "passcode": "5678"},
                    "Michael Brown (michael.brown@company.com)": {"email": "michael.brown@company.com", "passcode": "9012"},
                    "Emily Davis (emily.davis@company.com)": {"email": "emily.davis@company.com", "passcode": "3456"},
                    "Robert Wilson (robert.wilson@company.com)": {"email": "robert.wilson@company.com", "passcode": "7890"}
                }
                
                # Question 1: Rating
                st.markdown("#### 1️  Rate the Analysis Quality")
                rating = st.select_slider(
                    "How would you rate the accuracy and usefulness of the AI analysis?",
                    options=[0, 1, 2, 3, 4, 5],
                    value=3,
                    format_func=lambda x: f"{x} - {'Poor' if x <= 1 else 'Fair' if x <= 2 else 'Good' if x <= 3 else 'Very Good' if x <= 4 else 'Excellent'}",
                    key=f"{feedback_key_prefix}_rating"
                )
                
                # Question 2: Alternative Root Cause
                st.markdown("#### 2️  Alternative Root Cause (if applicable)")
                
                anomaly_categories = [
                    "No alternative needed - AI analysis was correct",
                    "Customer Timeout/Abandonment",
                    "Customer Cancellation",
                    "Card Reading/Hardware Issues",
                    "PIN Authentication Problems",
                    "Cash Dispenser Malfunction",
                    "Account/Balance Issues",
                    "Network/Communication Errors",
                    "System/Software Errors",
                    "Receipt Printer Problems",
                    "Security/Fraud Detection",
                    "Database/Core Banking Issues",
                    "Environmental Factors (Power, etc.)",
                    "User Interface/Display Problems",
                    "Other (please specify in comments)"
                ]
                
                alternative_cause = st.selectbox(
                    "If you believe the AI identified the wrong root cause, please select the correct one:",
                    anomaly_categories,
                    key=f"{feedback_key_prefix}_alternative"
                )
                
                # Question 3: Comments
                st.markdown("#### 3️  Additional Comments")
                feedback_comment = st.text_area(
                    "Please provide any specific feedback, suggestions, or observations:",
                    placeholder="e.g., 'The analysis missed...', 'Very helpful analysis!', 'Could improve by...'",
                    height=100,
                    key=f"{feedback_key_prefix}_comment"
                )
                
                # Check if any question has been answered
                rating_answered = st.session_state.get(f"{feedback_key_prefix}_rating", 3) != 3
                alternative_answered = st.session_state.get(f"{feedback_key_prefix}_alternative", anomaly_categories[0]) != anomaly_categories[0]
                comment_answered = bool(st.session_state.get(f"{feedback_key_prefix}_comment", "").strip())
                
                questions_answered = sum([rating_answered, alternative_answered, comment_answered])
                
                # User Authentication (only if questions answered)
                user_name = ""
                user_email = ""

                if questions_answered > 0:
                    st.markdown("---")
                    st.markdown("####   User Selection Required")
                    st.info("Please select your name to submit feedback.")
                    
                    selected_user = st.selectbox(
                        "Select your name and email:",
                        list(users.keys()),
                        key=f"{feedback_key_prefix}_user_select"
                    )
                    
                    if selected_user != "Select User":
                        user_name = selected_user.split(" (")[0]
                        user_email = users[selected_user]["email"]
                        st.success(f"  Selected: {user_name}")
                    
                    if selected_user == "Select User":
                        st.warning("  Please select your name and email to continue.")
                
                # Submit Feedback
                st.markdown("---")
                col1, col2, col3 = st.columns([2, 2, 3])
                
                with col1:
                    can_submit = questions_answered > 0 and selected_user != "Select User"

                    if st.button("Submit Feedback", 
                            key=f"{feedback_key_prefix}_submit",
                            disabled=not can_submit,
                            type="primary",
                            use_container_width=True):
                        
                        if questions_answered == 0:
                            st.error("Please answer at least one question before submitting.")
                        elif selected_user == "Select User":
                            st.error("Please select your name and email.")
                        else:
                            # Submit feedback to API
                            with st.spinner("Submitting feedback..."):
                                try:
                                    result = st.session_state.analysis_result
                                    
                                    feedback_data = {
                                        "transaction_id": selected_txn_id,
                                        "rating": st.session_state.get(f"{feedback_key_prefix}_rating", 3),
                                        "alternative_cause": st.session_state.get(f"{feedback_key_prefix}_alternative", anomaly_categories[0]),
                                        "comment": st.session_state.get(f"{feedback_key_prefix}_comment", ""),
                                        "user_name": user_name,
                                        "user_email": user_email,
                                        "model_version": result['metadata']['model'],
                                        "original_llm_response": result.get('analysis', '')
                                    }
                                    
                                    response = requests.post(
                                        f"{API_BASE_URL}/submit-llm-feedback",
                                        json=feedback_data,
                                        timeout=30
                                    )
                                    
                                    if response.status_code == 200:
                                        result_data = response.json()
                                        st.success(result_data['message'])
                                        
                                        # Clear form
                                        keys_to_clear = [
                                            f"{feedback_key_prefix}_rating",
                                            f"{feedback_key_prefix}_alternative",
                                            f"{feedback_key_prefix}_comment",
                                            f"{feedback_key_prefix}_user_select"
                                        ]
                                        for key in keys_to_clear:
                                            if key in st.session_state:
                                                del st.session_state[key]
                                        
                                        import time
                                        time.sleep(1)
                                        st.rerun()
                                    else:
                                        error_detail = response.json().get('detail', 'Failed to submit')
                                        st.error(f"  {error_detail}")
                                        
                                except Exception as e:
                                    st.error(f"  Error submitting feedback: {str(e)}")

                with col2:
                    if st.button("Clear Form", 
                            key=f"{feedback_key_prefix}_clear",
                            use_container_width=True):
                        keys_to_clear = [
                            f"{feedback_key_prefix}_rating",
                            f"{feedback_key_prefix}_alternative",
                            f"{feedback_key_prefix}_comment",
                            f"{feedback_key_prefix}_user_select",
                            f"{feedback_key_prefix}_passcode"
                        ]
                        for key in keys_to_clear:
                            if key in st.session_state:
                                del st.session_state[key]
                        st.rerun()


    
    except Exception as e:
        st.error(f"  Error: {str(e)}")
        import traceback
        with st.expander("  Debug Information"):
            st.code(traceback.format_exc())

def render_counters_analysis():
    """
FUNCTION: render_counters_analysis

DESCRIPTION:
    Renders the counters analysis interface in a Streamlit app. 
    Fetches transaction sources, performs analysis if needed, 
    allows selection of source files and transactions, and displays 
    counter data including first, start, per-transaction, logical, 
    and last counters.

USAGE:
    render_counters_analysis()

PARAMETERS:
    This function does not take any parameters. It relies on:
        - API_BASE_URL : Base URL for backend API calls
        - Streamlit session_state for storing selected options

RETURNS:
    None : The function directly renders Streamlit UI components 
           (dataframes, dropdowns, info messages, buttons).

RAISES:
    requests.exceptions.Timeout         : If an API request times out
    requests.exceptions.ConnectionError : If API server is not reachable
    Exception                           : For any unexpected errors during execution
"""

    st.markdown("###   Counters Analysis")
    
    need_analysis = False
    
    try:
        # Check if analysis is needed
        try:
            sources_response = requests.get(
                f"{API_BASE_URL}/get-transactions-with-sources",
                timeout=30
            )
            
            if sources_response.status_code == 200:
                sources_data = sources_response.json()
                available_sources = sources_data.get('source_files', [])
                
                if not available_sources:
                    need_analysis = True
            else:
                need_analysis = True
                
        except Exception as e:
            need_analysis = True
        
        # Perform analysis if needed
        if need_analysis:
            st.info("  Customer journals need to be analyzed first...")
            
            with st.spinner("Analyzing customer journals..."):
                try:
                    analyze_response = requests.post(
                        f"{API_BASE_URL}/analyze-customer-journals",
                        timeout=120
                    )
                    
                    if analyze_response.status_code == 200:
                        st.success("  Analysis complete!")
                        import time
                        time.sleep(0.5)
                        st.rerun()
                    else:
                        error_detail = analyze_response.json().get('detail', 'Analysis failed')
                        st.error(f"  {error_detail}")
                        return
                except Exception as e:
                    st.error(f"  Error during analysis: {str(e)}")
                    return
        
        # Get source files and transactions
        sources_response = requests.get(
            f"{API_BASE_URL}/get-transactions-with-sources",
            timeout=30
        )
        
        if sources_response.status_code != 200:
            st.error("Failed to retrieve transaction data")
            return
        
        sources_data = sources_response.json()
        available_sources = sources_data.get('source_files', [])
        all_transactions = sources_data.get('all_transactions', [])
        
        if not available_sources:
            st.warning("  No source files available")
            return
        
        if not all_transactions:
            st.warning("  No transactions available")
            return
        
        # Get TRC trace files to filter source files
        try:
            file_categories_response = requests.get(
                f"{API_BASE_URL}/debug-session",
                params={"session_id": "current_session"},
                timeout=30
            )
            
            if file_categories_response.status_code == 200:
                debug_data = file_categories_response.json()
                
                # Get matching sources (check which sources have corresponding TRC trace files)
                matching_sources_response = requests.get(
                    f"{API_BASE_URL}/get-matching-sources-for-trc",
                    timeout=30
                )
                
                if matching_sources_response.status_code == 200:
                    matching_data = matching_sources_response.json()
                    filtered_sources = matching_data.get('matching_sources', [])
                    
                    if not filtered_sources:
                        st.warning("  No source files found that match TRC trace files")
                        return
                    
                    available_sources = filtered_sources
                else:
                    st.warning("  Could not filter sources by TRC trace files")
        except Exception as e:
            st.error(f"  Error filtering sources: {e}")
        
        # Source file selection with unique display
        st.markdown("####   Select Source File")
        
        # Create unique identifiers for each source
        txn_df = pd.DataFrame(all_transactions)
        source_summary = {}
        
        for source in available_sources:
            source_txns = txn_df[txn_df['Source File'] == source]
            if len(source_txns) > 0:
                txn_count = len(source_txns)
                first_txn_time = source_txns.iloc[0]['Start Time']
                display_name = f"{source} (starts at {first_txn_time})"
                source_summary[display_name] = source
        
        if not source_summary:
            st.warning("  No transactions found in matching sources")
            return
        
        selected_display = st.selectbox(
            "Choose source file",
            options=list(source_summary.keys()),
            key="counters_source_select"
        )
        
        selected_source = source_summary[selected_display]
        
        # Filter transactions from this source
        source_transactions = txn_df[txn_df['Source File'] == selected_source]
        
        if len(source_transactions) == 0:
            st.warning(f"  No transactions found in source '{selected_source}'")
            return
        
        # Transaction selection
        st.markdown("---")
        st.markdown("####   Select Transaction")

        # Filter to only transactions from this specific source file
        source_only_transactions = txn_df[txn_df['Source File'] == selected_source].copy()

        # Count CIN/CI and COUT/GA transactions
        cin_cout_count = len(source_only_transactions[source_only_transactions['Transaction Type'].isin(['CIN/CI', 'COUT/GA'])])
        other_count = len(source_only_transactions) - cin_cout_count

        # Build transaction options - only CIN/COUT are selectable
        transaction_options = {}

        for _, txn in source_only_transactions.iterrows():
            txn_id = txn['Transaction ID']
            txn_type = txn['Transaction Type']
            display = f"{txn_id} | {txn_type} | {txn['End State']} | {txn['Start Time']}"
            
            if txn_type in ['CIN/CI', 'COUT/GA']:
                transaction_options[display] = txn_id
            else:
                # Add to options but mark as disabled with "(Not available)" suffix
                disabled_key = f"{display} (Not available)"
                transaction_options[disabled_key] = None

        # Show info message if there are disabled transactions
        if other_count > 0:
            st.info(f"  Counter analysis is only available for CIN/CI and COUT/GA transactions.")

        selected_display = st.selectbox(
            "Transaction",
            options=list(transaction_options.keys()),
            key="counters_txn_select",
            help="Only CIN/CI and COUT/GA transactions are available for counter analysis"
        )

        # Check if selected option is disabled
        if "(Not available)" in selected_display:
            st.warning("  This transaction type is not supported for counter analysis. Please select a CIN/CI or COUT/GA transaction.")
            return

        selected_txn_id = transaction_options[selected_display]
        selected_txn_data = source_transactions[source_transactions['Transaction ID'] == selected_txn_id].iloc[0]
        
        st.markdown("---")
        
        # Call API to get counter data
        with st.spinner("Loading counter data..."):
            try:
                response = cached_request(
                    'post',
                    f"{API_BASE_URL}/get-counter-data",
                    cache_enabled=True,
                    json={
                        "transaction_id": selected_txn_id,
                        "source_file": selected_source
                    },
                    timeout=60
                )
                
                if response.status_code == 200:
                    counter_data = response.json()
                    
                    # Display START counter (static - first counter in file)
                    from datetime import datetime
                    
                    start_date = counter_data['start_counter']['date']
                    start_time = counter_data['start_counter']['timestamp']
                    
                    # Format date as "DD Month YYYY"
                    try:
                        if len(start_date) == 6:  # YYMMDD format
                            dt = datetime.strptime(start_date, '%y%m%d')
                            formatted_start_date = dt.strftime('%d %B %Y')
                        else:
                            formatted_start_date = start_date
                    except:
                        formatted_start_date = start_date
                    
                    st.markdown(f"####   First Counter - {formatted_start_date} {start_time}")
                    st.caption("This counter represents the first transaction in the source file")
                    
                    start_df = pd.DataFrame(counter_data['start_counter']['counter_data'])

                    # Get column descriptions
                    col_descriptions = counter_data.get('column_descriptions', {})

                    # Create column config with tooltips
                    column_config = {}
                    for col in start_df.columns:
                        if col in col_descriptions:
                            column_config[col] = st.column_config.TextColumn(
                                col,
                                help=col_descriptions[col],
                                width="small"
                            )

                    st.dataframe(
                        start_df, 
                        use_container_width=True, 
                        hide_index=True,
                        column_config=column_config
                    )
                    
                    st.markdown("---")
                    
                    # Display first counter
                    first_date = counter_data['first_counter']['date']
                    first_time = counter_data['first_counter']['timestamp']
                    
                    # Format date as "DD Month YYYY"
                    try:
                        if len(first_date) == 6:  # YYMMDD format
                            dt = datetime.strptime(first_date, '%y%m%d')
                            formatted_first_date = dt.strftime('%d %B %Y')
                        else:
                            formatted_first_date = first_date
                    except:
                        formatted_first_date = first_date
                    
                    st.markdown(f"####   Start Counter - {formatted_first_date} {first_time}")
                    st.caption("This counter represents the first transaction from in the TRCTrace file based on the selected Transaction")
                    
                    first_df = pd.DataFrame(counter_data['first_counter']['counter_data'])

                    # Get column descriptions
                    col_descriptions = counter_data.get('column_descriptions', {})

                    # Create column config with tooltips
                    column_config = {}
                    for col in first_df.columns:
                        if col in col_descriptions:
                            column_config[col] = st.column_config.TextColumn(
                                col,
                                help=col_descriptions[col],
                                width="small"
                            )

                    st.dataframe(
                        first_df, 
                        use_container_width=True, 
                        hide_index=True,
                        column_config=column_config
                    )
                    
                    st.markdown("---")
                    
                    st.markdown("####   Counter per Transaction")

                    if 'counter_per_transaction' in counter_data and counter_data['counter_per_transaction']:
                        txn_table_data = []
                        
                        for txn_entry in counter_data['counter_per_transaction']:
                            txn_table_data.append({
                                'Date Timestamp': txn_entry['date_timestamp'],
                                'Transaction ID': txn_entry['transaction_id'],
                                'Transaction Type': txn_entry['transaction_type'],
                                'Transaction Summary with Result': txn_entry['transaction_summary'],
                                'Count': txn_entry['count'],
                                'Counter Summary': txn_entry['counter_summary'],
                                'Comment': txn_entry['comment']
                            })
                        
                        txn_df = pd.DataFrame(txn_table_data)
                        
                        # Create column config with tooltips for transaction table
                        txn_column_config = {
                            'Date Timestamp': st.column_config.TextColumn('Date Timestamp', help='Transaction date and time'),
                            'Transaction ID': st.column_config.TextColumn('Transaction ID', help='Unique transaction identifier'),
                            'Transaction Type': st.column_config.TextColumn('Transaction Type', help='Type of transaction (CIN/CI or COUT/GA)'),
                            'Transaction Summary with Result': st.column_config.TextColumn('Transaction Summary with Result', help='Success or failure status'),
                            'Count': st.column_config.TextColumn('Count', help='Denomination and count information'),
                            'Counter Summary': st.column_config.TextColumn('Counter Summary', help='Click to view detailed counter data'),
                            'Comment': st.column_config.TextColumn('Comment', help='Additional notes')
                        }
                        
                        # Apply styling for success/failure
                        def highlight_state(row):
                            summary = str(row['Transaction Summary with Result']).strip().lower()
                            
                            # Create a list of styles for each column
                            styles = ['color: white'] * len(row)  # Default all columns to white
                            
                            # Find the index of 'Transaction Summary with Result' column
                            summary_col_idx = row.index.get_loc('Transaction Summary with Result')
                            
                            # Apply color only to the summary column
                            if summary == 'successful':
                                styles[summary_col_idx] = 'color: green; font-weight: bold'
                            elif summary == 'unsuccessful':
                                styles[summary_col_idx] = 'color: red; font-weight: bold'
                            else:
                                styles[summary_col_idx] = 'color: white'
                            
                            return styles
                        
                        styled_df = txn_df.style.apply(highlight_state, axis=1)
                        
                        # Add click handling for View Counters
                        st.dataframe(
                            styled_df, 
                            use_container_width=True, 
                            hide_index=True,
                            on_select="rerun",
                            selection_mode="single-row",
                            key="counter_txn_table",
                            column_config=txn_column_config
                        )
                        
                        # Get selected row
                        if st.session_state.get("counter_txn_table") and st.session_state["counter_txn_table"].get("selection"):
                            selected_rows = st.session_state["counter_txn_table"]["selection"].get("rows", [])
                            
                            if selected_rows:
                                selected_idx = selected_rows[0]
                                selected_row = txn_df.iloc[selected_idx]
                                
                                if selected_row['Counter Summary'] == 'View Counters':
                                    st.markdown("---")
                                    st.markdown(f"####  Counters for Transaction: {selected_row['Transaction ID']}")
                                    st.caption(f"Time: {selected_row['Date Timestamp']}")
                                    

                                    
                                    # Extract time from date_timestamp (format: "DD Month YYYY HH:MM:SS")
                                    date_timestamp = selected_row['Date Timestamp']
                                    try:
                                        # Parse the full datetime
                                        txn_datetime = datetime.strptime(date_timestamp, '%d %B %Y %H:%M:%S')
                                        txn_time = txn_datetime.time()
                                        
                                        # Filter blocks that match this transaction time (within reasonable range)
                                        # Allow ±30 seconds margin
                                        margin_seconds = 30
                                        
                                        logical_counters = []
                                        
                                        if 'all_blocks' in counter_data:
                                            for block in counter_data['all_blocks']:
                                                block_time_str = block.get('time')
                                                
                                                if block_time_str:
                                                    # Parse block time string to time object
                                                    # block_time comes from API as string (e.g., "11:38:38")
                                                    try:
                                                        block_time = datetime.strptime(str(block_time_str), '%H:%M:%S').time()
                                                    except ValueError:
                                                        # Try with milliseconds format
                                                        try:
                                                            block_time = datetime.strptime(str(block_time_str), '%H:%M:%S.%f').time()
                                                        except ValueError:
                                                            continue  # Skip if time format is invalid
                                                    
                                                    # Convert both to datetime for comparison
                                                    block_datetime = datetime.combine(datetime.today(), block_time)
                                                    txn_datetime_today = datetime.combine(datetime.today(), txn_time)
                                                    
                                                    # Calculate time difference in seconds
                                                    time_diff = abs((block_datetime - txn_datetime_today).total_seconds())
                                                    
                                                    # Only include blocks within margin
                                                    if time_diff <= margin_seconds:
                                                        for counter in block.get('data', []):
                                                            if counter.get('Record_Type') == 'Logical':
                                                                
                                                                logical_counters.append({
                                                                    'Name (PName)': counter.get('UnitName', ''),
                                                                    'Value (Val)': counter.get('Val', ''),
                                                                    'Cur': counter.get('Cur', ''),
                                                                    'Ini': counter.get('Ini', ''),
                                                                    'Retr': counter.get('Retr', ''),
                                                                    'Disp': counter.get('Disp', ''),
                                                                    'RCNT (Reject Count)': counter.get('RCnt', ''),
                                                                    'Pres': counter.get('Pres', ''),
                                                                    'Cnt': counter.get('Cnt', ''),
                                                                    'Status (St)': counter.get('St', ''),
                                                                    'NrPCU': counter.get('No', '')
                                                                })
                                    
                                    except ValueError as e:
                                        st.error(f"Error parsing transaction time: {e}")
                                        logical_counters = []
                                    
                                    if logical_counters:
                                        counter_display_df = pd.DataFrame(logical_counters)
                                        
                                        # Remove duplicates based on all columns
                                        counter_display_df = counter_display_df.drop_duplicates()
                                        
                                        # Get column descriptions
                                        col_descriptions = counter_data.get('column_descriptions', {})
                                        
                                        # Column config with descriptions
                                        detail_column_config = {}
                                        for col in counter_display_df.columns:
                                            # Map display column names to description keys
                                            col_key_map = {
                                                'Name (PName)': 'UnitName',
                                                'Value (Val)': 'Val',
                                                'Cur': 'Cur',
                                                'Ini': 'Ini',
                                                'Retr': 'Retr',
                                                'Disp': 'Disp',
                                                'RCNT (Reject Count)': 'RCnt',
                                                'Pres': 'Pres',
                                                'Cnt': 'Cnt',
                                                'Status (St)': 'St',
                                                'NrPCU': 'HWsens'
                                            }
                                            
                                            desc_key = col_key_map.get(col, col)
                                            if desc_key in col_descriptions:
                                                detail_column_config[col] = st.column_config.TextColumn(
                                                    col,
                                                    help=col_descriptions[desc_key],
                                                    width="small"
                                                )
                                        
                                        st.dataframe(
                                            counter_display_df, 
                                            use_container_width=True, 
                                            hide_index=True,
                                            column_config=detail_column_config
                                        )
                                        
                                        st.caption(f"Showing {len(counter_display_df)} unique counter record(s)")
                                        
                                        if st.button("✕ Close Counters View", key="close_counters"):
                                            st.session_state["counter_txn_table"]["selection"]["rows"] = []
                                            st.rerun()
                                    else:
                                        st.info("No logical counters found for this transaction timeframe")
                    else:
                        st.info("No transaction data available")
                    
                    st.markdown("---")
                    
                    # Display last counter
                    last_date = counter_data['last_counter']['date']
                    last_time = counter_data['last_counter']['timestamp']
                    
                    # Format date as "DD Month YYYY"
                    try:
                        if len(last_date) == 6:  # YYMMDD format
                            dt = datetime.strptime(last_date, '%y%m%d')
                            formatted_last_date = dt.strftime('%d %B %Y')
                        else:
                            formatted_last_date = last_date
                    except:
                        formatted_last_date = last_date
                    
                    st.markdown(f"####   Last Counter - {formatted_last_date} {last_time}")
                    st.caption("This counter represents the last transaction in the source file")

                    last_df = pd.DataFrame(counter_data['last_counter']['counter_data'])

                    # Get column descriptions
                    col_descriptions = counter_data.get('column_descriptions', {})

                    # Create column config with tooltips
                    column_config = {}
                    for col in last_df.columns:
                        if col in col_descriptions:
                            column_config[col] = st.column_config.TextColumn(
                                col,
                                help=col_descriptions[col],
                                width="small"
                            )

                    st.dataframe(
                        last_df, 
                        use_container_width=True, 
                        hide_index=True,
                        column_config=column_config
                    )
                
                else:
                    error_detail = response.json().get('detail', 'Failed to get counter data')
                    st.error(f"  {error_detail}")
                    
            except requests.exceptions.Timeout:
                st.error("  Request timeout. Please try again.")
            except requests.exceptions.ConnectionError:
                st.error("  Connection error. Ensure the API server is running.")
            except Exception as e:
                st.error(f"  Error: {str(e)}")
                import traceback
                with st.expander("  Debug Information"):
                    st.code(traceback.format_exc())
    
    except Exception as e:
        st.error(f"  Error: {str(e)}")
        import traceback
        with st.expander("  Debug Information"):
            st.code(traceback.format_exc())

def render_acu_single_parse(): # MODIFIED
    """
    FUNCTION:
        render_acu_single_parse

    DESCRIPTION:
        Renders the Streamlit UI for loading, selecting, parsing, searching,
        and exporting ACU (ATM Configuration Utility) XML configuration files
        that were extracted from the main processed ZIP package.

    USAGE:
        render_acu_single_parse()

    PARAMETERS:
        None : This function does not accept any input parameters.
        
        (Uses Streamlit session state internally)
        - st.session_state.acu_extracted_files : List of ACU XML filenames
        - st.session_state.acu_parsed_df       : Parsed ACU parameter dataframe
        - st.session_state.acu_files_loaded    : Boolean flag to prevent reloading

    RETURNS:
        None : The function renders UI components directly to Streamlit 
               and does not return a value.

    RAISES:
        RuntimeError : If backend API calls fail unexpectedly.
        Exception    : Any unexpected errors during parsing or UI rendering 
                       are caught and shown via Streamlit error boxes.
    """
    #st.write("SESSION STATE DEBUG:", st.session_state)   # debug Added

    st.markdown("###   ACU Configuration Parser")
    
    
    # Initialize session state
    if 'acu_extracted_files' not in st.session_state:
        st.session_state.acu_extracted_files = None
    if 'acu_parsed_df' not in st.session_state:
        st.session_state.acu_parsed_df = None

    # This function now ONLY loads from the main processed package.
    # The separate uploader has been removed.
    if 'acu_files_loaded' not in st.session_state:
        st.session_state.acu_files_loaded = False

    if not st.session_state.acu_files_loaded:
        with st.spinner("Loading ACU files from processed package..."):
            try:
                resp = requests.get(f"{API_BASE_URL}/get-acu-files", timeout=30)
                #st.write("DEBUG BACKEND STATUS:", resp.status_code)      #add debug point 
                #st.write("DEBUG BACKEND RESPONSE:", resp.text)           #
                if resp.status_code == 200:
                    data = resp.json()
                    xml_files = data.get('acu_files', {})

                    # if no ACU files found → STOP UI here
                    if not xml_files:
                        st.error("  No ACU files found in the uploaded ZIP.")
                        st.stop()  # <<< IMPORTANT: stops rendering the rest of the UI

                        # ACU files exist → load normally
                    st.session_state.acu_extracted_files = xml_files
                    st.session_state.acu_files_loaded = True
                    st.success(f"  Loaded {len(xml_files)} ACU XML files from processed package")
                    st.rerun()

                else:
                        st.warning("No ACU files found in the processed package.")
            except Exception as e:
                st.error(f"Could not load ACU files from package: {e}")
    
    # File selection and parsing
    if st.session_state.get('acu_extracted_files'):
        st.info("Extract, parse, and analyze ACU configuration files with XSD documentation support.")
        xml_files = st.session_state.acu_extracted_files
        
        if xml_files:
            st.markdown("---")
            st.markdown("#### Select and Parse File")
            
            selected_file = st.selectbox(
                "Choose a file to parse:",
                options=xml_files,
                key="acu_file_select"
            )
            
            if st.button("  Parse Selected File", key="acu_parse_btn", type="primary"):
                with st.spinner(f"Parsing {selected_file}..."):
                    try:
                        # Content is always in the main session now
                        parse_request = [{"filename": selected_file}]
                        
                        response = requests.post(
                            f"{API_BASE_URL}/parse-acu-files",
                            json=parse_request,
                            timeout=120
                        )
                        
                        if response.status_code == 200:
                            result = response.json()
                            records = result.get('data', [])
                            
                            if records:
                                df = pd.DataFrame(records)
                                st.session_state.acu_parsed_df = df
                                
                                docs_count = sum(1 for r in records if r.get('Details'))
                                st.success(f"  Parsed {len(df)} parameters ({docs_count} with documentation)")
                            else:
                                st.warning("No parameters extracted from file")
                        else:
                            st.error(f"Parsing failed: {response.json().get('detail', 'Unknown error')}")
                    
                    except Exception as e:
                        st.error(f"Error: {str(e)}")
                        import traceback
                        with st.expander("  Debug Info"):
                            st.code(traceback.format_exc())
        else:
            st.warning("No XML files found")
    
    # Display results
    if st.session_state.get('acu_parsed_df') is not None and not st.session_state.acu_parsed_df.empty:
        df = st.session_state.acu_parsed_df
        
        st.markdown("---")
        st.markdown("#### View Parsed Data")
        
        # Search
        search_term = st.text_input(
            "  Search parameters:",
            placeholder="e.g., capacity, DISABLED...",
            key="acu_search"
        )
        
        display_df = df.copy()
        if search_term:
            mask = (
                display_df['Parameter'].str.contains(search_term, case=False, na=False) |
                display_df['Value'].str.contains(search_term, case=False, na=False)
            )
            display_df = display_df[mask]
        
        # Display table
        st.dataframe(
            display_df[['Parameter', 'Value']],
            use_container_width=True,
            hide_index=True,
            key="acu_data_table",
            on_select="rerun",
            selection_mode="single-row"
        )
        
        st.caption(f"Showing {len(display_df)} of {len(df)} parameters")
        
        # Show documentation for selected row
        selection = st.session_state.get("acu_data_table", {}).get("selection", {})
        if selection.get("rows"):
            selected_row_index = selection["rows"][0]
            selected_param = display_df.iloc[selected_row_index]
            
            if selected_param.get('Details'):
                st.markdown("---")
                st.markdown(f"####   Documentation: `{selected_param['Parameter']}`")
                with st.container(border=True):
                    st.markdown(selected_param['Details'])
            else:
                st.info("  Click a row to see documentation (if available)")
        else:
            st.info("  Click a row to see documentation (if available)")
        
        # Download
        st.markdown("---")
        csv = display_df.to_csv(index=False)
        st.download_button(
            label="  Download as CSV",
            data=csv,
            file_name=f"acu_config_export.csv",
            mime="text/csv"
        )


def render_acu_compare(): # MODIFIED
    """
FUNCTION: render_acu_compare

DESCRIPTION:
    Renders a Streamlit interface to compare ACU configuration XML files from two ZIP archives.
    Source A is automatically loaded from the main processed package, and Source B can be uploaded
    by the user. Users can select matching files from each source to view a side-by-side comparison.

USAGE:
    render_acu_compare()

PARAMETERS:
    None : Uses Streamlit widgets and session state for interaction.

RETURNS:
    None : The function renders UI elements directly and does not return a value.

RAISES:
    requests.exceptions.Timeout        : If API requests exceed the specified timeout.
    requests.exceptions.ConnectionError: If the API server is unreachable.
    Exception                         : For general errors during file loading, extraction, or comparison.
"""
    st.markdown("###   ACU Configuration Comparison")
    st.info("Compare ACU configuration files from two different ZIP archives.")
    
    # Initialize session state
    if 'acu_compare_data' not in st.session_state:
        st.session_state.acu_compare_data = {}
    
    comp_data = st.session_state.acu_compare_data
    
    # Source A is now automatically loaded from the main processed ZIP
    st.markdown("####   Source A (Main Package)")
    if not comp_data.get('files1'):
        with st.spinner("Loading ACU files from main package for Source A..."):
            try:
                
                resp = requests.get(f"{API_BASE_URL}/get-acu-files", timeout=30)
                
                
                if resp.status_code == 200:
                    data = resp.json()
                    all_files = data.get('acu_files', {})

                    #st.write("  DEBUG: Raw ACU files from Source A:", all_files)   #------added
                    if all_files:
                        comp_data['zip1_name'] = "Main Package"
                        comp_data['files1'] = {k: v for k, v in all_files.items() if not k.startswith('__xsd__')}
                        #st.write("  DEBUG: Filtered XML files for Source A:", comp_data['files1'])  # DEBUG ADDED
                        comp_data['files1_all'] = all_files
                        st.success(f"  **Source A:** Main Package loaded ({len(comp_data['files1'])} XML files)")
                        st.rerun()
                    else:
                        comp_data['files1'] = None
                        comp_data['no_files_found'] = True
                        st.warning("No ACU files found in the main package to use as Source A.")
                        st.stop()
                else:
                    st.error("Could not load ACU files from main package.")
            except Exception as e:
                st.error(f"Error loading Source A: {e}")
    else:
        st.success(f"✓ **Source A:** Main Package loaded ({len(comp_data['files1'])} XML files)")
    
    st.markdown("---")
    
    # Source B
    # Source B
    st.markdown("####   Source B")
    
    if comp_data.get('files2'):
        st.success(f"  **Source B:** {comp_data.get('zip2_name')} ({len(comp_data['files2'])} XML files)")
        if st.button("Replace Source B", key="acu_replace_b"):
            comp_data['zip2_name'] = None
            comp_data['files2'] = None
            comp_data['files2_all'] = None
            st.rerun()
    else:
        zip2 = st.file_uploader("Upload second ZIP", type="zip", key="acu_zip2")
        if zip2 and st.button("Process Source B", key="acu_process_b", type="primary"):
            with st.spinner("Extracting from Source B..."):
                try:
                    
                    files_payload = {'file': (zip2.name, zip2.getvalue(), 'application/zip')}
                    response = requests.post(
                        f"{API_BASE_URL}/extract-files/",
                        files=files_payload,
                        timeout=120
                    )
                    
                    
                    if response.status_code == 200:
                        result = response.json()
                        all_files = result.get('files', {})
                        
                        
                        if not all_files:
                            st.error("  No ACU files found in the uploaded ZIP.")
                        else:
                            comp_data['zip2_name'] = zip2.name
                            comp_data['files2'] = {k: v for k, v in all_files.items() if not k.startswith('__xsd__')}
                            comp_data['files2_all'] = all_files
            
                            st.success(f"  Source B: {len(comp_data['files2'])} XML files")
                            st.rerun()
                    else:
                        error_detail = response.json().get('detail', 'Unknown error')
                        st.error(f"  Error: {error_detail}")
                        with st.expander("  Debug Info"):
                            st.code(f"Status: {response.status_code}")
                            try:
                                st.json(response.json())
                            except:
                                st.text(response.text)
                                
                except requests.exceptions.Timeout:
                    st.error("⏱  Request timeout.")
                except requests.exceptions.ConnectionError:
                    st.error("  Connection error. Check if API server is running.")
                except Exception as e:
                    st.error(f"  Error: {str(e)}")
                    import traceback
                    with st.expander("  Debug Info"):
                        st.code(traceback.format_exc())
    
    # Comparison
    if comp_data.get('files1') and comp_data.get('files2'):
        st.markdown("---")
        st.markdown("####   Select Files to Compare")
        
        files1_list = sorted(comp_data['files1'].keys())
        files2_list = sorted(comp_data['files2'].keys())
        
        # Find common files
        common_files = set(os.path.basename(f) for f in files1_list) & set(os.path.basename(f) for f in files2_list)
        
        if not common_files:
            st.warning("  No files with matching names found")
            
            col1, col2 = st.columns(2)
            with col1:
                st.markdown("**Files in Source A:**")
                for f in files1_list:
                    st.caption(f"• {os.path.basename(f)}")
            with col2:
                st.markdown("**Files in Source B:**")
                for f in files2_list:
                    st.caption(f"• {os.path.basename(f)}")
        else:
            st.success(f"Found {len(common_files)} matching file(s)")
            
            # Select file
            selected_basename = st.selectbox(
                "Select file to compare:",
                options=sorted(common_files),
                key="acu_comp_file_select"
            )
            
            if selected_basename and st.button("  Compare Files", key="acu_do_compare", type="primary"):
                # Find full paths
                file1 = next(f for f in files1_list if os.path.basename(f) == selected_basename)
                file2 = next(f for f in files2_list if os.path.basename(f) == selected_basename)
                
                
                with st.spinner("Comparing files..."):
                    try:
                        content1 = comp_data['files1_all'][file1]
                        content2 = comp_data['files2_all'][file2]
                        
                        st.markdown("---")
                        st.markdown("####   File Comparison")
                        
                        col1, col2 = st.columns(2)
                        
                        with col1:
                            st.markdown(f"**{comp_data['zip1_name']}**")
                            st.code(content1, language='xml', line_numbers=True)
                        
                        with col2:
                            st.markdown(f"**{comp_data['zip2_name']}**")
                            st.code(content2, language='xml', line_numbers=True)
                    
                    except Exception as e:
                        st.error(f"Error: {str(e)}")

# ============================================
# MAIN APPLICATION UI
# ============================================

def show_main_app():
    """
    Display main application UI (shown after login)
    """
    # Get current user
    user = get_current_user()
    
    # Header with welcome message and logout button
    col1, col2 = st.columns([5, 1])
    
    with col1:
        display_name = user.get('name') or user.get('username', 'User')
        st.markdown(
            f"""
            <div style="
                font-size: 24px;
                font-weight: 700;
            ">
                Welcome, {display_name}
            </div>
            """,
            unsafe_allow_html=True
        )

    with col2:
        if st.button(" Logout", use_container_width=True, key="logout_btn"):
            logout_user()
            st.rerun()
    
    st.markdown("---")
    
    # Initialize app session state
    if 'zip_processed' not in st.session_state:
        st.session_state.zip_processed = False
    if 'processing_result' not in st.session_state:
        st.session_state.processing_result = None
    if 'selected_function' not in st.session_state:
        st.session_state.selected_function = None

    # ============================================
    # MAIN APPLICATION
    # ============================================

    st.title("DN Diagnostics Platform")
    st.caption("Comprehensive analysis tool for Diebold Nixdorf diagnostic files.")

    st.markdown("## Upload Zip Package")

    uploaded_file = st.file_uploader(
        "Select ZIP Archive",
        type=['zip'],
        help="Upload a ZIP file containing diagnostic files (max 500 MB)",
        key="zip_uploader"
    )
    # Check if file was deleted (uploader is now empty but we had processed a file before)
    if uploaded_file is None and st.session_state.zip_processed:
        st.session_state.zip_processed = False
        st.session_state.processing_result = None
        st.session_state.last_processed_file = None
        st.session_state.selected_function = None
        # Clear ACU-related session states
        if 'acu_extracted_files' in st.session_state:
            del st.session_state.acu_extracted_files
        if 'acu_parsed_df' in st.session_state:
            del st.session_state.acu_parsed_df
        if 'acu_files_loaded' in st.session_state:
            del st.session_state.acu_files_loaded
        # Clear cache
        clear_cache()
        st.info("  File removed. Please upload a new ZIP file to continue.")
        st.rerun()
        
    # Only process if file exists AND it's different from the last processed file
    if uploaded_file is not None:
        # Check if this is a new file or the same file we just processed
        current_file_id = f"{uploaded_file.name}_{uploaded_file.size}"
        
        if 'last_processed_file' not in st.session_state:
            st.session_state.last_processed_file = None
        
        # Only process if it's a new file
        if st.session_state.last_processed_file != current_file_id:
            file_size_mb = len(uploaded_file.getvalue()) / (1024 * 1024)
            st.info(f"File: {uploaded_file.name} ({file_size_mb:.2f} MB)")
            
            with st.spinner("Processing package..."):
                try:
                    files = {"file": (uploaded_file.name, uploaded_file.getvalue(), "application/zip")}
                    
                    response = requests.post(
                        f"{API_BASE_URL}/process-zip", 
                        files=files,
                        timeout=300  # Increased to 5 minutes for larger files
                    )
                    
                    if response.status_code == 200:
                        result = response.json()
                        st.session_state.zip_processed = True
                        st.session_state.processing_result = result
                        st.session_state.last_processed_file = current_file_id
                        
                        # Clear cache when new ZIP is uploaded
                        clear_cache()
                        
                        st.success("Package processed successfully.")
                        st.rerun()
                    else:
                        error_detail = response.json().get('detail', 'Unknown error occurred.')
                        st.error(f"Error: {error_detail}")
                        
                except requests.exceptions.Timeout:
                    st.error("Request timeout. The file may be too large or the server is not responding.")
                except requests.exceptions.ConnectionError:
                    st.error("Connection error. Please ensure the FastAPI server is running on localhost:8000.")
                except Exception as e:
                    st.error(f"Error: {str(e)}")
        else:
            # File already processed, show info
            file_size_mb = len(uploaded_file.getvalue()) / (1024 * 1024)

    if st.session_state.zip_processed:
        st.markdown("---")
        result = st.session_state.processing_result
        categories = result['categories']
        
        st.markdown("## Detected Files")
        cols = st.columns(6)
        
        category_display = {
            'customer_journals': ('Customer Journals', '📋'),
            'ui_journals': ('UI Journals', '🖥️'),
            'trc_trace': ('TRC Trace', '📝'),
            'trc_error': ('TRC Error', '⚠️'),
            'registry_files': ('Registry Files', '📄'),
            'acu_files': ('ACU XML Files', '⚡')
        }
        
        for idx, (category, (label, icon)) in enumerate(category_display.items()):
            count = categories.get(category, {}).get('count', 0)
            with cols[idx]:
                st.metric(label, count)
        
        st.markdown("---")
        
        st.markdown("## Analysis Functions")

        functionalities = {
            "transaction_stats": {
                "name": " Transaction Type Statistics",
                "description": "View statistics for different transaction types",
                "status": "ready",
                "requires": ["customer_journals"]
            },
            "individual_transaction": {
                "name": " Individual Transaction Analysis",
                "description": "Analyze a specific transaction in detail",
                "status": "ready",
                "requires": ["customer_journals"]
            },
            "ui_flow_individual": {
                "name": " UI Flow of Individual Transaction",
                "description": "Visualize UI flow for a specific transaction",
                "status": "ready",
                "requires": ["customer_journals", "ui_journals"]
            },
            "consolidated_flow": {
                "name": " Consolidated Transaction UI Flow and Analysis",
                "description": "View consolidated flow across multiple transactions",
                "status": "ready",
                "requires": ["customer_journals", "ui_journals"]
            },
            "transaction_comparison": {
                "name": " Transaction Comparison Analysis",
                "description": "Compare two transactions side by side",
                "status": "ready",
                "requires": ["customer_journals", "ui_journals"]
            },
            "registry_single": {
                "name": " Single View of Registry Files",
                "description": "View and analyze a single registry file",
                "status": "ready",
                "requires": ["registry_files"]
            },
            "registry_compare": {
                "name": " Compare Two Registry Files",
                "description": "Compare differences between two registry files",
                "status": "ready",
                "requires": ["registry_files"]
            },
            "counters_analysis": {
                "name": " Counters Analysis",
                "description": "Analyze counter data from TRC Trace files mapped to transactions",
                "status": "ready",
                "requires": ["customer_journals", "trc_trace"]
            },
            "acu_single_parse": {
                "name": " ACU Parser - Single Archive",
                "description": "Extract and parse ACU configuration files from a single ZIP",
                "status": "ready",
                "requires": ["acu_files"]  #fixed
            },
            "acu_compare": {
                "name": " ACU Parser - Compare Archives", 
                "description": "Compare ACU configuration files from two ZIP archives",
                "status": "ready",
                "requires": ["acu_files"]  #fixed
            }
        }

        available_file_types = [cat for cat, data in categories.items() if data.get('count', 0) > 0]

        # Build dropdown options in the order defined in functionalities
        dropdown_options = ["Select a function"]

        for func_id, func_data in functionalities.items():
            requirements_met = all(req in available_file_types for req in func_data['requires'])
            
            if requirements_met:
                # Add the function name as-is
                dropdown_options.append(func_data['name'])
            else:
                # Add with missing requirements indicator
                missing = [req for req in func_data['requires'] if req not in available_file_types]
                req_labels = {
                    'customer_journals': 'Customer Journals',
                    'ui_journals': 'UI Journals',
                    'trc_trace': 'TRC Trace',
                    'trc_error': 'TRC Error',
                    'registry_files': 'Registry Files',
                    'acu_files': 'ACU XML Files',
                }
                missing_str = ", ".join([req_labels.get(m, m) for m in missing])
                dropdown_options.append(f"{func_data['name']} (Missing: {missing_str})")

        selected_option = st.selectbox(
            "Select Analysis Function",
            options=dropdown_options,
            key="function_selector"
        )

        selected_func_id = None
        selected_func_data = None

        if selected_option != "Select a function":
            clean_option = selected_option.split(" (Missing:")[0]
            
            for func_id, func_data in functionalities.items():
                if func_data['name'] == clean_option:
                    selected_func_id = func_id
                    selected_func_data = func_data
                    break

        if selected_func_data:
            st.markdown("---")
            
            requirements_met = all(req in available_file_types for req in selected_func_data['requires'])
            
            if not requirements_met:
                missing = [req for req in selected_func_data['requires'] if req not in available_file_types]
                missing_str = ", ".join([req_labels.get(m, m) for m in missing])
                st.error(f"Cannot proceed. Missing required files: {missing_str}")
                st.info("Please upload a package containing the required file types.")
            
            elif selected_func_data['status'] == 'construction':
                render_under_construction(selected_func_data['name'])
            
            else:
                # Ready functionalities - show their content
                if selected_func_id == "transaction_stats":
                    render_transaction_stats()           
                elif selected_func_id == "individual_transaction":
                    render_individual_transaction_analysis()
                elif selected_func_id == "registry_single":
                    render_registry_single()
                elif selected_func_id == "registry_compare":
                    render_registry_compare()
                elif selected_func_id == "transaction_comparison":
                    render_transaction_comparison()
                elif selected_func_id == "ui_flow_individual":
                    render_ui_flow_individual()
                elif selected_func_id == "consolidated_flow":
                    render_consolidated_flow()
                elif selected_func_id == "counters_analysis":
                    render_counters_analysis()
                elif selected_func_id == "acu_single_parse":
                    render_acu_single_parse()
                elif selected_func_id == "acu_compare":
                    render_acu_compare()

    st.markdown("---")
    st.markdown("""
        <div style='text-align: center; color: #666666; font-size: 0.875rem;'>
            © 2025-26 Diebold Nixdorf Analysis Tools
        </div>
    """, unsafe_allow_html=True)

# ============================================
# APP ENTRY POINT
# ============================================

def main():
    """
    Main entry point - decides which page to show
    """
    # Initialize session
    initialize_session()
    initialize_admin_table()
    create_login_history_table()
    if not is_logged_in():
        if st.session_state.page == "login":
            show_login_page()
        elif st.session_state.page == "register":
            show_register_page()
    else:
        # LOGGED IN → MAIN APP
        show_main_app()

# ============================================
# RUN THE APP
# ============================================

if __name__ == "__main__":
    main()
