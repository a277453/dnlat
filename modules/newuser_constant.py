from enum import Enum


class NewUserUI(Enum):
    # -------- TITLES --------
    LOGIN_TITLE = "🔒 DN Diagnostics Login"
    REGISTER_TITLE = " DN Diagnostics Register"

    # -------- LOGIN --------
    LOGIN_USERNAME_LABEL = "Username"
    LOGIN_USERNAME_PLACEHOLDER = "Enter your username"
    LOGIN_PASSWORD_LABEL = "Password"
    LOGIN_PASSWORD_PLACEHOLDER = "Enter your password"
    LOGIN_BUTTON = "Login"
    LOGIN_SUCCESS = "Welcome {username}!"
    LOGIN_EMPTY_ERROR = "Please enter username and password"
    LOGIN_INVALID_ERROR = "Invalid username or password"
    LOGIN_PENDING_WARNING = (
        "{username} is pending admin approval.\n\n"
        "Please contact the administrator to activate your account."
    )

    # -------- REGISTER --------
    REGISTER_BUTTON = "Register New User"
    REGISTER_EMAIL_LABEL = "Email ID"
    REGISTER_EMAIL_PLACEHOLDER = "Enter DN Official Email"
    REGISTER_NAME_LABEL = "Name"
    REGISTER_NAME_PLACEHOLDER = "Enter your name"
    REGISTER_PASSWORD_LABEL = "Password"
    REGISTER_PASSWORD_PLACEHOLDER = "Enter your password"
    REGISTER_CONFIRM_PASSWORD_LABEL = "Confirm Password"
    REGISTER_CONFIRM_PASSWORD_PLACEHOLDER = "Re-enter your password"
    REGISTER_EMP_CODE_LABEL = "Employee Code"
    REGISTER_EMP_CODE_PLACEHOLDER = "Enter 8 digit employee code"
    REGISTER_ROLE_LABEL = "Role Type"
    REGISTER_DEFAULT_ROLE = "USER"
    REGISTER_SUBMIT_BUTTON = "Register"
    BACK_TO_LOGIN_BUTTON = "  Back to Login"

    # -------- VALIDATION MESSAGES --------
    ALL_FIELDS_REQUIRED = "All fields are required"
    INVALID_EMAIL = "Please use your official Diebold Nixdorf email ID"
    INVALID_NAME = "Name must contain only letters and spaces"
    PASSWORD_RULES_ERROR = (
        "Password must be at least 8 characters long and include "
        "uppercase, lowercase, Min 2 digits, and special character."
    )
    PASSWORD_MISMATCH = "Passwords do not match with confirm password"
    INVALID_EMP_CODE = "Please enter a valid 8-digit employee code"

    # -------- COMMON / UI --------
    AUTHENTICATING_SPINNER = "Authenticating..."
    HTML_BREAK = "<br>"

    # -------- REGEX PATTERNS --------
    EMAIL_PATTERN = r"^[a-zA-Z]+\.[a-zA-Z]+@dieboldnixdorf\.com$"
    NAME_PATTERN = r"^[A-Za-z ]+$"

    # -------- FORMS --------
    LOGIN_FORM_KEY = "login_form"
    REGISTER_FORM_KEY = "newUser_Form"

    # -------- SESSION / PAGE --------
    SESSION_LOGIN_SUCCESS = "login_success"
    SESSION_USERNAME = "username"
    SESSION_PAGE = "page"
    PAGE_LOGIN = "login"
    PAGE_REGISTER = "register"

    # -------- ERROR MESSAGES --------
    SERVICE_UNAVAILABLE = "Service temporarily unavailable. Please try again later."
    REGISTRATION_INTERNAL_ERROR = "Registration failed due to an internal error."

    LOGIN_USERNAME_KEY = "login_username"
    LOGIN_PASSWORD_KEY = "login_password"
    REGISTER_EMAIL_KEY = "register_email"
    REGISTER_NAME_KEY = "register_name"
    REGISTER_PASSWORD_KEY = "register_password"
    REGISTER_CONFIRM_PASSWORD_KEY = "register_confirm_password"
    REGISTER_EMP_CODE_KEY = "register_emp_code"
    REGISTER_ROLE_KEY = "register_role"

