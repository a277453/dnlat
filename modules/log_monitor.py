"""
log_monitor.py
==============
Continuously tails app.log, prints every line to the terminal, and extracts
only *major* infrastructure/security errors into a separate file for analysis.

What counts as a MAJOR error (captured):
  - DB connection failures / bootstrap failures
  - Authentication & RBAC failures (401, 403, JWT errors)
  - Session ID degraded to 'undefined' (employee_code unresolvable from DB)
  - Schema errors  (column renamed / deleted / missing)
  - Ollama / LLM service unavailable
  - Analysis pipeline crash  (top-level unexpected failure)
  - Unhandled exceptions  (logger.exception calls with tracebacks)
  - HTTP 500-level errors

What is intentionally EXCLUDED (noise):
  - Failed to parse / load a specific file
  - BadZipFile on individual uploads
  - No session / transaction found for a session_id
  - Missing UI journal or no screens found
  - Registry / customer / ACU file load failures
  - Empty data warnings
  - Routine WARNING level lines (RBAC 403 role mismatch is kept as it is
    a security signal, everything else at WARNING is dropped)

Usage:
    python log_monitor.py                        # defaults
    python log_monitor.py --log app.log --out critical_errors.log
    python log_monitor.py --log /var/log/app.log --out /tmp/errors.log --poll 0.5
"""

import re
import time
import argparse
import sys
from datetime import datetime
from pathlib import Path

# ---------------------------------------------------------------------------
# Patterns that QUALIFY a line as a major error worth capturing
# ---------------------------------------------------------------------------
MAJOR_PATTERNS = [
    # ── DB connectivity & schema ──────────────────────────────────────────
    re.compile(r"DB connection returned None", re.IGNORECASE),
    re.compile(r"Database connection failed", re.IGNORECASE),
    re.compile(r"DB connection failed", re.IGNORECASE),
    re.compile(r"employee_code could not be retrieved from DB", re.IGNORECASE),
    re.compile(r"employee_code column may be renamed or deleted", re.IGNORECASE),
    re.compile(r"could not import get_db_connection", re.IGNORECASE),
    re.compile(r"DB bootstrap failed", re.IGNORECASE),
    re.compile(r"DB query failed", re.IGNORECASE),
    re.compile(r"Failed to create login_history table due to DB connection failure", re.IGNORECASE),
    re.compile(r"create_reset_tokens_table: DB connection failed", re.IGNORECASE),
    re.compile(r"generate_reset_token: DB connection failed", re.IGNORECASE),
    re.compile(r"reset_user_password: DB connection failed", re.IGNORECASE),
    re.compile(r"verify_reset_identity: DB connection failed", re.IGNORECASE),
    re.compile(r"Login event not logged.*DB connection failed", re.IGNORECASE),
    re.compile(r"database.*error|error.*database", re.IGNORECASE),
    re.compile(r"connection.*refused|refused.*connection", re.IGNORECASE),
    re.compile(r"OperationalError|ProgrammingError|InterfaceError", re.IGNORECASE),

    # ── Auth / RBAC / JWT ─────────────────────────────────────────────────
    re.compile(r"RBAC \[40[13]\]", re.IGNORECASE),
    re.compile(r"MIDDLEWARE \[401\]", re.IGNORECASE),
    re.compile(r"FEEDBACK \[401\]", re.IGNORECASE),
    re.compile(r"Authentication token missing", re.IGNORECASE),
    re.compile(r"JWT decode failed", re.IGNORECASE),
    re.compile(r"could not decode JWT", re.IGNORECASE),
    re.compile(r"invalid.*token|token.*invalid|token.*expired", re.IGNORECASE),
    re.compile(r"invalid/expired token", re.IGNORECASE),
    re.compile(r"unauthorized|unauthenticated", re.IGNORECASE),
    re.compile(r"Login.*fail|fail.*login", re.IGNORECASE),
    re.compile(r"Login verification failed", re.IGNORECASE),
    re.compile(r"authenticate_user_backend.*failed", re.IGNORECASE),

    # ── Password reset / identity security ───────────────────────────────
    re.compile(r"Reset identity verification failed", re.IGNORECASE),
    re.compile(r"validate_reset_token:.*invalid.*expired.*used", re.IGNORECASE),
    re.compile(r"reset_password_endpoint: reset failed", re.IGNORECASE),
    re.compile(r"User registration failed", re.IGNORECASE),

    # ── Session degradation ───────────────────────────────────────────────
    re.compile(r"session ID will use.*undefined.*suffix", re.IGNORECASE),
    re.compile(r"_build_session_id.*degraded", re.IGNORECASE),

    # ── LLM / Ollama ──────────────────────────────────────────────────────
    re.compile(r"Ollama is not installed", re.IGNORECASE),
    re.compile(r"LLM.*fail|fail.*LLM", re.IGNORECASE),
    re.compile(r"LLM call failed", re.IGNORECASE),
    re.compile(r"chat_transaction.*LLM call failed", re.IGNORECASE),

    # ── Top-level pipeline crashes ────────────────────────────────────────
    re.compile(r"Analysis failed for transaction", re.IGNORECASE),
    re.compile(r"Visualization failed", re.IGNORECASE),
    re.compile(r"Unexpected failure", re.IGNORECASE),
    re.compile(r"Unexpected error in /", re.IGNORECASE),
    re.compile(r"chat_transaction.*no session found", re.IGNORECASE),
    re.compile(r"chat_transaction_stream.*no session found", re.IGNORECASE),

    # ── Unhandled exceptions & tracebacks ────────────────────────────────
    re.compile(r"Traceback \(most recent call last\)", re.IGNORECASE),
    re.compile(r"Exception|Critical", re.IGNORECASE),

    # ── HTTP 500 ──────────────────────────────────────────────────────────
    re.compile(r"\b500\b"),
]

# ---------------------------------------------------------------------------
# Patterns that DISQUALIFY a line even if a major pattern matched
# (file-level / per-request noise we explicitly want to suppress)
# ---------------------------------------------------------------------------
EXCLUDE_PATTERNS = [
    re.compile(r"failed to (load|parse|read) .*(\.xml|\.xsd|\.zip|\.json|\.csv|\.log|file)", re.IGNORECASE),
    re.compile(r"BadZipFile", re.IGNORECASE),
    re.compile(r"\[REGISTRY\].*failed to load", re.IGNORECASE),
    re.compile(r"\[CUSTOMER\].*failed to load", re.IGNORECASE),
    re.compile(r"\[UI\].*failed to load", re.IGNORECASE),
    re.compile(r"\[JOURNAL\].*failed to load", re.IGNORECASE),
    re.compile(r"\[TRC_(TRACE|ERROR)\].*failed to load", re.IGNORECASE),
    re.compile(r"No session found for session_id", re.IGNORECASE),
    re.compile(r"No transaction.*found", re.IGNORECASE),
    re.compile(r"No.*journal.*found|journal.*empty", re.IGNORECASE),
    re.compile(r"No screens found", re.IGNORECASE),
    re.compile(r"No data extracted from", re.IGNORECASE),
    re.compile(r"Content not found in session for", re.IGNORECASE),
    re.compile(r"Error processing .*(journal|file)", re.IGNORECASE),
    re.compile(r"Error extracting UI flow", re.IGNORECASE),
    re.compile(r"Could not read (registry|feedback) file", re.IGNORECASE),
    re.compile(r"Failed to delete Temp run folder", re.IGNORECASE),
    re.compile(r"Invalid file type uploaded", re.IGNORECASE),
    re.compile(r"Only ZIP files are accepted", re.IGNORECASE),
    re.compile(r"No processed ZIP found for session", re.IGNORECASE),
    re.compile(r"No file categories found for session", re.IGNORECASE),
    re.compile(r"Log file does not exist", re.IGNORECASE),
    re.compile(r"Unexpected error in /error-summary", re.IGNORECASE),
    re.compile(r"Unexpected error in /extract-registry", re.IGNORECASE),
    re.compile(r"Unexpected error in /.*acu", re.IGNORECASE),
    re.compile(r"ZIP extraction failed", re.IGNORECASE),
]

# ---------------------------------------------------------------------------
# Levels that are always excluded outright (before pattern matching)
# ---------------------------------------------------------------------------
EXCLUDED_LEVELS = {"DEBUG", "INFO", "WARNING"}

# WARNING lines that are security signals — override the level exclusion
# These are logged as WARNING in the codebase but are genuinely critical
WARNING_OVERRIDE_PATTERNS = [
    # RBAC / auth middleware
    re.compile(r"RBAC \[40[13]\]", re.IGNORECASE),
    re.compile(r"MIDDLEWARE \[401\]", re.IGNORECASE),
    re.compile(r"FEEDBACK \[401\]", re.IGNORECASE),
    # JWT
    re.compile(r"JWT decode failed", re.IGNORECASE),
    re.compile(r"could not decode JWT", re.IGNORECASE),
    re.compile(r"invalid/expired token", re.IGNORECASE),
    # Session degradation
    re.compile(r"session ID will use.*undefined", re.IGNORECASE),
    # Password reset / identity
    re.compile(r"Reset identity verification failed", re.IGNORECASE),
    re.compile(r"validate_reset_token:.*invalid", re.IGNORECASE),
    re.compile(r"reset_password_endpoint: reset failed", re.IGNORECASE),
]


def is_major_error(line: str) -> bool:
    """Return True if this log line should be captured as a major error."""

    # Extract log level from standard format:
    # 2026-05-06 03:16:22,344 [WARNING] [module:file] message
    level_match = re.search(r"\[(DEBUG|INFO|WARNING|ERROR|CRITICAL)\]", line)
    level = level_match.group(1) if level_match else None

    # Drop routine levels — unless it's a warning we care about
    if level in EXCLUDED_LEVELS:
        if level == "WARNING":
            if not any(p.search(line) for p in WARNING_OVERRIDE_PATTERNS):
                return False
        else:
            return False

    # Apply exclusion patterns first (file-level noise)
    if any(p.search(line) for p in EXCLUDE_PATTERNS):
        return False

    # Must match at least one major pattern
    return any(p.search(line) for p in MAJOR_PATTERNS)


def format_output_line(line: str) -> str:
    """Add a captured-at timestamp and return the decorated line."""
    captured_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    return f"[CAPTURED {captured_at}] {line}"


def tail_and_monitor(log_path: Path, out_path: Path, poll_interval: float):
    """
    Tail *log_path* indefinitely.
    - Every line is printed to stdout.
    - Major errors are also appended to *out_path*.
    """
    print(f"[log_monitor] Watching : {log_path}")
    print(f"[log_monitor] Errors  : {out_path}")
    print(f"[log_monitor] Poll     : {poll_interval}s")
    print("-" * 72)

    # Open the output file once (append mode so restarts don't clobber history)
    out_file = out_path.open("a", encoding="utf-8")
    out_file.write(
        f"\n{'='*72}\n"
        f"[log_monitor] Session started at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n"
        f"{'='*72}\n"
    )
    out_file.flush()

    # Seek to end of log so we only process new lines (not history)
    log_file = log_path.open("r", encoding="utf-8", errors="replace")
    log_file.seek(0, 2)  # SEEK_END

    in_traceback = False   # track multi-line traceback blocks

    try:
        while True:
            line = log_file.readline()

            if not line:
                time.sleep(poll_interval)
                continue

            line_stripped = line.rstrip("\n")

            # Always print every line to terminal
            print(line_stripped)

            # --- traceback continuation logic ---
            # Once we detect "Traceback (most recent call last)" we capture
            # all subsequent lines until we hit the next timestamped log line.
            if re.search(r"Traceback \(most recent call last\)", line):
                in_traceback = True

            is_new_log_line = bool(
                re.match(r"\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}", line)
            )
            if is_new_log_line and in_traceback:
                # This new timestamped line ends the traceback block.
                # Check if the new line itself is also a major error.
                in_traceback = False

            if in_traceback or is_major_error(line_stripped):
                decorated = format_output_line(line_stripped)
                out_file.write(decorated + "\n")
                out_file.flush()

    except KeyboardInterrupt:
        print("\n[log_monitor] Stopped by user.")
    finally:
        out_file.write(
            f"[log_monitor] Session ended at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n"
        )
        out_file.close()
        log_file.close()


def main():
    parser = argparse.ArgumentParser(
        description="Tail app.log and extract major infrastructure errors."
    )
    parser.add_argument(
        "--log",
        default="app.log",
        help="Path to the application log file (default: app.log)",
    )
    parser.add_argument(
        "--out",
        default="critical_errors.log",
        help="Output file for captured major errors (default: critical_errors.log)",
    )
    parser.add_argument(
        "--poll",
        type=float,
        default=1.0,
        help="Polling interval in seconds (default: 1.0)",
    )
    parser.add_argument(
        "--history",
        action="store_true",
        help="If set, scan the existing log content before tailing new lines.",
    )
    args = parser.parse_args()

    log_path = Path(args.log)
    out_path = Path(args.out)

    if not log_path.exists():
        print(f"[log_monitor] ERROR: log file not found: {log_path}", file=sys.stderr)
        sys.exit(1)

    if args.history:
        # One-shot scan of existing content before entering tail mode
        print("[log_monitor] Scanning existing log history first …")
        with log_path.open("r", encoding="utf-8", errors="replace") as f:
            lines = f.readlines()
        captured = [l.rstrip("\n") for l in lines if is_major_error(l.rstrip("\n"))]
        if captured:
            with out_path.open("a", encoding="utf-8") as out:
                out.write(
                    f"\n{'='*72}\n"
                    f"[log_monitor] History scan at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n"
                    f"{'='*72}\n"
                )
                for c in captured:
                    out.write(format_output_line(c) + "\n")
            print(f"[log_monitor] {len(captured)} historical errors written to {out_path}")
        else:
            print("[log_monitor] No major errors found in existing log history.")

    tail_and_monitor(log_path, out_path, args.poll)


if __name__ == "__main__":
    main()