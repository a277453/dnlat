"""
classifier.py
-------------
All severity-mapping rules in one place.
Tweak this file to change P1/P2/P3/P5 assignments without touching parsers.
"""

import re
from modules.error_classifier.models import Severity


# ---------------------------------------------------------------------------
# TRCERROR — CC_ENTRY block: "Type   : ..." line
# ---------------------------------------------------------------------------

# Maps substring found in the Type line → Severity
_TRCERROR_TYPE_MAP = [
    # P1
    ("Invalid Handling",        Severity.P1),
    ("exception",               Severity.P1),   # future-proofing

    # P2
    ("Controlled reboot",       Severity.P2),
    ("Installation Error",      Severity.P2),

    # P3
    # (none of the observed Type values map to P3 — warnings come via severity col)

    # P5
    ("Informational",           Severity.P5),
    ("Special entries",         Severity.P5),
]


def classify_trcerror_type(type_text: str) -> Severity:
    """Map the 'Type   : ...' line value to a Severity."""
    lower = type_text.lower()
    for keyword, sev in _TRCERROR_TYPE_MAP:
        if keyword.lower() in lower:
            return sev
    return Severity.P2  # safe default for unknown Type lines


# ---------------------------------------------------------------------------
# TRCERROR — CC_ENTRY block: StCode fallback (when no Type line or as override)
# ---------------------------------------------------------------------------

_STCODE_P1_PATTERNS = re.compile(
    r"ERR_MANIPULATION|EXCEPTION|CRITICAL", re.IGNORECASE
)
_STCODE_P2_PATTERNS = re.compile(
    r"ERR_ERROR|CCRM_KILLED|RCM_ERROR|SEV5_RESOURCE_ERROR|USBIO_ERR", re.IGNORECASE
)
_STCODE_P3_PATTERNS = re.compile(
    r"WARN|DFU_WARN", re.IGNORECASE
)
_STCODE_P5_PATTERNS = re.compile(
    r"ERR_INFO|INFO_|START_END", re.IGNORECASE
)


def classify_stcode(stcode: str) -> Severity:
    """Map a StCode string value to Severity."""
    if _STCODE_P1_PATTERNS.search(stcode):
        return Severity.P1
    if _STCODE_P2_PATTERNS.search(stcode):
        return Severity.P2
    if _STCODE_P3_PATTERNS.search(stcode):
        return Severity.P3
    if _STCODE_P5_PATTERNS.search(stcode):
        return Severity.P5
    return Severity.P2  # default unknown StCodes to P2


# ---------------------------------------------------------------------------
# TRCERROR — CSC section: severity column (field[4])
# ---------------------------------------------------------------------------

_CSC_SEV_MAP = {
    "CSC-ERROR":  Severity.P2,
    "CSC-WARN":   Severity.P3,
    "WARNING":    Severity.P3,
    "CSC-INFO":   Severity.P5,
    "CSC-LDINFO": Severity.P5,
    "TLS-INFO":   Severity.P5,
    "CC_ENTRY":   Severity.P2,  # handled separately but kept for safety
}


def classify_csc_severity_col(col: str) -> Severity:
    return _CSC_SEV_MAP.get(col.strip(), Severity.P5)


# ---------------------------------------------------------------------------
# TRCTRACE — trace-type column (field[4])
# ---------------------------------------------------------------------------

_TRCTRACE_SEV_MAP = {
    # P1
    "EXCEPTION":   Severity.P1,
    "JS_ERROR":    Severity.P1,
    "TRACE_ERRO":  Severity.P1,

    # P2
    "ERROR":       Severity.P2,

    # P3
    "WARNING":     Severity.P3,
    "JS_WARNING":  Severity.P3,

    # P5 — everything else defaults here
}

_TRCTRACE_P5_DEFAULT = Severity.P5


def classify_trctrace_type(trace_type: str) -> Severity:
    return _TRCTRACE_SEV_MAP.get(trace_type.strip(), _TRCTRACE_P5_DEFAULT)


# ---------------------------------------------------------------------------
# Message normalisation for dedup grouping
# Strips volatile parts so the same logical error counts as one row.
# ---------------------------------------------------------------------------

_NORM_PATTERNS = [
    (re.compile(r"0x[0-9a-fA-F]+"),        "[HEX]"),       # hex addresses
    (re.compile(r"\bPID:[0-9A-F.]+\b"),     "[PID]"),       # PID tokens
    (re.compile(r"\b\d{4,}\b"),             "[N]"),         # long integers
    (re.compile(r"SrcLine:\s*\d+"),         "SrcLine:[N]"), # source line nums
    (re.compile(r"\(\d+\.\s*call[^)]*\)"),  "([N]. call)"), # call counters
    (re.compile(r"\s+"),                    " "),            # collapse whitespace
]


def normalise_message(msg: str) -> str:
    result = msg.strip()
    for pattern, replacement in _NORM_PATTERNS:
        result = pattern.sub(replacement, result)
    return result.strip()