"""
trcerror_parser.py
------------------
Parses TRCERROR.PRN which contains two distinct sections:

  Section A — CSC/ProBase entries
    Header line:  22/67 250403 16:56:13.60 CSCWDFU  CSC-WARN  PID:... Data:...
    Body:         key: value lines until blank line

  Section B — XFS/ProTopas CC_ENTRY blocks
    Header line:  00/05 250403 16:19:01.27 TRCERR   CC_ENTRY  PID:... Data:...
    Body:         Type/Module/Process/APIName/StCode/SrcName/Add/Cause lines
"""

import re
import logging
from typing import List, Optional
from modules.error_classifier.models import ErrorRecord, Severity
from modules.error_classifier.classifier import (
    classify_trcerror_type,
    classify_stcode,
    classify_csc_severity_col,
    normalise_message,
)

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Regex patterns
# ---------------------------------------------------------------------------

# Matches both section headers:
#   00/05 250403 16:19:01.27 TRCERR  CC_ENTRY  PID:...
#   22/67 250403 16:56:13.60 CSCWDFU CSC-WARN  PID:...
_RE_ENTRY_HEADER = re.compile(
    r"^\d+/\d+\s+\d{6}\s+\d{2}:\d{2}:\d{2}\.\d+\s+(\S+)\s+(\S+)\s+PID:(\S+)\s+Data:(\d+)"
)

# Body field extractors
_RE_TYPE    = re.compile(r"^Type\s*:\s*(.+)")
_RE_MODULE  = re.compile(r"^Module\s*:\s*(.+)")
_RE_PROCESS = re.compile(r"^Process\s*:\s*(.+)")
_RE_APINAME = re.compile(r"^APIName\s*:\s*(.+)")
_RE_STCODE_CC = re.compile(r"^StCode\s*:\s*(\S+)")           # CC_ENTRY style
_RE_STCODE_CSC = re.compile(r"^StCode:\s*\S+\s+CodeTxt:\s*(\S+)")  # CSC style
_RE_ADD     = re.compile(r"^Add\s*:\s*'?(.+?)'?\s*$")
_RE_CAUSE   = re.compile(r"^Cause\s*:\s*(.*)")

# ActiveProcesses blob — strip this from reboot Cause text
_RE_ACTIVE_PROC = re.compile(r"ActiveProcesses:.*", re.DOTALL)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _clean_add(raw: str) -> str:
    """Strip surrounding quotes and whitespace from Add field."""
    return raw.strip().strip("'").strip()


def _clean_cause(lines: List[str]) -> str:
    """
    Join cause continuation lines, strip ActiveProcesses blob,
    collapse whitespace.
    """
    text = " ".join(l.strip() for l in lines if l.strip())
    text = _RE_ACTIVE_PROC.sub("", text).strip()
    return text


# ---------------------------------------------------------------------------
# Block parser
# ---------------------------------------------------------------------------

def _parse_block(source_col: str, sev_col: str, body_lines: List[str]) -> Optional[ErrorRecord]:
    """
    Parse a single TRCERROR entry block.
    source_col : field[3] from header (e.g. TRCERR, CSCWDFU, CSCWSEL, RM4)
    sev_col    : field[4] from header (e.g. CC_ENTRY, CSC-WARN, WARNING)
    body_lines : all lines after the header up to the next blank separator
    """

    type_text    = ""
    module_text  = ""
    process_text = ""
    api_name     = ""
    stcode       = ""
    add_text     = ""
    add_extra_lines: List[str] = []   # continuation lines after Add:
    cause_lines: List[str] = []
    in_add   = False
    in_cause = False

    # Known field prefixes — when seen, stop treating lines as Add continuation
    _FIELD_PREFIXES = (
        "Type", "Module", "Process", "APIName", "StCode", "StCode:",
        "SrcName", "Add", "Cause", "hCsc", "Spec", "StClass",
        "StWarn", "RetCode", "ActCode", "ErrCode", "SrcLine",
        "ActiveProcesses",
    )

    for line in body_lines:
        # Stop Add continuation if a known field starts
        if in_add and any(line.startswith(p) for p in _FIELD_PREFIXES):
            in_add = False

        if m := _RE_TYPE.match(line):
            type_text = m.group(1).strip()
            in_add = False; in_cause = False
        elif m := _RE_MODULE.match(line):
            module_text = m.group(1).strip()
            in_add = False; in_cause = False
        elif m := _RE_PROCESS.match(line):
            process_text = m.group(1).strip()
            in_add = False; in_cause = False
        elif m := _RE_APINAME.match(line):
            api_name = m.group(1).strip()
            in_add = False; in_cause = False
        elif m := _RE_STCODE_CSC.match(line):
            # CSC style: StCode: 0xe0001008  CodeTxt: USBIO_ERR_ALREADY_BOUND
            stcode = m.group(1).strip()
            in_add = False; in_cause = False
        elif m := _RE_STCODE_CC.match(line):
            # CC_ENTRY style: StCode : ERR_ERROR (0x24000010)
            stcode = m.group(1).strip()
            in_add = False; in_cause = False
        elif m := _RE_ADD.match(line):
            add_text = _clean_add(m.group(1))
            in_add = True; in_cause = False
        elif in_add and line.strip():
            # Continuation lines after Add: — captures TypeError, Stacktrace etc.
            add_extra_lines.append(line.strip())
        elif _RE_CAUSE.match(line):
            in_add = False
            rest = _RE_CAUSE.match(line).group(1).strip()
            if rest:
                cause_lines.append(rest)
            in_cause = True
        elif in_cause and line.strip():
            cause_lines.append(line.strip())

    # -----------------------------------------------------------------------
    # Determine severity
    # -----------------------------------------------------------------------
    if sev_col == "CC_ENTRY":
        # Primary: use Type field; secondary: StCode
        if type_text:
            severity = classify_trcerror_type(type_text)
        elif stcode:
            severity = classify_stcode(stcode)
        else:
            severity = Severity.P2
    else:
        # CSC section — use severity column
        severity = classify_csc_severity_col(sev_col)
        # Upgrade to P1 if StCode says so
        if stcode and classify_stcode(stcode) == Severity.P1:
            severity = Severity.P1

    # -----------------------------------------------------------------------
    # Determine source (prefer Module name, fallback to source_col DLL)
    # -----------------------------------------------------------------------
    # Module lines look like: "ProFlex4 UI - Gui for Application (980) StClass: 0x64e5"
    # Extract just the readable part before the StClass
    source = source_col
    if module_text:
        source = re.split(r"\s+StClass:", module_text)[0].strip()
    elif process_text:
        source = process_text

    # -----------------------------------------------------------------------
    # Determine trace (StCode) — fallback chain
    # -----------------------------------------------------------------------
    if not stcode:
        if type_text:
            stcode = type_text
        elif api_name:
            stcode = api_name
        else:
            stcode = sev_col

    # -----------------------------------------------------------------------
    # Determine message — priority: Add (+continuations) > Cause > APIName
    # -----------------------------------------------------------------------
    cause_text = _clean_cause(cause_lines)

    # Extract full TypeError block from Add continuation lines
    # Starts at the line containing "TypeError:" and captures everything after,
    # including all stacktrace "at ..." frames, until the block ends.
    _RE_TYPE_ERROR_START = re.compile(r"TypeError:", re.IGNORECASE)
    type_error_lines: List[str] = []
    in_type_error = False
    for extra in add_extra_lines:
        if _RE_TYPE_ERROR_START.search(extra) and not in_type_error:
            in_type_error = True
        if in_type_error:
            type_error_lines.append(extra.strip())
    type_error_text = "\n".join(type_error_lines) if type_error_lines else ""

    # Build the meaningful Add message (everything BEFORE the TypeError block)
    # Skip stacktrace frames, Stacktrace: header, type:/info: metadata lines
    _SKIP_PREFIXES = ("at ", "    at ", "Stacktrace:", "type:", "info:")
    extra_meaningful = []
    for l in add_extra_lines:
        if _RE_TYPE_ERROR_START.search(l):
            break   # stop before the TypeError block starts
        if not any(l.startswith(p) for p in _SKIP_PREFIXES):
            extra_meaningful.append(l)

    if add_text and add_text not in ("", "'"):
        message = add_text
        if extra_meaningful:
            message = f"{message} | {' | '.join(extra_meaningful)}"
        # Append cause if it adds info beyond the generic boilerplate
        generic_causes = {
            "a general error inside the module/control!",
            "an unexpected situation inside the module/control!",
            "a mandatory device is not operational or not installed",
        }
        if cause_text and cause_text.lower() not in generic_causes:
            message = f"{message} | {cause_text}"
    elif cause_text:
        message = cause_text
    elif api_name:
        message = api_name
    else:
        message = type_text or sev_col

    # Deduplicated TypeError string for the type_error field
    # Upgrade severity to P1 if a TypeError was found in the body
    if type_error_text and severity != Severity.P1:
        severity = Severity.P1

    norm_msg = normalise_message(message)
    dedup_key = f"{stcode}||{severity}||{source}||{norm_msg}"

    return ErrorRecord(
        trace=stcode,
        severity=severity,
        source=source,
        message=message,
        type_error=type_error_text,
        source_file="TRCERROR",
        _dedup_key=dedup_key,
    )


# ---------------------------------------------------------------------------
# Public parser
# ---------------------------------------------------------------------------

def parse_trcerror(filepath: str) -> List[ErrorRecord]:
    """Parse TRCERROR.PRN from disk. Kept for standalone/test usage."""
    try:
        with open(filepath, "r", encoding="utf-8", errors="replace") as fh:
            lines = [l.rstrip("\r\n") for l in fh.readlines()]
    except FileNotFoundError:
        logger.error(f"[trcerror_parser] File not found: {filepath}")
        return []

    records = _parse_lines(lines, filepath)
    logger.info(f"[trcerror_parser] Parsed {len(records)} records from {filepath}")
    return records

    # Flush last block
    if in_block and current_body:
        rec = _parse_block(current_source, current_sev, current_body)
        if rec:
            records.append(rec)

    logger.info(f"[trcerror_parser] Parsed {len(records)} records from {filepath}")
    return records


def parse_trcerror_from_text(content: str, filename: str = "TRCERROR.PRN") -> List[ErrorRecord]:
    """
    Parse TRCERROR content supplied as a string (from session memory).
    Delegates to parse_trcerror() by splitting on newlines — no disk access needed.
    """
    lines = content.splitlines()
    return _parse_lines(lines, filename)


def _parse_lines(lines: List[str], source_name: str) -> List[ErrorRecord]:
    """Core parsing logic shared by both file-based and text-based entry points."""
    records: List[ErrorRecord] = []
    current_source = ""
    current_sev    = ""
    current_body: List[str] = []
    in_block = False

    for line in lines:
        line = line.rstrip("\r\n")
        m = _RE_ENTRY_HEADER.match(line)
        if m:
            if in_block and current_body:
                rec = _parse_block(current_source, current_sev, current_body)
                if rec:
                    records.append(rec)
            current_source = m.group(1)
            current_sev    = m.group(2)
            current_body   = []
            in_block       = True
        elif in_block:
            if line.strip() == "":
                if current_body:
                    rec = _parse_block(current_source, current_sev, current_body)
                    if rec:
                        records.append(rec)
                    current_body = []
                    in_block = False
            else:
                current_body.append(line)

    if in_block and current_body:
        rec = _parse_block(current_source, current_sev, current_body)
        if rec:
            records.append(rec)

    return records