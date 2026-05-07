"""
trctrace_parser.py
------------------
Parses TRCTRACE.PRN — sequential numbered trace entries.
"""

import re
import logging
from typing import List
from modules.error_classifier.models import ErrorRecord, Severity
from modules.error_classifier.classifier import classify_trctrace_type, normalise_message

logger = logging.getLogger(__name__)

_RE_ENTRY_HEADER = re.compile(
    r"^(\d+)\s+(\d{6})\s+(\d{2}:\d{2}:\d{2}\.\d+)\s+(\S+)\s+(\S+)\s+PID:(\S+)\s+Data:(\d+)"
)
_RE_TYPE_ERROR_START = re.compile(r"TypeError:", re.IGNORECASE)


def _clean_body(lines: List[str]) -> str:
    parts = []
    for line in lines:
        stripped = line.strip().lstrip("*><! ").strip()
        if stripped:
            parts.append(stripped)
    return " | ".join(parts) if parts else ""


def _parse_lines(lines: List[str], source_name: str, skip_p5: bool) -> List[ErrorRecord]:
    records: List[ErrorRecord] = []
    current_source     = ""
    current_trace_type = ""
    current_severity   = Severity.P5
    current_body: List[str] = []
    in_block = False

    def flush_block():
        nonlocal current_body
        if not current_body:
            return
        if skip_p5 and current_severity == Severity.P5:
            current_body = []
            return

        message  = _clean_body(current_body)
        norm_msg = normalise_message(message)

        type_error_lines = []
        in_type_error = False
        for line in current_body:
            if _RE_TYPE_ERROR_START.search(line) and not in_type_error:
                in_type_error = True
            if in_type_error:
                type_error_lines.append(line.strip())
        type_error_text = "\n".join(type_error_lines) if type_error_lines else ""

        effective_severity = current_severity
        if type_error_text and effective_severity != Severity.P1:
            effective_severity = Severity.P1

        dedup_key = f"{current_trace_type}||{effective_severity}||{current_source}||{norm_msg}"

        records.append(ErrorRecord(
            trace=current_trace_type,
            severity=effective_severity,
            source=current_source,
            message=message,
            type_error=type_error_text,
            source_file=source_name,
            _dedup_key=dedup_key,
        ))
        current_body = []

    for line in lines:
        line = line.rstrip("\r\n")
        m = _RE_ENTRY_HEADER.match(line)
        if m:
            flush_block()
            current_source     = m.group(4)
            current_trace_type = m.group(5)
            current_severity   = classify_trctrace_type(current_trace_type)
            in_block           = True
        elif in_block:
            if line.strip() == "":
                flush_block()
                in_block = False
            else:
                current_body.append(line)

    flush_block()
    return records


def parse_trctrace(filepath: str, skip_p5: bool = False) -> List[ErrorRecord]:
    """Parse TRCTRACE.PRN from disk. Kept for standalone/test usage."""
    try:
        with open(filepath, "r", encoding="utf-8", errors="replace") as fh:
            lines = fh.readlines()
    except FileNotFoundError:
        logger.error(f"[trctrace_parser] File not found: {filepath}")
        return []
    records = _parse_lines(lines, "TRCTRACE", skip_p5)
    logger.info(f"[trctrace_parser] Parsed {len(records)} records from {filepath}")
    return records


def parse_trctrace_from_text(
    content: str, filename: str = "TRCTRACE.PRN", skip_p5: bool = False
) -> List[ErrorRecord]:
    """Parse TRCTRACE content from session memory string — no disk access."""
    lines = content.splitlines()
    records = _parse_lines(lines, filename, skip_p5)
    logger.info(f"[trctrace_parser] Parsed {len(records)} records from {filename}")
    return records