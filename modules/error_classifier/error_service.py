"""
error_service.py
----------------
Orchestrates the error classification pipeline for DNLAT.

Reads PRN file contents directly from the session (already loaded into memory
during ZIP extraction — no disk access needed), runs both parsers, aggregates
results, and returns a structured list ready for the API response.

Usage:
    from modules.error_classifier.error_service import ErrorClassifierService
    service = ErrorClassifierService()
    rows = service.classify_from_session(session_data)
"""

import io
from collections import defaultdict
from typing import List, Dict, Any

from modules.logging_config import logger
from modules.error_classifier.models import ErrorRecord, Severity
from modules.error_classifier.parsers.trcerror_parser import parse_trcerror_from_text
from modules.error_classifier.parsers.trctrace_parser import parse_trctrace_from_text
from modules.error_classifier.classifier import normalise_message


class ErrorClassifierService:

    def classify_from_session(
        self,
        session_data: dict,
        skip_p5: bool = False,
    ) -> List[Dict[str, Any]]:
        """
        Run the full error classification pipeline against PRN content
        stored in the session.

        Args:
            session_data : dict returned by session_service.get_session()
            skip_p5      : if True, P5 verbose entries are excluded from output

        Returns:
            List of dicts with keys:
                no, trace, severity, sev_label, source, message,
                type_error, count, source_file
        """
        all_records: List[ErrorRecord] = []

        # --- TRCERROR ---
        trc_error_contents: dict = session_data.get("trc_error_contents", {})
        if trc_error_contents:
            for filename, content in trc_error_contents.items():
                logger.info(f"[ErrorClassifier] Parsing TRCERROR file: {filename}")
                records = parse_trcerror_from_text(content, filename)
                all_records.extend(records)
        else:
            logger.warning("[ErrorClassifier] No trc_error_contents found in session")

        # --- TRCTRACE ---
        trc_trace_contents: dict = session_data.get("trc_trace_contents", {})
        if trc_trace_contents:
            for filename, content in trc_trace_contents.items():
                logger.info(f"[ErrorClassifier] Parsing TRCTRACE file: {filename}")
                records = parse_trctrace_from_text(content, filename, skip_p5=skip_p5)
                all_records.extend(records)
        else:
            logger.warning("[ErrorClassifier] No trc_trace_contents found in session")

        if not all_records:
            logger.warning("[ErrorClassifier] No records parsed from session PRN files")
            return []

        return self._aggregate(all_records, skip_p5=skip_p5)

    # -----------------------------------------------------------------------
    # Internal
    # -----------------------------------------------------------------------

    def _aggregate(self, records: List[ErrorRecord], skip_p5: bool) -> List[Dict]:
        counts: Dict[str, int] = defaultdict(int)
        representatives: Dict[str, ErrorRecord] = {}

        for rec in records:
            if skip_p5 and rec.severity == Severity.P5:
                continue
            key = rec._dedup_key
            counts[key] += 1
            if key not in representatives:
                representatives[key] = rec

        rows = []
        for key, rec in representatives.items():
            rows.append({
                "trace":       rec.trace,
                "severity":    rec.severity.value,
                "sev_label":   rec.severity.label,
                "source":      rec.source,
                "message":     rec.message,
                "type_error":  rec.type_error,
                "source_file": rec.source_file,
                "count":       counts[key],
                "_sort":       rec.severity.sort_order,
            })

        rows.sort(key=lambda r: (r["_sort"], -r["count"]))

        for i, row in enumerate(rows, start=1):
            row["no"] = i
            del row["_sort"]

        logger.info(
            f"[ErrorClassifier] Aggregated {len(rows)} unique error patterns "
            f"from {len(records)} total records"
        )
        return rows