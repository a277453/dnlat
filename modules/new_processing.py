#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
processing.py

Three services:
  1. ProcessingService        — prepares the final file categorization API response.
  2. LogPreprocessorService   — parses raw EJ / UI-Journal log text into
                                per-transaction dicts before sending to the LLM.
                                Reduces token usage by stripping noise and
                                extracting only the fields the model needs.
  3. TransactionMergerService — matches EJ transactions to JRN transactions by
                                timestamp-range overlap, merges them into a single
                                enriched record (EJ as base, JRN fields appended),
                                and labels unmatched transactions as ej_only /
                                jrn_only so the LLM always has full context.
"""

from pathlib import Path
from typing import Dict, List, Optional, Tuple
from .schemas import FileCategorizationResponse, CategoryCount
from modules.logging_config import logger

import re
import json
from collections import defaultdict
from datetime import datetime, timedelta

logger.info("Starting processing_service")


# ═══════════════════════════════════════════════════════════════
#  Shared timestamp utility
# ═══════════════════════════════════════════════════════════════

def _to_dt(ts: Optional[str]) -> Optional[datetime]:
    """Parse HH:MM:SS string to datetime (date-agnostic, using 1900-01-01)."""
    if not ts:
        return None
    try:
        return datetime.strptime(ts, "%H:%M:%S")
    except ValueError:
        return None


def _ranges_overlap(
    a_start: Optional[str], a_end: Optional[str],
    b_start: Optional[str], b_end: Optional[str],
    skew_seconds: int = 2
) -> bool:
    """
    Return True if two HH:MM:SS timestamp ranges overlap.
    Adds a skew_seconds buffer on each side to handle minor clock differences.
    Falls back to start-time proximity if either end is missing.
    """
    a0 = _to_dt(a_start)
    b0 = _to_dt(b_start)

    if a0 is None or b0 is None:
        return False

    skew = timedelta(seconds=skew_seconds)
    a1   = _to_dt(a_end)  or (a0 + timedelta(seconds=120))
    b1   = _to_dt(b_end)  or (b0 + timedelta(seconds=120))

    return (a0 - skew) <= (b1 + skew) and (b0 - skew) <= (a1 + skew)


# ═══════════════════════════════════════════════════════════════
#  1. ProcessingService  (unchanged)
# ═══════════════════════════════════════════════════════════════

class ProcessingService:
    """
    FUNCTION:
        ProcessingService

    DESCRIPTION:
        Handles the processing of categorized file data and prepares
        the final structured API response for the caller.

    USAGE:
        service = ProcessingService()
        result = service.prepare_response(file_categories, extract_path)
    """

    def prepare_response(
        self,
        file_categories: Dict[str, List[str]],
        extract_path: Path
    ) -> FileCategorizationResponse:
        """
        FUNCTION:
            prepare_response

        DESCRIPTION:
            Processes categorized file data, calculates totals, converts them
            into CategoryCount objects, and returns a standardized response model.

        PARAMETERS:
            file_categories (Dict[str, List[str]]) :
                Dictionary where key = category name,
                value = list of associated file names.
            extract_path (Path) :
                Path where files were extracted.

        RETURNS:
            FileCategorizationResponse :
                Includes total files, extraction path, and categorized file details.
        """
        logger.info("Preparing final file categorization response.")

        total_files = sum(len(files) for files in file_categories.values())
        logger.debug(f"Total files counted: {total_files}")

        category_counts = {
            category: CategoryCount(
                count=len(files),
                files=files
            )
            for category, files in file_categories.items()
        }
        logger.debug(f"Category counts prepared: {list(category_counts.keys())}")

        response = FileCategorizationResponse(
            total_files=total_files,
            extraction_path=str(extract_path),
            categories=category_counts
        )

        logger.info("File categorization response prepared successfully.")
        return response


# ═══════════════════════════════════════════════════════════════
#  2. LogPreprocessorService
# ═══════════════════════════════════════════════════════════════

# ── Shared patterns ───────────────────────────────────────────
_LINE_RE     = re.compile(r'^\s*(\d{2}:\d{2}:\d{2})\s+(\d+)\s+(.*)$')
_STAR_SEP_RE = re.compile(r'^\s*\*{3,}\s*$')

# ── EJ-specific patterns ──────────────────────────────────────
_EJ_START_CODES = {"3207", "3239"}
_EJ_END_CODE    = "3202"

_EJ_TXN_NO_RE  = re.compile(r"Transaction no\.\s+'([^']+)'")
_EJ_PAN_RE     = re.compile(r"pan\s+'([^']+)'")
_EJ_FUNC_RE    = re.compile(r"Function\s+'([^']+)'\s+selected")
_EJ_ACCT_RE    = re.compile(r"Account selected\s+\(([^)]+)\)\s+'([^']+)'")
_EJ_AMOUNT_RE  = re.compile(r"Total Amount (?:Deposited|Requested|Withdrawn)\s*:\s*([\d,\.]+)\s*(\w+)")
_EJ_NOTES_RE   = re.compile(r"Identified notes:\s*(.*)")
_EJ_STATE_RE   = re.compile(r"state\s+'(\w+)',\s+end-state\s*'(\w)'")
_EJ_CANCEL_RE  = re.compile(r"Customer cancels|Transaction cancelled", re.I)
_EJ_TIMEOUT_RE = re.compile(r"Customer timeout|Transaction timed out", re.I)
_EJ_PIN_RE     = re.compile(r"Pin entered", re.I)
_EJ_CARD_RE    = re.compile(r"Card successfully presented", re.I)

_EJ_FUNC_MAP = {
    "CIN/CI":      "Cash Deposit",
    "COUT/GA":     "Cash Withdrawal",
    "BAL/BI":      "Balance Inquiry",
    "PINCHG/CH":   "Pin Change",
    "PINAUTH/PV":  "Pin Verification",
    "VCPPREFS/PR": "Preferences",
    "COUTFAST":    "Fast Cash",
    "TRA":         "Transfer",
    "BILLPAY":     "Bill Payment",
}

# ── JRN-specific patterns ─────────────────────────────────────
_JRN_START_CODES = {"3207", "3239"}
_JRN_END_CODE    = "3202"

_JRN_UUID_RE      = re.compile(r'UUID:\s*<([^>]+)>', re.I)
_JRN_FUNC_RE      = re.compile(r"Function '([^']+)', Hostfunction '([^']+)' selected", re.I)
_JRN_RAW_RECV_RE  = re.compile(r'41005.*?Received raw message\s*:\s*(.*)', re.I)
_JRN_RESP_CODE_RE = re.compile(r'RESPONSE CODE:\s*([0-9A-Za-z]+)', re.I)
_JRN_STAN_RE      = re.compile(r'STAN:\s*([0-9]+)', re.I)
_JRN_REQ_AMT_RE   = re.compile(r'REQUESTED AMOUNT:\s*([\d,\.]+)', re.I)
_JRN_DISP_OK_RE   = re.compile(r'Valuedoc dispense function successful|Valuedoc\(s\) presented', re.I)
_JRN_RETRACT_RE   = re.compile(r'Money Retract Counter Changed:\s*Old\s*=\s*(\d+)\s*New\s*=\s*(\d+)', re.I)
_JRN_PRES_TOUT_RE = re.compile(r'Present timeout', re.I)
_JRN_PRES_FAIL_RE = re.compile(r'present failed', re.I)
_JRN_DEV_ERR_RE   = re.compile(r'Error notified for device\s+([A-Z_]+)', re.I)
_JRN_ERR_NR_RE    = re.compile(r'ErrorNr:\s*(\d+)\s*\(Class:\s*(\w+)', re.I)
_JRN_CANCEL_RE    = re.compile(r'Transaction canceled by customer', re.I)
_JRN_TIMEOUT_RE   = re.compile(r'Transaction timed out', re.I)
_JRN_APP_STATE_RE = re.compile(r'Application state is:\s*(\w+\s*\(\d+\))', re.I)
_JRN_PREF_RE      = re.compile(r'Fastcash Account preference selection changed to\s+(.*)', re.I)
_JRN_PREF_REST_RE = re.compile(r'Preferences restored', re.I)
_JRN_CARD_EJCT_RE = re.compile(r'Card ejected and taken', re.I)

_JRN_FUNC_MAP = {
    ('PINAUTH',  'PV'): "Pin Verification",
    ('COUT',     'GA'): "Cash Withdrawal",
    ('BAL',      'BI'): "Balance Inquiry",
    ('CIN',      'CI'): "Cash Deposit",
    ('PINCHG',   'CH'): "Pin Change",
    ('VCPPREFS', 'PR'): "Preferences",
    ('COUTFAST',  None): "Fast Cash",
    ('TRA',       None): "Transfer",
    ('BILLPAY',   None): "Bill Payment",
}


def _resolve_jrn_func(func: str, host: str) -> str:
    if not func:
        return "Unknown"
    key = (func.upper(), host.upper() if host else None)
    if key in _JRN_FUNC_MAP:
        return _JRN_FUNC_MAP[key]
    for (f, h), name in _JRN_FUNC_MAP.items():
        if f == func.upper() and h is None:
            return name
    return f"{func}/{host}" if host else func


def _decode_raw_jrn(raw: str) -> dict:
    decoded = (raw
               .replace('\\0a', '\n').replace('\x0a', '\n')
               .replace('\\1c', '|').replace('\x1c', '|')
               .replace('\\1d', '|').replace('\x1d', '|'))
    result = {}
    for pattern, key in (
        (_JRN_RESP_CODE_RE, 'response_code'),
        (_JRN_STAN_RE,      'stan'),
        (_JRN_REQ_AMT_RE,   'amount'),
    ):
        m = pattern.search(decoded)
        if m:
            result[key] = m.group(1).strip()
    return result


class LogPreprocessorService:
    """
    FUNCTION:
        LogPreprocessorService

    DESCRIPTION:
        Parses raw EJ (Customer Journal) and UI Journal (.jrn) log text
        into a list of per-transaction dicts. Each dict contains only the
        fields relevant for LLM analysis, stripping all noise to minimise
        input tokens.

    USAGE:
        svc = LogPreprocessorService()
        ej_records  = svc.preprocess_ej(raw_ej_text)
        jrn_records = svc.preprocess_jrn(raw_jrn_text)
        prompt      = svc.build_prompt(records, atm_id="DN_ATM_2043")
    """

    def preprocess_ej(self, raw_log: str) -> List[dict]:
        """
        FUNCTION:
            preprocess_ej

        DESCRIPTION:
            Parses raw Customer Journal (EJ) text into per-transaction dicts.

        PARAMETERS:
            raw_log (str) : Raw EJ log text as a single string.

        RETURNS:
            List[dict] : One dict per transaction with fields:
                         ts_start, ts_end, txn_number, pan, type,
                         account, amount, currency, notes,
                         pin_entered, card_returned, state,
                         end_state, status, events.
        """
        logger.info("Preprocessing EJ log.")
        lines = raw_log.splitlines()
        transactions = []
        current: Optional[dict] = None

        def _flush():
            nonlocal current
            if current:
                current["status"] = self._derive_ej_status(current)
                transactions.append(current)
            current = None

        for line in lines:
            if _STAR_SEP_RE.match(line):
                continue
            m = _LINE_RE.match(line)
            if not m:
                continue
            ts, code, msg = m.groups()

            if code in _EJ_START_CODES:
                _flush()
                current = {
                    "ts_start":      ts,
                    "ts_end":        None,
                    "txn_number":    None,
                    "pan":           None,
                    "type":          "Unknown",
                    "account":       None,
                    "amount":        None,
                    "currency":      None,
                    "notes":         None,
                    "pin_entered":   False,
                    "card_returned": False,
                    "state":         None,
                    "end_state":     None,
                    "events":        [],
                }
                continue

            if current is None:
                continue

            if (txn_m := _EJ_TXN_NO_RE.search(msg)) and not current["txn_number"]:
                current["txn_number"] = txn_m.group(1)
            if (pan_m := _EJ_PAN_RE.search(msg)) and not current["pan"]:
                current["pan"] = pan_m.group(1)
            if func_m := _EJ_FUNC_RE.search(msg):
                current["type"] = _EJ_FUNC_MAP.get(func_m.group(1), func_m.group(1))
            if (acct_m := _EJ_ACCT_RE.search(msg)) and not current["account"]:
                current["account"] = f"{acct_m.group(1)} {acct_m.group(2)}"
            if (amt_m := _EJ_AMOUNT_RE.search(msg)) and not current["amount"]:
                current["amount"]   = amt_m.group(1)
                current["currency"] = amt_m.group(2)
            if (notes_m := _EJ_NOTES_RE.search(msg)) and not current["notes"]:
                current["notes"] = notes_m.group(1).strip()
            if _EJ_PIN_RE.search(msg):
                current["pin_entered"] = True
            if _EJ_CARD_RE.search(msg):
                current["card_returned"] = True
            if _EJ_CANCEL_RE.search(msg):
                if "Customer cancelled" not in current["events"]:
                    current["events"].append("Customer cancelled")
            if _EJ_TIMEOUT_RE.search(msg):
                if "Timeout" not in current["events"]:
                    current["events"].append("Timeout")

            if code == _EJ_END_CODE:
                if state_m := _EJ_STATE_RE.search(msg):
                    current["state"]     = state_m.group(1)
                    current["end_state"] = state_m.group(2)
                current["ts_end"] = ts
                _flush()

        _flush()
        cleaned = [self._compact(t) for t in transactions]
        logger.info(f"EJ preprocessing complete. {len(cleaned)} transactions parsed.")
        return cleaned

    def preprocess_jrn(self, raw_log: str) -> List[dict]:
        """
        FUNCTION:
            preprocess_jrn

        DESCRIPTION:
            Parses raw UI Journal (.jrn) log text into per-transaction dicts.
            Captures protocol steps, preference changes, app state transitions,
            device errors, and card ejection events.

        PARAMETERS:
            raw_log (str) : Raw .jrn log text as a single string.

        RETURNS:
            List[dict] : One dict per transaction. JRN-specific fields:
                         uuid, protocol_steps, account_preference,
                         preferences_restored, card_ejected,
                         app_state_start, app_state_end,
                         response_code, stan, amount,
                         dispense_ok, retract_counter,
                         present_timeout, present_failed,
                         device_errors, cancelled, txn_timeout,
                         status, events.
        """
        logger.info("Preprocessing JRN log.")
        lines = raw_log.splitlines()
        transactions = []
        current: Optional[dict] = None
        pending_device: Optional[str] = None

        def _flush():
            nonlocal current
            if current:
                current["status"] = self._derive_jrn_status(current)
                current["events"] = self._build_jrn_events(current)
                transactions.append(current)
            current = None

        def _new_txn(ts: str) -> dict:
            return {
                "ts_start":            ts,
                "ts_end":              None,
                "uuid":                None,
                "type":                "Unknown",
                "app_state_start":     None,
                "app_state_end":       None,
                "protocol_steps":      [],
                "account_preference":  None,
                "preferences_restored":False,
                "card_ejected":        False,
                "response_code":       None,
                "stan":                None,
                "amount":              None,
                "dispense_ok":         False,
                "retract_counter":     None,
                "present_timeout":     False,
                "present_failed":      False,
                "device_errors":       [],
                "_dev_err_keys":       set(),
                "_pending_req":        {},
                "cancelled":           False,
                "txn_timeout":         False,
            }

        for line in lines:
            m = _LINE_RE.match(line)
            if not m:
                if current and pending_device:
                    if enm := _JRN_ERR_NR_RE.search(line):
                        key = (pending_device, enm.group(1))
                        if key not in current["_dev_err_keys"]:
                            current["_dev_err_keys"].add(key)
                            current["device_errors"].append({
                                "device":    pending_device,
                                "error_nr":  enm.group(1),
                                "err_class": enm.group(2),
                            })
                    pending_device = None
                if current:
                    if _JRN_CANCEL_RE.search(line):  current["cancelled"]   = True
                    if _JRN_TIMEOUT_RE.search(line): current["txn_timeout"] = True
                continue

            ts, code, msg = m.groups()
            pending_device = None

            if code in _JRN_START_CODES:
                _flush()
                current = _new_txn(ts)
                if app_m := _JRN_APP_STATE_RE.search(msg):
                    current["app_state_start"] = app_m.group(1).strip()
                continue

            if current is None:
                continue

            if code == _JRN_END_CODE:
                current["ts_end"] = ts
                _flush()
                continue

            # UUID
            if (uuid_m := _JRN_UUID_RE.search(msg)) and not current["uuid"]:
                current["uuid"] = uuid_m.group(1)

            # App state transitions
            if app_m := _JRN_APP_STATE_RE.search(msg):
                state_val = app_m.group(1).strip()
                if not current["app_state_start"]:
                    current["app_state_start"] = state_val
                else:
                    current["app_state_end"] = state_val

            # Protocol steps: 1042 = request sent, 1043 = response received
            if code == "1042":
                if req_m := re.search(r'Request\s+(\w+)\s+\((\w+)\)\s+sent to\s+(\S+)', msg, re.I):
                    current["_pending_req"][req_m.group(1)] = req_m.group(2)
            elif code == "1043":
                if resp_m := re.search(
                    r'(\S+)\s+returned\s+(\w+(?:\s+\w+)*)\s+\(\d+\)\s+for request\s+(\w+)',
                    msg, re.I
                ):
                    req_name = resp_m.group(3)
                    result   = resp_m.group(2).strip()
                    func_ctx = current["_pending_req"].pop(req_name, "?")
                    step     = f"{req_name}({func_ctx})→{result}"
                    if step not in current["protocol_steps"]:
                        current["protocol_steps"].append(step)

            # Function / type
            if func_m := _JRN_FUNC_RE.search(msg):
                current["type"] = _resolve_jrn_func(func_m.group(1), func_m.group(2))

            # 41005 receipt decode
            if code == "41005":
                if rrm := _JRN_RAW_RECV_RE.match(line):
                    fields = _decode_raw_jrn(rrm.group(1))
                    for attr in ("response_code", "stan", "amount"):
                        if fields.get(attr) and current[attr] is None:
                            current[attr] = fields[attr]

            # Preferences
            if pref_m := _JRN_PREF_RE.search(msg):
                current["account_preference"] = pref_m.group(1).strip()
            if _JRN_PREF_REST_RE.search(msg):
                current["preferences_restored"] = True

            # Card ejected
            if _JRN_CARD_EJCT_RE.search(msg):
                current["card_ejected"] = True

            # Dispense / retract / present flags
            if _JRN_DISP_OK_RE.search(msg):    current["dispense_ok"]    = True
            if rtm := _JRN_RETRACT_RE.search(msg):
                current["retract_counter"] = f"{rtm.group(1)}→{rtm.group(2)}"
            if _JRN_PRES_TOUT_RE.search(msg):  current["present_timeout"] = True
            if _JRN_PRES_FAIL_RE.search(msg):  current["present_failed"]  = True

            # Device errors
            if derr := _JRN_DEV_ERR_RE.search(msg):
                pending_device = derr.group(1).strip()
                if enm := _JRN_ERR_NR_RE.search(msg):
                    key = (pending_device, enm.group(1))
                    if key not in current["_dev_err_keys"]:
                        current["_dev_err_keys"].add(key)
                        current["device_errors"].append({
                            "device":    pending_device,
                            "error_nr":  enm.group(1),
                            "err_class": enm.group(2),
                        })
                    pending_device = None

            if _JRN_CANCEL_RE.search(msg):  current["cancelled"]   = True
            if _JRN_TIMEOUT_RE.search(msg): current["txn_timeout"] = True

        _flush()

        for t in transactions:
            t.pop("_dev_err_keys", None)
            t.pop("_pending_req",  None)

        cleaned = [self._compact(t) for t in transactions]
        logger.info(f"JRN preprocessing complete. {len(cleaned)} transactions parsed.")
        return cleaned

    def build_prompt(
        self,
        records: List[dict],
        atm_id: str = "",
        max_chars: int = 12_000
    ) -> str:
        """
        FUNCTION:
            build_prompt

        DESCRIPTION:
            Converts per-transaction records into a compact JSON prompt
            string ready to send to the LLM. Enforces a hard character cap.

        PARAMETERS:
            records   (List[dict]) : Output of preprocess_ej(), preprocess_jrn(),
                                     or TransactionMergerService.merge().
            atm_id    (str)        : ATM identifier embedded in the prompt header.
            max_chars (int)        : Hard cap on prompt length (default 12,000).

        RETURNS:
            str : LLM-ready prompt string.
        """
        payload = json.dumps(records, indent=2)
        if len(payload) > max_chars:
            payload = payload[:max_chars] + "\n... [TRUNCATED]"
            logger.warning("LLM prompt truncated to max_chars limit.")

        atm_line = f"ATM_ID={atm_id}\n" if atm_id else ""
        return (
            f"{atm_line}"
            f"The following is a structured list of transactions extracted from "
            f"the ATM log. Each entry contains only fields relevant to diagnosis.\n\n"
            f"LOG:\n{payload}"
        )

    # ── Private helpers ────────────────────────────────────────

    @staticmethod
    def _derive_ej_status(t: dict) -> str:
        events    = t.get("events", [])
        end_state = t.get("end_state")
        if "Customer cancelled" in events:  return "Customer Cancelled"
        if "Timeout" in events:             return "Timeout"
        if end_state == "N" and t.get("amount"): return "Success"
        if end_state == "E":                return "Incomplete"
        return "Unknown"

    @staticmethod
    def _derive_jrn_status(t: dict) -> str:
        if t.get("cancelled"):                               return "Customer Cancelled"
        if t.get("txn_timeout"):                             return "Timeout"
        if t.get("device_errors") or t.get("present_failed"): return "Error"
        if t.get("retract_counter") or t.get("present_timeout"): return "Retract"
        rc = t.get("response_code", "")
        if rc and rc not in ("0", "00"):                     return "Host Error"
        if t.get("dispense_ok"):                             return "Success"
        return "Normal"

    @staticmethod
    def _build_jrn_events(t: dict) -> List[str]:
        events = []
        if t.get("dispense_ok"):     events.append("Dispense OK")
        if t.get("retract_counter"): events.append(f"Retract counter {t['retract_counter']}")
        if t.get("present_timeout"): events.append("Present timeout")
        if t.get("present_failed"):  events.append("Present failed")
        for err in t.get("device_errors", []):
            events.append(f"DeviceError {err['device']} nr={err['error_nr']} class={err['err_class']}")
        if t.get("cancelled"):       events.append("Customer cancelled")
        if t.get("txn_timeout"):     events.append("Transaction timeout")
        return events

    @staticmethod
    def _compact(record: dict) -> dict:
        """Remove None, False, and empty list/string keys to reduce token count."""
        return {
            k: v for k, v in record.items()
            if v is not None and v is not False and v != [] and v != ""
        }


# ═══════════════════════════════════════════════════════════════
#  3. TransactionMergerService
# ═══════════════════════════════════════════════════════════════

# JRN-only fields appended to EJ base (never overwrite EJ ground truth).
# NOTE: "uuid" is intentionally excluded — it is used internally for matching
#       but stripped from the final record to save LLM input tokens.
_JRN_ENRICH_FIELDS = (
    "protocol_steps",
    "account_preference",
    "preferences_restored",
    "card_ejected",
    "app_state_start",
    "app_state_end",
    "response_code",
    "stan",
    "device_errors",
)


class TransactionMergerService:
    """
    FUNCTION:
        TransactionMergerService

    DESCRIPTION:
        Matches EJ transactions to JRN transactions by timestamp-range
        overlap, then merges them into a single enriched record.

        Matching strategy (two-tier, in priority order):
          1. UUID exact match — if EJ txn_number and JRN uuid are identical,
             the pair is matched immediately with high confidence.
          2. Timestamp-range overlap — fallback when UUID is absent in either
             record. Picks the JRN record with the greatest overlap duration.
             A skew_seconds buffer handles minor clock differences.

        Merge rules:
          - EJ fields are ground truth (txn_number, pan, amount, end_state).
          - JRN-only fields are appended where they add new information.
          - uuid is used for matching only and stripped from the final record
            to save LLM input tokens.
          - Duplicate events (cancel, timeout) are deduplicated.
          - Unmatched EJ transactions → source: "ej_only".
          - Unmatched JRN transactions → source: "jrn_only".
          - Matched pairs → source: "ej+jrn".

    USAGE:
        preprocessor = LogPreprocessorService()
        merger       = TransactionMergerService()

        ej_records   = preprocessor.preprocess_ej(raw_ej)
        jrn_records  = preprocessor.preprocess_jrn(raw_jrn)
        merged       = merger.merge(ej_records, jrn_records)

        prompt = preprocessor.build_prompt(merged, atm_id="DN_ATM_2043")
    """

    def merge(
        self,
        ej_records:   List[dict],
        jrn_records:  List[dict],
        skew_seconds: int = 2
    ) -> List[dict]:
        """
        FUNCTION:
            merge

        DESCRIPTION:
            Matches EJ and JRN records by timestamp-range overlap and merges
            matched pairs. Unmatched records are included with source labels.

        PARAMETERS:
            ej_records   (List[dict]) : Output of LogPreprocessorService.preprocess_ej().
            jrn_records  (List[dict]) : Output of LogPreprocessorService.preprocess_jrn().
            skew_seconds (int)        : Clock-skew tolerance in seconds (default 2).

        RETURNS:
            List[dict] : Merged records sorted by ts_start.
                         Each record has "source": "ej+jrn" | "ej_only" | "jrn_only".
        """
        logger.info(
            f"Merging {len(ej_records)} EJ + {len(jrn_records)} JRN records."
        )

        matched_jrn: set = set()
        merged: List[dict] = []

        for ej in ej_records:
            best_idx = self._find_best_jrn_match(
                ej, jrn_records, matched_jrn, skew_seconds
            )
            if best_idx is not None:
                matched_jrn.add(best_idx)
                merged.append(self._merge_pair(ej, jrn_records[best_idx]))
                logger.debug(
                    f"Matched EJ {ej.get('txn_number','?')} "
                    f"({ej.get('ts_start')}–{ej.get('ts_end')}) "
                    f"→ JRN uuid={jrn_records[best_idx].get('uuid','?')}"
                )
            else:
                ej_copy = dict(ej)
                ej_copy["source"] = "ej_only"
                merged.append(ej_copy)
                logger.debug(f"Unmatched EJ {ej.get('txn_number','?')}")

        # Append unmatched JRN records
        for idx, jrn in enumerate(jrn_records):
            if idx not in matched_jrn:
                jrn_copy = dict(jrn)
                jrn_copy["source"] = "jrn_only"
                merged.append(jrn_copy)
                logger.debug(f"Unmatched JRN uuid={jrn.get('uuid','?')}")

        merged.sort(key=lambda r: r.get("ts_start") or "")
        logger.info(f"Merge complete. {len(merged)} total records.")
        return merged

    # ── Private ───────────────────────────────────────────────

    @staticmethod
    def _find_best_jrn_match(
        ej: dict,
        jrn_records: List[dict],
        already_matched: set,
        skew_seconds: int
    ) -> Optional[int]:
        """
        Two-tier matching strategy:

        Tier 1 — UUID exact match (high confidence):
            EJ stores the transaction number in txn_number.
            JRN stores the same value in uuid (parsed from UUID: <...> lines).
            If both are present and identical, return immediately — no
            timestamp scoring needed.

        Tier 2 — Timestamp-range overlap (fallback):
            Used when UUID is absent in either record (partial logs, older
            firmware). Scores all candidates by overlap duration and returns
            the best. A skew_seconds buffer handles minor clock differences.

        Returns index into jrn_records, or None if no match found.
        """
        ej_txn_number = ej.get("txn_number")
        ej_start      = _to_dt(ej.get("ts_start"))

        if ej_start is None:
            return None

        ej_end = _to_dt(ej.get("ts_end")) or (ej_start + timedelta(seconds=120))
        skew   = timedelta(seconds=skew_seconds)

        # ── Tier 1: UUID exact match ──────────────────────────
        if ej_txn_number:
            for idx, jrn in enumerate(jrn_records):
                if idx in already_matched:
                    continue
                if jrn.get("uuid") == ej_txn_number:
                    logger.debug(
                        f"UUID match: EJ txn_number={ej_txn_number} "
                        f"-> JRN idx={idx}"
                    )
                    return idx

        # ── Tier 2: Timestamp-range overlap ───────────────────
        best_idx     = None
        best_overlap = timedelta(seconds=-1)

        for idx, jrn in enumerate(jrn_records):
            if idx in already_matched:
                continue

            jrn_start = _to_dt(jrn.get("ts_start"))
            if jrn_start is None:
                continue
            jrn_end = _to_dt(jrn.get("ts_end")) or (jrn_start + timedelta(seconds=120))

            if not _ranges_overlap(
                ej.get("ts_start"),  ej.get("ts_end"),
                jrn.get("ts_start"), jrn.get("ts_end"),
                skew_seconds
            ):
                continue

            overlap = (
                min(ej_end + skew, jrn_end + skew) -
                max(ej_start - skew, jrn_start - skew)
            )
            if overlap > best_overlap:
                best_overlap = overlap
                best_idx     = idx

        if best_idx is not None:
            logger.debug(
                f"Timestamp match: EJ txn_number={ej_txn_number or '?'} "
                f"-> JRN idx={best_idx} overlap={best_overlap.seconds}s"
            )

        return best_idx

    @staticmethod
    def _merge_pair(ej: dict, jrn: dict) -> dict:
        """
        Merge a matched EJ+JRN pair.
        EJ is ground truth. JRN enrichment fields are appended.
        uuid is intentionally excluded from enrichment — used for matching
        only, never sent to the LLM to save input tokens.
        Events are merged and deduplicated.
        JRN status is kept as a secondary signal only when it differs.
        """
        merged = dict(ej)
        merged["source"] = "ej+jrn"

        # Append JRN-only fields not already in EJ (uuid excluded)
        for field in _JRN_ENRICH_FIELDS:
            val = jrn.get(field)
            if val and field not in merged:
                merged[field] = val

        # Merge and deduplicate events
        ej_events = set(merged.get("events", []))
        for ev in jrn.get("events", []):
            if ev == "Customer cancelled" and "Customer cancelled" in ej_events:
                continue
            if ev == "Transaction timeout" and "Timeout" in ej_events:
                continue
            if ev not in ej_events:
                merged.setdefault("events", []).append(ev)
                ej_events.add(ev)

        # Keep JRN status as secondary signal if it adds information
        jrn_status = jrn.get("status")
        if jrn_status and jrn_status not in ("Normal", merged.get("status")):
            merged["jrn_status"] = jrn_status

        return merged