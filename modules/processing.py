from pathlib import Path
from typing import Dict, List, Optional, Tuple
from collections import defaultdict
from datetime import datetime, timedelta
from .schemas import FileCategorizationResponse, CategoryCount
from modules.logging_config import logger
import logging
import re
import json


logger.info("Starting processing_service")


# ═══════════════════════════════════════════════════════════════
#  Shared utilities
# ═══════════════════════════════════════════════════════════════

def _to_dt(ts):
    if not ts:
        return None
    try:
        return datetime.strptime(ts, "%H:%M:%S")
    except ValueError:
        return None


def _ranges_overlap(a_start, a_end, b_start, b_end, skew_seconds=2):
    a0 = _to_dt(a_start)
    b0 = _to_dt(b_start)
    if a0 is None or b0 is None:
        return False
    skew = timedelta(seconds=skew_seconds)
    a1 = _to_dt(a_end) or (a0 + timedelta(seconds=120))
    b1 = _to_dt(b_end) or (b0 + timedelta(seconds=120))
    return (a0 - skew) <= (b1 + skew) and (b0 - skew) <= (a1 + skew)


# ═══════════════════════════════════════════════════════════════
#  EJ (Customer Journal) regex constants
# ═══════════════════════════════════════════════════════════════

_LINE_RE     = re.compile(r'^\s*(\d{2}:\d{2}:\d{2})\s+(\d+)\s+(.*)$')
_STAR_SEP_RE = re.compile(r'^\s*\*{3,}\s*$')

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
_EJ_FUNC_MAP   = {
    "CIN/CI": "Cash Deposit", "COUT/GA": "Cash Withdrawal",
    "BAL/BI": "Balance Inquiry", "PINCHG/CH": "Pin Change",
    "PINAUTH/PV": "Pin Verification", "VCPPREFS/PR": "Preferences",
    "COUTFAST": "Fast Cash", "TRA": "Transfer", "BILLPAY": "Bill Payment",
}


# ═══════════════════════════════════════════════════════════════
#  JRN (UI Journal) regex constants
# ═══════════════════════════════════════════════════════════════

_JRN_START_CODES  = {"3207", "3239"}
_JRN_END_CODE     = "3202"
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
_JRN_HOST_CANC_RE = re.compile(r'Transaction canceled by host', re.I)
_JRN_TIMEOUT_RE   = re.compile(r'Transaction timed out', re.I)
_JRN_EMV_DECLINE_CODES = {'3960'}
_JRN_EMV_AAC_CODES     = {'3961'}
# Host decline reason patterns extracted from 41005 screen payload
_JRN_HOST_DECLINE_RE   = re.compile(
    r'(SORRY[^\x00-\x1f]{5,}'
    r'|UNABLE TO PERFORM[^\x00-\x1f]{0,80}'
    r'|AMOUNT IS LESS THAN[^\x00-\x1f]{0,80}'
    r'|INSUFFICIENT[^\x00-\x1f]{0,80}'
    r'|CARD NOT ACCEPTED[^\x00-\x1f]{0,80}'
    r'|TRANSACTION NOT ALLOWED[^\x00-\x1f]{0,80}'
    r'|MINIMUM[^\x00-\x1f]{0,80}'
    r'|MAXIMUM[^\x00-\x1f]{0,80})',
    re.I
)
_JRN_APP_STATE_RE = re.compile(r'Application state is:\s*(\w+\s*\(\d+\))', re.I)
_JRN_PREF_RE      = re.compile(r'Fastcash Account preference selection changed to\s+(.*)', re.I)
_JRN_PREF_REST_RE = re.compile(r'Preferences restored', re.I)
_JRN_CARD_EJCT_RE = re.compile(r'Card ejected and taken', re.I)
_JRN_FUNC_MAP     = {
    ('PINAUTH', 'PV'): "Pin Verification",
    ('COUT',    'GA'): "Cash Withdrawal",
    ('BAL',     'BI'): "Balance Inquiry",
    ('CIN',     'CI'): "Cash Deposit",
    ('PINCHG',  'CH'): "Pin Change",
    ('VCPPREFS','PR'): "Preferences",
    ('COUTFAST', None): "Fast Cash",
    ('TRA',      None): "Transfer",
    ('BILLPAY',  None): "Bill Payment",
}

_JRN_INTERNAL      = {"_dev_err_keys", "_pending_req"}
_JRN_ENRICH_FIELDS = (
    "protocol_steps", "account_preference", "preferences_restored",
    "card_ejected", "app_state_start", "app_state_end",
    "response_code", "stan", "device_errors",
)


# ═══════════════════════════════════════════════════════════════
#  JRN helper functions
# ═══════════════════════════════════════════════════════════════

def _resolve_jrn_func(func, host):
    if not func:
        return "Unknown"
    key = (func.upper(), host.upper() if host else None)
    if key in _JRN_FUNC_MAP:
        return _JRN_FUNC_MAP[key]
    for (f, h), name in _JRN_FUNC_MAP.items():
        if f == func.upper() and h is None:
            return name
    return f"{func}/{host}" if host else func


def _decode_raw_jrn(raw):
    decoded = (raw
               .replace('\\0a', '\n').replace('\x0a', '\n')
               .replace('\\1c', '|').replace('\x1c', '|')
               .replace('\\1d', '|').replace('\x1d', '|')
               .replace('\\0f', ' ').replace('\x0f', ' ')
               .replace('\\1b', ' ').replace('\x1b', ' '))
    result = {}
    for pattern, key in (
        (_JRN_RESP_CODE_RE, 'response_code'),
        (_JRN_STAN_RE,      'stan'),
        (_JRN_REQ_AMT_RE,   'amount'),
    ):
        m = pattern.search(decoded)
        if m:
            result[key] = m.group(1).strip()
    # Extract host decline reason from screen payload text
    dm = _JRN_HOST_DECLINE_RE.search(decoded)
    if dm:
        # Collapse whitespace / control chars left by escape replacement
        reason = re.sub(r'\s+', ' ', dm.group(1)).strip()
        result['host_decline_reason'] = reason
    return result


# ═══════════════════════════════════════════════════════════════
#  Service classes
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

    PARAMETERS:
        None

    RETURNS:
        None

    RAISES:
        None
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

        USAGE:
            result = self.prepare_response(file_categories, extract_path)

        PARAMETERS:
            file_categories (Dict[str, List[str]]) :
                Dictionary where key = category name,
                value = list of associated file names.
            extract_path (Path) :
                Path where files were extracted.

        RETURNS:
            FileCategorizationResponse :
                Includes total files, extraction path, and categorized file details.

        RAISES:
            None
        """
        logger.info("Preparing final file categorization response.")

        # Calculate totals
        total_files = sum(len(files) for files in file_categories.values())
        logger.debug(f"Total files counted: {total_files}")

        # Create category counts
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


class LogPreprocessorService:
    """
    FUNCTION:
        LogPreprocessorService

    DESCRIPTION:
        Parses raw ATM log text (EJ / Customer Journal and JRN / UI Journal)
        into structured transaction records. Also builds a token-efficient
        LLM prompt from a merged record list.

    USAGE:
        svc = LogPreprocessorService()
        ej_records  = svc.preprocess_ej(raw_ej_text)
        jrn_records = svc.preprocess_jrn(raw_jrn_text)
        prompt      = svc.build_prompt(merged_records, atm_id="ATM_01")

    PARAMETERS:
        None

    RETURNS:
        None

    RAISES:
        None
    """

    def preprocess_ej(self, raw_log: str) -> List[dict]:
        """
        FUNCTION:
            preprocess_ej

        DESCRIPTION:
            Parses a raw EJ (Customer Journal) log string into a list of
            compact transaction dicts. Each dict contains timing, type,
            amount, account, status, and key events — PII (PAN) is stripped.

        USAGE:
            records = svc.preprocess_ej(raw_log_text)

        PARAMETERS:
            raw_log (str) : Full text content of the EJ log file.

        RETURNS:
            List[dict] : One dict per detected transaction.

        RAISES:
            None
        """
        lines = raw_log.splitlines()
        transactions = []
        current = None

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
                    "ts_start": ts, "ts_end": None, "txn_number": None,
                    "pan": None, "type": "Unknown", "account": None,
                    "amount": None, "currency": None, "notes": None,
                    "pin_entered": False, "card_returned": False,
                    "state": None, "end_state": None, "events": [],
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
        return [self._compact(t) for t in transactions]

    def preprocess_jrn(self, raw_log: str) -> List[dict]:
        """
        FUNCTION:
            preprocess_jrn

        DESCRIPTION:
            Parses a raw JRN (UI Journal) log string into a list of compact
            transaction dicts. Extracts protocol steps, device errors,
            host response codes, STAN, dispense/retract events, and more.

        USAGE:
            records = svc.preprocess_jrn(raw_log_text)

        PARAMETERS:
            raw_log (str) : Full text content of the JRN log file.

        RETURNS:
            List[dict] : One dict per detected transaction.

        RAISES:
            None
        """
        lines = raw_log.splitlines()
        transactions = []
        current = None
        pending_device = None

        def _flush():
            nonlocal current
            if current:
                current["status"] = self._derive_jrn_status(current)
                current["events"] = self._build_jrn_events(current)
                transactions.append(current)
            current = None

        def _new_txn(ts):
            return {
                "ts_start": ts, "ts_end": None, "uuid": None,
                "type": "Unknown", "app_state_start": None, "app_state_end": None,
                "protocol_steps": [], "account_preference": None,
                "preferences_restored": False, "card_ejected": False,
                "response_code": None, "stan": None, "amount": None,
                "dispense_ok": False, "retract_counter": None,
                "present_timeout": False, "present_failed": False,
                "device_errors": [], "_dev_err_keys": set(),
                "_pending_req": {}, "cancelled": False, "host_cancelled": False,
                "txn_timeout": False, "host_decline_reason": None,
                "emv_declined": False, "emv_aac_requested": False,
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
                                "device": pending_device,
                                "error_nr": enm.group(1),
                                "err_class": enm.group(2),
                            })
                    pending_device = None
                if current:
                    if _JRN_CANCEL_RE.search(line):    current["cancelled"]      = True
                    if _JRN_HOST_CANC_RE.search(line): current["host_cancelled"] = True
                    if _JRN_TIMEOUT_RE.search(line):   current["txn_timeout"]    = True
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

            if uuid_m := _JRN_UUID_RE.search(msg):
                _uuid_val = uuid_m.group(1).strip()
                if _uuid_val:  # skip empty <> from 3201 start line; keep first real UUID
                    current["uuid"] = _uuid_val
            if app_m := _JRN_APP_STATE_RE.search(msg):
                state_val = app_m.group(1).strip()
                if not current["app_state_start"]:
                    current["app_state_start"] = state_val
                else:
                    current["app_state_end"] = state_val
            if code == "1042":
                if req_m := re.search(r'Request\s+(\w+)\s+\((\w+)\)\s+sent to\s+(\S+)', msg, re.I):
                    current["_pending_req"][req_m.group(1)] = req_m.group(2)
            elif code == "1043":
                if resp_m := re.search(
                    r'(\S+)\s+returned\s+(\w+(?:\s+\w+)*)\s+\(\d+\)\s+for request\s+(\w+)', msg, re.I
                ):
                    req_name = resp_m.group(3)
                    result   = resp_m.group(2).strip()
                    func_ctx = current["_pending_req"].pop(req_name, "?")
                    step     = f"{req_name}({func_ctx})→{result}"
                    if step not in current["protocol_steps"]:
                        current["protocol_steps"].append(step)
            if func_m := _JRN_FUNC_RE.search(msg):
                current["type"] = _resolve_jrn_func(func_m.group(1), func_m.group(2))
            if code == "41005":
                if rrm := _JRN_RAW_RECV_RE.search(line):  # .search — line starts with timestamp
                    fields = _decode_raw_jrn(rrm.group(1))
                    for attr in ("response_code", "stan", "amount"):
                        if fields.get(attr) and current[attr] is None:
                            current[attr] = fields[attr]
                    # Capture host decline reason (e.g. "AMOUNT IS LESS THAN MINIMUM")
                    if fields.get("host_decline_reason") and not current["host_decline_reason"]:
                        current["host_decline_reason"] = fields["host_decline_reason"]
            if pref_m := _JRN_PREF_RE.search(msg):
                current["account_preference"] = pref_m.group(1).strip()
            if _JRN_PREF_REST_RE.search(msg):
                current["preferences_restored"] = True
            if _JRN_CARD_EJCT_RE.search(msg):
                current["card_ejected"] = True
            if _JRN_DISP_OK_RE.search(msg):    current["dispense_ok"]     = True
            if rtm := _JRN_RETRACT_RE.search(msg):
                current["retract_counter"] = f"{rtm.group(1)}→{rtm.group(2)}"
            if _JRN_PRES_TOUT_RE.search(msg):  current["present_timeout"] = True
            if _JRN_PRES_FAIL_RE.search(msg):  current["present_failed"]  = True
            if derr := _JRN_DEV_ERR_RE.search(msg):
                pending_device = derr.group(1).strip()
                if enm := _JRN_ERR_NR_RE.search(msg):
                    key = (pending_device, enm.group(1))
                    if key not in current["_dev_err_keys"]:
                        current["_dev_err_keys"].add(key)
                        current["device_errors"].append({
                            "device": pending_device,
                            "error_nr": enm.group(1),
                            "err_class": enm.group(2),
                        })
                    pending_device = None
            if _JRN_CANCEL_RE.search(msg):     current["cancelled"]      = True
            if _JRN_HOST_CANC_RE.search(msg):  current["host_cancelled"] = True
            if _JRN_TIMEOUT_RE.search(msg):    current["txn_timeout"]    = True
            if code in _JRN_EMV_DECLINE_CODES: current["emv_declined"]   = True
            if code in _JRN_EMV_AAC_CODES:     current["emv_aac_requested"] = True

        _flush()
        for t in transactions:
            t.pop("_dev_err_keys", None)
            t.pop("_pending_req",  None)
        return [self._compact(t) for t in transactions]

    def build_prompt(self, records: List[dict], atm_id: str = "", max_chars: int = 12_000) -> str:
        """
        FUNCTION:
            build_prompt

        DESCRIPTION:
            Serialises a list of merged transaction records into a compact
            LLM-ready prompt string, optionally prefixed with the ATM ID.
            Long payloads are truncated at max_chars with a [TRUNCATED] marker.

        USAGE:
            prompt = svc.build_prompt(merged_records, atm_id="ATM_01")

        PARAMETERS:
            records   (List[dict]) : Merged transaction records.
            atm_id    (str)        : Optional ATM identifier for the prompt header.
            max_chars (int)        : Character limit before truncation (default 12 000).

        RETURNS:
            str : The formatted prompt string.

        RAISES:
            None
        """
        payload = json.dumps(records, indent=2, ensure_ascii=False)
        if len(payload) > max_chars:
            payload = payload[:max_chars] + "\n... [TRUNCATED]"
        atm_line = f"ATM_ID={atm_id}\n" if atm_id else ""
        return (
            f"{atm_line}"
            f"The following is a structured list of transactions extracted from "
            f"the ATM log. Each entry contains only fields relevant to diagnosis.\n\n"
            f"LOG:\n{payload}"
        )

    @staticmethod
    def _derive_ej_status(t: dict) -> str:
        events    = t.get("events", [])
        end_state = t.get("end_state")
        if "Customer cancelled" in events: return "Customer Cancelled"
        if "Timeout" in events:            return "Timeout"
        if end_state == "N" and t.get("amount"): return "Success"
        if end_state == "E":               return "Incomplete"
        return "Unknown"

    @staticmethod
    def _derive_jrn_status(t: dict) -> str:
        if t.get("host_cancelled"):                            return "Host Cancelled"
        if t.get("cancelled"):                                 return "Customer Cancelled"
        if t.get("txn_timeout"):                               return "Timeout"
        if t.get("device_errors") or t.get("present_failed"): return "Error"
        if t.get("retract_counter") or t.get("present_timeout"): return "Retract"
        rc = t.get("response_code", "")
        if rc and rc not in ("0", "00"):                       return "Host Error"
        if t.get("dispense_ok"):                               return "Success"
        return "Normal"

    @staticmethod
    def _build_jrn_events(t: dict) -> List[str]:
        # Events are ordered chronologically so the LLM can correctly
        # sequence what happened. Order matters — specifically:
        #   1. Card ejected (card is gone BEFORE any cash retract)
        #   2. Dispense OK (cash was successfully dispensed)
        #   3. Present failed / timeout (customer did not collect cash)
        #   4. Device errors (on CASH_DISPENSER — not card-related)
        #   5. CASH retract (explicit label — not card retract)
        # Without this ordering the LLM conflates cash retract with card retract.
        events = []
        # Step 1 — Card gone first
        if t.get("card_ejected"):
            events.append("Card ejected and taken by customer")
        # Step 2 — Dispense outcome
        if t.get("dispense_ok"):
            events.append("Dispense OK — cash successfully dispensed to customer slot")
        # Step 3 — Present failure (customer didn't collect)
        if t.get("present_failed"):
            events.append("Present failed — customer did not collect dispensed cash")
        if t.get("present_timeout"):
            events.append("Present timeout — cash not collected within time limit")
        # Step 4 — Device errors (CASH_DISPENSER — not card device)
        for err in t.get("device_errors", []):
            events.append(f"DeviceError {err['device']} nr={err['error_nr']} class={err['err_class']}")
        # Step 5 — CASH retract (explicit label to prevent card/cash confusion)
        if t.get("retract_counter"):
            events.append(
                f"CASH retracted (not card — card was already taken) — "
                f"Retract counter {t['retract_counter']}"
            )
        # Remaining events
        if t.get("host_cancelled"):
            reason = t.get("host_decline_reason", "")
            events.append("Host cancelled" + (f": {reason}" if reason else ""))
        if t.get("cancelled"):         events.append("Customer cancelled")
        if t.get("emv_declined"):      events.append("EMV chip declined transaction")
        if t.get("emv_aac_requested"): events.append("AAC second cryptogram requested")
        if t.get("txn_timeout"):       events.append("Transaction timeout")
        return events

    @staticmethod
    def _compact(record: dict) -> dict:
        # Fields stripped before any record reaches the LLM:
        #   pan, account, account_preference  — card/account PII
        #   amount, currency, notes           — financial amounts / denomination detail
        #   stan                              — host trace/audit number
        #   state, end_state                  — machine codes already encoded in status
        #   emv_declined, emv_aac_requested,  — boolean flags already encoded in events
        #   host_cancelled, cancelled,         — boolean flags already encoded in events/status
        #   txn_timeout, dispense_ok,          — boolean flags already encoded in events/status
        #   present_timeout, present_failed    — boolean flags already encoded in events
        STRIP_FIELDS = {
            "pan",
            "account",
            "account_preference",
            "amount",
            "currency",
            "notes",
            "stan",           # host audit trace number — not diagnostic
            "state",
            "end_state",
        }
        return {
            k: v for k, v in record.items()
            if k not in STRIP_FIELDS
            and v is not None and v is not False and v != [] and v != ""
        }


class TransactionMergerService:
    """
    FUNCTION:
        TransactionMergerService

    DESCRIPTION:
        Merges EJ and JRN records into one unified dict per transaction.

        Matching strategy (two-tier):
          1. UUID exact match  — EJ txn_number == JRN uuid
          2. Timestamp overlap — fallback when UUID is absent

        Merge rules:
          - All EJ fields are included as-is (ground truth).
          - All JRN fields are included.
              * Fields that don't exist in EJ are added directly.
              * Fields that conflict (same key, different meaning) are added
                with a "jrn_" prefix so nothing is lost.
          - Events from both logs are merged and deduplicated.
          - Unmatched records are still included but carry a
            "match_warning" field explaining why they are partial.

    USAGE:
        merger = TransactionMergerService()
        merged = merger.merge(ej_records, jrn_records)

    PARAMETERS:
        None

    RETURNS:
        None

    RAISES:
        None
    """

    def merge(
        self,
        ej_records: List[dict],
        jrn_records: List[dict],
        skew_seconds: int = 2
    ) -> List[dict]:
        """
        FUNCTION:
            merge

        DESCRIPTION:
            Merges a list of EJ records with a list of JRN records using
            UUID matching (tier 1) and timestamp-overlap matching (tier 2).
            Unmatched records from either source are appended as-is.

        USAGE:
            merged = merger.merge(ej_records, jrn_records)

        PARAMETERS:
            ej_records   (List[dict]) : Records produced by preprocess_ej.
            jrn_records  (List[dict]) : Records produced by preprocess_jrn.
            skew_seconds (int)        : Clock-skew tolerance in seconds (default 2).

        RETURNS:
            List[dict] : Unified, deduplicated, time-sorted transaction records.

        RAISES:
            None
        """
        matched_jrn = set()
        merged = []

        for ej in ej_records:
            best_idx = self._find_best_jrn_match(
                ej, jrn_records, matched_jrn, skew_seconds
            )
            if best_idx is not None:
                matched_jrn.add(best_idx)
                merged.append(
                    self._merge_pair(ej, jrn_records[best_idx])
                )
            else:
                merged.append(dict(ej))

        for idx, jrn in enumerate(jrn_records):
            if idx not in matched_jrn:
                merged.append({
                    k: v for k, v in jrn.items()
                    if k not in _JRN_INTERNAL
                })

        merged.sort(key=lambda r: r.get("ts_start") or "")
        return merged

    @staticmethod
    def _find_best_jrn_match(
        ej: dict,
        jrn_records: List[dict],
        already_matched: set,
        skew_seconds: int
    ) -> Optional[int]:
        """
        Tier 1: UUID exact match (EJ txn_number == JRN uuid).
        Tier 2: Timestamp-range overlap with clock-skew tolerance.
        Returns index into jrn_records, or None.
        """
        ej_txn_number = ej.get("txn_number")
        ej_start      = _to_dt(ej.get("ts_start"))
        if ej_start is None:
            return None
        ej_end = _to_dt(ej.get("ts_end")) or (ej_start + timedelta(seconds=120))
        skew   = timedelta(seconds=skew_seconds)

        # Tier 1 — UUID
        if ej_txn_number:
            for idx, jrn in enumerate(jrn_records):
                if idx in already_matched:
                    continue
                if jrn.get("uuid") == ej_txn_number:
                    return idx

        # Tier 2 — Timestamp overlap
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
                ej.get("ts_start"), ej.get("ts_end"),
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
        return best_idx

    @staticmethod
    def _merge_pair(ej: dict, jrn: dict) -> dict:
        """
        Combine one EJ + one JRN record into a single token-efficient dict.

        Deduplication rules (to minimise LLM input tokens):
          - jrn ts_start/ts_end   dropped  — identical to EJ timestamps after matching
          - jrn uuid              dropped  — same value as txn_number
          - jrn type              dropped  — same function code, already in EJ type
          - jrn status            dropped  — events list already carries the detail
          - dispense_ok (bool)    dropped  — implied by status=Success + Dispense OK event
          - cancelled   (bool)    dropped  — implied by status=Customer Cancelled + event
          - present_failed (bool) dropped  — already in events as "Present failed"
          - txn_timeout  (bool)   dropped  — already in events as "Transaction timeout"
          - state / end_state     dropped  — already encoded in status
          - Events are merged and deduplicated across both logs.
          - All remaining JRN-only fields (protocol_steps, device_errors,
            response_code, stan, account_preference, app_state_*,
            retract_counter, present_timeout, card_ejected,
            preferences_restored) are kept — they add unique context.
        """
        _JRN_DROP_AFTER_MERGE = {
            "ts_start", "ts_end",
            "uuid",
            "type",
            "status",
            # Boolean flags — already encoded in events/status, drop to save tokens
            "dispense_ok",
            "cancelled",
            "host_cancelled",
            "present_failed",
            "txn_timeout",
            "emv_declined",
            "emv_aac_requested",
            "present_timeout",
            # Sensitive fields — stripped by _compact per-record;
            # also blocked here so they cannot re-enter through JRN fold-in.
            "amount",
            "stan",
            "account_preference",
        }
        _EJ_DROP = {"state", "end_state"}

        result = {k: v for k, v in ej.items() if k not in _EJ_DROP}

        # Merge events — deduplicate across both logs
        ej_events  = list(result.get("events", []))
        seen       = set(ej_events)
        _EJ_TO_JRN = {"Customer cancelled": "Customer cancelled",
                      "Timeout": "Transaction timeout"}
        skip_jrn   = {_EJ_TO_JRN[e] for e in ej_events if e in _EJ_TO_JRN}
        for ev in jrn.get("events", []):
            if ev not in seen and ev not in skip_jrn:
                ej_events.append(ev)
                seen.add(ev)
        result["events"] = ej_events if ej_events else result.pop("events", None)

        # Fold in JRN-only fields
        for key, val in jrn.items():
            if key in _JRN_INTERNAL or key in _JRN_DROP_AFTER_MERGE or key == "events":
                continue
            if val and key not in result:
                result[key] = val

        # Final cleanup — remove any None/False/empty that crept in
        return {k: v for k, v in result.items()
                if v is not None and v is not False and v != [] and v != ""}