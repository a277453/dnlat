"""
journal_parser.py
=================
Single-responsibility module for all JOURNAL folder .jrn file operations.

Responsibilities:
    1. Match JOURNAL files to CUSTOMER files by date stem
    2. Parse JOURNAL .jrn into structured UI event DataFrame
    3. Extract diagnostic context for a given transaction time window
       — captures ALL diagnostic signals while masking ALL sensitive data
         before the result is used in any LLM prompt

Sensitive data masked / removed:
    PAN / card number, account number, track-2 data, PIN material,
    EMV cryptogram values (raw hex), amounts (requested / debit /
    balance / fee), raw 41004/41005 hex message bodies, XXXX placeholders.

Diagnostic data kept:
    TRN number, STAN, response code, terminal ID, transaction type,
    host outcome text, TVR/TSI flag patterns (no raw hex), EMV AID,
    cryptogram type (CI=xx), host/chip cancellation codes, card events,
    device state changes, protocol steps, app state.

Public API:
    match_journal_file(customer_file, ui_journal_files)  -> str | None
    parse_journal(file_path)                             -> pd.DataFrame
    extract_diagnostic_context(file_path, start, end)   -> dict
    extract_diagnostic_context_from_content(content, filename, start, end) -> dict
    mask_ej_log(transaction_log)                         -> str
"""

import re
import json
import pandas as pd
from datetime import datetime, time as dtime
from pathlib import Path
from typing import Union, List, Optional, Dict

from modules.logging_config import logger


# ══════════════════════════════════════════════════════════════════════════════
# SECTION A — SENSITIVE DATA MASKING
# ══════════════════════════════════════════════════════════════════════════════

_RE_TRACK2     = re.compile(r'\d{13,19}=\S+')
_RE_PAN        = re.compile(r'\b\d{4}[\s\-]?\d{4}[\s\-]?\d{4}[\s\-]?\d{1,7}\b')
_RE_XMASK      = re.compile(r'X{4,}')
_RE_HEX_BLOCK  = re.compile(r'[0-9A-Fa-f]{20,}')
_RE_AMOUNT     = re.compile(
    r'(?:REQUESTED AMOUNT|DEBIT AMOUNT|ISSUER FEE|A/C BALANCE|CASH AVAILABLE)'
    r':\s*[\d,\.]+',
    re.IGNORECASE
)
_RE_ACCOUNT    = re.compile(r'ACCOUNT:\s*\S+', re.IGNORECASE)
_RE_EMV_TAG    = re.compile(r'(9F26|9F10|9F37|5F24|5F20|57)[=:\s]+[0-9A-Fa-f]{8,}',
                             re.IGNORECASE)


def _mask_line(text: str) -> str:
    """Apply all PII/sensitive-data masking rules to a single text string."""
    text = _RE_TRACK2.sub('[CARD_DATA]', text)
    text = _RE_PAN.sub('[PAN]', text)
    text = _RE_XMASK.sub('[MASKED]', text)
    text = _RE_AMOUNT.sub(lambda m: m.group(0).split(':')[0] + ': [AMOUNT]', text)
    text = _RE_ACCOUNT.sub('ACCOUNT: [MASKED]', text)
    text = _RE_EMV_TAG.sub(lambda m: m.group(1) + ':[EMV_DATA]', text)
    text = _RE_HEX_BLOCK.sub('[HEX_DATA]', text)
    return text


def mask_ej_log(transaction_log: str) -> str:
    """
    FUNCTION: mask_ej_log

    DESCRIPTION:
        Masks all sensitive data in a Customer Journal (EJ) transaction log
        string before it is included in an LLM prompt. Also strips raw
        41004/41005 hex protocol lines (noise to the LLM after diagnostic
        fields are decoded into jrn_context).

    PARAMETERS:
        transaction_log (str) : Full EJ transaction log text.

    RETURNS:
        str : Sanitised log safe for LLM consumption.
    """
    masked_lines = []
    for line in transaction_log.splitlines():
        if re.search(r'\b(41004|41005)\b', line) and (
            '\\1c' in line or 'raw message' in line.lower()
        ):
            continue
        masked_lines.append(_mask_line(line))
    return '\n'.join(masked_lines)


# ══════════════════════════════════════════════════════════════════════════════
# SECTION B — MATCH
# ══════════════════════════════════════════════════════════════════════════════

def match_journal_file(
    customer_file: Union[str, Path],
    ui_journal_files: List[Union[str, Path]]
) -> Optional[str]:
    """
    FUNCTION: match_journal_file

    DESCRIPTION:
        Returns the JOURNAL file whose date stem matches the customer
        file's date stem (e.g. "20250909.jrn" -> "20250909").

    PARAMETERS:
        customer_file    (str | Path)       : Path to a CUSTOMER .jrn file.
        ui_journal_files (List[str | Path]) : JOURNAL folder file paths.

    RETURNS:
        str | None : Best-matching JOURNAL file path, or None.
    """
    customer_stem = Path(customer_file).stem
    for jf in ui_journal_files:
        if Path(jf).stem == customer_stem:
            logger.info("match_journal_file: %s -> %s", customer_stem, Path(jf).name)
            return str(jf)
    logger.warning(
        "match_journal_file: no match for '%s' in %s",
        customer_stem, [Path(f).name for f in ui_journal_files]
    )
    return None


# ══════════════════════════════════════════════════════════════════════════════
# SECTION C — PARSE (screen-flow events)
# ══════════════════════════════════════════════════════════════════════════════

def parse_journal(file_path: Union[str, Path]) -> pd.DataFrame:
    """
    FUNCTION: parse_journal

    DESCRIPTION:
        Parses a JOURNAL .jrn file into a DataFrame of UI screen-flow
        events (result/action entries with JSON payloads). Lines that
        do not match the UI event pattern are ignored — use
        extract_diagnostic_context() for raw diagnostic lines.

    PARAMETERS:
        file_path (str | Path) : Path to the JOURNAL .jrn file.

    RETURNS:
        pd.DataFrame : UI event rows. Empty if no events found.
    """
    file_path = Path(file_path)
    if not file_path.exists() or file_path.is_dir():
        logger.error("parse_journal: not found or directory: %s", file_path)
        return pd.DataFrame()

    pattern_no_date = re.compile(
        r'^(\d{2}:\d{2}:\d{2})\s+(\d+)\s+(\w+)\s+([<>*])\s+\[(\d+)\]\s+-\s+(\w+)\s+'
        r'(result|action):(.+)$'
    )
    pattern_with_date = re.compile(
        r'^(\d{2}/\d{2}/\d{4})\s+(\d{2}:\d{2}:\d{2})\s+(\d+)\s+(\w+)\s+([<>*])\s+'
        r'\[(\d+)\]\s+-\s+(\w+)\s+(result|action):(.+)$'
    )

    stem = file_path.stem
    dm   = re.search(r'(\d{8})', stem)
    file_date = datetime.strptime(dm.group(1), '%Y%m%d').strftime('%d/%m/%Y') if dm else stem

    cleaned_lines: List[str] = []
    seen: set = set()

    try:
        with open(file_path, 'r', encoding='utf-8', errors='ignore') as fh:
            for line in fh:
                line = line.strip()
                if not line:
                    continue
                m = pattern_no_date.match(line)
                if not m:
                    continue
                ts, lid, mod, direction, vid, screen, etype, edata = m.groups()
                if etype not in ('result', 'action'):
                    continue
                if mod == 'GUIDM' and screen != 'DMAuthorization':
                    continue
                try:
                    json.loads(edata)
                except json.JSONDecodeError:
                    continue
                clean = f"{file_date} {ts}  {lid} {mod} {direction} [{vid}] - {screen} {etype}:{edata}"
                if clean not in seen:
                    seen.add(clean)
                    cleaned_lines.append(clean)
    except Exception as exc:
        logger.error("parse_journal: read error on %s: %s", file_path, exc)
        return pd.DataFrame()

    if not cleaned_lines:
        return pd.DataFrame()

    rows: List[dict] = []
    for line in cleaned_lines:
        m = pattern_with_date.match(line)
        has_date = bool(m)
        if not m:
            m = pattern_no_date.match(line)
        if not m:
            continue

        if has_date:
            date, ts, lid, mod, direction, vid, screen, etype, edata = m.groups()
        else:
            ts, lid, mod, direction, vid, screen, etype, edata = m.groups()
            date = None

        try:
            jdata = json.loads(edata)
        except json.JSONDecodeError:
            jdata = {}

        row: dict = {
            'date': date, 'timestamp': ts, 'id': int(lid), 'module': mod,
            'direction': direction, 'viewid': int(vid), 'screen': screen,
            'event_type': etype, 'raw_json': edata,
        }

        if date:
            try:
                dobj = datetime.strptime(date, '%d/%m/%Y')
                row['date_formatted'] = dobj.strftime('%Y-%m-%d')
                row['day_of_week']    = dobj.strftime('%A')
            except ValueError:
                row['date_formatted'] = date
                row['day_of_week']    = None
        else:
            row['date_formatted'] = None
            row['day_of_week']    = None

        for k, v in jdata.items():
            if isinstance(v, (int, float)):
                row[f'json_{k}'] = v
            elif isinstance(v, str):
                try:
                    row[f'json_{k}'] = int(v) if '.' not in v else float(v)
                except ValueError:
                    row[f'json_{k}'] = v
            else:
                row[f'json_{k}'] = str(v)

        rows.append(row)

    df = pd.DataFrame(rows)
    logger.info("parse_journal: %d events from %s", len(df), file_path.name)
    return df


# ══════════════════════════════════════════════════════════════════════════════
# SECTION D — RECEIPT BLOCK DECODER (internal helper)
# ══════════════════════════════════════════════════════════════════════════════

def _decode_receipt_block(raw_msg: str) -> dict:
    """
    Decodes the \\0a-delimited receipt text embedded in 41005 raw messages.
    Returns only diagnostic fields — all sensitive fields are skipped.
    """
    fields: dict = {}
    receipt_lines = re.split(r'\\0a|\n', raw_msg)

    for rline in receipt_lines:
        rline = rline.strip()
        if not rline:
            continue

        m = re.match(r'TRANSACTION:\s*(.+)', rline, re.IGNORECASE)
        if m:
            fields['transaction_type'] = m.group(1).strip()
            continue

        m = re.match(r'TRN\s+NUMBER:\s*(\S+)', rline, re.IGNORECASE)
        if m:
            fields['trn_number'] = m.group(1).strip()
            continue

        m = re.match(r'STAN:\s*(\S+)', rline, re.IGNORECASE)
        if m:
            fields['stan'] = m.group(1).strip()
            continue

        m = re.match(r'RESPONSE\s+CODE:\s*(\S+)', rline, re.IGNORECASE)
        if m:
            fields['response_code'] = m.group(1).strip()
            continue

        m = re.match(r'TERMINAL\s+ID:\s*(\S+)', rline, re.IGNORECASE)
        if m:
            fields['terminal_id'] = m.group(1).strip()
            continue

        if re.search(r'\*+\s*UNABLE TO PERFORM REQUEST', rline, re.IGNORECASE):
            fields['host_outcome'] = 'UNABLE TO PERFORM REQUEST'
        elif re.search(r'NO REVERSAL REQUESTED', rline, re.IGNORECASE):
            fields.setdefault('host_notes', []).append('NO REVERSAL REQUESTED')
        elif re.search(r'TRANSACTION APPROVED', rline, re.IGNORECASE):
            fields['host_outcome'] = 'TRANSACTION APPROVED'
        elif re.search(r'TRANSACTION DECLINED', rline, re.IGNORECASE):
            fields['host_outcome'] = 'TRANSACTION DECLINED'
        elif re.search(r'INSUFFICIENT FUNDS', rline, re.IGNORECASE):
            fields['host_outcome'] = 'INSUFFICIENT FUNDS'

    return fields


# ══════════════════════════════════════════════════════════════════════════════
# SECTION E — DIAGNOSTIC CONTEXT EXTRACTION
# ══════════════════════════════════════════════════════════════════════════════

def extract_diagnostic_context(
    file_path: Union[str, Path],
    start_time_str: str,
    end_time_str: str
) -> Dict:
    """
    FUNCTION: extract_diagnostic_context

    DESCRIPTION:
        Scans a JOURNAL .jrn file for all diagnostic signals within the
        given transaction time window. Returns a structured dict with
        pre-masked data safe for direct inclusion in an LLM prompt.

        Categories extracted:

        PROTOCOL & STATE:
            protocol_steps   - TDR_ / TA_EVENT transitions with return codes
            app_state_start  - Application state at/before transaction start
            app_state_end    - Application state at/after transaction end

        HOST COMMUNICATION:
            host_requests    - 6303 request type + key string (no raw hex)
            host_replies     - 6304 reply codes
            host_outcome     - UNABLE TO PERFORM REQUEST / APPROVED / DECLINED
            host_notes       - NO REVERSAL REQUESTED etc
            trn_numbers      - TRN numbers from receipt blocks
            stan_values      - STAN values (non-N/A only)
            response_codes   - Response codes (non-N/A only)
            terminal_id      - Terminal ID (first seen)
            transaction_types- ICC CASH WITHDRAWAL etc from receipt blocks
            uuids            - 6306 transaction UUIDs

        EMV / CHIP:
            emv_events       - 3951-3954 EMV events (PAN stripped, AID kept)
            chip_decision    - 3960/3961/3275 chip decline + AAC type
            tvr_tsi          - 3959 TVR/TSI flag patterns
            cryptogram_info  - 3955/3956 CI values only

        DEVICE & CARD:
            device_errors    - ErrorNr with class/code
            device_states    - 5011 device state changes
            card_events      - 3205/3245/3973/5135 card events
            customer_actions - 3259 customer YES/NO selections

        SENSITIVE DATA MASKED:
            PAN, amounts, account, track-2, raw EMV hex, long hex blocks.

    PARAMETERS:
        file_path      (str | Path) : Path to the JOURNAL .jrn file.
        start_time_str (str)        : Transaction start "HH:MM:SS".
        end_time_str   (str)        : Transaction end   "HH:MM:SS".

    RETURNS:
        dict : All diagnostic fields. Empty-but-valid dict on any error.
    """
    empty: Dict = {
        'protocol_steps': [], 'app_state_start': None, 'app_state_end': None,
        'host_requests': [], 'host_replies': [], 'host_outcome': None,
        'host_notes': [], 'trn_numbers': [], 'stan_values': [],
        'response_codes': [], 'terminal_id': None, 'transaction_types': [],
        'uuids': [], 'emv_events': [], 'chip_decision': [], 'tvr_tsi': None,
        'cryptogram_info': [], 'device_errors': [], 'device_states': [],
        'card_events': [], 'customer_actions': [],
    }

    file_path = Path(file_path)
    if not file_path.exists():
        logger.warning("extract_diagnostic_context: not found: %s", file_path)
        return empty

    def _to_time(s: str) -> Optional[dtime]:
        try:
            p = s.strip().split(':')
            return dtime(int(p[0]), int(p[1]), int(p[2]))
        except Exception:
            return None

    t_start = _to_time(start_time_str)
    t_end   = _to_time(end_time_str)
    if t_start is None or t_end is None:
        logger.warning("extract_diagnostic_context: invalid times %s/%s", start_time_str, end_time_str)
        return empty

    result = {k: ([] if isinstance(v, list) else v) for k, v in empty.items()}

    re_line     = re.compile(r'^\s*(\d{2}:\d{2}:\d{2})\s+(\d+)\s+(.*)')
    re_errornr  = re.compile(r'ErrorNr:\s*(\d+)\s*\(Class:\s*(\S+)\s+Code:\s*(\S+)')
    re_devstate = re.compile(r'State of .* device (\S+).*changed to:\s*(\S+(?:\s+\(\d+\))?)')
    re_tdr      = re.compile(r'(TDR_\w+\(\w+\)|TA_EVENT\(\w+\))\s*[-\u2192>]+\s*(\w+)')
    re_tdr_ret  = re.compile(r'(TDR_\w+|TA_EVENT)\s*\((\w+)\).*returned\s+(\w+)\s*\(\d+\)')
    re_appstate = re.compile(r'Application state is:\s*(\w+\s*\(\d+\))')
    re_uuid     = re.compile(r'TRANSACTION UUID:\s*<([^>]*)>')
    re_ci       = re.compile(r'CI\s*=\s*([0-9A-Fa-f]{2})')
    re_tvr      = re.compile(r'TVR\s*=\s*(\S+).*TSI\s*=\s*(\S+)')

    CARD_IDS     = {'3205', '3245', '3973', '5135'}
    EMV_IDS      = {'3951', '3952', '3953', '3954', '3214'}
    CHIP_DEC_IDS = {'3960', '3961', '3275'}
    CRYPT_IDS    = {'3955', '3956'}

    last_app_before: Optional[str] = None
    first_app_after: Optional[str] = None
    pending_err:     bool           = False
    pending_err_ts:  str            = ''

    try:
        with open(file_path, 'r', encoding='utf-8', errors='ignore') as fh:
            for raw in fh:
                line = raw.strip()
                if not line:
                    pending_err = False
                    continue

                if pending_err:
                    me = re_errornr.search(line)
                    if me:
                        result['device_errors'].append(
                            f"{pending_err_ts} ErrorNr:{me.group(1)} "
                            f"Class:{me.group(2)} Code:{me.group(3)}"
                        )
                    pending_err = False
                    continue

                m = re_line.match(line)
                if not m:
                    continue

                ts_str, msg_id, rest = m.group(1), m.group(2), m.group(3)
                ts = _to_time(ts_str)
                if ts is None:
                    continue

                in_window = (t_start <= ts <= t_end)

                # App state (tracked outside window too)
                if msg_id == '1015':
                    ma = re_appstate.search(rest)
                    if ma:
                        sv = ma.group(1).strip()
                        if ts < t_start:
                            last_app_before = sv
                        elif in_window and result['app_state_start'] is None:
                            result['app_state_start'] = sv
                        elif ts > t_end and first_app_after is None:
                            first_app_after = sv

                if not in_window:
                    continue

                # Protocol steps — prefer 1043 return lines for accuracy
                mt_ret = re_tdr_ret.search(rest)
                if mt_ret:
                    step = f"{mt_ret.group(1)}({mt_ret.group(2)})\u2192{mt_ret.group(3)}"
                    if step not in result['protocol_steps']:
                        result['protocol_steps'].append(step)
                else:
                    for mt in re_tdr.finditer(rest):
                        step = f"{mt.group(1)}\u2192{mt.group(2)}"
                        if step not in result['protocol_steps']:
                            result['protocol_steps'].append(step)

                # Host request (6303)
                if msg_id == '6303':
                    km = re.search(r'\(KEY:\s*([^,)]+)', rest)
                    tm = re.search(r'TRANSACTION REQUEST\s+(\S+)', rest)
                    result['host_requests'].append(
                        f"{ts_str} REQUEST:{tm.group(1).strip() if tm else ''} "
                        f"KEY:{km.group(1).strip() if km else ''}"
                    )

                # Host reply (6304)
                if msg_id == '6304':
                    _cleaned_rest = re.sub(r'\s+', ' ', rest).strip()
                    result['host_replies'].append(
                        f"{ts_str} {_cleaned_rest}"
                    )

                # Transaction UUID (6306)
                if msg_id == '6306':
                    mu = re_uuid.search(rest)
                    if mu:
                        result['uuids'].append(f"{ts_str} UUID:<{mu.group(1)}>")

                # 41005 receipt decode
                if msg_id == '41005' and '\\0a' in rest:
                    receipt = _decode_receipt_block(rest)
                    if receipt.get('transaction_type'):
                        tt = receipt['transaction_type']
                        if tt not in result['transaction_types']:
                            result['transaction_types'].append(tt)
                    if receipt.get('trn_number'):
                        tn = receipt['trn_number']
                        if tn not in result['trn_numbers']:
                            result['trn_numbers'].append(tn)
                    if receipt.get('stan') and receipt['stan'] != 'N/A':
                        sv = receipt['stan']
                        if sv not in result['stan_values']:
                            result['stan_values'].append(sv)
                    if receipt.get('response_code') and receipt['response_code'] != 'N/A':
                        rc = receipt['response_code']
                        if rc not in result['response_codes']:
                            result['response_codes'].append(rc)
                    if receipt.get('terminal_id') and result['terminal_id'] is None:
                        result['terminal_id'] = receipt['terminal_id']
                    if receipt.get('host_outcome'):
                        result['host_outcome'] = receipt['host_outcome']
                    for note in receipt.get('host_notes', []):
                        if note not in result['host_notes']:
                            result['host_notes'].append(note)

                # EMV events (3951-3954, 3214) — strip PAN/crypto
                if msg_id in EMV_IDS:
                    if msg_id == '3954':
                        aid_m = re.search(r"AID\s+'([A-Fa-f0-9]+)'", rest)
                        safe  = f"AID:{aid_m.group(1)}" if aid_m else _mask_line(rest)
                    else:
                        safe = _mask_line(rest)
                    result['emv_events'].append(f"{ts_str} [{msg_id}] {safe}")

                # Chip decision (3960, 3961, 3275)
                if msg_id in CHIP_DEC_IDS:
                    result['chip_decision'].append(
                        f"{ts_str} [{msg_id}] {re.sub(r'  +', ' ', rest).strip()}"
                    )

                # TVR/TSI (3959) — flag patterns only
                if msg_id == '3959':
                    mt = re_tvr.search(rest)
                    if mt:
                        result['tvr_tsi'] = f"TVR={mt.group(1)} TSI={mt.group(2)}"

                # Cryptogram info (3955, 3956) — CI value only
                if msg_id in CRYPT_IDS:
                    mc   = re_ci.search(rest)
                    lbl  = 'FirstGenerateAC' if msg_id == '3955' else 'SecondGenerateAC'
                    result['cryptogram_info'].append(
                        f"{lbl} CI={mc.group(1) if mc else 'unknown'}"
                    )

                # Customer action (3259)
                if msg_id == '3259':
                    result['customer_actions'].append(
                        f"{ts_str} {re.sub(r'  +', ' ', rest).strip()}"
                    )

                # Device state changes (5011)
                if msg_id == '5011':
                    md = re_devstate.search(rest)
                    if md:
                        result['device_states'].append(
                            f"{ts_str} {md.group(1)} \u2192 {md.group(2)}"
                        )

                # ErrorNr
                if 'ErrorNr:' in rest:
                    me = re_errornr.search(rest)
                    if me:
                        result['device_errors'].append(
                            f"{ts_str} ErrorNr:{me.group(1)} "
                            f"Class:{me.group(2)} Code:{me.group(3)}"
                        )
                    else:
                        pending_err, pending_err_ts = True, ts_str
                elif 'Error notified' in rest:
                    pending_err, pending_err_ts = True, ts_str

                # Card events
                if msg_id in CARD_IDS:
                    clean = _mask_line(re.sub(r'<\w+>', '', rest).strip())
                    result['card_events'].append(f"{ts_str} [{msg_id}] {clean}")

    except Exception as exc:
        logger.error("extract_diagnostic_context: error on %s: %s", file_path, exc)
        return result

    if result['app_state_start'] is None and last_app_before:
        result['app_state_start'] = last_app_before
    if first_app_after:
        result['app_state_end'] = first_app_after

    logger.info(
        "extract_diagnostic_context: %s [%s-%s] steps=%d host=%d chip=%d "
        "errors=%d states=%d cards=%d",
        file_path.name, start_time_str, end_time_str,
        len(result['protocol_steps']), len(result['host_requests']),
        len(result['chip_decision']), len(result['device_errors']),
        len(result['device_states']), len(result['card_events']),
    )
    return result


def extract_diagnostic_context_from_content(
    jrn_content: str,
    jrn_filename: str,
    start_time_str: str,
    end_time_str: str
) -> Dict:
    """
    FUNCTION: extract_diagnostic_context_from_content

    DESCRIPTION:
        In-memory version of extract_diagnostic_context.
        Identical diagnostic extraction logic but reads from a string
        (session memory) instead of a file path on disk. This is needed
        because the temp extraction folder is deleted before the LLM
        analysis endpoint runs.

        See extract_diagnostic_context docstring for full category list.

    PARAMETERS:
        jrn_content    (str) : Raw text content of the JOURNAL .jrn file.
        jrn_filename   (str) : Filename (for logging only).
        start_time_str (str) : Transaction start "HH:MM:SS".
        end_time_str   (str) : Transaction end   "HH:MM:SS".

    RETURNS:
        dict : All diagnostic fields. Empty-but-valid dict on any error.
    """
    empty: Dict = {
        'protocol_steps': [], 'app_state_start': None, 'app_state_end': None,
        'host_requests': [], 'host_replies': [], 'host_outcome': None,
        'host_notes': [], 'trn_numbers': [], 'stan_values': [],
        'response_codes': [], 'terminal_id': None, 'transaction_types': [],
        'uuids': [], 'emv_events': [], 'chip_decision': [], 'tvr_tsi': None,
        'cryptogram_info': [], 'device_errors': [], 'device_states': [],
        'card_events': [], 'customer_actions': [],
    }

    if not jrn_content or not jrn_content.strip():
        logger.warning("extract_diagnostic_context_from_content: empty content for %s", jrn_filename)
        return empty

    def _to_time(s: str) -> Optional[dtime]:
        try:
            p = s.strip().split(':')
            return dtime(int(p[0]), int(p[1]), int(p[2]))
        except Exception:
            return None

    t_start = _to_time(start_time_str)
    t_end   = _to_time(end_time_str)
    if t_start is None or t_end is None:
        logger.warning(
            "extract_diagnostic_context_from_content: invalid times %s/%s",
            start_time_str, end_time_str
        )
        return empty

    result = {k: ([] if isinstance(v, list) else v) for k, v in empty.items()}

    re_line     = re.compile(r'^\s*(\d{2}:\d{2}:\d{2})\s+(\d+)\s+(.*)')
    re_errornr  = re.compile(r'ErrorNr:\s*(\d+)\s*\(Class:\s*(\S+)\s+Code:\s*(\S+)')
    re_devstate = re.compile(r'State of .* device (\S+).*changed to:\s*(\S+(?:\s+\(\d+\))?)')
    re_tdr      = re.compile(r'(TDR_\w+\(\w+\)|TA_EVENT\(\w+\))\s*[-\u2192>]+\s*(\w+)')
    re_tdr_ret  = re.compile(r'(TDR_\w+|TA_EVENT)\s*\((\w+)\).*returned\s+(\w+)\s*\(\d+\)')
    re_appstate = re.compile(r'Application state is:\s*(\w+\s*\(\d+\))')
    re_uuid     = re.compile(r'TRANSACTION UUID:\s*<([^>]*)>')
    re_ci       = re.compile(r'CI\s*=\s*([0-9A-Fa-f]{2})')
    re_tvr      = re.compile(r'TVR\s*=\s*(\S+).*TSI\s*=\s*(\S+)')

    CARD_IDS     = {'3205', '3245', '3973', '5135'}
    EMV_IDS      = {'3951', '3952', '3953', '3954', '3214'}
    CHIP_DEC_IDS = {'3960', '3961', '3275'}
    CRYPT_IDS    = {'3955', '3956'}

    last_app_before: Optional[str] = None
    first_app_after: Optional[str] = None
    pending_err:     bool           = False
    pending_err_ts:  str            = ''

    try:
        for raw in jrn_content.splitlines():
            line = raw.strip()
            if not line:
                pending_err = False
                continue

            if pending_err:
                me = re_errornr.search(line)
                if me:
                    result['device_errors'].append(
                        f"{pending_err_ts} ErrorNr:{me.group(1)} "
                        f"Class:{me.group(2)} Code:{me.group(3)}"
                    )
                pending_err = False
                continue

            m = re_line.match(line)
            if not m:
                continue

            ts_str, msg_id, rest = m.group(1), m.group(2), m.group(3)
            ts = _to_time(ts_str)
            if ts is None:
                continue

            in_window = (t_start <= ts <= t_end)

            # App state (tracked outside window too)
            if msg_id == '1015':
                ma = re_appstate.search(rest)
                if ma:
                    sv = ma.group(1).strip()
                    if ts < t_start:
                        last_app_before = sv
                    elif in_window and result['app_state_start'] is None:
                        result['app_state_start'] = sv
                    elif ts > t_end and first_app_after is None:
                        first_app_after = sv

            if not in_window:
                continue

            # Protocol steps
            mt_ret = re_tdr_ret.search(rest)
            if mt_ret:
                step = f"{mt_ret.group(1)}({mt_ret.group(2)})\u2192{mt_ret.group(3)}"
                if step not in result['protocol_steps']:
                    result['protocol_steps'].append(step)
            else:
                for mt in re_tdr.finditer(rest):
                    step = f"{mt.group(1)}\u2192{mt.group(2)}"
                    if step not in result['protocol_steps']:
                        result['protocol_steps'].append(step)

            # Host request (6303)
            if msg_id == '6303':
                km = re.search(r'\(KEY:\s*([^,)]+)', rest)
                tm = re.search(r'TRANSACTION REQUEST\s+(\S+)', rest)
                result['host_requests'].append(
                    f"{ts_str} REQUEST:{tm.group(1).strip() if tm else ''} "
                    f"KEY:{km.group(1).strip() if km else ''}"
                )

            # Host reply (6304)
            if msg_id == '6304':
                _cleaned_rest = re.sub(r'\s+', ' ', rest).strip()
                result['host_replies'].append(
                    f"{ts_str} {_cleaned_rest}"
                )

            # Transaction UUID (6306)
            if msg_id == '6306':
                mu = re_uuid.search(rest)
                if mu:
                    result['uuids'].append(f"{ts_str} UUID:<{mu.group(1)}>")

            # 41005 receipt decode
            if msg_id == '41005' and '\\0a' in rest:
                receipt = _decode_receipt_block(rest)
                if receipt.get('transaction_type'):
                    tt = receipt['transaction_type']
                    if tt not in result['transaction_types']:
                        result['transaction_types'].append(tt)
                if receipt.get('trn_number'):
                    tn = receipt['trn_number']
                    if tn not in result['trn_numbers']:
                        result['trn_numbers'].append(tn)
                if receipt.get('stan') and receipt['stan'] != 'N/A':
                    sv = receipt['stan']
                    if sv not in result['stan_values']:
                        result['stan_values'].append(sv)
                if receipt.get('response_code') and receipt['response_code'] != 'N/A':
                    rc = receipt['response_code']
                    if rc not in result['response_codes']:
                        result['response_codes'].append(rc)
                if receipt.get('terminal_id') and result['terminal_id'] is None:
                    result['terminal_id'] = receipt['terminal_id']
                if receipt.get('host_outcome'):
                    result['host_outcome'] = receipt['host_outcome']
                for note in receipt.get('host_notes', []):
                    if note not in result['host_notes']:
                        result['host_notes'].append(note)

            # EMV events (3951-3954, 3214)
            if msg_id in EMV_IDS:
                if msg_id == '3954':
                    aid_m = re.search(r"AID\s+'([A-Fa-f0-9]+)'", rest)
                    safe  = f"AID:{aid_m.group(1)}" if aid_m else _mask_line(rest)
                else:
                    safe = _mask_line(rest)
                result['emv_events'].append(f"{ts_str} [{msg_id}] {safe}")

            # Chip decision (3960, 3961, 3275)
            if msg_id in CHIP_DEC_IDS:
                result['chip_decision'].append(
                    f"{ts_str} [{msg_id}] {re.sub(r'  +', ' ', rest).strip()}"
                )

            # TVR/TSI (3959)
            if msg_id == '3959':
                mt = re_tvr.search(rest)
                if mt:
                    result['tvr_tsi'] = f"TVR={mt.group(1)} TSI={mt.group(2)}"

            # Cryptogram info (3955, 3956)
            if msg_id in CRYPT_IDS:
                mc   = re_ci.search(rest)
                lbl  = 'FirstGenerateAC' if msg_id == '3955' else 'SecondGenerateAC'
                result['cryptogram_info'].append(
                    f"{lbl} CI={mc.group(1) if mc else 'unknown'}"
                )

            # Customer action (3259)
            if msg_id == '3259':
                result['customer_actions'].append(
                    f"{ts_str} {re.sub(r'  +', ' ', rest).strip()}"
                )

            # Device state changes (5011)
            if msg_id == '5011':
                md = re_devstate.search(rest)
                if md:
                    result['device_states'].append(
                        f"{ts_str} {md.group(1)} \u2192 {md.group(2)}"
                    )

            # ErrorNr
            if 'ErrorNr:' in rest:
                me = re_errornr.search(rest)
                if me:
                    result['device_errors'].append(
                        f"{ts_str} ErrorNr:{me.group(1)} "
                        f"Class:{me.group(2)} Code:{me.group(3)}"
                    )
                else:
                    pending_err, pending_err_ts = True, ts_str
            elif 'Error notified' in rest:
                pending_err, pending_err_ts = True, ts_str

            # Card events
            if msg_id in CARD_IDS:
                clean = _mask_line(re.sub(r'<\w+>', '', rest).strip())
                result['card_events'].append(f"{ts_str} [{msg_id}] {clean}")

    except Exception as exc:
        logger.error("extract_diagnostic_context_from_content: error on %s: %s", jrn_filename, exc)
        return result

    if result['app_state_start'] is None and last_app_before:
        result['app_state_start'] = last_app_before
    if first_app_after:
        result['app_state_end'] = first_app_after

    logger.info(
        "extract_diagnostic_context_from_content: %s [%s-%s] steps=%d host=%d chip=%d "
        "errors=%d states=%d cards=%d",
        jrn_filename, start_time_str, end_time_str,
        len(result['protocol_steps']), len(result['host_requests']),
        len(result['chip_decision']), len(result['device_errors']),
        len(result['device_states']), len(result['card_events']),
    )
    return result