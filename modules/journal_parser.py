"""
journal_parser.py
=================
Single-responsibility module for all JOURNAL folder .jrn file operations.

Responsibilities:
    1. Match JOURNAL files to CUSTOMER files by date stem
    2. Parse JOURNAL .jrn into structured UI event DataFrame
    3. Extract diagnostic context (ErrorNr, TDR steps, device states,
       card events, app state) for a given transaction time window

Public API:
    match_journal_file(customer_file, ui_journal_files)
        -> str | None

    parse_journal(file_path)
        -> pd.DataFrame

    extract_diagnostic_context(file_path, start_time_str, end_time_str)
        -> dict

Both transaction_analyzer.py and routes.py import from this module.
ui_journal_processor.py continues to exist for screen-flow features
but diagnostic extraction is centralised here.
"""

import re
import json
import pandas as pd
from datetime import datetime, time as dtime
from pathlib import Path
from typing import Union, List, Optional, Dict

from modules.logging_config import logger


# ──────────────────────────────────────────────────────────────────────────────
# 1. MATCH — pair a CUSTOMER .jrn file with the correct JOURNAL .jrn file
# ──────────────────────────────────────────────────────────────────────────────

def match_journal_file(
    customer_file: Union[str, Path],
    ui_journal_files: List[Union[str, Path]]
) -> Optional[str]:
    """
    FUNCTION: match_journal_file

    DESCRIPTION:
        Given a CUSTOMER journal file path and a list of JOURNAL folder file
        paths, returns the JOURNAL file whose date stem matches the customer
        file's date stem (e.g. "20250909.jrn" → "20250909").

        Falls back to returning None when no exact match exists so callers
        can decide whether to use all available files or skip enrichment.

    USAGE:
        jrn_path = match_journal_file(
            "CUSTOMER/20250909.jrn",
            ["JOURNAL/20250908.jrn", "JOURNAL/20250909.jrn"]
        )
        # → "JOURNAL/20250909.jrn"

    PARAMETERS:
        customer_file    (str | Path)       : Path to a CUSTOMER .jrn file.
        ui_journal_files (List[str | Path]) : Paths to JOURNAL folder files.

    RETURNS:
        str | None : Best-matching JOURNAL file path, or None if no match.

    RAISES:
        None
    """
    customer_stem = Path(customer_file).stem  # e.g. "20250909"

    # Exact stem match first
    for jf in ui_journal_files:
        if Path(jf).stem == customer_stem:
            logger.info(
                "match_journal_file: exact match %s → %s",
                customer_stem, Path(jf).name
            )
            return str(jf)

    logger.warning(
        "match_journal_file: no exact match for stem '%s' in %s",
        customer_stem, [Path(f).name for f in ui_journal_files]
    )
    return None


# ──────────────────────────────────────────────────────────────────────────────
# 2. PARSE — turn a JOURNAL .jrn file into a structured DataFrame
#    (screen-flow events only — same contract as parse_ui_journal in
#     ui_journal_processor.py so both can be used interchangeably)
# ──────────────────────────────────────────────────────────────────────────────

def parse_journal(file_path: Union[str, Path]) -> pd.DataFrame:
    """
    FUNCTION: parse_journal

    DESCRIPTION:
        Parses a JOURNAL folder .jrn file and returns a DataFrame of UI
        screen-flow events (result / action entries with JSON payloads).
        This is the same data extracted by ui_journal_processor.parse_ui_journal
        but sourced from the centralised journal_parser module.

        Rows that do not match the UI event pattern are silently ignored —
        use extract_diagnostic_context() for the raw diagnostic lines.

    USAGE:
        df = parse_journal("JOURNAL/20250909.jrn")

    PARAMETERS:
        file_path (str | Path) : Path to the JOURNAL .jrn file.

    RETURNS:
        pd.DataFrame : Columns: date, timestamp, id, module, direction,
                       viewid, screen, event_type, raw_json, plus flattened
                       json_* columns from the event payload.
                       Empty DataFrame if no events are found.

    RAISES:
        None
    """
    file_path = Path(file_path)
    logger.info("parse_journal: %s", file_path.name)

    if not file_path.exists() or file_path.is_dir():
        logger.error("parse_journal: file not found or is directory: %s", file_path)
        return pd.DataFrame()

    # ── Regex patterns ────────────────────────────────────────────────────────
    pattern_no_date = re.compile(
        r'^(\d{2}:\d{2}:\d{2})\s+(\d+)\s+(\w+)\s+([<>*])\s+\[(\d+)\]\s+-\s+(\w+)\s+'
        r'(result|action):(.+)$'
    )
    pattern_with_date = re.compile(
        r'^(\d{2}/\d{2}/\d{4})\s+(\d{2}:\d{2}:\d{2})\s+(\d+)\s+(\w+)\s+([<>*])\s+'
        r'\[(\d+)\]\s+-\s+(\w+)\s+(result|action):(.+)$'
    )

    # ── Derive file date from stem (YYYYMMDD) ─────────────────────────────────
    stem = file_path.stem
    dm   = re.search(r'(\d{8})', stem)
    if dm:
        try:
            file_date = datetime.strptime(dm.group(1), '%Y%m%d').strftime('%d/%m/%Y')
        except ValueError:
            file_date = stem
    else:
        file_date = stem

    # ── Pass 1: collect and deduplicate valid event lines ─────────────────────
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

                clean = (
                    f"{file_date} {ts}  {lid} {mod} {direction} "
                    f"[{vid}] - {screen} {etype}:{edata}"
                )
                if clean not in seen:
                    seen.add(clean)
                    cleaned_lines.append(clean)

    except Exception as exc:
        logger.error("parse_journal: read error on %s: %s", file_path, exc)
        return pd.DataFrame()

    if not cleaned_lines:
        logger.warning("parse_journal: no valid event lines in %s", file_path.name)
        return pd.DataFrame()

    # ── Pass 2: parse enriched lines into rows ────────────────────────────────
    rows: List[dict] = []
    for line in cleaned_lines:
        m = pattern_with_date.match(line)
        has_date = True
        if not m:
            m = pattern_no_date.match(line)
            has_date = False
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
            'date':       date,
            'timestamp':  ts,
            'id':         int(lid),
            'module':     mod,
            'direction':  direction,
            'viewid':     int(vid),
            'screen':     screen,
            'event_type': etype,
            'raw_json':   edata,
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

        # Flatten JSON payload
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


# ──────────────────────────────────────────────────────────────────────────────
# 3. DIAGNOSE — extract device-level diagnostic context for one transaction
# ──────────────────────────────────────────────────────────────────────────────

def extract_diagnostic_context(
    file_path: Union[str, Path],
    start_time_str: str,
    end_time_str: str
) -> Dict:
    """
    FUNCTION: extract_diagnostic_context

    DESCRIPTION:
        Scans a JOURNAL .jrn file for raw diagnostic lines that fall within a
        given transaction time window and returns structured data suitable for
        direct inclusion in the LLM prompt.

        Extracts lines that parse_journal (and parse_ui_journal) silently
        ignore — specifically:
            • TDR_ protocol state transitions
            • ErrorNr device errors (inline and continuation-line format)
            • 5011 device state change events
            • Card-related events (3205 unreadable, 3245 read, 3973 tap fail,
              5135 retain counter)
            • 1015 application state lines (before / during / after window)

    USAGE:
        ctx = extract_diagnostic_context(
            "JOURNAL/20250909.jrn",
            "18:27:26",
            "18:28:34"
        )

    PARAMETERS:
        file_path      (str | Path) : Path to the JOURNAL .jrn file.
        start_time_str (str)        : Transaction start time  "HH:MM:SS".
        end_time_str   (str)        : Transaction end time    "HH:MM:SS".

    RETURNS:
        dict :
            {
                "protocol_steps"  : List[str],  # TDR_ transitions
                "device_errors"   : List[str],  # ErrorNr with class/code
                "device_states"   : List[str],  # 5011 state changes
                "card_events"     : List[str],  # card read/tap/unreadable
                "app_state_start" : str | None, # app state at txn start
                "app_state_end"   : str | None, # app state at txn end
            }

    RAISES:
        None — returns an empty-but-valid dict on any error.
    """
    empty: Dict = {
        'protocol_steps':  [],
        'device_errors':   [],
        'device_states':   [],
        'card_events':     [],
        'app_state_start': None,
        'app_state_end':   None,
    }

    file_path = Path(file_path)
    if not file_path.exists():
        logger.warning("extract_diagnostic_context: not found: %s", file_path)
        return empty

    # ── Parse time window ─────────────────────────────────────────────────────
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
            "extract_diagnostic_context: invalid times %s / %s",
            start_time_str, end_time_str
        )
        return empty

    result = {
        'protocol_steps':  [],
        'device_errors':   [],
        'device_states':   [],
        'card_events':     [],
        'app_state_start': None,
        'app_state_end':   None,
    }

    # ── Compiled patterns ─────────────────────────────────────────────────────
    re_line      = re.compile(r'^\s*(\d{2}:\d{2}:\d{2})\s+(\d+)\s+(.*)')
    re_errornr   = re.compile(
        r'ErrorNr:\s*(\d+)\s*\(Class:\s*(\S+)\s+Code:\s*(\S+)'
    )
    re_devstate  = re.compile(
        r'State of .* device (\S+).*changed to:\s*(\S+(?:\s+\(\d+\))?)'
    )
    re_tdr       = re.compile(
        r'(TDR_\w+\(\w+\))\s*[-\u2192>]+\s*(\w+)'
    )
    re_appstate  = re.compile(
        r'Application state is:\s*(\w+\s*\(\d+\))'
    )

    # Message IDs we care about
    CARD_IDS    = {'3205', '3245', '3973', '5135'}
    DEV_ST_ID   = '5011'
    APP_ST_ID   = '1015'

    # State tracking across lines
    last_app_before: Optional[str] = None
    first_app_after: Optional[str] = None
    pending_err:    bool            = False
    pending_err_ts: str             = ''

    try:
        with open(file_path, 'r', encoding='utf-8', errors='ignore') as fh:
            for raw in fh:
                line = raw.strip()
                if not line:
                    pending_err = False
                    continue

                # ── Continuation line: ErrorNr detail (no timestamp prefix) ──
                if pending_err:
                    m_err = re_errornr.search(line)
                    if m_err:
                        result['device_errors'].append(
                            f"{pending_err_ts} ErrorNr:{m_err.group(1)} "
                            f"Class:{m_err.group(2)} Code:{m_err.group(3)}"
                        )
                    pending_err = False
                    continue

                # ── Timestamp line ─────────────────────────────────────────
                m = re_line.match(line)
                if not m:
                    continue

                ts_str = m.group(1)
                msg_id = m.group(2)
                rest   = m.group(3)

                ts = _to_time(ts_str)
                if ts is None:
                    continue

                in_window = (t_start <= ts <= t_end)

                # ── App state — track context before/during/after ──────────
                if msg_id == APP_ST_ID:
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

                # ── TDR protocol steps ─────────────────────────────────────
                for mt in re_tdr.finditer(rest):
                    step = f"{mt.group(1)}\u2192{mt.group(2)}"
                    if step not in result['protocol_steps']:
                        result['protocol_steps'].append(step)

                # ── Device state changes (5011) ────────────────────────────
                if msg_id == DEV_ST_ID:
                    md = re_devstate.search(rest)
                    if md:
                        result['device_states'].append(
                            f"{ts_str} {md.group(1)} \u2192 {md.group(2)}"
                        )

                # ── ErrorNr — inline or flagged for next-line pickup ───────
                if 'ErrorNr:' in rest:
                    me = re_errornr.search(rest)
                    if me:
                        result['device_errors'].append(
                            f"{ts_str} ErrorNr:{me.group(1)} "
                            f"Class:{me.group(2)} Code:{me.group(3)}"
                        )
                    else:
                        pending_err    = True
                        pending_err_ts = ts_str
                elif 'Error notified' in rest:
                    # 5004 lines — ErrorNr detail follows on the next line
                    pending_err    = True
                    pending_err_ts = ts_str

                # ── Card events ────────────────────────────────────────────
                if msg_id in CARD_IDS:
                    clean = re.sub(r'<\w+>', '', rest).strip()
                    result['card_events'].append(f"{ts_str} [{msg_id}] {clean}")

    except Exception as exc:
        logger.error("extract_diagnostic_context: error on %s: %s", file_path, exc)
        return result

    # Fill app state from surrounding context when not found inside window
    if result['app_state_start'] is None and last_app_before:
        result['app_state_start'] = last_app_before
    if first_app_after:
        result['app_state_end'] = first_app_after

    logger.info(
        "extract_diagnostic_context: %s [%s-%s] → "
        "steps=%d errors=%d dev_states=%d card_events=%d",
        file_path.name, start_time_str, end_time_str,
        len(result['protocol_steps']),
        len(result['device_errors']),
        len(result['device_states']),
        len(result['card_events']),
    )
    return result