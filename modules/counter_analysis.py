"""
counter_analysis.py  (modules/counter_analysis.py)
-------------------
All Counter Analysis functionality extracted from api/routes.py.

Exposes a single FastAPI router (`counter_router`) with three endpoints:
  GET  /get-matching-sources-for-trc
  POST /get-counter-data
  POST /get-counter-comparison

The router endpoints are registered lazily via init_counter_router(), which
api/routes.py calls after defining require_elevated_role and _resolve_session_id.
This avoids a circular import — counter_analysis never imports from routes.

Usage in api/routes.py:
    from modules.counter_analysis import init_counter_router, counter_router
    init_counter_router(require_elevated_role, _resolve_session_id)
    router.include_router(counter_router)
"""

import re
import traceback
from collections import defaultdict
from datetime import datetime, time as dt_time
from typing import Callable, Optional

import pandas as pd
from fastapi import APIRouter, Depends, Header, HTTPException, Query
from pydantic import BaseModel

from modules.logging_config import logger
from modules.session import session_service

counter_router = APIRouter()

# Populated by init_counter_router() called from api/routes.py
_require_elevated_role: Optional[Callable] = None
_resolve_session_id:    Optional[Callable] = None


async def _rbac_proxy(authorization: str = Header(default=None)):
    """
    Stable proxy for the RBAC dependency.

    Depends(_require_elevated_role) is evaluated at decoration time when
    _require_elevated_role is still None.  This wrapper is defined once at
    import time (so it is always callable), and delegates to the real
    require_elevated_role at request time after init_counter_router() has
    populated the module global.
    """
    if _require_elevated_role is None:
        raise RuntimeError(
            "counter_analysis.init_counter_router() was never called. "
            "Ensure api/routes.py calls it before including the router."
        )
    return await _require_elevated_role(authorization=authorization)


# ── Pydantic request models ──────────────────────────────────────────────────
class CounterDataRequest(BaseModel):
    transaction_id: str
    source_file: str


class CounterComparisonRequest(BaseModel):
    transaction_id: str
    source_file: str
    compare_mode: str  # "first" | "previous"
# ─────────────────────────────────────────────────────────────────────────────


def init_counter_router(
    rbac_dependency: Callable,
    session_resolver: Callable,
) -> None:
    """
    Inject the RBAC dependency and session resolver into this module.

    Must be called from api/routes.py AFTER require_elevated_role and
    _resolve_session_id are defined there, and BEFORE the router is
    included into the main app router.

    The endpoints are defined at module level and read _require_elevated_role
    and _resolve_session_id as module globals set by this function.

    Parameters
    ----------
    rbac_dependency   : the require_elevated_role coroutine from api/routes.py
    session_resolver  : the _resolve_session_id function from api/routes.py
    """
    global _require_elevated_role, _resolve_session_id
    _require_elevated_role = rbac_dependency
    _resolve_session_id    = session_resolver


@counter_router.get("/get-matching-sources-for-trc", dependencies=[Depends(_rbac_proxy)])
async def get_matching_sources_for_trc(session_id: str = Query(default=None)):
    """
        FUNCTION: get_matching_sources_for_trc

        DESCRIPTION:
            Retrieves a list of source files that have corresponding TRC trace files in the current session.
            Matches source file dates with TRC trace file contents to determine availability.

        USAGE:
            response = await get_matching_sources_for_trc(session_id="session_123")

        PARAMETERS:
            session_id (str) : Optional. The session ID to search for matching TRC trace files.
                            Defaults to CURRENT_SESSION_ID if not provided.

        RETURNS:
            dict : A dictionary containing:
                - "matching_sources" (list) : List of source file names that have at least one matching TRC trace file.

        RAISES:
            HTTPException :
                - 404 : If the session with the given ID does not exist
                - 500 : For any unexpected server error during processing
"""

    session_id = _resolve_session_id(session_id)
    try:
        if not session_service.session_exists(session_id):
            raise HTTPException(status_code=404, detail="No session found")

        session_data = session_service.get_session(session_id)

        # All source files stored in session — may include deduplicated variants:
        # e.g. ["20250404", "20250404_1", "20250404_2", "20250405"]
        all_sources = session_data.get('source_files', [])

        # TRC filenames (may also be deduplicated): TRCTRACE.prn, TRCTRACE_1.prn …
        file_categories = session_data.get('file_categories', {})
        trc_trace_files = file_categories.get('trc_trace', [])
        trc_trace_contents = session_data.get('trc_trace_contents', {})

        if not trc_trace_files:
            return {"matching_sources": []}

        # For each source file, check whether its exact YYMMDD date appears on
        # a real timestamp line in ANY of the TRC files in the session.
        #
        # We deliberately do NOT build a global "all dates in all TRCs" set and
        # match sources against it — that would surface sources whose date appears
        # in the TRC body for unrelated reasons (e.g. a counter value that happens
        # to look like a date).  Instead we check per-source, per-TRC, so a source
        # is only included when its date is structurally present as a timestamp.
        #
        # _1/_2 dedup variants of the same date are each checked independently and
        # all included when their shared date matches — they are distinct .jrn files
        # and must appear as separate dropdown entries.
        matching_sources = []
        for source in all_sources:
            yymmdd = _source_stem_to_yymmdd(source)
            if not yymmdd:
                logger.debug(f"[TRC-MATCH] Cannot derive YYMMDD for source={source}")
                continue
            matched_trc = None
            for trc_filename in trc_trace_files:
                trc_content = trc_trace_contents.get(trc_filename, '')
                if _trc_contains_date(trc_content, yymmdd):
                    matched_trc = trc_filename
                    break
            if matched_trc:
                matching_sources.append(source)
                logger.info(f"[TRC-MATCH] {source} (yymmdd={yymmdd}) -> {matched_trc}")
            else:
                logger.debug(f"[TRC-MATCH] No TRC match for source={source} (yymmdd={yymmdd})")

        logger.info(f"[TRC-MATCH] matching_sources: {matching_sources}")
        return {"matching_sources": matching_sources}

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"[TRC-MATCH] error: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Error: {str(e)}")



@counter_router.post("/get-counter-data", dependencies=[Depends(_rbac_proxy)])
async def get_counter_data(
    request: CounterDataRequest,
    session_id: str = Query(default=None)
):
    """
        FUNCTION: get_counter_data

        DESCRIPTION:
            Retrieves counter data from TRC trace files mapped to a specific transaction. 
            Finds the corresponding TRC trace file for the transaction's source file and date, 
            extracts start, first, and last counters, and builds a per-transaction counter table 
            including CIN/CI and COUT/GA transactions. Handles transaction logs to extract 
            denomination information and flags transactions with counters available.

        USAGE:
            response = await get_counter_data(request=CounterDataRequest(transaction_id="TX123", source_file="20250404"))

        PARAMETERS:
            request (CounterDataRequest) : Pydantic model containing:
                - transaction_id (str) : ID of the transaction to retrieve counters for
                - source_file (str)    : Source file associated with the transaction
            session_id (str)           : Optional. Session ID to fetch data from. Defaults to CURRENT_SESSION_ID.

        RETURNS:
            dict : Dictionary containing counter data:
                - "transaction_id" (str)           : Transaction ID
                - "source_file" (str)              : Source file name
                - "all_blocks" (list)              : All counter blocks extracted from TRC files
                - "column_descriptions" (dict)     : Column descriptions for counters
                - "start_counter" (dict)           : First counter in file (static)
                    - "date" (str)
                    - "timestamp" (str)
                    - "counter_data" (list of dict)
                - "first_counter" (dict)           : Counter just before transaction start time (dynamic)
                - "last_counter" (dict)            : Last counter in file (static)
                - "counter_per_transaction" (list of dict) : Summary of each transaction with:
                    - "date_timestamp" (str)
                    - "transaction_id" (str)
                    - "transaction_type" (str)
                    - "transaction_summary" (str)
                    - "transaction_state" (str)
                    - "count" (str)
                    - "counter_summary" (str)
                    - "comment" (str)

        RAISES:
            HTTPException :
                - 400 : No transaction data available or no TRC trace files found
                - 404 : Session not found, transaction not found, or no matching TRC trace file
                - 500 : For any unexpected errors during processing
"""

    session_id = _resolve_session_id(session_id)
    try:
        # print(f" Getting counter data for transaction: {request.transaction_id}")

        # Check session
        if not session_service.session_exists(session_id):
            raise HTTPException(
                status_code=404,
                detail="No session found"
            )

        session_data = session_service.get_session(session_id)

        # Get transaction data
        transaction_data = session_data.get('transaction_data')
        if not transaction_data:
            raise HTTPException(
                status_code=400,
                detail="No transaction data available"
            )

        # Find the transaction
        df = pd.DataFrame(transaction_data)

        # Filter transactions to only those from the selected source file
        source_transactions = df[df['Source File'] == request.source_file]

        source_transactions = source_transactions.drop_duplicates(subset=['Transaction ID'], keep='first')

        if len(source_transactions) == 0:
            raise HTTPException(
                status_code=404,
                detail=f"No transactions found in source '{request.source_file}'"
            )

        if request.transaction_id not in source_transactions['Transaction ID'].values:
            raise HTTPException(
                status_code=404,
                detail=f"Transaction {request.transaction_id} not found in source '{request.source_file}'"
            )

        txn_data = source_transactions[source_transactions['Transaction ID'] == request.transaction_id].iloc[0]

        # Get TRC trace filenames and contents from session
        file_categories = session_data.get('file_categories', {})
        trc_trace_files = file_categories.get('trc_trace', [])
        trc_trace_contents = session_data.get('trc_trace_contents', {})

        if not trc_trace_files:
            raise HTTPException(
                status_code=400,
                detail="No TRC trace files available"
            )

        # Resolve source file (handles YYYYMMDD, YYYYMMDD_1, YYYYMMDD_2 …)
        # to the correct TRC file via structured header-date extraction.
        txn_date_full = request.source_file  # kept for date formatting below
        matching_trc, matching_trc_content = _find_trc_for_source(
            request.source_file, trc_trace_files, trc_trace_contents
        )

        if not matching_trc:
            raise HTTPException(
                status_code=404,
                detail=f"No matching TRC trace file found for source '{request.source_file}'"
            )

        # Extract counter blocks from TRC content in session memory
        txn_start_time = str(txn_data.get('Start Time', ''))
        txn_end_time = str(txn_data.get('End Time', ''))
        txn_type_for_blocks = str(txn_data.get('Transaction Type', ''))

        # Extract ALL blocks scanning both CDM and CIM commands.
        # This is used for first/start/last counter selection — whichever
        # command appears first/last/before-txn-time wins, regardless of
        # whether the selected transaction is a deposit or withdrawal.
        all_counter_blocks = extract_counter_blocks_from_string(
            matching_trc_content, txn_type=None
        )

        # Also extract blocks filtered by transaction type — used only for
        # the main counter table displayed for the selected transaction.
        typed_counter_blocks = extract_counter_blocks_from_string(
            matching_trc_content, txn_type=txn_type_for_blocks
        )

        if not all_counter_blocks:
            print(" No counter blocks found")
            start_counter_data = []
            first_counter_data = []
            last_counter_data = []
            start_timestamp = txn_start_time
            first_timestamp = txn_start_time
            last_timestamp = txn_end_time
        else:
            # Parse transaction times

            def parse_time_from_trc_local(time_str):
                """Parse time from TRC trace format (HH:MM:SS or HH:MM:SS.MS)"""
                try:
                    # Handle "21:17:33" or "21:17:33.04" format
                    if '.' in time_str:
                        base_time = time_str.split('.')[0]
                    else:
                        base_time = time_str

                    # Parse as time object
                    parsed = datetime.strptime(base_time, '%H:%M:%S').time()
                    return parsed
                except Exception as e:
                    # print(f" Error parsing time '{time_str}': {e}")
                    return None

            # Extract just the time portion from transaction start/end
            txn_start_time_only = txn_start_time.split()[-1] if ' ' in txn_start_time else txn_start_time
            txn_end_time_only = txn_end_time.split()[-1] if ' ' in txn_end_time else txn_end_time

            txn_start_dt = parse_time_from_trc_local(txn_start_time_only)
            txn_end_dt = parse_time_from_trc_local(txn_end_time_only)

            # print(f" Transaction start time: {txn_start_dt}")
            # print(f" Transaction end time: {txn_end_dt}")
            # print(f" Total counter blocks: {len(all_counter_blocks)}")

            # 1. First counter: STATIC - absolute first block in the file
            first_block = all_counter_blocks[0]
            first_counter_data = first_block['data']
            first_timestamp = first_block['timestamp']

            # 2. Start counter: DYNAMIC - last counter block strictly BEFORE transaction start time
            start_block = None

            if txn_start_dt:
                for block in all_counter_blocks:
                    block_time = block.get('time')
                    if block_time and block_time < txn_start_dt:
                        start_block = block  # keep updating — last one before start wins
                    elif block_time and block_time >= txn_start_dt:
                        break  # passed the transaction time, stop

            # Fallback: if no block found before start time, use first block
            if not start_block:
                start_block = all_counter_blocks[0]

            start_counter_data = start_block['data']
            start_timestamp = start_block['timestamp']

            # 3. Last counter: STATIC - absolute last block in the file
            last_block = all_counter_blocks[-1]
            last_counter_data = last_block['data']
            last_timestamp = last_block['timestamp']

            # print(f" First counter (static - first in file): {len(start_counter_data)} rows at {start_timestamp}")
            # print(f" Start counter (dynamic - at/after txn start): {len(first_counter_data)} rows at {first_timestamp}")
            # print(f" Last counter (static - last in file): {len(last_counter_data)} rows at {last_timestamp}")

        # Get transaction date
        txn_date = txn_date_full

        # Format the date for display (YYYYMMDD -> "DD Month YYYY")
        txn_date_formatted = txn_date
        if len(txn_date) == 8:  # YYYYMMDD
            try:
                dt = datetime.strptime(txn_date, '%Y%m%d')
                txn_date_formatted = dt.strftime('%d %B %Y')
            except:
                txn_date_formatted = txn_date

        # Build Counter per Transaction table
        counter_per_transaction = []

        # First, reset index to avoid index mismatch issues
        source_transactions_reset = source_transactions.reset_index(drop=True)

        # Find the position (not index) of the selected transaction
        selected_txn_position = source_transactions_reset[source_transactions_reset['Transaction ID'] == request.transaction_id].index[0]

        # Get all transactions from that position onwards
        transactions_subset = source_transactions_reset.iloc[selected_txn_position:]

        # Filter only CIN/CI and COUT/GA transactions
        transactions_subset = transactions_subset[
            transactions_subset['Transaction Type'].isin(['Cash Deposit', 'Cash Withdrawal'])
        ]

        # print(f"  Building counter per transaction table for {len(transactions_subset)} transactions (CIN/COUT only)")

        for _, txn_row in transactions_subset.iterrows():
            txn_id = txn_row['Transaction ID']
            txn_type = txn_row.get('Transaction Type', 'Unknown')
            txn_state = txn_row.get('End State', 'Unknown')
            txn_start_time = str(txn_row.get('Start Time', ''))
            txn_end_time = str(txn_row.get('End Time', ''))
            txn_log = str(txn_row.get('Transaction Log', ''))

            # Parse date and time
            if ' ' in txn_start_time:
                date_part = txn_start_time.split()[0] if len(txn_start_time.split()) > 0 else txn_date
                time_part = txn_start_time.split()[1] if len(txn_start_time.split()) > 1 else txn_start_time
            else:
                date_part = txn_date
                time_part = txn_start_time

            # Format date as "DD Month YYYY" (e.g., "29 May 2025")
            date_formatted = date_part
            if len(date_part) == 8:  # YYYYMMDD
                try:
                    dt = datetime.strptime(date_part, '%Y%m%d')
                    date_formatted = dt.strftime('%d %B %Y')
                except:
                    date_formatted = date_part

            # Extract count information from transaction log
            # Pattern for COUT: "Dispense info - 1 note(s) of 500,00 INR from cassette 5 (SLOT3)"
            # Pattern for CIN: "Identified notes:     1 x    500 INR"
            count_info = []

            # Check conditions for displaying denomination or cancellation
            is_cancelled = "Transaction cancelled. Customer timeout." in txn_log
            is_successful = txn_state == 'Successful'
            has_card_presented = "Card successfully presented" in txn_log
            has_banknotes_presented = "Banknotes presented" in txn_log

            # Decision logic based on conditions
            if is_cancelled and not (is_successful and (has_card_presented or has_banknotes_presented)):
                # Show "Transaction Canceled" for all cancelled transactions EXCEPT when successful with card/banknotes presented
                count_display = "Transaction Canceled"
            else:
                # Show denomination for:
                # 1. No cancellation + successful
                # 2. Cancellation + successful + (card presented OR banknotes presented)

                if txn_type == 'Cash Withdrawal':
                    # COUT pattern: "Dispense info - 1 note(s) of 500,00 INR from cassette 5 (SLOT3)"
                    for log_line in txn_log.split('\n'):
                        match = re.search(r'(\d+)\s+note\(s\)\s+of\s+([\d,\.]+)\s+([A-Z]{3})', log_line, re.IGNORECASE)
                        if match:
                            note_count = match.group(1)
                            amount = match.group(2).replace(',', '.')  # Handle comma as decimal separator
                            currency = match.group(3)
                            count_info.append(f"{currency} {amount} x{note_count}")

                elif txn_type == 'Cash Deposit':
                    # CIN pattern: "Identified notes:     1 x    500 INR"
                    for log_line in txn_log.split('\n'):
                        match = re.search(r'(\d+)\s+x\s+([\d,\.]+)\s+([A-Z]{3})', log_line, re.IGNORECASE)
                        if match:
                            note_count = match.group(1)
                            amount = match.group(2).replace(',', '.')
                            currency = match.group(3)
                            count_info.append(f"{currency} {amount} x{note_count}")

                count_display = ", ".join(count_info) if count_info else ""

            # Create transaction summary
            if txn_state == 'Successful':
                summary = f"Successful"
            elif txn_state == 'Unsuccessful':
                summary = f"Unsuccessful"
            else:
                summary = txn_state

            # Check for counters in transaction timeframe
            counter_summary = ""
            try:
                txn_start_dt = parse_time_from_trc(time_part)
                txn_end_dt = parse_time_from_trc(txn_end_time.split()[-1] if ' ' in txn_end_time else txn_end_time)

                if txn_start_dt and txn_end_dt and all_counter_blocks:
                    for block in all_counter_blocks:
                        block_time = block.get('time')
                        if block_time and txn_start_dt <= block_time <= txn_end_dt:
                            counter_summary = "View Counters"
                            break
            except Exception as e:
                print(f" Error checking counters for {txn_id}: {e}")

            counter_per_transaction.append({
                'date_timestamp': f"{date_formatted} {time_part}",
                'transaction_id': txn_id,
                'transaction_type': txn_type,
                'transaction_summary': summary,
                'transaction_state': txn_state,
                'count': count_display,
                'counter_summary': counter_summary,
                'comment': ''
            })

        # print(f" Created counter per transaction table with {len(counter_per_transaction)} entries")

        # Strip non-JSON-serialisable fields ('time', 'source_cmd') before
        # sending typed_counter_blocks in the response.
        # typed_counter_blocks is filtered by transaction type (CDM for withdrawal,
        # CIM for deposit) and is used for the main counter table display.
        serialisable_blocks = [
            {'timestamp': b['timestamp'], 'data': b['data']}
            for b in typed_counter_blocks
        ]

        response_data = {
            "transaction_id": request.transaction_id,
            "source_file": request.source_file,
            "all_blocks": serialisable_blocks,
            "column_descriptions": get_counter_column_descriptions(),
            "start_counter": {
                "date": txn_date_formatted,
                "timestamp": start_timestamp,
                "counter_data": start_counter_data
            },
            "first_counter": {
                "date": txn_date_formatted,
                "timestamp": first_timestamp,
                "counter_data": first_counter_data
            },
            "last_counter": {
                "date": txn_date_formatted,
                "timestamp": last_timestamp,
                "counter_data": last_counter_data
            },
            "counter_per_transaction": counter_per_transaction
        }

        return response_data

    except HTTPException:
        raise
    except Exception as e:
        traceback.print_exc()
        raise HTTPException(
            status_code=500,
            detail=f"Failed to get counter data: {str(e)}"
        )


def get_counter_column_descriptions():
    """Return descriptions for counter table columns (covers both CDM and CIM formats)"""
    return {
        # Common
        'No':          'Cassette number',
        'Ty':          'Type',
        'Cur':         'Currency',
        'Val':         'Denomination',
        'Ini':         'Ini - count in number',
        'Cnt':         'Cnt - Remaining counters formula: INI - (RETRACT + DISP)',
        'Retr':        'Retract',
        'PName':       'Position / Slot name',
        'Record_Type': 'Record Type',
        # CDM-specific
        'RCnt':        'Reject Count -> (Reject + Presented (Pres))',
        # CIM-specific
        'IT':          'Item Type',
        'ICnt':        'Initial Count',
        'Rej':         'Rejected notes count',
    }


# ============================================
# COUNTER COMPARISON HELPERS + ENDPOINT


def _eligible_counter_rows(counter_data: list, txn_type: str) -> list:
    """
    Filter counter rows to only cash-dispensing cassettes.

    Eligibility rules (robust across all known TRC log formats):
      - Cur must be non-empty  → excludes retract/reject bins that carry no currency
      - Val must be non-zero   → excludes any cassette with denomination 0/00/000/…
      - Ty must NOT be in the known non-cash set {'02', '06'} as a safety net,
        but we do NOT hard-code a specific target Ty value (03, 04, 12, etc.)
        because different ATM firmware versions use different codes.

    txn_type is accepted for signature compatibility but is no longer used
    to filter by Ty — the Cur+Val combination is the reliable signal.
    """
    # Known non-cash unit types to always exclude regardless of other fields
    NON_CASH_TY = {'02', '06'}

    eligible = []
    for row in counter_data:
        cur = str(row.get('Cur', '')).strip()
        val = str(row.get('Val', '')).strip()
        ty  = str(row.get('Ty',  '')).strip()

        # Must have a currency code
        if not cur:
            continue

        # Val must be non-zero (handles 0, 00, 000, 0000, 00000 …)
        stripped_val = val.lstrip('0')
        if stripped_val == '' or stripped_val == '0':
            continue

        # Exclude known non-cash unit types
        if ty in NON_CASH_TY:
            continue

        eligible.append(row)
    return eligible


def _counter_row_key(row: dict) -> tuple:
    """Unique identity key for a cassette row: (No, Ty, ID, Cur, Val)."""
    return (
        str(row.get('No',  '')).strip(),
        str(row.get('Ty',  '')).strip(),
        str(row.get('ID',  '')).strip(),
        str(row.get('Cur', '')).strip(),
        str(row.get('Val', '')).strip(),
    )


def _rows_to_cnt_map(rows: list) -> dict:
    """Map row-key -> Cnt string for a list of eligible rows."""
    return {_counter_row_key(r): str(r.get('Cnt', '')).strip() for r in rows}


def _find_first_delta_block(all_blocks: list, start_block_idx: int, baseline_cnt_map: dict) -> dict | None:
    """
    Walk forward through all_blocks from start_block_idx + 1.
    Return the first block where ANY row key present in baseline_cnt_map
    has a different Cnt value.  Returns None if no delta is found.
    """
    for block in all_blocks[start_block_idx + 1:]:
        for row in block.get('data', []):
            key = _counter_row_key(row)
            if key not in baseline_cnt_map:
                continue
            if str(row.get('Cnt', '')).strip() != baseline_cnt_map[key]:
                return block
    return None


def _compute_counter_comparison(
    all_blocks:   list,
    first_block:  dict,
    start_block:  dict,
    txn_type:     str,
    compare_mode: str,   # "first" | "previous"
) -> dict:
    """
    Core comparison engine used by /get-counter-comparison.

    compare_mode == "first":
        baseline  = eligible Cnt values from first_block (first in TRC file)
        candidate = start_block, then walk forward for first Cnt delta
        second    = delta block, or start_block if no delta found

    compare_mode == "previous":
        baseline  = eligible Cnt values from start_block (last block before txn start)
        candidate = start_block, then walk forward for first Cnt delta
        second    = delta block, or start_block if no delta found

    Returns:
        {
          "baseline_timestamp": str,
          "second_timestamp":   str,
          "rows": [
            {
              "No", "Ty", "ID", "Cur", "Val",
              "Ini", "Disp", "Pres", "Retr", "PName",
              "baseline_cnt": str,
              "second_cnt":   str,
              "delta":        int,
              "direction":    "increase" | "decrease" | "unchanged",
            },
            ...
          ]
        }
    """
    baseline_block = first_block if compare_mode == "first" else start_block

    baseline_rows      = _eligible_counter_rows(baseline_block.get('data', []), txn_type)
    baseline_cnt_map   = _rows_to_cnt_map(baseline_rows)
    baseline_timestamp = baseline_block.get('timestamp', '')

    # Find the index of start_block inside all_blocks
    start_ts  = start_block.get('timestamp', '')
    start_idx = 0
    for i, b in enumerate(all_blocks):
        if b.get('timestamp', '') == start_ts:
            start_idx = i
            break

    # Walk forward from start_block to find first Cnt delta
    delta_block = _find_first_delta_block(all_blocks, start_idx, baseline_cnt_map)

    # If no delta block was found:
    #   - For "first" mode: fall back to start_block (the counter just before txn start)
    #     so the user can at least see the snapshot even if no change occurred yet.
    #   - For "previous" mode: start_block IS the baseline, so if there is no delta
    #     block forward of it, there is genuinely nothing to compare against — return
    #     empty rows rather than a misleading all-zero delta table.
    if delta_block is None:
        if compare_mode == "previous":
            return {
                "baseline_timestamp": baseline_timestamp,
                "second_timestamp":   "",
                "rows":               [],
            }
        else:
            second_block = start_block
    else:
        second_block = delta_block

    second_timestamp = second_block.get('timestamp', '')

    # Build a lookup from the second block for display columns
    second_row_detail: dict[tuple, dict] = {}
    for row in second_block.get('data', []):
        second_row_detail[_counter_row_key(row)] = row

    second_eligible = _eligible_counter_rows(second_block.get('data', []), txn_type)
    second_cnt_map  = _rows_to_cnt_map(second_eligible)

    result_rows = []
    for row in baseline_rows:
        key       = _counter_row_key(row)
        b_cnt_str = baseline_cnt_map.get(key, '0')
        s_cnt_str = second_cnt_map.get(key, b_cnt_str)

        try:
            b_cnt = int(b_cnt_str.lstrip('0') or '0')
        except ValueError:
            b_cnt = 0
        try:
            s_cnt = int(s_cnt_str.lstrip('0') or '0')
        except ValueError:
            s_cnt = 0

        delta     = s_cnt - b_cnt
        direction = "increase" if delta > 0 else ("decrease" if delta < 0 else "unchanged")

        display_row = second_row_detail.get(key, row)

        result_rows.append({
            "No":           str(row.get('No',  '')).strip(),
            "Ty":           str(row.get('Ty',  '')).strip(),
            "ID":           str(row.get('ID',  '')).strip(),
            "Cur":          str(row.get('Cur', '')).strip(),
            "Val":          str(row.get('Val', '')).strip(),
            "Ini":          str(display_row.get('Ini',  '')).strip(),
            "Disp":         str(display_row.get('Disp', '')).strip(),
            "Pres":         str(display_row.get('Pres', '')).strip(),
            "Retr":         str(display_row.get('Retr', '')).strip(),
            "PName":        str(display_row.get('PName', '')).strip(),
            "baseline_cnt": b_cnt_str,
            "second_cnt":   s_cnt_str,
            "delta":        delta,
            "direction":    direction,
        })

    return {
        "baseline_timestamp": baseline_timestamp,
        "second_timestamp":   second_timestamp,
        "rows":               result_rows,
    }




@counter_router.post("/get-counter-comparison", dependencies=[Depends(_rbac_proxy)])
async def get_counter_comparison(
    request:    CounterComparisonRequest,
    session_id: str = Query(default=None)
):
    """
        FUNCTION: get_counter_comparison

        DESCRIPTION:
            Computes a before/after Cnt comparison for a single Cash Withdrawal or
            Cash Deposit transaction using counter blocks extracted from the matching
            TRC trace file.

            Command selection:
              - Cash Withdrawal -> WFS_INF_CDM_CASH_UNIT_INFO blocks only
              - Cash Deposit    -> WFS_INF_CIM_CASH_UNIT_INFO blocks only

            Only rows that satisfy all three eligibility conditions are included:
                - Cur is not empty
                - Val is not effectively zero
                - Ty == '03' for Cash Withdrawal, Ty == '04' for Cash Deposit

            compare_mode == "first":
                baseline  = Cnt from the very first counter block in the TRC file.
                second    = Cnt from the first block at/after transaction start that shows
                            a delta vs the baseline; falls back to the start block if none found.

            compare_mode == "previous":
                baseline  = Cnt from the start block (first block at/after transaction start).
                second    = same delta-walk forward from that start block.

        USAGE:
            response = await get_counter_comparison(
                request=CounterComparisonRequest(
                    transaction_id="TX123",
                    source_file="20250404",
                    compare_mode="first"   # or "previous"
                )
            )

        PARAMETERS:
            request (CounterComparisonRequest):
                - transaction_id (str) : Target transaction ID.
                - source_file    (str) : Source file date string (YYYYMMDD).
                - compare_mode   (str) : "first" or "previous".
            session_id (str) : Optional session ID. Defaults to CURRENT_SESSION_ID.

        RETURNS:
            dict:
                - "transaction_id"     (str)
                - "source_file"        (str)
                - "transaction_type"   (str)
                - "compare_mode"       (str)
                - "baseline_label"     (str)
                - "baseline_timestamp" (str)
                - "second_label"       (str)
                - "second_timestamp"   (str)
                - "rows"               (list of dict) — one entry per eligible cassette row

        RAISES:
            HTTPException 400 : No transaction data or no TRC trace files available.
            HTTPException 404 : Session / transaction / TRC file not found.
            HTTPException 500 : Unexpected processing error.
    """
    session_id = _resolve_session_id(session_id)

    try:
        if not session_service.session_exists(session_id):
            raise HTTPException(status_code=404, detail="No session found")

        session_data = session_service.get_session(session_id)

        # Locate the transaction
        transaction_data = session_data.get('transaction_data')
        if not transaction_data:
            raise HTTPException(status_code=400, detail="No transaction data available")

        df = pd.DataFrame(transaction_data)
        source_transactions = df[df['Source File'] == request.source_file].drop_duplicates(
            subset=['Transaction ID'], keep='first'
        )

        if request.transaction_id not in source_transactions['Transaction ID'].values:
            raise HTTPException(
                status_code=404,
                detail=f"Transaction {request.transaction_id} not found in source '{request.source_file}'"
            )

        txn_data = source_transactions[
            source_transactions['Transaction ID'] == request.transaction_id
        ].iloc[0]

        txn_type = str(txn_data.get('Transaction Type', ''))
        if txn_type not in ('Cash Withdrawal', 'Cash Deposit'):
            raise HTTPException(
                status_code=400,
                detail="Counter comparison is only available for Cash Withdrawal / Cash Deposit transactions."
            )

        # Locate the matching TRC file
        file_categories    = session_data.get('file_categories', {})
        trc_trace_files    = file_categories.get('trc_trace', [])
        trc_trace_contents = session_data.get('trc_trace_contents', {})

        if not trc_trace_files:
            raise HTTPException(status_code=400, detail="No TRC trace files available")

        # Resolve source file (handles YYYYMMDD, YYYYMMDD_1, YYYYMMDD_2 …)
        # to the correct TRC file via structured header-date extraction.
        _matched_trc, matching_trc_content = _find_trc_for_source(
            request.source_file, trc_trace_files, trc_trace_contents
        )

        if not matching_trc_content:
            raise HTTPException(
                status_code=404,
                detail=f"No matching TRC trace file found for source '{request.source_file}'"
            )

        # Extract all counter blocks
        # Use CDM for Cash Withdrawal, CIM for Cash Deposit, both for unknown.
        all_blocks = extract_counter_blocks_from_string(matching_trc_content, txn_type=txn_type)
        if not all_blocks:
            raise HTTPException(status_code=404, detail="No counter blocks found in TRC file")

        # Build first_block and start_block (mirrors logic in get_counter_data)
        first_block = all_blocks[0]

        txn_start_time_str  = str(txn_data.get('Start Time', ''))
        txn_end_time_str    = str(txn_data.get('End Time', ''))
        txn_start_time_only = txn_start_time_str.split()[-1] if ' ' in txn_start_time_str else txn_start_time_str
        txn_end_time_only   = txn_end_time_str.split()[-1]   if ' ' in txn_end_time_str   else txn_end_time_str

        def _parse_t(s: str):
            try:
                return datetime.strptime(s.split('.')[0], '%H:%M:%S').time()
            except Exception:
                return None

        def _time_diff_seconds(t1, t2) -> float:
            base = datetime.today().date()
            dt1  = datetime.combine(base, t1)
            dt2  = datetime.combine(base, t2)
            return abs((dt2 - dt1).total_seconds())

        txn_start_dt = _parse_t(txn_start_time_only)
        txn_end_dt   = _parse_t(txn_end_time_only)

        # A counter block is only considered valid for this transaction if it falls
        # within MAX_COUNTER_GAP_SECONDS of the transaction end time.
        # Counters are written after dispense/deposit completes, so end time is the
        # better anchor. 30 min covers slow ATM logging; rejects counters 5+ hrs away.
        MAX_COUNTER_GAP_SECONDS = 1800
        anchor_dt = txn_end_dt or txn_start_dt

        start_block = None
        if txn_start_dt:
            for b in all_blocks:
                bt = b.get('time')
                if bt is None:
                    continue
                if bt >= txn_start_dt:
                    break  # passed the transaction time, stop
                # bt is strictly before txn_start_dt — keep as candidate
                # Still apply the proximity guard so we don't pick a block
                # from many hours earlier
                if anchor_dt and _time_diff_seconds(bt, anchor_dt) > MAX_COUNTER_GAP_SECONDS:
                    continue
                start_block = b  # last qualifying block before txn start wins
        else:
            start_block = first_block

        if start_block is None:
            nearest_ts = all_blocks[0].get('timestamp', '?') if all_blocks else '?'
            return {
                "transaction_id":       request.transaction_id,
                "source_file":          request.source_file,
                "transaction_type":     txn_type,
                "compare_mode":         request.compare_mode,
                "baseline_label":       "",
                "baseline_timestamp":   "",
                "second_label":         "",
                "second_timestamp":     "",
                "rows":                 [],
                "no_counter_available": True,
            }


        # Run the comparison
        comparison = _compute_counter_comparison(
            all_blocks   = all_blocks,
            first_block  = first_block,
            start_block  = start_block,
            txn_type     = txn_type,
            compare_mode = request.compare_mode,
        )

        if request.compare_mode == "first":
            baseline_label = "First Counter (start of file)"
            second_label   = "Counter after transaction (with delta)"
        else:
            baseline_label = "Start Counter (before transaction start)"
            second_label   = "Next counter showing change"

        return {
            "transaction_id":     request.transaction_id,
            "source_file":        request.source_file,
            "transaction_type":   txn_type,
            "compare_mode":       request.compare_mode,
            "baseline_label":     baseline_label,
            "baseline_timestamp": comparison["baseline_timestamp"],
            "second_label":       second_label,
            "second_timestamp":   comparison["second_timestamp"],
            "rows":               comparison["rows"],
        }

    except HTTPException:
        raise
    except Exception as e:
        traceback.print_exc()
        raise HTTPException(
            status_code=500,
            detail=f"Failed to compute counter comparison: {str(e)}"
        )





def safe_decode(blob: bytes) -> str:
    """Safely decode bytes to string"""
    encs = ["utf-8-sig", "utf-16", "utf-16-le", "utf-16-be", "cp1252", "latin-1", "utf-8"]
    for e in encs:
        try:
            return blob.decode(e)
        except Exception:
            continue
    return blob.decode("utf-8", errors="replace")


def parse_counter_data_from_trc(log_lines: list) -> list:
    """
    Parses WFS_INF_CDM_CASH_UNIT_INFO or WFS_INF_CIM_CASH_UNIT_INFO table format.

    CDM columns (captured): No, Ty, ID, Cur, Val, Ini, Cnt, RCnt, Retr, PName
    CDM columns (skipped):  UnitName, Min, Max, A, St, NrPCU, Disp, Pres
    Stop column: PName

    CIM columns (captured): No, Ty, IT, ID, Cur, Val, ICnt, Cnt, Ini, Retr, Rej, PposName
    CIM columns (skipped):  CT, Disp, Pres, Min, St, A
    Stop column: PposName

    Everything after the stop column is ignored (avoids the repeated PCU section in CIM).
    """
    counter_rows = []

    # ── Format definitions ────────────────────────────────────────────────────
    # Each format: (required_cols_to_detect_header, all_col_names, skip_cols, stop_col, stop_output_key)
    FORMATS = [
        {
            # CDM: WFS_INF_CDM_CASH_UNIT_INFO
            'required':  {'No', 'Ty', 'UnitName', 'NrPCU', 'PName'},
            'all_cols':  ['No', 'Ty', 'UnitName', 'ID', 'Cur', 'Val', 'Ini', 'Cnt',
                          'RCnt', 'Min', 'Disp', 'Pres', 'Retr', 'Max', 'A', 'St', 'NrPCU', 'PName'],
            'skip':      {'UnitName', 'Min', 'Max', 'A', 'St', 'NrPCU', 'Disp', 'Pres'},
            'stop_col':  'PName',
            'stop_key':  'PName',
        },
        {
            # CIM: WFS_INF_CIM_CASH_UNIT_INFO
            'required':  {'No', 'Ty', 'IT', 'ICnt', 'Rej', 'PposName'},
            'all_cols':  ['No', 'Ty', 'IT', 'ID', 'Cur', 'Val', 'ICnt', 'Cnt', 'Max',
                          'CT', 'Ini', 'Disp', 'Pres', 'Retr', 'Rej', 'Min', 'St', 'A', 'PposName'],
            'skip':      {'CT', 'Disp', 'Pres', 'Min', 'St', 'A', 'Max'},
            'stop_col':  'PposName',
            'stop_key':  'PName',   # normalise to PName so the rest of the app is unchanged
        },
    ]

    # ── Detect header line and which format it matches ────────────────────────
    header_line = None
    header_idx  = -1
    fmt         = None

    for idx, line in enumerate(log_lines):
        for candidate in FORMATS:
            if all(col in line for col in candidate['required']):
                header_line = line
                header_idx  = idx
                fmt         = candidate
                break
        if header_line:
            break

    if not header_line or fmt is None:
        return []

    # ── Build sorted column positions from the header ─────────────────────────
    col_positions = {}
    for col in fmt['all_cols']:
        pos = header_line.find(col)
        if pos != -1:
            col_positions[col] = pos

    sorted_cols = sorted(col_positions.items(), key=lambda x: x[1])

    if len(sorted_cols) < 5:
        return []

    stop_col  = fmt['stop_col']
    stop_key  = fmt['stop_key']
    skip_cols = fmt['skip']

    # Parse data lines
    for idx in range(header_idx + 1, len(log_lines)):
        line = log_lines[idx]

        if not line.strip():
            continue
        if line.strip().startswith('*') or line.strip().startswith('-'):
            continue
        if 'usTellerID' in line:
            continue
        if line.startswith('\t'):
            continue
        if not line or not line[0].isdigit():
            continue

        try:
            counter_data = {}

            for i, (col_name, col_start) in enumerate(sorted_cols):

                # Hard stop at the format's stop column — first token only, then break
                if col_name == stop_col:
                    raw = line[col_start:].strip() if col_start < len(line) else ''
                    if col_name not in skip_cols:
                        counter_data[stop_key] = raw.split()[0] if raw else ''
                    break

                # Get slice end from next column's start position
                next_col_start = sorted_cols[i + 1][1] if i + 1 < len(sorted_cols) else len(line)
                raw = line[col_start:next_col_start].strip() if col_start < len(line) else ''

                # Store only if not in skip list
                if col_name not in skip_cols:
                    counter_data[col_name] = raw

            # Skip rows missing No or Ty
            if not counter_data.get('No') or not counter_data.get('Ty'):
                continue

            # Val:
            val_raw = counter_data.get('Val', '')
            if val_raw.isdigit():
                counter_data['Val']

            counter_data['Record_Type'] = 'Logical'
            counter_rows.append(counter_data)

        except Exception:
            continue

    return counter_rows




def parse_time_from_trc(time_str: str) -> dt_time:
    """Parse time from TRC trace format (HH:MM:SS or HH:MM:SS.MS)"""
    try:
        if '.' in time_str:
            time_str = time_str.split('.')[0]
        return datetime.strptime(time_str, '%H:%M:%S').time()
    except Exception:
        return None



def extract_counter_blocks(trc_file_path: str, txn_type: str = None) -> list:
    """
    Extracts counter blocks from a TRC file.
    Each block is kept separate; no merging occurs.

    Command selection based on txn_type:
      - 'Cash Withdrawal' -> WFS_INF_CDM_CASH_UNIT_INFO only
      - 'Cash Deposit'    -> WFS_INF_CIM_CASH_UNIT_INFO only
      - None / unknown    -> both commands; whichever appears first in the file
                            is used for the first/start block, whichever appears
                            last is used for the last block.

    RETURNS:
        list of dicts:
            - 'time'       : datetime.time
            - 'timestamp'  : str (HH:MM:SS.ss)
            - 'data'       : list of counter row dicts
            - 'source_cmd' : 'CDM' or 'CIM'
    """
    try:
        with open(trc_file_path, 'r', encoding='utf-8', errors='ignore') as f:
            content = f.read()
        return extract_counter_blocks_from_string(content, txn_type=txn_type)
    except Exception as e:
        logger.error(f"Error extracting counter blocks: {e}")
        traceback.print_exc()
        return []


def extract_counter_blocks_from_string(content: str, txn_type: str = None) -> list:
    """
    Accepts file content as a string and extracts counter blocks.

    Command selection based on txn_type:
      - 'Cash Withdrawal' -> WFS_INF_CDM_CASH_UNIT_INFO only
      - 'Cash Deposit'    -> WFS_INF_CIM_CASH_UNIT_INFO only
      - None / unknown    -> both commands; whichever appears first in the file
                            is used for the first/start block, whichever appears
                            last is used for the last block.

    Each returned block dict carries a 'source_cmd' key ('CDM' or 'CIM') so
    callers can distinguish origin if needed.
    """
    CDM_CMD = 'WFS_INF_CDM_CASH_UNIT_INFO'
    CIM_CMD = 'WFS_INF_CIM_CASH_UNIT_INFO'

    # Determine which commands to scan for
    if txn_type == 'Cash Withdrawal':
        target_cmds = {CDM_CMD: 'CDM'}
    elif txn_type == 'Cash Deposit':
        target_cmds = {CIM_CMD: 'CIM'}
    else:
        target_cmds = {CDM_CMD: 'CDM', CIM_CMD: 'CIM'}

    all_counter_blocks = []
    try:
        lines = content.split('\n')
        i = 0
        while i < len(lines):
            line = lines[i]

            # Check if the line contains any of our target commands
            matched_cmd = None
            matched_label = None
            for cmd, label in target_cmds.items():
                if cmd in line:
                    matched_cmd = cmd
                    matched_label = label
                    break

            if matched_cmd:
                timestamp_str = None
                block_time = None

                ts_match = re.search(r'(\d+)\s+(\d{6})\s+(\d{2}:\d{2}:\d{2}\.\d{2})', line)
                if not ts_match and i > 0:
                    ts_match = re.search(r'(\d+)\s+(\d{6})\s+(\d{2}:\d{2}:\d{2}\.\d{2})', lines[i - 1])

                if ts_match:
                    timestamp_str = ts_match.group(3)
                    try:
                        block_time = datetime.strptime(timestamp_str, '%H:%M:%S.%f').time()
                    except Exception:
                        pass

                block_lines = []
                i += 1

                while i < len(lines):
                    current_line = lines[i]

                    # Stop if we hit any target command block (not just the same one)
                    if any(cmd in current_line for cmd in target_cmds):
                        i -= 1
                        break

                    if re.search(r'^\d{4,}\s+\d{6}\s+\d{2}:\d{2}:\d{2}\.\d{2}', current_line):
                        break

                    block_lines.append(current_line)
                    i += 1

                counter_data = parse_counter_data_from_trc(block_lines)

                if counter_data and timestamp_str:
                    all_counter_blocks.append({
                        'time': block_time,
                        'timestamp': timestamp_str,
                        'data': counter_data,
                        'source_cmd': matched_label,
                    })

            i += 1

    except Exception as e:
        logger.error(f"Error extracting counter blocks from string: {e}")

    return all_counter_blocks


# ─────────────────────────────────────────────────────────────────────────────
# SHARED HELPER — TRC date extraction & source-to-TRC resolution
# Used by: get_matching_sources_for_trc, get_counter_data, get_counter_comparison
#
# KEY DESIGN NOTES
# ─────────────────
# A single TRCTRACE.prn can span multiple calendar dates.  Every log line
# carries a YYMMDD stamp in field 2:
#
#   <seq>  <YYMMDD>  <HH:MM:SS.ff>  ...
#   12345  250404    09:31:22.04    WFS_INF_CDM_CASH_UNIT_INFO
#
# We extract ALL distinct YYMMDD values from every timestamp line in the whole
# file, so a multi-day TRC is correctly mapped to every date it covers.
#
# A session source file (stem of the .jrn) is stored as YYYYMMDD, and may
# carry a dedup suffix when the ZIP contained more than one file for the same
# date: 20250404, 20250404_1, 20250404_2 …  All variants for the same date
# map to the same TRC file — the TRC is per-date, not per-file-instance.
# ─────────────────────────────────────────────────────────────────────────────

# Matches a full TRC timestamp line: <seq> <YYMMDD> <HH:MM:SS.ff>
# Capture group 1 is the YYMMDD field.
_TRC_TIMESTAMP_RE = re.compile(r'^\d+\s+(\d{6})\s+\d{2}:\d{2}:\d{2}\.\d{2}')
_SOURCE_SUFFIX_RE = re.compile(r'_\d+$')


def _extract_all_yymmdd_from_trc_content(content: str) -> set:
    """
    Scan every line of a TRCTRACE.prn and return the set of all distinct
    YYMMDD date stamps found on real timestamp lines.

    Timestamp line format:  <seq>  <YYMMDD>  <HH:MM:SS.ff>  ...
    Only lines matching that strict pattern are considered — sequence number
    followed immediately by a 6-digit date field then a HH:MM:SS.ff time —
    which avoids false positives from counter-data or other numeric fields.

    Returns an empty set if no timestamp lines are found.
    """
    dates = set()
    for line in content.splitlines():
        m = _TRC_TIMESTAMP_RE.match(line.strip())
        if m:
            yymmdd = m.group(1)
            yy, mm, dd = yymmdd[:2], yymmdd[2:4], yymmdd[4:]
            # Validate: month 01-12, day 01-31, year 00-99
            if 1 <= int(mm) <= 12 and 1 <= int(dd) <= 31:
                dates.add(yymmdd)
    return dates


def _trc_contains_date(content: str, yymmdd: str) -> bool:
    """
    Return True only if *yymmdd* appears as the date field on at least one
    real timestamp line in the TRC content.  This is more precise than a
    raw substring search — it requires the 6-digit value to sit in the
    correct structural position (after the sequence number, before the time).
    """
    for line in content.splitlines():
        m = _TRC_TIMESTAMP_RE.match(line.strip())
        if m and m.group(1) == yymmdd:
            return True
    return False


def _source_stem_to_yymmdd(source_stem: str) -> Optional[str]:
    """
    Convert a session source-file stem to the 6-digit YYMMDD string used
    inside TRCTRACE.prn timestamp lines.

    Handles:
        "20250404"   -> "250404"   (YYYYMMDD, no suffix)
        "20250404_1" -> "250404"   (YYYYMMDD, dedup suffix stripped)
        "20250404_2" -> "250404"
        "250404"     -> "250404"   (already YYMMDD)
    """
    bare = _SOURCE_SUFFIX_RE.sub('', source_stem)   # strip _N dedup suffix
    if len(bare) == 8 and bare.isdigit():
        return bare[2:]                              # YYYYMMDD -> YYMMDD
    if len(bare) == 6 and bare.isdigit():
        return bare                                  # already YYMMDD
    return None


def _build_trc_date_map(
    trc_trace_files: list,
    trc_trace_contents: dict,
) -> dict:
    """
    Return a mapping of  YYMMDD -> trc_filename  built by reading every
    timestamp line in every TRC file.

    A single TRC file can span multiple dates, so it registers under all
    of them.  If two separate TRC files both cover the same date, the first
    one encountered wins — consistent with file ordering in the session.

    Example result:
        {
            "250404": "TRCTRACE.prn",
            "250405": "TRCTRACE.prn",   # same TRC spans two days
            "250406": "TRCTRACE_1.prn",
        }
    """
    date_map: dict[str, str] = {}
    for trc_filename in trc_trace_files:
        trc_content = trc_trace_contents.get(trc_filename, '')
        dates_in_file = _extract_all_yymmdd_from_trc_content(trc_content)
        for d in dates_in_file:
            if d not in date_map:          # first TRC for this date wins
                date_map[d] = trc_filename
        if dates_in_file:
            logger.debug(f"[TRC-DATE-MAP] {trc_filename} -> {sorted(dates_in_file)}")
        else:
            logger.warning(f"[TRC-DATE-MAP] No timestamp lines found in {trc_filename}")
    return date_map


def _find_trc_for_source(
    source_file: str,
    trc_trace_files: list,
    trc_trace_contents: dict,
) -> tuple[Optional[str], Optional[str]]:
    """
    Return (trc_filename, trc_content) for the TRC file that covers the date
    of *source_file*.

    Duplicate source variants (20250404_1, 20250404_2 …) all resolve to the
    same TRC file — they share the same date and there is one TRC per date,
    not one per source-file instance.

    Returns (None, None) if no matching TRC is found.
    """
    yymmdd = _source_stem_to_yymmdd(source_file)
    if not yymmdd:
        logger.warning(f"[TRC-FIND] Cannot derive YYMMDD from source '{source_file}'")
        return None, None

    date_map = _build_trc_date_map(trc_trace_files, trc_trace_contents)
    chosen_trc = date_map.get(yymmdd)

    if not chosen_trc:
        logger.warning(
            f"[TRC-FIND] No TRC file found for yymmdd={yymmdd} (source={source_file}). "
            f"Available dates: {sorted(date_map.keys())}"
        )
        return None, None

    logger.info(f"[TRC-FIND] source='{source_file}' yymmdd={yymmdd} -> trc='{chosen_trc}'")
    return chosen_trc, trc_trace_contents.get(chosen_trc, '')

# ─────────────────────────────────────────────────────────────────────────────