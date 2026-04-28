# modules/llm_service.py

import json
import time
import hashlib
import ollama
from pathlib import Path
from datetime import datetime

import re as _re

from modules.processing import LogPreprocessorService, TransactionMergerService
from modules.journal_parser import (
    match_journal_file,
    extract_diagnostic_context_from_content,
    mask_ej_log,
)
from modules.analysis import store_metadata
from modules.logging_config import logger
from modules.example_store import fetch_relevant_examples, build_example_block, needs_examples

import os
MODEL_NAME       = os.getenv("OLLAMA_MODEL", "llama3_log_analyzer")
MIN_PROMPT_CHARS = 150

# ── EJ line codes / patterns already captured by the structured record ────
# Lines matching these are redundant — they duplicate what _build_ej_record
# and extract_diagnostic_context already put into the JSON record.
_EJ_REDUNDANT_CODES = {
    '3239', '3202',            # txn open/close (ts_start/ts_end/status)
    '3201',                    # txn number line
    '60002',                   # account number — already in customer_actions [3260]
    # NOTE: 3207 (card retract) intentionally kept — contains retract reason
    # details (e.g. card not taken, card jammed) not always in ordered_events
}
_EJ_REDUNDANT_PATTERNS = _re.compile(
    r"(?i)"
    r"Transaction no\.|"            # txn number — in txn_number
    r"pan\s+'|"                     # PAN — stripped for PII
    r"Function\s+'[^']+'\s+sel|"   # function type — in type
    r"Account selected|"            # account — stripped by _compact
    r"Total Amount |"               # amount — stripped by _compact
    r"Identified notes:|"           # notes — stripped by _compact
    r"state\s+'[^']+',\s+end-|"   # end-state — in status
    r"Pin entered|"                 # pin — not diagnostic
    r"Card successfully pres|"     # card presented — generic, not diagnostic
    r"Customer cancels|"            # cancel — in events/status
    r"Transaction cancelled|"       # cancel — in events/status
    r"Customer timeout|"            # timeout — in events/status
    r"Transaction timed out|"       # timeout — in events/status
    r"\*{5,}"                       # star separators — noise
)


def _compact_ej_for_prompt(transaction_log: str, max_lines: int = 40) -> str:
    """
    FUNCTION: _compact_ej_for_prompt

    DESCRIPTION:
        Produces a compact, non-redundant version of the CUSTOMER EJ log
        for inclusion in the LLM prompt. Strips:
          - Lines whose event code is already captured in the structured record
          - Lines matching patterns already extracted by preprocess_ej
          - PII via mask_ej_log
          - Blank / star-separator lines
          - 41004/41005 raw hex protocol lines (already decoded by JRN context)

        Keeps only lines that might carry diagnostic information the
        structured parsers don't cover (unusual codes, operator messages,
        free-text warnings, hardware status lines, etc.).

    PARAMETERS:
        transaction_log (str)  : Raw per-transaction EJ log text.
        max_lines       (int)  : Hard cap on output lines (default 40).

    RETURNS:
        str : Compact, PII-masked EJ excerpt. Empty string if nothing unique.
    """
    if not transaction_log or not transaction_log.strip():
        return ""

    # First apply PII masking + 41004/41005 strip
    masked = mask_ej_log(transaction_log)

    kept = []
    line_re = _re.compile(r'^\s*(\d{2}:\d{2}:\d{2})\s+(\d+)\s+(.*)$')

    for line in masked.splitlines():
        stripped = line.strip()
        if not stripped:
            continue

        # Drop star separators
        if _re.match(r'^\s*\*{3,}\s*$', stripped):
            continue

        m = line_re.match(stripped)
        if m:
            code = m.group(2)
            msg  = m.group(3)

            # Drop lines with codes already in the structured record
            if code in _EJ_REDUNDANT_CODES:
                continue

            # Drop lines whose message matches an already-parsed pattern
            if _EJ_REDUNDANT_PATTERNS.search(msg):
                continue

        else:
            # Non-timestamped line — drop if it matches a redundant pattern
            if _EJ_REDUNDANT_PATTERNS.search(stripped):
                continue

        kept.append(stripped)

    # Truncate to max_lines
    if len(kept) > max_lines:
        kept = kept[:max_lines]
        kept.append(f"... [{len(kept)} of {len(masked.splitlines())} lines shown]")

    return '\n'.join(kept)


def _build_ej_record_from_txn_data(txn_data: dict, transaction_log: str) -> dict:
    """
    FUNCTION: _build_ej_record_from_txn_data

    DESCRIPTION:
        Builds a minimal EJ record dict directly from the already-parsed
        txn_data row (from the session DataFrame) instead of re-running
        preprocess_ej on the raw per-transaction log.

        This avoids the root-cause bug where preprocess_ej — designed to
        parse a full EJ file — misidentifies transaction boundaries when
        given a single sliced transaction log, causing the wrong ts_start
        to be selected and sending the wrong transaction to the LLM.

        SENSITIVE DATA:
        - raw_log is intentionally NOT included — it contains unredacted
          PAN, EMV AID, and host message text that bypasses _compact() stripping.
        - All fields are sourced from the pre-stripped txn_data DataFrame row.

        SEQUENCE CONTEXT:
        - ordered_events is constructed to give the LLM a chronologically
          accurate picture of what happened. This is critical for distinguishing
          CASH retract from CARD retract — without ordering, the LLM conflates
          them. card_ejected is explicitly placed before dispense/retract events
          so the model knows the card was already taken before cash issues occurred.

    PARAMETERS:
        txn_data        (dict) : Transaction row dict from session DataFrame.
        transaction_log (str)  : Raw per-transaction log — used only for
                                 char-count logging, NOT sent to the LLM.

    RETURNS:
        dict : EJ record compatible with TransactionMergerService and
               LogPreprocessorService.build_prompt().
    """
    def _safe_ts(val) -> str:
        if val is None:
            return ""
        try:
            import pandas as pd
            if pd.isna(val):
                return ""
        except (TypeError, ValueError):
            pass
        if hasattr(val, 'strftime'):
            return val.strftime("%H:%M:%S")
        s = str(val).strip()
        if s in ("NaT", "NaN", "nan", "None", ""):
            return ""
        return s[:8] if len(s) >= 8 else s

    def _parse_list_field(val):
        """Safely parse a stringified list back to a Python list."""
        if val is None:
            return []
        if isinstance(val, list):
            return val
        if isinstance(val, str) and val.strip():
            try:
                import ast
                parsed = ast.literal_eval(val)
                return parsed if isinstance(parsed, list) else [val]
            except Exception:
                return [val]
        return []

    # ── Build ordered_events with explicit sequencing ─────────────────────
    # The flat events list from JRN processing loses chronological order,
    # causing the LLM to conflate cash retract with card retract.
    # ordered_events reconstructs the correct sequence:
    #   1. Card ejected and taken (if true → card is GONE before anything else)
    #   2. Dispense OK (cash was dispensed successfully)
    #   3. Present failed / Present timeout (customer did not collect cash)
    #   4. Device errors (CASH_DISPENSER errors — on the cash, not the card)
    #   5. Retract counter change (CASH was retracted, not card)
    # This sequence is the primary signal the LLM uses to determine cause.
    raw_events     = _parse_list_field(txn_data.get("JRN Events"))
    card_ejected   = txn_data.get("JRN Card Ejected")
    retract_counter = txn_data.get("JRN Retract Counter")

    ordered_events = []

    # Step 1 — Card gone first (must appear before any retract event)
    if card_ejected:
        ordered_events.append("Card ejected and taken by customer")

    # Step 2 — Dispense outcome
    dispense_events = [e for e in raw_events if "Dispense" in e]
    ordered_events.extend(dispense_events)

    # Step 3 — Present failure (customer didn't collect cash)
    present_events = [e for e in raw_events if "Present" in e or "present" in e]
    ordered_events.extend(present_events)

    # Step 4 — Device errors (these are cash dispenser errors, post-dispense)
    device_events = [e for e in raw_events if "DeviceError" in e or "Error" in e]
    ordered_events.extend(device_events)

    # Step 5 — Retract (CASH retract — card already gone at step 1)
    if retract_counter:
        ordered_events.append(f"CASH retracted (not card) — Retract counter {retract_counter}")
    else:
        retract_events = [e for e in raw_events if "Retract" in e or "retract" in e]
        ordered_events.extend(retract_events)

    # Step 6 — Any remaining events not yet captured
    already_added = set(ordered_events)
    for ev in raw_events:
        if ev not in already_added:
            ordered_events.append(ev)

    raw_protocol_steps = _parse_list_field(txn_data.get("JRN Protocol Steps"))

    # ── Deduplicate protocol steps: drop →sent if a →result exists for same step ──
    # Uses Unicode arrow → explicitly to match actual log characters.
    # e.g. keep TDR_CV(CV)→PROCEED_NEXT, drop TDR_CV(CV)→sent
    _SENT = '→sent'
    _ARROW = '→'
    result_steps = {
        s.split(_ARROW)[0] for s in raw_protocol_steps
        if _ARROW in s and not s.endswith(_SENT)
    }
    protocol_steps_clean = [
        s for s in raw_protocol_steps
        if not (s.endswith(_SENT) and s.split(_ARROW)[0] in result_steps)
    ]

    record = {
        "ts_start":         _safe_ts(txn_data.get("Start Time")),
        "ts_end":           _safe_ts(txn_data.get("End Time")),
        # txn_number intentionally excluded — same as transaction_id, pure duplicate
        "type":             str(txn_data.get("Transaction Type", "Unknown")),
        "status":           str(txn_data.get("End State", "Unknown")),
        # NOTE: raw_log intentionally excluded — contains PAN and unredacted host data
        # JRN-enriched fields already merged during analyze_customer_journals
        "protocol_steps":   protocol_steps_clean,
        "device_errors":    _parse_list_field(txn_data.get("JRN Device Errors")),
        "events":           ordered_events,      # sequenced — see above
        "response_code":    txn_data.get("JRN Response Code"),
        "app_state_start":  txn_data.get("JRN App State Start"),
        "app_state_end":    txn_data.get("JRN App State End"),
        "device_states":    txn_data.get("JRN Device States"),
        "card_events":      txn_data.get("JRN Card Events"),
        # card_ejected and retract_counter are encoded into ordered_events above;
        # kept here as explicit boolean/string fields for the LLM to reference directly
        "card_ejected":     bool(card_ejected) if card_ejected else None,
        "retract_counter":  retract_counter,
    }

    # ── Strip low-value fields that bloat the prompt ────────────────────
    # These fields add token cost with minimal diagnostic value for the LLM.
    LOW_VALUE_FIELDS = {
        'uuids',            # reference IDs — not diagnostic
        'trn_numbers',      # reference IDs — not diagnostic
        'stan_values',      # reference IDs — not diagnostic
        'host_requests',    # already represented in protocol_steps
        'terminal_id',      # used as ATM ID in summary, not needed in body
    }
    record = {k: v for k, v in record.items() if k not in LOW_VALUE_FIELDS}

    # EMV events — keep only lines that contain a failure/decline/error signal.
    # Pure AID listing lines (3951/3952/3953/3954) with no failure are noise.
    if record.get('emv_events'):
        emv_filtered = [
            e for e in record['emv_events']
            if any(kw in e for kw in (
                'fail', 'Fail', 'error', 'Error',
                'decline', 'Decline', 'offline', 'Offline',
                'fallback', 'Fallback', 'blocked', 'Blocked'
            ))
        ]
        if emv_filtered:
            record['emv_events'] = emv_filtered
        else:
            del record['emv_events']

    # Strip empty/null values so build_prompt receives only meaningful fields
    return {k: v for k, v in record.items() if v not in (None, [], "", False)}


def _enrich_record_with_jrn_context(record: dict, jrn_context: dict) -> dict:
    """
    FUNCTION: _enrich_record_with_jrn_context

    DESCRIPTION:
        Merges diagnostic fields from extract_diagnostic_context_from_content()
        into the EJ record. Only non-empty fields are added so the LLM prompt
        stays compact. Fields already present in the record (from session-time
        enrichment) are NOT overwritten — the JRN context fills gaps only.

    PARAMETERS:
        record      (dict) : EJ record from _build_ej_record_from_txn_data.
        jrn_context (dict) : Output of extract_diagnostic_context_from_content.

    RETURNS:
        dict : Enriched record (mutated in place and returned for convenience).
    """
    # Map jrn_context keys → record keys (only fields the LLM prompt uses)
    # LOW-VALUE FIELDS INTENTIONALLY EXCLUDED:
    #   host_requests  — already represented in protocol_steps
    #   uuids          — reference IDs, not diagnostic
    #   trn_numbers    — reference IDs, not diagnostic
    #   stan_values    — reference IDs, not diagnostic
    #   terminal_id    — used as ATM ID, not needed in record body
    # emv_events is included but filtered to failure-only lines below.
    FIELD_MAP = {
        'protocol_steps':    'protocol_steps',
        'device_errors':     'device_errors',
        'device_states':     'device_states',
        'card_events':       'card_events',
        'app_state_start':   'app_state_start',
        'app_state_end':     'app_state_end',
        'response_codes':    'response_code',
        'host_replies':      'host_replies',
        'host_outcome':      'host_outcome',
        'host_notes':        'host_notes',
        'emv_events':        'emv_events',
        'chip_decision':     'chip_decision',
        'tvr_tsi':           'tvr_tsi',
        'cryptogram_info':   'cryptogram_info',
        'customer_actions':  'customer_actions',
        'transaction_types': 'transaction_types',
    }

    for src_key, dst_key in FIELD_MAP.items():
        src_val = jrn_context.get(src_key)

        # Skip empty values
        if src_val is None or src_val == [] or src_val == '':
            continue

        existing = record.get(dst_key)

        # For list fields: extend if existing is empty, or merge without duplicates
        if isinstance(src_val, list):
            if not existing or existing == []:
                record[dst_key] = src_val
            elif isinstance(existing, list):
                existing_set = set(str(e) for e in existing)
                for item in src_val:
                    if str(item) not in existing_set:
                        existing.append(item)
        # For scalar fields: fill only if blank
        elif isinstance(src_val, str):
            # response_codes is a list in jrn_context but scalar in record
            if dst_key == 'response_code' and isinstance(src_val, list):
                if not existing and src_val:
                    record[dst_key] = src_val[0]
            elif not existing:
                record[dst_key] = src_val

    # ── Post-enrichment: deduplicate protocol_steps ─────────────────────
    # JRN context merges raw protocol_steps including →sent lines.
    # Apply the same dedup logic here to strip them after enrichment.
    if record.get('protocol_steps'):
        _SENT  = '→sent'
        _ARROW = '→'
        _result_steps = {
            s.split(_ARROW)[0] for s in record['protocol_steps']
            if _ARROW in s and not s.endswith(_SENT)
        }
        record['protocol_steps'] = [
            s for s in record['protocol_steps']
            if not (s.endswith(_SENT) and s.split(_ARROW)[0] in _result_steps)
        ]

    # ── Post-enrichment: filter EMV events to failure-only lines ──────────
    # Pure AID listing lines added during enrichment are noise for the LLM.
    if record.get('emv_events'):
        emv_filtered = [
            e for e in record['emv_events']
            if any(kw in e for kw in (
                'fail', 'Fail', 'error', 'Error',
                'decline', 'Decline', 'offline', 'Offline',
                'fallback', 'Fallback', 'blocked', 'Blocked'
            ))
        ]
        if emv_filtered:
            record['emv_events'] = emv_filtered
        else:
            record.pop('emv_events', None)

    return record


def analyze_transaction(
    transaction_id: str,
    transaction_log: str,
    txn_data: dict,
    ui_journal_files: list,
    ui_journal_contents: dict = None,
    customer_journal_contents: dict = None,
    employee_code: str = None,
) -> dict:
    """
    FUNCTION:
        analyze_transaction

    DESCRIPTION:
        Full LLM analysis pipeline for a single ATM transaction.

        KEY DESIGN: Builds the EJ record directly from the session DataFrame
        row (txn_data) instead of calling preprocess_ej on the raw log.
        preprocess_ej is designed for full EJ files and misidentifies
        transaction boundaries when given a single sliced transaction log —
        especially for chain transactions (TID 3239) which lack a standard
        3201 open line. This was causing the wrong ts_start to be selected
        and an entirely different transaction being sent to the LLM.

        Data flow to LLM (both CUSTOMER + JOURNAL):
        1. Structured EJ record — built from txn_data (CUSTOMER folder data).
        2. JRN diagnostic context — extracted from JOURNAL folder .jrn content
           in session memory via extract_diagnostic_context_from_content().
           Merged into the EJ record as enrichment fields.
        3. Masked EJ raw log — the CUSTOMER folder transaction log, PII-stripped,
           appended as additional context for the LLM to reference.

        JRN enrichment path:
        - Primary:  JRN fields already present in txn_data (merged during
                    analyze_customer_journals at session creation time) — used directly.
        - Fallback: If JRN enrichment columns are empty (EJ-only session),
                    attempt to match and merge the JRN file now via
                    extract_diagnostic_context_from_content (reads from session memory).

        Also includes:
        - Input monitoring (char count, estimated tokens, fingerprint)
        - Actual token usage from Ollama response
        - Debug dump to llm_debug_input.json
        - Metadata storage via store_metadata

    PARAMETERS:
        transaction_id           (str)  : Unique transaction identifier (filenameHHMMSS format).
        transaction_log          (str)  : Raw per-transaction log text from the DataFrame.
        txn_data                 (dict) : Transaction row dict from session DataFrame.
        ui_journal_files         (list) : JOURNAL/-level JRN filenames (VCP-PRO excluded).
        ui_journal_contents      (dict) : Filename→content map for UI + JOURNAL folder JRN files.
        customer_journal_contents(dict) : Filename→content map for CUSTOMER folder EJ files.
        employee_code            (str)  : Optional employee code for metadata storage.

    RETURNS:
        dict : Structured response with summary, analysis, timestamp, metadata.

    RAISES:
        ValueError  : If the record has insufficient data for the model.
        ImportError : If the ollama package is not installed.
    """

    if ui_journal_contents is None:
        ui_journal_contents = {}
    if customer_journal_contents is None:
        customer_journal_contents = {}

    _preprocessor = LogPreprocessorService()
    _merger        = TransactionMergerService()

    # ── STEP 1: Build EJ record directly from txn_data ───────────────────
    ej_record = _build_ej_record_from_txn_data(txn_data, transaction_log)

    logger.info(
        f"EJ record built from txn_data | "
        f"txn_id={transaction_id} | "
        f"ts_start={ej_record.get('ts_start')} | "
        f"type={ej_record.get('type')} | "
        f"status={ej_record.get('status')} | "
        f"has_protocol_steps={bool(ej_record.get('protocol_steps'))} | "
        f"has_device_errors={bool(ej_record.get('device_errors'))}"
    )

    # Guard: record must have at minimum ts_start and type
    meaningful_keys = {k for k, v in ej_record.items() if v not in (None, [], "", False)}
    if len(meaningful_keys) < 2:
        raise ValueError(
            "Transaction record has insufficient diagnostic information to send to the model."
        )

    # ── STEP 2: JRN enrichment from JOURNAL folder ───────────────────────
    # Use extract_diagnostic_context_from_content to get structured diagnostic
    # data from the JOURNAL folder .jrn file. This is ALWAYS attempted when
    # we have a matching JRN file, regardless of whether txn_data already has
    # JRN fields — the JOURNAL folder may contain richer data.

    jrn_already_enriched = bool(
        txn_data.get("JRN Protocol Steps") or
        txn_data.get("JRN Response Code") or
        txn_data.get("JRN Device Errors")
    )

    jrn_context_enriched = False
    source_stem = str(txn_data.get('Source File') or txn_data.get('Source_File') or '')
    matched_jrn_file = match_journal_file(source_stem, ui_journal_files)

    if matched_jrn_file:
        jrn_filename = Path(matched_jrn_file).name
        raw_jrn = ui_journal_contents.get(jrn_filename, '')

        if raw_jrn and raw_jrn.strip():
            ts_start = ej_record.get('ts_start', '')
            ts_end   = ej_record.get('ts_end', '')

            if ts_start and ts_end:
                logger.info(
                    f"Extracting JOURNAL diagnostic context from {jrn_filename} "
                    f"[{ts_start}-{ts_end}] for {transaction_id}"
                )
                jrn_context = extract_diagnostic_context_from_content(
                    jrn_content=raw_jrn,
                    jrn_filename=jrn_filename,
                    start_time_str=ts_start,
                    end_time_str=ts_end,
                )

                # Count non-empty fields for logging
                ctx_fields = sum(
                    1 for v in jrn_context.values()
                    if v and v != [] and v != ''
                )
                logger.info(
                    f"JOURNAL context extracted: {ctx_fields} non-empty fields "
                    f"from {jrn_filename}"
                )

                if ctx_fields > 0:
                    _enrich_record_with_jrn_context(ej_record, jrn_context)
                    jrn_context_enriched = True
                    logger.info(
                        f"EJ record enriched with JOURNAL context for {transaction_id}"
                    )
            else:
                logger.warning(
                    f"Cannot extract JOURNAL context — missing ts_start/ts_end "
                    f"for {transaction_id}"
                )
        else:
            logger.warning(
                f"JOURNAL file '{jrn_filename}' not found or empty in session contents "
                f"(available keys: {list(ui_journal_contents.keys())[:10]})"
            )
    else:
        logger.warning(
            f"No matching JOURNAL file for source '{source_stem}' in {ui_journal_files}"
        )

    # Legacy fallback: preprocess_jrn for EJ-only sessions with no JOURNAL
    # context extracted above.
    merged_record = ej_record
    if not jrn_already_enriched and not jrn_context_enriched and matched_jrn_file:
        jrn_filename = Path(matched_jrn_file).name
        raw_jrn = ui_journal_contents.get(jrn_filename, '')
        if raw_jrn and raw_jrn.strip():
            try:
                jrn_records = _preprocessor.preprocess_jrn(raw_jrn)
                if jrn_records:
                    logger.info(
                        f"JRN preprocess_jrn fallback: {len(jrn_records)} record(s) "
                        f"from {jrn_filename}"
                    )
                    merged_list = _merger.merge([ej_record], jrn_records)
                    if merged_list:
                        merged_record = merged_list[0]
                        logger.info(
                            f"JRN preprocess_jrn fallback merge successful "
                            f"for {transaction_id}"
                        )
                else:
                    logger.warning(
                        f"JRN preprocess_jrn fallback: no records from {jrn_filename}"
                    )
            except Exception as jrn_err:
                logger.warning(f"JRN preprocess_jrn fallback failed: {jrn_err}")

    jrn_data_available = jrn_already_enriched or jrn_context_enriched or (merged_record is not ej_record)
    logger.info(
        f"JRN data summary | already_enriched={jrn_already_enriched} | "
        f"context_enriched={jrn_context_enriched} | "
        f"preprocess_merged={merged_record is not ej_record} | "
        f"jrn_data_available={jrn_data_available}"
    )

    # ── STEP 2b: Strip emv_events that carry no diagnostic value ────────────
    # [3214] card track 2 lines with fully masked data have zero signal for the LLM.
    # Keep only lines that contain an AID, chip decision, or non-masked content.
    if ej_record.get('emv_events'):
        ej_record['emv_events'] = [
            e for e in ej_record['emv_events']
            if not (
                '[3214]' in e and
                '[MASKED]' in e and
                'AID:' not in e
            )
        ]
        if not ej_record['emv_events']:
            del ej_record['emv_events']

    # ── STEP 3: Build prompt ──────────────────────────────────────────────
    user_content = _preprocessor.build_prompt(
        [merged_record],
        atm_id=str(txn_data.get('Terminal ID', ''))
    )

    # ── STEP 3b: Append compact CUSTOMER journal EJ excerpt ─────────────
    # Only lines that carry diagnostic value NOT already in the structured
    # record are kept.  PII is masked, redundant patterns stripped, and
    # output is capped at 40 lines to avoid prompt bloat.
    if transaction_log and transaction_log.strip():
        compact_ej = _compact_ej_for_prompt(transaction_log, max_lines=40)
        if compact_ej.strip():
            user_content += (
                "\n\n--- CUSTOMER JOURNAL (EJ) SUPPLEMENTARY LINES ---\n"
                f"{compact_ej}"
            )
            logger.info(
                f"Appended compact CUSTOMER EJ excerpt ({len(compact_ej)} chars, "
                f"from {len(transaction_log)} raw) to LLM prompt for {transaction_id}"
            )
        else:
            logger.info(
                f"No non-redundant EJ lines for {transaction_id} — "
                f"structured record covers everything"
            )

    # ── STEP 3c: Inject dynamic few-shot examples ───────────────────────
    # Only injected for complex transactions (host offline, cancel, retract,
    # chained, device errors). Simple cases (customer cancel, timeout, success)
    # skip injection entirely — saves ~660 tokens per call.
    # top_k=1 to minimise token overhead while still providing format guidance.
    if needs_examples(ej_record):
        relevant_examples = fetch_relevant_examples(ej_record, top_k=1)
        if relevant_examples:
            example_block = build_example_block(relevant_examples)
            user_content = example_block + "\n\nNow analyze the following transaction and produce output in the same format:\n\n" + user_content
            logger.info(
                f"Injected {len(relevant_examples)} few-shot example(s) into prompt "
                f"for {transaction_id}"
            )
        else:
            logger.info(f"Complex transaction but no matching examples found for {transaction_id}")
    else:
        logger.info(f"Simple transaction — skipping few-shot injection for {transaction_id}")

    if len(user_content.strip()) < MIN_PROMPT_CHARS:
        raise ValueError(
            "No diagnostic information could be extracted from the transaction record. "
            "The transaction may have no parseable content."
        )

    # ── STEP 4: Input monitoring ──────────────────────────────────────────
    prompt_char_count  = len(user_content)
    prompt_token_est   = prompt_char_count // 4
    prompt_fingerprint = hashlib.md5(user_content.encode()).hexdigest()[:8]

    logger.info(
        f"LLM INPUT MONITOR | "
        f"txn_id={transaction_id} | "
        f"chars={prompt_char_count} | "
        f"tokens_est={prompt_token_est} | "
        f"jrn_available={jrn_data_available} | "
        f"fingerprint={prompt_fingerprint}"
    )

    if prompt_token_est > 3500:
        logger.warning(
            f"Prompt for {transaction_id} is large (~{prompt_token_est} estimated tokens). "
            f"Consider trimming the transaction log before sending to the model."
        )

    # ── Debug dump ────────────────────────────────────────────────────────
    debug_path = Path("llm_debug_input.json")
    debug_path.write_text(
        json.dumps(
            {
                "transaction_id":        transaction_id,
                "prompt":                user_content,
                "jrn_available":         jrn_data_available,
                "jrn_already_enriched":  jrn_already_enriched,
                "jrn_context_enriched":  jrn_context_enriched,
                "prompt_chars":          prompt_char_count,
                "prompt_tokens_est":     prompt_token_est,
                "prompt_fingerprint":    prompt_fingerprint,
            },
            indent=2,
            ensure_ascii=False
        ),
        encoding="utf-8"
    )
    logger.info(f"LLM input dumped to {debug_path.resolve()}")

    # ── STEP 5: Ollama call ───────────────────────────────────────────────
    # Uses ollama.chat() with a fresh single-message list on every call.
    # No history object is ever appended to — each call is fully independent.
    # options are passed explicitly to override any Modelfile defaults at
    # call time (temperature, num_predict).
    logger.info(f"Calling Ollama model: {MODEL_NAME}")
    analysis_start = time.perf_counter()

    response = ollama.chat(
        model=MODEL_NAME,
        messages=[{"role": "user", "content": user_content}],
        options={
            "temperature": 0.1,
            "num_predict": 400,
        }
    )

    analysis_duration = round(time.perf_counter() - analysis_start, 3)
    raw_response      = response["message"]["content"].strip()

    # ── STEP 6: Token usage logging ───────────────────────────────────────
    actual_prompt_tokens   = response.get("prompt_eval_count", "N/A")
    actual_response_tokens = response.get("eval_count", "N/A")

    if isinstance(actual_prompt_tokens, int) and isinstance(actual_response_tokens, int):
        total_tokens = actual_prompt_tokens + actual_response_tokens
        logger.info(
            f"LLM TOKEN USAGE | "
            f"txn_id={transaction_id} | "
            f"prompt_tokens={actual_prompt_tokens} | "
            f"response_tokens={actual_response_tokens} | "
            f"total_tokens={total_tokens}"
        )
    else:
        logger.info(
            f"LLM TOKEN USAGE | "
            f"txn_id={transaction_id} | "
            f"prompt_tokens={actual_prompt_tokens} | "
            f"response_tokens={actual_response_tokens} | "
            f"total_tokens=N/A"
        )

    logger.info(
        f"LLM analysis complete | "
        f"txn_id={transaction_id} | "
        f"model={MODEL_NAME} | "
        f"jrn_used={jrn_data_available} | "
        f"time={analysis_duration}s | "
        f"response_chars={len(raw_response)}"
    )

    # ── STEP 7: Store metadata ────────────────────────────────────────────
    store_metadata(
        transaction_id        = transaction_id,
        employee_code         = employee_code,
        model                 = MODEL_NAME,
        transaction_type      = str(txn_data.get('Transaction Type', 'Unknown')),
        transaction_state     = str(txn_data.get('End State', 'Unknown')),
        source_file           = str(txn_data.get('Source File', 'Unknown')),
        start_time            = str(txn_data.get('Start Time', '')),
        end_time              = str(txn_data.get('End Time', '')),
        log_length            = len(transaction_log),
        response_length       = len(raw_response),
        analysis_time_seconds = analysis_duration,
        llm_analysis          = raw_response
    )
    logger.info(f"Metadata stored for txn: {transaction_id}")

    # ── STEP 8: Return structured response ───────────────────────────────
    return {
        "summary":   "Transaction log analysis completed",
        "analysis":  raw_response,
        "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "metadata": {
            "transaction_id":        transaction_id,
            "model":                 MODEL_NAME,
            "log_length":            prompt_char_count,
            "response_length":       len(raw_response),
            "analysis_type":         "anomaly_detection",
            "transaction_type":      str(txn_data.get('Transaction Type', 'Unknown')),
            "transaction_state":     str(txn_data.get('End State', 'Unknown')),
            "start_time":            str(txn_data.get('Start Time', '')),
            "end_time":              str(txn_data.get('End Time', '')),
            "source_file":           str(txn_data.get('Source File', 'Unknown')),
            "jrn_data_available":    jrn_data_available,
            "jrn_context_enriched":  jrn_context_enriched,
            "analysis_time_seconds": analysis_duration,
            "prompt_tokens":         actual_prompt_tokens,
            "response_tokens":       actual_response_tokens,
            "prompt_fingerprint":    prompt_fingerprint,
        }
    }