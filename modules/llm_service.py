# modules/llm_service.py

import json
import time
import hashlib
import ollama
from pathlib import Path
from datetime import datetime

from modules.processing import LogPreprocessorService, TransactionMergerService
from modules.journal_parser import match_journal_file, mask_ej_log
from modules.analysis import store_metadata
from modules.logging_config import logger

MODEL_NAME       = "llama3_log_analyzer"
MIN_PROMPT_CHARS = 150


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
        - raw_log is PII-masked via mask_ej_log() before inclusion.
          PAN, EMV AID, track-2, amounts, and host hex are stripped.
        - All fields are sourced from the pre-stripped txn_data DataFrame row.

        SEQUENCE CONTEXT:
        - ordered_events is constructed to give the LLM a chronologically
          accurate picture of what happened. This is critical for distinguishing
          CASH retract from CARD retract — without ordering, the LLM conflates
          them. card_ejected is explicitly placed before dispense/retract events
          so the model knows the card was already taken before cash issues occurred.

    PARAMETERS:
        txn_data        (dict) : Transaction row dict from session DataFrame.
        transaction_log (str)  : Raw per-transaction log — masked and included
                                 as customer_journal_log for LLM context.

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
    raw_events     = _parse_list_field(txn_data.get("JRN Events"))
    card_ejected   = txn_data.get("JRN Card Ejected")
    retract_counter = txn_data.get("JRN Retract Counter")

    ordered_events = []
    if card_ejected:
        ordered_events.append("Card ejected and taken by customer")
    dispense_events = [e for e in raw_events if "Dispense" in e]
    ordered_events.extend(dispense_events)
    present_events = [e for e in raw_events if "Present" in e or "present" in e]
    ordered_events.extend(present_events)
    device_events = [e for e in raw_events if "DeviceError" in e or "Error" in e]
    ordered_events.extend(device_events)
    if retract_counter:
        ordered_events.append(f"CASH retracted (not card) — Retract counter {retract_counter}")
    else:
        retract_events = [e for e in raw_events if "Retract" in e or "retract" in e]
        ordered_events.extend(retract_events)
    already_added = set(ordered_events)
    for ev in raw_events:
        if ev not in already_added:
            ordered_events.append(ev)

    # ── Extract compressed EJ signals (NOT raw log) ───────────────────────
    # Parse only diagnostic-relevant lines from the EJ transaction log.
    # This replaces sending the full masked log — targets ~80% token reduction.
    ej_flow = []        # key transaction flow steps
    ej_host_comm = []   # host request/reply pairs
    ej_errors = []      # EJ-level error indicators

    if transaction_log and transaction_log.strip():
        import re as _re
        for line in transaction_log.splitlines():
            line = line.strip()
            if not line:
                continue

            # Extract message code from EJ line: "HH:MM:SS <code> <message>"
            m = _re.match(r'(\d{2}:\d{2}:\d{2})\s+(\d+)\s+(.*)', line)
            if not m:
                continue
            ts, code, msg = m.group(1), m.group(2), m.group(3)

            # Transaction flow events (compact summary)
            if code == '3207':
                ej_flow.append(f"{ts} Card inserted")
            elif code == '3201':
                pass  # Transaction started — redundant with ts_start
            elif code == '3217':
                func_m = _re.search(r"Function '([^']+)' selected", msg)
                if func_m:
                    ej_flow.append(f"{ts} Function: {func_m.group(1)}")
            elif code == '3233':
                ej_flow.append(f"{ts} PIN entered")
            elif code == '3209':
                ej_flow.append(f"{ts} Card presented to customer")
            elif code == '3245':
                ej_flow.append(f"{ts} Card retained")
            elif code == '3202':
                state_m = _re.search(r"state '(\w+)'.*end-state\s*'(\w)'", msg)
                if state_m:
                    ej_flow.append(f"{ts} End state={state_m.group(1)}/{state_m.group(2)}")
            elif code == '3219':
                ej_flow.append(f"{ts} Dispense command")
            elif code == '3220':
                ej_flow.append(f"{ts} Retract")

            # Host communication (compressed)
            elif code == '6303':
                req_m = _re.search(r'TRANSACTION REQUEST\s+(\S+)', msg)
                key_m = _re.search(r'\(KEY:\s*([^,)]+)', msg)
                if req_m:
                    ej_host_comm.append(
                        f"{ts} REQ:{req_m.group(1).strip()}"
                        + (f" KEY:{key_m.group(1).strip()}" if key_m else "")
                    )
            elif code == '6304':
                reply_m = _re.search(r'TRANSACTION REPLY\s+(\S+)\s+(\d+)', msg)
                if reply_m:
                    ej_host_comm.append(f"{ts} REPLY:{reply_m.group(1)} code={reply_m.group(2)}")

            # Error indicators from EJ
            elif code == '3974':
                ej_flow.append(f"{ts} {msg[:80]}")  # mag stripe / EMV fallback note
            elif 'cancel' in msg.lower() or 'timeout' in msg.lower():
                ej_errors.append(f"{ts} {msg[:60]}")

    record = {
        "ts_start":         _safe_ts(txn_data.get("Start Time")),
        "ts_end":           _safe_ts(txn_data.get("End Time")),
        "txn_number":       str(txn_data.get("Transaction ID", "")),
        "type":             str(txn_data.get("Transaction Type", "Unknown")),
        "status":           str(txn_data.get("End State", "Unknown")),
        # Compressed EJ signals (NOT raw log):
        "ej_flow":          ej_flow,         # key transaction steps
        "ej_host_comm":     ej_host_comm,    # host request/reply summary
        "ej_errors":        ej_errors,       # EJ-level error indicators
        # JRN-exclusive diagnostic fields:
        "protocol_steps":   _parse_list_field(txn_data.get("JRN Protocol Steps")),
        "device_errors":    _parse_list_field(txn_data.get("JRN Device Errors")),
        "response_code":    txn_data.get("JRN Response Code"),
        "device_states":    txn_data.get("JRN Device States"),
        # Sequenced events for retract diagnosis:
        "events":           ordered_events,
        # Retract context:
        "card_ejected":     bool(card_ejected) if card_ejected else None,
        "retract_counter":  retract_counter,
    }

    # Strip empty/null values so build_prompt receives only meaningful fields
    return {k: v for k, v in record.items() if v not in (None, [], "", False)}


def analyze_transaction(
    transaction_id: str,
    transaction_log: str,
    txn_data: dict,
    ui_journal_files: list,
    employee_code: str = None,
    ui_journal_contents: dict = None,
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

        JRN enrichment path:
        - Primary:  JRN fields already present in txn_data (merged during
                    analyze_customer_journals at session creation time) — used directly.
        - Fallback: If JRN enrichment columns are empty (EJ-only session),
                    attempt to match and merge the JRN file now via preprocess_jrn
                    + TransactionMergerService.

        Also includes:
        - Input monitoring (char count, estimated tokens, fingerprint)
        - Actual token usage from Ollama response
        - Debug dump to llm_debug_input.json
        - Metadata storage via store_metadata

    PARAMETERS:
        transaction_id   (str)  : Unique transaction identifier (filenameHHMMSS format).
        transaction_log  (str)  : Raw per-transaction log text from the DataFrame.
        txn_data         (dict) : Transaction row dict from session DataFrame.
        ui_journal_files (list) : JOURNAL/-level JRN file paths (VCP-PRO excluded).
        employee_code    (str)  : Optional employee code for metadata storage.

    RETURNS:
        dict : Structured response with summary, analysis, timestamp, metadata.

    RAISES:
        ValueError  : If the record has insufficient data for the model.
        ImportError : If the ollama package is not installed.
    """

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

    # ── STEP 2: Determine JRN enrichment path ────────────────────────────
    # Primary: JRN fields already present in txn_data from session creation.
    jrn_already_enriched = bool(
        txn_data.get("JRN Protocol Steps") or
        txn_data.get("JRN Response Code") or
        txn_data.get("JRN Device Errors")
    )

    merged_record = ej_record

    if jrn_already_enriched:
        logger.info(
            f"JRN fields already present in txn_data for {transaction_id} — "
            f"skipping JRN file re-read"
        )
    else:
        # Fallback: EJ-only session — attempt JRN file match + merge now.
        logger.info(
            f"No JRN enrichment in txn_data for {transaction_id} — "
            f"attempting JRN file fallback"
        )
        source_stem      = str(txn_data.get('Source File') or txn_data.get('Source_File') or '')
        matched_jrn_file = match_journal_file(source_stem, ui_journal_files)

        if matched_jrn_file:
            try:
                # Try in-memory content first (disk may have been deleted)
                raw_jrn = None
                _contents = ui_journal_contents or {}
                if _contents.get(matched_jrn_file):
                    raw_jrn = _contents[matched_jrn_file]
                    logger.info(f"JRN fallback: reading from session memory — {matched_jrn_file}")
                elif Path(matched_jrn_file).exists():
                    raw_jrn = Path(matched_jrn_file).read_text(encoding='utf-8', errors='replace')
                    logger.info(f"JRN fallback: reading from disk — {matched_jrn_file}")
                else:
                    logger.warning(
                        f"JRN fallback: file not on disk and not in session memory — "
                        f"{matched_jrn_file}"
                    )

                if raw_jrn and raw_jrn.strip():
                    # ── Primary: use journal_parser.extract_diagnostic_context ──
                    # preprocess_jrn() expects EJ-style codes (3207/3202) which
                    # JRN files don't have. extract_diagnostic_context() handles
                    # the actual JRN line format correctly.
                    import tempfile
                    from modules.journal_parser import extract_diagnostic_context

                    ts_start = ej_record.get("ts_start", "")
                    ts_end   = ej_record.get("ts_end", "")

                    if ts_start and ts_end:
                        # Write to temp file since extract_diagnostic_context expects a path
                        try:
                            with tempfile.NamedTemporaryFile(
                                mode='w', suffix='.jrn', delete=False, encoding='utf-8'
                            ) as tmp:
                                tmp.write(raw_jrn)
                                tmp_path = tmp.name

                            diag = extract_diagnostic_context(tmp_path, ts_start, ts_end)

                            # Clean up temp file
                            try:
                                Path(tmp_path).unlink()
                            except Exception:
                                pass

                            # Check if we got meaningful diagnostic data
                            has_diag = any(
                                bool(diag.get(k))
                                for k in ('protocol_steps', 'device_errors', 'host_replies',
                                          'response_codes', 'card_events', 'device_states',
                                          'emv_events', 'chip_decision')
                            )

                            if has_diag:
                                merged_record = dict(ej_record)

                                # Only include JRN fields that add NEW diagnostic value
                                # beyond what the customer_journal_log already provides.
                                #
                                # KEEP (JRN-exclusive signals):
                                #   device_errors  — ErrorNr with class/code (not in EJ)
                                #   device_states  — 5011 state changes (not in EJ)
                                #   protocol_steps — TDR_ transitions (not in EJ)
                                #   cryptogram_info— CI= values (not in EJ)
                                #   tvr_tsi        — TVR/TSI flags (not in EJ)
                                #
                                # SKIP (already in customer_journal_log):
                                #   emv_events     — duplicates EJ 3951-3954 lines
                                #   card_events    — duplicates EJ 3207/3209/3205
                                #   host_outcome   — duplicates EJ 6303/6304
                                #   host_notes     — duplicates EJ host reply context
                                #   chip_decision  — partially in EJ; keep only for
                                #                    enrichment when EJ lacks 3960/3961
                                #   customer_actions — duplicates EJ 3259

                                for field in ('device_errors', 'device_states',
                                              'protocol_steps', 'cryptogram_info',
                                              'tvr_tsi'):
                                    if diag.get(field):
                                        merged_record[field] = diag[field]

                                # chip_decision: only include if EJ log doesn't
                                # already contain 3960/3961 decline lines
                                ej_log = ej_record.get('customer_journal_log', '')
                                if diag.get('chip_decision') and '3960' not in ej_log:
                                    merged_record['chip_decision'] = diag['chip_decision']

                                if diag.get('response_codes'):
                                    merged_record['response_code'] = diag['response_codes'][0]
                                if diag.get('stan_values'):
                                    merged_record['stan'] = diag['stan_values'][0]
                                if diag.get('app_state_start'):
                                    merged_record['app_state_start'] = diag['app_state_start']
                                if diag.get('app_state_end'):
                                    merged_record['app_state_end'] = diag['app_state_end']

                                logger.info(
                                    f"JRN fallback via extract_diagnostic_context: "
                                    f"protocol_steps={len(diag.get('protocol_steps', []))} "
                                    f"device_errors={len(diag.get('device_errors', []))} "
                                    f"card_events={len(diag.get('card_events', []))} "
                                    f"response_codes={diag.get('response_codes', [])}"
                                )
                            else:
                                logger.warning(
                                    f"JRN fallback: extract_diagnostic_context returned "
                                    f"no meaningful data for {matched_jrn_file}"
                                )
                        except Exception as diag_err:
                            logger.warning(f"JRN fallback via extract_diagnostic_context failed: {diag_err}")
                    else:
                        logger.warning(
                            f"JRN fallback: missing ts_start/ts_end — cannot extract context"
                        )
                elif raw_jrn is not None:
                    logger.warning(f"JRN fallback file is empty: {matched_jrn_file}")
            except Exception as jrn_err:
                logger.warning(f"JRN fallback failed: {jrn_err} — using EJ only")
        else:
            logger.warning(
                f"JRN fallback: no matching file for source '{source_stem}' — EJ only"
            )

    jrn_data_available = jrn_already_enriched or (merged_record is not ej_record)
    logger.info(f"JRN data available: {jrn_data_available}")

    # ── STEP 3: Build prompt ──────────────────────────────────────────────
    user_content = _preprocessor.build_prompt(
        [merged_record],
        atm_id=str(txn_data.get('Terminal ID', ''))
    )

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

    messages = [{"role": "user", "content": user_content}]

    # ── Debug dump ────────────────────────────────────────────────────────
    debug_path = Path("llm_debug_input.json")
    debug_path.write_text(
        json.dumps(
            {
                "transaction_id":       transaction_id,
                "messages":             messages,
                "jrn_available":        jrn_data_available,
                "jrn_already_enriched": jrn_already_enriched,
                "prompt_chars":         prompt_char_count,
                "prompt_tokens_est":    prompt_token_est,
                "prompt_fingerprint":   prompt_fingerprint,
            },
            indent=2,
            ensure_ascii=False
        ),
        encoding="utf-8"
    )
    logger.info(f"LLM input dumped to {debug_path.resolve()}")

    # ── STEP 5: Ollama call ───────────────────────────────────────────────
    logger.info(f"Calling Ollama model: {MODEL_NAME}")
    analysis_start = time.perf_counter()

    response = ollama.chat(model=MODEL_NAME, messages=messages)

    analysis_duration = round(time.perf_counter() - analysis_start, 3)
    raw_response      = response["message"]["content"].strip()

    # ── STEP 6: Token usage logging ───────────────────────────────────────
    actual_prompt_tokens   = response.get("prompt_eval_count", "N/A")
    actual_response_tokens = response.get("eval_count", "N/A")

    _total_tokens = (
        actual_prompt_tokens + actual_response_tokens
        if isinstance(actual_prompt_tokens, int) and isinstance(actual_response_tokens, int)
        else 'N/A'
    )

    logger.info(
        f"LLM TOKEN USAGE | "
        f"txn_id={transaction_id} | "
        f"prompt_tokens={actual_prompt_tokens} | "
        f"response_tokens={actual_response_tokens} | "
        f"total_tokens={_total_tokens}"
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
            "analysis_time_seconds": analysis_duration,
            "prompt_tokens":         actual_prompt_tokens,
            "response_tokens":       actual_response_tokens,
            "prompt_fingerprint":    prompt_fingerprint,
        }
    }