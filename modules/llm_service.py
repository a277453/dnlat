# modules/llm_service.py

import json
import time
import hashlib
import ollama
from pathlib import Path
from datetime import datetime

from modules.processing import LogPreprocessorService, TransactionMergerService
from modules.journal_parser import match_journal_file
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

    record = {
        "ts_start":         _safe_ts(txn_data.get("Start Time")),
        "ts_end":           _safe_ts(txn_data.get("End Time")),
        "txn_number":       str(txn_data.get("Transaction ID", "")),
        "type":             str(txn_data.get("Transaction Type", "Unknown")),
        "status":           str(txn_data.get("End State", "Unknown")),
        # NOTE: raw_log intentionally excluded — contains PAN and unredacted host data
        # JRN-enriched fields already merged during analyze_customer_journals
        "protocol_steps":   _parse_list_field(txn_data.get("JRN Protocol Steps")),
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

    # Strip empty/null values so build_prompt receives only meaningful fields
    return {k: v for k, v in record.items() if v not in (None, [], "", False)}


def analyze_transaction(
    transaction_id: str,
    transaction_log: str,
    txn_data: dict,
    ui_journal_files: list,
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
                raw_jrn = Path(matched_jrn_file).read_text(encoding='utf-8', errors='replace')
                if raw_jrn.strip():
                    jrn_records = _preprocessor.preprocess_jrn(raw_jrn)
                    if jrn_records:
                        logger.info(
                            f"JRN fallback: {len(jrn_records)} record(s) from "
                            f"{Path(matched_jrn_file).name}"
                        )
                        merged_list = _merger.merge([ej_record], jrn_records)
                        if merged_list:
                            merged_record = merged_list[0]
                            logger.info(f"JRN fallback merge successful for {transaction_id}")
                    else:
                        logger.warning(
                            f"JRN fallback: no records extracted from "
                            f"{Path(matched_jrn_file).name}"
                        )
                else:
                    logger.warning(f"JRN fallback file is empty: {Path(matched_jrn_file).name}")
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

    logger.info(
        f"LLM TOKEN USAGE | "
        f"txn_id={transaction_id} | "
        f"prompt_tokens={actual_prompt_tokens} | "
        f"response_tokens={actual_response_tokens} | "
        f"total_tokens={actual_prompt_tokens + actual_response_tokens
            if isinstance(actual_prompt_tokens, int) and isinstance(actual_response_tokens, int)
            else 'N/A'}"
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