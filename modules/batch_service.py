"""
batch_service.py

Batch Preprocessing Service for DNLAT.

Triggered automatically in the background after a ZIP upload is processed.
For every EJ + JRN file pair in the session, it:
  1. Parses and merges transactions via LogPreprocessorService + TransactionMergerService
  2. Builds the compact filtered_input prompt (same as llm_service does per-transaction)
  3. Estimates token count
  4. Inserts one row per transaction into `txn_input_cache` (PostgreSQL)

After this runs, llm_service.analyze_transaction() reads filtered_input directly
from the DB instead of re-building it — zero re-parsing, zero re-merging.

Fallback behaviour (three layers):
  LAYER 1 — DB unreachable at batch time:
    _connect() returns None → batch exits cleanly, warning logged, nothing cached.
    Analysis falls back to on-the-fly builds via _get_filtered_input() in llm_service.

  LAYER 2 — DB drops mid-batch:
    _upsert_transaction() wraps each INSERT in try/except.
    One failed row is logged and skipped — the rest of the batch continues.
    Any missed rows fall back to on-the-fly builds at analysis time.

  LAYER 3 — Unexpected error anywhere:
    Outer try/except in batch_preprocess_session catches everything.
    Background task never crashes the worker process.
"""

import hashlib
import time
import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

import asyncpg

from modules.processing import LogPreprocessorService
from modules.journal_parser import match_journal_file, extract_diagnostic_context_from_content
from modules.example_store import fetch_relevant_examples, build_example_block, needs_examples
from modules.logging_config import logger
from modules.llm_service import (
    _build_ej_record_from_txn_data,
    _enrich_record_with_jrn_context,
    _compact_ej_for_prompt,
)

# ── Module logger ─────────────────────────────────────────────────────────────

BATCH_LOG = logging.getLogger("batch_service")

# ── DB connect timeout (seconds) ─────────────────────────────────────────────
# Prevents the background task from hanging on a dead/slow DB.
DB_CONNECT_TIMEOUT = 5


# ─────────────────────────────────────────────────────────────────────────────
# DB helpers
# ─────────────────────────────────────────────────────────────────────────────

async def _connect(db_url: str) -> Optional[asyncpg.Connection]:
    """
    Attempt to open a DB connection with a hard timeout.

    Returns None instead of raising if the DB is unreachable — callers treat
    None as a signal to skip all DB work and degrade gracefully.
    """
    try:
        return await asyncpg.connect(db_url, timeout=DB_CONNECT_TIMEOUT)
    except Exception as e:
        BATCH_LOG.warning(f"DB connection failed: {e}")
        return None


async def ensure_table_exists(conn: asyncpg.Connection) -> bool:
    """
    Create txn_input_cache if it does not already exist.
    Safe to call on every startup — uses IF NOT EXISTS.

    Returns True on success, False on failure (caller should abort batch).
    """
    try:
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS txn_input_cache (
                txn_id              VARCHAR(128)    PRIMARY KEY,
                session_id          VARCHAR(128)    NOT NULL,
                date                VARCHAR(8),
                time_start          VARCHAR(8),
                time_end            VARCHAR(8),
                duration_seconds    INTEGER,
                transaction_type    VARCHAR(64),
                status              VARCHAR(32),
                ej_filename         VARCHAR(256),
                jrn_filename        VARCHAR(256),
                filtered_input      TEXT            NOT NULL,
                prompt_hash         VARCHAR(16),
                token_estimate      INTEGER,
                analysis_result     TEXT,
                processed_at        TIMESTAMPTZ     DEFAULT NOW()
            );

            CREATE INDEX IF NOT EXISTS idx_txn_cache_session
                ON txn_input_cache (session_id);

            CREATE INDEX IF NOT EXISTS idx_txn_cache_date
                ON txn_input_cache (date);

            CREATE INDEX IF NOT EXISTS idx_txn_cache_hash
                ON txn_input_cache (prompt_hash);
        """)
        return True
    except Exception as e:
        BATCH_LOG.warning(f"ensure_table_exists failed: {e}")
        return False


# ─────────────────────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────────────────────

def _estimate_tokens(text: str) -> int:
    """Rough token estimate: 1 token ≈ 4 characters."""
    return len(text) // 4


def _prompt_hash(text: str) -> str:
    """First 16 chars of MD5 — used for dedup and observability."""
    return hashlib.md5(text.encode()).hexdigest()[:16]



# ─────────────────────────────────────────────────────────────────────────────
# Per-row upsert  (LAYER 2 fallback lives here)
# ─────────────────────────────────────────────────────────────────────────────

async def _upsert_transaction(
    conn: asyncpg.Connection,
    session_id: str,
    ej_filename: str,
    jrn_filename: Optional[str],
    txn: dict,
    filtered_input: str,
) -> bool:
    """
    Insert a single transaction row into txn_input_cache.

    Returns True if inserted successfully, False if skipped or failed.

    ON CONFLICT DO NOTHING — if the row already exists (re-upload of same
    file) we skip silently. Change to DO UPDATE SET ... if you want
    filtered_input refreshed on re-upload.

    Each row is wrapped in its own try/except so a mid-batch DB blip or
    constraint error drops only that one row — the rest of the batch continues.
    Any rows that fail here will fall back to on-the-fly builds at analysis time.
    """
    txn_id     = str(txn.get("txn_number") or txn.get("transaction_id") or "")
    date_val   = str(txn.get("date") or ej_filename or "")[:8]
    time_start = str(txn.get("ts_start") or "")
    time_end   = str(txn.get("ts_end") or "")
    duration   = txn.get("duration_seconds")
    txn_type   = str(txn.get("type") or txn.get("transaction_type") or "Unknown")
    status     = str(txn.get("status") or "Unknown")
    token_est  = _estimate_tokens(filtered_input)
    p_hash     = _prompt_hash(filtered_input)

    if not txn_id:
        BATCH_LOG.warning(
            f"[{session_id}] Skipping transaction with no txn_id "
            f"(ej={ej_filename}, ts={time_start})"
        )
        return False

    try:
        await conn.execute("""
            INSERT INTO txn_input_cache (
                txn_id, session_id, date, time_start, time_end,
                duration_seconds, transaction_type, status,
                ej_filename, jrn_filename,
                filtered_input, prompt_hash, token_estimate,
                analysis_result, processed_at
            ) VALUES (
                $1,  $2,  $3,  $4,  $5,
                $6,  $7,  $8,
                $9,  $10,
                $11, $12, $13,
                NULL, $14
            )
            ON CONFLICT (txn_id) DO NOTHING
        """,
            txn_id, session_id, date_val, time_start, time_end,
            duration, txn_type, status,
            ej_filename, jrn_filename,
            filtered_input, p_hash, token_est,
            datetime.now(tz=timezone.utc),
        )

        BATCH_LOG.debug(
            f"[{session_id}] Cached txn_id={txn_id} | "
            f"type={txn_type} | status={status} | "
            f"tokens≈{token_est} | hash={p_hash}"
        )
        return True

    except Exception as e:
        # Row-level failure — log and continue, do NOT abort the batch.
        BATCH_LOG.warning(
            f"[{session_id}] Insert failed for txn_id={txn_id}: {e} — skipping row"
        )
        return False


# ─────────────────────────────────────────────────────────────────────────────
# Per-transaction processing  (mirrors llm_service.analyze_transaction exactly)
# ─────────────────────────────────────────────────────────────────────────────

async def _process_single_txn(
    conn: asyncpg.Connection,
    session_id: str,
    txn_data: dict,
    ui_contents: dict,
    ui_journal_files: list,
    preprocessor: LogPreprocessorService,
) -> bool:
    """
    Build filtered_input for one transaction using the identical pipeline as
    llm_service.analyze_transaction — same record builder, same JRN enrichment,
    same prompt builder, same EJ append, same few-shot injection.

    Returns True if the row was inserted successfully, False otherwise.
    """
    txn_id     = str(txn_data.get("Transaction ID") or "")
    txn_log    = str(txn_data.get("Transaction Log") or "")
    source_file = str(txn_data.get("Source File") or txn_data.get("Source_File") or "")

    if not txn_id:
        BATCH_LOG.warning(f"[{session_id}] Skipping txn with no Transaction ID")
        return False

    # ── STEP 1: Build EJ record from txn_data (identical to llm_service) ──
    try:
        ej_record = _build_ej_record_from_txn_data(txn_data, txn_log)
    except Exception as e:
        BATCH_LOG.warning(
            f"[{session_id}] _build_ej_record_from_txn_data failed "
            f"for {txn_id}: {e} — skipping"
        )
        return False

    meaningful_keys = {k for k, v in ej_record.items() if v not in (None, [], "", False)}
    if len(meaningful_keys) < 2:
        BATCH_LOG.warning(
            f"[{session_id}] Insufficient data in EJ record for {txn_id} — skipping"
        )
        return False

    # ── STEP 2: JRN enrichment (identical to llm_service) ─────────────────
    matched_jrn = match_journal_file(source_file, ui_journal_files)
    if matched_jrn:
        raw_jrn = ui_contents.get(matched_jrn, "")
        if raw_jrn and raw_jrn.strip():
            ts_start = ej_record.get("ts_start", "")
            ts_end   = ej_record.get("ts_end", "")
            if ts_start and ts_end:
                try:
                    jrn_context = extract_diagnostic_context_from_content(
                        jrn_content=raw_jrn,
                        jrn_filename=matched_jrn,
                        start_time_str=ts_start,
                        end_time_str=ts_end,
                    )
                    ctx_fields = sum(
                        1 for v in jrn_context.values()
                        if v and v != [] and v != ""
                    )
                    if ctx_fields > 0:
                        _enrich_record_with_jrn_context(ej_record, jrn_context)
                        BATCH_LOG.debug(
                            f"[{session_id}] JRN enriched {txn_id} "
                            f"({ctx_fields} fields from {matched_jrn})"
                        )
                except Exception as e:
                    BATCH_LOG.warning(
                        f"[{session_id}] JRN enrichment failed for {txn_id}: {e}"
                    )

    # ── STEP 2b: Strip low-signal EMV events (identical to llm_service) ───
    if ej_record.get("emv_events"):
        ej_record["emv_events"] = [
            e for e in ej_record["emv_events"]
            if not ("[3214]" in e and "[MASKED]" in e and "AID:" not in e)
        ]
        if not ej_record["emv_events"]:
            del ej_record["emv_events"]

    # ── STEP 3: Build prompt (identical to llm_service) ───────────────────
    try:
        filtered_input = preprocessor.build_prompt(
            [ej_record],
            atm_id=str(txn_data.get("Terminal ID", "")),
        )
    except Exception as e:
        BATCH_LOG.warning(
            f"[{session_id}] build_prompt failed for {txn_id}: {e} — skipping"
        )
        return False

    # ── STEP 3b: Append compact EJ excerpt (identical to llm_service) ─────
    if txn_log and txn_log.strip():
        try:
            compact_ej = _compact_ej_for_prompt(txn_log, max_lines=40)
            if compact_ej.strip():
                filtered_input += (
                    "\n\n--- CUSTOMER JOURNAL (EJ) SUPPLEMENTARY LINES ---\n"
                    + compact_ej
                )
        except Exception as e:
            BATCH_LOG.warning(
                f"[{session_id}] _compact_ej_for_prompt failed for {txn_id}: {e}"
            )

    # ── STEP 3c: Inject few-shot examples (identical to llm_service) ──────
    try:
        if needs_examples(ej_record):
            relevant_examples = fetch_relevant_examples(ej_record, top_k=1)
            if relevant_examples:
                example_block    = build_example_block(relevant_examples)
                _waiting_phrases = (
                    "after the examples you will receive",
                    "you will receive a new transaction",
                    "a new transaction to analyze",
                )
                cleaned_lines = [
                    line for line in example_block.splitlines()
                    if not any(p in line.lower() for p in _waiting_phrases)
                ]
                example_block  = "\n".join(cleaned_lines)
                filtered_input = (
                    example_block
                    + "\n\n=== END OF EXAMPLES — DO NOT ANALYZE THE ABOVE ===\n\n"
                    + "ANALYZE THIS TRANSACTION NOW. Do not ask for more input. "
                    + "Do not repeat or summarize the examples. "
                    + "Produce the full analysis output immediately.\n\n"
                    + filtered_input
                )
                BATCH_LOG.debug(
                    f"[{session_id}] Few-shot injected for {txn_id} "
                    f"({len(relevant_examples)} example(s))"
                )
    except Exception as e:
        BATCH_LOG.warning(
            f"[{session_id}] Few-shot injection failed for {txn_id}: {e} "
            f"— storing prompt without examples"
        )

    # ── INSERT into txn_input_cache ───────────────────────────────────────
    txn_meta = {
        "txn_number":       txn_id,
        "transaction_id":   txn_id,
        "date":             str(txn_data.get("Source File") or "")[:8],
        "ts_start":         str(txn_data.get("Start Time") or ej_record.get("ts_start") or ""),
        "ts_end":           str(txn_data.get("End Time") or ej_record.get("ts_end") or ""),
        "duration_seconds": txn_data.get("Duration (seconds)"),
        "type":             str(txn_data.get("Transaction Type") or "Unknown"),
        "status":           str(txn_data.get("End State") or "Unknown"),
    }

    return await _upsert_transaction(
        conn=conn,
        session_id=session_id,
        ej_filename=source_file,
        jrn_filename=matched_jrn,
        txn=txn_meta,
        filtered_input=filtered_input,
    )


# ─────────────────────────────────────────────────────────────────────────────
# Entry point — called from routes.py as a BackgroundTask
# ─────────────────────────────────────────────────────────────────────────────

async def batch_preprocess_session(
    session_id: str,
    session_data: dict,
    db_url: str,
) -> None:
    """
    Main entry point. Called by the upload route as a FastAPI BackgroundTask.

    Args:
        session_id   : UUID of the current session.
        session_data : The full session dict from session_service.get_session().
        db_url       : asyncpg-compatible PostgreSQL connection string.
                       e.g. "postgresql://user:pass@localhost:5432/dnlat"

    Fallback contract:
        This function NEVER raises. All failure modes are caught, logged as
        warnings, and the function returns cleanly. The upload response and
        the UI are never blocked. Individual transaction analysis will fall
        back to on-the-fly builds via _get_filtered_input() in llm_service.py
        for any transactions that were not cached.
    """
    t_start = time.perf_counter()
    BATCH_LOG.info(f"[{session_id}] Batch preprocessing started")

    # ── LAYER 1: DB unreachable — exit cleanly ─────────────────────────────
    conn = await _connect(db_url)
    if conn is None:
        BATCH_LOG.warning(
            f"[{session_id}] DB unreachable — batch preprocessing skipped. "
            f"All analysis requests will fall back to on-the-fly builds."
        )
        return

    try:
        # ── Ensure schema — exit cleanly if this fails ─────────────────────
        table_ok = await ensure_table_exists(conn)
        if not table_ok:
            BATCH_LOG.warning(
                f"[{session_id}] Could not ensure cache table exists — "
                f"batch preprocessing skipped."
            )
            return

        preprocessor = LogPreprocessorService()

        # ── Read from session — same sources llm_service uses ──────────────
        # transaction_data: fully parsed list of txn_data dicts from
        #   transaction_analyzer — same input _build_ej_record_from_txn_data needs.
        # ui_journal_contents + journal_llm_contents: merged exactly as
        #   analyze_transaction_llm does (line: all_jrn_contents = {**ui, **llm})
        transaction_data     = session_data.get("transaction_data", [])
        ui_contents          = session_data.get("ui_journal_contents", {}) or {}
        journal_llm_contents = session_data.get("journal_llm_contents", {}) or {}

        # Merge both — ui_journal_contents keys take precedence on collision
        # (matches the behavior in analyze_transaction_llm route exactly)
        all_jrn_contents = {**ui_contents, **journal_llm_contents}

        # Exclude VCP-PRO entries from JRN pool
        all_jrn_contents = {
            k: v for k, v in all_jrn_contents.items()
            if "vcp-pro" not in k.replace("\\", "/").lower()
        }
        ui_journal_files = list(all_jrn_contents.keys())

        if not transaction_data:
            BATCH_LOG.warning(
                f"[{session_id}] No transaction_data in session — "
                f"nothing to batch (run analyze-customer-journals first)"
            )
            return

        BATCH_LOG.info(
            f"[{session_id}] Found {len(transaction_data)} transaction(s) | "
            f"{len(ui_journal_files)} JRN file(s) in session"
        )

        total_inserted = 0

        for txn_data in transaction_data:
            try:
                success = await _process_single_txn(
                    conn=conn,
                    session_id=session_id,
                    txn_data=txn_data,
                    ui_contents=all_jrn_contents,
                    ui_journal_files=ui_journal_files,
                    preprocessor=preprocessor,
                )
                if success:
                    total_inserted += 1
            except Exception as e:
                BATCH_LOG.error(
                    f"[{session_id}] Unexpected error processing txn "
                    f"{txn_data.get('Transaction ID', '?')}: {e} — skipping",
                    exc_info=True,
                )

        elapsed = round(time.perf_counter() - t_start, 2)
        BATCH_LOG.info(
            f"[{session_id}] Batch preprocessing complete | "
            f"transactions cached: {total_inserted} | "
            f"elapsed: {elapsed}s"
        )

    except Exception as e:
        # ── LAYER 3: Outer safety net ──────────────────────────────────────
        # Should never be reached, but ensures the background task never
        # crashes the worker process regardless of what goes wrong.
        BATCH_LOG.error(
            f"[{session_id}] Batch preprocessing encountered unexpected error: {e}",
            exc_info=True,
        )
    finally:
        # Always close the connection, even on error paths
        try:
            await conn.close()
        except Exception:
            pass