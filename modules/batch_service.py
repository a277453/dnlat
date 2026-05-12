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

from modules.processing import LogPreprocessorService, TransactionMergerService
from modules.journal_parser import match_journal_file
from modules.example_store import fetch_relevant_examples, build_example_block, needs_examples
from modules.logging_config import logger

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


def _read_file(path_str: str) -> Optional[str]:
    """Read a file with UTF-8 fallback. Returns None on any failure."""
    p = Path(path_str)
    if not p.exists():
        BATCH_LOG.warning(f"File not found: {path_str}")
        return None
    try:
        return p.read_text(encoding="utf-8", errors="replace")
    except Exception as e:
        BATCH_LOG.warning(f"Failed to read {p.name}: {e}")
        return None


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
# Per EJ+JRN pair processing
# ─────────────────────────────────────────────────────────────────────────────

async def process_ej_jrn_pair(
    conn: asyncpg.Connection,
    session_id: str,
    ej_path: str,
    jrn_path: Optional[str],
    preprocessor: LogPreprocessorService,
    merger: TransactionMergerService,
) -> int:
    """
    Process one EJ file (and its matched JRN file) and insert all transactions
    into txn_input_cache.

    Returns the number of transactions successfully inserted.
    """
    ej_filename  = Path(ej_path).stem       # e.g. "20250403"
    jrn_filename = Path(jrn_path).name if jrn_path else None

    BATCH_LOG.info(
        f"[{session_id}] Processing pair: EJ={ej_filename} | "
        f"JRN={jrn_filename or 'none'}"
    )

    # ── 1. Read raw files ──────────────────────────────────────────────────
    ej_raw  = _read_file(ej_path)
    jrn_raw = _read_file(jrn_path) if jrn_path else None

    if not ej_raw:
        BATCH_LOG.warning(f"[{session_id}] Empty or unreadable EJ: {ej_path}")
        return 0

    # ── 2. Preprocess EJ ──────────────────────────────────────────────────
    try:
        ej_records = preprocessor.preprocess_ej(ej_raw)
    except Exception as e:
        BATCH_LOG.error(
            f"[{session_id}] EJ preprocessing failed ({ej_filename}): {e}"
        )
        return 0

    if not ej_records:
        BATCH_LOG.warning(
            f"[{session_id}] No EJ records parsed from {ej_filename}"
        )
        return 0

    BATCH_LOG.info(
        f"[{session_id}] EJ={ej_filename}: {len(ej_records)} record(s) parsed"
    )

    # ── 3. Preprocess JRN (optional enrichment) ───────────────────────────
    jrn_records = []
    if jrn_raw:
        try:
            jrn_records = preprocessor.preprocess_jrn(jrn_raw)
            BATCH_LOG.info(
                f"[{session_id}] JRN={jrn_filename}: "
                f"{len(jrn_records)} record(s) parsed"
            )
        except Exception as e:
            BATCH_LOG.warning(
                f"[{session_id}] JRN preprocessing failed ({jrn_filename}): {e} "
                f"— continuing with EJ-only"
            )

    # ── 4. Merge EJ + JRN ─────────────────────────────────────────────────
    if ej_records and jrn_records:
        try:
            merged = merger.merge(ej_records, jrn_records)
            BATCH_LOG.info(
                f"[{session_id}] Merged {len(merged)} transaction(s) "
                f"from EJ+JRN pair"
            )
        except Exception as e:
            BATCH_LOG.warning(
                f"[{session_id}] Merge failed, falling back to EJ-only: {e}"
            )
            merged = ej_records
    else:
        merged = ej_records

    # ── 5. Build filtered_input and insert per transaction ─────────────────
    inserted = 0
    for txn in merged:
        try:
            filtered_input = preprocessor.build_prompt(
                records=[txn],
                atm_id=str(txn.get("atm_id") or ""),
            )
        except Exception as e:
            BATCH_LOG.warning(
                f"[{session_id}] build_prompt failed for txn "
                f"{txn.get('txn_number')}: {e} — skipping"
            )
            continue

        # ── 5b. Inject few-shot examples (same logic as llm_service) ──────
        # Complex transactions (retract, offline, chained, device errors) need
        # example guidance to maintain output quality. Baked into filtered_input
        # at batch time so cache hits get full prompt quality with zero extra
        # work at analysis time.
        try:
            if needs_examples(txn):
                relevant_examples = fetch_relevant_examples(txn, top_k=1)
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
                        f"[{session_id}] Few-shot injected for txn "
                        f"{txn.get('txn_number')} "
                        f"({len(relevant_examples)} example(s))"
                    )
        except Exception as e:
            # Few-shot failure is non-fatal — store prompt without examples
            BATCH_LOG.warning(
                f"[{session_id}] Few-shot injection failed for txn "
                f"{txn.get('txn_number')}: {e} — storing prompt without examples"
            )

        success = await _upsert_transaction(
            conn=conn,
            session_id=session_id,
            ej_filename=ej_filename,
            jrn_filename=jrn_filename,
            txn=txn,
            filtered_input=filtered_input,
        )
        if success:
            inserted += 1

    return inserted


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
        merger        = TransactionMergerService()

        file_categories = session_data.get("file_categories", {})

        # Customer journal files (EJ) — primary source
        ej_files = file_categories.get("customer_journals", [])

        # Journal files (JRN) — enrichment; exclude VCP-PRO subtree
        all_jrn_files = [
            f for f in file_categories.get("ui_journals", [])
            if "vcp-pro" not in str(f).replace("\\", "/").lower()
        ]

        if not ej_files:
            BATCH_LOG.warning(
                f"[{session_id}] No customer journal files found — "
                f"nothing to batch"
            )
            return

        BATCH_LOG.info(
            f"[{session_id}] Found {len(ej_files)} EJ file(s) | "
            f"{len(all_jrn_files)} JRN file(s)"
        )

        total_inserted = 0

        for ej_path in ej_files:
            ej_stem     = Path(ej_path).stem
            matched_jrn = match_journal_file(ej_stem, all_jrn_files)

            if matched_jrn:
                BATCH_LOG.info(
                    f"[{session_id}] Matched: {Path(ej_path).name} "
                    f"→ {Path(matched_jrn).name}"
                )
            else:
                BATCH_LOG.info(
                    f"[{session_id}] No JRN match for {Path(ej_path).name} "
                    f"— EJ-only processing"
                )

            try:
                count = await process_ej_jrn_pair(
                    conn=conn,
                    session_id=session_id,
                    ej_path=ej_path,
                    jrn_path=matched_jrn,
                    preprocessor=preprocessor,
                    merger=merger,
                )
                total_inserted += count
            except Exception as e:
                # One EJ file failed entirely — log and continue with the rest
                BATCH_LOG.error(
                    f"[{session_id}] Unexpected error processing "
                    f"{Path(ej_path).name}: {e} — skipping file",
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