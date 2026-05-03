"""
modules/chunk_service.py
========================
All business logic for chunked ZIP uploads.

Responsibilities
----------------
  save_chunk()          – persist one incoming chunk to a staging directory
  assemble_and_process()– merge all chunks into a ZIP, validate, run pipeline
  cancel_upload()       – delete a partial upload's staging directory

The router (api/chunked_upload.py) only calls these three functions.
"""

import base64 as _b64
import io as _io
import shutil
import tempfile
import time
import zipfile
from pathlib import Path
from typing import Optional

from fastapi import HTTPException

from modules.categorization import CategorizationService
from modules.extraction import ZipExtractionService, extract_from_zip_bytes
from modules.flat_file_generator import FlatFileMerger
from modules.logging_config import logger
from modules.processing import ProcessingService
from modules.session import session_service

# ── Staging directory for in-flight uploads ──────────────────────────────────
STAGING_ROOT = Path(tempfile.gettempdir()) / "dn_chunk_uploads"
STAGING_ROOT.mkdir(parents=True, exist_ok=True)

_STALE_SECONDS = 60 * 60        # 1 hour — staging folders older than this are purged



# ─────────────────────────────────────────────────────────────────────────────
# INTERNAL UTILITIES
# ─────────────────────────────────────────────────────────────────────────────

def _purge_stale() -> None:
    """Remove staging dirs not touched in the last hour."""
    cutoff = time.time() - _STALE_SECONDS
    for child in STAGING_ROOT.iterdir():
        if child.is_dir() and child.stat().st_mtime < cutoff:
            shutil.rmtree(child, ignore_errors=True)
            logger.info("[CHUNK_SVC] Purged stale dir: %s", child.name)


def _chunk_path(upload_id: str, index: int) -> Path:
    d = STAGING_ROOT / upload_id
    d.mkdir(parents=True, exist_ok=True)
    return d / f"chunk_{index:06d}.bin"


# ─────────────────────────────────────────────────────────────────────────────
# PUBLIC FUNCTION 1 — save_chunk
# ─────────────────────────────────────────────────────────────────────────────

def save_chunk(
    upload_id: str,
    chunk_index: int,
    total_chunks: int,
    filename: str,
    data: bytes,
) -> dict:
    """
    Persist one chunk to disk.

    Parameters
    ----------
    upload_id    : Client-generated UUID for this upload session.
    chunk_index  : 0-based position of this chunk.
    total_chunks : Total number of chunks the client will send.
    filename     : Original ZIP filename — used only for validation here.
    data         : Raw bytes of the chunk.

    Returns
    -------
    dict with status, upload_id, chunk_index, received_bytes.
    """
    _purge_stale()

    if not filename.lower().endswith(".zip"):
        raise HTTPException(status_code=400, detail="Only ZIP files are supported.")

    if total_chunks < 1:
        raise HTTPException(status_code=400, detail="total_chunks must be ≥ 1.")

    if not (0 <= chunk_index < total_chunks):
        raise HTTPException(
            status_code=400,
            detail=f"chunk_index {chunk_index} is out of range (total={total_chunks}).",
        )

    try:
        _chunk_path(upload_id, chunk_index).write_bytes(data)
    except Exception as exc:
        raise HTTPException(
            status_code=500,
            detail=f"Failed to write chunk {chunk_index}: {exc}",
        )

    logger.info(
        "[CHUNK_SVC] Saved chunk %d/%d, %s bytes ,upload_id=%s",
        chunk_index + 1, total_chunks, f"{len(data):,}", upload_id,
    )
    return {
        "status":         "ok",
        "upload_id":      upload_id,
        "chunk_index":    chunk_index,
        "received_bytes": len(data),
    }


# ─────────────────────────────────────────────────────────────────────────────
# PUBLIC FUNCTION 2 — assemble_and_process
# ─────────────────────────────────────────────────────────────────────────────

async def assemble_and_process(
    upload_id: str,
    total_chunks: int,
    mode: Optional[str],
) -> dict:
    """
    1. Verify every chunk is present.
    2. Concatenate chunks in order → ZIP bytes.
    3. Delete the staging directory immediately.
    4. Validate the ZIP.
    5. Run the full extraction + categorisation + session pipeline.
    6. Return a response dict identical in shape to /process-zip.

    Parameters
    ----------
    upload_id    : UUID used when calling save_chunk().
    total_chunks : Must match the value sent during upload.
    mode         : Optional processing mode forwarded to CategorizationService.
    """
    staging = STAGING_ROOT / upload_id

    if not staging.exists():
        raise HTTPException(
            status_code=404,
            detail=f"No staged chunks for upload_id={upload_id}. "
                   "Call /upload-chunk first.",
        )

    # ── Check all chunks are present ─────────────────────────────────────────
    missing = [
        i for i in range(total_chunks)
        if not (staging / f"chunk_{i:06d}.bin").exists()
    ]
    if missing:
        raise HTTPException(
            status_code=400,
            detail=f"Missing chunk(s): {missing}. Upload them before finalising.",
        )

    # ── Assemble ─────────────────────────────────────────────────────────────
    logger.info("[CHUNK_SVC] Assembling %d chunks , upload_id=%s", total_chunks, upload_id)
    t0 = time.perf_counter()

    zip_bytes = b"".join(
        (staging / f"chunk_{i:06d}.bin").read_bytes()
        for i in range(total_chunks)
    )

    shutil.rmtree(staging, ignore_errors=True)      # free disk space immediately
    logger.info(
        "[CHUNK_SVC] Assembly done: %s bytes in %.2fs",
        f"{len(zip_bytes):,}", time.perf_counter() - t0,
    )

    # ── Validate ─────────────────────────────────────────────────────────────
    if not zipfile.is_zipfile(_io.BytesIO(zip_bytes)):
        raise HTTPException(
            status_code=400,
            detail="Assembled file is not a valid ZIP. One or more chunks may be corrupt.",
        )

    # ── Run extraction pipeline ───────────────────────────────────────────────
    try:
        return await _run_extraction_pipeline(zip_bytes, mode)
    except HTTPException:
        raise
    except Exception as exc:
        logger.error("[CHUNK_SVC] Pipeline error: %s", exc, exc_info=True)
        raise HTTPException(status_code=500, detail=f"Processing failed: {exc}")


# ─────────────────────────────────────────────────────────────────────────────
# PUBLIC FUNCTION 3 — cancel_upload
# ─────────────────────────────────────────────────────────────────────────────

def cancel_upload(upload_id: str) -> dict:
    """
    Delete all staged chunks for an aborted upload.
    Safe to call even when the upload_id does not exist.
    """
    staging = STAGING_ROOT / upload_id
    if staging.exists():
        shutil.rmtree(staging, ignore_errors=True)
        logger.info("[CHUNK_SVC] Cancelled upload_id=%s", upload_id)
        return {"status": "cancelled", "upload_id": upload_id}
    return {"status": "not_found", "upload_id": upload_id}


# ─────────────────────────────────────────────────────────────────────────────
# PRIVATE — extraction + categorisation + session pipeline
# ─────────────────────────────────────────────────────────────────────────────

async def _run_extraction_pipeline(zip_bytes: bytes, mode: Optional[str]) -> dict:
    """
    Mirrors the body of routes.process_zip_file() so the two endpoints
    produce identical response dicts.
    """
    from api.routes import set_processed_files_dir, CURRENT_SESSION_ID as _CURRENT_SESSION_ID

    start = time.perf_counter()

    # ── 1. Extract ────────────────────────────────────────────────────────────
    ZipExtractionService().cleanup_old_extracts(max_age_hours=0.5)
    try:
        extract_path, total_members, acu_zip_bytes = ZipExtractionService().extract_zip(zip_bytes)
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"ZIP extraction failed: {exc}")
    logger.info("[CHUNK_SVC] Extracted %d members → %s", total_members, extract_path)

    # ── 2. ACU extraction ─────────────────────────────────────────────────────
    acu_logs: list = []
    try:
        acu_files = (
            extract_from_zip_bytes(acu_zip_bytes, acu_logs, target_prefixes=("jdd", "x3"))
            if acu_zip_bytes else {}
        )
        logger.info(
            "[CHUNK_SVC] ACU: %d XML, %d XSD",
            sum(1 for k in acu_files if not k.startswith("__xsd__")),
            sum(1 for k in acu_files if k.startswith("__xsd__")),
        )
    except Exception as exc:
        logger.error("[CHUNK_SVC] ACU error: %s", exc)
        acu_files = {}
        acu_logs.append(f"ACU error: {exc}")

    # ── 3. Categorisation ─────────────────────────────────────────────────────
    file_categories: dict = {
        "customer_journals": [], "ui_journals": [],
        "trc_trace": [],        "trc_error": [],
        "registry_files": [],   "acu_files": [],
        "journal_llm_files": [], "unidentified": [],
    }
    CategorizationService().categorize_files(extract_path, file_categories, set(), mode=mode)
    logger.info("[CHUNK_SVC] Categorisation complete")

    # ── 4. Flat-file merge ────────────────────────────────────────────────────
    FlatFileMerger.run(
        customer_paths=file_categories.get("customer_journals", []),
        ui_paths=file_categories.get("ui_journals", []),
        llm_paths=file_categories.get("journal_llm_files", []),
    )

    # ── 5. Load file contents into memory ────────────────────────────────────
    def _read_text(p: Path) -> str:
        for enc in ("utf-8", "latin1", "windows-1252", "utf-16"):
            try:
                return p.read_text(encoding=enc, errors="strict")
            except Exception:
                continue
        return p.read_text(encoding="latin1", errors="replace")

    def _load_text(branch: str) -> dict:
        out = {}
        for ps in file_categories.get(branch, []):
            p = Path(ps)
            try:
                out[p.name] = _read_text(p)
            except Exception as exc:
                logger.error("[CHUNK_SVC][%s] %s: %s", branch.upper(), p.name, exc)
        return out

    registry_contents = {
        Path(ps).name: _b64.b64encode(Path(ps).read_bytes()).decode("utf-8")
        for ps in file_categories.get("registry_files", [])
        if _safe_read_bytes(ps) is not None
    }

    customer_journal_contents = _load_text("customer_journals")
    ui_journal_contents       = _load_text("ui_journals")
    journal_llm_contents      = _load_text("journal_llm_files")
    trc_trace_contents        = _load_text("trc_trace")
    trc_error_contents        = _load_text("trc_error")
    extra_contents            = {Path(ps).name: {} for ps in file_categories.get("unidentified", [])}

    # ── 6. Strip full paths → bare filenames ─────────────────────────────────
    for branch in file_categories:
        file_categories[branch] = [Path(p).name for p in file_categories[branch]]

    # ── 7. Session ────────────────────────────────────────────────────────────
    set_processed_files_dir(None)
    session_service.create_session(_CURRENT_SESSION_ID, file_categories, None)
    _upd = session_service.update_session
    _upd(_CURRENT_SESSION_ID, "acu_extracted_files",       acu_files)
    _upd(_CURRENT_SESSION_ID, "acu_extraction_logs",       acu_logs)
    _upd(_CURRENT_SESSION_ID, "registry_contents",         registry_contents)
    _upd(_CURRENT_SESSION_ID, "customer_journal_contents", customer_journal_contents)
    _upd(_CURRENT_SESSION_ID, "ui_journal_contents",       ui_journal_contents)
    _upd(_CURRENT_SESSION_ID, "journal_llm_contents",      journal_llm_contents)
    _upd(_CURRENT_SESSION_ID, "trc_trace_contents",        trc_trace_contents)
    _upd(_CURRENT_SESSION_ID, "trc_error_contents",        trc_error_contents)
    _upd(_CURRENT_SESSION_ID, "extra_contents",            extra_contents)

    # ── 8. Clean up temp extract dir ─────────────────────────────────────────
    try:
        shutil.rmtree(extract_path, ignore_errors=False)
    except Exception as exc:
        logger.error("[CHUNK_SVC] Could not remove temp dir %s: %s", extract_path, exc)

    # ── 9. Build response ─────────────────────────────────────────────────────
    result = ProcessingService().prepare_response(file_categories, extract_path)
    result.acu_extraction_logs = acu_logs

    total_time = round(time.perf_counter() - start, 2)
    logger.info("[CHUNK_SVC] Pipeline done in %.2fs", total_time)

    result_dict = result.dict() if hasattr(result, "dict") else dict(result)
    result_dict["processing_time_seconds"] = total_time
    result_dict["session_id"] = _CURRENT_SESSION_ID
    return result_dict


def _safe_read_bytes(path_str: str) -> Optional[bytes]:
    """Read bytes from a path, returning None on any error."""
    try:
        return Path(path_str).read_bytes()
    except Exception as exc:
        logger.error("[CHUNK_SVC][REGISTRY] %s: %s", path_str, exc)
        return None
