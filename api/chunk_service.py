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
    authorization: Optional[str] = None,
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
        return await _run_extraction_pipeline(zip_bytes, mode, authorization)
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

async def _run_extraction_pipeline(zip_bytes: bytes, mode: Optional[str], authorization: Optional[str] = None) -> dict:
    """
    Mirrors the body of routes.process_zip_file() so the two endpoints
    produce identical response dicts.
    """
    from api.routes import set_processed_files_dir, _build_session_id

    start = time.perf_counter()

    # ── 1. Extract ────────────────────────────────────────────────────────────
    ZipExtractionService().cleanup_old_extracts(max_age_hours=0.5)
    try:
        extract_path, total_members, acu_zip_bytes_list = ZipExtractionService().extract_zip(zip_bytes)
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"ZIP extraction failed: {exc}")
    logger.info("[CHUNK_SVC] Extracted %d members → %s", total_members, extract_path)

    # ── 2. ACU extraction ─────────────────────────────────────────────────────
    # extract_zip returns a LIST of acu.zip byte-strings (one per main ZIP found).
    # Must loop and merge — passing the list directly to extract_from_zip_bytes
    # would silently fail because it expects bytes, not List[bytes].
    acu_logs: list = []
    acu_files: dict = {}
    try:
        for acu_zip_bytes in acu_zip_bytes_list:
            partial = extract_from_zip_bytes(acu_zip_bytes, acu_logs, target_prefixes=("jdd", "x3"))
            for key, content in partial.items():
                dedup_key = key
                while dedup_key in acu_files:
                    dedup_key += "_dup"
                acu_files[dedup_key] = content
        logger.info(
            "[CHUNK_SVC] ACU: %d XML, %d XSD",
            sum(1 for k in acu_files if not k.startswith("__xsd__")),
            sum(1 for k in acu_files if k.startswith("__xsd__")),
        )
        if not acu_zip_bytes_list:
            logger.info("[CHUNK_SVC] No acu.zip found in uploaded package — skipping ACU extraction.")
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
        write_to_disk=True,
        output_dir=Path(__file__).parent.parent / "merged_output",
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
    # Build a fresh session ID from the JWT, purge the old one for this user,
    # then create the new session. We do NOT read the module-level
    # CURRENT_SESSION_ID from routes.py because that import captures the value
    # at import time (always "") — instead we call _build_session_id directly.
    import api.routes as _routes_mod
    session_id, emp_suffix = _build_session_id(authorization)
    purged = session_service.cleanup_sessions_by_suffix(emp_suffix)
    if purged:
        logger.info("[CHUNK_SVC] Purged %d old session(s) with suffix '%s'", purged, emp_suffix)
    _routes_mod._USER_SESSIONS[emp_suffix] = session_id   # per-user session registry
    logger.info("[CHUNK_SVC] Session ID built: %s → stored for %s", session_id, emp_suffix)

    set_processed_files_dir(None)
    session_service.create_session(session_id, file_categories, None)
    _upd = session_service.update_session
    _upd(session_id, "acu_extracted_files",       acu_files)
    _upd(session_id, "acu_extraction_logs",       acu_logs)
    _upd(session_id, "registry_contents",         registry_contents)
    _upd(session_id, "customer_journal_contents", customer_journal_contents)
    _upd(session_id, "ui_journal_contents",       ui_journal_contents)
    _upd(session_id, "journal_llm_contents",      journal_llm_contents)
    _upd(session_id, "trc_trace_contents",        trc_trace_contents)
    _upd(session_id, "trc_error_contents",        trc_error_contents)
    _upd(session_id, "extra_contents",            extra_contents)

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
    result_dict["session_id"] = session_id
    return result_dict


def _safe_read_bytes(path_str: str) -> Optional[bytes]:
    """Read bytes from a path, returning None on any error."""
    try:
        return Path(path_str).read_bytes()
    except Exception as exc:
        logger.error("[CHUNK_SVC][REGISTRY] %s: %s", path_str, exc)
        return None